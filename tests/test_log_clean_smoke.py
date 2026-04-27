from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

SECONDS_PER_DAY = 86_400


def set_mtime(path: Path, *, age_days: int) -> None:
    """Set a file or directory mtime to now minus the requested age."""
    timestamp = time.time() - (age_days * SECONDS_PER_DAY)
    os.utime(path, (timestamp, timestamp))


def rendered_records(records: dict[str, Any]) -> list[Any]:
    """Flatten rendered audit samples from the DAG audit-bucket mapping."""
    return [record for bucket in records.values() for record in bucket.rendered]


def dir_candidate(module: Any, path: Path) -> Any:
    stat_result = path.stat(follow_symlinks=False)
    return module.DirCandidate(
        path=str(path),
        device=int(stat_result.st_dev),
        inode=int(stat_result.st_ino),
    )


def file_candidate(module: Any, path: Path) -> Any:
    stat_result = path.stat(follow_symlinks=False)
    return module.FileCandidate(
        path=str(path),
        device=int(stat_result.st_dev),
        inode=int(stat_result.st_ino),
        mtime=float(stat_result.st_mtime),
        mtime_ns=int(stat_result.st_mtime_ns),
        size_bytes=int(stat_result.st_size),
    )


def test_module_smoke_imports_and_exposes_dag(import_log_clean: Any) -> None:
    module = import_log_clean

    assert module.DAG_ID == "airflow_log_cleanup"
    assert module.SCHEDULE == "@daily"
    assert module.DELETE_ENABLED is True
    assert module.dag is not None
    assert callable(module.execute_cleanup)


@pytest.mark.parametrize("raw_value", [1, "1", " 7 ", 12])
def test_coerce_positive_int_accepts_positive_values(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    assert module._coerce_positive_int(raw_value, field_name="x") == int(str(raw_value).strip())


@pytest.mark.parametrize("raw_value", [0, "0"])
def test_coerce_positive_int_skips_zero_retention(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    with pytest.raises(module.AirflowSkipException, match="x=0 is unsafe"):
        module._coerce_positive_int(raw_value, field_name="x")


@pytest.mark.parametrize("raw_value", [0, "0"])
def test_coerce_positive_int_rejects_zero_when_skip_disabled(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    with pytest.raises(ValueError, match="x must be > 0"):
        module._coerce_positive_int(raw_value, field_name="x", zero_is_skip=False)


@pytest.mark.parametrize("raw_value", [True, False, -1, "-3", "abc", None])
def test_coerce_positive_int_rejects_invalid_values(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    with pytest.raises(ValueError):
        module._coerce_positive_int(raw_value, field_name="x")


@pytest.mark.parametrize(
    ("num_bytes", "expected"),
    [
        (0, "0 B"),
        (1, "1 B"),
        (1024, "1.00 KiB"),
        (1536, "1.50 KiB"),
    ],
)
def test_human_bytes_renders_binary_units(import_log_clean: Any, num_bytes: int, expected: str) -> None:
    module = import_log_clean

    assert module._human_bytes(num_bytes) == expected


def test_log_table_renders_header_rows_and_final_border(import_log_clean: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_log_clean
    captured: list[str] = []

    def capture_log(level: int, fmt: str, *args: Any, **_kwargs: Any) -> None:
        captured.append(fmt % args)

    monkeypatch.setattr(module.LOGGER, "log", capture_log)

    module._log_table(
        level=logging.INFO,
        number="99",
        title="Test Table",
        headers=["Field", "Value"],
        rows=[["flag", True], ["items", ["a", "b"]], ["path", Path("/tmp/x")]],
    )

    assert captured
    rendered = captured[0]
    lines = rendered.splitlines()
    assert lines[0].startswith("99 :: Test Table")
    table_lines = lines[1:]
    assert table_lines[0].startswith("+")
    assert table_lines[-1].startswith("+")
    assert "true" in rendered
    assert "a, b" in rendered
    assert "/tmp/x" in rendered


def test_audit_details_filters_empty_values_and_stringifies_complex_values(import_log_clean: Any) -> None:
    module = import_log_clean

    details = module._audit_details(
        none_value=None,
        empty_value="",
        bool_value=False,
        int_value=7,
        complex_value={"x": 1},
    )

    assert ("none_value", None) not in details
    assert ("empty_value", "") not in details
    assert ("bool_value", False) in details
    assert ("int_value", 7) in details
    assert ("complex_value", "{'x': 1}") in details


def test_add_audit_record_uses_relative_path_and_caps_rendered_samples(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    records: dict[str, Any] = {}
    root = tmp_path / "logs"
    root.mkdir()
    first = root / "a.log"
    second = root / "b.log"
    first.write_text("a", encoding="utf-8")
    second.write_text("b", encoding="utf-8")

    module._add_audit_record(records, audit_limit=1, cleanup_root=str(root), path=str(first), item_type="file", why="same reason")
    module._add_audit_record(records, audit_limit=1, cleanup_root=str(root), path=str(second), item_type="file", why="same reason")

    bucket = records["same reason"]
    assert bucket.total == 2
    assert [record.real_path for record in bucket.rendered] == ["a.log"]


def test_merge_audit_buckets_preserves_total_and_render_cap(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target: dict[str, Any] = {}
    source: dict[str, Any] = {}
    root = tmp_path / "logs"
    root.mkdir()
    for name in ["one.log", "two.log", "three.log"]:
        path = root / name
        path.write_text(name, encoding="utf-8")
        module._add_audit_record(source, audit_limit=0, cleanup_root=str(root), path=str(path), item_type="file", why="reason")

    module._merge_audit_buckets(target, source, audit_limit=2)

    assert target["reason"].total == 3
    assert len(target["reason"].rendered) == 2


def test_log_audit_list_caps_delete_mode_and_disables_cap_for_report_only(
    import_log_clean: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    records: dict[str, Any] = {}
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(3):
        path = root / f"{index}.log"
        path.write_text("x", encoding="utf-8")
        module._add_audit_record(records, audit_limit=0, cleanup_root=str(root), path=str(path), item_type="file", why="old")

    captured: list[str] = []

    def capture_info(fmt: str, *args: Any, **_kwargs: Any) -> None:
        captured.append(fmt % args)

    monkeypatch.setattr(module.LOGGER, "info", capture_info)

    module._log_audit_list("12", "Deleted Items", records, evaluation_epoch=time.time(), delete_log_cap=2)
    module._log_audit_list("12", "Candidate Items", records, evaluation_epoch=time.time(), delete_log_cap=0)

    assert "rendered_items=2" in captured[0]
    assert "capped_items=1" in captured[0]
    assert "DELETE_LOG_CAP=2" in captured[0]
    assert "rendered_items=3" in captured[1]
    assert "capped_items=0" in captured[1]
    assert "DELETE_LOG_CAP=disabled" in captured[1]


def test_resolve_top_level_targets_tracks_directory_file_symlink_and_deny_list(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    (tmp_path / "worker").mkdir()
    (tmp_path / "triggerer").mkdir()
    (tmp_path / "top-level.log").write_text("x", encoding="utf-8")
    external = tmp_path / "external"
    external.mkdir()
    (tmp_path / "linked-dir").symlink_to(external, target_is_directory=True)

    included, excluded = module._resolve_top_level_targets(
        base_log_folder=str(tmp_path),
        target_deny_list=["worker"],
    )

    assert [item.label for item in included] == ["scheduler", "triggerer"]
    excluded_by_label = {item.label: item for item in excluded}
    assert excluded_by_label["worker"].reason == "Excluded by TARGET_DENY_LIST"
    assert excluded_by_label["linked-dir"].item_type == "symlink"
    assert excluded_by_label["top-level.log"].item_type == "file"


@pytest.mark.parametrize("base_root", ["relative/path", "/", "/tmp", "/var/log"])
def test_build_settings_rejects_unsafe_base_roots(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    base_root: str,
) -> None:
    module = import_log_clean
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = base_root
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "30",
        module.DELETE_LOG_CAP_VARIABLE_KEY: "10",
    }

    with pytest.raises(module.AirflowSkipException, match="logging.base_log_folder"):
        module._build_settings({"dry_run": False})


def test_build_settings_aggregates_multiple_invalid_runtime_values(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "0",
        module.DELETE_LOG_CAP_VARIABLE_KEY: "0",
    }

    with pytest.raises(module.AirflowSkipException) as exc_info:
        module._build_settings({"dry_run": False})

    message = str(exc_info.value)
    assert module.MAX_LOG_AGE_VARIABLE_KEY in message
    assert module.DELETE_LOG_CAP_VARIABLE_KEY in message


def test_build_settings_accepts_delete_log_cap_zero_in_dry_run(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "30",
        module.DELETE_LOG_CAP_VARIABLE_KEY: "0",
    }

    settings = module._build_settings({"dry_run": True})

    assert settings.effective_delete_mode == "report-only"
    assert settings.delete_log_cap == 0


@pytest.mark.parametrize(
    ("dry_run", "expected_mode", "expected_cap"),
    [
        (True, "report-only", 0),
        ("true", "report-only", 0),
        (False, "delete", 7),
        ("false", "delete", 7),
    ],
)
def test_build_settings_parameter_variations(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    dry_run: bool | str,
    expected_mode: str,
    expected_cap: int,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    (tmp_path / "worker").mkdir()
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "45",
        module.TARGET_DENY_LIST_VARIABLE_KEY: "worker,bad/name, worker",
        module.DELETE_LOG_CAP_VARIABLE_KEY: "7",
    }

    settings = module._build_settings({"dry_run": dry_run})

    assert settings.target_deny_list == ["worker"]
    assert settings.invalid_target_deny_list == ["bad/name"]
    assert settings.max_log_age_days == 45
    assert settings.dry_run is (str(dry_run).strip().lower() == "true" if isinstance(dry_run, str) else dry_run)
    assert settings.effective_delete_mode == expected_mode
    assert settings.delete_log_cap == expected_cap


def test_scan_cleanup_target_classifies_old_young_symlink_and_cross_device_entries(
    import_log_clean: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    root = tmp_path / "scheduler"
    root.mkdir()
    old_file = root / "old.log"
    young_file = root / "young.log"
    cross_device_file = root / "cross.log"
    old_file.write_text("old", encoding="utf-8")
    young_file.write_text("young", encoding="utf-8")
    cross_device_file.write_text("cross", encoding="utf-8")
    set_mtime(old_file, age_days=5)
    set_mtime(young_file, age_days=1)
    set_mtime(cross_device_file, age_days=5)
    symlink_path = root / "current.log"
    symlink_path.symlink_to(young_file)

    original_stat = Path.stat
    root_device = original_stat(root, follow_symlinks=False).st_dev

    def patched_stat(self: Path, *, follow_symlinks: bool = True) -> Any:
        stat_result = original_stat(self, follow_symlinks=follow_symlinks)
        device = root_device + 99 if self == cross_device_file else root_device
        if self == root or root in self.parents:
            return SimpleNamespace(
                st_mode=stat_result.st_mode,
                st_ino=stat_result.st_ino,
                st_dev=device,
                st_size=stat_result.st_size,
                st_mtime_ns=stat_result.st_mtime_ns,
                st_mtime=stat_result.st_mtime,
            )
        return stat_result

    monkeypatch.setattr(Path, "stat", patched_stat)

    result = module._scan_cleanup_target(
        root,
        max_age_days=2,
        report_root=str(tmp_path),
        evaluation_epoch=time.time(),
        audit_limit=0,
    )

    excluded = rendered_records(result.excluded_records)
    assert [Path(candidate.path).name for candidate in result.old_files] == ["old.log"]
    assert result.stats.files_scanned_regular == 2
    assert result.stats.files_skipped_non_regular == 1
    assert result.stats.files_skipped_cross_device == 1
    assert any(record.why == "entry is not a regular file" for record in excluded)
    assert any(record.why == "file is on a different filesystem" for record in excluded)
    assert any(record.why == "regular file age is not above threshold 2d" for record in excluded)


def test_collect_empty_directories_handles_ignored_files_and_blocking_entries(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    root = tmp_path / "scheduler"
    removable = root / "will-empty"
    blocked = root / "blocked"
    removable.mkdir(parents=True)
    blocked.mkdir(parents=True)
    removable_file = removable / "old.log"
    blocked_file = blocked / "keep.log"
    removable_file.write_text("old", encoding="utf-8")
    blocked_file.write_text("keep", encoding="utf-8")

    result = module._collect_empty_directories(
        root,
        report_root=str(root),
        audit_limit=0,
        ignored_regular_files={str(removable_file)},
    )

    assert [candidate.path for candidate in result.empty_directories] == [str(removable)]
    assert blocked not in [Path(candidate.path) for candidate in result.empty_directories]


def test_delete_files_removes_matching_candidate_and_reports_bytes(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "old.log"
    target.write_text("abcdef", encoding="utf-8")
    set_mtime(target, age_days=10)

    result = module._delete_files([file_candidate(module, target)], report_root=str(tmp_path), audit_limit=10)

    assert result.deleted == 1
    assert result.deleted_bytes == 6
    assert not target.exists()
    assert len(result.deleted_records) == 1


def test_delete_files_skips_candidate_changed_after_scan(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "old.log"
    target.write_text("old", encoding="utf-8")
    candidate = file_candidate(module, target)
    time.sleep(0.001)
    target.write_text("changed-size", encoding="utf-8")

    result = module._delete_files([candidate], report_root=str(tmp_path), audit_limit=10)

    skipped = rendered_records(result.skipped_records)
    assert result.deleted == 0
    assert target.exists()
    assert any(record.why == "delete skipped because candidate changed after scan" for record in skipped)


def test_delete_files_skips_disappeared_candidate(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "old.log"
    target.write_text("old", encoding="utf-8")
    candidate = file_candidate(module, target)
    target.unlink()

    result = module._delete_files([candidate], report_root=str(tmp_path), audit_limit=10)

    skipped = rendered_records(result.skipped_records)
    assert result.deleted == 0
    assert any(record.why == "delete skipped because file disappeared before deletion" for record in skipped)


def test_delete_directories_removes_deepest_first_candidates(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    parent = tmp_path / "parent"
    child = parent / "child"
    child.mkdir(parents=True)

    result = module._delete_directories(
        [dir_candidate(module, parent), dir_candidate(module, child)],
        report_root=str(tmp_path),
        audit_limit=10,
    )

    assert result.deleted == 2
    assert not parent.exists()


def test_delete_directories_skips_non_empty_candidate(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "not-empty"
    target.mkdir()
    candidate = dir_candidate(module, target)
    (target / "keep.log").write_text("x", encoding="utf-8")

    result = module._delete_directories([candidate], report_root=str(tmp_path), audit_limit=10)

    skipped = rendered_records(result.skipped_records)
    assert result.deleted == 0
    assert target.exists()
    assert any(record.why == "delete skipped because directory was not empty at rmdir time" for record in skipped)
