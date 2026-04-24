from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def set_mtime(path: Path, *, age_days: int) -> None:
    """Set a file or directory mtime to now minus the requested age."""
    ts = time.time() - (age_days * 86400)
    os.utime(path, (ts, ts))


def force_same_device_stats(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Normalize st_dev for paths below one root so scan tests are deterministic."""
    original_stat = Path.stat
    root_device = original_stat(root, follow_symlinks=False).st_dev

    def patched_stat(self: Path, *, follow_symlinks: bool = True) -> Any:
        result = original_stat(self, follow_symlinks=follow_symlinks)
        if self == root or root in self.parents:
            return SimpleNamespace(
                st_mode=result.st_mode,
                st_ino=result.st_ino,
                st_dev=root_device,
                st_size=result.st_size,
                st_mtime_ns=result.st_mtime_ns,
                st_mtime=result.st_mtime,
            )
        return result

    monkeypatch.setattr(Path, "stat", patched_stat)


def test_module_smoke_imports_and_exposes_dag(import_log_clean: Any) -> None:
    module = import_log_clean

    assert module.DAG_ID == "log_clean"
    assert module.SCHEDULE == "@daily"
    assert module.dag is not None
    assert callable(module.execute_cleanup)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("TRUE", True),
        (" false ", False),
        ("1", False),
    ],
)
def test_as_bool_normalizes_common_inputs(import_log_clean: Any, raw_value: Any, expected: bool) -> None:
    module = import_log_clean

    assert module._as_bool(raw_value) is expected


@pytest.mark.parametrize("raw_value", [0, "0", 7, "12"])
def test_coerce_non_negative_int_accepts_valid_values(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    assert module._coerce_non_negative_int(raw_value, field_name="x") == int(raw_value)


@pytest.mark.parametrize("raw_value", [True, -1, "-3", "abc", None])
def test_coerce_non_negative_int_rejects_invalid_values(import_log_clean: Any, raw_value: Any) -> None:
    module = import_log_clean

    with pytest.raises(ValueError):
        module._coerce_non_negative_int(raw_value, field_name="x")


@pytest.mark.parametrize("path_value", ["relative/path", "/", "/tmp", "/var/log"])
def test_validate_cleanup_root_rejects_unsafe_locations(import_log_clean: Any, path_value: str) -> None:
    module = import_log_clean

    with pytest.raises(ValueError):
        module._validate_cleanup_root(path_value)


def test_validate_cleanup_root_accepts_specific_absolute_path(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    candidate = tmp_path / "nested" / "logs"
    candidate.mkdir(parents=True)

    assert module._validate_cleanup_root(str(candidate)) == str(candidate.resolve())


def test_parse_target_name_list_dedupes_and_filters_invalid_entries(import_log_clean: Any) -> None:
    module = import_log_clean

    valid, invalid = module._parse_target_name_list(
        " scheduler , scheduler, worker, ../escape, bad/name, ., \\, worker ",
        field_name="TARGET_DENY_LIST",
    )

    assert valid == ["scheduler", "worker"]
    assert invalid == [".", "../escape", "\\", "bad/name"]


def test_resolve_top_level_targets_tracks_included_and_excluded(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    (tmp_path / "worker").mkdir()
    (tmp_path / "triggerer").mkdir()

    included, excluded = module._resolve_top_level_targets(
        base_log_folder=str(tmp_path),
        target_deny_list=["worker"],
    )

    assert [item.label for item in included] == ["scheduler", "triggerer"]
    assert [item.label for item in excluded] == ["worker"]


def test_build_settings_parses_variables_and_runtime_flags(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    (tmp_path / "worker").mkdir()
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.TARGET_DENY_LIST_VARIABLE_KEY: "worker,bad/name",
        module.MAX_LOG_AGE_VARIABLE_KEY: "45",
    }

    settings = module._build_settings({"dry_run": "true"})

    assert settings.base_log_folder == str(tmp_path)
    assert settings.target_deny_list == ["worker"]
    assert settings.invalid_target_deny_list == ["bad/name"]
    assert settings.max_log_age_days == 45
    assert settings.dry_run is True
    assert settings.effective_delete_mode == "report-only"
    assert [item.label for item in settings.included_targets] == ["scheduler"]
    assert [item.label for item in settings.excluded_targets] == ["worker"]


def test_scan_cleanup_target_classifies_old_new_and_non_regular_files(
    import_log_clean: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    root = tmp_path / "scheduler"
    root.mkdir()

    old_file = root / "old.log"
    old_file.write_text("old", encoding="utf-8")
    set_mtime(old_file, age_days=5)

    new_file = root / "new.log"
    new_file.write_text("new", encoding="utf-8")
    set_mtime(new_file, age_days=1)

    symlink_path = root / "current.log"
    symlink_path.symlink_to(new_file)

    force_same_device_stats(monkeypatch, root)

    result = module._scan_cleanup_target(
        root,
        max_age_days=2,
        report_root=str(tmp_path),
    )

    summary = result.stats
    assert [candidate.path for candidate in result.old_files] == [str(old_file)]
    assert result.old_files[0].size_bytes == old_file.stat().st_size
    assert result.old_files[0].inode == old_file.stat().st_ino
    assert summary.directories_visited == 1
    assert summary.files_scanned_regular == 2
    assert summary.files_skipped_non_regular == 1
    assert summary.candidate_file_total_size_bytes == old_file.stat().st_size
    assert any(record.why == "entry is not a regular file" for record in result.excluded_records)

    age_records = [record for record in result.excluded_records if record.why == "regular file age is not above threshold 2d"]
    assert len(age_records) == 1
    assert age_records[0].real_path == "scheduler/new.log"
    assert age_records[0].detail.startswith("age_days=")
    assert "threshold_days=2" in age_records[0].detail


def test_collect_empty_directories_returns_deepest_first(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    root = tmp_path / "scheduler"
    (root / "keep" / "nested").mkdir(parents=True)
    (root / "remove" / "deep").mkdir(parents=True)
    (root / "keep" / "nested" / "data.log").write_text("x", encoding="utf-8")

    result = module._collect_empty_directories(
        root,
        report_root=str(root),
    )

    assert result.empty_directories == [
        str(root / "remove" / "deep"),
        str(root / "remove"),
    ]
    assert result.excluded_records == []


def test_collect_empty_directories_keeps_parent_when_subtree_contains_file(
    import_log_clean: Any,
    tmp_path: Path,
) -> None:
    module = import_log_clean
    root = tmp_path / "scheduler"
    (root / "parent" / "child" / "grandchild").mkdir(parents=True)
    (root / "parent" / "keep.log").write_text("x", encoding="utf-8")

    result = module._collect_empty_directories(
        root,
        report_root=str(root),
    )

    assert result.empty_directories == [
        str(root / "parent" / "child" / "grandchild"),
        str(root / "parent" / "child"),
    ]


def test_delete_files_removes_targets_and_reports_bytes(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "old.log"
    target.write_text("abcdef", encoding="utf-8")
    set_mtime(target, age_days=10)

    stat_result = target.stat(follow_symlinks=False)
    candidate = module.FileCandidate(
        path=str(target),
        device=int(stat_result.st_dev),
        inode=int(stat_result.st_ino),
        mtime=float(stat_result.st_mtime),
        size_bytes=int(stat_result.st_size),
    )

    result = module._delete_files([candidate])

    assert result.deleted == 1
    assert result.deleted_bytes == 6
    assert not target.exists()
    assert len(result.deleted_records) == 1


def test_delete_directories_removes_empty_targets(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    target = tmp_path / "empty"
    target.mkdir()

    result = module._delete_directories([str(target)])

    assert result.deleted == 1
    assert not target.exists()


def test_lock_helpers_are_exclusive(import_log_clean: Any, tmp_path: Path) -> None:
    module = import_log_clean
    lock_path = tmp_path / "cleanup.lock"

    assert module._try_create_lock(lock_path) is True
    assert module._try_create_lock(lock_path) is False
    module._remove_lock(lock_path)
    assert not lock_path.exists()


def test_root_scan_summary_rows_include_decision_metadata_when_skip_counters_exist(import_log_clean: Any) -> None:
    module = import_log_clean
    totals = module.RunTotals(
        roots_processed=1,
        directories_visited=2,
        directory_entries_seen=3,
        file_entries_seen=4,
        files_scanned_regular=5,
        regular_file_total_size_bytes=6,
        candidate_file_total_size_bytes=7,
        old_file_candidates=1,
        empty_dir_candidates=0,
        directories_skipped_inaccessible=2,
        files_skipped_non_regular=1,
    )

    rows = module._root_scan_summary_rows(totals)
    by_name = {row[0]: row for row in rows}

    assert by_name["directories_skipped_inaccessible"][2:] == ["skipped", "Directory metadata unreadable during traversal"]
    assert by_name["files_skipped_non_regular"][2:] == ["skipped", "Entry is not a regular file"]
    assert by_name["roots_processed"][2:] == ["", ""]


def test_execute_cleanup_smoke_dry_run_returns_completed_and_keeps_files(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    scheduler_root = tmp_path / "scheduler"
    denied_root = tmp_path / "worker"
    scheduler_root.mkdir()
    denied_root.mkdir()

    old_file = scheduler_root / "old.log"
    old_file.write_text("obsolete", encoding="utf-8")
    set_mtime(old_file, age_days=4)

    recent_file = scheduler_root / "recent.log"
    recent_file.write_text("fresh", encoding="utf-8")
    set_mtime(recent_file, age_days=1)

    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "2",
        module.TARGET_DENY_LIST_VARIABLE_KEY: "worker",
    }
    airflow_stub_modules["current_context"]["params"] = {"dry_run": True}

    logged_sections: list[tuple[str, str, Any]] = []

    force_same_device_stats(monkeypatch, scheduler_root)

    monkeypatch.setattr(module, "_log_section", lambda number, title, rows: logged_sections.append((number, title, rows)))
    monkeypatch.setattr(module, "_log_info_table", lambda number, title, headers, rows: logged_sections.append((number, title, rows)))
    monkeypatch.setattr(module, "_log_audit_list", lambda number, title, records: logged_sections.append((number, title, records)))

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["roots_processed"] == 1
    assert result["old_file_candidates"] == 1
    assert result["files_deleted"] == 0
    assert result["empty_dirs_deleted"] == 0
    assert old_file.exists()
    assert any(title == "Overall Outcome" for _, title, _ in logged_sections)


def test_execute_cleanup_returns_skipped_locked_when_lock_exists(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "2",
        module.TARGET_DENY_LIST_VARIABLE_KEY: "",
    }

    monkeypatch.setattr(module, "_try_create_lock", lambda _path: False)
    monkeypatch.setattr(module, "_log_section", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_info_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_audit_list", lambda *args, **kwargs: None)

    result = module.execute_cleanup()

    assert result["status"] == "skipped_locked"
    assert result["roots_processed"] == 0
    assert result["files_deleted"] == 0
