from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest

SECONDS_PER_DAY = 86_400


def set_mtime(path: Path, *, age_days: int) -> None:
    """Set a file or directory mtime to now minus the requested age."""
    timestamp = time.time() - (age_days * SECONDS_PER_DAY)
    os.utime(path, (timestamp, timestamp))


def silence_operator_logs(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable verbose table/audit rendering during execute_cleanup tests."""
    monkeypatch.setattr(module, "_log_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_section", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_audit_list", lambda *args, **kwargs: None)


def configure_cleanup_run(
    module: Any,
    airflow_stub_modules: dict[str, Any],
    base_log_folder: Path,
    lock_file: Path,
    *,
    dry_run: bool,
    max_age_days: int = 2,
    target_deny_list: str = "",
    delete_log_cap: str = "10",
) -> None:
    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(base_log_folder)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: str(max_age_days),
        module.TARGET_DENY_LIST_VARIABLE_KEY: target_deny_list,
        module.DELETE_LOG_CAP_VARIABLE_KEY: delete_log_cap,
    }
    airflow_stub_modules["current_context"].clear()
    airflow_stub_modules["current_context"].update({"params": {"dry_run": dry_run}})
    module.LOCK_FILE_PATH = str(lock_file)


def test_execute_cleanup_dry_run_keeps_files_and_reports_candidates(
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

    denied_old_file = denied_root / "denied.log"
    denied_old_file.write_text("keep", encoding="utf-8")
    set_mtime(denied_old_file, age_days=4)

    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "dry-run.lock",
        dry_run=True,
        max_age_days=2,
        target_deny_list="worker",
        delete_log_cap="0",
    )
    silence_operator_logs(module, monkeypatch)

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["roots_processed"] == 1
    assert result["old_file_candidates"] == 1
    assert result["files_deleted"] == 0
    assert result["empty_dirs_deleted"] == 0
    assert old_file.exists()
    assert recent_file.exists()
    assert denied_old_file.exists()


def test_execute_cleanup_delete_mode_deletes_old_files_and_empty_dirs(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    scheduler_root = tmp_path / "scheduler"
    denied_root = tmp_path / "worker"
    delete_after_scan_dir = scheduler_root / "ephemeral"
    scheduler_root.mkdir()
    denied_root.mkdir()
    delete_after_scan_dir.mkdir()

    old_file = scheduler_root / "old.log"
    old_file.write_text("obsolete", encoding="utf-8")
    set_mtime(old_file, age_days=5)

    nested_old_file = delete_after_scan_dir / "nested-old.log"
    nested_old_file.write_text("stale", encoding="utf-8")
    set_mtime(nested_old_file, age_days=5)

    recent_file = scheduler_root / "recent.log"
    recent_file.write_text("fresh", encoding="utf-8")
    set_mtime(recent_file, age_days=1)

    denied_old_file = denied_root / "denied.log"
    denied_old_file.write_text("keep", encoding="utf-8")
    set_mtime(denied_old_file, age_days=5)

    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "delete.lock",
        dry_run=False,
        max_age_days=2,
        target_deny_list="worker",
        delete_log_cap="5",
    )
    silence_operator_logs(module, monkeypatch)

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["roots_processed"] == 1
    assert result["old_file_candidates"] == 2
    assert result["files_deleted"] == 2
    assert result["empty_dirs_deleted"] == 1
    assert result["files_deleted_bytes"] == len("obsolete") + len("stale")
    assert not old_file.exists()
    assert not nested_old_file.exists()
    assert not delete_after_scan_dir.exists()
    assert recent_file.exists()
    assert denied_old_file.exists()


def test_execute_cleanup_report_only_when_delete_enabled_false(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    scheduler_root = tmp_path / "scheduler"
    scheduler_root.mkdir()
    old_file = scheduler_root / "old.log"
    old_file.write_text("obsolete", encoding="utf-8")
    set_mtime(old_file, age_days=5)

    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "report-only.lock",
        dry_run=False,
        max_age_days=2,
        delete_log_cap="0",
    )
    monkeypatch.setattr(module, "DELETE_ENABLED", False)
    silence_operator_logs(module, monkeypatch)

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["old_file_candidates"] == 1
    assert result["files_deleted"] == 0
    assert old_file.exists()


def test_execute_cleanup_returns_skipped_locked_when_lock_exists(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "locked.lock",
        dry_run=False,
        max_age_days=2,
        delete_log_cap="5",
    )
    monkeypatch.setattr(module, "_try_create_lock", lambda _path: False)
    silence_operator_logs(module, monkeypatch)

    result = module.execute_cleanup()

    assert result["status"] == "skipped_locked"
    assert result["roots_processed"] == 0
    assert result["files_deleted"] == 0


def test_execute_cleanup_raises_configuration_skip_with_all_invalid_values(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    (tmp_path / "scheduler").mkdir()
    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "invalid.lock",
        dry_run=False,
        max_age_days=0,
        delete_log_cap="0",
    )
    silence_operator_logs(module, monkeypatch)

    with pytest.raises(module.AirflowSkipException) as exc_info:
        module.execute_cleanup()

    message = str(exc_info.value)
    assert module.MAX_LOG_AGE_VARIABLE_KEY in message
    assert module.DELETE_LOG_CAP_VARIABLE_KEY in message


def test_execute_cleanup_skips_target_that_disappears_during_scan(
    import_log_clean: Any,
    airflow_stub_modules: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_log_clean
    scheduler_root = tmp_path / "scheduler"
    scheduler_root.mkdir()
    configure_cleanup_run(
        module,
        airflow_stub_modules,
        tmp_path,
        tmp_path / "disappearing.lock",
        dry_run=True,
        max_age_days=2,
    )
    silence_operator_logs(module, monkeypatch)

    original_scan = module._scan_cleanup_target

    def remove_then_scan(root: Path, *args: Any, **kwargs: Any) -> Any:
        root.rmdir()
        return original_scan(root, *args, **kwargs)

    monkeypatch.setattr(module, "_scan_cleanup_target", remove_then_scan)

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["roots_processed"] == 0
    assert result["files_deleted"] == 0
