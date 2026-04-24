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
    """Normalize st_dev for paths below one root so delete-mode tests are deterministic."""
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

    airflow_stub_modules["conf"].values[("logging", "base_log_folder")] = str(tmp_path)
    airflow_stub_modules["Variable"].values = {
        module.MAX_LOG_AGE_VARIABLE_KEY: "2",
        module.TARGET_DENY_LIST_VARIABLE_KEY: "worker",
    }
    airflow_stub_modules["current_context"]["params"] = {"dry_run": False}

    force_same_device_stats(monkeypatch, scheduler_root)

    monkeypatch.setattr(module, "_log_section", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_info_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_log_audit_list", lambda *args, **kwargs: None)

    result = module.execute_cleanup()

    assert result["status"] == "completed"
    assert result["roots_processed"] == 1
    assert result["old_file_candidates"] == 2
    assert result["files_deleted"] == 2
    assert result["empty_dirs_deleted"] == 1
    assert not old_file.exists()
    assert not nested_old_file.exists()
    assert not delete_after_scan_dir.exists()
    assert recent_file.exists()
    assert denied_old_file.exists()
    assert result["files_deleted_bytes"] == len("obsolete") + len("stale")
