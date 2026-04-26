# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Airflow log cleanup DAG. | Airflow 3.0+ compatible.

Purpose
-------
Clean old Airflow log files only inside the validated ``logging.base_log_folder``
while preserving strict filesystem safety and detailed audit visibility.

Target policy
-------------
- ``TARGET_DENY_LIST`` protects top-level directories below the validated base root.
- Every other discovered top-level directory below the validated base root remains in scope.

Retention policy
----------------
Only regular files with age strictly greater than ``MAX_LOG_AGE_DAYS`` are
candidates for deletion. Empty directories inside included targets are removed
only after file evaluation, including the included target root when it becomes
empty.
"""

from __future__ import annotations

import errno
import logging
import os
import stat
import textwrap
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, Variable, get_current_context, task

LOGGER = logging.getLogger(__name__)

__version__ = "2.10"

DAG_ID = "airflow_log_cleanup"
START_DATE = pendulum.datetime(2024, 1, 1, tz="Europe/Prague")
SCHEDULE = "@daily"
DAG_OWNER_NAME = "operations"
ALERT_EMAIL_ADDRESSES: list[str] = []

DELETE_ENABLED = True
LOCK_FILE_PATH = "/tmp/airflow_log_cleanup.lock"
MAX_LOG_AGE_VARIABLE_KEY = "MAX_LOG_AGE_DAYS"
TARGET_DENY_LIST_VARIABLE_KEY = "TARGET_DENY_LIST"

TAGS = [
    f"ver: {__version__}",
    "airflow-log-cleanup",
    "airflow-log-retention",
    "airflow-maintenance",
    "airflow-operations",
    "filesystem-cleanup",
    "retention-policy",
]


SECTION_WIDTHS: dict[str, list[int]] = {
    "00": [30, 50],
    "01": [10, 44, 22, 36, 72],
    "02": [10, 44, 22, 36, 72],
    "04": [10, 18, 34, 110],
    "05": [36, 18, 12, 55],
    "06": [40, 26],
    "07": [40, 26],
    "99": [24, 100],
}

BROAD_ROOTS = {
    "/",
    "/bin",
    "/boot",
    "/dev",
    "/etc",
    "/home",
    "/lib",
    "/lib64",
    "/opt",
    "/opt/airflow",
    "/proc",
    "/root",
    "/run",
    "/sbin",
    "/srv",
    "/sys",
    "/tmp",
    "/usr",
    "/var",
    "/var/log",
}


@dataclass(frozen=True)
class TargetSpec:
    """Resolved top-level target under the validated cleanup root.

    Only real top-level directories may be included for cleanup traversal.
    Symlinks and other non-directory entries are retained as excluded targets
    for audit output, but they are never traversed.
    """

    label: str
    path: str
    reason: str
    item_type: str = "directory"


@dataclass(frozen=True)
class CleanupSettings:
    """Runtime configuration resolved from Airflow config, variables, and params."""

    base_log_folder: str
    target_deny_list: list[str]
    invalid_target_deny_list: list[str]
    included_targets: list[TargetSpec]
    excluded_targets: list[TargetSpec]
    max_log_age_days: int
    dry_run: bool
    effective_delete_mode: str
    delete_enabled: bool
    lock_file_path: str


@dataclass(frozen=True)
class AuditRecord:
    """One operator-facing audit line item.

    ``why`` is intentionally stable so audit records group cleanly.
    Variable per-record values belong in ``detail``.
    """

    item_type: str
    real_path: str
    why: str
    observed_epoch: float = 0.0
    detail: str = ""


@dataclass(frozen=True)
class SummaryMetric:
    """Root summary row definition with optional decision text."""

    summary_item: str
    attr_name: str
    decision: str = ""
    evaluation_method: str = ""
    human_bytes: bool = False


SUMMARY_METRICS: tuple[SummaryMetric, ...] = (
    SummaryMetric("roots_processed", "roots_processed"),
    SummaryMetric("directories_visited", "directories_visited"),
    SummaryMetric("directory_entries_seen", "directory_entries_seen"),
    SummaryMetric("file_entries_seen", "file_entries_seen"),
    SummaryMetric("files_scanned_regular", "files_scanned_regular"),
    SummaryMetric("regular_file_total_size", "regular_file_total_size_bytes", human_bytes=True),
    SummaryMetric("old_file_candidates", "old_file_candidates"),
    SummaryMetric(
        "old_file_candidate_total_size",
        "candidate_file_total_size_bytes",
        human_bytes=True,
    ),
    SummaryMetric("empty_dir_candidates", "empty_dir_candidates"),
    SummaryMetric(
        "directories_skipped_inaccessible",
        "directories_skipped_inaccessible",
        "skipped",
        "Directory metadata unreadable during traversal",
    ),
    SummaryMetric(
        "directories_skipped_not_directory",
        "directories_skipped_not_directory",
        "skipped",
        "Entry is not a traversable directory",
    ),
    SummaryMetric(
        "directories_skipped_mount_boundary",
        "directories_skipped_mount_boundary",
        "skipped",
        "Directory is on a different filesystem",
    ),
    SummaryMetric(
        "files_skipped_inaccessible",
        "files_skipped_inaccessible",
        "skipped",
        "File metadata unreadable during evaluation",
    ),
    SummaryMetric(
        "files_skipped_cross_device",
        "files_skipped_cross_device",
        "skipped",
        "File is on a different filesystem",
    ),
    SummaryMetric(
        "files_skipped_non_regular",
        "files_skipped_non_regular",
        "skipped",
        "Entry is not a regular file",
    ),
)


@dataclass
class ScanStats:
    """Counters accumulated while scanning one included target."""

    directories_visited: int = 0
    directory_entries_seen: int = 0
    file_entries_seen: int = 0
    files_scanned_regular: int = 0
    regular_file_total_size_bytes: int = 0
    candidate_file_total_size_bytes: int = 0
    directories_skipped_inaccessible: int = 0
    directories_skipped_not_directory: int = 0
    directories_skipped_mount_boundary: int = 0
    files_skipped_inaccessible: int = 0
    files_skipped_cross_device: int = 0
    files_skipped_non_regular: int = 0


@dataclass
class RunTotals:
    """Aggregated counters across all included targets in one DAG run."""

    roots_processed: int = 0
    directories_visited: int = 0
    directory_entries_seen: int = 0
    file_entries_seen: int = 0
    files_scanned_regular: int = 0
    regular_file_total_size_bytes: int = 0
    candidate_file_total_size_bytes: int = 0
    old_file_candidates: int = 0
    empty_dir_candidates: int = 0
    files_deleted: int = 0
    files_deleted_bytes: int = 0
    empty_dirs_deleted: int = 0
    directories_skipped_inaccessible: int = 0
    directories_skipped_not_directory: int = 0
    directories_skipped_mount_boundary: int = 0
    files_skipped_inaccessible: int = 0
    files_skipped_cross_device: int = 0
    files_skipped_non_regular: int = 0
    duration_seconds: float = 0.0

    def add_scan(self, stats: ScanStats, *, old_file_candidates: int, empty_dir_candidates: int) -> None:
        for attr in (
            "directories_visited",
            "directory_entries_seen",
            "file_entries_seen",
            "files_scanned_regular",
            "regular_file_total_size_bytes",
            "candidate_file_total_size_bytes",
            "directories_skipped_inaccessible",
            "directories_skipped_not_directory",
            "directories_skipped_mount_boundary",
            "files_skipped_inaccessible",
            "files_skipped_cross_device",
            "files_skipped_non_regular",
        ):
            setattr(self, attr, getattr(self, attr) + getattr(stats, attr))
        self.roots_processed += 1
        self.old_file_candidates += old_file_candidates
        self.empty_dir_candidates += empty_dir_candidates

    def add_action(self, *, files_deleted: int, files_deleted_bytes: int, empty_dirs_deleted: int) -> None:
        self.files_deleted += files_deleted
        self.files_deleted_bytes += files_deleted_bytes
        self.empty_dirs_deleted += empty_dirs_deleted


@dataclass(frozen=True)
class FileDeleteResult:
    """Result of regular file deletion.

    ``skipped_records`` contains delete-phase safety skips. These are different
    from scan-phase exclusions because the path was a valid old-file candidate
    during scanning but became unsafe, unavailable, or inconsistent before the
    delete operation completed.
    """

    deleted: int = 0
    deleted_bytes: int = 0
    deleted_records: tuple[tuple[str, float], ...] = ()
    skipped_records: tuple[AuditRecord, ...] = ()


@dataclass(frozen=True)
class DirDeleteResult:
    """Result of empty-directory deletion.

    ``skipped_records`` contains delete-phase directory skips. These explain why
    a directory candidate was not removed after the empty-directory collection
    phase selected it.
    """

    deleted: int = 0
    deleted_records: tuple[tuple[str, float], ...] = ()
    skipped_records: tuple[AuditRecord, ...] = ()


@dataclass(frozen=True)
class FileCandidate:
    """Regular file selected during scan with identity metadata.

    The deletion phase revalidates these fields before unlinking the path. This
    prevents deleting a different filesystem object if the path was replaced
    between scan and delete.
    """

    path: str
    device: int
    inode: int
    mtime: float
    size_bytes: int


@dataclass(frozen=True)
class EmptyDirCollectResult:
    """Empty-directory collection result and related audit artifacts."""

    empty_directories: list[str] = field(default_factory=list)
    excluded_records: list[AuditRecord] = field(default_factory=list)


@dataclass(frozen=True)
class ScanResult:
    """Scan result for one included target."""

    stats: ScanStats
    old_files: list[FileCandidate]
    excluded_records: list[AuditRecord]


def _as_bool(value: Any) -> bool:
    return value if isinstance(value, bool) else str(value).strip().lower() == "true"


def _coerce_positive_int(value: Any, *, field_name: str) -> int:
    """Coerce an Airflow Variable value into a strictly positive integer.

    Retention set to zero is intentionally rejected by skipping the task because
    it would make nearly all existing regular files eligible for cleanup.
    Negative and malformed values remain hard configuration errors.
    """

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer, got bool.")

    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer, got {value!r}.") from exc

    if parsed == 0:
        message = f"{field_name}=0 is unsafe for log cleanup. Task processing skipped because retention must be greater than 0."
        LOGGER.error("%s", message)
        raise AirflowSkipException(message)

    if parsed < 0:
        raise ValueError(f"{field_name} must be > 0, got {parsed}.")

    return parsed


def _human_bytes(num_bytes: int) -> str:
    if num_bytes <= 0:
        return "0 B"
    value, units, index = float(num_bytes), ["B", "KiB", "MiB", "GiB", "TiB", "PiB"], 0
    while value >= 1024.0 and index < len(units) - 1:
        value /= 1024.0
        index += 1
    return f"{int(value)} {units[index]}" if index == 0 else f"{value:.2f} {units[index]}"


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value)
    return str(value)


def _wrap_cell(value: Any, width: int) -> list[str]:
    lines: list[str] = []
    for paragraph in _normalize_text(value).replace("\t", "    ").splitlines() or [""]:
        lines.extend(
            textwrap.wrap(
                paragraph,
                width=width,
                break_long_words=True,
                break_on_hyphens=False,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            or [""]
        )
    return lines or [""]


def _section_title(number: str, title: str) -> str:
    return f"{number} :: {title}"


def _section_widths(number: str, headers: list[str]) -> list[int]:
    return SECTION_WIDTHS.get(number, [max(5, len(header)) for header in headers])


def _render_table(headers: list[str], rows: list[list[Any]], widths: list[int]) -> str:
    def border() -> str:
        return "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    def render_row(values: list[Any]) -> list[str]:
        padded = list(values) + [""] * (len(headers) - len(values))
        columns = [_wrap_cell(padded[index], widths[index]) for index in range(len(headers))]
        height = max(len(column) for column in columns)
        return ["| " + " | ".join((column[row_index] if row_index < len(column) else "").ljust(widths[index]) for index, column in enumerate(columns)) + " |" for row_index in range(height)]

    rendered = [border(), *render_row(headers), border()]
    for row in rows or [["<none>"] + [""] * (len(headers) - 1)]:
        rendered.extend(render_row(row))
    rendered.append(border())
    return "\n".join(rendered)


def _log_block(level: str, number: str, title: str, body: str) -> None:
    (LOGGER.warning if level == "warning" else LOGGER.info)("%s", f"{_section_title(number, title)}\n{body}\n\n")


def _log_table(level: str, number: str, title: str, headers: list[str], rows: list[list[Any]]) -> None:
    _log_block(level, number, title, _render_table(headers, rows, _section_widths(number, headers)))


def _log_info_table(number: str, title: str, headers: list[str], rows: list[list[Any]]) -> None:
    _log_table("info", number, title, headers, rows)


def _log_warning_table(number: str, title: str, headers: list[str], rows: list[list[Any]]) -> None:
    _log_table("warning", number, title, headers, rows)


def _log_section(number: str, title: str, rows: list[tuple[str, Any]]) -> None:
    _log_info_table(number, title, ["Field", "Value"], [[key, value] for key, value in rows])


def _validate_cleanup_root(path_value: str) -> str:
    path = Path(path_value).expanduser()
    resolved = str(path.resolve(strict=False))
    if not path.is_absolute():
        raise ValueError(f"Cleanup root must be absolute: {path_value!r}")
    if resolved in BROAD_ROOTS:
        raise ValueError(f"Refusing unsafe cleanup root {resolved!r}; path is too broad.")
    if len(Path(resolved).parts) < 3:
        raise ValueError(f"Refusing unsafe cleanup root {resolved!r}; path is not specific enough.")
    return resolved.rstrip("/")


def _read_logging_path(section_key: str, *, required: bool) -> str | None:
    value = conf.get("logging", section_key, fallback=None)
    if value is None or not value.strip():
        if required:
            raise ValueError(f"logging.{section_key} is empty in airflow.cfg. Provide a valid absolute directory path.")
        return None
    return _validate_cleanup_root(value)


def _is_valid_target_name(name: str) -> bool:
    return bool(name) and "/" not in name and "\\" not in name and name not in {".", ".."} and Path(name).name == name


def _parse_target_name_list(raw_value: str | None, *, field_name: str) -> tuple[list[str], list[str]]:
    valid, invalid = set(), []
    for item in [] if raw_value is None else str(raw_value).split(","):
        candidate = item.strip()
        if not candidate:
            continue
        if _is_valid_target_name(candidate):
            valid.add(candidate)
        else:
            invalid.append(candidate)
    if invalid:
        LOGGER.warning("Invalid target names ignored for %s: %s", field_name, ", ".join(invalid))
    return sorted(valid), sorted(invalid)


def _sort_directories_deepest_first(paths: list[str]) -> list[str]:
    return sorted(
        paths,
        key=lambda item: (
            -len(Path(item).parts),
            -len(item),
            Path(item).name.casefold(),
            Path(item).name,
            item,
        ),
    )


def _sort_file_candidates_shortest_first(candidates: list[FileCandidate]) -> list[FileCandidate]:
    """Sort file candidates by path length and path for stable reporting/deletion."""

    return sorted(candidates, key=lambda item: (len(item.path), item.path))


def _path_identity(path: str | Path) -> str:
    """Return a stable absolute path key without resolving symlinks."""

    return os.path.abspath(os.fspath(path))


def _relative_path(path_str: str, cleanup_root: str) -> str:
    """Render a path relative to the cleanup root for audit output.

    The function first tries a resolved path comparison to normalize filesystem
    representation. If resolution fails because of OS/path issues, it falls back
    to a non-resolved relative comparison.

    If the path cannot be represented below ``cleanup_root``, the original path
    string is returned unchanged. This keeps audit rendering non-fatal while
    avoiding broad exception handling.
    """

    path = Path(path_str)
    root = Path(cleanup_root)

    try:
        return str(path.resolve(strict=False).relative_to(root))
    except (OSError, RuntimeError, ValueError):
        try:
            return str(path.relative_to(root))
        except ValueError:
            return path_str


def _append_audit_record(
    records: list[AuditRecord],
    *,
    cleanup_root: str,
    path: str,
    item_type: str,
    why: str,
    observed_epoch: float = 0.0,
    detail: str = "",
) -> None:
    records.append(
        AuditRecord(
            item_type=item_type,
            real_path=_relative_path(path, cleanup_root),
            why=why,
            observed_epoch=observed_epoch,
            detail=detail,
        )
    )


def _dedupe_audit_records(records: list[AuditRecord]) -> list[AuditRecord]:
    seen: set[tuple[str, str, str, float, str]] = set()
    deduped: list[AuditRecord] = []

    for record in records:
        key = (
            record.item_type,
            record.real_path,
            record.why,
            record.observed_epoch,
            record.detail,
        )
        if key not in seen:
            seen.add(key)
            deduped.append(record)

    return deduped


def _audit_item_type_order(item_type: str) -> int:
    """Return stable audit item type ordering inside one reason group."""

    order = {
        "file": 10,
        "directory": 20,
        "entry": 30,
        "symlink": 40,
    }
    return order.get(item_type, 99)


def _audit_detail_float(record: AuditRecord, key_name: str) -> float:
    """Extract a float value from AuditRecord.detail.

    Expected detail format:
        age_days=0.38; threshold_days=1

    Missing or invalid values are sorted last.
    """

    for part in record.detail.split(";"):
        key, separator, value = part.strip().partition("=")
        if key == key_name and separator:
            try:
                return float(value)
            except ValueError:
                return float("inf")

    return float("inf")


def _audit_record_sort_key(record: AuditRecord) -> tuple[int, float, float, int, int, str, str]:
    """Return deterministic sort key for audit records inside one reason group.

    Regular age-threshold file records sort by item type, then ``age_days``
    descending, so the oldest files are shown first. Other records sort by item
    type, observed timestamp, path depth, path length, path, and detail.

    Empty-directory cleanup records are sorted by
    ``_group_audit_records_by_reason()`` because that ordering is reason-specific
    and must apply to both dry-run candidates and real delete records.
    """

    return (
        _audit_item_type_order(record.item_type),
        -_audit_detail_float(record, "age_days"),
        record.observed_epoch,
        len(Path(record.real_path).parts),
        len(record.real_path),
        record.real_path,
        record.detail,
    )


def _group_audit_records_by_reason(records: list[AuditRecord]) -> list[tuple[str, list[AuditRecord]]]:
    """Group audit records by reason and sort each group deterministically.

    Age-based regular-file records are ordered from oldest to youngest.

    Empty-directory cleanup records are ordered by deepest path first, then
    longest path, then directory basename, so leaf directories are displayed
    before parent directories with deterministic name ordering.
    """

    grouped: dict[str, list[AuditRecord]] = defaultdict(list)

    for record in _dedupe_audit_records(records):
        grouped[record.why].append(record)

    result: list[tuple[str, list[AuditRecord]]] = []

    for reason in sorted(grouped):
        grouped_records = grouped[reason]

        if reason in {
            "would delete because directory would be empty during cleanup phase",
            "deleted because directory was empty during cleanup phase",
        }:
            sorted_records = sorted(
                grouped_records,
                key=lambda record: (
                    -len(Path(record.real_path).parts),
                    -len(record.real_path),
                    Path(record.real_path).name.casefold(),
                    Path(record.real_path).name,
                    record.real_path,
                    record.detail,
                ),
            )
        else:
            sorted_records = sorted(grouped_records, key=_audit_record_sort_key)

        result.append((reason, sorted_records))

    return result


def _log_audit_list(number: str, title: str, records: list[AuditRecord]) -> None:
    """Log audit records grouped by reason with one blank line between groups.

    Audit age is rendered once from ``observed_epoch`` as ``observed_age_days``.
    Duplicate ``age_days`` values stored in technical detail are suppressed at
    render time while preserving other useful detail fields, such as
    ``threshold_days``.
    """

    grouped = _group_audit_records_by_reason(records)

    if not grouped:
        LOGGER.info("%s\n- none\n\n", _section_title(number, title))
        return

    if title in {"Deleted Items", "Candidate Items"}:
        grouped = sorted(
            grouped,
            key=lambda item: (
                0 if item[1] and item[1][0].item_type == "file" else 1,
                item[0],
            ),
        )

    lines: list[str] = []
    title_prefix = title.lower()
    now_epoch = time.time()

    for group_index, (reason, grouped_records) in enumerate(grouped, start=1):
        lines.append(f"- {title_prefix} reason: {reason}")

        for index, record in enumerate(grouped_records, start=1):
            suffix_parts: list[str] = []

            if record.observed_epoch > 0:
                observed_age_days = max(0.0, now_epoch - record.observed_epoch) / 86_400
                suffix_parts.append(f"observed_age_days={observed_age_days:.2f}")

            if record.detail:
                retained_detail_parts: list[str] = []

                for detail_part in record.detail.split(";"):
                    normalized_detail_part = detail_part.strip()

                    if not normalized_detail_part:
                        continue

                    detail_key = normalized_detail_part.partition("=")[0].strip()

                    if detail_key in {"age_days", "size_bytes"}:
                        continue

                    retained_detail_parts.append(normalized_detail_part)

                if retained_detail_parts:
                    suffix_parts.append(f"detail={'; '.join(retained_detail_parts)}")

            suffix = f" | {' | '.join(suffix_parts)}" if suffix_parts else ""
            lines.append(f"  {index}. [{record.item_type}] {record.real_path}{suffix}")

        if group_index < len(grouped):
            lines.append(" ")

    LOGGER.info("%s\n%s\n\n", _section_title(number, title), "\n".join(lines))


def _resolve_top_level_targets(
    *,
    base_log_folder: str,
    target_deny_list: list[str],
) -> tuple[list[TargetSpec], list[TargetSpec]]:
    """Resolve safe cleanup targets directly below the validated log root.

    The function intentionally does not use ``Path.is_dir()`` for child entries
    because that call follows symlinks. A top-level symlink under the Airflow log
    root could otherwise be accepted as a cleanup root and point outside the
    validated filesystem boundary.

    Only entries whose own inode is a real directory are included. Symlinks,
    regular files, sockets, FIFOs, devices, unreadable entries, and any other
    non-directory entries are retained as excluded targets for audit output.
    """

    root = Path(base_log_folder)

    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise ValueError(f"Validated base log folder metadata is unreadable: {base_log_folder}: {exc}") from exc

    if not stat.S_ISDIR(root_stat.st_mode):
        raise ValueError(f"Validated base log folder is not a real directory: {base_log_folder}")

    included: list[TargetSpec] = []
    excluded: list[TargetSpec] = []

    try:
        children = sorted(root.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise ValueError(f"Validated base log folder cannot be listed: {base_log_folder}: {exc}") from exc

    for child in children:
        label = child.name
        child_path = str(child)

        try:
            child_stat = child.stat(follow_symlinks=False)
        except OSError:
            excluded.append(
                TargetSpec(
                    label=label,
                    path=child_path,
                    reason="Excluded because top-level entry metadata is unreadable",
                    item_type="entry",
                )
            )
            continue

        if stat.S_ISLNK(child_stat.st_mode):
            excluded.append(
                TargetSpec(
                    label=label,
                    path=child_path,
                    reason="Excluded because top-level entry is a symlink; symlink targets are never traversed",
                    item_type="symlink",
                )
            )
            continue

        if not stat.S_ISDIR(child_stat.st_mode):
            excluded.append(
                TargetSpec(
                    label=label,
                    path=child_path,
                    reason="Excluded because top-level entry is not a real directory",
                    item_type="entry",
                )
            )
            continue

        if label in target_deny_list:
            excluded.append(
                TargetSpec(
                    label=label,
                    path=child_path,
                    reason="Excluded by TARGET_DENY_LIST",
                    item_type="directory",
                )
            )
            continue

        included.append(
            TargetSpec(
                label=label,
                path=child_path,
                reason="Included by validated base root scope with its subfolders",
                item_type="directory",
            )
        )

    def sort_key(item: TargetSpec) -> tuple[int, str, str]:
        return (len(item.path), item.path, item.label)

    return sorted(included, key=sort_key), sorted(excluded, key=sort_key)


def _build_settings(params: dict[str, Any]) -> CleanupSettings:
    dry_run = _as_bool(params.get("dry_run", False))
    base_log_folder = _read_logging_path("base_log_folder", required=True)
    assert base_log_folder is not None

    deny_list, invalid_deny_list = _parse_target_name_list(
        Variable.get(TARGET_DENY_LIST_VARIABLE_KEY, default=""),
        field_name=TARGET_DENY_LIST_VARIABLE_KEY,
    )
    max_log_age_days = _coerce_positive_int(
        Variable.get(MAX_LOG_AGE_VARIABLE_KEY, default="30"),
        field_name=MAX_LOG_AGE_VARIABLE_KEY,
    )
    included, excluded = _resolve_top_level_targets(
        base_log_folder=base_log_folder,
        target_deny_list=deny_list,
    )
    return CleanupSettings(
        base_log_folder=base_log_folder,
        target_deny_list=deny_list,
        invalid_target_deny_list=invalid_deny_list,
        included_targets=included,
        excluded_targets=excluded,
        max_log_age_days=max_log_age_days,
        dry_run=dry_run,
        effective_delete_mode="report-only" if dry_run or not DELETE_ENABLED else "delete",
        delete_enabled=DELETE_ENABLED,
        lock_file_path=LOCK_FILE_PATH,
    )


def _scan_cleanup_target(
    root: Path,
    *,
    max_age_days: int,
    report_root: str,
) -> ScanResult:
    """Scan one cleanup target for old regular files.

    The scan is intentionally constrained to one real filesystem device and does
    not follow symlink directories. Unreadable directories, non-directory
    traversal entries, cross-device directories, unreadable files, cross-device
    files, and non-regular files are skipped and audited.
    """

    now_ts = time.time()
    retention_threshold_seconds = max_age_days * 86_400

    old_files: list[FileCandidate] = []
    excluded_records: list[AuditRecord] = []
    stats = ScanStats()

    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"Failed reading target root metadata for {root}: {exc}") from exc

    if not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"Target root is not a real directory: {root}")

    root_device = root_stat.st_dev

    def walk_error(exc: OSError) -> None:
        error_path = str(exc.filename) if getattr(exc, "filename", None) else str(root)
        stats.directories_skipped_inaccessible += 1
        _append_audit_record(
            excluded_records,
            cleanup_root=report_root,
            path=error_path,
            item_type="directory",
            why="directory metadata unreadable during traversal",
        )

    for current_str, dirnames, filenames in os.walk(
        root,
        topdown=True,
        followlinks=False,
        onerror=walk_error,
    ):
        current = Path(current_str)
        stats.directories_visited += 1
        stats.directory_entries_seen += len(dirnames)
        stats.file_entries_seen += len(filenames)

        allowed_dirnames: list[str] = []

        for dirname in dirnames:
            candidate = current / dirname

            try:
                candidate_stat = candidate.stat(follow_symlinks=False)
            except OSError:
                stats.directories_skipped_inaccessible += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory metadata unreadable during traversal",
                )
                continue

            if not stat.S_ISDIR(candidate_stat.st_mode):
                stats.directories_skipped_not_directory += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a traversable directory",
                )
                continue

            if candidate_stat.st_dev != root_device:
                stats.directories_skipped_mount_boundary += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory is on a different filesystem",
                )
                continue

            allowed_dirnames.append(dirname)

        dirnames[:] = allowed_dirnames

        for filename in filenames:
            candidate = current / filename

            try:
                candidate_stat = candidate.stat(follow_symlinks=False)
            except OSError:
                stats.files_skipped_inaccessible += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why="file metadata unreadable",
                )
                continue

            if candidate_stat.st_dev != root_device:
                stats.files_skipped_cross_device += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why="file is on a different filesystem",
                )
                continue

            if not stat.S_ISREG(candidate_stat.st_mode):
                stats.files_skipped_non_regular += 1
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a regular file",
                )
                continue

            size_bytes = int(candidate_stat.st_size)
            file_age_seconds = max(0.0, now_ts - float(candidate_stat.st_mtime))
            age_days_display = file_age_seconds / 86_400

            stats.files_scanned_regular += 1
            stats.regular_file_total_size_bytes += size_bytes

            if file_age_seconds <= retention_threshold_seconds:
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why=f"regular file age is not above threshold {max_age_days}d",
                    observed_epoch=float(candidate_stat.st_mtime),
                    detail=f"age_days={age_days_display:.2f}; threshold_days={max_age_days}",
                )
                continue

            old_files.append(
                FileCandidate(
                    path=str(candidate),
                    device=int(candidate_stat.st_dev),
                    inode=int(candidate_stat.st_ino),
                    mtime=float(candidate_stat.st_mtime),
                    size_bytes=size_bytes,
                )
            )
            stats.candidate_file_total_size_bytes += size_bytes

    return ScanResult(
        stats=stats,
        old_files=_sort_file_candidates_shortest_first(old_files),
        excluded_records=excluded_records,
    )


def _collect_empty_directories(
    root: Path,
    *,
    report_root: str,
    ignored_regular_files: set[str] | None = None,
) -> EmptyDirCollectResult:
    """Collect directories that are empty or would become empty.

    ``ignored_regular_files`` is used for dry-run parity. It represents regular
    files already selected as old-file deletion candidates. During dry-run, those
    files are treated as logically absent so the empty-directory candidate list
    matches what delete mode would discover after file deletion.

    Any directory subtree that cannot be traversed is treated as blocking. This
    prevents a parent directory from being classified as removable when one of
    its child directories was unreadable or skipped by ``os.walk`` error
    handling.
    """

    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"Failed reading target root metadata for {root}: {exc}") from exc

    if not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"Target root is not a real directory: {root}")

    root_device = root_stat.st_dev
    ignored_file_keys = {_path_identity(path) for path in ignored_regular_files or set()}

    excluded_records: list[AuditRecord] = []
    discovered_dirs: list[Path] = []
    blocked_directory_keys: set[str] = set()

    def walk_error(exc: OSError) -> None:
        error_path = str(exc.filename) if getattr(exc, "filename", None) else str(root)
        blocked_directory_keys.add(_path_identity(error_path))
        _append_audit_record(
            excluded_records,
            cleanup_root=report_root,
            path=error_path,
            item_type="directory",
            why="directory metadata unreadable during empty-directory traversal",
        )

    for current_str, dirnames, _ in os.walk(
        root,
        topdown=True,
        followlinks=False,
        onerror=walk_error,
    ):
        current = Path(current_str)
        discovered_dirs.append(current)
        allowed_dirnames: list[str] = []

        for dirname in dirnames:
            candidate = current / dirname

            try:
                candidate_stat = candidate.stat(follow_symlinks=False)
            except OSError:
                blocked_directory_keys.add(_path_identity(candidate))
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory metadata unreadable during empty-directory traversal",
                )
                continue

            if not stat.S_ISDIR(candidate_stat.st_mode):
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a traversable directory during empty-directory traversal",
                )
                continue

            if candidate_stat.st_dev != root_device:
                blocked_directory_keys.add(_path_identity(candidate))
                _append_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory is on a different filesystem during empty-directory traversal",
                )
                continue

            allowed_dirnames.append(dirname)

        dirnames[:] = allowed_dirnames

    removable_dirs: list[str] = []
    subtree_has_blocking_entry: dict[Path, bool] = {}

    for directory in sorted(
        set(discovered_dirs),
        key=lambda item: len(item.relative_to(root).parts),
        reverse=True,
    ):
        directory_key = _path_identity(directory)

        if directory_key in blocked_directory_keys:
            subtree_has_blocking_entry[directory] = True
            continue

        try:
            directory_stat = directory.stat(follow_symlinks=False)
        except OSError:
            blocked_directory_keys.add(directory_key)
            _append_audit_record(
                excluded_records,
                cleanup_root=report_root,
                path=str(directory),
                item_type="directory",
                why="directory metadata unreadable during empty-directory evaluation",
            )
            subtree_has_blocking_entry[directory] = True
            continue

        if not stat.S_ISDIR(directory_stat.st_mode):
            _append_audit_record(
                excluded_records,
                cleanup_root=report_root,
                path=str(directory),
                item_type="entry",
                why="entry is not a directory during empty-directory evaluation",
            )
            subtree_has_blocking_entry[directory] = True
            continue

        if directory_stat.st_dev != root_device:
            blocked_directory_keys.add(directory_key)
            _append_audit_record(
                excluded_records,
                cleanup_root=report_root,
                path=str(directory),
                item_type="directory",
                why="directory is on a different filesystem during empty-directory evaluation",
            )
            subtree_has_blocking_entry[directory] = True
            continue

        has_blocking_entry = False

        try:
            for child in directory.iterdir():
                child_key = _path_identity(child)

                try:
                    child_stat = child.stat(follow_symlinks=False)
                except OSError:
                    blocked_directory_keys.add(child_key)
                    _append_audit_record(
                        excluded_records,
                        cleanup_root=report_root,
                        path=str(child),
                        item_type="entry",
                        why="directory contents unreadable during empty-directory evaluation",
                    )
                    has_blocking_entry = True
                    continue

                if stat.S_ISDIR(child_stat.st_mode):
                    if child_stat.st_dev != root_device or subtree_has_blocking_entry.get(child, False) or child_key in blocked_directory_keys:
                        has_blocking_entry = True
                    continue

                if child_key in ignored_file_keys:
                    continue

                has_blocking_entry = True

        except OSError:
            blocked_directory_keys.add(directory_key)
            _append_audit_record(
                excluded_records,
                cleanup_root=report_root,
                path=str(directory),
                item_type="directory",
                why="directory contents unreadable during empty-directory evaluation",
            )
            has_blocking_entry = True

        subtree_has_blocking_entry[directory] = has_blocking_entry

        if not has_blocking_entry:
            removable_dirs.append(str(directory))

    return EmptyDirCollectResult(
        empty_directories=_sort_directories_deepest_first(removable_dirs),
        excluded_records=excluded_records,
    )


def _delete_files(candidates: list[FileCandidate], *, report_root: str) -> FileDeleteResult:
    """Delete scanned regular file candidates after identity revalidation.

    A path is deleted only when the current filesystem object still matches the
    scan-time regular file identity. If the path disappeared, became unreadable,
    became non-regular, or was replaced by another inode, it is skipped and
    captured as a structured delete-phase audit record.

    This deliberately treats delete-time inconsistencies as skip conditions
    instead of crashing the DAG run. The cleanup task should be conservative:
    uncertain paths are kept.
    """

    deleted = 0
    deleted_bytes = 0
    deleted_records: list[tuple[str, float]] = []
    skipped_records: list[AuditRecord] = []

    for candidate in candidates:
        path = Path(candidate.path)

        try:
            current_stat = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file disappeared before deletion",
                observed_epoch=candidate.mtime,
                detail=f"scan_size_bytes={candidate.size_bytes}",
            )
            continue
        except OSError as exc:
            LOGGER.warning(
                "Skipping file deletion candidate with unreadable metadata: %s: %s",
                path,
                exc,
            )
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file metadata became unreadable",
                observed_epoch=candidate.mtime,
                detail=f"errno={getattr(exc, 'errno', '')}; error={exc}",
            )
            continue

        if not stat.S_ISREG(current_stat.st_mode):
            LOGGER.warning(
                "Skipping file deletion candidate because it is not a regular file: %s",
                path,
            )
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="entry",
                why="delete skipped because candidate is no longer a regular file",
                observed_epoch=float(current_stat.st_mtime),
                detail=f"mode={oct(stat.S_IMODE(current_stat.st_mode))}",
            )
            continue

        current_identity = (
            int(current_stat.st_dev),
            int(current_stat.st_ino),
            float(current_stat.st_mtime),
            int(current_stat.st_size),
        )
        expected_identity = (
            candidate.device,
            candidate.inode,
            candidate.mtime,
            candidate.size_bytes,
        )

        if current_identity != expected_identity:
            LOGGER.warning(
                "Skipping file deletion candidate because it changed after scan: %s",
                path,
            )
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because candidate changed after scan",
                observed_epoch=float(current_stat.st_mtime),
                detail=(
                    f"scan_device={candidate.device}; "
                    f"scan_inode={candidate.inode}; "
                    f"scan_size_bytes={candidate.size_bytes}; "
                    f"current_device={int(current_stat.st_dev)}; "
                    f"current_inode={int(current_stat.st_ino)}; "
                    f"current_size_bytes={int(current_stat.st_size)}"
                ),
            )
            continue

        try:
            path.unlink()
        except FileNotFoundError:
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file disappeared before unlink",
                observed_epoch=candidate.mtime,
                detail=f"scan_size_bytes={candidate.size_bytes}",
            )
            continue
        except OSError as exc:
            LOGGER.warning("Failed deleting file candidate: %s: %s", path, exc)
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because unlink failed",
                observed_epoch=candidate.mtime,
                detail=f"errno={getattr(exc, 'errno', '')}; error={exc}",
            )
            continue

        deleted += 1
        deleted_bytes += max(0, candidate.size_bytes)
        deleted_records.append((candidate.path, candidate.mtime))

    return FileDeleteResult(
        deleted=deleted,
        deleted_bytes=deleted_bytes,
        deleted_records=tuple(deleted_records),
        skipped_records=tuple(skipped_records),
    )


def _delete_directories(paths: list[str], *, report_root: str) -> DirDeleteResult:
    """Delete empty directories after revalidating each path safely.

    The function deletes only real directories. It does not rely on
    ``Path.exists()`` because that follows symlinks. Every candidate is
    revalidated with ``follow_symlinks=False`` immediately before ``rmdir()``.
    Non-existing paths, non-directories, symlinks, and non-empty directories are
    skipped.

    Only real directories are removed. Symlinks, missing paths, non-directories,
    unreadable paths, non-empty directories, and failed removals are skipped and
    captured as structured delete-phase audit records.
    """

    deleted = 0
    deleted_records: list[tuple[str, float]] = []
    skipped_records: list[AuditRecord] = []

    for path_str in _sort_directories_deepest_first(paths):
        path = Path(path_str)

        try:
            path_stat = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="directory",
                why="delete skipped because directory disappeared before deletion",
            )
            continue
        except OSError as exc:
            LOGGER.warning("Skipping directory deletion candidate with unreadable metadata: %s: %s", path, exc)
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="directory",
                why="delete skipped because directory metadata became unreadable",
                detail=f"errno={getattr(exc, 'errno', '')}; error={exc}",
            )
            continue

        if not stat.S_ISDIR(path_stat.st_mode):
            LOGGER.warning("Skipping directory deletion candidate because it is not a real directory: %s", path)
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="entry",
                why="delete skipped because candidate is no longer a real directory",
                observed_epoch=float(path_stat.st_mtime),
                detail=f"mode={oct(stat.S_IMODE(path_stat.st_mode))}",
            )
            continue

        observed_epoch = float(path_stat.st_mtime)

        try:
            path.rmdir()
        except FileNotFoundError:
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="directory",
                why="delete skipped because directory disappeared before rmdir",
                observed_epoch=observed_epoch,
            )
            continue
        except NotADirectoryError:
            LOGGER.warning("Skipping directory deletion candidate because it stopped being a directory: %s", path)
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="entry",
                why="delete skipped because candidate stopped being a directory",
                observed_epoch=observed_epoch,
            )
            continue
        except OSError as exc:
            if exc.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                _append_audit_record(
                    skipped_records,
                    cleanup_root=report_root,
                    path=path_str,
                    item_type="directory",
                    why="delete skipped because directory was not empty at rmdir time",
                    observed_epoch=observed_epoch,
                    detail=f"errno={getattr(exc, 'errno', '')}; error={exc}",
                )
                continue

            LOGGER.warning("Failed deleting empty directory candidate: %s: %s", path, exc)
            _append_audit_record(
                skipped_records,
                cleanup_root=report_root,
                path=path_str,
                item_type="directory",
                why="delete skipped because rmdir failed",
                observed_epoch=observed_epoch,
                detail=f"errno={getattr(exc, 'errno', '')}; error={exc}",
            )
            continue

        deleted += 1
        deleted_records.append((path_str, observed_epoch))

    return DirDeleteResult(
        deleted=deleted,
        deleted_records=tuple(deleted_records),
        skipped_records=tuple(skipped_records),
    )


def _try_create_lock(lock_file: Path) -> bool:
    try:
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return False
    try:
        os.write(fd, f"{os.getpid()}\n".encode())
    finally:
        os.close(fd)
    return True


def _remove_lock(lock_file: Path) -> None:
    try:
        lock_file.unlink(missing_ok=True)
    except OSError as exc:
        _log_warning_table("99", "Lock Cleanup Warning", ["Field", "Value"], [["lock_file", str(lock_file)], ["error", str(exc)]])


def _switch_rows(settings: CleanupSettings) -> list[list[Any]]:
    return [
        [10, "LOGGING__BASE_LOG_FOLDER", "airflow.cfg", settings.base_log_folder, "Single validated cleanup root"],
        [20, "TARGET_DENY_LIST", "Airflow Variable", settings.target_deny_list, "Optional protected top-level targets"],
        [30, "MAX_LOG_AGE_DAYS", "Airflow Variable", settings.max_log_age_days, "Retention threshold in full days for file eligibility"],
        [40, "DRY_RUN", "DAG Param", settings.dry_run, "If true, candidates are reported but nothing is deleted"],
        [50, "DELETE_ENABLED", "Code constant", settings.delete_enabled, "Global code-side delete capability switch"],
        [60, "LOCK_FILE_PATH", "Code constant", settings.lock_file_path, "Shared worker lock used to prevent concurrent cleanup collisions"],
    ]


def _evaluated_state_rows(settings: CleanupSettings) -> list[list[Any]]:
    return [
        [10, "VALIDATED_BASE_LOG_FOLDER", "evaluated", settings.base_log_folder, "Single validated cleanup root"],
        [20, "TARGET_DENY_LIST_VALID", "parsed", settings.target_deny_list, "Valid top-level names excluded from evaluated root scope"],
        [30, "TARGET_DENY_LIST_INVALID", "parsed", settings.invalid_target_deny_list, "Ignored because names are not safe top-level folder names"],
        [40, "EVALUATED_EXCLUDED_TARGETS", "evaluated", [target.label for target in settings.excluded_targets], "Resolved targets excluded by TARGET_DENY_LIST (symlink, non-directory, and unreadable entries)"],
        [50, "MAX_LOG_AGE_DAYS", "evaluated", settings.max_log_age_days, "Retention threshold in full days"],
        [60, "DRY_RUN", "evaluated", settings.dry_run, "Report candidates without deleting"],
        [70, "EFFECTIVE_DELETE_MODE", "evaluated", settings.effective_delete_mode, "Final execution mode after dry-run and code-side delete switch"],
    ]


def _deletion_scope_rows(max_age_days: int, *, dry_run: bool) -> list[list[Any]]:
    return [
        [10, "scan scope", "validated base root", "Only paths under LOGGING__BASE_LOG_FOLDER are checked."],
        [20, "scan scope", "deny-listed targets", "Top-level folders in TARGET_DENY_LIST are skipped with all subfolders."],
        [30, "file eligibility", "regular old files", f"Regular files older than {max_age_days} day(s) can be deleted."],
        [40, "file exclusion", "files not old enough", f"Files aged {max_age_days} day(s) or less are kept."],
        [50, "filesystem safety", "unsafe entries", "Unreadable, non-regular, and cross-filesystem paths are skipped."],
        [60, "directory cleanup", "empty directories", "Empty directories inside included targets may be removed, including the target root if it becomes empty."],
        [70, "execution mode", "dry-run vs delete", "Dry-run reports only." if dry_run else "Delete mode removes matched files and empty directories."],
    ]


def _root_scan_summary_rows(totals: RunTotals) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for metric in SUMMARY_METRICS:
        value = getattr(totals, metric.attr_name)
        rendered_value = _human_bytes(value) if metric.human_bytes else value
        decision = metric.decision if isinstance(value, int) and value > 0 else ""
        method = metric.evaluation_method if isinstance(value, int) and value > 0 else ""
        rows.append([metric.summary_item, rendered_value, decision, method])
    return rows


def _action_audit_title(settings: CleanupSettings) -> str:
    """Return the section title for action audit records."""

    return "Deleted Items" if settings.effective_delete_mode == "delete" else "Candidate Items"


def _action_outcome_rows(
    settings: CleanupSettings,
    totals: RunTotals,
    *,
    action_skipped_count: int,
) -> list[tuple[str, Any]]:
    """Return final delete/action outcome rows for operator-facing summary."""

    return [
        ("mode", settings.effective_delete_mode),
        ("files_deleted", totals.files_deleted),
        ("files_deleted_total_size", _human_bytes(totals.files_deleted_bytes)),
        ("empty_dirs_deleted", totals.empty_dirs_deleted),
        ("action_skipped_items", action_skipped_count),
    ]


def _overall_outcome_rows(totals: RunTotals) -> list[tuple[str, Any]]:
    return [
        ("status", "completed"),
        ("roots_processed", totals.roots_processed),
        ("directories_visited", totals.directories_visited),
        ("directory_entries_seen", totals.directory_entries_seen),
        ("file_entries_seen", totals.file_entries_seen),
        ("files_scanned_regular", totals.files_scanned_regular),
        ("regular_file_total_size", _human_bytes(totals.regular_file_total_size_bytes)),
        ("old_file_candidates", totals.old_file_candidates),
        ("old_file_candidate_total_size", _human_bytes(totals.candidate_file_total_size_bytes)),
        ("empty_dir_candidates", totals.empty_dir_candidates),
        ("files_deleted", totals.files_deleted),
        ("files_deleted_total_size", _human_bytes(totals.files_deleted_bytes)),
        ("empty_dirs_deleted", totals.empty_dirs_deleted),
        ("duration_seconds", totals.duration_seconds),
    ]


def _locked_result(duration_seconds: float) -> dict[str, Any]:
    return {
        "status": "skipped_locked",
        "roots_processed": 0,
        "directories_visited": 0,
        "directory_entries_seen": 0,
        "file_entries_seen": 0,
        "files_scanned_regular": 0,
        "regular_file_total_size_bytes": 0,
        "candidate_file_total_size_bytes": 0,
        "old_file_candidates": 0,
        "empty_dir_candidates": 0,
        "files_deleted": 0,
        "files_deleted_bytes": 0,
        "empty_dirs_deleted": 0,
        "duration_seconds": duration_seconds,
    }


def _completed_result(totals: RunTotals, *, action_skipped_count: int) -> dict[str, Any]:
    return {
        "status": "completed",
        "roots_processed": totals.roots_processed,
        "directories_visited": totals.directories_visited,
        "directory_entries_seen": totals.directory_entries_seen,
        "file_entries_seen": totals.file_entries_seen,
        "files_scanned_regular": totals.files_scanned_regular,
        "regular_file_total_size_bytes": totals.regular_file_total_size_bytes,
        "candidate_file_total_size_bytes": totals.candidate_file_total_size_bytes,
        "old_file_candidates": totals.old_file_candidates,
        "empty_dir_candidates": totals.empty_dir_candidates,
        "files_deleted": totals.files_deleted,
        "files_deleted_bytes": totals.files_deleted_bytes,
        "empty_dirs_deleted": totals.empty_dirs_deleted,
        "duration_seconds": totals.duration_seconds,
        "action_skipped_items": action_skipped_count,
    }


with DAG(
    dag_id=DAG_ID,
    start_date=START_DATE,
    schedule=SCHEDULE,
    max_active_runs=1,
    catchup=False,
    default_args={
        "owner": DAG_OWNER_NAME,
        "depends_on_past": False,
        "email": ALERT_EMAIL_ADDRESSES,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={"dry_run": False},
    tags=TAGS,
    doc_md=__doc__,
) as dag:

    @task(pool="default_pool", pool_slots=1)
    def execute_cleanup() -> dict[str, Any]:
        settings = _build_settings(get_current_context().get("params") or {})
        task_started = time.monotonic()

        _log_section(
            "00",
            "Execution Context",
            [
                ("MAX_LOG_AGE_DAYS", settings.max_log_age_days),
                ("DRY_RUN", settings.dry_run),
                ("DELETE_ENABLED", settings.delete_enabled),
                ("EFFECTIVE_DELETE_MODE", settings.effective_delete_mode),
                ("LOCK_FILE_PATH", settings.lock_file_path),
            ],
        )
        _log_info_table("01", "Configurable switches", ["Priority", "SwitchOrParameter", "SourceType", "CurrentValue", "Purpose"], _switch_rows(settings))
        _log_info_table("02", "Evaluated State", ["Priority", "State", "Source", "Value", "Meaning"], _evaluated_state_rows(settings))
        _log_info_table("04", "Deletion Scope and Exclusions", ["Priority", "Area", "Subject", "Explanation"], _deletion_scope_rows(settings.max_log_age_days, dry_run=settings.dry_run))

        lock_file = Path(settings.lock_file_path)
        if not _try_create_lock(lock_file):
            result = _locked_result(round(time.monotonic() - task_started, 3))
            _log_section("07", "Overall Outcome", [("status", result["status"]), ("roots_processed", result["roots_processed"]), ("duration_seconds", result["duration_seconds"])])
            _log_audit_list("10", "Action Skipped Items", [])
            _log_audit_list("11", "Excluded Items", [])
            _log_audit_list("12", _action_audit_title(settings), [])
            return result

        totals = RunTotals()
        action_audit_records: list[AuditRecord] = []
        action_skipped_audit_records: list[AuditRecord] = []
        excluded_audit_records: list[AuditRecord] = []

        for target in settings.excluded_targets:
            _append_audit_record(
                excluded_audit_records,
                cleanup_root=settings.base_log_folder,
                path=target.path,
                item_type=target.item_type,
                why=target.reason,
            )

        try:
            for target in settings.included_targets:
                cleanup_root = Path(target.path)

                scan_result = _scan_cleanup_target(
                    cleanup_root,
                    max_age_days=settings.max_log_age_days,
                    report_root=settings.base_log_folder,
                )
                if settings.dry_run or not settings.delete_enabled:
                    deleted_files = FileDeleteResult()
                    empty_dir_result = _collect_empty_directories(
                        cleanup_root,
                        report_root=settings.base_log_folder,
                        ignored_regular_files={candidate.path for candidate in scan_result.old_files},
                    )
                    deleted_dirs = DirDeleteResult()
                else:
                    deleted_files = _delete_files(
                        scan_result.old_files,
                        report_root=settings.base_log_folder,
                    )

                    empty_dir_result = _collect_empty_directories(
                        cleanup_root,
                        report_root=settings.base_log_folder,
                    )
                    deleted_dirs = _delete_directories(
                        empty_dir_result.empty_directories,
                        report_root=settings.base_log_folder,
                    )

                totals.add_scan(scan_result.stats, old_file_candidates=len(scan_result.old_files), empty_dir_candidates=len(empty_dir_result.empty_directories))
                totals.add_action(files_deleted=deleted_files.deleted, files_deleted_bytes=deleted_files.deleted_bytes, empty_dirs_deleted=deleted_dirs.deleted)

                excluded_audit_records.extend(scan_result.excluded_records)
                action_skipped_audit_records.extend(deleted_files.skipped_records)
                action_skipped_audit_records.extend(deleted_dirs.skipped_records)
                excluded_audit_records.extend(empty_dir_result.excluded_records)

                if settings.effective_delete_mode == "delete":
                    for path_str, observed_epoch in deleted_files.deleted_records:
                        _append_audit_record(
                            action_audit_records,
                            cleanup_root=settings.base_log_folder,
                            path=path_str,
                            item_type="file",
                            why=f"deleted because regular file age exceeded {settings.max_log_age_days}d",
                            observed_epoch=float(observed_epoch),
                        )

                    for path_str, observed_epoch in deleted_dirs.deleted_records:
                        _append_audit_record(
                            action_audit_records,
                            cleanup_root=settings.base_log_folder,
                            path=path_str,
                            item_type="directory",
                            why="deleted because directory was empty during cleanup phase",
                            observed_epoch=float(observed_epoch),
                        )
                else:
                    for candidate in scan_result.old_files:
                        _append_audit_record(
                            action_audit_records,
                            cleanup_root=settings.base_log_folder,
                            path=candidate.path,
                            item_type="file",
                            why=f"would delete because regular file age exceeded {settings.max_log_age_days}d",
                            observed_epoch=float(candidate.mtime),
                            detail=f"size_bytes={candidate.size_bytes}",
                        )

                    for path_str in empty_dir_result.empty_directories:
                        _append_audit_record(
                            action_audit_records,
                            cleanup_root=settings.base_log_folder,
                            path=path_str,
                            item_type="directory",
                            why="would delete because directory would be empty during cleanup phase",
                        )

            totals.duration_seconds = round(time.monotonic() - task_started, 3)
            _log_info_table("05", "Root Scan Summary", ["SummaryItem", "Value", "Decision", "EvaluationMethod"], _root_scan_summary_rows(totals))
            _log_section("06", "Action Outcome Summary", _action_outcome_rows(settings, totals, action_skipped_count=len(action_skipped_audit_records)))
            _log_section("07", "Overall Outcome", _overall_outcome_rows(totals))
            _log_audit_list("10", "Action Skipped Items", action_skipped_audit_records)
            _log_audit_list("11", "Excluded Items", excluded_audit_records)
            _log_audit_list("12", _action_audit_title(settings), action_audit_records)
            return _completed_result(
                totals,
                action_skipped_count=len(action_skipped_audit_records),
            )
        finally:
            _remove_lock(lock_file)

    execute_cleanup()
