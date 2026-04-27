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
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, Param, Variable, get_current_context, task

LOGGER = logging.getLogger(__name__)

__version__ = "2.10"

DAG_ID = "airflow_log_cleanup"
START_DATE = pendulum.datetime(2024, 1, 1, tz="Europe/Prague")
SCHEDULE = "@daily"
DAG_OWNER_NAME = "operations,maintenance"
ALERT_EMAIL_ADDRESSES: list[str] = []
DELETE_ENABLED = True
# Worker-local lock. This prevents same-host cleanup collisions.
LOCK_FILE_PATH = "/tmp/airflow_log_cleanup.lock"
MAX_LOG_AGE_VARIABLE_KEY = "MAX_LOG_AGE_DAYS"
TARGET_DENY_LIST_VARIABLE_KEY = "TARGET_DENY_LIST"
DELETE_LOG_CAP_VARIABLE_KEY = "DELETE_LOG_CAP"
SECONDS_PER_DAY = 86_400
PROGRESS_LOG_INTERVAL_SECONDS = 10
TASK_EXECUTION_TIMEOUT = timedelta(minutes=5)

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
    "03": [30, 125],
    "04": [10, 18, 34, 110],
    "05": [36, 18, 12, 55],
    "06": [40, 26],
    "08": [10, 44, 22, 36, 72],
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
    delete_log_cap: int
    lock_file_path: str


AuditValue = str | int | float | bool
AuditDetails = tuple[tuple[str, AuditValue], ...]


@dataclass(frozen=True)
class AuditRecord:
    """One operator-facing audit line item.

    ``why`` is intentionally stable so audit records group cleanly.
    Variable per-record values belong in structured ``details``.
    """

    item_type: str
    real_path: str
    why: str
    observed_epoch: float = 0.0
    details: AuditDetails = ()


AuditRecordKey = tuple[str, str, str, float, AuditDetails]


@dataclass
class AuditBucket:
    """Bounded audit collector for one audit reason group.

    ``total`` keeps the exact number of records observed for the reason group.
    ``rendered`` keeps only a capped deduplicated sample for log output.
    """

    total: int = 0
    rendered: list[AuditRecord] = field(default_factory=list)
    _rendered_keys: set[AuditRecordKey] = field(default_factory=set, repr=False)

    def add(self, record: AuditRecord, *, limit: int) -> None:
        self.total += 1
        self.add_rendered_sample(record, limit=limit)

    def add_rendered_sample(self, record: AuditRecord, *, limit: int) -> None:
        if limit > 0 and len(self.rendered) >= limit:
            return
    
        key = _audit_record_key(record)
        if key in self._rendered_keys:
            return

        self._rendered_keys.add(key)
        self.rendered.append(record)


AuditBuckets = dict[str, AuditBucket]


@dataclass(frozen=True)
class SummaryMetric:
    """Root summary row definition with optional decision text."""

    summary_item: str
    attr_name: str
    decision: str = ""
    evaluation_method: str = ""
    human_bytes: bool = False


SUMMARY_METRICS: tuple[SummaryMetric, ...] = (
    SummaryMetric(
        summary_item="roots_processed",
        attr_name="roots_processed",
    ),
    SummaryMetric(
        summary_item="directories_visited",
        attr_name="directories_visited",
    ),
    SummaryMetric(
        summary_item="directory_entries_seen",
        attr_name="directory_entries_seen",
    ),
    SummaryMetric(
        summary_item="file_entries_seen",
        attr_name="file_entries_seen",
    ),
    SummaryMetric(
        summary_item="files_scanned_regular",
        attr_name="files_scanned_regular",
    ),
    SummaryMetric(
        summary_item="regular_file_total_size",
        attr_name="regular_file_total_size_bytes",
        human_bytes=True,
    ),
    SummaryMetric(
        summary_item="old_file_candidates",
        attr_name="old_file_candidates",
    ),
    SummaryMetric(
        summary_item="old_file_candidate_total_size",
        attr_name="candidate_file_total_size_bytes",
        human_bytes=True,
    ),
    SummaryMetric(
        summary_item="empty_dir_candidates",
        attr_name="empty_dir_candidates",
    ),
    SummaryMetric(
        summary_item="directories_skipped_inaccessible",
        attr_name="directories_skipped_inaccessible",
        decision="skipped",
        evaluation_method="Directory metadata unreadable during traversal",
    ),
    SummaryMetric(
        summary_item="directories_skipped_not_directory",
        attr_name="directories_skipped_not_directory",
        decision="skipped",
        evaluation_method="Entry is not a traversable directory",
    ),
    SummaryMetric(
        summary_item="directories_skipped_mount_boundary",
        attr_name="directories_skipped_mount_boundary",
        decision="skipped",
        evaluation_method="Directory is on a different filesystem",
    ),
    SummaryMetric(
        summary_item="files_skipped_inaccessible",
        attr_name="files_skipped_inaccessible",
        decision="skipped",
        evaluation_method="File metadata unreadable during evaluation",
    ),
    SummaryMetric(
        summary_item="files_skipped_cross_device",
        attr_name="files_skipped_cross_device",
        decision="skipped",
        evaluation_method="File is on a different filesystem",
    ),
    SummaryMetric(
        summary_item="files_skipped_non_regular",
        attr_name="files_skipped_non_regular",
        decision="skipped",
        evaluation_method="Entry is not a regular file",
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
class DeletedFileRecord:
    """Deleted regular file audit source with explicit field names."""

    path: str
    observed_epoch: float
    size_bytes: int


@dataclass(frozen=True)
class DeletedDirectoryRecord:
    """Deleted empty-directory audit source with explicit field names."""

    path: str
    observed_epoch: float


@dataclass(frozen=True)
class FileDeleteResult:
    """Result of regular file deletion.

    ``deleted_records`` contains named records instead of positional tuples so
    action-audit construction remains explicit and type-safe.

    ``skipped_records`` contains delete-phase safety skips. These are different
    from scan-phase exclusions because the path was a valid old-file candidate
    during scanning but became unsafe, unavailable, or inconsistent before the
    delete operation completed.
    """

    deleted: int = 0
    deleted_bytes: int = 0
    deleted_records: tuple[DeletedFileRecord, ...] = ()
    skipped_records: AuditBuckets = field(default_factory=dict)


@dataclass(frozen=True)
class DirDeleteResult:
    """Result of empty-directory deletion.

    ``deleted_records`` contains named records instead of positional tuples so
    action-audit construction remains explicit and type-safe.

    ``skipped_records`` contains delete-phase directory skips. These explain why
    a directory candidate was not removed after the empty-directory collection
    phase selected it.
    """

    deleted: int = 0
    deleted_records: tuple[DeletedDirectoryRecord, ...] = ()
    skipped_records: AuditBuckets = field(default_factory=dict)


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
    mtime_ns: int
    size_bytes: int


@dataclass(frozen=True)
class DirCandidate:
    path: str
    device: int
    inode: int


@dataclass(frozen=True)
class EmptyDirCollectResult:
    empty_directories: list[DirCandidate] = field(default_factory=list)
    excluded_records: AuditBuckets = field(default_factory=dict)


@dataclass(frozen=True)
class ScanResult:
    """Scan result for one included target."""

    stats: ScanStats
    old_files: list[FileCandidate]
    excluded_records: AuditBuckets


def _coerce_positive_int(value: Any, *, field_name: str, zero_is_skip: bool = True) -> int:
    """Coerce an Airflow Variable value into a strictly positive integer.

    ``zero_is_skip=True`` preserves retention safety for ``MAX_LOG_AGE_DAYS``.
    ``zero_is_skip=False`` rejects non-retention controls such as
    ``DELETE_LOG_CAP`` as configuration errors. No artificial maximum is applied.
    """

    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer, got bool.")

    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer, got {value!r}.") from exc

    if parsed == 0:
        if zero_is_skip:
            raise AirflowSkipException(f"{field_name}=0 is unsafe for log cleanup because value must be greater than 0.")
        raise ValueError(f"{field_name} must be > 0, got 0.")

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


def _resolve_evaluation_epoch(context: dict[str, Any]) -> float:
    """Return one stable epoch used for age evaluation and audit rendering.

    Prefer the DAG run start timestamp because it remains stable across task
    retries for the same DAG run. Fall back to wall-clock time when the runtime
    context does not provide a usable DAG run timestamp.
    """

    dag_run = context.get("dag_run")
    dag_run_start = getattr(dag_run, "start_date", None)

    if dag_run_start is not None:
        try:
            return float(dag_run_start.timestamp())
        except (AttributeError, TypeError, ValueError, OSError):
            LOGGER.warning("Unable to derive evaluation epoch from dag_run.start_date; falling back to current wall-clock time.")

    return time.time()


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


def _log_table(
    *,
    level: int,
    number: str,
    title: str,
    headers: list[str],
    rows: list[list[Any]],
) -> None:
    LOGGER.log(
        level,
        "%s\n%s\n\n",
        _section_title(number, title),
        _render_table(headers, rows, _section_widths(number, headers)),
    )


def _log_section(
    number: str,
    title: str,
    rows: list[tuple[str, Any]],
    *,
    level: int = logging.INFO,
) -> None:
    _log_table(
        level=level,
        number=number,
        title=title,
        headers=["Field", "Value"],
        rows=[[key, value] for key, value in rows],
    )


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
    valid: set[str] = set()
    invalid: list[str] = []
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


def _audit_details(**values: Any) -> AuditDetails:
    details: list[tuple[str, AuditValue]] = []

    for key, value in values.items():
        if value is None or value == "":
            continue

        if isinstance(value, bool):
            details.append((key, value))
        elif isinstance(value, (str, int, float)):
            details.append((key, value))
        else:
            details.append((key, str(value)))

    return tuple(details)


def _audit_record_key(record: AuditRecord) -> AuditRecordKey:
    return (
        record.item_type,
        record.real_path,
        record.why,
        record.observed_epoch,
        record.details,
    )


def _new_audit_buckets() -> AuditBuckets:
    return {}


def _make_audit_record(
    *,
    cleanup_root: str,
    path: str,
    item_type: str,
    why: str,
    observed_epoch: float = 0.0,
    details: AuditDetails = (),
) -> AuditRecord:
    return AuditRecord(
        item_type=item_type,
        real_path=_relative_path(path, cleanup_root),
        why=why,
        observed_epoch=observed_epoch,
        details=details,
    )


def _add_audit_record(
    records: AuditBuckets,
    *,
    audit_limit: int,
    cleanup_root: str,
    path: str,
    item_type: str,
    why: str,
    observed_epoch: float = 0.0,
    details: AuditDetails = (),
) -> None:
    record = _make_audit_record(
        cleanup_root=cleanup_root,
        path=path,
        item_type=item_type,
        why=why,
        observed_epoch=observed_epoch,
        details=details,
    )
    records.setdefault(record.why, AuditBucket()).add(
        record,
        limit=audit_limit,
    )


def _merge_audit_buckets(
    target: AuditBuckets,
    source: AuditBuckets,
    *,
    audit_limit: int,
) -> None:
    render_limit = audit_limit

    for reason, source_bucket in source.items():
        target_bucket = target.setdefault(reason, AuditBucket())
        target_bucket.total += source_bucket.total

        for record in source_bucket.rendered:
            target_bucket.add_rendered_sample(record, limit=render_limit)


def _audit_total(records: AuditBuckets) -> int:
    return sum(bucket.total for bucket in records.values())


def _audit_detail_number(record: AuditRecord, key_name: str) -> float:
    for key, value in record.details:
        if key != key_name:
            continue

        try:
            return float(value)
        except (TypeError, ValueError):
            return float("inf")

    return float("inf")


def _audit_item_type_order(item_type: str) -> int:
    """Return stable audit item type ordering inside one reason group."""

    order = {
        "file": 10,
        "directory": 20,
        "entry": 30,
        "symlink": 40,
    }
    return order.get(item_type, 99)


def _directory_path_sort_key(path_value: str) -> tuple[int, int, str, str, str]:
    """Sort directory paths deepest-first with deterministic name ordering."""

    path = Path(path_value)
    return (
        -len(path.parts),
        -len(path_value),
        path.name.casefold(),
        path.name,
        path_value,
    )


def _directory_audit_record_sort_key(record: AuditRecord) -> tuple[int, int, str, str, str, str]:
    """Sort directory audit records deepest-first with deterministic detail ordering."""

    return (*_directory_path_sort_key(record.real_path), repr(record.details))


def _audit_record_sort_key(record: AuditRecord) -> tuple[int, float, float, int, int, str, str]:
    """Return deterministic sort key for audit records inside one reason group."""

    return (
        _audit_item_type_order(record.item_type),
        -_audit_detail_number(record, "age_days"),
        record.observed_epoch,
        len(Path(record.real_path).parts),
        len(record.real_path),
        record.real_path,
        repr(record.details),
    )


def _group_audit_buckets_by_reason(
    records: AuditBuckets,
) -> list[tuple[str, int, list[AuditRecord]]]:
    """Group capped audit samples by reason with exact observed totals."""

    result: list[tuple[str, int, list[AuditRecord]]] = []

    for reason in sorted(records):
        bucket = records[reason]
        grouped_records = bucket.rendered

        if reason in {
            "would delete because directory would be empty during cleanup phase",
            "deleted because directory was empty during cleanup phase",
        }:
            sorted_records = sorted(grouped_records, key=_directory_audit_record_sort_key)
        else:
            sorted_records = sorted(grouped_records, key=_audit_record_sort_key)

        result.append((reason, bucket.total, sorted_records))

    return result


def _log_audit_list(
    number: str,
    title: str,
    records: AuditBuckets,
    *,
    evaluation_epoch: float,
    delete_log_cap: int,
) -> None:
    """Log capped audit samples grouped by reason with exact observed totals.

    ``DELETE_LOG_CAP`` limits rendered item lines per reason group in both
    dry-run and delete mode. It does not change cleanup logic, deletion
    decisions, counters, or XCom result values.
    """

    grouped = _group_audit_buckets_by_reason(records)

    if not grouped:
        LOGGER.info("%s\n- none\n\n", _section_title(number, title))
        return

    if title in {"Deleted Items", "Candidate Items"}:
        grouped = sorted(
            grouped,
            key=lambda item: (
                0 if item[2] and item[2][0].item_type == "file" else 1,
                item[0],
            ),
        )

    lines: list[str] = []
    title_prefix = title.lower()
    cap_enabled = delete_log_cap > 0
    cap_label = f"DELETE_LOG_CAP={delete_log_cap}" if cap_enabled else "DELETE_LOG_CAP=disabled"
    
    for group_index, (reason, group_total, grouped_records) in enumerate(grouped, start=1):
        rendered_group_records = grouped_records[:delete_log_cap] if cap_enabled else grouped_records
        rendered_count = len(rendered_group_records)
        capped_count = max(0, group_total - rendered_count) if cap_enabled else 0
    
        lines.append(
            f"- {title_prefix} reason: {reason} | total_items={group_total} | "
            f"rendered_items={rendered_count} | capped_items={capped_count} | {cap_label}"
        )
        for index, record in enumerate(rendered_group_records, start=1):
            suffix_parts: list[str] = []

            if record.item_type == "file" and record.observed_epoch > 0:
                observed_age_days = max(0.0, evaluation_epoch - record.observed_epoch) / SECONDS_PER_DAY
                suffix_parts.append(f"observed_age_days={observed_age_days:.2f}")

            retained_detail_parts = [f"{key}={value}" for key, value in record.details if key not in {"age_days"}]

            if retained_detail_parts:
                suffix_parts.append(f"detail={'; '.join(retained_detail_parts)}")

            suffix = f" | {' | '.join(suffix_parts)}" if suffix_parts else ""
            lines.append(f"  {index}. [{record.item_type}] {record.real_path}{suffix}")

        if capped_count > 0:
            lines.append(f"  ... capped {capped_count} item(s) by DELETE_LOG_CAP={delete_log_cap}")

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

        if stat.S_ISREG(child_stat.st_mode):
            excluded.append(
                TargetSpec(
                    label=label,
                    path=child_path,
                    reason=("Excluded because top-level root file is outside cleanup target policy; only top-level directories are traversed"),
                    item_type="file",
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
    config_errors: list[tuple[str, str]] = []

    raw_dry_run = params.get("dry_run", False)
    dry_run = False
    if isinstance(raw_dry_run, bool):
        dry_run = raw_dry_run
    elif isinstance(raw_dry_run, str) and raw_dry_run.strip().lower() in {"true", "false"}:
        dry_run = raw_dry_run.strip().lower() == "true"
    else:
        config_errors.append(
            (
                "dry_run",
                f"dry_run must be a boolean or one of the strings 'true'/'false', got {raw_dry_run!r}.",
            )
        )

    base_log_folder: str | None = None
    try:
        base_log_folder = _read_logging_path("base_log_folder", required=True)
        if base_log_folder is None:
            config_errors.append(
                (
                    "logging.base_log_folder",
                    "logging.base_log_folder is required for log cleanup.",
                )
            )
    except ValueError as exc:
        config_errors.append(("logging.base_log_folder", str(exc)))

    deny_list, invalid_deny_list = _parse_target_name_list(
        Variable.get(TARGET_DENY_LIST_VARIABLE_KEY, default=""),
        field_name=TARGET_DENY_LIST_VARIABLE_KEY,
    )

    effective_delete_mode = "report-only" if dry_run or not DELETE_ENABLED else "delete"
    delete_log_cap = 0
    if effective_delete_mode == "delete":
        try:
            delete_log_cap = _coerce_positive_int(
                Variable.get(DELETE_LOG_CAP_VARIABLE_KEY, default="10"),
                field_name=DELETE_LOG_CAP_VARIABLE_KEY,
                zero_is_skip=False,
            )
        except (AirflowSkipException, ValueError) as exc:
            config_errors.append((DELETE_LOG_CAP_VARIABLE_KEY, str(exc)))
    
    max_log_age_days = 1
    try:
        max_log_age_days = _coerce_positive_int(
            Variable.get(MAX_LOG_AGE_VARIABLE_KEY, default="30"),
            field_name=MAX_LOG_AGE_VARIABLE_KEY,
        )
    except (AirflowSkipException, ValueError) as exc:
        config_errors.append((MAX_LOG_AGE_VARIABLE_KEY, str(exc)))

    included: list[TargetSpec] = []
    excluded: list[TargetSpec] = []
    if base_log_folder is not None:
        try:
            included, excluded = _resolve_top_level_targets(
                base_log_folder=base_log_folder,
                target_deny_list=deny_list,
            )
        except ValueError as exc:
            config_errors.append(("target_resolution", str(exc)))

    if config_errors:
        details = "\n".join(f"- {field}: {message}" for field, message in config_errors)
        raise AirflowSkipException(f"Invalid airflow_log_cleanup configuration. Task processing skipped.\n{details}")

    if base_log_folder is None:
        raise AirflowSkipException("Invalid airflow_log_cleanup configuration. Task processing skipped.\n- logging.base_log_folder: logging.base_log_folder is required for log cleanup.")

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
        delete_log_cap=delete_log_cap,
        lock_file_path=LOCK_FILE_PATH,
    )


def _scan_cleanup_target(
    root: Path,
    *,
    max_age_days: int,
    report_root: str,
    evaluation_epoch: float,
    audit_limit: int,
) -> ScanResult:
    """Scan one cleanup target for old regular files.

    The scan is intentionally constrained to one real filesystem device and does
    not follow symlink directories. Unreadable directories, non-directory
    traversal entries, cross-device directories, unreadable files, cross-device
    files, and non-regular files are skipped and audited.

    ``evaluation_epoch`` is resolved once by the task and reused for every
    target. This keeps age eligibility deterministic inside the run and avoids
    age drift between scanning and audit rendering.
    """

    last_progress_log = time.monotonic()
    retention_threshold_seconds = max_age_days * SECONDS_PER_DAY

    old_files: list[FileCandidate] = []
    excluded_records: AuditBuckets = _new_audit_buckets()
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
        _add_audit_record(
            excluded_records,
            audit_limit=audit_limit,
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

        progress_now = time.monotonic()
        if progress_now - last_progress_log >= PROGRESS_LOG_INTERVAL_SECONDS:
            LOGGER.info(
                "cleanup_scan_progress target=%s current=%s directories_visited=%d directory_entries_seen=%d file_entries_seen=%d files_scanned_regular=%d",
                root,
                current,
                stats.directories_visited,
                stats.directory_entries_seen,
                stats.file_entries_seen,
                stats.files_scanned_regular,
            )
            last_progress_log = progress_now

        allowed_dirnames: list[str] = []

        for dirname in dirnames:
            candidate = current / dirname

            try:
                candidate_stat = candidate.stat(follow_symlinks=False)
            except OSError:
                stats.directories_skipped_inaccessible += 1
                _add_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    audit_limit=audit_limit,
                    path=str(candidate),
                    item_type="directory",
                    why="directory metadata unreadable during traversal",
                )
                continue

            if not stat.S_ISDIR(candidate_stat.st_mode):
                stats.directories_skipped_not_directory += 1
                _add_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    audit_limit=audit_limit,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a traversable directory",
                )
                continue

            if candidate_stat.st_dev != root_device:
                stats.directories_skipped_mount_boundary += 1
                _add_audit_record(
                    excluded_records,
                    cleanup_root=report_root,
                    audit_limit=audit_limit,
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
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why="file metadata unreadable",
                )
                continue

            if candidate_stat.st_dev != root_device:
                stats.files_skipped_cross_device += 1
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why="file is on a different filesystem",
                )
                continue

            if not stat.S_ISREG(candidate_stat.st_mode):
                stats.files_skipped_non_regular += 1
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a regular file",
                )
                continue

            size_bytes = int(candidate_stat.st_size)
            file_age_seconds = max(0.0, evaluation_epoch - float(candidate_stat.st_mtime))
            age_days_display = file_age_seconds / SECONDS_PER_DAY

            stats.files_scanned_regular += 1
            stats.regular_file_total_size_bytes += size_bytes

            if file_age_seconds <= retention_threshold_seconds:
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="file",
                    why=f"regular file age is not above threshold {max_age_days}d",
                    observed_epoch=float(candidate_stat.st_mtime),
                    details=_audit_details(
                        age_days=round(age_days_display, 2),
                        threshold_days=max_age_days,
                    ),
                )
                continue

            old_files.append(
                FileCandidate(
                    path=str(candidate),
                    device=int(candidate_stat.st_dev),
                    inode=int(candidate_stat.st_ino),
                    mtime=float(candidate_stat.st_mtime),
                    mtime_ns=int(candidate_stat.st_mtime_ns),
                    size_bytes=size_bytes,
                )
            )
            stats.candidate_file_total_size_bytes += size_bytes

    return ScanResult(
        stats=stats,
        old_files=sorted(old_files, key=lambda item: (len(item.path), item.path)),
        excluded_records=excluded_records,
    )


def _collect_empty_directories(
    root: Path,
    *,
    report_root: str,
    audit_limit: int,
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

    excluded_records: AuditBuckets = _new_audit_buckets()
    discovered_dirs: list[Path] = []
    blocked_directory_keys: set[str] = set()

    def walk_error(exc: OSError) -> None:
        error_path = str(exc.filename) if getattr(exc, "filename", None) else str(root)
        blocked_directory_keys.add(_path_identity(error_path))
        _add_audit_record(
            excluded_records,
            audit_limit=audit_limit,
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
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory metadata unreadable during empty-directory traversal",
                )
                continue

            if not stat.S_ISDIR(candidate_stat.st_mode):
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="entry",
                    why="entry is not a traversable directory during empty-directory traversal",
                )
                continue

            if candidate_stat.st_dev != root_device:
                blocked_directory_keys.add(_path_identity(candidate))
                _add_audit_record(
                    excluded_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=str(candidate),
                    item_type="directory",
                    why="directory is on a different filesystem during empty-directory traversal",
                )
                continue

            allowed_dirnames.append(dirname)

        dirnames[:] = allowed_dirnames

    removable_dirs: list[DirCandidate] = []
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
            _add_audit_record(
                excluded_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=str(directory),
                item_type="directory",
                why="directory metadata unreadable during empty-directory evaluation",
            )
            subtree_has_blocking_entry[directory] = True
            continue

        if not stat.S_ISDIR(directory_stat.st_mode):
            _add_audit_record(
                excluded_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=str(directory),
                item_type="entry",
                why="entry is not a directory during empty-directory evaluation",
            )
            subtree_has_blocking_entry[directory] = True
            continue

        if directory_stat.st_dev != root_device:
            blocked_directory_keys.add(directory_key)
            _add_audit_record(
                excluded_records,
                audit_limit=audit_limit,
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
                    _add_audit_record(
                        excluded_records,
                        audit_limit=audit_limit,
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
            _add_audit_record(
                excluded_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=str(directory),
                item_type="directory",
                why="directory contents unreadable during empty-directory evaluation",
            )
            has_blocking_entry = True

        subtree_has_blocking_entry[directory] = has_blocking_entry

        if not has_blocking_entry:
            removable_dirs.append(
                DirCandidate(
                    path=str(directory),
                    device=int(directory_stat.st_dev),
                    inode=int(directory_stat.st_ino),
                )
            )

    return EmptyDirCollectResult(
        empty_directories=sorted(
            removable_dirs,
            key=lambda item: _directory_path_sort_key(item.path),
        ),
        excluded_records=excluded_records,
    )


def _delete_files(
    candidates: list[FileCandidate],
    *,
    report_root: str,
    audit_limit: int,
) -> FileDeleteResult:
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
    deleted_records: list[DeletedFileRecord] = []
    skipped_records: AuditBuckets = _new_audit_buckets()

    last_progress_log = time.monotonic()

    for index, candidate in enumerate(candidates, start=1):
        progress_now = time.monotonic()
        if progress_now - last_progress_log >= PROGRESS_LOG_INTERVAL_SECONDS:
            LOGGER.info(
                "cleanup_delete_files_progress processed=%d total=%d deleted=%d skipped=%d",
                index - 1,
                len(candidates),
                deleted,
                _audit_total(skipped_records),
            )
            last_progress_log = progress_now

        path = Path(candidate.path)

        try:
            current_stat = path.stat(follow_symlinks=False)
        except FileNotFoundError as exc:
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file disappeared before deletion",
                observed_epoch=candidate.mtime,
                details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
            )
            continue
        except OSError as exc:
            LOGGER.warning(
                "Skipping file deletion candidate with unreadable metadata: %s: %s",
                path,
                exc,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file metadata became unreadable",
                observed_epoch=candidate.mtime,
                details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
            )
            continue

        if not stat.S_ISREG(current_stat.st_mode):
            LOGGER.warning(
                "Skipping file deletion candidate because it is not a regular file: %s",
                path,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="entry",
                why="delete skipped because candidate is no longer a regular file",
                observed_epoch=float(current_stat.st_mtime),
                details=_audit_details(mode=oct(stat.S_IMODE(current_stat.st_mode))),
            )
            continue

        current_identity = (
            int(current_stat.st_dev),
            int(current_stat.st_ino),
            int(current_stat.st_mtime_ns),
            int(current_stat.st_size),
        )
        expected_identity = (
            candidate.device,
            candidate.inode,
            candidate.mtime_ns,
            candidate.size_bytes,
        )

        if current_identity != expected_identity:
            LOGGER.warning(
                "Skipping file deletion candidate because it changed after scan: %s",
                path,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because candidate changed after scan",
                observed_epoch=float(current_stat.st_mtime),
                details=_audit_details(
                    scan_device=candidate.device,
                    scan_inode=candidate.inode,
                    scan_size_bytes=candidate.size_bytes,
                    current_device=int(current_stat.st_dev),
                    current_inode=int(current_stat.st_ino),
                    current_size_bytes=int(current_stat.st_size),
                ),
            )
            continue

        try:
            path.unlink()
        except FileNotFoundError as exc:
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because file disappeared before unlink",
                observed_epoch=candidate.mtime,
                details=_audit_details(
                    errno=getattr(exc, "errno", ""),
                    error=str(exc),
                    scan_size_bytes=candidate.size_bytes,
                ),
            )
            continue
        except OSError as exc:
            LOGGER.warning("Failed deleting file candidate: %s: %s", path, exc)
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="file",
                why="delete skipped because unlink failed",
                observed_epoch=candidate.mtime,
                details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
            )
            continue

        deleted += 1
        deleted_bytes += max(0, candidate.size_bytes)
        deleted_records.append(
            DeletedFileRecord(
                path=candidate.path,
                observed_epoch=candidate.mtime,
                size_bytes=candidate.size_bytes,
            )
        )

    return FileDeleteResult(
        deleted=deleted,
        deleted_bytes=deleted_bytes,
        deleted_records=tuple(deleted_records),
        skipped_records=skipped_records,
    )


def _delete_directories(
    candidates: list[DirCandidate],
    *,
    report_root: str,
    audit_limit: int,
) -> DirDeleteResult:
    """Delete empty directories after revalidating each candidate safely.

    The function deletes only real directories whose current filesystem identity
    still matches the identity collected during empty-directory evaluation.

    It does not rely on ``Path.exists()`` because that follows symlinks. Every
    candidate is revalidated with ``follow_symlinks=False`` immediately before
    ``rmdir()``.

    Missing paths, unreadable paths, non-directories, changed/recreated
    directories, non-empty directories, and failed removals are skipped and
    captured as structured delete-phase audit records.
    """

    deleted = 0
    deleted_records: list[DeletedDirectoryRecord] = []
    skipped_records: AuditBuckets = _new_audit_buckets()

    for candidate in sorted(
        candidates,
        key=lambda item: _directory_path_sort_key(item.path),
    ):
        path = Path(candidate.path)

        try:
            path_stat = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="directory",
                why="delete skipped because directory disappeared before deletion",
            )
            continue
        except OSError as exc:
            LOGGER.warning(
                "Skipping directory deletion candidate with unreadable metadata: %s: %s",
                path,
                exc,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="directory",
                why="delete skipped because directory metadata became unreadable",
                details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
            )
            continue

        if not stat.S_ISDIR(path_stat.st_mode):
            LOGGER.warning(
                "Skipping directory deletion candidate because it is not a real directory: %s",
                path,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="entry",
                why="delete skipped because candidate is no longer a real directory",
                observed_epoch=float(path_stat.st_mtime),
                details=_audit_details(mode=oct(stat.S_IMODE(path_stat.st_mode))),
            )
            continue

        observed_epoch = float(path_stat.st_mtime)

        current_identity = (
            int(path_stat.st_dev),
            int(path_stat.st_ino),
        )
        expected_identity = (
            candidate.device,
            candidate.inode,
        )

        if current_identity != expected_identity:
            LOGGER.warning(
                "Skipping directory deletion candidate because it was replaced after collection: %s",
                path,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="directory",
                why="delete skipped because directory was replaced after collection",
                observed_epoch=observed_epoch,
                details=_audit_details(
                    scan_device=candidate.device,
                    scan_inode=candidate.inode,
                    current_device=int(path_stat.st_dev),
                    current_inode=int(path_stat.st_ino),
                    current_mtime_ns=int(path_stat.st_mtime_ns),
                ),
            )
            continue

        try:
            path.rmdir()
        except FileNotFoundError:
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="directory",
                why="delete skipped because directory disappeared before rmdir",
                observed_epoch=observed_epoch,
            )
            continue
        except NotADirectoryError:
            LOGGER.warning(
                "Skipping directory deletion candidate because it stopped being a directory: %s",
                path,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="entry",
                why="delete skipped because candidate stopped being a directory",
                observed_epoch=observed_epoch,
            )
            continue
        except OSError as exc:
            if exc.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                _add_audit_record(
                    skipped_records,
                    audit_limit=audit_limit,
                    cleanup_root=report_root,
                    path=candidate.path,
                    item_type="directory",
                    why="delete skipped because directory was not empty at rmdir time",
                    observed_epoch=observed_epoch,
                    details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
                )
                continue

            LOGGER.warning(
                "Failed deleting empty directory candidate: %s: %s",
                path,
                exc,
            )
            _add_audit_record(
                skipped_records,
                audit_limit=audit_limit,
                cleanup_root=report_root,
                path=candidate.path,
                item_type="directory",
                why="delete skipped because rmdir failed",
                observed_epoch=observed_epoch,
                details=_audit_details(errno=getattr(exc, "errno", ""), error=str(exc)),
            )
            continue

        deleted += 1
        deleted_records.append(
            DeletedDirectoryRecord(
                path=candidate.path,
                observed_epoch=observed_epoch,
            )
        )

    return DirDeleteResult(
        deleted=deleted,
        deleted_records=tuple(deleted_records),
        skipped_records=skipped_records,
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
        _log_table(
            level=logging.WARNING,
            number="99",
            title="Lock Cleanup Warning",
            headers=["Field", "Value"],
            rows=[
                ["lock_file", str(lock_file)],
                ["error", str(exc)],
            ],
        )


def _switch_rows(settings: CleanupSettings) -> list[list[Any]]:
    return [
        [10, "LOGGING__BASE_LOG_FOLDER", "airflow.cfg", settings.base_log_folder, "Single validated cleanup root"],
        [20, "TARGET_DENY_LIST", "Airflow Variable", settings.target_deny_list, "Optional protected top-level targets"],
        [30, "MAX_LOG_AGE_DAYS", "Airflow Variable", settings.max_log_age_days, "Retention threshold in full days for file eligibility"],
        [40, "DELETE_LOG_CAP", "Airflow Variable", settings.delete_log_cap if settings.effective_delete_mode == "delete" else "disabled", "Maximum rendered audit sample items per reason group in dry-run and delete mode"],
        [50, "DRY_RUN", "DAG Param", settings.dry_run, "If true, candidates are reported but nothing is deleted"],
        [60, "DELETE_ENABLED", "Code constant", settings.delete_enabled, "Global code-side delete capability switch"],
        [70, "LOCK_FILE_PATH", "Code constant", settings.lock_file_path, "Shared worker lock used to prevent concurrent cleanup collisions"],
    ]


def _evaluated_state_rows(settings: CleanupSettings) -> list[list[Any]]:
    return [
        [10, "VALIDATED_BASE_LOG_FOLDER", "evaluated", settings.base_log_folder, "Single validated cleanup root"],
        [20, "TARGET_DENY_LIST_VALID", "parsed", settings.target_deny_list, "Valid top-level names excluded from evaluated root scope"],
        [30, "TARGET_DENY_LIST_INVALID", "parsed", settings.invalid_target_deny_list, "Ignored because names are not safe top-level folder names"],
        [40, "EVALUATED_EXCLUDED_TARGETS", "evaluated", [target.label for target in settings.excluded_targets], "Resolved targets excluded by TARGET_DENY_LIST (symlink, non-directory, and unreadable entries)"],
        [50, "MAX_LOG_AGE_DAYS", "evaluated", settings.max_log_age_days, "Retention threshold in full days"],
        [60, "DELETE_LOG_CAP", "evaluated", settings.delete_log_cap if settings.effective_delete_mode == "delete" else "disabled", "Audit sample cap is applied only in delete mode; dry-run renders uncapped audit samples"],
        [70, "DRY_RUN", "evaluated", settings.dry_run, "Report candidates without deleting"],
        [80, "EFFECTIVE_DELETE_MODE", "evaluated", settings.effective_delete_mode, "Final execution mode after dry-run and code-side delete switch"],
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


def _append_action_audit_records(
    records: AuditBuckets,
    *,
    settings: CleanupSettings,
    scan_result: ScanResult,
    empty_dir_result: EmptyDirCollectResult,
    deleted_files: FileDeleteResult,
    deleted_dirs: DirDeleteResult,
) -> None:
    audit_limit = max(1, settings.delete_log_cap) if settings.effective_delete_mode == "delete" else 0

    if settings.effective_delete_mode == "delete":
        for record in deleted_files.deleted_records:
            _add_audit_record(
                records,
                audit_limit=audit_limit,
                cleanup_root=settings.base_log_folder,
                path=record.path,
                item_type="file",
                why=f"deleted because regular file age exceeded {settings.max_log_age_days}d",
                observed_epoch=record.observed_epoch,
            )

        for record in deleted_dirs.deleted_records:
            _add_audit_record(
                records,
                audit_limit=audit_limit,
                cleanup_root=settings.base_log_folder,
                path=record.path,
                item_type="directory",
                why="deleted because directory was empty during cleanup phase",
                observed_epoch=record.observed_epoch,
            )

        return

    for candidate in scan_result.old_files:
        _add_audit_record(
            records,
            audit_limit=audit_limit,
            cleanup_root=settings.base_log_folder,
            path=candidate.path,
            item_type="file",
            why=f"would delete because regular file age exceeded {settings.max_log_age_days}d",
            observed_epoch=float(candidate.mtime),
        )

    for candidate in empty_dir_result.empty_directories:
        _add_audit_record(
            records,
            audit_limit=audit_limit,
            cleanup_root=settings.base_log_folder,
            path=candidate.path,
            item_type="directory",
            why="would delete because directory would be empty during cleanup phase",
        )


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


OVERALL_OUTCOME_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("roots_processed", "roots_processed", False),
    ("directories_visited", "directories_visited", False),
    ("directory_entries_seen", "directory_entries_seen", False),
    ("file_entries_seen", "file_entries_seen", False),
    ("files_scanned_regular", "files_scanned_regular", False),
    ("regular_file_total_size", "regular_file_total_size_bytes", True),
    ("old_file_candidates", "old_file_candidates", False),
    ("old_file_candidate_total_size", "candidate_file_total_size_bytes", True),
    ("empty_dir_candidates", "empty_dir_candidates", False),
    ("files_deleted", "files_deleted", False),
    ("files_deleted_total_size", "files_deleted_bytes", True),
    ("empty_dirs_deleted", "empty_dirs_deleted", False),
    ("duration_seconds", "duration_seconds", False),
)


def _overall_outcome_rows(totals: RunTotals) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = [("status", "completed")]

    for label, attr_name, render_bytes in OVERALL_OUTCOME_FIELDS:
        value = getattr(totals, attr_name)
        rows.append((label, _human_bytes(value) if render_bytes else value))

    return rows


def _result_dict(
    *,
    status: str,
    totals: RunTotals | None = None,
    duration_seconds: float,
    action_skipped_count: int | None = None,
) -> dict[str, Any]:
    source = totals or RunTotals()

    result: dict[str, Any] = {
        "status": status,
        "roots_processed": source.roots_processed,
        "directories_visited": source.directories_visited,
        "directory_entries_seen": source.directory_entries_seen,
        "file_entries_seen": source.file_entries_seen,
        "files_scanned_regular": source.files_scanned_regular,
        "regular_file_total_size_bytes": source.regular_file_total_size_bytes,
        "candidate_file_total_size_bytes": source.candidate_file_total_size_bytes,
        "old_file_candidates": source.old_file_candidates,
        "empty_dir_candidates": source.empty_dir_candidates,
        "files_deleted": source.files_deleted,
        "files_deleted_bytes": source.files_deleted_bytes,
        "empty_dirs_deleted": source.empty_dirs_deleted,
        "duration_seconds": duration_seconds,
    }

    if action_skipped_count is not None:
        result["action_skipped_items"] = action_skipped_count

    return result


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
        "email_on_failure": bool(ALERT_EMAIL_ADDRESSES),
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={"dry_run": Param(False, type="boolean")},
    tags=TAGS,
    doc_md=__doc__,
) as dag:

    @task(
        pool="default_pool",
        pool_slots=1,
        execution_timeout=TASK_EXECUTION_TIMEOUT,
    )
    def execute_cleanup() -> dict[str, Any]:
        task_started = time.monotonic()
        context = get_current_context()
        evaluation_epoch = _resolve_evaluation_epoch(context)

        try:
            settings = _build_settings(context["params"])
        except (AirflowSkipException, ValueError) as exc:
            _log_section(
                "03",
                "Configuration Failure",
                [
                    ("status", "failed_configuration"),
                    ("exception_type", type(exc).__name__),
                    ("error", str(exc)),
                    ("duration_seconds", round(time.monotonic() - task_started, 3)),
                ],
                level=logging.ERROR,
            )
            raise

        _log_section(
            "00",
            "Execution Context",
            [
                ("MAX_LOG_AGE_DAYS", settings.max_log_age_days),
                ("DELETE_LOG_CAP", settings.delete_log_cap),
                ("DRY_RUN", settings.dry_run),
                ("DELETE_ENABLED", settings.delete_enabled),
                ("EFFECTIVE_DELETE_MODE", settings.effective_delete_mode),
                ("LOCK_FILE_PATH", settings.lock_file_path),
                ("EVALUATION_EPOCH", round(evaluation_epoch, 3)),
            ],
        )
        _log_table(
            level=logging.INFO,
            number="01",
            title="Configurable switches",
            headers=[
                "Priority",
                "SwitchOrParameter",
                "SourceType",
                "CurrentValue",
                "Purpose",
            ],
            rows=_switch_rows(settings),
        )
        _log_table(
            level=logging.INFO,
            number="02",
            title="Evaluated State",
            headers=["Priority", "State", "Source", "Value", "Meaning"],
            rows=_evaluated_state_rows(settings),
        )

        advisory_rows: list[list[Any]] = []
        for invalid_name in settings.invalid_target_deny_list:
            advisory_rows.append(
                [
                    10,
                    TARGET_DENY_LIST_VARIABLE_KEY,
                    "ignored_invalid_top_level_name",
                    invalid_name,
                    "Ignored because the value is not a safe top-level folder name.",
                ]
            )

        if not settings.included_targets:
            advisory_rows.append(
                [
                    20,
                    "target_resolution",
                    "no_included_targets",
                    settings.base_log_folder,
                    "No top-level directory under the validated base root is eligible for cleanup.",
                ]
            )

        if advisory_rows:
            _log_table(
                level=logging.WARNING,
                number="08",
                title="Configuration Advisories",
                headers=["Priority", "Source", "Advisory", "Value", "Meaning"],
                rows=advisory_rows,
            )

        _log_table(
            level=logging.INFO,
            number="04",
            title="Deletion Scope and Exclusions",
            headers=["Priority", "Area", "Subject", "Explanation"],
            rows=_deletion_scope_rows(settings.max_log_age_days, dry_run=settings.dry_run),
        )

        lock_file = Path(settings.lock_file_path)
        if not _try_create_lock(lock_file):
            result = _result_dict(
                status="skipped_locked",
                duration_seconds=round(time.monotonic() - task_started, 3),
            )
            _log_section(
                "07",
                "Overall Outcome",
                [
                    ("status", result["status"]),
                    ("roots_processed", result["roots_processed"]),
                    ("duration_seconds", result["duration_seconds"]),
                ],
            )
            _log_audit_list(
                "10",
                "Action Skipped Items",
                _new_audit_buckets(),
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "11",
                "Excluded Items",
                _new_audit_buckets(),
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "12",
                _action_audit_title(settings),
                _new_audit_buckets(),
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            return result

        totals = RunTotals()
        action_audit_records: AuditBuckets = _new_audit_buckets()
        action_skipped_audit_records: AuditBuckets = _new_audit_buckets()
        excluded_audit_records: AuditBuckets = _new_audit_buckets()
        audit_limit = max(1, settings.delete_log_cap) if settings.effective_delete_mode == "delete" else 0

        for target in settings.excluded_targets:
            _add_audit_record(
                excluded_audit_records,
                audit_limit=audit_limit,
                cleanup_root=settings.base_log_folder,
                path=target.path,
                item_type=target.item_type,
                why=target.reason,
            )

        for invalid_name in settings.invalid_target_deny_list:
            _add_audit_record(
                excluded_audit_records,
                audit_limit=audit_limit,
                cleanup_root=settings.base_log_folder,
                path=settings.base_log_folder,
                item_type="entry",
                why=("invalid TARGET_DENY_LIST item ignored because it is not a safe top-level folder name"),
                details=_audit_details(invalid_target_name=invalid_name),
            )

        if not settings.included_targets:
            _add_audit_record(
                excluded_audit_records,
                audit_limit=audit_limit,
                cleanup_root=settings.base_log_folder,
                path=settings.base_log_folder,
                item_type="directory",
                why="no included cleanup targets were resolved under validated base root",
            )

        try:
            for target in settings.included_targets:
                cleanup_root = Path(target.path)

                try:
                    scan_result = _scan_cleanup_target(
                        cleanup_root,
                        max_age_days=settings.max_log_age_days,
                        report_root=settings.base_log_folder,
                        evaluation_epoch=evaluation_epoch,
                        audit_limit=audit_limit,
                    )
                except RuntimeError as exc:
                    _add_audit_record(
                        excluded_audit_records,
                        audit_limit=audit_limit,
                        cleanup_root=settings.base_log_folder,
                        path=target.path,
                        item_type="directory",
                        why="target skipped because target root became unavailable during scan",
                        details=_audit_details(
                            exception_type=type(exc).__name__,
                            error=str(exc),
                        ),
                    )
                    continue

                deleted_files = FileDeleteResult()
                deleted_dirs = DirDeleteResult()

                try:
                    if settings.effective_delete_mode != "delete":
                        empty_dir_result = _collect_empty_directories(
                            cleanup_root,
                            report_root=settings.base_log_folder,
                            audit_limit=audit_limit,
                            ignored_regular_files={candidate.path for candidate in scan_result.old_files},
                        )
                    else:
                        deleted_files = _delete_files(
                            scan_result.old_files,
                            report_root=settings.base_log_folder,
                            audit_limit=audit_limit,
                        )

                        empty_dir_result = _collect_empty_directories(
                            cleanup_root,
                            report_root=settings.base_log_folder,
                            audit_limit=audit_limit,
                        )
                        deleted_dirs = _delete_directories(
                            empty_dir_result.empty_directories,
                            report_root=settings.base_log_folder,
                            audit_limit=audit_limit,
                        )
                except RuntimeError as exc:
                    empty_dir_result = EmptyDirCollectResult()
                    _add_audit_record(
                        excluded_audit_records,
                        audit_limit=audit_limit,
                        cleanup_root=settings.base_log_folder,
                        path=target.path,
                        item_type="directory",
                        why="target skipped because target root became unavailable during empty-directory evaluation",
                        details=_audit_details(
                            exception_type=type(exc).__name__,
                            error=str(exc),
                        ),
                    )

                totals.add_scan(
                    scan_result.stats,
                    old_file_candidates=len(scan_result.old_files),
                    empty_dir_candidates=len(empty_dir_result.empty_directories),
                )
                totals.add_action(
                    files_deleted=deleted_files.deleted,
                    files_deleted_bytes=deleted_files.deleted_bytes,
                    empty_dirs_deleted=deleted_dirs.deleted,
                )
                _merge_audit_buckets(
                    excluded_audit_records,
                    scan_result.excluded_records,
                    audit_limit=audit_limit,
                )
                _merge_audit_buckets(
                    excluded_audit_records,
                    empty_dir_result.excluded_records,
                    audit_limit=audit_limit,
                )
                _merge_audit_buckets(
                    action_skipped_audit_records,
                    deleted_files.skipped_records,
                    audit_limit=audit_limit,
                )
                _merge_audit_buckets(
                    action_skipped_audit_records,
                    deleted_dirs.skipped_records,
                    audit_limit=audit_limit,
                )
                _append_action_audit_records(
                    action_audit_records,
                    settings=settings,
                    scan_result=scan_result,
                    empty_dir_result=empty_dir_result,
                    deleted_files=deleted_files,
                    deleted_dirs=deleted_dirs,
                )

            totals.duration_seconds = round(time.monotonic() - task_started, 3)

            _log_table(
                level=logging.INFO,
                number="05",
                title="Root Scan Summary",
                headers=["SummaryItem", "Value", "Decision", "EvaluationMethod"],
                rows=_root_scan_summary_rows(totals),
            )
            _log_section(
                "06",
                "Action Outcome Summary",
                _action_outcome_rows(
                    settings,
                    totals,
                    action_skipped_count=_audit_total(action_skipped_audit_records),
                ),
            )
            _log_section("07", "Overall Outcome", _overall_outcome_rows(totals))
            _log_audit_list(
                "10",
                "Action Skipped Items",
                action_skipped_audit_records,
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "11",
                "Excluded Items",
                excluded_audit_records,
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "12",
                _action_audit_title(settings),
                action_audit_records,
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )

            return _result_dict(
                status="completed",
                totals=totals,
                duration_seconds=totals.duration_seconds,
                action_skipped_count=_audit_total(action_skipped_audit_records),
            )
        finally:
            _remove_lock(lock_file)

    execute_cleanup()
