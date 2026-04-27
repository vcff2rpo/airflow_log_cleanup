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

Operational model
-----------------
The DAG evaluates a single Airflow logging root, resolves real top-level
directories below that root, applies an optional top-level deny list, scans for
old regular files, and then optionally deletes matching files plus directories
that are empty after file evaluation.

Safety model
------------
The implementation is intentionally conservative:

- broad roots such as ``/``, ``/opt``, ``/tmp``, and ``/var/log`` are refused;
- top-level symlinks are excluded rather than traversed;
- traversal and deletion stay on the same filesystem device as the target root;
- every file candidate is identity-checked again before ``unlink()``;
- every directory candidate is identity-checked again before ``rmdir()``;
- filesystem races are treated as skip/audit conditions, not fatal cleanup
  failures.

Configuration contract
----------------------
``MAX_LOG_AGE_DAYS`` controls retention. A value of ``0`` is treated as unsafe
and skips the task. ``TARGET_DENY_LIST`` is a comma-separated list of safe
top-level folder names under the validated log root. ``DELETE_LOG_CAP`` is used
only in delete mode to cap rendered audit samples per reason group; dry-run
renders audit samples uncapped.

Audit contract
--------------
Audit output distinguishes exact observed totals from rendered samples. The
``total`` counter remains exact even when output is capped. Rendered samples are
deduplicated within each reason group to keep logs readable while preserving the
cleanup decision summary.
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

__version__ = "2.11"
# prod version

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
    """Resolved top-level cleanup target.

    Instances describe direct children of the validated Airflow log root. Included
    targets are traversable real directories. Excluded targets are still recorded so
    operators can see why a root was not traversed.
    """

    label: str
    path: str
    reason: str
    item_type: str = "directory"


@dataclass(frozen=True)
class CleanupSettings:
    """Fully evaluated runtime settings for one cleanup task run.

    The object contains only resolved values: Airflow configuration, Airflow
    Variables, DAG params, target-resolution results, and the final effective mode.
    The dataclass is frozen; nested target lists should be treated as read-only
    after construction so scan/delete phases stay aligned with logged settings.
    """

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
    """Single audit item rendered under one stable reason group.

    ``real_path`` is stored relative to the validated cleanup root when possible.
    ``observed_epoch`` is populated for age-related records so audit output can show
    the age that was evaluated for the filesystem object.
    """

    item_type: str
    real_path: str
    why: str
    observed_epoch: float = 0.0
    details: AuditDetails = ()


AuditRecordKey = tuple[str, str, str, float, AuditDetails]


@dataclass
class AuditBucket:
    """Audit accumulator for one reason group.

    ``total`` is the exact number of records observed. ``rendered`` is only the
    deduplicated sample that will be printed. This separation lets delete mode cap
    log volume without corrupting counters or XCom summaries.
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

        key: AuditRecordKey = (
            record.item_type,
            record.real_path,
            record.why,
            record.observed_epoch,
            record.details,
        )
        if key in self._rendered_keys:
            return

        self._rendered_keys.add(key)
        self.rendered.append(record)


AuditBuckets = dict[str, AuditBucket]


@dataclass(frozen=True)
class SummaryMetric:
    """Definition of one row in the root scan summary table.

    The tuple links a human-facing summary label to a ``RunTotals`` attribute and
    optionally provides decision text when the value is non-zero.
    """

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
    """Counters collected while scanning one included cleanup target.

    These counters describe what the scanner observed, not what was deleted.
    Deletion counters are kept separately in ``RunTotals`` and delete result objects.
    """

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
    """Aggregated counters for one DAG task execution.

    ``add_scan`` merges per-target scan counters. ``add_action`` merges delete
    results. The final object drives operator-facing summary tables and the returned
    XCom dictionary.
    """

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
    """Deleted file audit source.

    The record stores deleted-file path, scan-time mtime, and scan-time size as
    explicit typed metadata for the final action audit.
    """

    path: str
    observed_epoch: float
    size_bytes: int


@dataclass(frozen=True)
class DeletedDirectoryRecord:
    """Deleted directory audit source for the final action audit section."""

    path: str
    observed_epoch: float


@dataclass(frozen=True)
class FileDeleteResult:
    """Result object returned by the regular-file deletion phase.

    ``skipped_records`` captures delete-time safety skips separately from scan-time
    exclusions because the path was a valid candidate during scan but became unsafe
    or unavailable before deletion completed.
    """

    deleted: int = 0
    deleted_bytes: int = 0
    deleted_records: tuple[DeletedFileRecord, ...] = ()
    skipped_records: AuditBuckets = field(default_factory=dict)


@dataclass(frozen=True)
class DirDeleteResult:
    """Result object returned by the empty-directory deletion phase."""

    deleted: int = 0
    deleted_records: tuple[DeletedDirectoryRecord, ...] = ()
    skipped_records: AuditBuckets = field(default_factory=dict)


@dataclass(frozen=True)
class FileCandidate:
    """Regular file selected by scan for possible deletion.

    The deletion phase compares these scan-time identity fields with the current
    filesystem object before unlinking. This prevents deleting a replaced path.
    """

    path: str
    device: int
    inode: int
    mtime: float
    mtime_ns: int
    size_bytes: int


@dataclass(frozen=True)
class DirCandidate:
    """Directory selected as empty or logically empty after file evaluation."""

    path: str
    device: int
    inode: int


@dataclass(frozen=True)
class EmptyDirCollectResult:
    """Result of empty-directory discovery.

    The collector returns removable directory candidates plus audit records for
    directories or entries that blocked safe empty-directory classification.
    """

    empty_directories: list[DirCandidate] = field(default_factory=list)
    excluded_records: AuditBuckets = field(default_factory=dict)


@dataclass(frozen=True)
class ScanResult:
    """Result of scanning one included cleanup target."""

    stats: ScanStats
    old_files: list[FileCandidate]
    excluded_records: AuditBuckets


def _coerce_positive_int(value: Any, *, field_name: str, zero_is_skip: bool = True) -> int:
    """Parse an Airflow Variable value as a strictly positive integer.

    ``zero_is_skip`` differentiates retention from non-retention controls.
    Retention value ``0`` is treated as an unsafe cleanup configuration and skips
    the task. Delete-mode output cap ``0`` is a configuration error.
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
    """Render a byte count using binary units for operator-facing logs."""
    if num_bytes <= 0:
        return "0 B"
    value, units, index = float(num_bytes), ["B", "KiB", "MiB", "GiB", "TiB", "PiB"], 0
    while value >= 1024.0 and index < len(units) - 1:
        value /= 1024.0
        index += 1
    return f"{int(value)} {units[index]}" if index == 0 else f"{value:.2f} {units[index]}"


def _log_table(
    *,
    level: int,
    number: str,
    title: str,
    headers: list[str],
    rows: list[list[Any]],
) -> None:
    """Render and emit a fixed-width table section to the task logger.

    The function is intentionally local and dependency-free so table output remains
    stable in Airflow task logs. Values are normalized for readability but never used
    for cleanup decisions.
    """
    widths = SECTION_WIDTHS.get(number, [max(5, len(header)) for header in headers])
    border_line = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    rendered = [border_line]
    # Always render a header row and at least one data row so empty sections
    # remain visually explicit in Airflow task logs.

    table_rows: list[list[Any]] = [headers, *(rows or [["<none>"] + [""] * (len(headers) - 1)])]
    for table_index, values in enumerate(table_rows):
        if table_index == 1:
            rendered.append(border_line)

        padded = list(values) + [""] * (len(headers) - len(values))
        columns: list[list[str]] = []
        for index in range(len(headers)):
            cell_value = padded[index]
            if cell_value is None:
                normalized = ""
            elif isinstance(cell_value, Path):
                normalized = str(cell_value)
            elif isinstance(cell_value, bool):
                normalized = "true" if cell_value else "false"
            elif isinstance(cell_value, (list, tuple, set)):
                normalized = ", ".join(str(item) for item in cell_value)
            else:
                normalized = str(cell_value)

            wrapped_cell: list[str] = []
            for paragraph in normalized.replace("\t", "    ").splitlines() or [""]:
                wrapped_cell.extend(
                    textwrap.wrap(
                        paragraph,
                        width=widths[index],
                        break_long_words=True,
                        break_on_hyphens=False,
                        replace_whitespace=False,
                        drop_whitespace=False,
                    )
                    or [""]
                )
            columns.append(wrapped_cell or [""])

        height = max(len(column) for column in columns)
        rendered.extend("| " + " | ".join((column[row_index] if row_index < len(column) else "").ljust(widths[index]) for index, column in enumerate(columns)) + " |" for row_index in range(height))

    rendered.append(border_line)

    LOGGER.log(
        level,
        "%s\n%s\n\n",
        f"{number} :: {title}",
        "\n".join(rendered),
    )


def _log_section(
    number: str,
    title: str,
    rows: list[tuple[str, Any]],
    *,
    level: int = logging.INFO,
) -> None:
    """Emit a standard two-column ``Field`` / ``Value`` table section."""
    _log_table(
        level=level,
        number=number,
        title=title,
        headers=["Field", "Value"],
        rows=[[key, value] for key, value in rows],
    )


def _path_identity(path: str | Path) -> str:
    """Return an absolute path identity key without resolving symlinks.

    This is used for comparison inside one traversal/evaluation pass. Avoid
    ``Path.resolve()`` here because symlink resolution would weaken the cleanup
    safety model.
    """
    return os.path.abspath(os.fspath(path))


def _audit_details(**values: Any) -> AuditDetails:
    """Normalize optional audit metadata into a stable immutable tuple.

    Empty values are omitted to keep audit lines concise. Primitive scalar values
    are preserved; complex values are stringified before rendering.
    """
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
    """Create, normalize, deduplicate, and store one audit record.

    The function centralizes relative-path rendering and bucket insertion so all
    scan, delete, and exclusion paths use the same audit semantics.
    """
    audit_path = Path(path)
    audit_root = Path(cleanup_root)

    # Prefer a normalized relative path for operator readability. If the path
    # cannot be represented under the cleanup root, keep the original path in
    # the audit record instead of failing the cleanup task.
    try:
        real_path = str(audit_path.resolve(strict=False).relative_to(audit_root))
    except (OSError, RuntimeError, ValueError):
        try:
            real_path = str(audit_path.relative_to(audit_root))
        except ValueError:
            real_path = path

    record = AuditRecord(
        item_type=item_type,
        real_path=real_path,
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
    """Merge audit buckets while preserving exact totals and rendered samples."""
    for reason, source_bucket in source.items():
        target_bucket = target.setdefault(reason, AuditBucket())
        target_bucket.total += source_bucket.total

        for record in source_bucket.rendered:
            target_bucket.add_rendered_sample(record, limit=audit_limit)


def _directory_path_sort_key(path_value: str) -> tuple[int, int, str, str, str]:
    """Return a deterministic deepest-first sort key for directory paths.

    Deepest-first ordering is used for directory deletion and audit rendering so
    children are handled before their parents.
    """
    path = Path(path_value)
    return (
        -len(path.parts),
        -len(path_value),
        path.name.casefold(),
        path.name,
        path_value,
    )


def _log_audit_list(
    number: str,
    title: str,
    records: AuditBuckets,
    *,
    evaluation_epoch: float,
    delete_log_cap: int,
) -> None:
    """Render grouped audit records with exact totals and optional cap output.

    ``delete_log_cap`` is active only when greater than zero. Report-only mode
    passes zero to render uncapped audit samples. The cap affects log volume only; counters and
    cleanup decisions are not changed.
    """
    grouped: list[tuple[str, int, list[AuditRecord]]] = []
    # Sort each reason group deterministically so repeated runs are easier to
    # diff even when filesystem traversal order changes.

    for reason in sorted(records):
        bucket = records[reason]
        grouped_records = bucket.rendered

        if reason in {
            "would delete because directory would be empty during cleanup phase",
            "deleted because directory was empty during cleanup phase",
        }:
            sorted_records = sorted(
                grouped_records,
                key=lambda record: (*_directory_path_sort_key(record.real_path), repr(record.details)),
            )
        else:
            sortable_records: list[tuple[tuple[int, float, float, int, int, str, str], AuditRecord]] = []
            for record in grouped_records:
                age_days = float("inf")
                for key, value in record.details:
                    if key != "age_days":
                        continue
                    try:
                        age_days = float(value)
                    except (TypeError, ValueError):
                        age_days = float("inf")
                    break

                sortable_records.append(
                    (
                        (
                            {"file": 10, "directory": 20, "entry": 30, "symlink": 40}.get(record.item_type, 99),
                            -age_days,
                            record.observed_epoch,
                            len(Path(record.real_path).parts),
                            len(record.real_path),
                            record.real_path,
                            repr(record.details),
                        ),
                        record,
                    )
                )
            sorted_records = [record for _, record in sorted(sortable_records, key=lambda item: item[0])]

        grouped.append((reason, bucket.total, sorted_records))

    if not grouped:
        LOGGER.info("%s\n- none\n\n", f"{number} :: {title}")
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

        lines.append(f"- {title_prefix} reason: {reason} | total_items={group_total} | rendered_items={rendered_count} | capped_items={capped_count} | {cap_label}")
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

    LOGGER.info("%s\n%s\n\n", f"{number} :: {title}", "\n".join(lines))


def _resolve_top_level_targets(
    *,
    base_log_folder: str,
    target_deny_list: list[str],
) -> tuple[list[TargetSpec], list[TargetSpec]]:
    """Resolve included and excluded top-level cleanup targets.

    Only real direct child directories of the validated log root become traversal
    roots. Symlinks, files, devices, unreadable entries, and deny-listed directories
    are excluded and returned for audit visibility.
    """
    root = Path(base_log_folder)
    # ``follow_symlinks=False`` is mandatory here: a top-level symlink must be
    # audited as excluded instead of becoming a cleanup traversal root.

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

    return (
        sorted(included, key=lambda item: (len(item.path), item.path, item.label)),
        sorted(excluded, key=lambda item: (len(item.path), item.path, item.label)),
    )


def _build_settings(params: dict[str, Any]) -> CleanupSettings:
    """Resolve and validate all runtime configuration for the task.

    The function aggregates configuration errors so operators can fix all invalid
    values in one pass. Invalid configuration raises ``AirflowSkipException`` to
    prevent unsafe cleanup execution.
    """
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
        raw_base_log_folder = conf.get("logging", "base_log_folder", fallback=None)
        if raw_base_log_folder is None or not raw_base_log_folder.strip():
            config_errors.append(
                (
                    "logging.base_log_folder",
                    "logging.base_log_folder is empty in airflow.cfg. Provide a valid absolute directory path.",
                )
            )
        else:
            base_path = Path(raw_base_log_folder).expanduser()
            resolved_base_log_folder = str(base_path.resolve(strict=False))
            if not base_path.is_absolute():
                raise ValueError(f"Cleanup root must be absolute: {raw_base_log_folder!r}")
            if resolved_base_log_folder in BROAD_ROOTS:
                raise ValueError(f"Refusing unsafe cleanup root {resolved_base_log_folder!r}; path is too broad.")
            if len(Path(resolved_base_log_folder).parts) < 3:
                raise ValueError(f"Refusing unsafe cleanup root {resolved_base_log_folder!r}; path is not specific enough.")
            base_log_folder = resolved_base_log_folder.rstrip("/")

    except ValueError as exc:
        config_errors.append(("logging.base_log_folder", str(exc)))

    deny_list_values: set[str] = set()
    invalid_deny_list: list[str] = []

    for item in str(Variable.get(TARGET_DENY_LIST_VARIABLE_KEY, default="")).split(","):
        candidate = item.strip()
        if not candidate:
            continue

        if "/" not in candidate and "\\" not in candidate and candidate not in {".", ".."} and Path(candidate).name == candidate:
            deny_list_values.add(candidate)
        else:
            invalid_deny_list.append(candidate)

    if invalid_deny_list:
        LOGGER.warning(
            "Invalid target names ignored for %s: %s",
            TARGET_DENY_LIST_VARIABLE_KEY,
            ", ".join(invalid_deny_list),
        )

    deny_list = sorted(deny_list_values)
    invalid_deny_list = sorted(invalid_deny_list)

    effective_delete_mode = "report-only" if dry_run or not DELETE_ENABLED else "delete"
    # A zero cap means "uncapped rendering" in report-only mode. In delete mode
    # the configured cap must be strictly positive.
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
    """Scan one included target for old regular-file candidates.

    Traversal does not follow symlinks and stays on the target root filesystem.
    Young files, non-regular entries, unreadable paths, and cross-device paths are
    kept and audited rather than deleted.
    """
    last_progress_log = time.monotonic()
    retention_threshold_seconds = max_age_days * SECONDS_PER_DAY

    old_files: list[FileCandidate] = []
    excluded_records: AuditBuckets = {}
    stats = ScanStats()

    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"Failed reading target root metadata for {root}: {exc}") from exc

    if not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"Target root is not a real directory: {root}")

    root_device = root_stat.st_dev
    # Keep traversal on the root device. This avoids crossing into mounted
    # volumes that may not be part of the Airflow log-retention scope.

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
    """Collect directories that are empty or would become empty safely.

    During dry-run, ``ignored_regular_files`` contains old-file candidates and lets
    the collector report directories that would be empty after those files were
    deleted. Unreadable or cross-device subtrees block parent deletion.
    """
    try:
        root_stat = root.stat(follow_symlinks=False)
    except OSError as exc:
        raise RuntimeError(f"Failed reading target root metadata for {root}: {exc}") from exc

    if not stat.S_ISDIR(root_stat.st_mode):
        raise RuntimeError(f"Target root is not a real directory: {root}")

    root_device = root_stat.st_dev
    # Dry-run parity: old-file candidates are treated as logically absent so the
    # reported empty-directory candidates match delete-mode behavior.

    ignored_file_keys = {_path_identity(path) for path in ignored_regular_files or set()}

    excluded_records: AuditBuckets = {}
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
    """Delete regular-file candidates after delete-time identity revalidation.

    The function deletes only when the current path still matches device, inode,
    mtime nanoseconds, and size captured during scan. Any race or mismatch is
    audited as a delete-phase skip.
    """
    deleted = 0
    deleted_bytes = 0
    deleted_records: list[DeletedFileRecord] = []
    skipped_records: AuditBuckets = {}

    last_progress_log = time.monotonic()

    for index, candidate in enumerate(candidates, start=1):
        progress_now = time.monotonic()
        if progress_now - last_progress_log >= PROGRESS_LOG_INTERVAL_SECONDS:
            LOGGER.info(
                "cleanup_delete_files_progress processed=%d total=%d deleted=%d skipped=%d",
                index - 1,
                len(candidates),
                deleted,
                sum(bucket.total for bucket in skipped_records.values()),
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

        # Revalidate object identity immediately before deletion to avoid
        # unlinking a different file if the path changed after scanning.
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
    """Delete empty-directory candidates after identity revalidation.

    Directories are removed deepest-first. A candidate is skipped if it disappeared,
    changed identity, became non-directory, or is no longer empty at ``rmdir`` time.
    """
    deleted = 0
    deleted_records: list[DeletedDirectoryRecord] = []
    skipped_records: AuditBuckets = {}

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

        # Revalidate directory identity before rmdir for the same race-safety
        # reason used by file deletion.
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
        """Execute the Airflow log cleanup task.

        The task performs configuration resolution, audit setup, target scanning,
        optional deletion, empty-directory evaluation, summary logging, and final XCom
        result construction. Destructive operations run only when effective mode is
        ``delete``.
        """
        task_started = time.monotonic()
        context = get_current_context()

        dag_run = context.get("dag_run")
        dag_run_start = getattr(dag_run, "start_date", None)
        if dag_run_start is not None:
            try:
                evaluation_epoch = float(dag_run_start.timestamp())
            except (AttributeError, TypeError, ValueError, OSError):
                LOGGER.warning("Unable to derive evaluation epoch from dag_run.start_date; falling back to current wall-clock time.")
                evaluation_epoch = time.time()
        else:
            evaluation_epoch = time.time()

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
                ("DELETE_LOG_CAP", settings.delete_log_cap if settings.effective_delete_mode == "delete" else "disabled"),
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
            rows=[
                [10, "LOGGING__BASE_LOG_FOLDER", "airflow.cfg", settings.base_log_folder, "Single validated cleanup root"],
                [20, "TARGET_DENY_LIST", "Airflow Variable", settings.target_deny_list, "Optional protected top-level targets"],
                [30, "MAX_LOG_AGE_DAYS", "Airflow Variable", settings.max_log_age_days, "Retention threshold in full days for file eligibility"],
                [
                    40,
                    "DELETE_LOG_CAP",
                    "Airflow Variable",
                    settings.delete_log_cap if settings.effective_delete_mode == "delete" else "disabled",
                    "Maximum rendered audit sample items per reason group in delete mode; dry-run/report-only mode is uncapped",
                ],
                [50, "DRY_RUN", "DAG Param", settings.dry_run, "If true, candidates are reported but nothing is deleted"],
                [60, "DELETE_ENABLED", "Code constant", settings.delete_enabled, "Global code-side delete capability switch"],
                [70, "LOCK_FILE_PATH", "Code constant", settings.lock_file_path, "Shared worker lock used to prevent concurrent cleanup collisions"],
            ],
        )
        _log_table(
            level=logging.INFO,
            number="02",
            title="Evaluated State",
            headers=["Priority", "State", "Source", "Value", "Meaning"],
            rows=[
                [10, "VALIDATED_BASE_LOG_FOLDER", "evaluated", settings.base_log_folder, "Single validated cleanup root"],
                [20, "TARGET_DENY_LIST_VALID", "parsed", settings.target_deny_list, "Valid top-level names excluded from evaluated root scope"],
                [30, "TARGET_DENY_LIST_INVALID", "parsed", settings.invalid_target_deny_list, "Ignored because names are not safe top-level folder names"],
                [
                    40,
                    "EVALUATED_EXCLUDED_TARGETS",
                    "evaluated",
                    [target.label for target in settings.excluded_targets],
                    "Resolved targets excluded by TARGET_DENY_LIST (symlink, non-directory, and unreadable entries)",
                ],
                [50, "MAX_LOG_AGE_DAYS", "evaluated", settings.max_log_age_days, "Retention threshold in full days"],
                [
                    60,
                    "DELETE_LOG_CAP",
                    "evaluated",
                    settings.delete_log_cap if settings.effective_delete_mode == "delete" else "disabled",
                    "Audit sample cap is applied only in delete mode; dry-run renders uncapped audit samples",
                ],
                [70, "DRY_RUN", "evaluated", settings.dry_run, "Report candidates without deleting"],
                [80, "EFFECTIVE_DELETE_MODE", "evaluated", settings.effective_delete_mode, "Final execution mode after dry-run and code-side delete switch"],
            ],
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
            rows=[
                [10, "scan scope", "validated base root", "Only paths under LOGGING__BASE_LOG_FOLDER are checked."],
                [20, "scan scope", "deny-listed targets", "Top-level folders in TARGET_DENY_LIST are skipped with all subfolders."],
                [30, "file eligibility", "regular old files", f"Regular files older than {settings.max_log_age_days} day(s) can be deleted."],
                [40, "file exclusion", "files not old enough", f"Files aged {settings.max_log_age_days} day(s) or less are kept."],
                [50, "filesystem safety", "unsafe entries", "Unreadable, non-regular, and cross-filesystem paths are skipped."],
                [60, "directory cleanup", "empty directories", "Empty directories inside included targets may be removed, including the target root if it becomes empty."],
                [
                    70,
                    "effective mode",
                    "dry-run vs delete",
                    "Delete mode removes matched files and empty directories." if settings.effective_delete_mode == "delete" else "Report-only mode; no files or directories are deleted.",
                ],
            ],
        )

        lock_file = Path(settings.lock_file_path)
        if not _try_create_lock(lock_file):
            result: dict[str, Any] = {
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
                "duration_seconds": round(time.monotonic() - task_started, 3),
            }
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
                {},
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "11",
                "Excluded Items",
                {},
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            _log_audit_list(
                "12",
                ("Deleted Items" if settings.effective_delete_mode == "delete" else "Candidate Items"),
                {},
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )
            return result

        totals = RunTotals()
        # Separate buckets keep scan exclusions, delete-time skips, and final
        # action records independently readable in the task log.

        action_audit_records: AuditBuckets = {}
        action_skipped_audit_records: AuditBuckets = {}
        excluded_audit_records: AuditBuckets = {}
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

                if settings.effective_delete_mode == "delete":
                    for deleted_file_record in deleted_files.deleted_records:
                        _add_audit_record(
                            action_audit_records,
                            audit_limit=audit_limit,
                            cleanup_root=settings.base_log_folder,
                            path=deleted_file_record.path,
                            item_type="file",
                            why=f"deleted because regular file age exceeded {settings.max_log_age_days}d",
                            observed_epoch=deleted_file_record.observed_epoch,
                        )

                    for deleted_directory_record in deleted_dirs.deleted_records:
                        _add_audit_record(
                            action_audit_records,
                            audit_limit=audit_limit,
                            cleanup_root=settings.base_log_folder,
                            path=deleted_directory_record.path,
                            item_type="directory",
                            why="deleted because directory was empty during cleanup phase",
                            observed_epoch=deleted_directory_record.observed_epoch,
                        )
                else:
                    for file_candidate in scan_result.old_files:
                        _add_audit_record(
                            action_audit_records,
                            audit_limit=audit_limit,
                            cleanup_root=settings.base_log_folder,
                            path=file_candidate.path,
                            item_type="file",
                            why=f"would delete because regular file age exceeded {settings.max_log_age_days}d",
                            observed_epoch=float(file_candidate.mtime),
                        )

                    for directory_candidate in empty_dir_result.empty_directories:
                        _add_audit_record(
                            action_audit_records,
                            audit_limit=audit_limit,
                            cleanup_root=settings.base_log_folder,
                            path=directory_candidate.path,
                            item_type="directory",
                            why="would delete because directory would be empty during cleanup phase",
                        )

            totals.duration_seconds = round(time.monotonic() - task_started, 3)

            _log_table(
                level=logging.INFO,
                number="05",
                title="Root Scan Summary",
                headers=["SummaryItem", "Value", "Decision", "EvaluationMethod"],
                rows=[
                    [
                        metric.summary_item,
                        _human_bytes(getattr(totals, metric.attr_name)) if metric.human_bytes else getattr(totals, metric.attr_name),
                        metric.decision if isinstance(getattr(totals, metric.attr_name), int) and getattr(totals, metric.attr_name) > 0 else "",
                        metric.evaluation_method if isinstance(getattr(totals, metric.attr_name), int) and getattr(totals, metric.attr_name) > 0 else "",
                    ]
                    for metric in SUMMARY_METRICS
                ],
            )
            _log_section(
                "06",
                "Action Outcome Summary",
                [
                    ("mode", settings.effective_delete_mode),
                    ("files_deleted", totals.files_deleted),
                    ("files_deleted_total_size", _human_bytes(totals.files_deleted_bytes)),
                    ("empty_dirs_deleted", totals.empty_dirs_deleted),
                    ("action_skipped_items", sum(bucket.total for bucket in action_skipped_audit_records.values())),
                ],
            )
            _log_section(
                "07",
                "Overall Outcome",
                [
                    ("status", "completed"),
                    *[
                        (
                            label,
                            _human_bytes(getattr(totals, attr_name)) if render_bytes else getattr(totals, attr_name),
                        )
                        for label, attr_name, render_bytes in OVERALL_OUTCOME_FIELDS
                    ],
                ],
            )
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
                ("Deleted Items" if settings.effective_delete_mode == "delete" else "Candidate Items"),
                action_audit_records,
                evaluation_epoch=evaluation_epoch,
                delete_log_cap=settings.delete_log_cap,
            )

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
                "action_skipped_items": sum(bucket.total for bucket in action_skipped_audit_records.values()),
            }

        finally:
            _remove_lock(lock_file)

    execute_cleanup()
