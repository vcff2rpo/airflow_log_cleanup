# HOWTO.md — Airflow Log Cleanup DAG Runbook

Manual operating guide for the `airflow_log_cleanup` DAG.

![Airflow Log Cleanup DAG — Operator Runbook Flow](readme_visuals/02_operational_runbook_flow.png)

This runbook covers deployment validation, first rollout, dry-run review, delete-mode execution, troubleshooting, emergency stop, and post-run evidence collection.

---

## 1. Scope

This DAG cleans old Airflow log files only under `logging.base_log_folder`.

It does not clean:

- Airflow metadata database rows
- object-storage logs
- archived/compressed logs
- logs outside `logging.base_log_folder`
- arbitrary system directories

The cleanup policy is intentionally conservative. If a path is unsafe, changed, unreadable, cross-device, not regular, or not a real directory when expected, it is kept and audited.

---

## 2. Operator prerequisites

Before running the DAG, confirm:

- you can read Airflow task logs
- you can set Airflow Variables
- you can inspect the worker filesystem if stale lock cleanup is required
- you know the expected value of `logging.base_log_folder`
- you know which top-level log subfolders must be protected by `TARGET_DENY_LIST`

---

## 3. Configuration reference

### Airflow config

```bash
airflow config get-value logging base_log_folder
```

The value must be:

- absolute
- specific to the Airflow log directory
- not one of the blocked broad roots
- readable by the Airflow worker user

### Airflow Variables

| Variable | Required | Default | Valid value | Operational meaning |
|---|---:|---:|---|---|
| `MAX_LOG_AGE_DAYS` | No | `30` | integer `> 0` | Retention threshold. Files are candidates only when age is strictly greater than this number of days. |
| `TARGET_DENY_LIST` | No | empty | comma-separated top-level names | Top-level directories under `logging.base_log_folder` that must not be scanned. |
| `DELETE_LOG_CAP` | No | `10` | integer `> 0` in delete mode | Caps rendered audit samples per reason group in delete mode. Report-only mode is uncapped and displays `disabled`. |

### DAG param

| Param | Default | Meaning |
|---|---:|---|
| `dry_run` | `false` | `true` means report-only; `false` allows delete mode when `DELETE_ENABLED=True`. |

### Code switch

| Constant | Current value | Meaning |
|---|---:|---|
| `DELETE_ENABLED` | `True` | If set to `False`, all runs are report-only even when `dry_run=false`. |

---

## 4. First deployment checklist

### Step 1 — Copy DAG file

```bash
cp log_clean_maint.py "$AIRFLOW_HOME/dags/log_clean_maint.py"
```

### Step 2 — Validate Python syntax

```bash
python -m py_compile "$AIRFLOW_HOME/dags/log_clean_maint.py"
```

Expected: no output and exit code `0`.

### Step 3 — Confirm Airflow sees the DAG

```bash
airflow dags list | grep airflow_log_cleanup
```

Expected: one row for `airflow_log_cleanup`.

### Step 4 — Confirm root configuration

```bash
airflow config get-value logging base_log_folder
```

Reject the deployment if the value is missing, relative, or broad, for example:

```text
/
/tmp
/var
/var/log
/opt
/opt/airflow
```

### Step 5 — Inspect top-level target candidates

```bash
LOG_ROOT="$(airflow config get-value logging base_log_folder)"
find "$LOG_ROOT" -mindepth 1 -maxdepth 1 -printf '%y %p\n' | sort
```

Use this to decide `TARGET_DENY_LIST`.

### Step 6 — Set variables

Example:

```bash
airflow variables set MAX_LOG_AGE_DAYS 30
airflow variables set TARGET_DENY_LIST 'scheduler,triggerer,dag_processor'
airflow variables set DELETE_LOG_CAP 25
```

### Step 7 — Run report-only validation

```bash
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": true}'
```

Do not proceed to delete mode until the report-only output is reviewed.

---

## 5. Report-only review procedure

Open the task log for `execute_cleanup`.

Review these sections in order.

### `00 :: Execution Context`

Confirm:

- `MAX_LOG_AGE_DAYS` is expected
- `DELETE_LOG_CAP` shows `disabled`
- `DRY_RUN` is `true`
- `EFFECTIVE_DELETE_MODE` is `report-only`
- `LOCK_FILE_PATH` is expected

### `01 :: Configurable switches`

Confirm:

- `logging.base_log_folder` points to the correct Airflow log directory
- `TARGET_DENY_LIST` is expected
- `MAX_LOG_AGE_DAYS` is expected
- `DELETE_LOG_CAP` is disabled because report-only mode is uncapped

### `02 :: Evaluated State`

Confirm:

- `TARGET_DENY_LIST_VALID` contains only intended protected folders
- `TARGET_DENY_LIST_INVALID` is empty
- `EVALUATED_EXCLUDED_TARGETS` contains only expected exclusions
- `EFFECTIVE_DELETE_MODE` is `report-only`

### `08 :: Configuration Advisories`

This section appears only when there are advisories.

Review it if present:

- invalid deny-list names
- no included targets

### `04 :: Deletion Scope and Exclusions`

Confirm the explanation matches expected operating mode:

```text
Report-only mode; no files or directories are deleted.
```

### `05 :: Root Scan Summary`

Review:

- `roots_processed`
- `directories_visited`
- `files_scanned_regular`
- `old_file_candidates`
- `old_file_candidate_total_size`
- `empty_dir_candidates`
- skip counters

High skip counts are not automatically errors. They show paths retained for safety.

### `11 :: Excluded Items`

Review grouped reasons for retained paths:

- deny-listed targets
- unreadable paths
- non-directory entries
- non-regular entries
- cross-device entries
- young files not above threshold

### `12 :: Candidate Items`

In report-only mode this section lists would-delete files and would-delete directories.

Proceed only if candidate paths are expected.

---

## 6. Delete-mode procedure

Use delete mode only after report-only output has been reviewed.

### Step 1 — Confirm switch state

Confirm code constant:

```python
DELETE_ENABLED = True
```

### Step 2 — Confirm variables

```bash
airflow variables get MAX_LOG_AGE_DAYS
airflow variables get TARGET_DENY_LIST
airflow variables get DELETE_LOG_CAP
```

### Step 3 — Trigger delete run manually

```bash
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": false}'
```

### Step 4 — Review delete-mode log sections

Confirm:

- `00 :: Execution Context`
  - `DRY_RUN=false`
  - `EFFECTIVE_DELETE_MODE=delete`
  - `DELETE_LOG_CAP` is a positive integer
- `06 :: Action Outcome Summary`
  - `files_deleted`
  - `files_deleted_total_size`
  - `empty_dirs_deleted`
  - `action_skipped_items`
- `10 :: Action Skipped Items`
  - delete-time candidates preserved because they changed, disappeared, became unreadable, or failed deletion
- `11 :: Excluded Items`
  - scan/evaluation skips
- `12 :: Deleted Items`
  - deleted files and empty directories

### Step 5 — Record result

Capture:

- DAG run ID
- task try number
- start/end time
- `files_deleted`
- `files_deleted_total_size`
- `empty_dirs_deleted`
- `action_skipped_items`
- any abnormal skip reasons

---

## 7. Scheduled operation

The DAG is scheduled daily:

```text
SCHEDULE = "@daily"
```

Because current defaults are:

```python
DELETE_ENABLED = True
params={"dry_run": False}
```

scheduled runs operate in delete mode by default.

Use one of these controls if scheduled deletion is not desired:

- pause the DAG
- trigger manually with `dry_run=true`
- change `DELETE_ENABLED = False` and redeploy
- protect specific top-level targets through `TARGET_DENY_LIST`

---

## 8. Emergency stop

Use the fastest applicable option.

### Option A — Pause the DAG

```bash
airflow dags pause airflow_log_cleanup
```

### Option B — Force report-only behavior in code

Change:

```python
DELETE_ENABLED = False
```

Redeploy the DAG.

### Option C — Protect all top-level targets

List current top-level directories:

```bash
LOG_ROOT="$(airflow config get-value logging base_log_folder)"
find "$LOG_ROOT" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort
```

Set all names into `TARGET_DENY_LIST`:

```bash
airflow variables set TARGET_DENY_LIST 'scheduler,triggerer,dag_processor,worker'
```

### Option D — Use manual dry runs only

```bash
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": true}'
```

---

## 9. Stale lock procedure

The DAG uses a worker-local lock:

```text
/tmp/airflow_log_cleanup.lock
```

If a task returns:

```text
status = skipped_locked
```

perform this procedure.

### Step 1 — Confirm no active cleanup task is running

```bash
ps -ef | grep -E 'airflow|execute_cleanup|log_clean' | grep -v grep
```

Also check the Airflow UI for active `airflow_log_cleanup` runs.

### Step 2 — Inspect lock file

```bash
ls -l /tmp/airflow_log_cleanup.lock
cat /tmp/airflow_log_cleanup.lock
```

The file contains the PID written by the process that acquired the lock.

### Step 3 — Validate PID

```bash
PID="$(cat /tmp/airflow_log_cleanup.lock 2>/dev/null || true)"
test -n "$PID" && ps -fp "$PID"
```

### Step 4 — Remove only if safe

Remove the lock only if:

- no cleanup task is active
- the PID is not running or is unrelated
- no Airflow worker is currently executing this DAG task

```bash
rm -f /tmp/airflow_log_cleanup.lock
```

### Step 5 — Re-run report-only validation

```bash
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": true}'
```

---

## 10. Troubleshooting matrix

| Symptom | Likely cause | What to inspect | Action |
|---|---|---|---|
| `Configuration Failure` | Invalid variable, invalid root, unreadable root | `03 :: Configuration Failure` | Correct all listed fields and rerun. |
| `DELETE_LOG_CAP=0` error | Delete mode requires positive cap | `03 :: Configuration Failure` | Set `DELETE_LOG_CAP` to positive integer. |
| `MAX_LOG_AGE_DAYS=0` skip | Retention zero is unsafe | `03 :: Configuration Failure` | Set `MAX_LOG_AGE_DAYS` to positive integer. |
| No included targets | All top-level dirs denied or unavailable | `08 :: Configuration Advisories`, `11 :: Excluded Items` | Review `TARGET_DENY_LIST` and root contents. |
| Expected folder skipped | Symlink, non-directory, unreadable, or deny-listed | `02 :: Evaluated State`, `11 :: Excluded Items` | Fix permissions or deny-list. |
| Expected file retained | Young, non-regular, cross-device, unreadable | `05 :: Root Scan Summary`, `11 :: Excluded Items` | Confirm threshold and file type. |
| Candidate skipped at delete | File changed/disappeared before unlink | `10 :: Action Skipped Items` | Usually safe; rerun later if needed. |
| Directory not removed | Non-empty, changed, unreadable subtree | `10 :: Action Skipped Items`, `11 :: Excluded Items` | Inspect blocking entries. |
| Task timeout | Log tree too large for 5-minute timeout | Airflow task failure logs | Increase `TASK_EXECUTION_TIMEOUT` after review. |

---

## 11. Audit evidence checklist

For change/audit records, capture these sections:

- `00 :: Execution Context`
- `01 :: Configurable switches`
- `02 :: Evaluated State`
- `04 :: Deletion Scope and Exclusions`
- `05 :: Root Scan Summary`
- `06 :: Action Outcome Summary`
- `07 :: Overall Outcome`
- `08 :: Configuration Advisories`, if emitted
- `10 :: Action Skipped Items`
- `11 :: Excluded Items`
- `12 :: Candidate Items` or `12 :: Deleted Items`

Recommended evidence fields:

| Field | Source |
|---|---|
| DAG ID | Airflow UI / DAG run |
| DAG run ID | Airflow UI |
| Task try number | Airflow UI |
| Effective mode | `00`, `06` |
| Retention threshold | `00`, `01`, `02` |
| Target deny list | `01`, `02` |
| Roots processed | `05`, `07` |
| Old file candidates | `05`, `07` |
| Files deleted | `06`, `07` |
| Bytes deleted | `06`, `07` |
| Directories deleted | `06`, `07` |
| Action skipped items | `06`, `10` |
| Excluded items | `11` |

---

## 12. Safe operating patterns

### Conservative rollout

1. Deploy DAG paused.
2. Configure variables.
3. Trigger report-only run.
4. Review candidate items.
5. Adjust deny list if needed.
6. Trigger second report-only run.
7. Unpause or trigger delete mode.
8. Review delete results.

### Production daily operation

1. Let scheduled run execute.
2. Review summary sections.
3. Investigate abnormal skip spikes.
4. Monitor task duration against timeout.
5. Periodically confirm `TARGET_DENY_LIST` still matches deployed Airflow components.

### Incident response

1. Pause DAG.
2. Capture latest task log.
3. Capture Airflow Variables.
4. Capture `logging.base_log_folder`.
5. Inspect `10 :: Action Skipped Items` and `11 :: Excluded Items`.
6. Confirm whether any deletion occurred from `06 :: Action Outcome Summary`.
7. Do not remove lock files until active task state is confirmed.

---

## 13. Command reference

### Variables

```bash
airflow variables set MAX_LOG_AGE_DAYS 30
airflow variables set TARGET_DENY_LIST 'scheduler,triggerer,dag_processor'
airflow variables set DELETE_LOG_CAP 25

airflow variables get MAX_LOG_AGE_DAYS
airflow variables get TARGET_DENY_LIST
airflow variables get DELETE_LOG_CAP
```

### DAG operations

```bash
airflow dags list | grep airflow_log_cleanup
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": true}'
airflow dags trigger airflow_log_cleanup --conf '{"dry_run": false}'
airflow dags pause airflow_log_cleanup
airflow dags unpause airflow_log_cleanup
```

### Filesystem inspection

```bash
LOG_ROOT="$(airflow config get-value logging base_log_folder)"
find "$LOG_ROOT" -mindepth 1 -maxdepth 1 -printf '%y %p\n' | sort
du -sh "$LOG_ROOT"/*
```

### Lock inspection

```bash
ls -l /tmp/airflow_log_cleanup.lock
cat /tmp/airflow_log_cleanup.lock
rm -f /tmp/airflow_log_cleanup.lock
```

Remove the lock only after confirming no cleanup run is active.

---

## 14. Non-goals

This DAG does not provide:

- stale-lock auto-recovery
- delete rollback
- log compression
- archive-before-delete
- recursive deny-list policy
- metadata database cleanup
- object-storage cleanup
- retention by DAG ID, task ID, or run ID semantics
