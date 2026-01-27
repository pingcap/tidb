# Segmented Restore Plan (WIP)

## Plan (2026-01-23, current run)
1) Read `/Volumes/eXternal/Developer/seg-pitr-workload` to understand external test structure and expected integration points.
2) Integrate the external test into this repo (tests, scripts, or CI hooks) with minimal duplication.
3) Build BR and run the integrated test (requires unsandboxed access) to verify behavior.
4) Record outcomes, gaps, and follow-ups here.

## Plan (2026-01-22, current run)
1) Run `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh` (requires unsandboxed access) to confirm current failure state.
2) If it fails, trace the failing path in BR restore (especially segmented point restore) and implement the minimal fix.
3) Re-run the script to verify, then record outcomes and follow-up risks here.

## Plan (2025-xx-xx, current sprint)
1) Read `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh` to understand the failing step and expected output.
2) Run the script (requires unsandboxed access) to reproduce the current failure and capture the exact error text.
3) Trace where the `AddIndex` checksum / Total_kvs mismatch is generated and ensure the error message is formatted as `Error: AddIndex: Total_kvs mismatch: ...`.
4) Fix any segmented-restore gaps discovered along the way and update this doc with results.

## Current Status
- Ran `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh` (first run).
- Failure in second segment restore:
  - Error: `no base id map found from saved id or last restored PiTR`
  - Stack: `br/pkg/restore/log_client/client.go:1068` via `GetBaseIDMapAndMerge`.

## Hypothesis
- `tidb_pitr_id_map` has `restore_id` column; id maps are saved with restore_id from the first segment.
- Subsequent segments create a new restore_id, so loading by restore_id + restored_ts fails.

## Plan
1) Update id-map loading to fall back to the latest `restore_id` for a given `restored_ts` when loading base maps (start-ts path).
2) Re-run `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh` to find next failure.
3) Iterate on remaining segmented-restore gaps (e.g., tiflash replica handling, ingest index repair), recording findings here.

## Progress
- Implemented id-map fallback for previous segments in `br/pkg/restore/log_client/id_map.go`.
- Rebuilt BR (`make build_br`).
- Re-ran `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh` successfully.
  - Now it fails. Tiflash cases are added.
  - Error: exist table(s) have tiflash replica, please remove it before restore
- Added TiFlash recorder persistence across segments and only reset replicas on the final segment.
- Auto-treat `--last` as false when `--restored-ts` is set to a non-max TS and `--last` wasn't explicitly specified.
- Rebuilt BR and re-ran `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh`: **PASS**.
## Progress (2026-01-22)
- Reproduced failure: `AddIndex: Total_kvs mismatch` after segmented restore.
- Root cause: running ingest index repair in non-final segments writes new MVCC meta versions that block later log restore, leaving extra indexes.
- Fix:
  - Persist ingest recorder items across segments.
  - Skip ingest index repair until `--last=true`, then repair once.
- Rebuilt BR and re-ran `/Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh`: **PASS**.
## Progress (2026-01-xx)
- DRY cleanup: refactored PiTR ingest/tiflash item persistence to share JSON storage helpers and checkpoint fallback logic.
- Deduplicated ingest item counting by adding `ingestrec.CountItems` and reusing it in log client + stream restore logging.
## Progress (2026-02-xx)
- Unify PiTR ingest/tiflash item persistence with checkpoint storage type (table vs external).
- Added checkpoint-side PiTR item store and moved load/save logic off log backup storage.
- Guard segmented restore: non-final segments now require checkpoint storage.

## Progress (2026-01-23)
- Integrated the segmented PiTR workload into `br/tests/seg_pitr_workload` and added a new br test `br/tests/br_pitr_segmented_restore/run.sh`.
- Switched workload state storage to a JSON file (no sqlite dependency) and updated CLI flags accordingly.
- Added the new test to `br/tests/run_group_br_tests.sh` (G07).
