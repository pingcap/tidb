# MV Scheduling (pkg/mvs) Design and Implementation Notes

This document summarizes the latest design and implementation skeleton under `pkg/mvs/`.
It reflects the current code, including TODOs and gaps.

## Scope and Goals

- Provide a scheduling skeleton for Materialized Views (MV) and MV Logs (MVLOG).
- Refresh in-memory task queues from system tables.
- Distribute tasks across TiDB nodes with consistent hashing.
- React to MV/MVLOG DDL changes by triggering a refresh.

## Package Layout

- `pkg/mvs/utils/tasks.go`: MVJobsManager, MV/MVLOG task definitions, refresh loop, SQL metadata loading.
- `pkg/mvs/utils/server_maintainer.go`: ServerConsistentHash, node discovery, node assignment.
- `pkg/mvs/utils/consistenthash.go`: Consistent hash ring implementation.
- `pkg/mvs/utils/utils.go`: Notifier (multi-producer single-consumer).
- `pkg/mvs/utils/ddl_listener.go`: DDL event listener registration and notification.

## Core Components

### MVJobsManager

- Holds a SQL executor (`sessionctx.Context`) and two in-memory queues:
    - MV queue: `pending` map and `orderByNext` slice sorted by `nextRefresh`.
    - MVLOG queue: `pending` map and `orderByNext` slice sorted by `nextPurge`.
- Starts a background loop (`Start`) and stops it (`Close`) via an atomic `running` flag.
- Refreshes queue data with `RefreshAll`.

### mv / mvLog

- `mv`: fields for refresh interval and next refresh time.
- `mvLog`: fields for purge interval and next purge time.
- `refresh()` and `purge()` are stubbed; TODOs describe intended transactional flow.

### Notifier

- `Notifier` provides an MPSC wake-up channel to reduce redundant wakeups.
- Used by DDL listener to wake the MVJobsManager loop.

### ServerConsistentHash / ConsistentHash

- `ConsistentHash` implements a CRC32-based ring with virtual nodes.
- `ServerConsistentHash` uses the ring to map task IDs to TiDB nodes.
- `Available(key)` is used by MVJobsManager to keep only tasks for the current node.

## Metadata Tables (as used by code)

The current refresh logic reads from:

- `mysql.tidb_mlogs` joined with `mysql.tidb_mlog_purge`
- `mysql.tidb_mviews` joined with `mysql.tidb_mview_refresh`

DDL examples for MV/MVLOG tables are documented as comments in `tasks.go`.

## Runtime Flow

### Start/Close

1. `Start` uses `running` to ensure a single loop.
2. A ticker fires every second and triggers `RefreshAll` if data is stale.
3. DDL events wake the loop via the Notifier.
4. `Close` flips `running` to false and wakes the loop to exit.

### RefreshAll

1. `refreshAllTiDBMLogPurge`:
   - Reads MVLOG purge metadata.
   - Computes `nextPurge`.
   - Filters by `ServerConsistentHash.Available`.
   - Updates `mvLog` queue (map + ordered slice).
2. `refreshAllTiDBMViews`:
   - Reads MV refresh metadata.
   - Computes `nextRefresh`.
   - Filters by `ServerConsistentHash.Available`.
   - Updates `mv` queue (map + ordered slice).

### Execution (stub)

- `exec()` collects due MVLOGs and MVs from ordered queues.
- Actual execution logic is TODO.

## DDL Event Trigger

- `RegisterMVDDLEventHandler` registers a handler in the DDL notifier.
- On MV/MVLOG DDL events, it calls `Notifier.Wake()` to trigger refresh.

## Consistent Hashing Strategy

- Task assignment is based on task ID (MV/MVLOG ID).
- The goal is stable assignment with minimal remapping on node changes.

## Current Gaps / TODOs

- `mv.refresh()` / `mvLog.purge()` are not implemented (only pseudocode).
- `exec()` is not wired to periodic execution.
- `ServerConsistentHash.Refresh` does not fully update the `servers` map.
- DDL listener includes a TODO for initializing `mvs`.

## Suggested Next Steps (if implementing)

- Implement transaction logic in `refresh()` and `purge()`.
- Drive `exec()` from the background loop.
- Complete node refresh logic in `ServerConsistentHash`.
- Initialize and wire `MVJobsManager` in DDL listener or domain startup path.
