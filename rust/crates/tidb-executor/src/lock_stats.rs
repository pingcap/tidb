//! Go's ADMIN executors and the `SimpleExec` arms that touch no user table.
//!
//! `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX`: the `AdminStmt::AdminCheck` arm
//! of the session's ADMIN dispatch.
//!
//! The check itself lives in `crate::admin_check` (which mirrors Go
//! `pkg/executor/check_table_index.go`); this file is only the statement
//! seam -- name resolution, the output SHAPE, and the refusals.
//!
//! # The shapes, from TiDB's own recording
//!
//! From `tests/integrationtest/r/util/admin.result` and
//! `tests/integrationtest/r/executor/admin.result`, which are TiDB's oracle
//! and are only ever read here:
//!
//! - `admin check table t;` -- NO output at all: Go's `CheckTable` plan has
//!   an EMPTY schema, so the server sends an OK packet, not a zero-column
//!   result set (see [`admin_check_passed`]). Producing nothing is the whole
//!   success contract, which is exactly why the check underneath must be
//!   real: an `Ok` returned without reading a byte is indistinguishable from
//!   a passing check, and would be a success return that is not one.
//! - `admin check index t idx;` -- also no output; the same consistency
//!   check, narrowed to one index.
//! - `admin check index t idx (2, 4);` -- a RESULT SET: the indexed columns
//!   followed by `extra_handle`, in index order, for the rows whose handle
//!   falls in a half-open interval.
//!
//! # What this refuses, by name
//!
//! A table whose rows are not stored as index-bearing bytes has no index
//! entries to check against, so `ADMIN CHECK` over one is REFUSED rather than
//! answered OK. Same for the multi-table `ADMIN CHECK TABLE t1, t2` form,
//! which Go's own planner rejects.

//! Go `pkg/executor/lockstats`: the statement `LockStatsExec` and
//! `UnlockStatsExec` run, as the planner hands it over.

/// One table target retained from a parsed lock statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsLockTarget {
    /// Schema name after default-schema resolution.
    pub schema: String,
    /// Table name as written.
    pub table: String,
    /// Named partitions, if this is a partition-only operation.
    pub partitions: Vec<String>,
}

/// One parsed `LOCK STATS` or `UNLOCK STATS` statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStatsLockStatement {
    /// `true` for LOCK and `false` for UNLOCK.
    pub lock: bool,
    /// Targets in source order.
    pub targets: Vec<StatsLockTarget>,
}
