// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! [`TableAccess`]: the one negotiation between the driver and a base-table
//! source, and the one place its obligations are written down.
//!
//! # What is being negotiated
//!
//! The driver plans a `SELECT` top-down and then offers the source below it
//! three things it would rather not do itself, in this order:
//!
//! 1. a **kept-column set** ([`TableAccess::accept_column_prune`]) -- Go's
//!    `rule_column_pruning.go`;
//! 2. a set of **pushed conjuncts** ([`TableAccess::accept_scan_filter`]) --
//!    Go's predicate push-down into the cop task;
//! 3. a **row cap** ([`TableAccess::accept_scan_limit`]) -- Go's `Limit`
//!    inside the cop task below the scan.
//!
//! Each offer is answered by the source alone, with a `bool`, and each answer
//! is a promise the driver then *relies on*: an accepted conjunct is removed
//! from the `Selection` above, an accepted prune renumbers the `FROM` scope,
//! an accepted cap lets the `LimitExec` above stop asking. There is no
//! "did it work?" flag anywhere in the driver, and there does not need to be:
//! every method here is **fail-closed**. The default refuses, and refusing is
//! always correct -- it only costs work the operator above was doing anyway.
//!
//! # The staged-row promise (the load-bearing rule)
//!
//! This is the obligation that every source shares and that no source may
//! quietly reinterpret, so it is stated once, here.
//!
//! A source's rows are not only its storage engine's rows. Inside an explicit
//! transaction, [`ClusterTableStorage`](tidb_store_driver_txn::cluster_storage::ClusterTableStorage)
//! merges the session's staged mutation buffer into the same key-ordered
//! stream the snapshot produces (Go's `MemBuffer` in front of `kv.Snapshot`,
//! and Go's `UnionScan` above a coprocessor reader). A row this very statement's
//! transaction wrote appears in that stream **without having passed through
//! any coprocessor, index range, or handle lookup**.
//!
//! Therefore:
//!
//! > A source that answers `true` to [`TableAccess::accept_scan_filter`]
//! > promises to apply **every** pushed conjunct to **every** row it emits --
//! > snapshot rows, remotely filtered rows, and client-side merged staged rows
//! > alike. A source that answers `true` to
//! > [`TableAccess::accept_scan_limit`] promises to stop after `cap` rows *of
//! > that same merged stream*, not of any half of it.
//!
//! The consequence for a remote (coprocessor) source is concrete and has bitten
//! already: a predicate lowered into the request narrows what crosses the wire,
//! but the source must still evaluate the full pushed predicate locally on the
//! merged stream, and a cap may only travel with the request when nothing is
//! staged. Remote evaluation is thus always a *performance* choice and never a
//! semantic one -- see `tidb_executor::remote_scan`.
//!
//! A source that cannot make one of these promises does not implement that
//! method: the fail-closed default answers `false` and the operator above keeps
//! doing the work.
//!
//! # How a source is reached
//!
//! [`Executor::table_access`](crate::Executor::table_access) returns
//! `None` by default, so an operator is offered nothing unless it opts in by
//! returning itself. That is the same fail-closed shape one level up: a new
//! operator cannot accidentally inherit a promise it never made. A transparent
//! wrapper (the `EXPLAIN ANALYZE` meter) forwards to its child, because
//! metering must not change what runs.

use crate::predicate_pushdown::PushedScanFilter;
use tidb_sessionctx::StmtContext;

/// A base-table source that can take over work from the operators above it.
///
/// Every method is fail-closed; the promises each acceptance makes -- and in
/// particular the staged-row rule that applies to all of them -- are the
/// module doc above.
pub trait TableAccess {
    /// Go `RuntimeStatsColl.GetCopCountAndRows` for a live remote reader.
    fn cop_count_and_rows(&self) -> Option<(u64, u64)> {
        None
    }

    /// Offers `filter` to this source, as Go's predicate push-down offers a
    /// conjunct to the node below it.
    ///
    /// Returning `true` is a promise the driver relies on to *remove* those
    /// conjuncts from the `Selection` above: the source must apply every one
    /// of them to every row it emits, staged rows included (see the module
    /// doc). Offers are cumulative: physical planning may reach the same leaf
    /// again with predicates derived by a parent join, and accepting that
    /// later offer must preserve every conjunct accepted earlier. This mirrors
    /// Go `pkg/planner/core/operator/logicalop/logical_join.go::DeriveOtherConditions`
    /// deriving child conditions and
    /// `pkg/planner/core/find_best_task.go::addPushedDownSelection4PhysicalTableScan`
    /// retaining the table plan's accumulated filter condition. Replacing the
    /// old filter would leave its already-removed `Selection` unevaluated.
    /// A source that cannot promise this leaves the default `false` and the
    /// whole `WHERE` stays where it was.
    ///
    /// See [`crate::predicate_pushdown`] for the split rule and the reasoning.
    fn accept_scan_filter(&mut self, filter: &PushedScanFilter, ctx: &StmtContext) -> bool {
        let _ = (filter, ctx);
        false
    }
}
