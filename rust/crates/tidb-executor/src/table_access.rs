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
//! transaction, [`ClusterTableStorage`](crate::cluster_storage::ClusterTableStorage)
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
//! semantic one -- see [`crate::remote_scan`].
//!
//! A source that cannot make one of these promises does not implement that
//! method: the fail-closed default answers `false` and the operator above keeps
//! doing the work.
//!
//! # How a source is reached
//!
//! [`Executor::table_access`](crate::executor::Executor::table_access) returns
//! `None` by default, so an operator is offered nothing unless it opts in by
//! returning itself. That is the same fail-closed shape one level up: a new
//! operator cannot accidentally inherit a promise it never made. A transparent
//! wrapper (the `EXPLAIN ANALYZE` meter) forwards to its child, because
//! metering must not change what runs.

use std::cell::Cell;
use std::rc::Rc;

use crate::predicate_pushdown::PushedScanFilter;
use crate::remote_scan::{PushdownPartialAggregate, PushdownTopN};
use crate::StmtContext;

/// A base-table source that can take over work from the operators above it.
///
/// Every method is fail-closed; the promises each acceptance makes -- and in
/// particular the staged-row rule that applies to all of them -- are the
/// module doc above.
pub trait TableAccess {
    /// Records the physical scan estimate selected by the access-path coster.
    /// It changes no rows and exists so later operator negotiation can make
    /// the same partial/final aggregation choice as the optimizer.
    fn accept_scan_estimate(&mut self, rows: f64) {
        let _ = rows;
    }

    /// Offers the scan a real TiKV partial aggregation. Returning `true`
    /// changes the source schema to the one partial-result column and promises
    /// that a local fallback computes exactly the same partial rows.
    fn accept_partial_aggregate(
        &mut self,
        aggregate: &PushdownPartialAggregate,
        ctx: &StmtContext,
    ) -> bool {
        let _ = (aggregate, ctx);
        false
    }

    /// Offers `filter` to this source, as Go's predicate push-down offers a
    /// conjunct to the node below it.
    ///
    /// Returning `true` is a promise the driver relies on to *remove* those
    /// conjuncts from the `Selection` above: the source must apply every one
    /// of them to every row it emits, staged rows included (see the module
    /// doc). A source that cannot promise that leaves the default `false` and
    /// the whole `WHERE` stays where it was.
    ///
    /// See [`crate::predicate_pushdown`] for the split rule and the reasoning.
    fn accept_scan_filter(&mut self, filter: &PushedScanFilter, ctx: &StmtContext) -> bool {
        let _ = (filter, ctx);
        false
    }

    /// Offers a projection that must run only after an already accepted scan
    /// filter. `keep` indexes the scan row the filter sees, in final output
    /// order.
    ///
    /// Returning `true` promises two equivalent paths: a local scan filters
    /// the wider row before projecting it, while a remote scan returns the
    /// narrow row only when every predicate was evaluated in the coprocessor.
    /// The source schema must immediately describe the projected row.
    fn accept_post_filter_projection(&mut self, keep: &[usize]) -> bool {
        let _ = keep;
        false
    }

    /// Offers a coprocessor TopN hint. The caller keeps an equivalent local
    /// TopN, so refusal changes only where rows are reduced, never the answer.
    fn accept_remote_topn(&mut self, topn: &PushdownTopN) -> bool {
        let _ = topn;
        false
    }

    /// Offers this source a row cap, as Go's `LIMIT` push-down puts a `Limit`
    /// inside the cop task below the scan (captured: `Limit_12 | cop[tikv] |
    /// offset:0, count:3` under `IndexRangeScan_11`).
    ///
    /// `cap` is `offset + count`, because the offset rows are consumed above
    /// and must still be produced -- exactly what Go's cop-side `Limit`
    /// carries (`limit 2, 3` lowers to `offset:0, count:5`).
    ///
    /// Returning `true` promises the source stops after `cap` rows *that it
    /// itself emits*. The driver may therefore only offer a cap when every
    /// filter the query applies is applied at or below this source, and when
    /// the row order this source produces is the order the `LIMIT` selects
    /// from.
    fn accept_scan_limit(&mut self, cap: u64) -> bool {
        let _ = cap;
        false
    }

    /// Offers Go's `PhysicalIndexLookUpReader.PushedLimit` to an ordered
    /// non-covering index lookup. Unlike a cop scan cap, the SQL offset is
    /// consumed from the index handle stream before table lookup tasks are
    /// built, and only `count` table rows may be requested afterward.
    fn accept_embedded_lookup_limit(&mut self, offset: u64, count: u64) -> bool {
        let _ = (offset, count);
        false
    }

    /// The live count of rows this source read from storage, before any
    /// filter it accepted -- `TableFullScan`'s `actRows`, which a pushed
    /// predicate must not change. `None` for anything that is not such a
    /// scan.
    fn scanned_rows_counter(&self) -> Option<Rc<Cell<u64>>> {
        None
    }

    /// Offers this source the clustered-handle ranges the `WHERE` implies, as
    /// Go's `deriveTablePathStats` gives a `PhysicalTableScan` its
    /// `ranger.BuildTableRange` ranges and turns a `TableFullScan` into a
    /// `TableRangeScan`.
    ///
    /// This offer is unlike the others above: it is not a promise to take
    /// over any *evaluation*. The `WHERE` those ranges were derived from
    /// stays in the pipeline above the source either way, so a source that
    /// accepts still returns every row the statement admits -- it just does
    /// not read the records that lie outside the ranges. A source is free to
    /// read a SUPERSET of them (that is the ordinary answer for a shape it
    /// cannot encode); it must never read less.
    ///
    /// `ranges` is over the single handle column, ascending and disjoint, in
    /// [`crate::handle_range`]'s form. An EMPTY slice is the contradictory
    /// `WHERE` no handle satisfies, and reads nothing.
    fn accept_handle_ranges(&mut self, ranges: &[crate::kv_table::IndexRange]) -> bool {
        let _ = ranges;
        false
    }

    /// Offers this source the PARTITIONS a `WHERE` proved it has to read, as
    /// Go's `PartitionProcessor` keeps only the surviving partitions'
    /// `DataSource`s under its union.
    ///
    /// Like [`TableAccess::accept_handle_ranges`] this takes over no
    /// evaluation: the `WHERE` stays above, so accepting only means the
    /// source stops READING partitions that cannot hold a matching row.
    /// Reading a SUPERSET is always allowed; reading less than `ids` is what
    /// silently loses rows, so a source that cannot restrict itself exactly
    /// leaves the default `false`.
    ///
    /// `ids` are physical partition ids of this source's own table, ascending
    /// (see [`crate::partition_pruning`]).
    fn accept_partition_pruning(&mut self, ids: &[i64]) -> bool {
        let _ = ids;
        false
    }

    /// Offers this source the chance to emit only the columns at `keep`
    /// (offsets into its current output row, in the requested output order),
    /// as Go's column pruning or cop projection narrows a `DataSource`'s
    /// schema.
    ///
    /// Returning `true` is a promise the driver relies on to renumber the
    /// `FROM` scope: from the next `open` on, every row this source emits
    /// must be exactly `keep.len()` wide and hold `keep`'s columns in
    /// `keep`'s order, and
    /// [`Executor::schema`](crate::executor::Executor::schema) must already
    /// describe that narrow row. A source that cannot promise it leaves the
    /// default `false` and the driver keeps the full-width scope unchanged.
    ///
    /// See [`crate::column_prune`] for the eligibility gate and the reasoning.
    fn accept_column_prune(&mut self, keep: &[usize]) -> bool {
        let _ = keep;
        false
    }

    /// Tells this source that the order it WALKS its access object in is the
    /// order the statement asked for -- Go's `keep order:true` on a scan,
    /// which is `IndexLookUpExecutor.keepOrder` at run time.
    ///
    /// Unlike the offers above this one takes over no work and can be refused
    /// with no consequence at all: it does not license the driver to drop a
    /// `Sort`, and this tier's `ORDER BY` always keeps its `SortExec`. What it
    /// buys is TIE ORDER. Go's unordered double read answers in HANDLE order
    /// (see [`crate::access_path::IndexRangeSourceExec`]) and its ordered one
    /// answers in index order; a source told nothing would give handle order
    /// for both, and rows that tie on the `ORDER BY` key would then leave in
    /// the wrong order under a stable sort.
    ///
    /// A source with only one order to give ignores this and stays correct,
    /// which is why the default answers `false`.
    fn accept_keep_order(&mut self, descending: bool) -> bool {
        let _ = descending;
        false
    }
}
