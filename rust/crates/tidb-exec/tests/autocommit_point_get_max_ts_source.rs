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

//! Both directions of Go's autocommit point-get start-timestamp shortcut.
//!
//! Go source under test, quoted so the guard set stays auditable from here:
//!
//! `pkg/sessiontxn/isolation/optimistic.go`
//! ```text
//! func (p *OptimisticTxnContextProvider) GetStmtReadTS() (uint64, error) {
//!     // If `math.MaxUint64` is used for point get optimization, it is not
//!     // necessary to activate the txn. Just return `math.MaxUint64` to save
//!     // the performance.
//!     if p.optimizeWithMaxTS {
//!         return math.MaxUint64, nil
//!     }
//!     return p.baseTxnContextProvider.GetStmtReadTS()
//! }
//!
//! func (p *OptimisticTxnContextProvider) AdviseOptimizeWithPlan(plan any) (err error) {
//!     if p.optimizeWithMaxTS || p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
//!         return nil
//!     }
//!     if p.txn != nil {
//!         return nil
//!     }
//!     ...
//!     ok = plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(p.sctx.GetSessionVars(), realPlan)
//! ```
//!
//! `pkg/planner/core/common_plans.go`
//! ```text
//! func IsPointGetWithPKOrUniqueKeyByAutoCommit(vars *variable.SessionVars, p base.Plan) bool {
//!     if !IsAutoCommitTxn(vars) { return false }
//!     switch v := p.(type) {
//!     case *physicalop.PhysicalTableReader:
//!         tableScan, ok := v.TablePlans[0].(*physicalop.PhysicalTableScan)
//!         if !ok { return false }
//!         isPointRange := len(tableScan.Ranges) == 1 &&
//!             tableScan.Ranges[0].IsPointNonNullable(vars.StmtCtx.TypeCtx())
//!         if !isPointRange { return false }
//!         pkLength := 1
//!         if tableScan.Table.IsCommonHandle { ... }
//!         return len(tableScan.Ranges[0].LowVal) == pkLength
//!     ...
//! func IsAutoCommitTxn(vars *variable.SessionVars) bool {
//!     return vars.IsAutocommit() && !vars.InTxn()
//! }
//! ```
//!
//! The asymmetry is the whole point: `MaxUint64` reads the latest committed
//! version and therefore ignores snapshot isolation. A test that only proved
//! the shortcut is taken would pass just as well for an implementation that
//! took it everywhere — which would silently return rows from the wrong
//! snapshot inside a transaction. So every case below states which direction
//! it pins.

#![allow(missing_docs)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{CancelHandle, QueryDispatch, QueryTransport, TimestampSource, TransportRequest};
use tidb_exec::real_tikv_read::{RealTiKvReadSession, MAX_TS_POINT_GET_SNAPSHOT};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable, ReadOnlyScanPlan};

#[derive(Clone, Debug)]
struct CountingTimestampSource {
    values: Rc<RefCell<VecDeque<u64>>>,
    calls: Rc<Cell<usize>>,
}

impl CountingTimestampSource {
    fn new(values: impl IntoIterator<Item = u64>) -> Self {
        Self {
            values: Rc::new(RefCell::new(values.into_iter().collect())),
            calls: Rc::new(Cell::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.get()
    }
}

impl TimestampSource for CountingTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.calls.set(self.calls.get() + 1);
        Ok(self
            .values
            .borrow_mut()
            .pop_front()
            .expect("a statement asked for a timestamp the pin did not budget"))
    }
}

struct EmptyResponse;

impl QueryResponse for EmptyResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(None)
    }

    fn close(&mut self) {}
}

#[derive(Default)]
struct TransportState {
    start_timestamps: RefCell<Vec<u64>>,
}

struct CapturingTransport {
    state: Rc<TransportState>,
}

impl QueryTransport for CapturingTransport {
    type Response = EmptyResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        self.state
            .start_timestamps
            .borrow_mut()
            .push(request.metadata().start_ts);
        Ok(Some(EmptyResponse))
    }
}

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_not_null("balance", 8),
        ],
    )
}

fn new_session(
    timestamps: CountingTimestampSource,
    state: &Rc<TransportState>,
) -> RealTiKvReadSession<CapturingTransport, CountingTimestampSource> {
    RealTiKvReadSession::new(
        configured_table(),
        CapturingTransport {
            state: Rc::clone(state),
        },
        timestamps,
    )
}

fn plan(sql: &str) -> ReadOnlyScanPlan {
    ReadOnlyScanPlan::lower(sql, &configured_table()).expect("the pin's SQL lowers")
}

/// Direction one: an autocommit point get on the clustered primary key takes
/// ZERO timestamps and still issues exactly one request, at `MaxUint64`.
#[test]
fn autocommit_point_get_on_the_primary_key_takes_no_timestamp() {
    let timestamps = CountingTimestampSource::new([]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    let query = session
        .execute("SELECT balance FROM accounts WHERE id = 7")
        .unwrap();

    assert_eq!(query.snapshot_ts(), Some(MAX_TS_POINT_GET_SNAPSHOT));
    assert_eq!(timestamps.calls(), 0, "a point get must not consult PD");
    assert_eq!(
        *state.start_timestamps.borrow(),
        [MAX_TS_POINT_GET_SNAPSHOT]
    );
}

/// Direction one, still on the shortcut: a residual non-handle predicate over
/// a point handle keeps it. Go checks only `tableScan.Ranges`, so a Selection
/// above the scan does not disqualify the plan.
#[test]
fn point_handle_with_a_residual_selection_keeps_the_shortcut() {
    let timestamps = CountingTimestampSource::new([]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    let query = session
        .execute("SELECT id FROM accounts WHERE id = 7 AND balance > 10")
        .unwrap();

    assert_eq!(query.snapshot_ts(), Some(MAX_TS_POINT_GET_SNAPSHOT));
    assert_eq!(timestamps.calls(), 0);
}

/// Direction two, guard `len(Ranges) == 1`: a plan with more than one range is
/// not the single-row read the shortcut assumes and must still pay.
///
/// The two-POINT-range shape Go also rejects here (`id IN (7, 9)`,
/// `id = 7 OR id = 9`) cannot be built at this tier at all: lowering refuses
/// `OR` and `IN` with `UnsupportedPredicate(BooleanOperator)`, so no
/// `ReadOnlyScanPlan` ever carries two point ranges. `id != 0` is the
/// reachable multi-range shape, and it exercises the same `len == 1` arm.
#[test]
fn a_multi_range_plan_still_takes_a_timestamp() {
    let timestamps = CountingTimestampSource::new([501]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    let query = session
        .execute("SELECT id FROM accounts WHERE id != 0")
        .unwrap();

    assert_eq!(query.snapshot_ts(), Some(501));
    assert_eq!(timestamps.calls(), 1);
    assert_eq!(*state.start_timestamps.borrow(), [501]);
}

/// Direction two, guard `IsPointNonNullable`: a non-point range — including a
/// range that happens to cover one row — is not a point get.
#[test]
fn a_range_scan_still_takes_a_timestamp() {
    let timestamps = CountingTimestampSource::new([601, 602]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    let bounded = session
        .execute("SELECT id FROM accounts WHERE id >= 7 AND id <= 8")
        .unwrap();
    assert_eq!(bounded.snapshot_ts(), Some(601));

    let full = session.execute("SELECT id FROM accounts").unwrap();
    assert_eq!(full.snapshot_ts(), Some(602));

    assert_eq!(timestamps.calls(), 2);
    assert_eq!(*state.start_timestamps.borrow(), [601, 602]);
}

/// Direction two, guard `IsPointNonNullable` again: a predicate on a
/// non-handle column pins no handle at all, so the plan scans the table.
#[test]
fn a_point_predicate_on_a_non_handle_column_still_takes_a_timestamp() {
    let timestamps = CountingTimestampSource::new([701]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    let query = session
        .execute("SELECT id FROM accounts WHERE balance = 7")
        .unwrap();

    assert_eq!(query.snapshot_ts(), Some(701));
    assert_eq!(timestamps.calls(), 1);
}

/// Direction two, guard `IsAutoCommitTxn`: the same point get running inside
/// an explicit transaction reads at the transaction's own pinned `start_ts`,
/// never at `MaxUint64`. The session routes an open transaction through
/// `execute_plan_at_snapshot`, which is the seam this pins; a `MaxUint64` read
/// here would silently break repeatable read for every later statement in the
/// transaction.
#[test]
fn the_same_point_get_inside_a_transaction_reads_at_the_transaction_snapshot() {
    let timestamps = CountingTimestampSource::new([]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    // One timestamp acquired for the transaction, then reused by every
    // statement in it, exactly as the session's explicit-transaction path does.
    let transaction_start_ts = 900;
    let cancellation = std::sync::Arc::new(CancelHandle::default());
    let first = session
        .execute_plan_at_snapshot(
            plan("SELECT balance FROM accounts WHERE id = 7"),
            transaction_start_ts,
            std::sync::Arc::clone(&cancellation),
        )
        .unwrap();
    assert_eq!(first.snapshot_ts(), Some(transaction_start_ts));

    let second = session
        .execute_plan_at_snapshot(
            plan("SELECT balance FROM accounts WHERE id = 7"),
            transaction_start_ts,
            cancellation,
        )
        .unwrap();
    assert_eq!(second.snapshot_ts(), Some(transaction_start_ts));

    assert_eq!(timestamps.calls(), 0, "the transaction pinned its own ts");
    assert_eq!(
        *state.start_timestamps.borrow(),
        [transaction_start_ts, transaction_start_ts],
        "no statement in a transaction may read at MaxUint64"
    );
}

/// Direction two, guard set that this node refuses rather than approximates:
/// a stale read never reaches the shortcut because lowering rejects
/// `AS OF TIMESTAMP` outright. Go gates it with `isBeginStmtWithStaleRead`;
/// here the plan cannot exist, which is the stronger property.
#[test]
fn a_stale_read_never_produces_a_plan_that_could_take_the_shortcut() {
    let error = ReadOnlyScanPlan::lower(
        "SELECT balance FROM accounts AS OF TIMESTAMP '2026-01-01 00:00:00' WHERE id = 7",
        &configured_table(),
    )
    .expect_err("stale read is refused before any plan exists");
    assert!(
        format!("{error}").to_lowercase().contains("stale"),
        "unexpected refusal: {error}"
    );
}

/// Resolves what a read at `start_ts` sees, the way TiKV's MVCC reader does:
/// the newest version whose `commit_ts` is at or below `start_ts`.
///
/// This is a MODEL of TiKV, not TiKV. It exists so the visibility consequence
/// of `MaxUint64` is stated executably rather than only asserted in prose.
/// Confirming that a real TiKV agrees needs a cluster run; the transport at
/// this tier stores no versions.
fn visible_at(versions: &[(u64, &'static str)], start_ts: u64) -> Option<&'static str> {
    versions
        .iter()
        .filter(|(commit_ts, _)| *commit_ts <= start_ts)
        .max_by_key(|(commit_ts, _)| *commit_ts)
        .map(|(_, value)| *value)
}

/// The concurrency meaning of `MaxUint64`, which is what makes it both the
/// win and the hazard.
///
/// A row another session commits BETWEEN two autocommit point reads must
/// become visible to the second one — that is exactly what reading the latest
/// committed version buys. The same pair of reads inside one transaction must
/// NOT see it, because they share one pinned `start_ts`. An implementation
/// that took the shortcut unconditionally would pass the first half and
/// silently break the second.
#[test]
fn a_concurrent_commit_is_visible_between_autocommit_reads_but_not_inside_a_transaction() {
    let timestamps = CountingTimestampSource::new([]);
    let state = Rc::new(TransportState::default());
    let mut session = new_session(timestamps.clone(), &state);

    // Another session commits `after` at commit_ts 1000, between the reads.
    let versions = [(100_u64, "before"), (1000_u64, "after")];

    let first = session
        .execute("SELECT balance FROM accounts WHERE id = 7")
        .unwrap();
    let second = session
        .execute("SELECT balance FROM accounts WHERE id = 7")
        .unwrap();
    let autocommit_reads = state.start_timestamps.borrow().clone();
    assert_eq!(
        autocommit_reads,
        [MAX_TS_POINT_GET_SNAPSHOT, MAX_TS_POINT_GET_SNAPSHOT]
    );
    assert_eq!(first.snapshot_ts(), Some(MAX_TS_POINT_GET_SNAPSHOT));
    assert_eq!(second.snapshot_ts(), Some(MAX_TS_POINT_GET_SNAPSHOT));
    assert_eq!(
        visible_at(&versions, autocommit_reads[1]),
        Some("after"),
        "an autocommit point read must observe a commit that landed before it"
    );

    // The same pair inside one transaction, whose start_ts predates the commit.
    let state = Rc::new(TransportState::default());
    let mut in_transaction = new_session(timestamps.clone(), &state);
    let transaction_start_ts = 500;
    for _ in 0..2 {
        in_transaction
            .execute_plan_at_snapshot(
                plan("SELECT balance FROM accounts WHERE id = 7"),
                transaction_start_ts,
                std::sync::Arc::new(CancelHandle::default()),
            )
            .unwrap();
    }
    let transaction_reads = state.start_timestamps.borrow().clone();
    assert_eq!(
        transaction_reads,
        [transaction_start_ts, transaction_start_ts]
    );
    assert_eq!(
        visible_at(&versions, transaction_reads[1]),
        Some("before"),
        "a read inside a transaction must NOT observe a later commit"
    );
    assert_eq!(timestamps.calls(), 0);
}

/// The plan-shape predicate on its own, so a future caller that reuses it
/// inherits the same answers.
#[test]
fn the_plan_shape_predicate_matches_the_guard_it_ports() {
    assert!(plan("SELECT id FROM accounts WHERE id = 7").is_point_get_on_handle());
    assert!(plan("SELECT id FROM accounts WHERE 7 = id").is_point_get_on_handle());
    assert!(!plan("SELECT id FROM accounts WHERE id != 0").is_point_get_on_handle());
    assert!(!plan("SELECT id FROM accounts WHERE id >= 7 AND id <= 8").is_point_get_on_handle());
    assert!(!plan("SELECT id FROM accounts").is_point_get_on_handle());
    assert!(!plan("SELECT id FROM accounts WHERE balance = 7").is_point_get_on_handle());
    // A contradiction has no ranges at all; it short-circuits before either
    // branch, and must never report itself as a point get.
    assert!(!plan("SELECT id FROM accounts WHERE id > 10 AND id < 0").is_point_get_on_handle());
}
