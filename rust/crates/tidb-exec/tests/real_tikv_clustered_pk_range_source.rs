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

#![allow(missing_docs)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use prost::Message;
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    signed_handle_ranges_to_kv_ranges, QueryDispatch, QueryTransport, RequestKeyRange,
    SignedHandleRange, TimestampSource, TransportRequest,
};
use tidb_exec::real_tikv_read::{RealTiKvPlanExecutorKind, RealTiKvReadSession};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
use tidb_proto::tipb::{DagRequest, ExecType};

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
            .expect("one timestamp per nonempty range plan"))
    }
}

struct EmptyResponse;

impl QueryResponse for EmptyResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(None)
    }

    fn close(&mut self) {}
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestObservation {
    start_ts: u64,
    ranges: Vec<RequestKeyRange>,
    dag_data: Vec<u8>,
}

#[derive(Default)]
struct TransportState {
    sends: Cell<usize>,
    requests: RefCell<Vec<RequestObservation>>,
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
        let metadata = request.metadata();
        let ranges = metadata
            .key_ranges
            .as_ref()
            .expect("clustered table request carries key ranges");
        assert!(ranges.is_non_partitioned());
        let [ranges] = ranges.partitions() else {
            panic!("one non-partitioned range group")
        };
        self.state.sends.set(self.state.sends.get() + 1);
        self.state.requests.borrow_mut().push(RequestObservation {
            start_ts: metadata.start_ts,
            ranges: ranges.clone(),
            dag_data: metadata.data.clone().expect("DAG request bytes"),
        });
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

fn expected_ranges(ranges: &[(i64, i64)]) -> Vec<RequestKeyRange> {
    let ranges = ranges
        .iter()
        .map(|(low, high)| SignedHandleRange::inclusive(*low, *high).unwrap())
        .collect::<Vec<_>>();
    signed_handle_ranges_to_kv_ranges(42, &ranges)
}

fn evidence_ranges(
    query: &tidb_exec::real_tikv_read::RealTiKvQuery,
) -> Vec<(i64, i64, bool, bool)> {
    query
        .plan_evidence()
        .handle_ranges()
        .iter()
        .map(|range| {
            (
                range.low(),
                range.high(),
                range.low_exclude(),
                range.high_exclude(),
            )
        })
        .collect()
}

#[test]
fn planner_ranges_are_the_only_request_ranges_and_residual_selection_survives() {
    let timestamps = CountingTimestampSource::new([101, 102, 103]);
    let state = Rc::new(TransportState::default());
    let mut session = RealTiKvReadSession::new(
        configured_table(),
        CapturingTransport {
            state: Rc::clone(&state),
        },
        timestamps.clone(),
    );

    let full = session.execute("SELECT id FROM accounts").unwrap();
    assert_eq!(full.snapshot_ts(), Some(101));
    assert_eq!(full.plan_evidence().handle_range_count(), 1);
    assert_eq!(evidence_ranges(&full), [(i64::MIN, i64::MAX, false, false)]);

    let narrowed = session
        .execute("SELECT id FROM accounts WHERE id >= -5 AND id < 9")
        .unwrap();
    assert_eq!(narrowed.snapshot_ts(), Some(102));
    assert_eq!(narrowed.plan_evidence().handle_range_count(), 1);
    assert_eq!(evidence_ranges(&narrowed), [(-5, 8, false, false)]);
    assert_eq!(
        narrowed.plan_evidence().executor_kinds(),
        [RealTiKvPlanExecutorKind::TableScan]
    );
    assert_eq!(narrowed.plan_evidence().predicate_count(), 0);

    let split_with_residual = session
        .execute("SELECT id FROM accounts WHERE id != 0 AND balance > 10")
        .unwrap();
    assert_eq!(split_with_residual.snapshot_ts(), Some(103));
    assert_eq!(split_with_residual.plan_evidence().handle_range_count(), 2);
    assert_eq!(
        evidence_ranges(&split_with_residual),
        [(i64::MIN, -1, false, false), (1, i64::MAX, false, false),]
    );
    assert_eq!(
        split_with_residual.plan_evidence().executor_kinds(),
        [
            RealTiKvPlanExecutorKind::TableScan,
            RealTiKvPlanExecutorKind::Selection,
        ]
    );
    assert_eq!(split_with_residual.plan_evidence().predicate_count(), 1);

    assert_eq!(timestamps.calls(), 3);
    assert_eq!(state.sends.get(), 3);
    let requests = state.requests.borrow();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.start_ts)
            .collect::<Vec<_>>(),
        [101, 102, 103]
    );
    assert_eq!(requests[0].ranges, expected_ranges(&[(i64::MIN, i64::MAX)]));
    assert_eq!(requests[1].ranges, expected_ranges(&[(-5, 8)]));
    assert_eq!(
        requests[2].ranges,
        expected_ranges(&[(i64::MIN, -1), (1, i64::MAX)])
    );
    let narrowed_dag = DagRequest::decode(requests[1].dag_data.as_slice()).unwrap();
    assert_eq!(narrowed_dag.executors.len(), 1);
    assert_eq!(
        narrowed_dag.executors[0].tp,
        Some(ExecType::TypeTableScan as i32)
    );
    let residual_dag = DagRequest::decode(requests[2].dag_data.as_slice()).unwrap();
    assert_eq!(residual_dag.executors.len(), 2);
    assert_eq!(
        residual_dag.executors[1].tp,
        Some(ExecType::TypeSelection as i32)
    );
    assert_eq!(
        residual_dag.executors[1]
            .selection
            .as_ref()
            .unwrap()
            .conditions
            .len(),
        1
    );
}

#[test]
fn contradictory_handle_ranges_return_empty_before_tso_or_transport() {
    let timestamps = CountingTimestampSource::new([]);
    let state = Rc::new(TransportState::default());
    let mut session = RealTiKvReadSession::new(
        configured_table(),
        CapturingTransport {
            state: Rc::clone(&state),
        },
        timestamps.clone(),
    );

    let query = session
        .execute("SELECT id FROM accounts WHERE id > 10 AND id < 0")
        .expect("planner contradiction is a successful empty query");
    assert_eq!(query.snapshot_ts(), None);
    assert_eq!(query.plan_evidence().handle_range_count(), 0);
    assert!(query.plan_evidence().handle_ranges().is_empty());
    assert_eq!(
        query.plan_evidence().executor_kinds(),
        [RealTiKvPlanExecutorKind::TableScan]
    );
    assert_eq!(query.plan_evidence().predicate_count(), 0);
    assert_eq!(timestamps.calls(), 0);
    assert_eq!(state.sends.get(), 0);
    assert!(state.requests.borrow().is_empty());
    assert_eq!(session.last_snapshot_ts(), None);

    let mut record_set = query.into_record_set();
    assert_eq!(record_set.columns().len(), 1);
    assert_eq!(record_set.columns()[0].name, "id");
    assert!(record_set.next_batch(1).unwrap().is_empty());
    record_set.close().unwrap();
}
