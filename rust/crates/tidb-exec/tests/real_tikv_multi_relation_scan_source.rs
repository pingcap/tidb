// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(missing_docs)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    CancelHandle, QueryDispatch, QueryTransport, TimestampSource, TransportRequest,
};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

use crate::real_tikv_multi_read::{RealTiKvMultiReadError, RealTiKvMultiReadSession};
use crate::real_tikv_read::RealTiKvSessionTransportFactory;

#[derive(Clone, Debug)]
struct CountingTimestampSource {
    value: u64,
    calls: Arc<AtomicUsize>,
}

impl CountingTimestampSource {
    fn new(value: u64) -> Self {
        Self {
            value,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl TimestampSource for CountingTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RequestObservation {
    transport_id: usize,
    start_ts: u64,
    start_key: Vec<u8>,
}

#[derive(Default)]
struct FactoryState {
    opened: usize,
    requests: Vec<RequestObservation>,
    closed_responses: Vec<usize>,
    fail_send_on_transport: Option<usize>,
}

#[derive(Clone, Default)]
struct CapturingTransportFactory {
    state: Arc<Mutex<FactoryState>>,
}

impl CapturingTransportFactory {
    fn fail_second_send() -> Self {
        Self {
            state: Arc::new(Mutex::new(FactoryState {
                fail_send_on_transport: Some(1),
                ..FactoryState::default()
            })),
        }
    }
}

impl RealTiKvSessionTransportFactory for CapturingTransportFactory {
    type Transport = CapturingTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        let mut state = self.state.lock().unwrap();
        let transport_id = state.opened;
        state.opened += 1;
        drop(state);
        Ok(CapturingTransport {
            transport_id,
            state: Arc::clone(&self.state),
        })
    }
}

struct CapturingTransport {
    transport_id: usize,
    state: Arc<Mutex<FactoryState>>,
}

impl QueryTransport for CapturingTransport {
    type Response = EmptyResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let metadata = request.metadata();
        let [ranges] = metadata
            .key_ranges
            .as_ref()
            .expect("table read carries ranges")
            .partitions
            .as_slice()
        else {
            panic!("one nonpartitioned range group")
        };
        let start_key = ranges
            .first()
            .expect("noncontradictory scan has a range")
            .start_key
            .clone();
        let mut state = self.state.lock().unwrap();
        state.requests.push(RequestObservation {
            transport_id: self.transport_id,
            start_ts: metadata.start_ts,
            start_key,
        });
        if state.fail_send_on_transport == Some(self.transport_id) {
            return Err("injected second transport failure".to_owned());
        }
        drop(state);
        Ok(Some(EmptyResponse {
            transport_id: self.transport_id,
            state: Arc::clone(&self.state),
        }))
    }
}

struct EmptyResponse {
    transport_id: usize,
    state: Arc<Mutex<FactoryState>>,
}

impl QueryResponse for EmptyResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(None)
    }

    fn close(&mut self) {
        self.state
            .lock()
            .unwrap()
            .closed_responses
            .push(self.transport_id);
    }
}

fn configured_tables() -> [ConfiguredTable; 2] {
    [
        ConfiguredTable::new(
            "test",
            "accounts",
            42,
            [
                ConfiguredColumn::clustered_primary_key("id", 7),
                ConfiguredColumn::stored_not_null("balance", 8),
            ],
        ),
        ConfiguredTable::new(
            "test",
            "profiles",
            84,
            [
                ConfiguredColumn::clustered_primary_key("id", 11),
                ConfiguredColumn::stored_not_null("score", 12),
            ],
        ),
    ]
}

#[test]
fn both_physical_scans_share_one_timestamp_and_cancellation_authority() {
    let timestamps = CountingTimestampSource::new(7_777);
    let factory = CapturingTransportFactory::default();
    let state = Arc::clone(&factory.state);
    let authority = RealTiKvMultiReadSession::new(configured_tables(), factory, timestamps.clone());
    let cancellation = Arc::new(CancelHandle::default());
    assert_eq!(authority.configured_tables()[0].table_id(), 42);
    assert_eq!(authority.configured_tables()[1].table_id(), 84);

    let query = authority
        .execute_with_cancellation(
            [
                "SELECT id FROM accounts WHERE id >= -5 AND balance > 10",
                "SELECT score FROM profiles WHERE id != 0",
            ],
            Arc::clone(&cancellation),
        )
        .unwrap();

    assert_eq!(timestamps.calls(), 1);
    assert_eq!(query.snapshot_ts(), 7_777);
    assert_eq!(
        query
            .relations()
            .iter()
            .map(|relation| relation.snapshot_ts())
            .collect::<Vec<_>>(),
        [Some(7_777), Some(7_777)]
    );
    assert_eq!(
        query
            .relations()
            .iter()
            .map(|relation| relation.table_id())
            .collect::<Vec<_>>(),
        [42, 84]
    );
    assert_eq!(query.relations()[0].plan_evidence().predicate_count(), 1);
    assert_eq!(query.relations()[0].plan_evidence().handle_range_count(), 1);
    assert_eq!(query.relations()[1].plan_evidence().predicate_count(), 0);
    assert_eq!(query.relations()[1].plan_evidence().handle_range_count(), 2);
    assert_eq!(query.sessions()[0].configured_table().table_id(), 42);
    assert_eq!(query.sessions()[1].configured_table().table_id(), 84);

    let state = state.lock().unwrap();
    assert_eq!(state.opened, 2);
    assert_eq!(
        state
            .requests
            .iter()
            .map(|request| (request.transport_id, request.start_ts))
            .collect::<Vec<_>>(),
        [(0, 7_777), (1, 7_777)]
    );
    assert_ne!(state.requests[0].start_key, state.requests[1].start_key);
    drop(state);

    cancellation.cancel();
    assert!(query
        .relations()
        .iter()
        .all(|relation| relation.is_cancelled()));
    let relations = query.into_relations();
    assert_eq!(relations[0].table_id(), 42);
    assert_eq!(relations[1].table_id(), 84);
}

#[test]
fn invalid_second_plan_fails_before_timestamp_transport_or_left_send() {
    let timestamps = CountingTimestampSource::new(8_888);
    let factory = CapturingTransportFactory::default();
    let state = Arc::clone(&factory.state);
    let authority = RealTiKvMultiReadSession::new(configured_tables(), factory, timestamps.clone());

    let error = authority
        .execute([
            "SELECT id FROM accounts WHERE id > 0",
            "SELECT id FROM profiles ORDER BY id",
        ])
        .err()
        .expect("ordering is rejected during right-side preflight");
    assert!(matches!(
        error,
        RealTiKvMultiReadError::Plan { relation: 1, .. }
    ));
    assert_eq!(timestamps.calls(), 0);
    let state = state.lock().unwrap();
    assert_eq!(state.opened, 0);
    assert!(state.requests.is_empty());
}

#[test]
fn zero_timestamp_fails_before_opening_or_sending_either_transport() {
    let timestamps = CountingTimestampSource::new(0);
    let factory = CapturingTransportFactory::default();
    let state = Arc::clone(&factory.state);
    let authority = RealTiKvMultiReadSession::new(configured_tables(), factory, timestamps.clone());

    let error = authority
        .execute(["SELECT id FROM accounts", "SELECT id FROM profiles"])
        .err()
        .expect("zero timestamp must fail closed");
    assert!(matches!(error, RealTiKvMultiReadError::ZeroTimestamp));
    assert_eq!(timestamps.calls(), 1);
    let state = state.lock().unwrap();
    assert_eq!(state.opened, 0);
    assert!(state.requests.is_empty());
}

#[test]
fn second_send_failure_cancels_and_closes_the_first_lazy_response() {
    let timestamps = CountingTimestampSource::new(9_999);
    let factory = CapturingTransportFactory::fail_second_send();
    let state = Arc::clone(&factory.state);
    let authority = RealTiKvMultiReadSession::new(configured_tables(), factory, timestamps.clone());
    let cancellation = Arc::new(CancelHandle::default());

    let error = authority
        .execute_with_cancellation(
            ["SELECT id FROM accounts", "SELECT id FROM profiles"],
            Arc::clone(&cancellation),
        )
        .err()
        .expect("second physical send is injected to fail");
    assert!(matches!(
        error,
        RealTiKvMultiReadError::Read { relation: 1, .. }
    ));
    assert_eq!(timestamps.calls(), 1);
    assert!(cancellation.is_cancelled());
    let state = state.lock().unwrap();
    assert_eq!(state.opened, 2);
    assert_eq!(state.requests.len(), 2);
    assert_eq!(state.closed_responses, [0]);
}
