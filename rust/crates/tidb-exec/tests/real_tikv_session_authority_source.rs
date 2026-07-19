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

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{QueryDispatch, QueryTransport, TimestampSource, TransportRequest};
use tidb_exec::real_tikv_read::{RealTiKvReadAuthority, RealTiKvSessionTransportFactory};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
use tidb_proto::tipb::{Chunk, SelectResponse};

#[derive(Default)]
struct SessionEvidence {
    sends: AtomicUsize,
    nexts: AtomicUsize,
    closes: AtomicUsize,
}

#[derive(Clone, Default)]
struct EvidenceRegistry {
    sessions: Arc<Mutex<BTreeMap<u64, Arc<SessionEvidence>>>>,
}

impl EvidenceRegistry {
    fn insert(&self, session: u64, evidence: Arc<SessionEvidence>) {
        self.sessions
            .lock()
            .expect("evidence registry lock")
            .insert(session, evidence);
    }

    fn get(&self, session: u64) -> Arc<SessionEvidence> {
        Arc::clone(
            self.sessions
                .lock()
                .expect("evidence registry lock")
                .get(&session)
                .expect("opened session evidence"),
        )
    }
}

struct IsolatedResponse {
    subsets: VecDeque<QueryResultSubset>,
    evidence: Arc<SessionEvidence>,
}

impl QueryResponse for IsolatedResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.evidence.nexts.fetch_add(1, Ordering::Relaxed);
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
        self.evidence.closes.fetch_add(1, Ordering::Relaxed);
        self.subsets.clear();
    }
}

struct IsolatedTransport {
    session: u64,
    evidence: Arc<SessionEvidence>,
}

impl QueryTransport for IsolatedTransport {
    type Response = IsolatedResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        assert!(request.is_bound());
        self.evidence.sends.fetch_add(1, Ordering::Relaxed);
        Ok(Some(IsolatedResponse {
            subsets: VecDeque::from([QueryResultSubset {
                data: encoded_row(self.session as i64),
                runtime: None,
            }]),
            evidence: Arc::clone(&self.evidence),
        }))
    }
}

struct ProcessTransportFactory {
    next_session: AtomicU64,
    evidence: EvidenceRegistry,
}

impl ProcessTransportFactory {
    fn new(evidence: EvidenceRegistry) -> Self {
        Self {
            next_session: AtomicU64::new(1),
            evidence,
        }
    }
}

impl RealTiKvSessionTransportFactory for ProcessTransportFactory {
    type Transport = IsolatedTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        let evidence = Arc::new(SessionEvidence::default());
        self.evidence.insert(session, Arc::clone(&evidence));
        Ok(IsolatedTransport { session, evidence })
    }
}

#[derive(Clone)]
struct ProcessTimestampSource {
    next: Arc<AtomicU64>,
}

impl TimestampSource for ProcessTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "test",
        "accounts",
        42,
        vec![ConfiguredColumn::clustered_primary_key("id", 7)],
    )
}

fn encoded_row(value: i64) -> Vec<u8> {
    let mut rows_data = vec![8];
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    while unsigned >= 0x80 {
        rows_data.push((unsigned as u8) | 0x80);
        unsigned >>= 7;
    }
    rows_data.push(unsigned as u8);
    SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn process_authority_opens_isolated_worker_local_sessions() {
    // Source obligations:
    // - pkg/distsql/distsql_test.go:42 (TestSelectNormal)
    // - pkg/distsql/select_result_test.go:183 (TestSelRespChannelIterRead)
    // - pkg/executor/distsql_test.go:68 (TestCopClientSend)
    let evidence = EvidenceRegistry::default();
    let authority = RealTiKvReadAuthority::new(
        configured_table(),
        ProcessTransportFactory::new(evidence.clone()),
        ProcessTimestampSource {
            next: Arc::new(AtomicU64::new(700)),
        },
        9_001,
    );
    assert_send_sync::<RealTiKvReadAuthority<ProcessTransportFactory, ProcessTimestampSource>>();

    let mut first = authority.open_session().expect("first local session");
    let mut second = authority.open_session().expect("second local session");
    assert_eq!(authority.cluster_id(), 9_001);
    assert_eq!(first.cluster_id(), 9_001);
    assert_eq!(second.cluster_id(), 9_001);
    assert_eq!(first.identity().authority_id(), authority.authority_id());
    assert_eq!(second.identity().authority_id(), authority.authority_id());
    assert_ne!(
        first.identity().session_id(),
        second.identity().session_id()
    );

    let first_query = first.execute("SELECT id FROM accounts").unwrap();
    let second_query = second.execute("SELECT id FROM accounts").unwrap();
    assert_eq!(first_query.snapshot_ts(), 700);
    assert_eq!(second_query.snapshot_ts(), 701);
    assert_eq!(first.last_snapshot_ts(), Some(700));
    assert_eq!(second.last_snapshot_ts(), Some(701));
    assert_eq!(
        first_query.session_identity().authority_id(),
        second_query.session_identity().authority_id()
    );
    assert_ne!(
        first_query.session_identity().session_id(),
        second_query.session_identity().session_id()
    );

    first_query.cancel();
    assert!(first_query.is_cancelled());
    assert!(!second_query.is_cancelled());

    let first_evidence = evidence.get(1);
    let second_evidence = evidence.get(2);
    assert_eq!(first_evidence.sends.load(Ordering::Relaxed), 1);
    assert_eq!(second_evidence.sends.load(Ordering::Relaxed), 1);
    assert_eq!(first_evidence.nexts.load(Ordering::Relaxed), 0);
    assert_eq!(second_evidence.nexts.load(Ordering::Relaxed), 0);

    let mut first_record_set = first_query.into_record_set();
    let mut second_record_set = second_query.into_record_set();
    assert_eq!(
        first_record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    assert_eq!(first_evidence.nexts.load(Ordering::Relaxed), 1);
    assert_eq!(second_evidence.nexts.load(Ordering::Relaxed), 0);
    first_record_set.close().unwrap();
    assert_eq!(first_evidence.closes.load(Ordering::Relaxed), 1);
    assert_eq!(second_evidence.closes.load(Ordering::Relaxed), 0);

    assert_eq!(
        second_record_set.next_batch(1).unwrap(),
        vec![vec![Datum::Int(2)]]
    );
    second_record_set.close().unwrap();
    assert_eq!(second_evidence.nexts.load(Ordering::Relaxed), 1);
    assert_eq!(second_evidence.closes.load(Ordering::Relaxed), 1);
}
