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

//! Source-backed terminal configured ORDER BY/LIMIT execution tests.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{QueryDispatch, QueryTransport, TimestampSource, TransportRequest};
use tidb_exec::{
    configured_ordered_query::{
        ConfiguredOrderedQueryEvidence, ConfiguredOrderedQueryRecordSet,
        PreparedConfiguredOrderedQueryTail,
    },
    real_tikv_read::{RealTiKvReadSessionOpener, RealTiKvSessionTransportFactory},
};
use tidb_planner::{
    configured_join_plan::ConfiguredJoinPlan,
    configured_order_limit::ConfiguredOrderedJoinPlan,
    read_only_scan::{configured_catalog::ConfiguredCatalog, ConfiguredColumn, ConfiguredTable},
};
use tidb_proto::tipb::{Chunk, SelectResponse};

#[derive(Clone, Default)]
struct ResponseProbe {
    next_calls: Arc<AtomicUsize>,
    close_calls: Arc<AtomicUsize>,
}

impl ResponseProbe {
    fn next_calls(&self) -> usize {
        self.next_calls.load(Ordering::SeqCst)
    }

    fn close_calls(&self) -> usize {
        self.close_calls.load(Ordering::SeqCst)
    }
}

struct ScriptedResponse {
    subsets: VecDeque<QueryResultSubset>,
    probe: ResponseProbe,
}

impl QueryResponse for ScriptedResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.probe.next_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.subsets.pop_front())
    }

    fn close(&mut self) {
        self.probe.close_calls.fetch_add(1, Ordering::SeqCst);
        self.subsets.clear();
    }
}

#[derive(Clone)]
struct ScriptedTransportFactory {
    responses: Arc<Mutex<VecDeque<ScriptedResponse>>>,
}

impl ScriptedTransportFactory {
    fn new(responses: [ScriptedResponse; 2]) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        }
    }
}

impl RealTiKvSessionTransportFactory for ScriptedTransportFactory {
    type Transport = ScriptedTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(ScriptedTransport {
            response: self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("one scripted relation response"),
        })
    }
}

struct ScriptedTransport {
    response: ScriptedResponse,
}

impl QueryTransport for ScriptedTransport {
    type Response = ScriptedResponse;

    fn send(
        &mut self,
        _request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let response = std::mem::replace(
            &mut self.response,
            ScriptedResponse {
                subsets: VecDeque::new(),
                probe: ResponseProbe::default(),
            },
        );
        Ok(Some(response))
    }
}

#[derive(Clone, Debug)]
struct FixedTimestamp;

impl TimestampSource for FixedTimestamp {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(6_026)
    }
}

fn tables() -> [ConfiguredTable; 2] {
    [
        ConfiguredTable::new(
            "Sales",
            "Accounts",
            101,
            [
                ConfiguredColumn::clustered_primary_key("AccountID", 11),
                ConfiguredColumn::stored_not_null("Balance", 19),
            ],
        ),
        ConfiguredTable::new(
            "Sales",
            "Orders",
            202,
            [
                ConfiguredColumn::clustered_primary_key("OrderID", 7),
                ConfiguredColumn::stored_not_null("AccountID", 23),
                ConfiguredColumn::stored_not_null("Amount", 31),
            ],
        ),
    ]
}

fn encode_signed_varint(output: &mut Vec<u8>, value: i64) {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    while unsigned >= 0x80 {
        output.push((unsigned as u8) | 0x80);
        unsigned >>= 7;
    }
    output.push(unsigned as u8);
}

fn response(rows: &[&[i64]]) -> (ScriptedResponse, ResponseProbe) {
    let probe = ResponseProbe::default();
    let subsets = rows
        .iter()
        .map(|row| {
            let mut rows_data = Vec::new();
            for value in *row {
                rows_data.push(8);
                encode_signed_varint(&mut rows_data, *value);
            }
            QueryResultSubset {
                data: SelectResponse {
                    chunks: vec![Chunk {
                        rows_data: Some(rows_data),
                        rows_meta: Vec::new(),
                    }],
                    ..SelectResponse::default()
                }
                .encode_to_vec(),
                runtime: None,
            }
        })
        .collect();
    (
        ScriptedResponse {
            subsets,
            probe: probe.clone(),
        },
        probe,
    )
}

struct Fixture {
    result: ConfiguredOrderedQueryRecordSet,
    probes: [ResponseProbe; 2],
}

fn execute(
    sql: &str,
    left: (ScriptedResponse, ResponseProbe),
    right: (ScriptedResponse, ResponseProbe),
    topn_capacity: usize,
) -> Fixture {
    let configured_tables = tables();
    let catalog = ConfiguredCatalog::new(configured_tables.clone()).unwrap();
    let plan = ConfiguredOrderedJoinPlan::lower(sql, &catalog).unwrap();
    let join = plan.join().expect("nonempty test query").clone();
    let tail = plan.order_limit().expect("test tail").clone();
    let opener = RealTiKvReadSessionOpener::new(
        configured_tables[0].clone(),
        ScriptedTransportFactory::new([left.0, right.0]),
        FixedTimestamp,
        99,
    );
    let mut session = opener.open_multi_session(configured_tables).unwrap();
    let inner = session.execute_configured_inner_join(join).unwrap();
    Fixture {
        result: ConfiguredOrderedQueryRecordSet::new(inner, tail, topn_capacity).unwrap(),
        probes: [left.1, right.1],
    }
}

#[test]
fn configured_topn_orders_full_schema_then_projects_hidden_keys_away() {
    // Source: pkg/executor/builder.go and pkg/executor/join/inner_join_probe_test.go:
    // 494 TestInnerJoinProbeBasic, 543 TestInnerJoinProbeAllJoinKeys.
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a JOIN Orders o USING (AccountID) \
         ORDER BY o.AccountID DESC, o.OrderID ASC LIMIT 1, 3",
        response(&[&[1, 10], &[2, 20], &[1, 11]]),
        response(&[&[100, 1, 1000], &[101, 2, 2000], &[102, 1, 1001]]),
        4,
    );

    assert_eq!(fixture.result.snapshot_ts(), Some(6_026));
    assert_eq!(
        fixture.result.next_batch(2).unwrap(),
        vec![
            vec![Datum::Int(10), Datum::Int(1000)],
            vec![Datum::Int(11), Datum::Int(1000)],
        ],
        "the USING-hidden right AccountID and unprojected OrderID order full rows before output projection"
    );
    let pulls_after_one_drain = [
        fixture.probes[0].next_calls(),
        fixture.probes[1].next_calls(),
    ];
    assert_eq!(
        fixture.result.next_batch(2).unwrap(),
        vec![vec![Datum::Int(10), Datum::Int(1001)]]
    );
    assert_eq!(
        [
            fixture.probes[0].next_calls(),
            fixture.probes[1].next_calls(),
        ],
        pulls_after_one_drain,
        "later output batches drain only the bounded TopN result, never the join again"
    );
    assert!(fixture.result.next_batch(1).unwrap().is_empty());
    let Some(ConfiguredOrderedQueryEvidence::TopN(evidence)) = fixture.result.completed_evidence()
    else {
        panic!("completed TopN exposes bounded execution accounting");
    };
    assert_eq!(evidence.capacity(), 4);
    assert_eq!(evidence.high_water_candidates(), 4);
    assert_eq!(evidence.rows_consumed(), 5);
    assert_eq!(evidence.rows_emitted(), 3);
    fixture.result.close().unwrap();
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
}

#[test]
fn configured_limit_uses_the_one_join_reader_and_closes_without_a_hidden_pull() {
    // Source: pkg/executor/builder.go and pkg/executor/join/joiner_test.go:
    // 46 TestJoinerOtherConditionChunkUsesInitChunkSize, 93 TestRequiredRows.
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o LIMIT 1, 2",
        response(&[&[1, 10], &[2, 20]]),
        response(&[&[100, 7, 1000], &[101, 8, 2000]]),
        1,
    );

    assert_eq!(
        fixture.result.next_batch(8).unwrap(),
        vec![
            vec![Datum::Int(10), Datum::Int(2000)],
            vec![Datum::Int(20), Datum::Int(1000)],
        ]
    );
    assert_eq!(
        fixture.probes[0].next_calls(),
        2,
        "second left row is needed for result two"
    );
    assert_eq!(
        fixture.probes[1].next_calls(),
        3,
        "no row after the LIMIT window is pulled"
    );
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
    assert!(fixture.result.lifecycle().is_closed());
    let Some(ConfiguredOrderedQueryEvidence::Limit(evidence)) = fixture.result.completed_evidence()
    else {
        panic!("completed LIMIT exposes exact source consumption");
    };
    assert_eq!(evidence.rows_requested(), 3);
    assert_eq!(evidence.rows_skipped(), 1);
    assert_eq!(evidence.rows_emitted(), 2);
    assert!(evidence.source_closed());
}

#[test]
fn prepared_topn_rejects_capacity_before_a_join_or_transport_exists() {
    // Source: pkg/executor/builder.go: ORDER BY/LIMIT admission must occur
    // before physical reader construction.
    let configured_tables = tables();
    let catalog = ConfiguredCatalog::new(configured_tables).unwrap();
    let plan = ConfiguredOrderedJoinPlan::lower(
        "SELECT a.Balance FROM Accounts a CROSS JOIN Orders o ORDER BY o.Amount LIMIT 2, 3",
        &catalog,
    )
    .unwrap();
    let join = plan.join().unwrap();
    let tail = plan.order_limit().unwrap().clone();

    assert!(matches!(
        PreparedConfiguredOrderedQueryTail::prepare(tail, join.full_schema().len(), 4),
        Err(
            tidb_exec::configured_ordered_query::ConfiguredOrderedQueryError::TopN(
                tidb_exec::configured_topn::ConfiguredTopNError::CapacityExceeded {
                    end_exclusive: 5,
                    capacity: 4,
                }
            )
        )
    ));
}

#[test]
fn local_empty_preserves_opened_join_metadata_without_readers() {
    // Source: pkg/server/conn.go: an empty result still writes normal column
    // definitions, but LIMIT 0 must not create a snapshot or physical reader.
    let configured_tables = tables();
    let catalog = ConfiguredCatalog::new(configured_tables.clone()).unwrap();
    let join = ConfiguredJoinPlan::lower(
        "SELECT a.Balance AS balance, o.Amount FROM Accounts a JOIN Orders o USING (AccountID)",
        &catalog,
    )
    .unwrap();
    let mut local = ConfiguredOrderedQueryRecordSet::local_empty(
        &join,
        [&configured_tables[0], &configured_tables[1]],
    )
    .unwrap();
    let opened = execute(
        "SELECT a.Balance AS balance, o.Amount FROM Accounts a JOIN Orders o USING (AccountID) LIMIT 1",
        response(&[&[1, 10]]),
        response(&[&[100, 1, 1000]]),
        1,
    );

    assert_eq!(local.columns(), opened.result.columns());
    assert_eq!(local.snapshot_ts(), None);
    assert!(local.next_batch(1).unwrap().is_empty());
    assert!(local.lifecycle().has_advanced());
    local.close().unwrap();
    assert!(local.lifecycle().is_finished());
    assert!(local.lifecycle().is_closed());
}

#[test]
fn early_limit_close_is_non_pulling_and_reports_closed_accounting() {
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o LIMIT 10",
        response(&[&[1, 10], &[2, 20]]),
        response(&[&[100, 7, 1000], &[101, 8, 2000]]),
        1,
    );

    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(10), Datum::Int(1000)]]
    );
    let pulls_before_close = [
        fixture.probes[0].next_calls(),
        fixture.probes[1].next_calls(),
    ];
    fixture.result.close().unwrap();
    assert_eq!(
        [
            fixture.probes[0].next_calls(),
            fixture.probes[1].next_calls()
        ],
        pulls_before_close,
        "connection close does not read one more physical row to account for LIMIT"
    );
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
    let Some(ConfiguredOrderedQueryEvidence::Limit(evidence)) = fixture.result.completed_evidence()
    else {
        panic!("early record-set close exposes LIMIT accounting");
    };
    assert_eq!(evidence.rows_requested(), 1);
    assert_eq!(evidence.rows_skipped(), 0);
    assert_eq!(evidence.rows_emitted(), 1);
    assert!(evidence.source_closed());
}
