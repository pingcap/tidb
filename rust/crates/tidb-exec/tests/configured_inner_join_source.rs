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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{
    CancelHandle, QueryDispatch, QueryTransport, TimestampSource, TransportRequest,
};
use tidb_exec::configured_inner_join::ConfiguredInnerJoinError;
use tidb_exec::real_tikv_read::{RealTiKvReadSessionOpener, RealTiKvSessionTransportFactory};
use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    configured_join_plan::ConfiguredJoinPlan,
    read_only_scan::{ConfiguredColumn, ConfiguredTable},
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
    fail_on_call: Option<usize>,
    probe: ResponseProbe,
}

impl QueryResponse for ScriptedResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        let call = self.probe.next_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_on_call == Some(call) {
            return Err(QueryResponseError::Source(
                "injected join source failure".to_owned(),
            ));
        }
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
    sends: Arc<AtomicUsize>,
}

impl ScriptedTransportFactory {
    fn new(responses: [ScriptedResponse; 2]) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            sends: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl RealTiKvSessionTransportFactory for ScriptedTransportFactory {
    type Transport = ScriptedTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(ScriptedTransport {
            response: Some(
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("one response per configured relation"),
            ),
            sends: Arc::clone(&self.sends),
        })
    }
}

struct ScriptedTransport {
    response: Option<ScriptedResponse>,
    sends: Arc<AtomicUsize>,
}

impl QueryTransport for ScriptedTransport {
    type Response = ScriptedResponse;

    fn send(
        &mut self,
        _request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(self.response.take())
    }
}

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
}

impl TimestampSource for CountingTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.value)
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

fn encoded_row(row: &[Option<i64>]) -> Vec<u8> {
    let mut rows_data = Vec::new();
    for value in row {
        match value {
            Some(value) => {
                rows_data.push(8);
                encode_signed_varint(&mut rows_data, *value);
            }
            None => rows_data.push(0),
        }
    }
    SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn response(rows: &[&[i64]]) -> (ScriptedResponse, ResponseProbe) {
    response_with_failure(rows, None)
}

fn response_with_failure(
    rows: &[&[i64]],
    fail_on_call: Option<usize>,
) -> (ScriptedResponse, ResponseProbe) {
    let probe = ResponseProbe::default();
    let subsets = rows
        .iter()
        .map(|row| QueryResultSubset {
            data: encoded_row(&row.iter().copied().map(Some).collect::<Vec<_>>()),
            runtime: None,
        })
        .collect();
    (
        ScriptedResponse {
            subsets,
            fail_on_call,
            probe: probe.clone(),
        },
        probe,
    )
}

fn null_key_response(row: &[Option<i64>]) -> (ScriptedResponse, ResponseProbe) {
    let probe = ResponseProbe::default();
    (
        ScriptedResponse {
            subsets: VecDeque::from([QueryResultSubset {
                data: encoded_row(row),
                runtime: None,
            }]),
            fail_on_call: None,
            probe: probe.clone(),
        },
        probe,
    )
}

struct Fixture {
    result: tidb_exec::configured_inner_join::ConfiguredInnerJoinRecordSet,
    probes: [ResponseProbe; 2],
    timestamp_calls: Arc<AtomicUsize>,
    sends: Arc<AtomicUsize>,
    cancellation: Arc<CancelHandle>,
}

fn execute(
    sql: &str,
    left: (ScriptedResponse, ResponseProbe),
    right: (ScriptedResponse, ResponseProbe),
) -> Fixture {
    execute_with_tables(tables(), sql, left, right)
}

fn execute_with_tables(
    configured_tables: [ConfiguredTable; 2],
    sql: &str,
    left: (ScriptedResponse, ResponseProbe),
    right: (ScriptedResponse, ResponseProbe),
) -> Fixture {
    let catalog = ConfiguredCatalog::new(configured_tables.clone()).unwrap();
    let plan = ConfiguredJoinPlan::lower(sql, &catalog).unwrap();
    let timestamps = CountingTimestampSource::new(6_001);
    let factory = ScriptedTransportFactory::new([left.0, right.0]);
    let sends = Arc::clone(&factory.sends);
    let opener = RealTiKvReadSessionOpener::new(
        configured_tables[0].clone(),
        factory,
        timestamps.clone(),
        99,
    );
    let mut session = opener.open_multi_session(configured_tables).unwrap();
    let cancellation = Arc::new(CancelHandle::default());
    let result = session
        .execute_configured_inner_join_with_cancellation(plan, Arc::clone(&cancellation))
        .unwrap();
    Fixture {
        result,
        probes: [left.1, right.1],
        timestamp_calls: timestamps.calls,
        sends,
        cancellation,
    }
}

#[test]
fn inner_equality_preserves_duplicate_multiplicity_projection_and_metadata_order() {
    // pkg/executor/join/inner_join_probe_test.go:494 TestInnerJoinProbeBasic
    // pkg/executor/join/inner_join_probe_test.go:543 TestInnerJoinProbeAllJoinKeys
    let mut fixture = execute(
        "SELECT a.Balance AS balance, o.Amount, a.AccountID \
         FROM Accounts a JOIN Orders o ON o.AccountID = a.AccountID",
        response(&[&[1, 10], &[2, 20], &[1, 11]]),
        response(&[&[100, 1, 1000], &[101, 2, 2000], &[102, 1, 1001]]),
    );

    assert_eq!(fixture.result.snapshot_ts(), Some(6_001));
    assert_eq!(fixture.timestamp_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.sends.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture
            .result
            .columns()
            .iter()
            .map(|column| (
                column.schema.as_str(),
                column.table.as_str(),
                column.org_table.as_str(),
                column.name.as_str(),
                column.org_name.as_str(),
                column.flag,
            ))
            .collect::<Vec<_>>(),
        [
            ("Sales", "a", "Accounts", "balance", "Balance", 1),
            ("Sales", "o", "Orders", "Amount", "Amount", 1),
            ("Sales", "a", "Accounts", "AccountID", "AccountID", 3),
        ]
    );
    assert_eq!(
        fixture.result.next_batch(2).unwrap(),
        vec![
            vec![Datum::Int(10), Datum::Int(1000), Datum::Int(1)],
            vec![Datum::Int(10), Datum::Int(1001), Datum::Int(1)],
        ]
    );
    assert_eq!(
        fixture.result.next_batch(8).unwrap(),
        vec![
            vec![Datum::Int(20), Datum::Int(2000), Datum::Int(2)],
            vec![Datum::Int(11), Datum::Int(1000), Datum::Int(1)],
            vec![Datum::Int(11), Datum::Int(1001), Datum::Int(1)],
        ]
    );
    assert!(fixture.result.next_batch(1).unwrap().is_empty());
}

/// The same `Accounts`/`Orders` shape `tables()` builds by hand, but produced
/// through the exact path a `--load-table` node uses at startup: Go-shaped
/// stored `TableInfo` JSON (the same fixture family
/// `cluster_catalog_loader_source.rs` pins) decoded by
/// `tidb_meta::value::parse_table_info` and then admitted by
/// `configure_loaded_table`. This is the evidence that the join dispatcher
/// runs unchanged over catalog-loaded tables, not only over command-line ones.
const GO_LOADED_ACCOUNTS_TABLE: &str = r#"{"id":101,"name":{"O":"Accounts","L":"accounts"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":11,"name":{"O":"AccountID","L":"accountid"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":19,"name":{"O":"Balance","L":"balance"},"offset":1,"type":{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":19,"version":5}"#;

const GO_LOADED_ORDERS_TABLE: &str = r#"{"id":202,"name":{"O":"Orders","L":"orders"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":7,"name":{"O":"OrderID","L":"orderid"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":23,"name":{"O":"AccountID","L":"accountid"},"offset":1,"type":{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":31,"name":{"O":"Amount","L":"amount"},"offset":2,"type":{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":31,"version":5}"#;

fn loaded_tables() -> [ConfiguredTable; 2] {
    let accounts = tidb_meta::value::parse_table_info(GO_LOADED_ACCOUNTS_TABLE.as_bytes(), 9)
        .expect("Accounts TableInfo decodes");
    let orders = tidb_meta::value::parse_table_info(GO_LOADED_ORDERS_TABLE.as_bytes(), 9)
        .expect("Orders TableInfo decodes");
    [
        tidb_exec::cluster_catalog::configure_loaded_table("Sales", &accounts)
            .expect("Accounts is inside the widened read domain"),
        tidb_exec::cluster_catalog::configure_loaded_table("Sales", &orders)
            .expect("Orders is inside the widened read domain"),
    ]
}

#[test]
fn inner_equality_join_runs_unchanged_over_catalog_loaded_tables() {
    // Same assertions as
    // `inner_equality_preserves_duplicate_multiplicity_projection_and_metadata_order`,
    // but over `loaded_tables()` instead of the hand-built `tables()`: proof
    // that the join dispatcher does not care whether its two `ConfiguredTable`s
    // came from `--read-table` or from a cluster catalog load.
    let mut fixture = execute_with_tables(
        loaded_tables(),
        "SELECT a.Balance AS balance, o.Amount, a.AccountID \
         FROM Accounts a JOIN Orders o ON o.AccountID = a.AccountID",
        response(&[&[1, 10], &[2, 20], &[1, 11]]),
        response(&[&[100, 1, 1000], &[101, 2, 2000], &[102, 1, 1001]]),
    );

    assert_eq!(fixture.result.snapshot_ts(), Some(6_001));
    assert_eq!(
        fixture.result.next_batch(2).unwrap(),
        vec![
            vec![Datum::Int(10), Datum::Int(1000), Datum::Int(1)],
            vec![Datum::Int(10), Datum::Int(1001), Datum::Int(1)],
        ]
    );
    assert_eq!(
        fixture.result.next_batch(8).unwrap(),
        vec![
            vec![Datum::Int(20), Datum::Int(2000), Datum::Int(2)],
            vec![Datum::Int(11), Datum::Int(1000), Datum::Int(1)],
            vec![Datum::Int(11), Datum::Int(1001), Datum::Int(1)],
        ]
    );
    assert!(fixture.result.next_batch(1).unwrap().is_empty());
}

#[test]
fn required_rows_resume_inside_one_multi_match_left_row_without_overreading() {
    // pkg/executor/join/joiner_test.go:93 TestRequiredRows
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o",
        response(&[&[1, 10], &[2, 20]]),
        response(&[&[100, 7, 1000], &[101, 8, 2000]]),
    );

    assert!(fixture.result.next_batch(0).unwrap().is_empty());
    assert_eq!(fixture.probes[0].next_calls(), 0);
    assert_eq!(fixture.probes[1].next_calls(), 0);

    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(10), Datum::Int(1000)]]
    );
    assert_eq!(fixture.probes[0].next_calls(), 1);
    assert_eq!(fixture.probes[1].next_calls(), 1);

    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(10), Datum::Int(2000)]]
    );
    assert_eq!(fixture.probes[0].next_calls(), 1);
    assert_eq!(fixture.probes[1].next_calls(), 2);

    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(20), Datum::Int(1000)]]
    );
    assert_eq!(fixture.probes[0].next_calls(), 2);
    assert_eq!(fixture.probes[1].next_calls(), 3);
}

#[test]
fn cross_and_comma_emit_the_same_left_major_cartesian_product() {
    for from in ["Accounts a CROSS JOIN Orders o", "Accounts a, Orders o"] {
        let mut fixture = execute(
            &format!("SELECT a.Balance, o.Amount FROM {from}"),
            response(&[&[1, 10], &[2, 20]]),
            response(&[&[100, 7, 1000], &[101, 8, 2000]]),
        );
        assert_eq!(
            fixture.result.next_batch(8).unwrap(),
            vec![
                vec![Datum::Int(10), Datum::Int(1000)],
                vec![Datum::Int(10), Datum::Int(2000)],
                vec![Datum::Int(20), Datum::Int(1000)],
                vec![Datum::Int(20), Datum::Int(2000)],
            ]
        );
    }
}

#[test]
fn empty_left_never_reads_right_and_close_releases_both_sources_once() {
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID = o.AccountID",
        response(&[]),
        response(&[&[100, 1, 1000]]),
    );

    assert!(fixture.result.next_batch(8).unwrap().is_empty());
    assert_eq!(fixture.probes[0].next_calls(), 1);
    assert_eq!(fixture.probes[1].next_calls(), 0);
    fixture.result.close().unwrap();
    fixture.result.close().unwrap();
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
    assert!(fixture.result.lifecycle().is_finished());
    assert!(fixture.result.lifecycle().is_closed());
}

#[test]
fn contradiction_is_local_empty_with_projection_metadata_and_no_tso_or_send() {
    let mut fixture = execute(
        "SELECT a.Balance AS b, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID = o.AccountID WHERE a.AccountID > 10 AND a.AccountID < 0",
        response(&[]),
        response(&[]),
    );

    assert_eq!(fixture.result.snapshot_ts(), None);
    assert_eq!(fixture.timestamp_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .result
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["b", "Amount"]
    );
    assert!(fixture.result.next_batch(8).unwrap().is_empty());
}

#[test]
fn pushed_selection_output_is_not_filtered_again_by_the_join_runtime() {
    // pkg/executor/join/inner_join_probe_test.go:664 TestInnerJoinProbeWithSel
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID = o.AccountID WHERE a.Balance > 100",
        // Deliberately violates the pushed Selection. The row models TiKV's
        // already-executed output and must not be filtered again client-side.
        response(&[&[1, -5]]),
        response(&[&[100, 1, 9]]),
    );
    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(-5), Datum::Int(9)]]
    );
}

#[test]
fn midstream_source_error_cancels_and_closes_both_children() {
    // pkg/executor/join/inner_join_probe_test.go:632 TestInnerJoinProbeOtherCondition
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID = o.AccountID",
        response(&[&[1, 10]]),
        response_with_failure(&[&[100, 1, 1000]], Some(2)),
    );

    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![Datum::Int(10), Datum::Int(1000)]]
    );
    let error = fixture.result.next_batch(1).unwrap_err();
    assert!(matches!(
        error,
        ConfiguredInnerJoinError::Source { relation: 1, .. }
    ));
    assert!(fixture.cancellation.is_cancelled());
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
    assert!(fixture.result.lifecycle().is_closed());
}

#[test]
fn caller_cancellation_stops_consumption_and_closes_both_children() {
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a CROSS JOIN Orders o",
        response(&[&[1, 10]]),
        response(&[&[100, 1, 1000]]),
    );

    fixture.cancellation.cancel();
    assert!(matches!(
        fixture.result.next_batch(1),
        Err(ConfiguredInnerJoinError::Cancelled)
    ));
    assert_eq!(fixture.probes[0].next_calls(), 0);
    assert_eq!(fixture.probes[1].next_calls(), 0);
    assert_eq!(fixture.probes[0].close_calls(), 1);
    assert_eq!(fixture.probes[1].close_calls(), 1);
}

/// A `NULL` join key makes `=` UNKNOWN, and an inner join emits a row only when
/// its condition is TRUE, so the `NULL`-keyed left row matches nothing — the
/// stream ends empty rather than erroring or pairing `NULL` with `NULL`.
#[test]
fn null_join_key_matches_nothing_including_another_null() {
    let mut fixture = execute(
        "SELECT a.Balance, o.Amount FROM Accounts a JOIN Orders o \
         ON a.AccountID = o.AccountID",
        null_key_response(&[None, Some(10)]),
        // The right relation's key is `NULL` too: `NULL = NULL` is still
        // UNKNOWN, so even this pair is not emitted.
        null_key_response(&[None, Some(1), Some(1000)]),
    );

    assert!(fixture.result.next_batch(1).expect("empty join stream").is_empty());
    fixture.result.finish().expect("stream finishes cleanly");
    assert!(!fixture.cancellation.is_cancelled());
}

/// Row-based (`Chunk.rows_data`) encoding of an arbitrary datum row, used to
/// drive the join's decoded-datum comparison and projection with kinds beyond
/// signed `BIGINT` (unsigned, `DOUBLE`, `CHAR`), matching each column's own
/// `chunk_field_type` rather than the row-codec's own type tag.
fn encoded_datum_row(row: &[Datum]) -> Vec<u8> {
    let rows_data = tidb_codec::encode_value(row).expect("fixture datums must encode");
    SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn datum_response(rows: &[&[Datum]]) -> (ScriptedResponse, ResponseProbe) {
    let probe = ResponseProbe::default();
    let subsets = rows
        .iter()
        .map(|row| QueryResultSubset {
            data: encoded_datum_row(row),
            runtime: None,
        })
        .collect();
    (
        ScriptedResponse {
            subsets,
            fail_on_call: None,
            probe: probe.clone(),
        },
        probe,
    )
}

/// A left/right pair whose join key crosses the signed/unsigned `BIGINT`
/// domain, plus a `DOUBLE` and `CHAR` output column on the right, mirroring
/// the widened `ConfiguredScalarType` set the real-TiKV scan path
/// (`ConfiguredScalarType::chunk_field_type`) already decodes.
fn cross_signed_tables() -> [ConfiguredTable; 2] {
    [
        ConfiguredTable::new(
            "Sales",
            "Wide",
            301,
            [
                ConfiguredColumn::clustered_primary_key("id", 1),
                ConfiguredColumn::stored_unsigned_bigint_not_null("uid", 2),
            ],
        ),
        ConfiguredTable::new(
            "Sales",
            "Peer",
            302,
            [
                ConfiguredColumn::clustered_primary_key("id", 1),
                ConfiguredColumn::stored_not_null("sid", 2),
                ConfiguredColumn::stored_double_not_null("score", 3),
                ConfiguredColumn::stored_char_not_null("tag", 4, 4),
            ],
        ),
    ]
}

#[test]
fn cross_signedness_boundary_values_never_falsely_match() {
    // pkg/types/compare.go CompareInt: an unsigned value above i64::MAX and a
    // negative signed value are never equal on the other side, not just
    // unequal to each other by coincidence of shared bit pattern.
    let mut fixture = execute_with_tables(
        cross_signed_tables(),
        "SELECT w.uid, p.sid FROM Wide w JOIN Peer p ON w.uid = p.sid",
        datum_response(&[&[Datum::Int(1), Datum::UInt(1u64 << 63)]]),
        datum_response(&[&[
            Datum::Int(1),
            Datum::Int(-1),
            Datum::Real(1.5),
            Datum::new_collation_string(b"ab".to_vec(), tidb_datatype::Collation::Utf8Mb4Bin),
        ]]),
    );
    assert!(fixture.result.next_batch(8).unwrap().is_empty());
}

#[test]
fn cross_signedness_equal_values_within_i64_range_still_match() {
    let mut fixture = execute_with_tables(
        cross_signed_tables(),
        "SELECT w.uid, p.sid, p.score, p.tag FROM Wide w JOIN Peer p ON w.uid = p.sid",
        datum_response(&[&[Datum::Int(1), Datum::UInt(5)]]),
        datum_response(&[&[
            Datum::Int(1),
            Datum::Int(5),
            Datum::Real(2.5),
            Datum::new_collation_string(b"ok".to_vec(), tidb_datatype::Collation::Utf8Mb4Bin),
        ]]),
    );
    // The scripted response omits `SelectResponse.encode_type`, so the shared
    // scan/decode seam (`RealTiKvReadSession::execute_plan`) falls back to
    // `EncodeType::TypeDefault`'s generic value codec, which is not yet
    // column-type-aware for strings: `tag` decodes as `Datum::Bytes`, not a
    // collation-carrying `Datum::String`. The join's key/output handling must
    // accept either representation (see `join_key_eq`); this assertion pins
    // today's actual decoded shape rather than the wire-ideal one.
    assert_eq!(
        fixture.result.next_batch(1).unwrap(),
        vec![vec![
            Datum::UInt(5),
            Datum::Int(5),
            Datum::Real(2.5),
            Datum::Bytes(b"ok".to_vec()),
        ]]
    );
}

#[test]
fn joined_output_columns_report_each_source_columns_own_wire_type() {
    let configured_tables = cross_signed_tables();
    let catalog = ConfiguredCatalog::new(configured_tables.clone()).unwrap();
    let plan = ConfiguredJoinPlan::lower(
        "SELECT w.uid, p.sid, p.score, p.tag FROM Wide w JOIN Peer p ON w.uid = p.sid",
        &catalog,
    )
    .unwrap();
    let columns = tidb_exec::configured_inner_join::configured_join_columns(
        &plan,
        [&configured_tables[0], &configured_tables[1]],
    )
    .unwrap();
    assert_eq!(
        columns
            .iter()
            .map(|column| (column.type_code, column.flag & 0x0020, column.charset))
            .collect::<Vec<_>>(),
        [
            // `uid`: BIGINT UNSIGNED -> LONGLONG, UnsignedFlag set.
            (tidb_datatype::FieldTypeCode::LongLong.mysql_type(), 0x0020, tidb_protocol::BINARY_DEFAULT_COLLATION_ID),
            // `sid`: signed BIGINT -> LONGLONG, no unsigned flag.
            (tidb_datatype::FieldTypeCode::LongLong.mysql_type(), 0, tidb_protocol::BINARY_DEFAULT_COLLATION_ID),
            // `score`: DOUBLE.
            (tidb_datatype::FieldTypeCode::Double.mysql_type(), 0, tidb_protocol::BINARY_DEFAULT_COLLATION_ID),
            // `tag`: CHAR at utf8mb4_bin, positive result-column collation id.
            (tidb_datatype::FieldTypeCode::String.mysql_type(), 0, 46),
        ]
    );
}
