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

//! A string comparison at the layer that answers rows AND counts the wire.
//!
//! Rows alone cannot see a push-down: the scan source re-applies every pushed
//! conjunct locally, so a predicate that never travelled still answers
//! correctly, just after dragging the relation across the network. And a wire
//! count alone cannot see correctness: a comparison evaluated at the region
//! with the WRONG COLLATOR sends a smaller, wrong set of rows and the local
//! pass cannot put back a row that never arrived. So both are pinned here,
//! against a coprocessor fake that really executes the condition it is sent --
//! with the collator named on the condition's own field type, which is where
//! TiKV reads it from.
//!
//! The fixtures are chosen so a wrong collation is visible: `'A'` and `'a'`
//! are the same value under `utf8mb4_general_ci` and different under
//! `utf8mb4_bin`, and one row is multi-byte so a byte-length error shows up as
//! a missing or extra row rather than as nothing at all.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use tidb_distsql::{QueryDispatch, QueryTransport, TransportRequest};
use tidb_exec::cop_scan::CopScanSource;
use tidb_exec::real_tikv_read::RealTiKvSessionTransportFactory;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SnapshotPairs,
};
use tidb_executor::driver::{run_select_on, Catalog};
use tidb_executor::kv_table::{KvColumn, KvTable};
use tidb_executor::remote_scan::PushdownScanner;
use tidb_executor::storage::StorageError;
use tidb_executor::StmtContext;
use tidb_proto::tipb::{Chunk, DagRequest, ExecType, Expr, ExprType, ScalarFuncSig, SelectResponse};
use tidb_txnkv::Key;

/// The region's rows, as `(id, s)`. `'A'` and `'a'` differ only in case, so a
/// case-insensitive and a binary collation disagree about how many of them
/// `s = 'a'` selects; `'a\u{e9}b'` is multi-byte, so a byte-length or
/// transcoding error changes the answer instead of passing silently.
fn region_rows() -> Vec<(i64, &'static str)> {
    vec![(1, "A"), (2, "a"), (3, "a\u{e9}b"), (4, "B")]
}

#[derive(Debug)]
struct EmptySnapshot;

impl ClusterSnapshot for EmptySnapshot {
    fn get(&mut self, _key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }

    fn scan(
        &mut self,
        _start: &Key,
        _end: &Key,
        _limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        Ok(Vec::new())
    }

    fn start_ts(&self) -> u64 {
        4_242
    }
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


/// What the fake did with one request.
#[derive(Clone, Debug, Default)]
struct Observation {
    /// Conditions in the DAG's Selection.
    conditions: usize,
    /// The signature of the single condition, when there is one.
    signature: Option<i32>,
    /// Rows the fake sent back -- the wire receipt.
    rows_sent: usize,
    /// `DAGRequest.time_zone_name` / `time_zone_offset`, the zone TiKV
    /// evaluates this request's conditions in.
    time_zone: (String, i64),
    /// `kv.Request.Concurrency` and `kv.Request.ResourceGroupName`, two of the
    /// fields Go's `SetFromSessionVars` fills on every read.
    concurrency: isize,
    resource_group_name: String,
}

#[derive(Debug, Default)]
struct FakeRegion {
    observations: Mutex<Vec<Observation>>,
}

struct FakeTransport {
    region: Arc<FakeRegion>,
}

struct FakeResponse {
    subsets: Vec<QueryResultSubset>,
}

impl QueryResponse for FakeResponse {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        Ok(if self.subsets.is_empty() {
            None
        } else {
            Some(self.subsets.remove(0))
        })
    }

    fn close(&mut self) {
        self.subsets.clear();
    }
}

/// Evaluates one `Selection` condition over a row, the way TiKV does it.
///
/// Only the shape this lowering sends is understood: a `*String` comparison
/// between the string column and a constant. **The collator comes from the
/// CONDITION's own field type**, not from the column leaf's -- that is exactly
/// where TiKV's `map_compare_string_sig` reads it, and it is the field a wrong
/// derivation would corrupt.
fn admits(condition: &Expr, value: &str) -> bool {
    let collation_id = condition
        .field_type
        .as_ref()
        .and_then(|ft| ft.collate)
        .expect("the comparison node states its collation");
    let collator = tidb_datatype::get_collator_by_id(
        tidb_datatype::restore_collation_id_if_needed(collation_id),
    );
    let column_first = condition.children[0].tp == Some(ExprType::ColumnRef as i32);
    let constant = &condition.children[usize::from(column_first)];
    assert_eq!(
        constant.tp,
        Some(ExprType::String as i32),
        "the constant travels as raw bytes"
    );
    let literal = constant.val.as_deref().expect("the constant carries bytes");
    let ordering = if column_first {
        collator.compare(value.as_bytes(), literal)
    } else {
        collator.compare(literal, value.as_bytes())
    };
    match ScalarFuncSig::try_from(condition.sig.expect("a signature")).expect("a known signature") {
        ScalarFuncSig::EqString => ordering.is_eq(),
        ScalarFuncSig::NeString => ordering.is_ne(),
        ScalarFuncSig::LtString => ordering.is_lt(),
        ScalarFuncSig::LeString => ordering.is_le(),
        ScalarFuncSig::GtString => ordering.is_gt(),
        ScalarFuncSig::GeString => ordering.is_ge(),
        other => panic!("the fake was sent a signature it cannot evaluate: {other:?}"),
    }
}

impl QueryTransport for FakeTransport {
    type Response = FakeResponse;

    fn send(
        &mut self,
        request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let bytes = request.metadata().data.clone().expect("a DAG request");
        let dag = DagRequest::decode(bytes.as_slice()).expect("the request is a TiDB DAG");
        let scan = dag.executors[0]
            .tbl_scan
            .as_ref()
            .expect("the first executor is the table scan");
        let column_ids: Vec<i64> = scan
            .columns
            .iter()
            .map(|column| column.column_id.unwrap_or(-1))
            .collect();
        let conditions: Vec<Expr> = dag
            .executors
            .iter()
            .filter(|executor| executor.tp == Some(ExecType::TypeSelection as i32))
            .flat_map(|executor| {
                executor
                    .selection
                    .as_ref()
                    .map(|selection| selection.conditions.clone())
                    .unwrap_or_default()
            })
            .collect();

        let mut rows_data = Vec::new();
        let mut sent = 0usize;
        for (id, value) in region_rows() {
            if !conditions
                .iter()
                .all(|condition| admits(condition, value))
            {
                continue;
            }
            for column_id in &column_ids {
                match column_id {
                    2 => {
                        // `CompactBytesFlag`: a zig-zag varint length, then
                        // the bytes -- Go `EncodeCompactBytes`.
                        rows_data.push(2);
                        encode_signed_varint(&mut rows_data, value.len() as i64);
                        rows_data.extend_from_slice(value.as_bytes());
                    }
                    // The handle column (`_tidb_rowid`, id -1) carries the
                    // row's handle, which is the row's `id` here.
                    _ => {
                        rows_data.push(8);
                        encode_signed_varint(&mut rows_data, id);
                    }
                }
            }
            sent += 1;
        }
        self.region.observations.lock().unwrap().push(Observation {
            conditions: conditions.len(),
            signature: conditions.first().and_then(|condition| condition.sig),
            rows_sent: sent,
            time_zone: (
                dag.time_zone_name.clone().unwrap_or_default(),
                dag.time_zone_offset.unwrap_or_default(),
            ),
            concurrency: request.metadata().concurrency,
            resource_group_name: request.metadata().resource_group_name.clone(),
        });

        let response = SelectResponse {
            chunks: vec![Chunk {
                rows_data: Some(rows_data),
                rows_meta: Vec::new(),
            }],
            ..SelectResponse::default()
        };
        Ok(Some(FakeResponse {
            subsets: vec![QueryResultSubset {
                data: response.encode_to_vec(),
                runtime: None,
            }],
        }))
    }
}

struct FakeFactory {
    region: Arc<FakeRegion>,
}

impl RealTiKvSessionTransportFactory for FakeFactory {
    type Transport = FakeTransport;

    fn open_session_transport(&self) -> Result<Self::Transport, String> {
        Ok(FakeTransport {
            region: Arc::clone(&self.region),
        })
    }
}

/// `t(id BIGINT, s VARCHAR(20) COLLATE <collation>)` read through a real
/// [`CopScanSource`].
fn fixture(collation: &str) -> (Catalog, Arc<FakeRegion>) {
    let region = Arc::new(FakeRegion::default());
    let factory = Arc::new(FakeFactory {
        region: Arc::clone(&region),
    });
    let scanner = Arc::new(CopScanSource::new(factory));
    let snapshot: Arc<Mutex<dyn ClusterSnapshot>> = Arc::new(Mutex::new(EmptySnapshot));
    let mut text = FieldType::new(FieldTypeCode::Varchar).with_collation_name(collation);
    text.set_flen(20);
    let columns = vec![
        KvColumn {
            name: "id".to_owned(),
            id: 1,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        },
        KvColumn {
            name: "s".to_owned(),
            id: 2,
            field_type: text,
            column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
            default_value: None,
            origin_default: None,
            comment: String::new(),
            generated: None,
        },
    ];
    let storage = ClusterTableStorage::new(MutationBuffer::new(), snapshot)
        .with_remote_scanner(scanner as Arc<dyn PushdownScanner>);
    let mut catalog = Catalog::default();
    catalog.register_kv("t", KvTable::with_storage(93, columns, Box::new(storage)));
    (catalog, region)
}

/// The rows a `SELECT id, s` returns for the given fixture ids.
fn expected(collation: &str, ids: &[i64]) -> Vec<Vec<Datum>> {
    ids.iter()
        .map(|id| {
            let (_, value) = region_rows()
                .into_iter()
                .find(|(row_id, _)| row_id == id)
                .expect("a fixture id");
            vec![
                Datum::Int(*id),
                Datum::String(tidb_datatype::StringDatum::new(
                    value.as_bytes().to_vec(),
                    tidb_datatype::Collation::from_name(collation)
                        .expect("a known collation"),
                )),
            ]
        })
        .collect()
}

/// The one observation the statement produced.
fn sole_observation(region: &FakeRegion) -> Observation {
    let observations = region.observations.lock().unwrap();
    let [observation] = observations.as_slice() else {
        panic!("exactly one coprocessor request: {observations:?}");
    };
    observation.clone()
}

/// A string comparison narrows the WIRE, and narrows it by the collation the
/// column declares.
///
/// Before the string signatures lowered, this statement sent all four rows and
/// filtered locally; the answer was already right, which is why only the
/// receipt could see the difference. Now the region evaluates the comparison
/// and sends only the rows it selects -- and how many that is depends on the
/// collation, which is the whole risk this path carries.
#[test]
fn a_string_comparison_travels_and_the_collation_decides_which_rows_return() {
    // A case-insensitive collation: `'A'` and `'a'` are one value.
    let (catalog, region) = fixture("utf8mb4_general_ci");
    let rows = run_select_on(
        "SELECT id, s FROM t WHERE s = 'a'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    let observation = sole_observation(&region);
    assert_eq!(
        observation.conditions, 1,
        "the comparison travelled as a Selection condition"
    );
    assert_eq!(observation.signature, Some(ScalarFuncSig::EqString as i32));
    assert_eq!(
        observation.rows_sent, 2,
        "the wire carries only the rows the region's own comparison selected, \
         not the whole relation"
    );
    assert_eq!(rows, expected("utf8mb4_general_ci", &[1, 2]));

    // The same statement over a binary collation: `'A'` is a different value.
    let (catalog, region) = fixture("utf8mb4_bin");
    let rows = run_select_on(
        "SELECT id, s FROM t WHERE s = 'a'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(sole_observation(&region).rows_sent, 1);
    assert_eq!(rows, expected("utf8mb4_bin", &[2]));
}

/// The multi-byte row, selected by a comparison whose answer a byte-wise
/// stand-in would get wrong.
///
/// Under `utf8mb4_general_ci` the accented `'a\u{e9}b'` sorts BELOW `'azb'`,
/// because the collator weighs `\u{e9}` as `e`; a byte comparison puts the
/// two-byte `\u{c3}\u{a9}` sequence above `z`. So the row's presence in the
/// answer, and the wire count that carries it, separate a real UTF-8 collator
/// from a byte one.
#[test]
fn the_multibyte_row_is_selected_by_the_collator_and_not_by_its_bytes() {
    let (catalog, region) = fixture("utf8mb4_general_ci");
    let rows = run_select_on(
        "SELECT id, s FROM t WHERE s < 'azb'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    let observation = sole_observation(&region);
    assert_eq!(observation.signature, Some(ScalarFuncSig::LtString as i32));
    assert_eq!(
        observation.rows_sent, 3,
        "'A', 'a' and the accented row are all below 'azb' to the collator"
    );
    assert_eq!(rows, expected("utf8mb4_general_ci", &[1, 2, 3]));

    // The same statement over the binary collation selects three rows again --
    // but NOT THE SAME THREE. The accented row's bytes put it above 'azb' and
    // it drops out, while 'B' (which the case-insensitive collator weighed as
    // 'b', above 'a') sorts below by its byte value and comes in. A count-only
    // assertion would call these two answers identical, so the ids are what
    // separate them.
    let (catalog, region) = fixture("utf8mb4_bin");
    let rows = run_select_on(
        "SELECT id, s FROM t WHERE s < 'azb'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    assert_eq!(sole_observation(&region).rows_sent, 3);
    assert_eq!(rows, expected("utf8mb4_bin", &[1, 2, 4]));
}

/// The BEFORE half, still reachable: a string comparison this lowering refuses
/// drags the whole relation across the wire and filters locally.
///
/// `latin1_bin` is refused because Go's `inferCollation` applies repertoire
/// rules to a non-`utf8mb4` charset that this lowering does not implement. The
/// ANSWER is unchanged -- the scan source applies every pushed conjunct to
/// every row it emits anyway -- so only the receipt can see the difference,
/// which is the whole reason the count is asserted beside the rows. This is
/// also the shape the first mutation probe produces: neutering the lowering
/// makes every accepted case look like this one.
#[test]
fn a_refused_string_comparison_still_answers_right_and_sends_the_whole_relation() {
    let (catalog, region) = fixture("latin1_bin");
    let rows = run_select_on(
        "SELECT id, s FROM t WHERE s = 'a'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    let observation = sole_observation(&region);
    assert_eq!(
        observation.conditions, 0,
        "the comparison did not travel: no Selection at all"
    );
    assert_eq!(
        observation.rows_sent,
        region_rows().len(),
        "so every row of the relation crossed the wire"
    );
    assert_eq!(
        rows,
        expected("latin1_bin", &[2]),
        "and the answer is right regardless, which is why rows alone cannot \
         see a push-down"
    );
}

/// The request carries the ZONE OF THE STATEMENT THAT ISSUED IT, not a
/// constant fixed when the node booted.
///
/// Go reads it fresh for every request --
/// `dagReq.TimeZoneName, dagReq.TimeZoneOffset =
/// timeutil.Zone(ctx.GetSessionVars().Location())`
/// (`pkg/executor/internal/builder/builder_utils.go`) -- and the scanner this
/// exercises is ONE object shared by every connection of a node, so a zone
/// held there could be neither corrected by `SET time_zone` nor kept private
/// to one connection.
///
/// The two spellings are both pinned because they are not the same field. Go
/// builds a fixed offset as `time.FixedZone("", ofst)`
/// (`timeutil.ParseTimeZone`), whose `String()` -- the value `Zone` returns --
/// is EMPTY: TiKV prefers a non-empty NAME and can only load one a zone
/// database knows, so `"+08:00"` sent as a name is a name that does not
/// resolve. A named zone sends its name and its offset at this instant.
///
/// The row table itself lives in `crate::dag_zone_contract`, shared with the
/// OTHER node type that stamps a DAG request
/// (`real_tikv_read_source::set_time_zone_threads_into_every_subsequent_dag_request`),
/// so the two cannot drift apart: they are one assertion run twice.
#[test]
fn each_request_carries_the_issuing_statements_time_zone() {
    let zoned = |zone: tidb_expr::SessionTimeZone| {
        let (catalog, region) = fixture("utf8mb4_bin");
        let context = StmtContext::for_query().with_clock((0, 0, 0), zone);
        run_select_on("SELECT id, s FROM t WHERE s = 'a'", &catalog, &context)
            .expect("the scan is served by the coprocessor");
        sole_observation(&region).time_zone
    };
    crate::dag_zone_contract::assert_go_dag_zone_contract("cop_scan", |zone| zoned(zone.clone()));
}

/// Go `distsql.RequestBuilder.SetFromSessionVars`, which every read in
/// `pkg/distsql` runs before the request leaves TiDB.
///
/// A zero-value `kv.Request` is not "the defaults": `Concurrency: 0` and an
/// EMPTY `ResourceGroupName` correspond to no TiDB session at all -- a stock
/// one is `tidb_distsql_scan_concurrency = 15` (`DefDistSQLScanConcurrency`)
/// and resource group `default`, which is what `RequestContext::default()`
/// holds and what the resource-control context on the wire is keyed by.
#[test]
fn the_request_carries_the_stock_session_concurrency_and_resource_group() {
    let (catalog, region) = fixture("utf8mb4_bin");
    run_select_on(
        "SELECT id, s FROM t WHERE s = 'a'",
        &catalog,
        &StmtContext::for_query(),
    )
    .expect("the scan is served by the coprocessor");
    let observation = sole_observation(&region);
    assert_eq!(observation.concurrency, 15);
    assert_eq!(observation.resource_group_name, "default");
}
