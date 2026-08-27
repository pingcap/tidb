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

//! Go-parity pins for `pkg/store/mockstore` part1 (`origin/master` slice),
//! testport batch b061 (`pkg/store/mockstore.part1`: every `func Test*` /
//! `func Benchmark*` under `pkg/store/mockstore/**` except part2's tail —
//! `tikv/util_test.go` ×3 and `util/lockwaiter/lockwaiter_test.go` ×2).
//!
//! Verification verdicts after re-deriving each Go test against its source:
//!
//! * Already ported and verified faithful by this batch's re-read: the four
//!   `lockstore_test.go` tests ([`crate::lockstore::tests`],
//!   [`crate::iterator::tests`]), `detector_test.go::TestDeadlock`
//!   ([`crate::detector::tests`]), and all 27 `mvcc_test.go` tests plus
//!   unistore's mock-PD/keyspace surfaces in `tidb-txnkv/src/unistore.rs`.
//!   Three arms those landed ports skip are exercised HERE because the
//!   ported store implements them: `TestCheckTxnStatus`'s mismatching-
//!   primary tail, `TestCheckSecondaryLocksStatus`'s pessimistic-secondary
//!   arm, and `TestMinCommitTs`' second push segment.
//! * Ported here as executable pins: `cop_handler_test.go`'s
//!   `TestIsPrefixNext`, `TestPointGet`, `TestClosureExecutor`.
//! * Divergence found while verifying: `pd_test.go::TestMockPDServiceDiscovery`
//!   expects master's `normalizeMockPDAddrs` to KEEP a `unix://localhost:m0`
//!   endpoint (3 clients); the workspace helper rejects non-http(s) schemes
//!   and pins only 2. Production files are out of this batch's scope, so the
//!   master vectors are pinned below under `#[ignore]`.
//! * Gaps whose subject has no Rust transcreation anywhere in the workspace
//!   (`go-parity-gap`, ignored with the master vectors): the two top-level
//!   integration tests (`cluster_test.go`, `tikv_test.go`),
//!   `mockcopr/executor_test.go`, the MPP/exchanger shapes of
//!   `cop_handler_test.go`, `raw_handler_test.go`, `pd.go`'s global-config
//!   trio and member lister, and `tikv/gc_worker.go`'s GC states manager.
//!   The five `main_test.go` harnesses and four lockstore benchmarks are
//!   recorded as skips with reasons (module tail).

use crate::cophandler::{handle_cop_request, REQ_TYPE_DAG};
use crate::mvcc_store::{KvError, MvccStore, PessimisticLockReq, PrewriteReq};
use prost::Message;
use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_datatype::Datum;
use tidb_proto::{coprocessor, tipb};

/// Go's batch constants (`cop_handler_test.go:40-45`).
const START_TS: u64 = 10;
const LOCK_TTL_COP: u64 = 60_000;
const DAG_REQUEST_START_TS: u64 = 100;
const COP_TABLE_ID: i64 = 0;

/// mysql type ids the Go file builds columns with (`mysql.TypeLonglong`,
/// `mysql.TypeString`, `mysql.TypeDouble`) and `-mysql.DefaultCollationID`
/// (46; the negative marks "restore per type" on the wire).
const TYPE_LONGLONG: i32 = 8;
const TYPE_STRING: i32 = 254;
const TYPE_DOUBLE: i32 = 5;
const NEG_DEFAULT_COLLATION_ID: i32 = -46;

// ---------------------------------------------------------------------------
// Local mirrors of cop_handler_test.go's OWN helpers (they live in the Go
// test file, so their semantics belong to this pin, not to a product API).
// ---------------------------------------------------------------------------

/// Go `convertToPrefixNext` (`cop_handler_test.go:168`): the smallest key
/// strictly larger than the input, computed by suffix increment.
fn convert_to_prefix_next(mut key: Vec<u8>) -> Vec<u8> {
    if key.is_empty() {
        return vec![0];
    }
    for byte in key.iter_mut().rev() {
        if *byte != 255 {
            *byte += 1;
            return key;
        }
        *byte = 0;
    }
    // Every byte was 255: saturate the buffer and grow by one zero byte.
    for byte in key.iter_mut() {
        *byte = 255;
    }
    key.push(0);
    key
}

/// Go `isPrefixNext` (`cop_handler_test.go:184`): prefix-nexting the first
/// argument lands exactly on the second.
fn is_prefix_next(key: &[u8], expected: &[u8]) -> bool {
    convert_to_prefix_next(key.to_vec()) == expected
}

/// The three `tipb.ColumnInfo`s of Go `prepareTestTableData`
/// (`cop_handler_test.go:96-115`): column ids `[1, 2, 3]`, types
/// longlong/string/double, `Collation: -mysql.DefaultCollationID` each.
fn cop_column_infos() -> Vec<tipb::ColumnInfo> {
    [(1, TYPE_LONGLONG), (2, TYPE_STRING), (3, TYPE_DOUBLE)]
        .into_iter()
        .map(|(id, tp)| tipb::ColumnInfo {
            column_id: Some(id),
            tp: Some(tp),
            collation: Some(NEG_DEFAULT_COLLATION_ID),
            ..tipb::ColumnInfo::default()
        })
        .collect()
}

/// Encodes one row value EXACTLY like Go's
/// `tablecodec.EncodeRow(..., &rowcodec.Encoder{Enable: true})`: the NEW
/// row format over datums `(handle, "abc", 10.0)` at column ids `[1,2,3]`.
fn cop_row_value(handle: i64) -> Vec<u8> {
    let row = [
        Datum::Int(handle),
        Datum::new_string("abc"),
        Datum::new_real(10.0),
    ];
    tidb_tablecodec::encode_table_row(None, &row, &[1, 2, 3], true, None).expect("row encodes")
}

/// Go `getTestPointRange` (`cop_handler_test.go:148`): the half-open record
/// range covering exactly one int handle.
fn cop_point_range(table_id: i64, handle: i64) -> coprocessor::KeyRange {
    let start = encode_row_key_with_handle(table_id, &RecordHandle::Int(handle));
    let end = convert_to_prefix_next(start.clone());
    coprocessor::KeyRange {
        start,
        end,
        ..coprocessor::KeyRange::default()
    }
}

/// Commits one test row through prewrite+commit the way Go's `initTestData`
/// does (`cop_handler_test.go:57`): version `START_TS + 2*slot`, commit at
/// +1, ttl 60000, primary = the row key itself.
fn cop_init_row(store: &mut MvccStore, key: &[u8], handle: i64, slot: usize) {
    use tidb_proto::{KvrpcMutation, KvrpcOp};
    let start_version = START_TS + 2 * slot as u64;
    store
        .prewrite(&PrewriteReq {
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::Put as i32,
                key: key.to_vec(),
                value: cop_row_value(handle),
                ..KvrpcMutation::default()
            }],
            primary_lock: key.to_vec(),
            start_version,
            lock_ttl: LOCK_TTL_COP,
            ..PrewriteReq::default()
        })
        .expect("prewrites");
    store
        .commit(&[key.to_vec()], start_version, start_version + 1)
        .expect("commits");
}

/// Writes `(handle, "abc", 10.0)` rows for handles `0..3` into the store,
/// Go `initTestData(store, data.encodedTestKVDatas)` for `keyNumber = 3`.
fn cop_seed_three_rows(store: &mut MvccStore) {
    for handle in 0..3_i64 {
        let key = encode_row_key_with_handle(COP_TABLE_ID, &RecordHandle::Int(handle));
        cop_init_row(store, &key, handle, handle as usize);
    }
}

/// Encodes a DAG request through the protobuf surface and dispatches it the
/// way Go's `newDagContext(..., dagReq, dagRequestStartTs)` +
/// `buildExecutorsAndExecute` pair does: visibility ts comes from the
/// coprocessor request (`dagCtx.startTS = req.StartTs`).
fn run_dag(
    store: &mut MvccStore,
    dag: tipb::DagRequest,
    ranges: Vec<coprocessor::KeyRange>,
) -> tipb::SelectResponse {
    let mut data = Vec::new();
    dag.encode(&mut data).expect("a DAG request encodes");
    let resp = handle_cop_request(
        store,
        &coprocessor::Request {
            tp: REQ_TYPE_DAG,
            data,
            ranges,
            start_ts: DAG_REQUEST_START_TS,
            ..coprocessor::Request::default()
        },
    );
    assert!(resp.other_error.is_empty(), "{}", resp.other_error);
    assert!(resp.locked.is_none(), "no lock was standing");
    tipb::SelectResponse::decode(resp.data.as_slice()).expect("a select response")
}

/// Go's `dagBuilder` chain (`cop_handler_test.go:216+`) narrowed to the
/// shapes those tests build: TableScan over the given columns + output
/// offsets `{0, 1}`, optionally plus Selection / Limit executors.
fn cop_dag(
    scan_columns: Vec<tipb::ColumnInfo>,
    table_id: i64,
    condition: Option<tipb::Expr>,
    limit: Option<u64>,
) -> tipb::DagRequest {
    let mut executors = vec![tipb::Executor {
        tp: Some(tipb::ExecType::TypeTableScan as i32),
        tbl_scan: Some(tipb::TableScan {
            columns: scan_columns,
            table_id: Some(table_id),
            ..tipb::TableScan::default()
        }),
        ..tipb::Executor::default()
    }];
    if let Some(condition) = condition {
        executors.push(tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(tipb::Selection {
                conditions: vec![condition],
                ..tipb::Selection::default()
            }),
            ..tipb::Executor::default()
        });
    }
    if let Some(limit) = limit {
        executors.push(tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(tipb::Limit {
                limit: Some(limit),
                ..tipb::Limit::default()
            }),
            ..tipb::Executor::default()
        });
    }
    tipb::DagRequest {
        executors,
        output_offsets: vec![0, 1],
        ..tipb::DagRequest::default()
    }
}

/// Builds Go's `buildEQIntExpr` (`cop_handler_test.go:434`):
/// ScalarFunc(EQInt)(ColumnRef(offset), Int64(value)); `buildNEIntExpr` is
/// the same shape under [`tidb_proto::tipb::ScalarFuncSig::NeInt`].
fn cop_cmp_int_expr(sig: tipb::ScalarFuncSig, offset: i64, value: i64) -> tipb::Expr {
    let mut column_offset = Vec::new();
    tidb_codec::encode_int(&mut column_offset, offset);
    let mut literal = Vec::new();
    tidb_codec::encode_int(&mut literal, value);
    tipb::Expr {
        tp: Some(tipb::ExprType::ScalarFunc as i32),
        sig: Some(sig as i32),
        children: vec![
            tipb::Expr {
                tp: Some(tipb::ExprType::ColumnRef as i32),
                val: Some(column_offset),
                ..tipb::Expr::default()
            },
            tipb::Expr {
                tp: Some(tipb::ExprType::Int64 as i32),
                val: Some(literal),
                ..tipb::Expr::default()
            },
        ],
        ..tipb::Expr::default()
    }
}

// ---------------------------------------------------------------------------
// Executable ports.
// ---------------------------------------------------------------------------

/// Go `TestIsPrefixNext` (`cop_handler_test.go:286`), every vector: suffix
/// increment stops at the first non-255 byte, rolls 255s over, and grows an
/// all-255 buffer by one trailing zero byte.
#[test]
fn is_prefix_next_lands_on_gos_expected_successors() {
    let cases: [(&str, (&[u8], &[u8])); 11] = [
        ("empty grows a zero byte", (&[], &[0])),
        ("zero increments", (&[0], &[1])),
        ("one increments", (&[1], &[2])),
        ("single ff appends", (&[255], &[255, 0])),
        ("ff run appends", (&[255, 255, 255], &[255, 255, 255, 0])),
        ("carry across", (&[1, 255], &[2, 0])),
        ("deep carry", (&[0, 1, 255], &[0, 2, 0])),
        ("tail plain step", (&[0, 1, 255, 5], &[0, 1, 255, 6])),
        ("tail carry", (&[0, 1, 5, 255], &[0, 1, 6, 0])),
        ("double carry", (&[0, 1, 255, 255], &[0, 2, 0, 0])),
        ("triple carry", (&[0, 255, 255, 255], &[1, 0, 0, 0])),
    ];
    for (name, (key, expected)) in cases {
        assert!(is_prefix_next(key, expected), "{name}");
    }
}

/// Go `TestPointGet` (`cop_handler_test.go:300`): a point range on
/// `math.MinInt64` answers ZERO chunks / zero rows; the same DAG on handle 0
/// answers exactly one row carrying TWO columns whose values equal the
/// written `(0, "abc")` (Go compares through the binary collator).
#[test]
fn point_get_answers_nothing_for_min_int64_and_one_row_for_zero() {
    let mut store = MvccStore::new();
    cop_seed_three_rows(&mut store);

    // Point get on math.MinInt64 returns nothing.
    let dag = cop_dag(cop_column_infos(), COP_TABLE_ID, None, None);
    let select = run_dag(&mut store, dag, vec![cop_point_range(COP_TABLE_ID, i64::MIN)]);
    assert!(
        select.chunks.is_empty(),
        "require.Len(chunks, 0): MinInt64 answers nothing"
    );

    // Point get on handle 0 returns one row with two columns.
    let dag = cop_dag(cop_column_infos(), COP_TABLE_ID, None, None);
    let select = run_dag(&mut store, dag, vec![cop_point_range(COP_TABLE_ID, 0)]);
    assert_eq!(select.chunks.len(), 1);
    let rows_data = select.chunks[0].rows_data.as_deref().expect("rows");
    let returned = tidb_codec::decode(rows_data, 2).expect("codec.Decode(rowsData, 2)");
    assert_eq!(returned.len(), 2, "returned row should have 2 cols");
    let (first, second) = (&returned[0], &returned[1]);
    // The string column decodes byte-preserving; Go compares it through the
    // BINARY collator, so the pinned content is exactly these bytes.
    match first {
        Datum::Int(value) => assert_eq!(*value, 0),
        other => panic!("unexpected first column: {other:?}"),
    }
    let text = match second {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(text) => text.bytes(),
        other => panic!("unexpected second column: {other:?}"),
    };
    assert_eq!(text, b"abc", "the string column survives");
}

/// Go `TestClosureExecutor` (`cop_handler_test.go:357`): the composed
/// executor chain `TableScan + Selection(EQInt(offset 1, -1)) + Limit(1)`
/// over a point range on handle 1 answers ZERO rows — nothing survives the
/// pushed selection.
#[test]
fn closure_executor_selection_over_point_range_answers_zero_rows() {
    let mut store = MvccStore::new();
    cop_seed_three_rows(&mut store);

    let dag = cop_dag(
        cop_column_infos(),
        COP_TABLE_ID,
        Some(cop_cmp_int_expr(tipb::ScalarFuncSig::EqInt, 1, -1)),
        Some(1),
    );
    let select = run_dag(&mut store, dag, vec![cop_point_range(COP_TABLE_ID, 1)]);
    // require.NoError(err); require.Equal(rowCount, 0).
    let rows: usize = select
        .chunks
        .iter()
        .filter_map(|chunk| chunk.rows_data.as_deref())
        .map(|rows| tidb_codec::decode(rows, 2).expect("a row decodes").len())
        .sum();
    assert_eq!(rows, 0, "the selection filters everything away");
}

// ---------------------------------------------------------------------------
// Arms of mvcc_test.go whose subjects exist on the ported store although the
// landed ports skipped them — executed through the public store surface.
// ---------------------------------------------------------------------------

/// Go `TestCheckTxnStatus`'s tail (`mvcc_test.go:794-806`): when the lock at
/// `lock_ts` belongs to ANOTHER transaction — its primary differs from the
/// checked key — the check fails with `ErrPrimaryMismatch`; the pessimistic
/// rollback that follows clears the key again.
#[test]
fn check_txn_status_reports_a_foreign_primary_and_rolls_it_back() {
    use tidb_proto::{KvrpcMutation, KvrpcOp};

    let mut store = MvccStore::new();
    let pk_here = b"tpk";
    let primary_there = b"another_key";
    let start_ts = 43_u64;
    store
        .pessimistic_lock(&PessimisticLockReq {
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::PessimisticLock as i32,
                key: pk_here.to_vec(),
                ..KvrpcMutation::default()
            }],
            primary_lock: primary_there.to_vec(),
            start_version: start_ts,
            for_update_ts: start_ts,
            ..PessimisticLockReq::default()
        })
        .expect("locks");

    let err = store
        .check_txn_status(&crate::mvcc_store::CheckTxnStatusReq {
            primary_key: pk_here.to_vec(),
            lock_ts: start_ts,
            caller_start_ts: 44,
            current_ts: 44,
            rollback_if_not_exist: true,
            ..crate::mvcc_store::CheckTxnStatusReq::default()
        })
        .expect_err("the mismatch refuses");
    let KvError::PrimaryMismatch { key, lock } = err else {
        panic!("expected ErrPrimaryMismatch, got {err:?}")
    };
    assert_eq!(key, pk_here);
    assert_eq!(lock.primary_lock, primary_there);

    // MustPessimisticRollback(pk, startTs, startTs) leaves the key free:
    // a fresh optimistic prewrite lands where the standing lock used to be.
    store.pessimistic_rollback(&[pk_here.to_vec()], start_ts, start_ts);
    let start_version = 44_u64;
    store
        .prewrite(&PrewriteReq {
            mutations: vec![{
                use tidb_proto::{KvrpcMutation, KvrpcOp};
                KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: pk_here.to_vec(),
                    value: b"val".to_vec(),
                    ..KvrpcMutation::default()
                }
            }],
            primary_lock: pk_here.to_vec(),
            start_version,
            lock_ttl: 100,
            ..PrewriteReq::default()
        })
        .expect("the key freed up");
    store
        .commit(&[pk_here.to_vec()], start_version, start_version + 1)
        .expect("commits");
}

/// Go `TestCheckSecondaryLocksStatus`'s pessimistic arm
/// (`mvcc_test.go:784-792`): a standing PESSIMISTIC lock among the reported
/// secondary keys is rolled back BY the check itself — no locks come back,
/// commit ts stays zero, and a rollback record takes its place so even the
/// original transaction cannot prewrite there anymore.
#[test]
fn check_secondary_locks_rolls_back_a_pessimistic_secondary() {
    use tidb_proto::{KvrpcMutation, KvrpcOp};

    let mut store = MvccStore::new();
    let (pk, secondary) = (b"pk".as_slice(), b"secondary".as_slice());
    store
        .pessimistic_lock(&PessimisticLockReq {
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::PessimisticLock as i32,
                key: secondary.to_vec(),
                ..KvrpcMutation::default()
            }],
            primary_lock: pk.to_vec(),
            start_version: 11,
            for_update_ts: 11,
            ..PessimisticLockReq::default()
        })
        .expect("locks");

    let status = store
        .check_secondary_locks(&[secondary.to_vec()], 11)
        .expect("checks");
    assert!(status.locks.is_empty());
    assert_eq!(status.commit_ts, 0);

    // MustGetRollback(secondary, 11): the tombstone AND the dead lock are
    // observable together — a re-prewrite of the same transaction dies on
    // the rollback record rather than colliding with any lock.
    let err = store
        .prewrite(&PrewriteReq {
            mutations: vec![KvrpcMutation {
                op: KvrpcOp::Put as i32,
                key: secondary.to_vec(),
                value: b"val".to_vec(),
                ..KvrpcMutation::default()
            }],
            primary_lock: secondary.to_vec(),
            start_version: 11,
            lock_ttl: 200,
            ..PrewriteReq::default()
        })
        .expect_err("the tombstone stands");
    assert_eq!(err, KvError::AlreadyRollback);
}

/// Go `TestMinCommitTs` whole (`mvcc_test.go:1191`): both segments. First a
/// push past caller 20 floors commits to 21; then a re-prewrite at ts 30 is
/// pushed by caller 40 to floor 41 and STILL commits once above it (at 50).
#[test]
fn min_commit_ts_pushes_keep_the_floor_until_a_commit_above_it() {
    use tidb_proto::{KvrpcMutation, KvrpcOp, KvrpcTxnAction};

    let mut store = MvccStore::new();
    let (k, v) = (b"tk".as_slice(), b"v".as_slice());
    let put_at = |store: &mut MvccStore, start_version: u64, min_commit_ts: u64| {
        store
            .prewrite(&PrewriteReq {
                mutations: vec![KvrpcMutation {
                    op: KvrpcOp::Put as i32,
                    key: k.to_vec(),
                    value: v.to_vec(),
                    ..KvrpcMutation::default()
                }],
                primary_lock: k.to_vec(),
                start_version,
                lock_ttl: 100,
                min_commit_ts,
                ..PrewriteReq::default()
            })
            .expect("prewrites");
    };
    let push_at = |store: &mut MvccStore, lock_ts: u64, caller: u64| {
        store
            .check_txn_status(&crate::mvcc_store::CheckTxnStatusReq {
                primary_key: k.to_vec(),
                lock_ts,
                caller_start_ts: caller,
                current_ts: caller,
                ..crate::mvcc_store::CheckTxnStatusReq::default()
            })
            .expect("MustCheckTxnStatus")
    };

    // Segment one: min_commit_ts 11, caller 20 pushes; commits at/below the
    // pushed floor refuse and the commit above it lands.
    put_at(&mut store, 10, 11);
    let status = push_at(&mut store, 10, 20);
    assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
    assert_eq!(status.lock_info.as_ref().map(|l| l.lock_ttl), Some(100));
    assert!(store.commit(&[k.to_vec()], 10, 15).is_err());
    assert!(store.commit(&[k.to_vec()], 10, 20).is_err());
    store.commit(&[k.to_vec()], 10, 21).expect("lands");

    // Segment two: min_commit_ts 30 at start ts 30; caller 40 pushes the
    // floor to 41; committing at 50 succeeds.
    put_at(&mut store, 30, 30);
    let status = push_at(&mut store, 30, 40);
    assert_eq!(status.action, KvrpcTxnAction::MinCommitTsPushed);
    store.commit(&[k.to_vec()], 30, 50).expect("above the push");
}

// ---------------------------------------------------------------------------
// go-parity-gap pins: the subject has NO Rust transcreation anywhere in the
// workspace. Each body carries the master vectors verbatim so that landing
// the transcreation can flip #[ignore] into a runnable check.
// ---------------------------------------------------------------------------

/// Go `TestMockPDServiceDiscovery` (`pd_test.go:101`). Master's
/// `normalizeMockPDAddrs` keeps every address `pkg/util.NormalizeServiceURL`
/// accepts, and master accepts unix family endpoints verbatim — three
/// clients with URLs `http://127.0.0.1:2379`, `http://172.32.21.32:2379`,
/// `unix://localhost:m0`. The workspace helper rejects them.
#[test]
#[ignore = "go-parity-gap: pkg/util/service_url.go NormalizeServiceURL is not transcreated; \
           tidb-txnkv's normalize_mock_pd_url rejects unix:// so only 2 of Go's 3 clients exist"]
fn mock_pd_service_discovery_keeps_the_unix_endpoint() {
    // ("invalid_pd_address", invalid)
    // ("127.0.0.1:2379", http://127.0.0.1:2379)
    // ("http://172.32.21.32:2379", http://172.32.21.32:2379)
    // ("unix://localhost:m0", unix://localhost:m0)
    panic!("blocked on transcreating pkg/util/service_url.go (NormalizeServiceURL)");
}

/// Go `TestGetAllMembersUsesInjectedPDAddrs` (`pd_test.go:121`): a client
/// built over normalized injected addrs answers GetMembersResponse with one
/// member per addr — each member's ClientUrls holds exactly that URL — and
/// the leader is members[0].
#[test]
#[ignore = "go-parity-gap: unistore pdClient.GetAllMembers has no Rust transcreation yet"]
fn get_all_members_uses_injected_pd_addrs() {
    // addrs ["127.0.0.1:2379", "unix://localhost:m0"]:
    // members.len()==2; members[0].client_urls==["http://127.0.0.1:2379"];
    // members[1].client_urls==["unix://localhost:m0"]; leader==members[0].
    panic!("blocked on transcreating pkg/store/mockstore/unistore/pd.go GetAllMembers");
}

/// Go `TestLoad` (`pd_test.go:41`): storing `LoadOkGlobalConfig=ok` makes
/// LoadGlobalConfig answer `/global/config/LoadOkGlobalConfig`="ok" while a
/// never-stored name answers its normalized key with an EMPTY value.
#[test]
#[ignore = "go-parity-gap: unistore pdClient.LoadGlobalConfig/StoreGlobalConfig have no Rust transcreation yet"]
fn load_global_config_answers_stored_values_and_blank_misses() {
    // Store([{"LoadOkGlobalConfig","ok"}])
    // Load(["LoadOkGlobalConfig","LoadErrGlobalConfig"]) =>
    //   ("/global/config/LoadOkGlobalConfig", "ok")
    //   ("/global/config/LoadErrGlobalConfig", "")
    panic!("blocked on transcreating pkg/store/mockstore/unistore/pd.go Load/StoreGlobalConfig");
}

/// Go `TestStore` (`pd_test.go:61`): loading before storing answers "", the
/// store writes through, the reload answers "ok".
#[test]
#[ignore = "go-parity-gap: unistore pdClient.LoadGlobalConfig/StoreGlobalConfig have no Rust transcreation yet"]
fn store_global_config_round_trips_new_object() {
    // Load(["NewObject"]) => ""; Store([{"NewObject","ok"}]); Load => "ok".
    panic!("blocked on transcreating pkg/store/mockstore/unistore/pd.go Load/StoreGlobalConfig");
}

/// Go `TestWatch` (`pd_test.go:78`): WatchGlobalConfig streams 10 bursts of
/// every stored item before closing; each delivered event's name and value
/// are non-empty.
#[test]
#[ignore = "go-parity-gap: unistore pdClient.WatchGlobalConfig has no Rust transcreation yet"]
fn watch_global_config_streams_ten_bursts_of_every_item() {
    // Store("/global/config", {"NewObject":"ok"}); Watch(revision 0);
    // drain exactly 10 events, each res[0].Name != "" && Value != "".
    panic!("blocked on transcreating pkg/store/mockstore/unistore/pd.go WatchGlobalConfig");
}

/// Go `TestRawHandler` (`raw_handler_test.go:27`): put/get/delete one key,
/// batch put/get/delete three pairs preserving order, scan `[keys[0],
/// keys[9])` limit 2 answering the first two stored pairs, delete-range
/// wiping them, and an empty scan afterwards.
#[test]
#[ignore = "go-parity-gap: pkg/store/mockstore/unistore/raw_handler.go has no Rust transcreation (kv_handler.rs seeds server.go only)"]
fn raw_handler_round_trips_raw_kv_ops_in_order() {
    // keys[i]="key{i}", vals[i]="val{i}", i in 0..10:
    //   put(k0,v0); get(k0)==v0; del(k0)
    //   batchPut[(k1,v1),(k3,v3),(k5,v5)]; batchGet[k1,k3,k5]==same pairs
    //   batchDel[k1,k3,k5]
    //   batchPut[(k6,v6),(k7,v7),(k8,v8)]
    //   scan(k0..k9, limit 2).kvs == [(k6,v6),(k7,v7)]
    //   deleteRange(k0..k9); scan(k0..k9).kvs == []
    panic!("blocked on transcreating pkg/store/mockstore/unistore/raw_handler.go");
}

/// Go `TestClusterSplit` (`cluster_test.go`): 1000 committed rows plus index
/// entries split into 10 regions apiece leave 12 regions total; scanning
/// each region slice inside the record/index prefix yields exactly 100-row
/// pages whose union covers all 1000 keys of each kind.
#[test]
#[ignore = "go-parity-gap: rides client-go testutils NewMockTiKV/MockCluster.SplitKeys across SQL-free mvcc scans; no hostable seam exists in this workspace (third_party MockCluster is not reachable from any crate without manifest edits outside this batch's scope)"]
fn cluster_split_regions_cover_every_record_and_index_key() {
    // tblID=1 idxID=2 colID=3: 1000 rows value=strconv.Itoa(handle) in new
    // format; index entry key=EncodeIndexSeekKey(tblID,idxID,EncodeKey(str,
    // handle)) value "0"; commit at ts2.
    // SplitKeys(recordPrefix, PrefixNext(recordPrefix), 10):
    // GetAllRegions() == 12 regions; per-prefix-region scan length == 100
    // when non-empty; unions contain exactly the 1000 record / index keys.
    panic!("blocked on hosting TestClusterSplit over a client-go seam");
}

/// Go `TestConfig` (`tikv_test.go`): `MockTiKVDriver.Open("mocktikv://")`
/// follows `txn_local_latches.enabled` from the global config (latches on→
/// `IsLatchEnabled()` true, off→false), while `driver.Open(":")` and
/// `driver.Open("faketikv://")` fail.
#[test]
#[ignore = "go-parity-gap: MockTiKVDriver.Open / IsLatchEnabled have no Rust transcreation (node_config treats mocktikv as unsupported store)"]
fn config_mock_tikv_driver_pins_txn_local_latches_and_bad_urls() {
    // UpdateGlobal{TxnLocalLatches{enabled:true, capacity:10240}} ->
    // Open("mocktikv://").IsLatchEnabled() == true; enabled:false -> false;
    // Open(":") errors; Open("faketikv://") errors.
    panic!("blocked on transcreating pkg/store/mockstore/tikv.go driver surface");
}

/// Go `TestResolvedLargeTxnLocks` (`mockcopr/executor_test.go:44`): a reader
/// meeting the secondary lock of a large txn (primary TTL 100s, secondary
/// TTL 200ms, min_commit_ts=start+1) resolves instead of blocking — plain
/// scan, BatchGet (`id in (1)`), and PointGet under begin/rollback all
/// answer the earlier committed row; afterwards MVCCStore.Scan still
/// reports the large primary's lock alive (ErrLocked).
#[test]
#[ignore = "go-parity-gap: rides session BootstrapSession + distsql over mock TiKV + failpoint DisablePaging; none of those seams exist in this workspace"]
fn resolved_large_txn_secondary_locks_do_not_block_readers() {
    // create table t(id int pk, val int); insert (1,1); tso=t.
    // Prewrite(primary,"value",ttl 100000,minCommitTs=t+1);
    // Prewrite(rowKey,"value",ttl 200,primary="primary").
    // select *, BatchGet, PointGet all yield "1 1"; Scan(rowKey,t)[0].Err
    // remains ErrLocked against the large txn's primary.
    panic!("blocked on the mockcopr + session integration seam");
}

/// Go `TestMppExecutor` (`cop_handler_test.go:385`): driving
/// TableScan + Selection(EQInt(offset 1, 1)) + Limit(1) over the point range
/// through `buildAndRunMPPExecutor(dagCtx, dagReq, pagingSize=0)` with
/// CollectRangeCounts answers counts[0] == 1 — the PER-RANGE SCANNED-row
/// count, not the filtered output — and no error.
#[test]
#[ignore = "go-parity-gap: mppExecBuilder/buildAndRunMPPExecutor (cophandler/mpp.go, mpp_exec.go) has no Rust transcreation"]
fn mpp_executor_counts_one_scanned_row_for_the_point_range() {
    // Same three seeded rows and request-start-ts DAG shape as
    // TestClosureExecutor, plus setCollectRangeCounts(true);
    // buildAndRunMPPExecutor => rowCount[0]==counts[0]==1 && err==nil.
    panic!("blocked on transcreating cophandler/mpp.go");
}

/// Go `BenchmarkExecutors` (`cop_handler_test.go:551`): over the triangular
/// grid rows,limit ∈ {1,10,…,100000}² with limit<=rows, prepare NE-filtered
/// DAGs (Selection NEInt(offset 0, 1), Limit(limit), CollectRangeCounts,
/// request ts 3000000, full-range [handle 0, handle rows)) on fresh stores
/// and drive buildAndRunMPPExecutor without error.
#[test]
#[ignore = "go-parity-gap: timed bench harness over the untranscreated MPP executor; its correctness content reduces to shapes already pinned above"]
fn benchmark_executors_runs_the_whole_grid_without_error() {
    // for rows in {1,10,100,1000,10000,100000} { for limit <= rows {
    //   prepare(rows, limit); run(dagReq, dagCtx) must not error } }
    panic!("blocked on transcreating cophandler/mpp.go");
}

/// Go `TestExchSenderExecNextReturnsWhenCtxCanceledBeforeTunnelConnected`
/// (`cop_handler_test.go:641`): `exchSenderExec.next()` must RETURN rather
/// than block on `connectedCh` when the request context is canceled before
/// any tunnel connects — pinned in Go with a goroutine racing a 1-second
/// deadline.
#[test]
#[ignore = "go-parity-gap: exchSenderExec/ExchangerTunnel (cophandler/mpp_exec.go) has no Rust transcreation"]
fn exch_sender_next_returns_when_context_cancels_before_connect() {
    // tunnel{dataCh cap 1, connectedCh open, errCh cap 1}; spawn next();
    // cancel(); the spawned next() completes within 1s.
    panic!("blocked on transcreating cophandler/mpp_exec.go");
}

/// Go `TestMockGCStatesManager` (`tikv/mock_pd_test.go:26`), looped over
/// keyspace ids {NullKeyspaceID, 0, 1}: a fresh GC state carries safe points
/// 0 and one bootstrap GC barrier which ExcludeGCBarriers hides; advancing
/// txn/GC safe points moves 0→10 and 0→9 while refusing decreases and gc-ts
/// beyond txn-ts; SetGCBarrier validates arguments (non-empty id, ts>0,
/// nonzero TTL) and holds tsp below barrier ts; DeleteGCBarrier frees it.
#[test]
#[ignore = "go-parity-gap: newGCStatesManager (tikv/gc_worker.go) has no Rust transcreation; gc_state.rs transcreates client-go's CLIENT half, not this owner side"]
fn mock_gc_states_manager_barriers_hold_and_release_safe_points() {
    // Per keyspace id: GetGCState(false) → gc/txn safe point 0, one
    // barrier, empty list; GetGCState(default excl) → HasGCBarriers false,
    // GetGCBarriers errors; AdvanceTxnSafePoint(10)/AdvanceGCSafePoint(9)
    // move old→new; declines at 9 / 8 / gc 11; SetGCBarrier("",20,never),
    // ("b1",0,never), ("b1",20,0), ("b1",9,never) all error;
    // SetGCBarrier("b2",25,never) ok; tsp advance to target 30 blocks at
    // 25 naming b2; DeleteGCBarrier("b2") returns b2@25; advance→30 ok.
    panic!("blocked on transcreating tikv/gc_worker.go GCStatesManager");
}

// ---------------------------------------------------------------------------
// Recorded skips needing no executable artifact here.
//
// * The five `func TestMain` harnesses (`mockstore`, `mockcopr`,
//   `cophandler`, `lockstore`, unistore root) are goleak + testsetup
//   boilerplate: they fail the package when goroutines leak and stage global
//   fixtures. No analogue exists in this crate's harness; crate precedent
//   (lib.rs header) records main_test.go files as skipped for this reason.
// * The four lockstore benchmarks (`BenchmarkMemStoreDeleteInsertGet`,
//   `BenchmarkMemStoreIterate`, `BenchmarkPutWithHint`, `BenchmarkPut`) are
//   timing loops over Put/Delete/Get/iterate — operations the ported unit
//   tests pin semantically. Skipped-reason: timing harnesses with no
//   cargo-bench counterpart in scope (lib.rs "Not ported").
