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

//! Go's `ToPB` of a planned MPP fragment, to the byte: the TiFlash reader's
//! table plan (`ExchangeSender` and below) of a statement planned on this
//! node, against the bytes Go's `ToPB` gives the same plan (captured with a
//! testkit probe over `explain`, so fragment generation has not filled the
//! sender's tasks). Only the table id differs between the two catalogs.

use super::*;
use prost::Message;
use tidb_planner_core_operator_physicalop::to_pb::BuildPbContext;
use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_proto::tipb;

fn find_reader(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalTableReader> {
    if let PhysicalPlan::TableReader(reader) = plan {
        return Some(reader);
    }
    plan.children().iter().find_map(find_reader)
}

fn decode_hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

/// The table id of the scan below `executor`, set to `table_id`.
fn set_table_id(executor: &mut tipb::Executor, table_id: i64) {
    if let Some(scan) = executor.tbl_scan.as_mut() {
        scan.table_id = Some(table_id);
    }
    let child = executor
        .exchange_sender
        .as_mut()
        .and_then(|sender| sender.child.as_deref_mut())
        .or_else(|| executor.selection.as_mut().and_then(|selection| selection.child.as_deref_mut()));
    if let Some(child) = child {
        set_table_id(child, table_id);
    }
}

/// Go `PhysicalExchangeSender.ToPB` (with `PhysicalSelection.ToPB` and
/// `PhysicalTableScan.ToPB` below it) of a pushed TiFlash filter and of a
/// bare TiFlash read.
#[test]
fn mpp_fragment_to_pb_is_gos_to_the_byte() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(431)).expect("session opens");
    for sql in ["CREATE TABLE test.t (a int)", "ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1"] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    rows(&mut session, "use test");
    let table_id: i64 = displayed(rows(
        &mut session,
        "select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = 't'",
    ))[0][0]
        .parse()
        .unwrap();

    let cases = [
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ * from t where a + 1 > 20",
            "0809521145786368616e676553656e6465725f313262f202080022cc02080222b3020ae90108904e1a8c0108904e1a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000000120002a1e08081081011801200028c1ffffffffffffffff01320662696e6172794000380020cb012a1e08081080011814200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000001420002a1e08081081011802200028c1ffffffffffffffff01320662696e6172794000380020782a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038001a45080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f3130880100900100520c53656c656374696f6e5f3131880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ a from t",
            "0809521045786368616e676553656e6465725f39626908002244080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b0010018002800400048006000520f5461626c6546756c6c5363616e5f38880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
        ),
    ];
    for (sql, go_hex) in cases {
        rows(&mut session, sql);
        let plan = session.published_plan().unwrap_or_else(|| panic!("{sql} publishes its plan"));
        let reader = find_reader(&plan).unwrap_or_else(|| panic!("{sql} reads through a TableReader"));
        let table_plan = reader.table_plan.as_deref().expect("the reader has a table plan");
        let eval_ctx = tidb_exprctx::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
        let ctx = BuildPbContext {
            expr_ctx: &eval_ctx,
            tiflash_fast_scan: false,
            tiflash_fine_grained_shuffle_batch_size: 8192,
            group_concat_max_len: 1024,
            in_explain_stmt: true,
        };
        let got = table_plan.to_pb(&ctx, tidb_kv::store_type::StoreType::TiFlash).unwrap_or_else(|err| panic!("{sql}: {err:?}"));

        let mut want = tipb::Executor::decode(decode_hex(go_hex).as_slice()).unwrap();
        set_table_id(&mut want, table_id);
        let hex = |executor: &tipb::Executor| executor.encode_to_vec().iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert_eq!(hex(&got), hex(&want), "{sql}\ngot:  {got:?}\nwant: {want:?}");
    }
}
