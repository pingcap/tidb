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

/// Go's walk of the table plan: every `ExchangeSender`, parents first.
fn collect_senders<'a>(plan: &'a PhysicalPlan, senders: &mut Vec<&'a PhysicalPlan>) {
    if matches!(plan, PhysicalPlan::ExchangeSender(_)) {
        senders.push(plan);
    }
    for child in plan.children() {
        collect_senders(child, senders);
    }
}

/// Every executor id below `executor`, its operator name kept.
fn clear_executor_ids(executor: &mut tipb::Executor) {
    if let Some(id) = executor.executor_id.as_mut() {
        if let Some(underscore) = id.rfind('_') {
            id.truncate(underscore);
        }
    }
    for child in executor_children(executor) {
        clear_executor_ids(child);
    }
}

fn executor_children(executor: &mut tipb::Executor) -> Vec<&mut tipb::Executor> {
    [
        executor.exchange_sender.as_mut().and_then(|e| e.child.as_deref_mut()),
        executor.selection.as_mut().and_then(|e| e.child.as_deref_mut()),
        executor.projection.as_mut().and_then(|e| e.child.as_deref_mut()),
        executor.aggregation.as_mut().and_then(|e| e.child.as_deref_mut()),
        executor.sort.as_mut().and_then(|e| e.child.as_deref_mut()),
        executor.window.as_mut().and_then(|e| e.child.as_deref_mut()),
    ]
    .into_iter()
    .flatten()
    .chain(executor.join.as_mut().map_or(&mut [][..], |join| join.children.as_mut_slice()).iter_mut())
    .collect()
}

/// Each scan's table id below `executor`, mapped from Go's catalog to this
/// node's.
fn set_table_ids(executor: &mut tipb::Executor, table_ids: &[(i64, i64)]) {
    let ours = |go_id: Option<i64>| table_ids.iter().find(|(go, _)| Some(*go) == go_id).map(|(_, ours)| *ours);
    if let Some(scan) = executor.tbl_scan.as_mut() {
        scan.table_id = ours(scan.table_id);
    }
    if let Some(scan) = executor.partition_table_scan.as_mut() {
        scan.table_id = ours(scan.table_id);
    }
    for child in executor_children(executor) {
        set_table_ids(child, table_ids);
    }
}

/// Go `PhysicalExchangeSender.ToPB` (with `PhysicalSelection.ToPB` and
/// `PhysicalTableScan.ToPB` below it) of a pushed TiFlash filter, a bare
/// TiFlash read, a two-phase aggregation, a broadcast join, two window
/// functions over a partial sort and a partitioned table's dynamic-pruning
/// scan (each of their fragments' senders).
#[test]
fn mpp_fragment_to_pb_is_gos_to_the_byte() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(431)).expect("session opens");
    for sql in [
        "CREATE TABLE test.t (a int)",
        "ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1",
        "CREATE TABLE test.s (b int, c int)",
        "ALTER TABLE test.s SET HYPO TIFLASH REPLICA 1",
        "CREATE TABLE test.p (a int, b int) PARTITION BY HASH(a) PARTITIONS 2",
        "ALTER TABLE test.p SET HYPO TIFLASH REPLICA 1",
    ] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    rows(&mut session, "use test");
    let table_id = |session: &mut Session, name: &str| -> i64 {
        displayed(rows(
            session,
            &format!("select tidb_table_id from information_schema.tables where table_schema = 'test' and table_name = '{name}'"),
        ))[0][0]
            .parse()
            .unwrap()
    };
    // Go's probe catalog: t is 117, s is 119, p is 121.
    let table_ids =
        [(117, table_id(&mut session, "t")), (119, table_id(&mut session, "s")), (121, table_id(&mut session, "p"))];

    let cases: [(&str, &[&str]); 7] = [
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ * from t where a + 1 > 20",
            &["0809521145786368616e676553656e6465725f313262f202080022cc02080222b3020ae90108904e1a8c0108904e1a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000000120002a1e08081081011801200028c1ffffffffffffffff01320662696e6172794000380020cb012a1e08081080011814200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000001420002a1e08081081011802200028c1ffffffffffffffff01320662696e6172794000380020782a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038001a45080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f3130880100900100520c53656c656374696f6e5f3131880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040"],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ a from t",
            &["0809521045786368616e676553656e6465725f39626908002244080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b0010018002800400048006000520f5461626c6546756c6c5363616e5f38880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040"],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ a, count(*) from t group by a",
            &[
                "0809521145786368616e676553656e6465725f333962b005080022ea04080b520c50726f6a656374696f6e5f356ad1040a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038000a3108c9011208800000000000000020002a1e08081081011815200028c1ffffffffffffffff01320662696e617279400038001ae903080b520d50726f6a656374696f6e5f33356acf030a3108c9011208800000000000000020002a1e08081081011815200028c1ffffffffffffffff01320662696e617279400038000a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001ae70208032acf020a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e61727940003800125c08ba171a3108c9011208800000000000000020002a1e08081081011815200028c1ffffffffffffffff01320662696e6172794000380020002a1e08081081011815200028c1ffffffffffffffff01320662696e617279400038004801125a08be171a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380048011800325f080a423f121e08081081011815200028c1ffffffffffffffff01320662696e6172794000121d08031000180b200028c1ffffffffffffffff01320662696e6172794000521345786368616e676552656365697665725f333888010090018040520a486173684167675f333688010090018040880100900100880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321e08081081011815200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
                "0809521145786368616e676553656e6465725f333762890308021a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380022f20108032ada010a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e61727940003800125b08b9171a3008011208800000000000000120002a1e08081081011801200028c1ffffffffffffffff01320662696e6172794000380020002a1e08081081011815200028c1ffffffffffffffff01320662696e61727940003800480218003245080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f32328801009001003800520a486173684167675f3333880100900180402a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000321e08081081011815200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380188010090018040",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t, s]) */ t.a, s.c from t join s on t.a = s.b where s.c > 1",
            &[
                "0809521145786368616e676553656e6465725f353162da060800229506080b520d50726f6a656374696f6e5f35306afb050a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038000a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a9405080b520d50726f6a656374696f6e5f34376afa040a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038000a3008c9011208800000000000000220002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a930408074afa03080010001a5e080a423e121d08031000180b200028c1ffffffffffffffff01320662696e6172794000121d08031000180b200028c1ffffffffffffffff01320662696e6172794000521345786368616e676552656365697665725f3332880100900180401ae901080222d0010a860108904e1a5b08904e1a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380020ac182a1f0808108181201801200028c1ffffffffffffffff01320662696e6172794000380020a0182a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038001a45080012290875121b0801100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f3333880100900100520c53656c656374696f6e5f333488010090010020002a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e61727940003800323008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038003a1f0808108081201801200028c1ffffffffffffffff01320662696e6172794000421f0808108081201801200028c1ffffffffffffffff01320662696e61727940006800520b486173684a6f696e5f343688010090018040880100900100880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
                "0809521145786368616e676553656e6465725f333162da030801229503080222fc020a860108904e1a5b08904e1a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380020ac182a1f0808108181201801200028c1ffffffffffffffff01320662696e6172794000380020a0182a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038000a8c0108904e1a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000000120002a1e08081081011801200028c1ffffffffffffffff01320662696e6172794000380020782a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038001a62080012460877121b0801100318c1ffffffffffffffff01200b28003000a80100b00100121b0802100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f3239880100900100520c53656c656374696f6e5f3330880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380188010090018040",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[s]) */ b, row_number() over (partition by b order by c) from s",
            &[
                "0809521145786368616e676553656e6465725f323462eb04080022a604080b520c50726f6a656374696f6e5f386a8d040a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038000a3008c9011208800000000000000220002a1d080810011815200028c1ffffffffffffffff01320662696e617279400038001aa603080e520957696e646f775f323382018e030a2608a11f20002a1d080810011815200028c1ffffffffffffffff01320662696e6172794000380012340a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010001a340a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010002212080012060802100018001a060802100018002ae301080d5207536f72745f31357ace010a340a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010000a340a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e61727940003800100010011a5e080a423e121d08031000180b200028c1ffffffffffffffff01320662696e6172794000121d08031000180b200028c1ffffffffffffffff01320662696e6172794000521345786368616e676552656365697665725f3134880108900180408801089001804088010890018040880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d080810011815200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
                "0809521145786368616e676553656e6465725f313362f70108021a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038002262080012460877121b0801100318c1ffffffffffffffff01200b28003000a80100b00100121b0802100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f31328801009001002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380188010890018040",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[s]) */ b, sum(c) over (partition by b order by c rows between 1 preceding and current row) from s",
            &[
                "0809521145786368616e676553656e6465725f323462d2050800228b05080b520c50726f6a656374696f6e5f386af2040a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038000a3208c9011208800000000000000220002a1f08f6011080011820200028c1ffffffffffffffff01320662696e617279400038001a8904080e520957696e646f775f32338201f1030a880108ba171a5e08904e120208001a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380020042a1f08f601108001180a200028c1ffffffffffffffff01320662696e6172794000380020002a1f08f6011080011820200028c1ffffffffffffffff01320662696e6172794000380012340a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010001a340a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010002212080012060801100018011a060802100018002ae301080d5207536f72745f31357ace010a340a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000380010000a340a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e61727940003800100010011a5e080a423e121d08031000180b200028c1ffffffffffffffff01320662696e6172794000121d08031000180b200028c1ffffffffffffffff01320662696e6172794000521345786368616e676552656365697665725f3134880108900180408801089001804088010890018040880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321f08f6011080011820200028c1ffffffffffffffff01320662696e6172794000380088010090018040",
                "0809521145786368616e676553656e6465725f313362f70108021a3008c9011208800000000000000020002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038002262080012460877121b0801100318c1ffffffffffffffff01200b28003000a80100b00100121b0802100318c1ffffffffffffffff01200b28003000a80100b001001800280040004800600052105461626c6546756c6c5363616e5f31328801009001002a1d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380188010890018040",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[p]) */ a, b from p where b > 3",
            &["0809521145786368616e676553656e6465725f313262cd020800228802080222ef010a8c0108904e1a3008c9011208800000000000000120002a1d08031000180b200028c1ffffffffffffffff01320662696e617279400038001a3008011208800000000000000320002a1e08081081011801200028c1ffffffffffffffff01320662696e6172794000380020782a1f0808108081201801200028c1ffffffffffffffff01320662696e617279400038001a5e080c52105461626c6546756c6c5363616e5f313072420879121b0801100318c1ffffffffffffffff01200b28003000a80100b00100121b0802100318c1ffffffffffffffff01200b28003000a80100b00100180038005000880100900100520c53656c656374696f6e5f3131880100900100321d08031000180b200028c1ffffffffffffffff01320662696e6172794000321d08031000180b200028c1ffffffffffffffff01320662696e6172794000380088010090018040"],
        ),
    ];
    for (sql, go_hexes) in cases {
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
            use_dynamic_partition_prune: true,
        };
        let mut senders = Vec::new();
        collect_senders(table_plan, &mut senders);
        assert_eq!(senders.len(), go_hexes.len(), "{sql}");
        for (sender, go_hex) in senders.into_iter().zip(go_hexes) {
            let got = sender
                .to_pb(&ctx, tidb_kv::store_type::StoreType::TiFlash)
                .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
            let mut got = got;
            let mut want = tipb::Executor::decode(decode_hex(go_hex).as_slice()).unwrap();
            set_table_ids(&mut want, &table_ids);
            // The MPP exploration numbers plans differently from Go's (a
            // planner divergence, `docs/mpp-execplan.md`); these executor
            // ids are compared as Go's shape, not its numbers.
            if sql.contains("group by") || sql.contains(" join ") || sql.contains(" over ") {
                clear_executor_ids(&mut got);
                clear_executor_ids(&mut want);
            }
            let hex = |executor: &tipb::Executor| executor.encode_to_vec().iter().map(|b| format!("{b:02x}")).collect::<String>();
            assert_eq!(hex(&got), hex(&want), "{sql}\ngot:  {got:?}\nwant: {want:?}");
        }
    }
}

/// Go `projInjector.inject` recursing into a TiFlash reader's table plan:
/// an MPP aggregate's `SUM` argument gets `WrapCastForAggArgs`' cast and the
/// projection `InjectProjBelowAgg` puts below the aggregate to compute it
/// (Go's EXPLAIN, captured with a testkit probe over a HYPO TiFlash replica),
/// and the cast projection below an MPP union's child.
#[test]
fn mpp_aggregation_injects_gos_projection_below_the_aggregate() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(432)).expect("session opens");
    for sql in ["CREATE TABLE test.t (a int)", "ALTER TABLE test.t SET HYPO TIFLASH REPLICA 1"] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    rows(&mut session, "use test");
    let cases: [(&str, &[&str]); 5] = [
        (
            "explain select sum(a) from t",
            &[
                "HashAgg_22|1.00|root||funcs:sum(Column#6)->Column#4",
                "└─TableReader_24|1.00|root||MppVersion: 3, data:ExchangeSender_23",
                "  └─ExchangeSender_23|1.00|mpp[tiflash]||ExchangeType: PassThrough",
                "    └─HashAgg_9|1.00|mpp[tiflash]||funcs:sum(Column#9)->Column#6",
                "      └─Projection_38|10000.00|mpp[tiflash]||cast(test.t.a, decimal(10,0) BINARY)->Column#9",
                "        └─TableFullScan_21|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
            ],
        ),
        (
            "explain select a, sum(a) from t group by a",
            &[
                "TableReader_45|8000.00|root||MppVersion: 3, data:ExchangeSender_44",
                "└─ExchangeSender_44|8000.00|mpp[tiflash]||ExchangeType: PassThrough",
                "  └─Projection_5|8000.00|mpp[tiflash]||test.t.a, Column#4",
                "    └─Projection_39|8000.00|mpp[tiflash]||Column#4, test.t.a",
                "      └─HashAgg_37|8000.00|mpp[tiflash]||group by:Column#14, funcs:sum(Column#13)->Column#4, funcs:firstrow(Column#14)->test.t.a",
                "        └─Projection_46|10000.00|mpp[tiflash]||cast(test.t.a, decimal(10,0) BINARY)->Column#13, test.t.a->Column#14",
                "          └─ExchangeReceiver_23|10000.00|mpp[tiflash]||",
                "            └─ExchangeSender_22|10000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary]",
                "              └─TableFullScan_21|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ sum(a) from t",
            &[
                "HashAgg_17|1.00|root||funcs:sum(Column#5)->Column#4",
                "└─TableReader_19|1.00|root||MppVersion: 3, data:ExchangeSender_18",
                "  └─ExchangeSender_18|1.00|mpp[tiflash]||ExchangeType: PassThrough",
                "    └─HashAgg_9|1.00|mpp[tiflash]||funcs:sum(Column#7)->Column#5",
                "      └─Projection_28|10000.00|mpp[tiflash]||cast(test.t.a, decimal(10,0) BINARY)->Column#7",
                "        └─TableFullScan_16|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ a, sum(a) from t group by a",
            &[
                "TableReader_40|8000.00|root||MppVersion: 3, data:ExchangeSender_39",
                "└─ExchangeSender_39|8000.00|mpp[tiflash]||ExchangeType: PassThrough",
                "  └─Projection_5|8000.00|mpp[tiflash]||test.t.a, Column#4",
                "    └─Projection_34|8000.00|mpp[tiflash]||Column#4, test.t.a",
                "      └─HashAgg_32|8000.00|mpp[tiflash]||group by:Column#12, funcs:sum(Column#11)->Column#4, funcs:firstrow(Column#12)->test.t.a",
                "        └─Projection_41|10000.00|mpp[tiflash]||cast(test.t.a, decimal(10,0) BINARY)->Column#11, test.t.a->Column#12",
                "          └─ExchangeReceiver_18|10000.00|mpp[tiflash]||",
                "            └─ExchangeSender_17|10000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary]",
                "              └─TableFullScan_16|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
            ],
        ),
        (
            "explain select /*+ read_from_storage(tiflash[t]) */ a, count(*) from t group by a",
            &[
                "TableReader_40|8000.00|root||MppVersion: 3, data:ExchangeSender_39",
                "└─ExchangeSender_39|8000.00|mpp[tiflash]||ExchangeType: PassThrough",
                "  └─Projection_5|8000.00|mpp[tiflash]||test.t.a, Column#4",
                "    └─Projection_35|8000.00|mpp[tiflash]||Column#4, test.t.a",
                "      └─HashAgg_36|8000.00|mpp[tiflash]||group by:test.t.a, funcs:sum(Column#9)->Column#4, funcs:firstrow(test.t.a)->test.t.a",
                "        └─ExchangeReceiver_38|8000.00|mpp[tiflash]||",
                "          └─ExchangeSender_37|8000.00|mpp[tiflash]||ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t.a, collate: binary]",
                "            └─HashAgg_33|8000.00|mpp[tiflash]||group by:test.t.a, funcs:count(1)->Column#9",
                "              └─TableFullScan_22|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
            ],
        ),
    ];
    // The MPP exploration numbers its plans and columns differently from
    // Go's (`docs/mpp-execplan.md`); the shape and the operator info are
    // compared with those numbers blanked.
    let blank = |line: &str| {
        let mut out = String::with_capacity(line.len());
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            out.push(c);
            if (c == '_' || c == '#') && chars.peek().is_some_and(char::is_ascii_digit) {
                out.push('N');
                while chars.peek().is_some_and(char::is_ascii_digit) {
                    chars.next();
                }
            }
        }
        out
    };
    for (sql, want) in cases {
        let got: Vec<String> =
            displayed(rows(&mut session, sql)).into_iter().map(|row| blank(&row.join("|"))).collect();
        let want: Vec<String> = want.iter().map(|line| blank(line)).collect();
        assert_eq!(got, want, "{sql}");
    }

    // Go `injectProjBelowUnion`: an MPP union's child whose column differs
    // from the union's only in nullability gets a cast projection.
    for sql in ["CREATE TABLE test.s (b bigint not null)", "ALTER TABLE test.s SET HYPO TIFLASH REPLICA 1"] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    rows(&mut session, "set @@tidb_enforce_mpp = 1");
    let sql = "explain select a from t union all select b from s";
    // Go's testkit prints `bigint(20)`; a tidb-server, whose
    // `deprecate-integer-display-length` defaults on, prints `bigint`.
    let got: Vec<String> = displayed(rows(&mut session, sql)).into_iter().map(|row| blank(&row.join("|"))).collect();
    let want: Vec<String> = [
        "TableReader_33|20000.00|root||MppVersion: 3, data:ExchangeSender_32",
        "└─ExchangeSender_32|20000.00|mpp[tiflash]||ExchangeType: PassThrough",
        "  └─Union_9|20000.00|mpp[tiflash]||",
        "    ├─Projection_30|10000.00|mpp[tiflash]||cast(test.t.a, bigint BINARY)->Column#7",
        "    │ └─TableFullScan_17|10000.00|mpp[tiflash]|table:t|keep order:false, stats:pseudo",
        "    └─Projection_34|10000.00|mpp[tiflash]||cast(test.s.b, bigint BINARY)->Column#7",
        "      └─TableFullScan_27|10000.00|mpp[tiflash]|table:s|keep order:false, stats:pseudo",
    ]
    .iter()
    .map(|line| blank(line))
    .collect();
    assert_eq!(got, want, "{sql}");
}

/// Go `PhysicalTableScan.ExplainInfo`'s `PartitionTableScan:true`: a TiFlash
/// scan of a partitioned table read through MPP under dynamic pruning.
#[test]
fn mpp_partition_table_scan_explains_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(433)).expect("session opens");
    for sql in [
        "CREATE TABLE test.p (a int, b int) PARTITION BY HASH(a) PARTITIONS 2",
        "ALTER TABLE test.p SET HYPO TIFLASH REPLICA 1",
    ] {
        session.node_conn().execute_write(sql).expect(sql);
    }
    rows(&mut session, "use test");
    let got: Vec<String> = displayed(rows(
        &mut session,
        "explain select /*+ read_from_storage(tiflash[p]) */ a, b from p where b > 3",
    ))
    .into_iter()
    .map(|row| row.join("|"))
    .collect();
    assert_eq!(
        got,
        [
            "TableReader_13|3333.33|root|partition:all|MppVersion: 3, data:ExchangeSender_12",
            "└─ExchangeSender_12|3333.33|mpp[tiflash]||ExchangeType: PassThrough",
            "  └─Selection_11|3333.33|mpp[tiflash]||gt(test.p.b, 3)",
            "    └─TableFullScan_10|10000.00|mpp[tiflash]|table:p|keep order:false, stats:pseudo, PartitionTableScan:true",
        ]
    );
}
