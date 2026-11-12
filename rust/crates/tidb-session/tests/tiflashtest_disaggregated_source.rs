//! Go `pkg/executor/test/tiflashtest/tiflash_test.go`'s disaggregated TiFlash
//! plan tests.

use crate::support::testkit::{update_global_config, TestKit};

/// Go `external.GetTableByName(t, tk, db, name).Meta().ID`.
fn table_id(tk: &TestKit, db: &str, name: &str) -> i64 {
    let tidb_infoschema::TableEntry::Kv(table) = tk.get_table_by_name(db, name) else {
        panic!("{db}.{name} is not a table");
    };
    table.meta().read().id
}

/// Go `TestDisaggregatedTiFlashQuery`.
#[test]
fn test_disaggregated_tiflash_query() {
    let _config = update_global_config(|conf| conf.disaggregated_tiflash = true);

    let mut tk = TestKit::new_with_mock_tiflash(2);
    tk.must_exec("use test");
    tk.must_exec("drop table if exists tbl_1");
    tk.must_exec(
        "create table tbl_1 ( col_1 bigint not null default -1443635317331776148,
		col_2 text ( 176 ) collate utf8mb4_bin not null,
		col_3 decimal ( 8, 3 ),
		col_4 varchar ( 128 ) collate utf8mb4_bin not null,
		col_5 varchar ( 377 ) collate utf8mb4_bin,
		col_6 double,
		col_7 varchar ( 459 ) collate utf8mb4_bin,
		col_8 tinyint default -88 ) charset utf8mb4 collate utf8mb4_bin ;",
    );
    tk.must_exec("alter table tbl_1 set tiflash replica 1");
    let id = table_id(&tk, "test", "tbl_1");
    tk.update_table_replica_info(id, true);
    tk.must_exec("set @@session.tidb_isolation_read_engines=\"tiflash\"");

    tk.must_exec("explain select max( tbl_1.col_1 ) as r0 , sum( tbl_1.col_1 ) as r1 , sum( tbl_1.col_8 ) as r2 from tbl_1 where tbl_1.col_8 != 68 or tbl_1.col_3 between null and 939 order by r0,r1,r2;");

    tk.must_exec("set @@tidb_partition_prune_mode = 'static';");
    tk.must_exec("set @@session.tidb_isolation_read_engines=\"tiflash\"");
    tk.must_exec("create table t1(c1 int, c2 int) partition by hash(c1) partitions 3");
    tk.must_exec("insert into t1 values(1, 1), (2, 2), (3, 3)");
    tk.must_exec("alter table t1 set tiflash replica 1");
    let id = table_id(&tk, "test", "t1");
    tk.update_table_replica_info(id, true);
    tk.must_query("explain select * from t1 where c1 < 2").check([
        "PartitionUnion_11 9970.00 root  ",
        "├─TableReader_16 3323.33 root  MppVersion: 3, data:ExchangeSender_15",
        "│ └─ExchangeSender_15 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
        "│   └─Selection_14 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
        "│     └─TableFullScan_13 10000.00 mpp[tiflash] table:t1, partition:p0 keep order:false, stats:pseudo",
        "├─TableReader_20 3323.33 root  MppVersion: 3, data:ExchangeSender_19",
        "│ └─ExchangeSender_19 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
        "│   └─Selection_18 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
        "│     └─TableFullScan_17 10000.00 mpp[tiflash] table:t1, partition:p1 keep order:false, stats:pseudo",
        "└─TableReader_24 3323.33 root  MppVersion: 3, data:ExchangeSender_23",
        "  └─ExchangeSender_23 3323.33 mpp[tiflash]  ExchangeType: PassThrough",
        "    └─Selection_22 3323.33 mpp[tiflash]  lt(test.t1.c1, 2)",
        "      └─TableFullScan_21 10000.00 mpp[tiflash] table:t1, partition:p2 keep order:false, stats:pseudo",
    ]);
}

/// Go `TestDisaggregatedTiFlashGeneratedColumn`.
#[test]
fn test_disaggregated_tiflash_generated_column() {
    let mut tk = TestKit::new_with_mock_tiflash(2);
    tk.must_exec("use test");

    tk.must_exec("drop table if exists t1;");
    tk.must_exec("create table t1(c1 varchar(100), c2 varchar(100) AS (lower(c1)));");
    tk.must_exec("insert into t1(c1) values('ABC');");
    tk.must_exec("alter table t1 set tiflash replica 1;");
    let id = table_id(&tk, "test", "t1");
    tk.update_table_replica_info(id, true);

    tk.must_exec("drop table if exists t2;");
    tk.must_exec("create table t2(c1 int, c2 varchar(100));");
    tk.must_exec("insert into t2 values(1, 'xhy'), (2, 'abc');");
    tk.must_exec("alter table t2 set tiflash replica 1;");
    let id = table_id(&tk, "test", "t2");
    tk.update_table_replica_info(id, true);
    tk.must_exec("alter table t2 add index idx2((lower(c2)));");
    // unistore does not support later materialization
    tk.must_exec("set @@session.tidb_opt_enable_late_materialization=0");

    let test1 = |tk: &mut TestKit, force_tiflash: bool| {
        if force_tiflash {
            tk.must_exec("set tidb_isolation_read_engines = 'tiflash'");
        } else {
            tk.must_exec("set tidb_isolation_read_engines = 'tikv,tiflash'");
        }
        let sqls = [
            "explain select * from t2 where lower(c2) = 'abc';",
            "explain select count(*) from t2 where lower(c2) = 'abc';",
            "explain select count(c1) from t2 where lower(c2) = 'abc';",
        ];
        for sql in sqls {
            let mut gen_tiflash_plan = false;
            let mut selection_pushdown_tiflash = false;
            let mut agg_pushdown_tiflash = false;

            for row in tk.must_query(sql).rows() {
                let line = format!("[{}]", row.join(" "));
                if line.contains("tiflash") {
                    gen_tiflash_plan = true;
                }
                if line.contains("Selection") && line.contains("tiflash") {
                    selection_pushdown_tiflash = true;
                }
                if line.contains("Agg") && line.contains("tiflash") {
                    agg_pushdown_tiflash = true;
                }
            }
            if force_tiflash {
                // Can generate tiflash plan, also Agg/Selection can push down to tiflash.
                assert!(gen_tiflash_plan, "{sql}");
                assert!(selection_pushdown_tiflash, "{sql}");
                if sql.contains("count") {
                    assert!(agg_pushdown_tiflash, "{sql}");
                }
            } else {
                // Can generate tiflash plan, but Agg/Selection cannot push down to tiflash.
                assert!(!selection_pushdown_tiflash, "{sql}");
                if sql.contains("count") {
                    assert!(!agg_pushdown_tiflash, "{sql}");
                }
            }
        }
    };

    let test2 = |tk: &mut TestKit| {
        // Can generate tiflash plan when select generated column.
        // But Agg cannot push down to tiflash.
        let sqls = [
            "explain select * from t1;",
            "explain select c2 from t1;",
            "explain select count(c2) from t1;",
        ];
        for sql in sqls {
            let mut gen_tiflash_plan = false;
            let mut agg_pushdown_tiflash = false;
            for row in tk.must_query(sql).rows() {
                let line = format!("[{}]", row.join(" "));
                if line.contains("tiflash") {
                    gen_tiflash_plan = true;
                }
                if line.contains("tiflash") && line.contains("Agg") {
                    agg_pushdown_tiflash = true;
                }
            }
            assert!(gen_tiflash_plan, "{sql}");
            if sql.contains("count") {
                assert!(!agg_pushdown_tiflash, "{sql}");
            }
        }
    };

    {
        let _config = update_global_config(|conf| conf.disaggregated_tiflash = false);
        test1(&mut tk, true);
        test1(&mut tk, false);
        test2(&mut tk);
    }

    let _config = update_global_config(|conf| conf.disaggregated_tiflash = true);
    test1(&mut tk, true);
    test1(&mut tk, false);
    test2(&mut tk);
}
