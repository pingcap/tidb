//! WHEN THE JOIN REORDER FIRES, AND WHAT IT LEAVES ALONE.
//!
//! Every test here names the gate it is about and asserts either the PLAN or
//! the leaf order the reorder chose, because that is the reorder's whole
//! observable effect. The oracle for the plan that IS reordered is TiDB's own
//! recording, quoted in [`the_dp_builds_the_tree_tidb_records`].
//!
//! Both solvers are covered. A DEFAULT session takes the GREEDY one
//! (`tidb_opt_join_reorder_threshold` is `0` and every group is bigger), so
//! that arm is the one with corpus-wide reach; the DP is reachable only from a
//! session that raised the threshold. The refusals are the load-bearing tests:
//! a decline leaves the statement's written tree alone, and after the greedy
//! landed "the two thresholds agree" no longer proves one, so every refusal
//! asserts [`fired`] is `None` instead.

use super::*;

/// The schema the recorded topic uses
/// (`t/planner/core/join_reorder_through_projection.test`).
fn tables() -> Catalog {
    let mut catalog = Catalog::default();
    for name in ["t1", "t2", "t3", "t5"] {
        crate::run_create_table_on(
            &format!("CREATE TABLE {name} (a INT, b INT, c VARCHAR(32), PRIMARY KEY (a), KEY(b))"),
            &mut catalog,
        )
        .unwrap();
    }
    catalog
}

/// The plan `EXPLAIN` prints, one operator name per line with its tree prefix.
fn plan(sql: &str, catalog: &Catalog, threshold: i32) -> Vec<String> {
    let ctx = crate::StmtContext::for_query().with_join_reorder_threshold(threshold);
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    rows.iter()
        .map(|row| match &row[0] {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Whether the reorder FIRED, and where each written leaf ended up.
///
/// `None` is a decline, which is the assertion every refusal test below wants:
/// once the greedy runs at the default threshold, "the two thresholds agree"
/// no longer distinguishes a refusal from two solvers reaching the same tree.
fn fired(sql: &str, catalog: &Catalog, threshold: i32) -> Option<Vec<usize>> {
    let ctx = crate::StmtContext::for_query().with_join_reorder_threshold(threshold);
    fired_with_context(sql, catalog, &ctx)
}

fn fired_with_context(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Option<Vec<usize>> {
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    crate::driver::join_reorder::reorder(
        select.from.as_ref().unwrap(),
        select,
        select.where_clause.as_ref(),
        catalog,
        "test",
        ctx,
    )
    .map(|plan| plan.written_order)
}

/// The statement the recorded topic runs at
/// `set tidb_opt_join_reorder_threshold = 10`.
const THREE_WAY: &str = "SELECT t1.a, dt.key_a, dt.doubled_b FROM t1, t5, \
     (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
     WHERE t1.a = dt.key_a AND dt.key_a = t5.a";

/// The written tree is `(t1, t5), dt`, whose bottom join has no equality at
/// all; the DP's answer is `(t1, dt), t5`.
///
/// `r/planner/core/join_reorder_through_projection.result:1249` records
/// exactly that for this statement:
///
/// ```text
/// MergeJoin           inner join, left key:t2.a, right key:t5.a
/// ├─TableReader(Build)      TableFullScan table:t5  keep order:true
/// └─MergeJoin(Probe)  inner join, left key:t1.a, right key:t2.a
///   ├─Projection(Build)     t2.a, mul(t2.b, 2)
///   │ └─MergeJoin     inner join, left key:t2.a, right key:t3.a
///   │   ├─TableReader       TableFullScan table:t3  keep order:true
///   │   └─TableReader       TableFullScan table:t2  keep order:true
///   └─TableReader(Probe)    TableFullScan table:t1  keep order:true
/// ```
///
/// `t5` at the top and `t1` under the second join is the reorder; every leaf
/// keeping order is the merge that only becomes possible once it happens.
/// The trace also exposes each proven root/cop reader boundary. The join
/// equalities are consumed by the join operators, so there is no residual
/// root `Selection` above this tree.
#[test]
fn the_dp_builds_the_tree_tidb_records() {
    let catalog = tables();
    let reordered = plan(THREE_WAY, &catalog, 10);
    assert_eq!(
        reordered,
        vec![
            "MergeJoin_12",
            "├─TableReader_2(Build)",
            "│ └─TableFullScan_1",
            "└─MergeJoin_11(Probe)",
            "  ├─Projection_8(Build)",
            "  │ └─MergeJoin_7",
            "  │   ├─TableReader_4(Build)",
            "  │   │ └─TableFullScan_3",
            "  │   └─TableReader_6(Probe)",
            "  │     └─TableFullScan_5",
            "  └─TableReader_10(Probe)",
            "    └─TableFullScan_9",
        ],
        "the reordered plan is not TiDB's",
    );
}

/// Go disables TopN-assisted string-match estimation when
/// `tidb_default_string_match_selectivity` is non-zero. TPC-H q9 sets it to
/// `0.1`; using the analyzed `LIKE` estimate instead changes the DP's first
/// filtered leaf and selects a different six-way tree.
#[test]
fn dp_uses_the_session_string_match_default_for_tpch_q9_shape() {
    let mut catalog = Catalog::default();
    for ddl in [
        "CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name VARCHAR(55) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
        "CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_nationkey INT)",
        "CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_suppkey INT)",
        "CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT)",
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY)",
        "CREATE TABLE nation (n_nationkey INT PRIMARY KEY)",
    ] {
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for insert in [
        "INSERT INTO part VALUES (1, 'dim one'), (2, 'other')",
        "INSERT INTO supplier VALUES (1, 1)",
        "INSERT INTO lineitem VALUES (1, 1, 1)",
        "INSERT INTO partsupp VALUES (1, 1)",
        "INSERT INTO orders VALUES (1)",
        "INSERT INTO nation VALUES (1)",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    scale_analyzed_tpcc_table(
        &mut catalog,
        "part",
        200_000,
        &[("p_partkey", 200_000), ("p_name", 2)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "supplier",
        10_000,
        &[("s_suppkey", 10_000), ("s_nationkey", 25)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "lineitem",
        6_001_215,
        &[
            ("l_orderkey", 1_487_616),
            ("l_partkey", 200_000),
            ("l_suppkey", 10_000),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "partsupp",
        800_000,
        &[("ps_partkey", 200_000), ("ps_suppkey", 10_000)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        1_500_000,
        &[("o_orderkey", 1_487_616)],
        &ctx,
    );
    scale_analyzed_tpcc_table(&mut catalog, "nation", 25, &[("n_nationkey", 25)], &ctx);
    catalog.clear_dirty_content();

    let sql = "SELECT * FROM part, supplier, lineitem, partsupp, orders, nation \
        WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey \
          AND ps_partkey = l_partkey AND p_partkey = l_partkey \
          AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey \
          AND p_name LIKE '%dim%'";
    let go_session = crate::StmtContext::for_query()
        .with_join_reorder_threshold(60)
        .with_default_string_match_selectivity(0.1);
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &go_session,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let selection = rows
        .iter()
        .find(|row| {
            row.first()
                .is_some_and(|value| datum_text_for_test(value).contains("Selection"))
                && row
                    .get(4)
                    .is_some_and(|value| datum_text_for_test(value).contains("part.p_name"))
        })
        .unwrap_or_else(|| panic!("q9-shaped plan lost the part LIKE Selection: {rows:#?}"));
    assert_eq!(
        datum_text_for_test(&selection[1]),
        "20000.00",
        "a non-zero Go string-match default must disable TopN estimation",
    );
}

/// THE DEFAULT SESSION. `vardef.DefTiDBOptJoinReorderThreshold` is `0`, and
/// `useGreedy := ... || joinGroupNum > threshold` is then true for every group
/// (the smallest is 2), so Go takes the GREEDY solver -- which is why this is
/// the arm that fires on every multi-relation inner join in the corpus.
///
/// The written leaf order is `t1, t5, dt`; the greedy's is `t1, dt, t5`, so
/// `t5` moves from position `1` to position `2` and `dt` from `2` to `1`. That
/// is the tree TiDB records, quoted in [`the_dp_builds_the_tree_tidb_records`].
///
/// This exact order is what a REVERSED cost sort would break: `dt` is the most
/// expensive node, so a descending sort starts the tree there and reaches
/// `(dt, t1), t5` instead.
#[test]
fn the_default_threshold_takes_the_greedy_solver() {
    let catalog = tables();
    assert_eq!(fired(THREE_WAY, &catalog, 0), Some(vec![0, 2, 1]));
    assert_eq!(
        plan(THREE_WAY, &catalog, 0),
        plan(THREE_WAY, &catalog, 10),
        "the greedy and the DP disagree on the recorded statement",
    );
    // The written bottom join `(t1, t5)` has no equality: a cartesian product,
    // which is precisely what the reorder removes. Nothing hashes afterwards.
    assert!(
        !plan(THREE_WAY, &catalog, 0)
            .iter()
            .any(|row| row.contains("HashJoin")),
        "the greedy left the written cartesian join in place",
    );
}

/// THE CARTESIAN REFUSAL. `checkConnectionAndMakeJoin` returns no join at all
/// when the pair has no equality edge and
/// `tidb_opt_cartesian_join_order_threshold` is not positive -- and its default
/// is `0`. So `t1--t5` is joinable and `t2` never is: the greedy peels one
/// connected tree and leaves `t2` behind, which is Go's `makeBushyJoin` case
/// and this module's decline.
///
/// Dropping the refusal makes the greedy swallow `t2` and return a tree, which
/// this `None` catches.
#[test]
fn the_cartesian_refusal_leaves_a_second_component_behind() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A NON-EQUALITY CONJUNCT SPANNING TWO LEAVES is one of Go's `otherConds`,
/// and it does NOT stop the greedy: `hasOtherJoinCondition` only ever makes a
/// pair MORE joinable, and `LogicalJoin.DeriveStats` never reads
/// `OtherConditions`, so the conjunct moves no row count and the equality graph
/// alone decides the order. The group here is connected by `t1.a = t5.a` and
/// `t5.a = t2.a`, so both arms reorder it and reach the same tree.
///
/// This test guards the DECLINE that used to sit here: re-introducing it turns
/// the two `Some`s into `None`s.
#[test]
fn a_spanning_non_equality_conjunct_no_longer_stops_the_greedy() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a = t5.a AND t5.a = t2.a AND t1.b > t2.b";
    let without = "SELECT t1.a FROM t1, t5, t2 WHERE t1.a = t5.a AND t5.a = t2.a";
    assert_eq!(fired(sql, &catalog, 0), fired(without, &catalog, 0));
    assert!(fired(sql, &catalog, 0).is_some(), "the greedy declined");
    assert!(fired(sql, &catalog, 10).is_some(), "the DP arm declined");
}

/// A CONJUNCT WITH NO COLUMN OF THIS GROUP still declines the greedy arm, and
/// for a reason the previous decline blurred: Go's `ExprFromSchema` answers
/// TRUE for a constant or a purely correlated expression against ANY schema,
/// which makes it one of `makeJoin`'s `leftConds` and therefore a child
/// `Selection` with a selectivity this module would have to invent -- not one
/// of the `otherConds` that cost nothing.
#[test]
fn a_conjunct_owning_no_column_of_the_group_declines_the_greedy() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a = t5.a AND t5.a = t2.a AND 1";
    assert_eq!(fired(sql, &catalog, 0), None, "the greedy did not decline");
    assert!(fired(sql, &catalog, 10).is_some(), "the DP arm declined");
}

/// THE SOLVER BOUNDARY, to the exact integer. This group has THREE leaves and
/// only the greedy arm declines it -- on the zero-column conjunct above -- so
/// which solver ran is directly readable from whether the reorder fired.
///
/// Go's `joinGroupNum > threshold` puts the switch BETWEEN `2` and `3`: at a
/// threshold of `2` a three-leaf group is still greedy, and at `3` it is the
/// DP. An off-by-one in either direction moves one of these four.
#[test]
fn the_solver_switches_between_a_threshold_of_two_and_three() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a = t5.a AND t5.a = t2.a AND 1";
    for greedy in [i32::MIN, 0, 1, 2] {
        assert_eq!(
            fired(sql, &catalog, greedy),
            None,
            "threshold {greedy} did not take the greedy arm",
        );
    }
    for dp in [3, 4, i32::MAX] {
        assert!(
            fired(sql, &catalog, dp).is_some(),
            "threshold {dp} did not take the DP arm",
        );
    }
}

/// GO'S `hasOtherJoinCondition`, asserted DIRECTLY, because nothing this
/// module emits can show it.
///
/// The rule makes a pair with no equality edge joinable, and every such join
/// then reaches [`rebuild_node`] with no `ON` to spell -- which declines. So
/// the rule can turn one decline into another decline and never a tree, and a
/// mutation that breaks it survives every plan-level test in this file. Pinning
/// the predicate itself is the only test that can fail.
///
/// The three cases are Go's three `ExprFromSchema` calls: covered by the merged
/// schema, not covered by the left alone, not covered by the right alone.
#[test]
fn a_conjunct_connects_only_when_it_straddles_both_sides() {
    use std::collections::BTreeSet;
    let straddling = |leaves: [usize; 2]| BTreeSet::from(leaves);
    // `{0,2}` straddles `[0] x [2]`.
    assert!(crate::driver::join_reorder::has_other_join_condition(
        &[0],
        &[2],
        &[straddling([0, 2])],
    ));
    // Covered by the LEFT alone: Go's `leftConds`, not a connection.
    assert!(!crate::driver::join_reorder::has_other_join_condition(
        &[0, 2],
        &[1],
        &[straddling([0, 2])],
    ));
    // Covered by the RIGHT alone.
    assert!(!crate::driver::join_reorder::has_other_join_condition(
        &[1],
        &[0, 2],
        &[straddling([0, 2])],
    ));
    // Not covered by the merged schema at all: it reaches a third leaf.
    assert!(!crate::driver::join_reorder::has_other_join_condition(
        &[0],
        &[1],
        &[straddling([0, 2])],
    ));
    // No conjunct at all leaves the pair cartesian.
    assert!(!crate::driver::join_reorder::has_other_join_condition(
        &[0],
        &[2],
        &[],
    ));
}

/// THE COST SORT, where it is observable. `generateJoinOrderNode` sorts the
/// group by `baseNodeCumCost` ASCENDING and
/// `constructConnectedJoinTree` starts at `curJoinGroup[0]`, so the cheapest
/// node is the seed. Here the statement writes the EXPENSIVE relation first --
/// `dt` is a join of two tables, so its cumulative cost exceeds either base
/// table's -- and the seed must still be `t1`.
///
/// Written leaf order is `dt, t1, t5`; the greedy's is `t1, dt, t5`, so `dt`
/// moves from `0` to `1` and `t1` from `1` to `0`. Drop the sort and the seed
/// becomes `dt`, which yields `dt, t1, t5` -- the identity.
#[test]
fn the_seed_is_the_cheapest_node_not_the_first_written() {
    let catalog = tables();
    let sql = "SELECT t1.a, dt.key_a FROM \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt, \
        t1, t5 \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), Some(vec![1, 0, 2]));
}

/// Go's default advanced greedy framework compares the two cheapest leaves as
/// possible starts. The smallest `region` leaf can only join the large
/// `customer` leaf first, while the second-smallest `orders` leaf forms the
/// cheaper `orders-customer` subtree. `chooseBestGreedyStart(2)` therefore
/// chooses the second start even though the old greedy solver chose `region`.
#[test]
fn the_advanced_greedy_compares_the_two_cheapest_starts() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE region (r_key INT PRIMARY KEY)", &mut catalog)
        .unwrap();
    crate::run_create_table_on("CREATE TABLE orders (o_cust INT PRIMARY KEY)", &mut catalog)
        .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE customer (c_key INT PRIMARY KEY, c_region INT)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO region VALUES (1),(2)", &mut catalog, &ctx).unwrap();
    run_insert_on("INSERT INTO orders VALUES (1),(2)", &mut catalog, &ctx).unwrap();
    run_insert_on(
        "INSERT INTO customer VALUES (1,1),(2,1)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    scale_analyzed_tpcc_table(&mut catalog, "region", 1, &[("r_key", 1)], &ctx);
    scale_analyzed_tpcc_table(&mut catalog, "orders", 100, &[("o_cust", 100)], &ctx);
    scale_analyzed_tpcc_table(
        &mut catalog,
        "customer",
        1_000,
        &[("c_key", 1_000), ("c_region", 1)],
        &ctx,
    );
    catalog.clear_dirty_content();

    let sql = "SELECT * FROM region, orders, customer \
        WHERE r_key = c_region AND o_cust = c_key";
    assert_eq!(
        fired(sql, &catalog, 0),
        Some(vec![2, 0, 1]),
        "the default advanced solver must choose orders, customer, region",
    );
    let legacy = crate::StmtContext::for_query()
        .with_join_reorder_threshold(0)
        .with_advanced_join_reorder(false);
    assert_eq!(
        fired_with_context(sql, &catalog, &legacy),
        Some(vec![0, 2, 1]),
        "disabling the advanced framework must retain the legacy single start",
    );
}

/// Go's advanced greedy first exhausts equality-connected joins before it
/// admits an edge that has only `OtherConditions`. This is the TPC-H q7
/// topology: the cross-nation DNF touches `n1` and `n2`, but it must not make
/// those two cheap leaves directly joinable while the equality chain remains.
#[test]
fn the_advanced_greedy_defers_non_equality_edges_until_the_second_round() {
    let mut catalog = Catalog::default();
    for ddl in [
        "CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_nationkey INT)",
        "CREATE TABLE lineitem (l_suppkey INT, l_orderkey INT, l_shipdate DATE)",
        "CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT)",
        "CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_nationkey INT, c_mktsegment VARCHAR(10))",
        "CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))",
    ] {
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
    }
    let ctx = crate::StmtContext::for_query();
    for insert in [
        "INSERT INTO supplier VALUES (1,1),(2,2)",
        "INSERT INTO lineitem VALUES (1,1,'1992-01-01'),(2,2,'1993-01-01'),\
         (1,1,'1994-01-01'),(2,2,'1994-06-01'),(1,1,'1995-01-01'),\
         (2,2,'1995-06-01'),(1,1,'1995-12-01'),(2,2,'1996-06-01'),\
         (1,1,'1996-12-01'),(2,2,'1997-01-01'),(1,1,'1998-01-01'),\
         (2,2,'1998-12-01')",
        "INSERT INTO orders VALUES (1,1),(2,2)",
        "INSERT INTO customer VALUES (1,1,'AUTOMOBILE'),(2,2,'BUILDING')",
        "INSERT INTO nation VALUES (1,'JAPAN'),(2,'INDIA')",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    scale_analyzed_tpcc_table(
        &mut catalog,
        "supplier",
        10_000,
        &[("s_suppkey", 10_000), ("s_nationkey", 25)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "lineitem",
        6_001_215,
        &[
            ("l_suppkey", 10_000),
            ("l_orderkey", 1_487_616),
            ("l_shipdate", 2_526),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "orders",
        1_500_000,
        &[("o_orderkey", 1_487_616), ("o_custkey", 99_248)],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "customer",
        150_000,
        &[
            ("c_custkey", 149_568),
            ("c_nationkey", 25),
            ("c_mktsegment", 5),
        ],
        &ctx,
    );
    scale_analyzed_tpcc_table(
        &mut catalog,
        "nation",
        25,
        &[("n_nationkey", 25), ("n_name", 25)],
        &ctx,
    );
    catalog.clear_dirty_content();

    // q3 precedes q7 in the pinned TPC-H manifest and fully loads
    // customer.c_mktsegment. Its sampled histogram describes 149,998 rows,
    // while the table's realtime count and c_custkey histogram describe
    // 150,000. Go keeps that load in the domain stats cache, including for a
    // later statement planned through another connection.
    let customer_id = match catalog.get_in("test", "customer").unwrap() {
        TableEntry::Kv(table) => table.table_id,
        _ => panic!("customer is not a KV table"),
    };
    let mut customer_stats = catalog
        .table_statistics(customer_id)
        .map(|stats| (**stats).clone())
        .unwrap();
    let c_mktsegment_id = match catalog.get_in("test", "customer").unwrap() {
        TableEntry::Kv(table) => table
            .visible_columns()
            .iter()
            .find(|column| column.name == "c_mktsegment")
            .map(|column| column.id)
            .unwrap(),
        _ => unreachable!(),
    };
    customer_stats
        .columns
        .get_mut(&c_mktsegment_id)
        .unwrap()
        .histogram
        .buckets
        .last_mut()
        .unwrap()
        .count = 149_998;
    catalog.set_table_statistics(customer_id, std::sync::Arc::new(customer_stats));
    let q3_style = "SELECT * FROM customer, orders \
        WHERE c_mktsegment = 'AUTOMOBILE' AND c_custkey = o_custkey";
    assert!(fired(q3_style, &catalog, 0).is_some());
    let (loaded_columns, loaded_indexes) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .loaded_statistics();
    assert_eq!(loaded_columns, [c_mktsegment_id].into_iter().collect());
    assert!(loaded_indexes.is_empty(), "{loaded_indexes:?}");
    let q3_statement = tidb_parser::parse(q3_style).unwrap();
    let Stmt::Query(q3_query) = &q3_statement else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(q3_select) = &**q3_query else {
        panic!("not a SELECT");
    };
    crate::explain::explain_select_stmt(
        q3_select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let c_custkey_id = match catalog.get_in("test", "customer").unwrap() {
        TableEntry::Kv(table) => table
            .visible_columns()
            .iter()
            .find(|column| column.name == "c_custkey")
            .map(|column| column.id)
            .unwrap(),
        _ => unreachable!(),
    };
    let (loaded_columns, _) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .loaded_statistics();
    assert_eq!(
        loaded_columns,
        [c_mktsegment_id].into_iter().collect(),
        "physical access must not change the statement's resident snapshot"
    );
    let (pending_columns, _) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .pending_statistics();
    assert_eq!(pending_columns, [c_custkey_id].into_iter().collect());
    catalog.advance_statistics_loads();
    let (loaded_columns, _) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .loaded_statistics();
    assert_eq!(
        loaded_columns,
        [c_custkey_id, c_mktsegment_id].into_iter().collect(),
        "the next statement must observe the physical handle request"
    );
    let (pending_columns, pending_indexes) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .pending_statistics();
    assert!(pending_columns.is_empty(), "{pending_columns:?}");
    assert!(pending_indexes.is_empty(), "{pending_indexes:?}");
    catalog.advance_statistics_loads();
    let (loaded_columns_after_second_advance, _) = catalog
        .table_statistics(customer_id)
        .unwrap()
        .loaded_statistics();
    assert_eq!(loaded_columns_after_second_advance, loaded_columns);
    let catalog = catalog.clone();

    let sql = "SELECT * FROM supplier, lineitem, orders, customer, nation n1, nation n2 \
        WHERE s_suppkey = l_suppkey \
          AND o_orderkey = l_orderkey \
          AND c_custkey = o_custkey \
          AND s_nationkey = n1.n_nationkey \
          AND c_nationkey = n2.n_nationkey \
          AND ((n1.n_name = 'JAPAN' AND n2.n_name = 'INDIA') \
            OR (n1.n_name = 'INDIA' AND n2.n_name = 'JAPAN')) \
          AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'";
    assert_eq!(
        fired(sql, &catalog, 0),
        Some(vec![1, 2, 3, 4, 0, 5]),
        "the non-equality DNF joined the two nation leaves before the equality graph was exhausted",
    );
    let statement = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select,
        &catalog,
        "test",
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .unwrap();
    let info = rows
        .iter()
        .map(|row| datum_text_for_test(&row[4]))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(info.contains("test.nation.n_name"), "{rows:#?}");
    assert!(!info.contains("test.n1."), "{rows:#?}");
    assert!(!info.contains("test.n2."), "{rows:#?}");

    let grouped_sql = format!(
        "SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue \
         FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, \
                      EXTRACT(YEAR FROM l_shipdate) AS l_year, 1 AS volume {}) shipping \
         GROUP BY supp_nation, cust_nation, l_year \
         ORDER BY supp_nation, cust_nation, l_year",
        sql.strip_prefix("SELECT * ")
            .expect("q7 test SELECT prefix")
    );
    let statement = tidb_parser::parse(&grouped_sql).unwrap();
    let Stmt::Query(query) = &statement else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let grouped_select = (**select).clone();
    let grouped_catalog = catalog.clone();
    let (_, rows) = std::thread::Builder::new()
        .name("q7-grouped-plan".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            crate::explain::explain_select_stmt(
                &grouped_select,
                &grouped_catalog,
                "test",
                &crate::StmtContext::for_query(),
                crate::explain::ExplainFormat::Brief,
            )
        })
        .unwrap()
        .join()
        .unwrap()
        .unwrap();
    let info = rows
        .iter()
        .map(|row| datum_text_for_test(&row[4]))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(info.contains("test.nation.n_name"), "{rows:#?}");
    assert!(!info.contains("test.n1."), "{rows:#?}");
    assert!(!info.contains("test.n2."), "{rows:#?}");
    let final_join = rows
        .iter()
        .find(|row| {
            datum_text_for_test(&row[0]).contains("HashJoin")
                && datum_text_for_test(&row[4]).contains("customer.c_nationkey")
        })
        .expect("q7 final nation join");
    assert_eq!(
        datum_text_for_test(&final_join[1]),
        "38877.73",
        "the first same-version resident column supplies the Go analyzed count: {rows:#?}"
    );
}

/// THE SOLVER-CHOICE TABLE, as a plan-level assertion.
///
/// A group of three takes the DP at a threshold of `3` or more and the greedy
/// below that (`joinGroupNum > threshold`). Both solvers agree here, so the
/// assertion is that every threshold reorders -- the boundary is exercised,
/// not the disagreement.
#[test]
fn every_threshold_reorders_and_the_boundary_is_go_s() {
    let catalog = tables();
    for threshold in [i32::MIN, -1, 0, 1, 2, 3, 4, 10, i32::MAX] {
        assert_eq!(
            fired(THREE_WAY, &catalog, threshold),
            Some(vec![0, 2, 1]),
            "threshold {threshold} produced a different tree",
        );
    }
}

/// A TWO-NODE GROUP. Go's rule fires (`joinGroupNum > 1`) and the greedy runs,
/// but with one edge and two nodes there is only one tree to build, so the
/// cheapest node takes the left and the reorder is observable only when the
/// written order already had the expensive one first. Both leaves here are the
/// same pseudo size, so the STABLE sort keeps the written order and the tree
/// comes back unchanged -- fired, and a no-op.
#[test]
fn a_two_node_group_is_solved_and_leaves_equal_costs_where_they_were() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5 WHERE t1.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), Some(vec![0, 1]));
}

/// A ONE-NODE `FROM` is not a group at all: Go's rule needs
/// `len(curJoinGroup) > 1` before it picks a solver.
#[test]
fn a_single_relation_is_not_a_group() {
    let catalog = tables();
    assert_eq!(fired("SELECT a FROM t1 WHERE a = 1", &catalog, 0), None);
}

/// A group of three under a threshold of `2` is `joinGroupNum > threshold`,
/// the greedy arm, and the reorder happens there too -- Go's default solver is
/// not a "leave it alone" solver.
#[test]
fn a_group_larger_than_the_threshold_is_still_reordered() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, \
        (SELECT t2.a AS key_a FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    let greedy = plan(sql, &catalog, 2);
    assert_eq!(greedy, plan(sql, &catalog, 0));
    assert!(
        !greedy.iter().any(|row| row.contains("HashJoin")),
        "the written cartesian join survived the greedy: {greedy:?}",
    );
}

/// NO EQUALITY EDGE, so nothing connects the group: Go finishes such a group
/// with `makeBushyJoin` over its components, which this module declines. The
/// statement's own cartesian product stays where it was written.
#[test]
fn a_group_with_no_equality_keeps_the_written_tree() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 WHERE t1.b > 1";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A SECOND EQUALITY INTO THE NULL-EXTENDED LEAF. `t5` is null-extended by
/// the left join and is ALSO reachable from `t2` by an inner equality, so a
/// reorder could join `t2` to `t5` before the outer join is formed -- which
/// null-extends a different set of rows. Go carries the two edges as separate
/// `joinTypes` entries and resolves the order through machinery this module
/// does not have, so the whole group is declined here.
#[test]
fn an_edge_over_an_outer_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 LEFT JOIN t5 ON t1.a = t5.a JOIN t2 ON t5.a = t2.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A `STRAIGHT_JOIN` is the user asking for the written order, and is Go's
/// stop condition in the same test.
#[test]
fn a_straight_join_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1 STRAIGHT_JOIN t5 ON t1.a = t5.a \
               JOIN t2 ON t5.a = t2.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// A NON-COLUMN equi key is Go's `injectExpr` case, which materializes the
/// expression in a `Projection` the reorder then joins on. There is no way to
/// spell that in a `FROM` clause, so the group keeps its written tree rather
/// than losing the key.
///
/// The other two equalities make the group CONNECTED without the computed
/// one, so this test fails when the computed key is quietly dropped rather
/// than declined -- which is the failure the connectivity check would
/// otherwise mask.
#[test]
fn a_non_column_equi_key_is_refused() {
    let catalog = tables();
    let sql = "SELECT t1.a FROM t1, t5, t2 \
               WHERE t1.a + 1 = t5.a AND t1.a = t2.a AND t2.a = t5.a";
    assert_eq!(fired(sql, &catalog, 0), None);
    assert_eq!(fired(sql, &catalog, 10), None);
}

/// The reorder changes the ROW LAYOUT, and `*` expands in row order: Go
/// restores the written order with a `Projection` (`restoreSchemaIfChanged`)
/// and this tier restores it through `FromScope::star`. Without the restore
/// the three `t5` columns would come back where `dt`'s two were.
#[test]
fn a_star_still_expands_in_the_written_order() {
    let mut catalog = tables();
    for (table, row) in [("t1", "(1, 10, 'a')"), ("t5", "(1, 50, 'e')")] {
        run_insert_on(
            &format!("INSERT INTO {table} VALUES {row}"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    }
    for table in ["t2", "t3"] {
        run_insert_on(
            &format!("INSERT INTO {table} VALUES (1, 7, 'x')"),
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    }
    let sql = "SELECT * FROM t1, t5, \
        (SELECT t2.a AS key_a, t2.b * 2 AS doubled_b FROM t2 JOIN t3 ON t2.a = t3.a) dt \
        WHERE t1.a = dt.key_a AND dt.key_a = t5.a";
    let ctx = crate::StmtContext::for_query();
    let written = run_select_on(sql, &catalog, &ctx).unwrap();
    let reordered = run_select_on(
        sql,
        &catalog,
        &crate::StmtContext::for_query().with_join_reorder_threshold(10),
    )
    .unwrap();
    // `t1.a, t1.b, t1.c, t5.a, t5.b, t5.c, dt.key_a, dt.doubled_b` -- the
    // WRITTEN order, whose middle three columns the reorder would otherwise
    // push to the end.
    let numbers: Vec<Vec<i64>> = written
        .iter()
        .map(|row| {
            row.iter()
                .filter_map(|value| match value {
                    Datum::Int(int) => Some(*int),
                    _ => None,
                })
                .collect()
        })
        .collect();
    assert_eq!(numbers, vec![vec![1, 10, 1, 50, 1, 14]]);
    assert_eq!(reordered, written, "the reorder moved the output columns");
}

/// A qualified wildcard is still a direct-column projection.  When join
/// reorder changes the child layout, Go's projection eliminator absorbs this
/// identity mapping; retaining it would add a visible Projection below a
/// derived UNION branch (the Web3Bench R35 shape).
#[test]
fn a_qualified_star_does_not_leave_an_identity_projection() {
    let catalog = tables();
    let rows = plan(
        "SELECT t2.* FROM t2 JOIN t3 ON t2.a = t3.a AND t2.b < t3.b",
        &catalog,
        0,
    );
    assert!(
        !rows.iter().any(|row| row.contains("Projection")),
        "qualified wildcard left an identity projection: {rows:?}"
    );
}

// ---------------------------------------------------------------------------
// The outer-join group and the `leading` hint
// ---------------------------------------------------------------------------

/// The schema `t/planner/core/join_reorder2.test` creates.
fn join_reorder2_tables() -> Catalog {
    let mut catalog = Catalog::default();
    for name in ["t1", "t2", "t3", "t4", "t5"] {
        crate::run_create_table_on(
            &format!("CREATE TABLE {name} (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))"),
            &mut catalog,
        )
        .unwrap();
    }
    catalog
}

/// [`fired`] with `@@tidb_opt_join_reorder_through_sel` set, which is the
/// variable that decides whether the `Selection` over an outer join is a
/// barrier.
fn fired_through_sel(sql: &str, catalog: &Catalog, through_sel: bool) -> Option<Vec<usize>> {
    let ctx = crate::StmtContext::for_query().with_join_reorder_through_sel(through_sel);
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    crate::driver::join_reorder::reorder(
        select.from.as_ref().unwrap(),
        select,
        select.where_clause.as_ref(),
        catalog,
        "test",
        &ctx,
    )
    .map(|plan| plan.written_order)
}

/// The two statements of `planner/core/join_reorder2` this module reaches,
/// verbatim from `t/planner/core/join_reorder2.test:17-18`. Their written
/// leaves are `t1`, `t3`, `t4`, `t2` in that order.
const LEADING_T1_T2: &str = "select /*+ leading(t1, t2) */ * from t1 \
     inner join t3 on t1.id=t3.id left join t4 on t4.id=t3.id \
     join t2 on t1.id=t2.id where t3.name like 'test3' or t4.name like 'test4'";
const LEADING_T3_T4: &str = "select /*+ leading(t3, t4, t1, t2) */ * from t1 \
     inner join t3 on t1.id=t3.id left join t4 on t4.id=t3.id \
     join t2 on t1.id=t2.id where t3.name like 'test3' or t4.name like 'test4'";

/// `r/planner/core/join_reorder2.result` records, for `LEADING_T1_T2` at
/// `set @@tidb_opt_join_reorder_through_sel = 1`:
///
/// ```text
/// └─MergeJoin  left outer join, left side:MergeJoin, left key:t3.id, right key:t4.id
///   ├─TableFullScan  table:t4  keep order:true
///   └─MergeJoin(Probe)  inner join, left key:t1.id, right key:t3.id
///     ├─TableFullScan  table:t3  keep order:true
///     └─MergeJoin(Probe)  inner join, left key:t1.id, right key:t2.id
///       ├─TableFullScan  table:t2  keep order:true
///       └─TableFullScan  table:t1  keep order:true
/// ```
///
/// -- `((t1 join t2) join t3) left join t4`, whose FIRST pair is the hint's
/// own. The written leaves `t1, t3, t4, t2` therefore land at positions
/// `0, 2, 3, 1`.
#[test]
fn a_leading_hint_pins_the_first_pair_over_an_outer_join_group() {
    let catalog = join_reorder2_tables();
    assert_eq!(
        fired_through_sel(LEADING_T1_T2, &catalog, true),
        Some(vec![0, 2, 3, 1])
    );
}

/// The same statement's `leading(t3, t4, t1, t2)` sibling, whose pinned first
/// pair IS the outer join: TiDB records `MergeJoin  left outer join, left
/// side:TableReader, left key:t3.id, right key:t4.id` at the BOTTOM, under
/// two inner merges. That is `((t3 left join t4) join t1) join t2`, so the
/// written leaves `t1, t3, t4, t2` land at positions `2, 0, 1, 3`.
#[test]
fn a_leading_hint_can_pin_the_outer_join_itself() {
    let catalog = join_reorder2_tables();
    assert_eq!(
        fired_through_sel(LEADING_T3_T4, &catalog, true),
        Some(vec![2, 0, 1, 3])
    );
}

/// The `tidb_opt_join_reorder_through_sel = 0` copies of both statements keep
/// the WRITTEN tree, which is what `r/planner/core/join_reorder2.result`
/// records for them: `MergeJoin  inner join, left key:t1.id, right
/// key:t3.id` at the bottom, the pair the statement wrote.
///
/// `or(like(t3.name, ...), like(t4.name, ...))` reads `t4`, the null-extended
/// relation, so predicate pushdown leaves a `Selection` standing over the
/// outer join and `extractJoinGroupImpl` stops there unless the variable is
/// ON.
#[test]
fn a_selection_over_a_null_extended_column_declines_the_group() {
    let catalog = join_reorder2_tables();
    assert_eq!(fired_through_sel(LEADING_T1_T2, &catalog, false), None);
    assert_eq!(fired_through_sel(LEADING_T3_T4, &catalog, false), None);
}

/// A `leading` hint naming a relation this group does not hold is Go's
/// `ok == false` arm: the hint is dropped and the group reorders without it.
/// Nothing is pinned, so the greedy's own order stands -- which under pseudo
/// statistics is the written one.
#[test]
fn a_leading_hint_naming_an_absent_table_pins_nothing() {
    let catalog = join_reorder2_tables();
    let sql = "select /*+ leading(t9, t2) */ * from t1 \
        inner join t3 on t1.id=t3.id left join t4 on t4.id=t3.id \
        join t2 on t1.id=t2.id where t3.name like 'test3' or t4.name like 'test4'";
    assert_eq!(
        fired_through_sel(sql, &catalog, true),
        Some(vec![0, 1, 2, 3])
    );
}

/// The reordered outer-join tree returns the SAME rows as the written one,
/// including the row `t4` contributes nothing to and is null-extended on.
#[test]
fn the_reordered_outer_join_returns_the_rows_the_written_tree_does() {
    let mut catalog = join_reorder2_tables();
    for table in ["t1", "t2", "t3"] {
        for id in [1, 2] {
            run_insert_on(
                &format!("INSERT INTO {table} VALUES ({id}, 'test{id}')"),
                &mut catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
        }
    }
    // Only `id = 1` matches in `t4`, so the `id = 2` row is null-extended and
    // survives on the `t3.name like 'test3'` half of the `WHERE`... which it
    // does not match either, leaving the `t4` half to reject it. The point is
    // that both trees agree on WHICH rows survive.
    run_insert_on(
        "INSERT INTO t4 VALUES (1, 'test4')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let sql = "select /*+ leading(t1, t2) */ * from t1 \
        inner join t3 on t1.id=t3.id left join t4 on t4.id=t3.id \
        join t2 on t1.id=t2.id where t3.name like 'test%' or t4.name like 'test4'";
    let written = run_select_on(sql, &catalog, &crate::StmtContext::for_query()).unwrap();
    let reordered = run_select_on(
        sql,
        &catalog,
        &crate::StmtContext::for_query().with_join_reorder_through_sel(true),
    )
    .unwrap();
    assert_eq!(written.len(), 2, "both `t1` rows survive the `WHERE`");
    assert_eq!(
        reordered, written,
        "the reordered outer join dropped or duplicated a row"
    );
}
