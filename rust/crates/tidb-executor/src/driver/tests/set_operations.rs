//! `UNION`/`INTERSECT`/`EXCEPT` and common table expressions.
//!
//! Both are query-composition surfaces: a set operation combines two result
//! sets, and a CTE names one for reuse. Mirrors Go `pkg/executor`'s
//! `UnionExec`/`SetOprExec` and CTE executors.

use crate::StatsStmtCtx as _;
use super::*;

/// A merged CTE must expose the base table to the normal optimizer so its
/// analyzed statistics remain available. More than one consumer still uses
/// the shared materialization path rather than duplicating the CTE body.
#[test]
fn single_use_cte_explain_keeps_base_statistics_and_multiple_uses_materialize() {
    let store = crate::test_store::store();
    use crate::explain::explain_select_stmt;
    use tidb_sessionctx::explain_format::ExplainFormat;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    let ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()));
    crate::run_create_table_on(
        "CREATE TABLE warehouse (w_id BIGINT, w_name VARCHAR(16))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO warehouse VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let (table_id, statistics) = {
        let TableEntry::Kv(table) = catalog
            .table_mut_in(crate::driver::DEFAULT_DATABASE, "warehouse")
            .expect("warehouse exists")
        else {
            panic!("warehouse is not a KV table");
        };
        (
            table.table_id(),
            crate::analyze::kv::analyze_kv_table(
                std::sync::Arc::make_mut(table),
                &crate::analyze::AnalyzeOptions::default(),
                None,
                &ctx,
            )
            .unwrap(),
        )
    };
    ctx.stats_handle().set_table_statistics(table_id, std::sync::Arc::new(statistics));
    let ctx = ctx.with_fresh_staged_writes();

    let Stmt::Query(query) =
        tidb_parser::parse("WITH w AS (SELECT w_id, w_name FROM warehouse) SELECT w_id FROM w")
            .unwrap()
    else {
        panic!("expected query");
    };
    let QueryStmt::Select(select) = &*query else {
        panic!("expected SELECT");
    };
    let (_, plan) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
    let scan = plan
        .iter()
        .find(|row| datum_text_for_test(&row[0]).contains("TableFullScan"))
        .expect("the inlined CTE retains warehouse's base scan");
    assert_eq!(datum_text_for_test(&scan[1]), "3.00");
    assert_eq!(datum_text_for_test(&scan[3]), "table:warehouse");
    assert!(
        !datum_text_for_test(&scan[4]).contains("stats:pseudo"),
        "analyzed base statistics must survive CTE inlining: {plan:?}"
    );

    assert_eq!(
        run_select_on(
            "WITH w AS (SELECT w_id FROM warehouse) \
             SELECT x.w_id FROM w AS x JOIN w AS y ON x.w_id = y.w_id ORDER BY x.w_id",
            &catalog,
            &ctx,
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
        ],
    );
}

