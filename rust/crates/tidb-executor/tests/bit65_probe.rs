use tidb_executor::{ddl, run_create_table_in, run_insert_on, run_select_on, Catalog, CreateTableSettings, StmtContext};

#[test]
fn probe_add_col_check_flow() {
    let mut catalog = Catalog::default();
    let mut settings = CreateTableSettings::default();
    settings.enable_check_constraint = true;
    ddl::run_create_table_in(
        "create table t (a int primary key)",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    let ctx = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1), (2)", &mut catalog, &ctx).unwrap();
    match ddl::run_alter_table_in(
        "alter table t add column b int default -5 check (b > 0)",
        &mut catalog,
        "test",
        &ctx,
    ) {
        Ok(_) => println!("add col => OK"),
        Err(e) => println!("add col => ERR {e}"),
    }
    // With the CHECK attached, a violating insert must be refused.
    match run_insert_on("insert into t values (5, -5)", &mut catalog, &ctx) {
        Ok(_) => println!("post-violating insert => OK"),
        Err(e) => println!("post-violating insert => ERR {e}"),
    }
    match run_select_on("select a, b from t order by a", &catalog, &ctx) {
        Ok(rows) => {
            let text: Vec<String> = rows
                .into_iter()
                .map(|r| r.iter().map(|d| format!("{d:?}")).collect::<Vec<_>>().join("|"))
                .collect();
            println!("rows: {text:?}");
        }
        other => println!("select => {other:?}"),
    }
}
