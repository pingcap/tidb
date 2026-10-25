#![cfg(test)]

use std::sync::{Arc, Mutex};

use crate::{MdlRelatedTableSink, Session};

#[derive(Default)]
struct RecordingMdlSink {
    tables: Mutex<Vec<(i64, i64)>>,
    unresolved: Mutex<usize>,
}

impl MdlRelatedTableSink for RecordingMdlSink {
    fn record_table(&self, table_id: i64, version: i64) {
        self.tables.lock().unwrap().push((table_id, version));
    }

    fn record_unresolved(&self) {
        *self.unresolved.lock().unwrap() += 1;
    }
}

#[test]
fn view_query_records_base_table_ids_for_mdl() {
    let sink = Arc::new(RecordingMdlSink::default());
    let mut session = Session::new();
    session.set_mdl_related_table_sink(sink.clone());
    session
        .run("CREATE TABLE base_for_mdl (id INT PRIMARY KEY)")
        .unwrap();
    session
        .run("CREATE VIEW view_for_mdl AS SELECT id FROM base_for_mdl")
        .unwrap();
    sink.tables.lock().unwrap().clear();
    *sink.unresolved.lock().unwrap() = 0;
    session.run("BEGIN").unwrap();
    session.run("SELECT * FROM view_for_mdl").unwrap();

    let tables = sink.tables.lock().unwrap();
    // Go's preprocessor locks the view's own id (`handleTableName` ->
    // `tryLockMDLAndUpdateSchemaIfNecessary`), then `buildDataSource` locks
    // the base table; the in-process tier records the base table alone.
    let expected = if crate::tests_support::in_process_tier() {
        1
    } else {
        2
    };
    assert_eq!(
        tables.len(),
        expected,
        "a view must pin its concrete base table"
    );
    assert!(tables.iter().all(|(id, _)| *id > 0));
    assert_eq!(*sink.unresolved.lock().unwrap(), 0);
}

#[test]
fn autocommit_locking_read_records_mdl_but_plain_read_does_not() {
    let sink = Arc::new(RecordingMdlSink::default());
    let mut session = Session::new();
    session.set_mdl_related_table_sink(sink.clone());
    session
        .run("CREATE TABLE locking_for_mdl (id INT PRIMARY KEY)")
        .unwrap();

    session.run("SELECT * FROM locking_for_mdl").unwrap();
    assert!(sink.tables.lock().unwrap().is_empty());

    session
        .run("SELECT * FROM locking_for_mdl FOR UPDATE")
        .unwrap();
    assert_eq!(sink.tables.lock().unwrap().len(), 1);
}

/// Go locks a view's tables as `buildDataSource` resolves them, so a name in
/// the body that is a CTE locks nothing -- not the stored table that happens
/// to share its name -- while the table the CTE reads is locked.
#[test]
fn view_body_tables_are_locked_as_the_planner_resolves_them() {
    let sink = Arc::new(RecordingMdlSink::default());
    let mut session = Session::new();
    session.set_mdl_related_table_sink(sink.clone());
    session.run("CREATE TABLE c (id INT PRIMARY KEY)").unwrap();
    session
        .run("CREATE TABLE base_for_cte (id INT PRIMARY KEY)")
        .unwrap();
    session
        .run("CREATE VIEW cte_view AS WITH c AS (SELECT id FROM base_for_cte) SELECT * FROM c")
        .unwrap();
    let rows = crate::tests_support::row_text(session.run(
        "SELECT tidb_table_id FROM information_schema.tables \
         WHERE table_schema = 'test' AND table_name = 'base_for_cte'",
    ));
    let base: i64 = rows[0][0].parse().unwrap();
    sink.tables.lock().unwrap().clear();
    session.run("BEGIN").unwrap();
    session.run("SELECT * FROM cte_view").unwrap();
    let mut locked: Vec<i64> = sink.tables.lock().unwrap().iter().map(|(id, _)| *id).collect();
    locked.dedup();
    if crate::tests_support::in_process_tier() {
        assert_eq!(locked, [base]);
    } else {
        // The view itself first (preprocess), then what its body resolves.
        let rows = crate::tests_support::row_text(session.run(
            "SELECT tidb_table_id FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'cte_view'",
        ));
        let view: i64 = rows[0][0].parse().unwrap();
        assert_eq!(locked, [view, base]);
    }
    assert_eq!(*sink.unresolved.lock().unwrap(), 0);
}
