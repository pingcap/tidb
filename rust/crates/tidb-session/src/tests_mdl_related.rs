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
    assert_eq!(tables.len(), 1, "a view must pin its concrete base table");
    assert!(tables[0].0 > 0);
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
