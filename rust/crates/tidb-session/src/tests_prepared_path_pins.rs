//! The prepared plan cache's REUSABLE half: the access-path pin lifecycle
//! across `run_parsed_bound_owned_with_sql` executions. See
//! [`crate::prepared_path_pins`].

use tidb_datatype::Datum;
use tidb_executor::{bind_statement, PinnedLeafAccess};


use crate::Session;

const JOIN_SQL: &str =
    "select * from pin_l join pin_r on pin_l.a = pin_r.x where pin_l.b = ? and pin_r.c = ?";

fn join_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE pin_l (a int primary key, b int, key(b))")
        .unwrap();
    session
        .run("CREATE TABLE pin_r (x int primary key, c int, key(c))")
        .unwrap();
    for i in 1..=5 {
        session
            .run(&format!("INSERT INTO pin_l VALUES ({i}, {i})"))
            .unwrap();
        session
            .run(&format!("INSERT INTO pin_r VALUES ({i}, {i})"))
            .unwrap();
    }
    session
}

fn bound_join(params: [i64; 2]) -> tidb_ast::Stmt {
    let stmt = tidb_parser::parse(JOIN_SQL).expect("join parses");
    bind_statement(
        stmt,
        &[Datum::Int(params[0]), Datum::Int(params[1])],
    )
    .expect("binds")
}

/// A successful first execution CAPTURES one pin per join leaf; the second
/// execution of the same statement REPLAYS them without disturbing the
/// stored entry.
#[test]
fn a_join_capture_then_replay_keeps_one_stable_entry() {
    let mut session = join_session();

    let bound = bound_join([1, 1]);
    session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    assert!(!session.active_prepared_pin_is_open(), "state closes");

    let (first_pins, first_key) = {
        let store = session.prepared_plan_pins.borrow_mut();
        let entry = store.get(JOIN_SQL).expect("a successful miss stores pins");
        assert!(
            !entry.pins.is_empty(),
            "both join leaves record their winners"
        );
        (entry.pins.clone(), entry.key.schema_version)
    };

    // Replay with DIFFERENT literals: same statement text, same key, the
    // stored entry is reused untouched -- this is the mixed-workload shape
    // stability Go's cache provides.
    let bound = bound_join([3, 3]);
    let rows_before = session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    assert!(!session.active_prepared_pin_is_open());
    let store = session.prepared_plan_pins.borrow_mut();
    let entry = store.get(JOIN_SQL).unwrap();
    assert_eq!(entry.pins, first_pins, "replay does not rewrite the entry");
    assert_eq!(entry.key.schema_version, first_key);
    drop(store);

    // And the answer stays right under the replayed plan.
    if let crate::StmtOutput::Rows { rows, .. } = rows_before {
        assert_eq!(rows.len(), 1, "l.b=3 joins exactly r.c=3");
    } else {
        panic!("expected rows");
    }
}

/// DDL between two executions MOVES the key: the next execution replans
/// freely and re-captures, which is what keeps a pinned index from surviving
/// its own drop.
#[test]
fn ddl_moves_the_key_and_recaptures() {
    let mut session = join_session();
    let bound = bound_join([2, 2]);
    session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    let old_version = session
        .prepared_plan_pins
        .borrow_mut()
        .get(JOIN_SQL)
        .unwrap()
        .key
        .schema_version;

    session.run("CREATE TABLE pin_other (z int)").unwrap();

    let bound = bound_join([4, 4]);
    session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    let store = session.prepared_plan_pins.borrow_mut();
    let entry = store.get(JOIN_SQL).unwrap();
    assert_ne!(
        entry.key.schema_version, old_version,
        "the re-capture planned against the new schema"
    );
}

/// The captured pins name real shapes: every leaf records either the table
/// path or an index id, and re-capturing the same statement twice yields the
/// SAME shapes (planning here is deterministic).
#[test]
fn recaptured_shapes_match_the_originals() {
    let mut session = join_session();
    let bound = bound_join([1, 1]);
    session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    let first = session.prepared_plan_pins.borrow_mut().get(JOIN_SQL).unwrap().pins.clone();

    // Force a fresh capture of the same statement by moving the schema key.
    session.run("CREATE TABLE pin_other (z int)").unwrap();
    let bound = bound_join([1, 1]);
    session.run_parsed_bound_owned_with_sql(bound, JOIN_SQL).unwrap();
    let second = session.prepared_plan_pins.borrow_mut().get(JOIN_SQL).unwrap().pins.clone();

    assert_eq!(first.len(), second.len());
    for (leaf, shape) in &first {
        match (&shape, second.get(leaf)) {
            (PinnedLeafAccess::IndexId(a), Some(PinnedLeafAccess::IndexId(b))) => {
                assert_eq!(a, b, "{leaf} re-commits the same index");
            }
            (PinnedLeafAccess::TableScan, Some(PinnedLeafAccess::TableScan)) => {}
            other => panic!("shape drifted for {leaf}: {other:?}"),
        }
    }
}
