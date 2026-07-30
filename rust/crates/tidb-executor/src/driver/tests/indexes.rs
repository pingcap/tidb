//! Index maintenance and uniqueness: what a unique index rejects, what a
//! non-unique index accepts, and that an index entry addresses its row.
//!
//! Mirrors Go `pkg/table/tables/index.go` (`GenIndexKey`, the distinct-entry
//! rule that lets a unique index hold any number of NULLs).

use super::*;

/// UNIQUE indexes are enforced on every write path, and MySQL's rule that
/// a unique index permits any number of NULLs is Go's `distinct` flag:
/// an entry with a NULL indexed value is stored the non-distinct way and
/// never collides.
#[test]
fn unique_indexes() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(32) UNIQUE, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (1, 'a@x', 10), (2, 'b@x', 20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A repeated unique value is rejected, naming the index.
    match run_insert_on(
        "INSERT INTO u VALUES (3, 'a@x', 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    ) {
        Err(DriverError::DuplicateEntry { value, key }) => {
            assert_eq!(value, "a@x");
            // Captured from TiDB: the key is qualified table.index, as in
            // "Duplicate entry 'a' for key 'm.code'".
            assert_eq!(key, "u.email");
        }
        other => panic!("expected a duplicate-entry error, got {other:?}"),
    }
    // The rejected insert wrote nothing.
    assert_eq!(
        run_select_on(
            "SELECT id FROM u",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );

    // UPDATE is checked too, and a rejected update leaves the row alone.
    assert!(matches!(
        run_update_on(
            "UPDATE u SET email = 'a@x' WHERE id = 2",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::DuplicateEntry { .. })
    ));
    assert_eq!(
        datum_text_for_test(
            &run_select_on(
                "SELECT email FROM u WHERE id = 2",
                &catalog,
                &crate::StmtContext::for_query()
            )
            .unwrap()[0][0]
        ),
        "b@x"
    );
    // An update that frees a value lets another row take it.
    run_update_on(
        "UPDATE u SET email = 'c@x' WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (4, 'a@x', 40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // DELETE frees the value as well.
    run_delete_on(
        "DELETE FROM u WHERE id = 4",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (5, 'a@x', 50)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // MySQL permits many NULLs in a unique index.
    run_insert_on(
        "INSERT INTO u VALUES (6, NULL, 60)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (7, NULL, 70)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id FROM u",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        5
    );
}

/// A non-unique index accepts repeats: its key carries the handle, so two
/// rows with the same value are two entries (Go's non-distinct path).
#[test]
fn a_non_unique_index_accepts_repeats() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE n (id BIGINT PRIMARY KEY, tag VARCHAR(8), KEY tag_idx (tag))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO n VALUES (1, 'x'), (2, 'x'), (3, 'y')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id FROM n",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        3
    );
}

/// A unique index stores the handle as its value, which is what makes a
/// unique-key lookup a point read (Go's PointGetPlan on a unique key).
#[test]
fn a_unique_index_entry_points_at_its_row() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE k (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO k VALUES (7, 'abc'), (8, 'def')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("k") else {
        panic!("expected a kv table");
    };
    let mut table = table.clone();
    let index_id = table
        .indexes()
        .iter()
        .find(|index| index.name == "code")
        .expect("the unique index exists")
        .id;
    assert_eq!(
        table
            .lookup_unique(index_id, &[Datum::Bytes(b"abc".to_vec())])
            .unwrap(),
        Some(TableHandle::Int(7)),
        "the entry carries the row's handle"
    );
    assert_eq!(
        table
            .lookup_unique(index_id, &[Datum::Bytes(b"nope".to_vec())])
            .unwrap(),
        None
    );
}
