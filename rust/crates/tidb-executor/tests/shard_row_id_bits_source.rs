//! Go `handleTableOptions`'s `TableOptionShardRowID` arm
//! (`pkg/ddl/create_table.go:967-971`): a positive bit count on a table whose
//! primary key is the CLUSTERED row id is refused (8200); any other table
//! accepts the option with the value CLAMPED to `MaxShardRowIDBits` (15).

use tidb_executor::{run_create_table_on, Catalog};

#[test]
fn non_clustered_over_large_bits_are_clamped_not_rejected() {
    // `a int` with no primary key: the handle is the implicit _tidb_rowid, so
    // sharding applies and an over-large count clamps to 15. Fail-before: the
    // port refused this with "shard_row_id_bits should be less than 16".
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int) shard_row_id_bits = 20",
        &mut catalog,
    )
    .expect("non-clustered shard_row_id_bits=20 clamps to 15 in Go, not rejected");
}

#[test]
fn non_clustered_in_range_bits_are_recorded() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int) shard_row_id_bits = 4",
        &mut catalog,
    )
    .expect("in-range shard_row_id_bits on a non-clustered table");
}

#[test]
fn clustered_table_with_bits_is_rejected_with_8200() {
    // `a int primary key` is clustered by default, so any positive bit count
    // is refused (fail-before: the port accepted it).
    let mut catalog = Catalog::default();
    let err = run_create_table_on(
        "create table t (a int primary key) shard_row_id_bits = 4",
        &mut catalog,
    )
    .err()
    .expect("clustered table with shard_row_id_bits must be refused (8200)");
    assert_eq!(
        err.to_string(),
        "Unsupported shard_row_id_bits for table with primary key as row id"
    );
}
