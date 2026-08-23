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

//! `_tidb_rowid`, Go's extra handle column.

use crate::tests_support::row_text;
use crate::Session;

fn fixture() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int, b int, KEY ia(a))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (10, 1), (20, 2), (30, 3)")
        .unwrap();
    session
}

/// Go appends the extra handle column to a heap table's `DataSource` schema
/// (`buildDataSource`'s `NewExtraHandleSchemaCol`), where it reports the
/// record HANDLE rather than any stored column, and `unfoldWildStar` skips it
/// so `*` never carries it.
#[test]
fn the_extra_handle_column_reports_the_record_handle() {
    let mut session = fixture();

    assert_eq!(
        row_text(session.run("SELECT _tidb_rowid FROM t")),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, _tidb_rowid FROM t")),
        vec![vec!["10", "1"], vec!["20", "2"], vec!["30", "3"]]
    );
    // Qualified, and through an alias.
    assert_eq!(
        row_text(session.run("SELECT t._tidb_rowid FROM t")),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );

    // `*` expands to the stored columns alone, even beside the name.
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        vec![vec!["10", "1"], vec!["20", "2"], vec!["30", "3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT *, _tidb_rowid FROM t")),
        vec![
            vec!["10", "1", "1"],
            vec!["20", "2", "2"],
            vec!["30", "3", "3"]
        ]
    );

    // It is an ordinary column everywhere else: an ORDER BY key, a predicate.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY _tidb_rowid DESC")),
        vec![vec!["30"], vec!["20"], vec!["10"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE _tidb_rowid = 2")),
        vec![vec!["20"]]
    );

    // And through an index reader, whose entries carry the same handle.
    assert_eq!(
        row_text(session.run(
            "SELECT a, _tidb_rowid FROM t USE INDEX (ia) WHERE a > 10 ORDER BY a, _tidb_rowid"
        )),
        vec![vec!["20", "2"], vec!["30", "3"]]
    );
}

/// A table whose handle IS a declared column has no extra one: Go's
/// `buildDataSource` appends it only when `handleCols == nil` and the table is
/// not `IsCommonHandle`, so TiDB answers "Unknown column" for the name there.
#[test]
fn a_clustered_table_has_no_extra_handle_column() {
    let mut session = Session::new();
    for ddl in [
        "CREATE TABLE pk (a int PRIMARY KEY, b int)",
        "CREATE TABLE ch (a varchar(8), b int, PRIMARY KEY (a) CLUSTERED)",
    ] {
        session.run(ddl).unwrap();
    }
    for table in ["pk", "ch"] {
        let error = session
            .run(&format!("SELECT _tidb_rowid FROM {table}"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1054, "{table}");
    }
}

/// Go gives a WRITE's `DataSource` the same schema a read gets, so
/// `_tidb_rowid` resolves in an `UPDATE`'s `WHERE` and `SET` and in a
/// `DELETE`'s `WHERE` exactly as it does in a `SELECT`.
///
/// The row the write STAGES is still the stored one -- Go composes its new
/// row from the DataSource's stored columns, and the extra handle is not one
/// of them -- which is why the column can be read here without widening what
/// gets written. Source rows: `tests/integrationtest/t/executor/rowid.test`.
#[test]
fn a_write_reads_the_extra_handle_without_writing_it() {
    let mut session = fixture();

    assert_eq!(
        session
            .run("UPDATE t SET a = 99 WHERE _tidb_rowid = 2")
            .unwrap(),
        crate::StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, _tidb_rowid FROM t")),
        vec![
            vec!["10", "1", "1"],
            vec!["99", "2", "2"],
            vec!["30", "3", "3"]
        ]
    );

    // Readable in the SET list too, and the staged row stays two columns
    // wide -- a third would not decode.
    session
        .run("UPDATE t SET b = _tidb_rowid * 10 WHERE a = 10")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b FROM t WHERE _tidb_rowid = 1")),
        vec![vec!["10", "10"]]
    );

    assert_eq!(
        session.run("DELETE FROM t WHERE _tidb_rowid = 2").unwrap(),
        crate::StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a, _tidb_rowid FROM t")),
        vec![vec!["10", "1"], vec!["30", "3"]]
    );

    // Writing it is a different capability, gated in Go behind
    // `tidb_opt_write_row_id`; refused here rather than silently accepted.
    assert!(session
        .run("UPDATE t SET _tidb_rowid = 7 WHERE a = 10")
        .is_err());
}

/// A condition on `_tidb_rowid` bounds the SCAN, because the extra handle IS
/// the row handle.
///
/// Go `buildDataSource` appends `NewExtraHandleSchemaCol()` for a table with
/// neither an integer primary key nor a common handle, and builds
/// `ds.handleCols` FROM it -- so the ranger treats it exactly as it treats an
/// integer primary key, and `deriveTablePathStats` gives the table path real
/// ranges. Without that this tier read every row and filtered above.
///
/// Queries and shapes are the source corpus's own:
/// `tests/integrationtest/t/explain_easy.test`.
#[test]
fn a_rowid_comparison_bounds_the_table_scan() {
    let mut session = fixture();

    let scan = |session: &mut Session, sql: &str| {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| row.join(" | "))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let plan = scan(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE _tidb_rowid > 1",
    );
    assert!(
        plan.contains("TableRangeScan") && plan.contains("range:(1,+inf]"),
        "the rowid bounds the scan:\n{plan}"
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE _tidb_rowid > 1")),
        vec![vec!["20"], vec!["30"]]
    );

    // A second, unrelated conjunct stays above the range, as Go's does.
    let plan = scan(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE _tidb_rowid > 1 AND a > 0",
    );
    assert!(
        plan.contains("TableRangeScan") && plan.contains("gt(test.t.a, 0)"),
        "the other conjunct is still a filter:\n{plan}"
    );

    // And a predicate that names no rowid still reads the whole table.
    let plan = scan(&mut session, "EXPLAIN SELECT * FROM t WHERE a > 0");
    assert!(
        plan.contains("TableFullScan"),
        "an unrelated predicate builds no handle range:\n{plan}"
    );
}

/// An EQUALITY on `_tidb_rowid` is a point get, and consumes the predicate.
///
/// Go `findPKHandle`'s `!tblInfo.PKIsHandle` branch takes the `_tidb_rowid`
/// pair as the handle pair, typed `TypeLonglong`. Its recorded plan for
/// `select * from t where _tidb_rowid = 0` is a bare `Point_Get table:t
/// handle:0` -- no `Selection` above it, because the handle pins the row
/// completely.
#[test]
fn a_rowid_equality_is_a_point_get_with_no_filter_above_it() {
    let mut session = fixture();

    let plan = row_text(session.run("EXPLAIN SELECT * FROM t WHERE _tidb_rowid = 2"))
        .into_iter()
        .map(|row| row.join(" | "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("Point_Get") && plan.contains("handle:2"),
        "a rowid equality is a point get:\n{plan}"
    );
    assert!(
        !plan.contains("Selection"),
        "and the handle consumed the predicate:\n{plan}"
    );

    assert_eq!(
        row_text(session.run("SELECT a, b FROM t WHERE _tidb_rowid = 2")),
        vec![vec!["20", "2"]]
    );
    // The point get still reports the column when the statement asks for it.
    assert_eq!(
        row_text(session.run("SELECT _tidb_rowid, a FROM t WHERE _tidb_rowid = 2")),
        vec![vec!["2", "20"]]
    );
    // A handle no row has answers nothing rather than failing.
    assert!(row_text(session.run("SELECT a FROM t WHERE _tidb_rowid = 99")).is_empty());
}

/// Writing `_tidb_rowid` needs `tidb_opt_write_row_id`, and the value becomes
/// the record HANDLE.
///
/// Go `initInsertColumns` refuses the named column outright without the
/// variable, with a plain error that reaches the client as 1105. With it, a
/// non-zero value is the handle and `rebaseImplicitRowID` lifts the counter
/// above it so a later automatic handle cannot collide.
///
/// The sequence is the source corpus's own:
/// `tests/integrationtest/t/executor/rowid.test`.
#[test]
fn an_insert_may_write_the_extra_handle_under_its_variable() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int, b int)").unwrap();
    session
        .run("INSERT INTO t VALUES (1, 7), (1, 8), (1, 9)")
        .unwrap();

    // Refused by default, in Go's own words.
    let error = session
        .run("INSERT INTO t (a, b, _tidb_rowid) VALUES (2, 2, 2)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1105);
    assert_eq!(
        error.message,
        "insert, update and replace statements for _tidb_rowid are not supported"
    );

    session
        .run("SET SESSION tidb_opt_write_row_id = ON")
        .unwrap();
    session.run("DELETE FROM t WHERE _tidb_rowid = 2").unwrap();
    session
        .run("INSERT INTO t (a, b, _tidb_rowid) VALUES (2, 2, 2), (5, 5, 5)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b, _tidb_rowid FROM t")),
        vec![
            vec!["1", "7", "1"],
            vec!["2", "2", "2"],
            vec!["1", "9", "3"],
            vec!["5", "5", "5"],
        ]
    );
    // `rebaseImplicitRowID`: the next automatic handle clears the written 5.
    session.run("INSERT INTO t VALUES (9, 9)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT _tidb_rowid FROM t WHERE a = 9")),
        vec![vec!["6"]]
    );
    // Writing a handle that exists is the ordinary duplicate.
    let error = session
        .run("INSERT INTO t (a, _tidb_rowid) VALUES (1, 1)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1062);
}

/// A written ZERO row id is "allocate one", unless `NO_AUTO_VALUE_ON_ZERO`
/// says otherwise.
///
/// Go `adjustImplicitRowID`: a non-zero value is the handle; a NULL or zero
/// falls to the allocation branch, whose condition is `d.IsNull() ||
/// SQLMode&ModeNoAutoValueOnZero == 0` -- so with the mode set, a written
/// zero falls PAST it and is stored as handle 0.
#[test]
fn a_written_zero_row_id_is_stored_only_under_no_auto_value_on_zero() {
    let mut session = Session::new();
    session
        .run("SET SESSION tidb_opt_write_row_id = ON")
        .unwrap();
    session.run("CREATE TABLE t (a int)").unwrap();

    session
        .run("SET sql_mode = CONCAT(@@sql_mode, ',NO_AUTO_VALUE_ON_ZERO')")
        .unwrap();
    session
        .run("INSERT INTO t (a, _tidb_rowid) VALUES (5, 0)")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, _tidb_rowid FROM t WHERE a = 5")),
        vec![vec!["5", "0"]],
        "the mode stores the zero as the handle"
    );

    session
        .run("SET sql_mode = REPLACE(@@sql_mode, 'NO_AUTO_VALUE_ON_ZERO', '')")
        .unwrap();
    session
        .run("INSERT INTO t (a, _tidb_rowid) VALUES (6, 0)")
        .unwrap();
    assert_ne!(
        row_text(session.run("SELECT _tidb_rowid FROM t WHERE a = 6")),
        vec![vec!["0"]],
        "without it the zero means allocate"
    );

    // A NULL means allocate under either mode.
    session
        .run("INSERT INTO t (a, _tidb_rowid) VALUES (7, NULL)")
        .unwrap();
    assert_ne!(
        row_text(session.run("SELECT _tidb_rowid FROM t WHERE a = 7")),
        vec![vec!["0"]]
    );
}

/// `SHARD_ROW_ID_BITS` puts a shard in the HIGH bits of an allocated
/// `_tidb_rowid`, and the run of ids one shard covers belongs to the
/// TRANSACTION.
///
/// Go `AllocHandleIDs`: `if meta.ShardRowIDBits > 0 { base =
/// shardFmt.Compose(shard, base) }`, where `NewShardIDFormat` leaves
/// `64 - shardBits - 1` bits for the counter -- so with 15 shard bits a row's
/// shard is `_tidb_rowid >> 48`.
///
/// The shard comes from `GetRowIDShardGenerator().GetCurrentShard(n)`, whose
/// generator Go builds per TRANSACTION from `TxnCtx.StartTS` and drops when
/// the transaction ends. That is what makes `tidb_shard_allocate_step` count
/// ROWS within a transaction rather than statements: the same ten rows are
/// two shards inside one `BEGIN`/`COMMIT`, while three separate `INSERT`s are
/// three shards however large the step is.
///
/// Statements and expected counts are the source corpus's own:
/// `tests/integrationtest/t/table/tables.test`.
#[test]
fn a_sharded_rowid_carries_its_shard_and_the_run_belongs_to_the_transaction() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE shard_t (a int) SHARD_ROW_ID_BITS = 15")
        .unwrap();
    let shards = |session: &mut Session| {
        row_text(session.run("SELECT count(distinct(_tidb_rowid>>48)) FROM shard_t"))[0][0].clone()
    };

    // One statement, one transaction: the step counts rows, so eleven rows at
    // a step of three span four shards.
    session.run("SET @@tidb_shard_allocate_step=3").unwrap();
    session
        .run("INSERT INTO shard_t VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11)")
        .unwrap();
    assert_eq!(shards(&mut session), "4");

    // Four statements inside ONE transaction: still one run, so ten rows at a
    // step of five span two shards -- not four, which is what a
    // per-statement run would give.
    session.run("TRUNCATE TABLE shard_t").unwrap();
    session.run("SET @@tidb_shard_allocate_step=5").unwrap();
    session.run("BEGIN").unwrap();
    session
        .run("INSERT INTO shard_t VALUES (1),(2),(3)")
        .unwrap();
    session
        .run("INSERT INTO shard_t VALUES (4),(5),(6)")
        .unwrap();
    session
        .run("INSERT INTO shard_t VALUES (7),(8),(9)")
        .unwrap();
    session.run("INSERT INTO shard_t VALUES (10)").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(shards(&mut session), "2");

    // Three autocommit statements are three transactions, so three shards
    // even at the default step, which is far larger than three rows.
    session.run("TRUNCATE TABLE shard_t").unwrap();
    session
        .run("SET @@tidb_shard_allocate_step=default")
        .unwrap();
    session.run("INSERT INTO shard_t VALUES (10)").unwrap();
    session.run("INSERT INTO shard_t VALUES (11)").unwrap();
    session.run("INSERT INTO shard_t VALUES (12)").unwrap();
    assert_eq!(shards(&mut session), "3");

    // And the option is part of the definition.
    let text = row_text(session.run("SHOW CREATE TABLE shard_t"))[0][1].clone();
    assert!(
        text.ends_with(" /*T! SHARD_ROW_ID_BITS=15 */"),
        "the option round-trips through SHOW CREATE TABLE:\n{text}"
    );
}

/// An unsharded table's handle is the counter itself.
#[test]
fn a_table_without_shard_bits_allocates_a_plain_counter() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT _tidb_rowid FROM t")),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
    assert!(!row_text(session.run("SHOW CREATE TABLE t"))[0][1].contains("SHARD_ROW_ID_BITS"));
}
