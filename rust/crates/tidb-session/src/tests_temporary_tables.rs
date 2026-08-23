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

//! Temporary tables, both scopes.
//!
//! The two kinds differ in exactly two places and are otherwise the same
//! object, so the tests below are organised by those two:
//!
//! * WHERE THE SCHEMA LIVES. A GLOBAL temporary table is created by a real
//!   DDL job and is in the shared infoschema, so every session can name it; a
//!   LOCAL one never leaves `SessionVars.LocalTemporaryTables` and no other
//!   session can see it at all (Go `pkg/table/temptable`).
//! * HOW LONG THE ROWS LIVE. A global temporary table's rows belong to one
//!   TRANSACTION -- Go's snapshot interceptor answers empty for it and the
//!   commit filter throws its keys away, which together are all
//!   `ON COMMIT DELETE ROWS` means. A local one's rows belong to the SESSION
//!   and are copied out of the transaction buffer at commit
//!   (`session.commitTxnWithTemporaryData`).

use crate::tests_support::{row_text, show_create};
use crate::*;

/// A session with `test` selected, which is what every case below starts
/// from.
fn temporary_session() -> Session {
    Session::default()
}

/// The error a refused statement reports on the wire.
fn refusal(session: &mut Session, sql: &str) -> (u16, String) {
    let reported = session
        .run(sql)
        .expect_err("the statement must be refused")
        .to_mysql_error();
    (reported.code, reported.message)
}

/// Go `setTemporaryType` accepts `ON COMMIT DELETE ROWS`, and
/// `ConstructResultOfShowCreateTable` (`pkg/executor/show.go:1073`, `:1421`)
/// prints the kind back in the header and the clause back after the comment.
///
/// The recorded corpus statement is
/// `create global temporary table tmp1(a int(11), key idx_a(a)) on commit
/// delete rows` (`tests/integrationtest/t/bindinfo/temptable.test`).
#[test]
fn a_global_temporary_table_round_trips_through_show_create_table() {
    let mut session = temporary_session();
    session
        .run("create global temporary table tmp1(a int(11), key idx_a(a)) on commit delete rows")
        .expect("a global temporary table is created by an ordinary DDL");
    assert_eq!(
        show_create(&mut session, "tmp1"),
        "CREATE GLOBAL TEMPORARY TABLE `tmp1` (\n  \
         `a` int DEFAULT NULL,\n  \
         KEY `idx_a` (`a`)\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS"
    );
}

/// A LOCAL temporary table prints plain `TEMPORARY` and NO `ON COMMIT`
/// clause: the clause is a global-only property, and Go's suffix is gated on
/// `TempTableGlobal` alone.
#[test]
fn a_local_temporary_table_prints_no_on_commit_clause() {
    let mut session = temporary_session();
    session
        .run("create temporary table tmp2(a int(11), key idx_a(a))")
        .expect("a local temporary table needs no DDL job");
    assert_eq!(
        show_create(&mut session, "tmp2"),
        "CREATE TEMPORARY TABLE `tmp2` (\n  \
         `a` int DEFAULT NULL,\n  \
         KEY `idx_a` (`a`)\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
}

/// Go `setTemporaryType` (`pkg/ddl/create_table.go:1029`): the statement
/// parses with `OnCommitDelete = false` and is then refused, because a global
/// temporary table has no way to keep rows past its transaction --
/// `temporaryTableKVFilter` discards every one of its keys before commit.
#[test]
fn on_commit_preserve_rows_is_refused_rather_than_ignored() {
    let mut session = temporary_session();
    assert_eq!(
        refusal(
            &mut session,
            "create global temporary table g(a int) on commit preserve rows",
        ),
        (
            8200,
            "TiDB doesn't support ON COMMIT PRESERVE ROWS for now".to_owned()
        )
    );
}

/// Go `checkCreateTableGrammar` (`pkg/planner/core/preprocess.go:946`) and
/// `checkColumnOptions` (`:1197`). The three arguments are Go's own
/// spellings, `PLACEMENT` upper case among them.
#[test]
fn the_options_a_temporary_table_cannot_carry_are_refused_with_gos_own_words() {
    let mut session = temporary_session();
    session
        .run("create placement policy p primary_region='r' regions='r'")
        .expect("the policy the option below names must exist first");
    assert_eq!(
        refusal(
            &mut session,
            "create temporary table t (a int) shard_row_id_bits = 4",
        ),
        (
            8006,
            "`shard_row_id_bits` is unsupported on temporary tables.".to_owned()
        )
    );
    assert_eq!(
        refusal(
            &mut session,
            "create temporary table t (a int) placement policy = p",
        ),
        (
            8006,
            "`PLACEMENT` is unsupported on temporary tables.".to_owned()
        )
    );
    assert_eq!(
        refusal(
            &mut session,
            "create temporary table t (a bigint primary key auto_random)",
        ),
        (
            8006,
            "`auto_random` is unsupported on temporary tables.".to_owned()
        )
    );
}

/// Go `checkAddPartitionOnTemporaryMode` (`pkg/ddl/partition.go:4657`): a
/// partition is a distinct PHYSICAL table and a temporary table has none.
/// The error is 1562, not an 8006.
#[test]
fn a_partitioned_temporary_table_is_refused_with_1562() {
    let mut session = temporary_session();
    assert_eq!(
        refusal(
            &mut session,
            "create temporary table t (a int) partition by hash(a) partitions 2",
        ),
        (
            1562,
            "Cannot create temporary table with partitions".to_owned()
        )
    );
}

/// Go `checkCreateTableGrammar` (`preprocess.go:933`): copying a temporary
/// table is refused whatever the target's own kind is.
#[test]
fn create_table_like_a_temporary_table_is_refused() {
    let mut session = temporary_session();
    session
        .run("create temporary table src (a int)")
        .expect("the source");
    assert_eq!(
        refusal(&mut session, "create table copy1 like src"),
        (
            8006,
            "`create table like` is unsupported on temporary tables.".to_owned()
        )
    );
}

/// Go `createSessionTemporaryTable` (`pkg/executor/ddl.go:301`) asks
/// `getLocalTemporaryTable`, so the only collision a local temporary table
/// can have is with ANOTHER local temporary table. A permanent table of the
/// same name is SHADOWED, and -- the part that is easy to get wrong -- it is
/// still there when the temporary one is dropped.
#[test]
fn a_local_temporary_table_shadows_a_permanent_one_without_destroying_it() {
    let mut session = temporary_session();
    session.run("create table t (a int)").expect("permanent");
    session
        .run("insert into t values (1)")
        .expect("a row only the permanent table has");
    session
        .run("create temporary table t (b varchar(8))")
        .expect("the temporary table shadows rather than collides");

    // The name now resolves to the temporary table: its own column list, and
    // none of the permanent table's rows.
    session
        .run("insert into t values ('x')")
        .expect("the temporary table takes the write");
    assert_eq!(row_text(session.run("select * from t")), vec![vec!["x"]]);

    // A SECOND temporary table of the same name IS a collision.
    assert_eq!(
        refusal(&mut session, "create temporary table t (c int)"),
        (1050, "Table 'test.t' already exists".to_owned())
    );

    session
        .run("drop temporary table t")
        .expect("the temporary table goes");
    assert_eq!(row_text(session.run("select * from t")), vec![vec!["1"]]);
}

/// Go `DDLExec.Next` (`pkg/executor/ddl.go:129`): `DROP TEMPORARY TABLE`
/// matches LOCAL temporary tables only, reports every other name as one 1051,
/// and drops NOTHING when any name misses -- it returns before
/// `dropLocalTemporaryTables` runs.
#[test]
fn drop_temporary_table_matches_only_local_temporary_tables() {
    let mut session = temporary_session();
    session.run("create table perm (a int)").expect("permanent");
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table loc (a int)")
        .expect("local");

    assert_eq!(
        refusal(&mut session, "drop temporary table perm"),
        (1051, "Unknown table 'test.perm'".to_owned())
    );
    assert_eq!(
        refusal(&mut session, "drop temporary table g"),
        (1051, "Unknown table 'test.g'".to_owned())
    );
    // The local table listed beside a bad name survives, because Go reports
    // before it drops.
    assert_eq!(
        refusal(&mut session, "drop temporary table loc, nosuch"),
        (1051, "Unknown table 'test.nosuch'".to_owned())
    );
    session
        .run("select * from loc")
        .expect("the failed DROP dropped nothing");
    session
        .run("drop temporary table loc")
        .expect("named alone it goes");
    assert!(session.run("select * from loc").is_err());
}

/// Go `checkDropTemporaryTableGrammar` (`preprocess.go:1122`): 8007 for a
/// name that exists and is not a GLOBAL temporary table, checked over the
/// whole list before anything is dropped. A name that does not exist is left
/// to the drop itself.
#[test]
fn drop_global_temporary_table_refuses_every_other_kind_with_8007() {
    let mut session = temporary_session();
    session.run("create table perm (a int)").expect("permanent");
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");
    assert_eq!(
        refusal(&mut session, "drop global temporary table perm"),
        (
            8007,
            "`drop global temporary table` can only drop global temporary table".to_owned()
        )
    );
    // Checked over the WHOLE list first: `g` is not dropped by the statement
    // that names `perm` beside it.
    session.run("select * from g").expect("g is still there");
    session
        .run("drop global temporary table g")
        .expect("named alone it goes");
    assert!(session.run("select * from g").is_err());
}

/// A plain `DROP TABLE` drops a local temporary table too (Go strips such
/// names out of the statement before the DDL job is built), which is the
/// other half of the shadowing test above.
#[test]
fn drop_table_drops_a_local_temporary_table() {
    let mut session = temporary_session();
    session
        .run("create temporary table loc (a int)")
        .expect("local");
    session.run("drop table loc").expect("no DDL job needed");
    assert!(session.run("select * from loc").is_err());
}

/// `ON COMMIT DELETE ROWS`, which in Go is not a truncation step but the
/// consequence of where the rows are: `TxnCtx.TemporaryTables` is rebuilt per
/// transaction and the snapshot interceptor answers EMPTY for a global
/// temporary table, so nothing an earlier transaction wrote is reachable.
///
/// In autocommit every statement is its own transaction, so the rows an
/// `INSERT` writes are already gone by the next statement.
#[test]
fn a_global_temporary_tables_rows_do_not_survive_their_transaction() {
    let mut session = temporary_session();
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");

    session.run("insert into g values (1)").expect("autocommit");
    assert!(
        row_text(session.run("select * from g")).is_empty(),
        "an autocommit INSERT's rows die with its own transaction"
    );

    session.run("begin").expect("open a transaction");
    session.run("insert into g values (1), (2)").expect("write");
    assert_eq!(
        row_text(session.run("select count(*) from g")),
        vec![vec!["2"]],
        "inside the transaction the rows are the session's own"
    );
    session.run("commit").expect("commit");
    assert!(
        row_text(session.run("select * from g")).is_empty(),
        "COMMIT is where DELETE ROWS happens"
    );
}

/// The mirror of the case above: a LOCAL temporary table's rows are the
/// SESSION's, so they outlive the transaction that wrote them -- Go's
/// `commitTxnWithTemporaryData` copies exactly the `TempTableLocal` keys out
/// of the transaction buffer into `SessionVars.TemporaryTableData`.
#[test]
fn a_local_temporary_tables_rows_outlive_their_transaction() {
    let mut session = temporary_session();
    session
        .run("create temporary table loc (a int)")
        .expect("local");
    session.run("insert into loc values (1)").expect("write");
    assert_eq!(
        row_text(session.run("select * from loc")),
        vec![vec!["1"]],
        "the row is still there in the next statement"
    );

    session.run("begin").expect("open");
    session.run("insert into loc values (2)").expect("write");
    session.run("rollback").expect("discard");
    assert_eq!(
        row_text(session.run("select * from loc")),
        vec![vec!["1"]],
        "the rolled-back write is gone and the committed one is not"
    );
}

/// The scoping claim itself: a peer session over the SAME catalog can name a
/// global temporary table (its schema is shared) but reads none of this
/// session's rows, and cannot name a local temporary table at all.
#[test]
fn a_peer_session_shares_the_schema_of_a_global_temporary_table_and_none_of_its_rows() {
    let mut session = temporary_session();
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table loc (a int)")
        .expect("local");

    let mut peer = Session::with_catalog(session.shared_catalog());
    peer.run("select * from g")
        .expect("the SCHEMA of a global temporary table is shared");
    assert!(
        peer.run("select * from loc").is_err(),
        "a local temporary table belongs to one session"
    );

    session.run("begin").expect("open");
    session.run("insert into g values (1)").expect("write");
    assert!(
        row_text(peer.run("select * from g")).is_empty(),
        "the peer sees none of this session's global temporary rows"
    );
    session.run("commit").expect("commit");
}

/// Go `fetchShowInfoByName` (`pkg/executor/show.go:540`) returns nothing for
/// a `TempTableLocal`, and the enumerating form reads an infoschema that
/// never had it. A global temporary table IS listed, because it is an
/// ordinary infoschema object.
#[test]
fn show_tables_lists_a_global_temporary_table_and_not_a_local_one() {
    let mut session = temporary_session();
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table loc (a int)")
        .expect("local");
    let listed = row_text(session.run("show tables"));
    assert!(listed.contains(&vec!["g".to_owned()]), "{listed:?}");
    assert!(!listed.contains(&vec!["loc".to_owned()]), "{listed:?}");
    // It is still nameable, which is what makes the exclusion an enumeration
    // rule rather than a visibility one.
    session.run("select * from loc").expect("still nameable");
}

/// Go `preprocessor.checkBindGrammar` (`pkg/planner/core/preprocess.go:613`):
/// `dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("create binding")`.
///
/// The four shapes here are the corpus's own
/// (`tests/integrationtest/t/bindinfo/temptable.test`, recorded as
/// `Error 8006 (HY000): \`create binding\` is unsupported on temporary
/// tables.`): the temporary table can be reached through a CTE, a join, an
/// `IN` subquery or a derived table, and the check walks the whole statement
/// rather than its top-level tables.
#[test]
fn create_binding_over_a_temporary_table_is_8006() {
    let mut session = temporary_session();
    session.run("create table t1(a int(11))").expect("base");
    session
        .run("create global temporary table tmp1(a int(11), key idx_a(a)) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table tmp2(a int(11), key idx_a(a))")
        .expect("local");

    for temporary in ["tmp1", "tmp2"] {
        for sql in [
            format!(
                "create global binding for with cte1 as (select a from {temporary}) \
                 select * from cte1 using with cte1 as (select a from {temporary}) select * from cte1"
            ),
            format!(
                "create global binding for select * from t1 inner join {temporary} \
                 on t1.a={temporary}.a using select * from t1 inner join {temporary} \
                 on t1.a={temporary}.a"
            ),
            format!(
                "create global binding for select * from t1 where t1.a in \
                 (select a from {temporary}) using select * from t1 where t1.a in \
                 (select a from {temporary} use index (idx_a))"
            ),
            format!(
                "create global binding for select * from (select * from {temporary}) \
                 using select * from (select * from {temporary})"
            ),
            format!(
                "create global binding for delete from t1 where t1.a in \
                 (select a from {temporary}) using delete from t1 where t1.a in \
                 (select a from {temporary})"
            ),
        ] {
            assert_eq!(
                refusal(&mut session, &sql),
                (
                    8006,
                    "`create binding` is unsupported on temporary tables.".to_owned()
                ),
                "{sql}"
            );
        }
    }
}

/// The check must not swallow the ordinary case: a binding over permanent
/// tables alone still works, and a name that resolves to nothing is skipped
/// rather than reported (Go's "drop table -> drop binding" comment).
#[test]
fn create_binding_over_permanent_tables_is_unaffected() {
    let mut session = temporary_session();
    session
        .run("create table t1(a int(11), key ka(a))")
        .expect("base");
    session
        .run("create global temporary table tmp1(a int(11)) on commit delete rows")
        .expect("global");
    session
        .run(
            "create global binding for select * from t1 where a = 1 \
              using select * from t1 use index(ka) where a = 1",
        )
        .expect("no temporary table is named");
    // Dropping the table the binding was over and then dropping the binding
    // is Go's own worked example of why an unresolvable name is skipped.
    session.run("drop table t1").expect("gone");
    session
        .run("drop global binding for select * from t1 where a = 1")
        .expect("a binding outlives its table");
    // And the refusal is still armed for the temporary one.
    assert_eq!(
        refusal(
            &mut session,
            "create global binding for select * from tmp1 using select * from tmp1",
        ),
        (
            8006,
            "`create binding` is unsupported on temporary tables.".to_owned()
        )
    );
}

/// `SAVEPOINT` over both kinds, which is `executor/executor_txn`'s
/// `TestSavepointWithTemporaryTable` in the corpus.
///
/// A temporary table's rows are in the transaction membuffer Go's
/// `RollbackMemDBToCheckpoint` truncates, so `ROLLBACK TO` takes them back
/// like any other write. The two kinds differ only in what is left at COMMIT:
/// a local temporary table's surviving rows are copied into the session
/// (`commitTxnWithTemporaryData`), a global one's are dropped by the commit
/// filter.
#[test]
fn rollback_to_savepoint_takes_back_temporary_rows_of_both_kinds() {
    let mut session = temporary_session();
    session
        .run("create temporary table loc (id int primary key, v int)")
        .expect("local");
    session
        .run("insert into loc values(1, 101)")
        .expect("write");
    session.run("begin").expect("open");
    session.run("savepoint sp0").expect("sp0");
    session
        .run("insert into loc values(2, 202)")
        .expect("write");
    session.run("savepoint sp1").expect("sp1");
    session
        .run("insert into loc values(3, 303)")
        .expect("write");
    session.run("rollback to sp1").expect("back to sp1");
    assert_eq!(
        row_text(session.run("select id from loc order by id")),
        vec![vec!["1"], vec!["2"]]
    );
    session.run("commit").expect("commit");
    assert_eq!(
        row_text(session.run("select id from loc order by id")),
        vec![vec!["1"], vec!["2"]],
        "a local temporary table's committed rows are the session's"
    );

    session
        .run("create global temporary table g (id int primary key, v int) on commit delete rows")
        .expect("global");
    session.run("begin").expect("open");
    session.run("savepoint sp0").expect("sp0");
    session.run("insert into g values(2, 202)").expect("write");
    session.run("savepoint sp1").expect("sp1");
    session.run("insert into g values(3, 303)").expect("write");
    session.run("savepoint sp2").expect("sp2");
    session.run("insert into g values(4, 404)").expect("write");
    session.run("rollback to sp2").expect("back to sp2");
    assert_eq!(
        row_text(session.run("select id from g order by id")),
        vec![vec!["2"], vec!["3"]]
    );
    session.run("rollback to sp1").expect("back to sp1");
    assert_eq!(
        row_text(session.run("select id from g order by id")),
        vec![vec!["2"]]
    );
    session.run("commit").expect("commit");
    assert!(
        row_text(session.run("select id from g order by id")).is_empty(),
        "a global temporary table's rows never survive their transaction"
    );
}

/// Go `DDLExec.Next` refuses six statements on a LOCAL temporary table with
/// `ErrUnsupportedLocalTempTableDDL` (8200), each naming itself
/// (`pkg/executor/ddl.go:273`, `:349`, `:413`, `:425`). The reason is
/// structural: every one of them is carried out by a DDL job that finds its
/// table by the id it has in the meta store, and a local temporary table is
/// in no meta store.
#[test]
fn the_ddl_a_local_temporary_table_cannot_take_names_itself_in_8200() {
    let mut session = temporary_session();
    session
        .run("create temporary table loc (a int, b int, key kb(b))")
        .expect("local");
    for (sql, statement) in [
        ("alter table loc add column c int", "ALTER TABLE"),
        ("create index ka on loc (a)", "CREATE INDEX"),
        ("drop index kb on loc", "DROP INDEX"),
        ("rename table loc to loc2", "RENAME TABLE"),
    ] {
        assert_eq!(
            refusal(&mut session, sql),
            (
                8200,
                format!("TiDB doesn't support {statement} for local temporary table")
            ),
            "{sql}"
        );
    }
}

/// Go `checkAdminCheckTableGrammar` (`preprocess.go:908`): a temporary
/// table's rows never reach TiKV, so there is no stored index to check them
/// against and the statement is refused rather than answering OK about
/// nothing. Both scopes are refused, and the message names the statement.
#[test]
fn admin_check_on_a_temporary_table_is_8006() {
    let mut session = temporary_session();
    session
        .run("create global temporary table g (a int, key ka(a)) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table loc (a int, key ka(a))")
        .expect("local");
    for table in ["g", "loc"] {
        assert_eq!(
            refusal(&mut session, &format!("admin check table {table}")),
            (
                8006,
                "`admin check table` is unsupported on temporary tables.".to_owned()
            )
        );
        assert_eq!(
            refusal(&mut session, &format!("admin check index {table} ka")),
            (
                8006,
                "`admin check index` is unsupported on temporary tables.".to_owned()
            )
        );
    }
}

/// Go `buildDataSource` (`logical_plan_builder.go:4963`): a view is a shared
/// definition and a LOCAL temporary table is one connection's private object,
/// so a view over one would be unresolvable for every other session -- 1352.
/// A GLOBAL temporary table is NOT refused, because its schema really is
/// shared.
#[test]
fn a_view_over_a_local_temporary_table_is_1352_and_over_a_global_one_is_not() {
    let mut session = temporary_session();
    session
        .run("create temporary table loc (a int)")
        .expect("local");
    session
        .run("create global temporary table g (a int) on commit delete rows")
        .expect("global");
    assert_eq!(
        refusal(&mut session, "create view v as select * from loc"),
        (
            1352,
            "View's SELECT refers to a temporary table 'loc'".to_owned()
        )
    );
    // Reached through a subquery too, which is why the check walks the whole
    // body rather than its top-level tables.
    assert_eq!(
        refusal(
            &mut session,
            "create view v as select * from g where a in (select a from loc)",
        ),
        (
            1352,
            "View's SELECT refers to a temporary table 'loc'".to_owned()
        )
    );
    session
        .run("create view v as select * from g")
        .expect("a global temporary table's schema is shared");
}

/// `ddl/db_integration`'s `TestPlacementOnTemporaryTable`, which asserts that
/// the SAME statement reports two different errors depending on the scope.
///
/// A LOCAL temporary table is intercepted in Go's EXECUTOR
/// (`pkg/executor/ddl.go:425`) before any option is looked at, so it is 8200;
/// a GLOBAL one reaches the DDL package, where `executor.go:646` names the
/// option and reports 8006.
#[test]
fn alter_placement_is_8200_on_a_local_temporary_table_and_8006_on_a_global_one() {
    let mut session = temporary_session();
    session
        .run("create placement policy x primary_region='r1' regions='r1'")
        .expect("policy");
    session
        .run("create global temporary table tplacement1 (id int) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table tplacement2 (id int)")
        .expect("local");
    assert_eq!(
        refusal(&mut session, "alter table tplacement1 placement policy='x'"),
        (
            8006,
            "`placement` is unsupported on temporary tables.".to_owned()
        )
    );
    assert_eq!(
        refusal(&mut session, "alter table tplacement2 placement policy='x'"),
        (
            8200,
            "TiDB doesn't support ALTER TABLE for local temporary table".to_owned()
        )
    );
}

/// `ddl/db_integration`'s `TestDropWithGlobalTemporaryTableKeyWord`, which is
/// the sharpest statement of `DROP GLOBAL TEMPORARY TABLE`'s two outcomes: a
/// name that EXISTS and is the wrong kind is 8007 even under `IF EXISTS`,
/// while a name that does not exist at all falls through to the ordinary drop
/// and is 1051 -- or a warning under `IF EXISTS`.
#[test]
fn drop_global_temporary_table_separates_wrong_kind_from_missing() {
    let mut session = temporary_session();
    session.run("create table tb(id int)").expect("permanent");
    session
        .run("create global temporary table temp(id int) on commit delete rows")
        .expect("global");
    session
        .run("create temporary table ltemp1(id int)")
        .expect("local");

    for sql in [
        "drop global temporary table tb",
        "drop global temporary table ltemp1",
        "drop global temporary table ltemp1, temp",
        "drop global temporary table temp, ltemp1",
        "drop global temporary table xxx, ltemp1",
        "drop global temporary table if exists tb",
        "drop global temporary table if exists ltemp1",
    ] {
        assert_eq!(
            refusal(&mut session, sql),
            (
                8007,
                "`drop global temporary table` can only drop global temporary table".to_owned()
            ),
            "{sql}"
        );
    }
    assert_eq!(
        refusal(&mut session, "drop global temporary table xxx"),
        (1051, "Unknown table 'test.xxx'".to_owned())
    );
    session
        .run("drop global temporary table if exists xxx")
        .expect("a missing name under IF EXISTS is a warning");
    session
        .run("drop global temporary table temp")
        .expect("the right kind, named alone");
}

/// Go `checkTTLInfoValid` (`pkg/ddl/ttl.go:97`): the TTL job deletes expired
/// rows from TiKV, and a temporary table has none there.
#[test]
fn ttl_on_a_temporary_table_is_8151() {
    let mut session = temporary_session();
    assert_eq!(
        refusal(
            &mut session,
            "create temporary table t (a datetime) ttl = a + interval 1 day",
        ),
        (
            8151,
            "Set TTL for temporary table is not allowed".to_owned()
        )
    );
}
