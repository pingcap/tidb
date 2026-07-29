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

//! Errnos and messages here are the strings real TiDB printed, captured with a
//! testkit probe over a mock store.

use super::*;

/// Runs one sequence DDL statement against `catalog`.
fn run(catalog: &mut Catalog, sql: &str) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).expect("parse");
    let tidb_ast::Stmt::Ddl(ddl) = stmt else {
        panic!("not a DDL statement: {sql}");
    };
    match &*ddl {
        tidb_ast::DdlStmt::CreateSequence(create) => {
            run_create_sequence_in(create, catalog, "test").map(|_| ())
        }
        tidb_ast::DdlStmt::AlterSequence(alter) => run_alter_sequence_in(alter, catalog, "test"),
        tidb_ast::DdlStmt::DropSequence(drop) => run_drop_sequence_in(drop, catalog, "test"),
        other => panic!("not a sequence statement: {other:?}"),
    }
}

/// The `(code, message)` a failed statement reports on the wire.
fn error_of(catalog: &mut Catalog, sql: &str) -> (u16, String) {
    let error = run(catalog, sql).expect_err("expected an error");
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

/// Reads `n` values from a sequence in `catalog`.
fn take(catalog: &Catalog, name: &str, n: usize) -> Vec<Option<i64>> {
    let sequence = catalog.sequence_in("test", name).expect("sequence");
    (0..n).map(|_| sequence.allocator.next_val().ok()).collect()
}

/// The defaults `SHOW CREATE SEQUENCE` prints, captured verbatim.
#[test]
fn show_create_sequence_round_trips_the_captured_text() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence s1").unwrap();
    assert_eq!(
        show_create_sequence(catalog.sequence_in("test", "s1").unwrap()),
        "CREATE SEQUENCE `s1` start with 1 minvalue 1 maxvalue 9223372036854775806 \
         increment by 1 cache 1000 nocycle ENGINE=InnoDB"
    );

    run(
        &mut catalog,
        "create sequence s2 start with 5 increment by 3 minvalue 2 maxvalue 20 cache 2",
    )
    .unwrap();
    assert_eq!(
        show_create_sequence(catalog.sequence_in("test", "s2").unwrap()),
        "CREATE SEQUENCE `s2` start with 5 minvalue 2 maxvalue 20 increment by 3 \
         cache 2 nocycle ENGINE=InnoDB"
    );

    run(
        &mut catalog,
        "create sequence s5 increment by -1 minvalue -3 maxvalue 10 start with 1",
    )
    .unwrap();
    assert_eq!(
        show_create_sequence(catalog.sequence_in("test", "s5").unwrap()),
        "CREATE SEQUENCE `s5` start with 1 minvalue -3 maxvalue 10 increment by -1 \
         cache 1000 nocycle ENGINE=InnoDB"
    );

    // ALTER rewrites the printed options, and only the ones it names.
    run(&mut catalog, "alter sequence s2 increment by 5").unwrap();
    assert_eq!(
        show_create_sequence(catalog.sequence_in("test", "s2").unwrap()),
        "CREATE SEQUENCE `s2` start with 5 minvalue 2 maxvalue 20 increment by 5 \
         cache 2 nocycle ENGINE=InnoDB"
    );
}

/// The bounds a descending sequence defaults to are NOT the ascending ones
/// mirrored by the caller: Go picks them by the sign of the increment. There is
/// no capture for a bare `create sequence s increment by -1` printing them, so
/// the assertion is on the values Go's `handleSequenceOptions` computes.
#[test]
fn a_descending_sequence_takes_the_negative_defaults() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence d increment by -1").unwrap();
    let info = catalog.sequence_in("test", "d").unwrap().allocator.info();
    assert_eq!(info.start, -1);
    assert_eq!(info.max_value, -1);
    assert_eq!(info.min_value, -9_223_372_036_854_775_807);
}

/// Every captured 4136. The four spellings fail for four different clauses of
/// Go's `validateSequenceOptions`, and all report the same message.
#[test]
fn conflicting_option_values_are_4136() {
    let mut catalog = Catalog::default();
    for (sql, name) in [
        ("create sequence s6 increment by 0", "s6"),
        ("create sequence s7 minvalue 10 maxvalue 5", "s7"),
        ("create sequence s8 start with 1 minvalue 5", "s8"),
        ("create sequence s9 cache 0", "s9"),
    ] {
        assert_eq!(
            error_of(&mut catalog, sql),
            (
                4136,
                format!("Sequence 'test.{name}' values are conflicting")
            ),
            "{sql}"
        );
        assert!(catalog.sequence_in("test", name).is_none(), "{sql}");
    }
    // `start with 0 minvalue 0` is VALID (captured OK): the bounds only have
    // to be ordered, not positive.
    run(&mut catalog, "create sequence s10 start with 0 minvalue 0").unwrap();
}

/// An `ALTER` whose result would conflict is 4136 too, and leaves the sequence
/// untouched.
#[test]
fn an_alter_into_a_conflicting_state_is_4136_and_changes_nothing() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence s maxvalue 100").unwrap();
    assert_eq!(
        error_of(&mut catalog, "alter sequence s increment by 0"),
        (4136, "Sequence 'test.s' values are conflicting".to_owned())
    );
    assert_eq!(
        catalog
            .sequence_in("test", "s")
            .unwrap()
            .allocator
            .info()
            .increment,
        1
    );
}

/// A sequence shares the TABLE namespace, so a duplicate name is the TABLE
/// error 1050 -- captured for `create sequence s1` twice AND for
/// `create table s1 (a int)` over a sequence.
#[test]
fn a_duplicate_sequence_name_is_1050() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence s1").unwrap();
    assert_eq!(
        error_of(&mut catalog, "create sequence s1"),
        (1050, "Table 'test.s1' already exists".to_owned())
    );
    // `IF NOT EXISTS` reports nothing and creates nothing.
    run(&mut catalog, "create sequence if not exists s1").unwrap();
    assert_eq!(take(&catalog, "s1", 1), [Some(1)]);
}

/// `DROP SEQUENCE` and `ALTER SEQUENCE` report a missing name with DIFFERENT
/// errors, both captured:
///
/// ```text
/// drop sequence nosuch                  -- [schema:4139] Unknown SEQUENCE: 'test.nosuch'
/// alter sequence nosuch increment by 2  -- [schema:1146] Table 'test.nosuch' doesn't exist
/// ```
#[test]
fn a_missing_sequence_is_4139_for_drop_and_1146_for_alter() {
    let mut catalog = Catalog::default();
    assert_eq!(
        error_of(&mut catalog, "drop sequence nosuch"),
        (4139, "Unknown SEQUENCE: 'test.nosuch'".to_owned())
    );
    assert_eq!(
        error_of(&mut catalog, "alter sequence nosuch increment by 2"),
        (1146, "Table 'test.nosuch' doesn't exist".to_owned())
    );
    // `IF EXISTS` silences both.
    run(&mut catalog, "drop sequence if exists nosuch").unwrap();
    run(
        &mut catalog,
        "alter sequence if exists nosuch increment by 2",
    )
    .unwrap();
}

/// `DROP SEQUENCE a, b` drops what it finds before reporting what it does not.
/// Captured: after `drop sequence s1, nosuch` fails with 4139, `SHOW TABLES`
/// no longer lists `s1`.
#[test]
fn drop_sequence_drops_the_names_it_finds_before_reporting() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence s1").unwrap();
    assert_eq!(
        error_of(&mut catalog, "drop sequence s1, nosuch"),
        (4139, "Unknown SEQUENCE: 'test.nosuch'".to_owned())
    );
    assert!(catalog.sequence_in("test", "s1").is_none());
}

/// `RESTART` is an ALTER-only option, refused at parse time on a `CREATE` --
/// real TiDB errors on `create sequence s restart with 5` too (captured
/// through gorun).
#[test]
fn restart_is_an_alter_only_option() {
    let mut catalog = Catalog::default();
    assert!(tidb_parser::parse("create sequence s restart with 5").is_err());
    // On ALTER it works, including the bare form, which restarts at START.
    run(&mut catalog, "create sequence s start with 7").unwrap();
    assert_eq!(take(&catalog, "s", 2), [Some(7), Some(8)]);
    run(&mut catalog, "alter sequence s restart with 20").unwrap();
    assert_eq!(take(&catalog, "s", 1), [Some(20)]);
    run(&mut catalog, "alter sequence s restart").unwrap();
    assert_eq!(take(&catalog, "s", 1), [Some(7)]);
}

/// `ALTER SEQUENCE` naming one option keeps the rest, which is why the printed
/// text above changed only its `increment by`. It also discards the cache, so
/// the next value is re-seeked -- the captured 1, 4 then 6, 11, 16 run, here
/// through the catalog rather than the allocator directly.
#[test]
fn alter_sequence_keeps_unnamed_options_and_discards_the_cache() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence s increment by 3 cache 2").unwrap();
    assert_eq!(take(&catalog, "s", 2), [Some(1), Some(4)]);
    run(&mut catalog, "alter sequence s increment by 5").unwrap();
    assert_eq!(take(&catalog, "s", 3), [Some(6), Some(11), Some(16)]);
    let info = catalog.sequence_in("test", "s").unwrap().allocator.info();
    assert_eq!(
        info.cache_value, 2,
        "CACHE survived an ALTER that renamed only INCREMENT"
    );
}
