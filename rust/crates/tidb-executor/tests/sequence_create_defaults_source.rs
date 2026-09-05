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

//! Ports of Go `pkg/ddl/sequence_test.go::TestCreateSequence` (master,
//! `pkg/ddl/sequence_test.go:32`). The DDL half runs against the transcreated
//! `CREATE SEQUENCE` carrier (`src/ddl_sequence.rs`, mirroring Go
//! `pkg/ddl/sequence.go`'s option handling and `pkg/ddl/executor.go`'s
//! sequence job); the rows whose observable behavior lives in the planner
//! preprocessor or the privilege system are recorded as `#[ignore]` gap
//! tests with the contract re-derived from the Go source. Nothing is
//! approximated.

use tidb_executor::{run_create_sequence_in, Catalog, DriverError};

/// Parses and runs one sequence DDL statement against `catalog`.
fn run(catalog: &mut Catalog, sql: &str) -> Result<(), DriverError> {
    let stmt = tidb_parser::parse(sql).expect("sequence statement parses");
    let tidb_ast::Stmt::Ddl(ddl) = stmt else {
        panic!("expected a DDL envelope for {sql}")
    };
    match &*ddl {
        tidb_ast::DdlStmt::CreateSequence(create) => {
            run_create_sequence_in(create, catalog, "test").map(|_| ())
        }
        other => panic!("unexpected DDL payload for {sql}: {other:?}"),
    }
}

/// The `(code, message)` a failed statement reports on the wire.
fn error_of(catalog: &mut Catalog, sql: &str) -> (u16, String) {
    let error = run(catalog, sql).expect_err("expected an error");
    let mysql = error.to_mysql_error();
    (mysql.code, mysql.message)
}

/// Go rows at `pkg/ddl/sequence_test.go:41-58`: every conflicting option
/// combination fails with `ErrSequenceInvalidData` (4136, Go message
/// `Sequence 'test.seq' values are conflicting`, carried at
/// `src/driver/errors/mod.rs:753`). The `increment 0` row is the "increment
/// should not be set as 0" regression; the `maxvalue == i64::MAX` row is what
/// keeps the DEFAULT maximum at `MaxInt64 - 1`.
#[test]
fn create_sequence_conflicting_options_report_4136() {
    let invalid = [
        "create sequence seq increment 0",
        "create sequence seq maxvalue 1 minvalue 2",
        "create sequence seq maxvalue 1 minvalue 1",
        "create sequence seq maxvalue 9223372036854775807 minvalue 1",
        "create sequence seq maxvalue 1 start with 2",
        "create sequence seq increment 100000 cache 922337203685477",
    ];
    for sql in invalid {
        let mut catalog = Catalog::default();
        assert_eq!(
            error_of(&mut catalog, sql),
            (
                4136,
                "Sequence 'test.seq' values are conflicting".to_owned()
            ),
            "sql: {sql}"
        );
    }
}

/// Go rows at `pkg/ddl/sequence_test.go:65-75`: an option-free
/// `CREATE SEQUENCE` stores `model.Default*` values -- increment 1, start 1,
/// minvalue 1, maxvalue 9223372036854775806 (`MaxInt64 - 1`), cache enabled
/// with value 1000, cycle off -- and the entry is a SEQUENCE
/// (`Meta().IsSequence()`).
#[test]
fn create_sequence_stores_the_positive_default_options() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence seq").expect("option-free create succeeds");
    let sequence = catalog
        .sequence_in("test", "seq")
        .expect("sequence is a table-namespace entry");
    let info = sequence.allocator.info();
    assert_eq!(info.increment, 1, "model.DefaultSequenceIncrementValue");
    assert_eq!(info.start, 1, "model.DefaultPositiveSequenceStartValue");
    assert_eq!(info.min_value, 1, "model.DefaultPositiveSequenceMinValue");
    assert_eq!(
        info.max_value,
        9_223_372_036_854_775_806,
        "model.DefaultPositiveSequenceMaxValue"
    );
    assert!(info.cache, "Cache defaults to true");
    assert_eq!(info.cache_value, 1000, "model.DefaultSequenceCacheValue");
    assert!(!info.cycle, "Cycle defaults to false");
}

/// Go row `pkg/ddl/sequence_test.go:38`:
/// ``create sequence `seq  ` `` fails with `ErrWrongTableName` (1103) because
/// the planner preprocessor rejects a name whose last byte is a space
/// (`resolveCreateSequenceStmt`, `pkg/planner/core/preprocess.go:2091`, via
/// `util.IsInCorrectIdentifierName`, `pkg/util/util.go:280`).
#[test]
fn create_sequence_space_terminated_name_reports_1103() {
    let mut catalog = Catalog::default();
    assert_eq!(
        error_of(&mut catalog, "create sequence `seq  `"),
        (1103, "Incorrect table name 'seq  '".to_owned())
    );
}

/// Go row `pkg/ddl/sequence_test.go:61`: `create sequence seq CHARSET=utf8`
/// fails with `ErrSequenceUnsupportedTableOption` (8227), rendered as
/// `Unsupported sequence table-option utf8`. The Rust carrier previously
/// refused every table option (`src/ddl_sequence.rs:166`) with generic 1105.
#[test]
fn create_sequence_unsupported_table_option_reports_8227() {
    let mut catalog = Catalog::default();
    assert_eq!(
        error_of(&mut catalog, "create sequence seq charset=utf8"),
        (
            8227,
            "Unsupported sequence table-option utf8".to_owned()
        )
    );
}

/// Go row `pkg/ddl/sequence_test.go:63`: `create sequence seq comment="test"`
/// SUCCEEDS -- Go rejects every table option EXCEPT `COMMENT` and `ENGINE`
/// (`pkg/ddl/sequence.go`'s option loop). The comment is stored in
/// `SequenceInfo.Comment` and is included by `SHOW CREATE SEQUENCE`.
#[test]
fn create_sequence_accepts_a_comment_option() {
    let mut catalog = Catalog::default();
    run(&mut catalog, "create sequence seq comment='test'")
        .expect("Go accepts COMMENT on CREATE SEQUENCE");
    let sequence = catalog.sequence_in("test", "seq").expect("sequence");
    assert_eq!(sequence.comment, "test");
    assert_eq!(
        tidb_executor::show_create_sequence(sequence),
        "CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 \
         increment by 1 cache 1000 nocycle ENGINE=InnoDB COMMENT='test'"
    );
}

/// Go rows `pkg/ddl/sequence_test.go:77-93`: a user granted only `select` on
/// `test.*` is denied `create sequence my_seq` with
/// `[planner:1142]CREATE command denied to user 'myuser'@'localhost' for
/// table 'my_seq'`.
// go-parity-gap: no privilege/authorization carrier (planner privilege
// check and `session.Auth` are not transcreated in this tier).
#[test]
#[ignore]
fn create_sequence_requires_create_privilege() {
}
