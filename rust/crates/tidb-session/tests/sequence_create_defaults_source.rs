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
//! `pkg/ddl/sequence_test.go:32`), run through the Go testkit port: every
//! statement is SQL on a mock-store session, and the stored options are read
//! the way Go's `external.GetTableByName` reads them, from the current schema.

use crate::support::testkit::TestKit;

/// Go rows at `pkg/ddl/sequence_test.go:41-58`: every conflicting option
/// combination fails with `ErrSequenceInvalidData` (4136, Go message
/// `Sequence 'test.seq' values are conflicting`). The `increment 0` row is
/// the "increment should not be set as 0" regression; the
/// `maxvalue == i64::MAX` row is what keeps the DEFAULT maximum at
/// `MaxInt64 - 1`.
#[test]
fn create_sequence_conflicting_options_report_4136() {
    let mut tk = TestKit::new();
    tk.must_exec("drop sequence if exists seq");
    let invalid = [
        "create sequence seq increment 0",
        "create sequence seq maxvalue 1 minvalue 2",
        "create sequence seq maxvalue 1 minvalue 1",
        "create sequence seq maxvalue 9223372036854775807 minvalue 1",
        "create sequence seq maxvalue 1 start with 2",
        "create sequence seq increment 100000 cache 922337203685477",
    ];
    for sql in invalid {
        assert_eq!(
            tk.must_get_err_code(sql, 4136).message,
            "Sequence 'test.seq' values are conflicting",
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
    let mut tk = TestKit::new();
    tk.must_exec("create sequence seq");
    let schema = tk.info_schema();
    let sequence = schema
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
    let mut tk = TestKit::new();
    assert_eq!(
        tk.must_get_err_code("create sequence `seq  `", 1103).message,
        "Incorrect table name 'seq  '"
    );
}

/// Go row `pkg/ddl/sequence_test.go:61`: `create sequence seq CHARSET=utf8`
/// fails with `ErrSequenceUnsupportedTableOption` (8227), rendered as
/// `Unsupported sequence table-option utf8`.
#[test]
fn create_sequence_unsupported_table_option_reports_8227() {
    let mut tk = TestKit::new();
    assert_eq!(
        tk.must_get_err_code("create sequence seq CHARSET=utf8", 8227).message,
        "Unsupported sequence table-option utf8"
    );
}

/// Go row `pkg/ddl/sequence_test.go:63`: `create sequence seq comment="test"`
/// SUCCEEDS -- Go rejects every table option EXCEPT `COMMENT` and `ENGINE`
/// (`pkg/ddl/sequence.go`'s option loop). The comment is stored in
/// `SequenceInfo.Comment` and is included by `SHOW CREATE SEQUENCE`.
#[test]
fn create_sequence_accepts_a_comment_option() {
    let mut tk = TestKit::new();
    tk.must_exec("create sequence seq comment=\"test\"");
    assert_eq!(
        tk.info_schema().sequence_in("test", "seq").expect("sequence").comment,
        "test"
    );
    tk.must_query("show create sequence seq").check_at(
        &[1],
        ["CREATE SEQUENCE `seq` start with 1 minvalue 1 maxvalue 9223372036854775806 \
          increment by 1 cache 1000 nocycle ENGINE=InnoDB COMMENT='test'"],
    );
}
