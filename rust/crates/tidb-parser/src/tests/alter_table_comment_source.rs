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

//! Direct source rows for table-level `ALTER TABLE ... COMMENT`.

use super::*;

/// Exact standalone `TestDDL` COMMENT row at `pkg/parser/parser_test.go:3419`.
///
/// The complete mixed owner row, including its leading `ENABLE KEYS`, lives in
/// `ddl::alter_table_enable_disable_keys_match_go_owner_rows`; these rows keep
/// the COMMENT payload boundary explicit.
#[test]
fn alter_table_comment_restores_original_go_rows() {
    assert_eq!(
        r("alter table t comment 'cmt' partition by hash(a)"),
        "ALTER TABLE `t` COMMENT = 'cmt' PARTITION BY HASH (`a`) PARTITIONS 1"
    );
}

/// Go's `parseTableOptionStringLit` accepts optional `=` and only string
/// literals for its COMMENT branch.
#[test]
fn alter_table_comment_retains_the_shared_table_option_payload() {
    assert_eq!(
        r("alter table t comment 'a\\'b'"),
        "ALTER TABLE `t` COMMENT = 'a''b'"
    );
    assert!(parse("alter table t comment comment").is_err());

    let tidb_ast::Stmt::Ddl(statement) = parse("alter table t comment = 'typed'").expect("parse")
    else {
        panic!("expected ALTER TABLE statement");
    };
    let tidb_ast::DdlStmt::AlterTable(statement) = statement.as_ref() else {
        panic!("expected ALTER TABLE statement");
    };
    assert_eq!(
        statement.actions,
        vec![tidb_ast::AlterTableAction::SetTableOptions {
            options: vec![tidb_ast::TableOption::Comment("typed".to_string())],
        }]
    );
}
