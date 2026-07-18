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

//! Source-backed `ALTER TABLE ... ANALYZE PARTITION` rows from
//! `pkg/parser/ddl_alter_parser.go#parseAlterAnalyzePartition`.

use super::*;
use tidb_ast::{AdminStmt, AnalyzeTarget, Stmt};

#[test]
fn alter_analyze_partition_restores_the_go_analyze_table_shape() {
    let statement = parse("ALTER TABLE tkey14 ANALYZE PARTITION p3")
        .expect("parse the integration-test ALTER ANALYZE row");
    assert_eq!(statement.restore(), "ANALYZE TABLE `tkey14` PARTITION `p3`");
    let Stmt::Admin(admin) = statement else {
        panic!("ALTER TABLE ... ANALYZE must use Go's AnalyzeTableStmt envelope");
    };
    let AdminStmt::AnalyzeTable(analyze) = admin.as_ref() else {
        panic!("expected typed ANALYZE TABLE statement");
    };
    assert_eq!(analyze.tables, vec![vec!["tkey14".to_owned()]]);
    assert_eq!(analyze.partitions, vec!["p3".to_owned()]);
    assert!(matches!(analyze.target, AnalyzeTarget::Default));
    assert!(analyze.options.is_empty());
}

#[test]
fn alter_analyze_partition_requires_a_partition_list() {
    assert!(parse("ALTER TABLE tkey14 ANALYZE").is_err());
    assert!(parse("ALTER TABLE tkey14 ANALYZE PARTITION").is_err());
}
