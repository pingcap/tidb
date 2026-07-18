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

//! Direct `ANALYZE INCREMENTAL` rows from Go's `TestAnalyze` and executor
//! integration fixture.

use super::*;
use tidb_ast::{AdminStmt, AnalyzeIncrementalTarget, Stmt};

#[test]
fn analyze_incremental_restores_original_go_parser_rows() {
    for (sql, restored) in [
        (
            "analyze incremental table t index",
            "ANALYZE INCREMENTAL TABLE `t` INDEX",
        ),
        (
            "analyze incremental table t index idx",
            "ANALYZE INCREMENTAL TABLE `t` INDEX `idx`",
        ),
    ] {
        assert_eq!(r(sql), restored, "{sql}");
    }
}

#[test]
fn analyze_incremental_preserves_partition_target_and_index_payload() {
    let statement = parse("analyze incremental table t partition p0 index idx")
        .expect("parse original executor integration row");
    assert_eq!(
        statement.restore(),
        "ANALYZE INCREMENTAL TABLE `t` PARTITION `p0` INDEX `idx`"
    );
    let Stmt::Admin(admin) = statement else {
        panic!("expected administrative statement");
    };
    let AdminStmt::AnalyzeIncremental(analyze) = admin.as_ref() else {
        panic!("expected typed incremental ANALYZE statement");
    };
    assert_eq!(
        analyze.indexes.as_deref(),
        Some(["idx".to_string()].as_slice())
    );
    assert_eq!(
        analyze.target,
        AnalyzeIncrementalTarget::Partitions {
            tables: vec![vec!["t".to_string()]],
            partitions: vec!["p0".to_string()],
        }
    );
}
