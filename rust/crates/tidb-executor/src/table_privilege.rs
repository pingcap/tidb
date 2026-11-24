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

//! Go's `visitInfo` checks are the planner's
//! ([`tidb_planner::privilege_check`]); this keeps the tables a statement
//! reads by name.

pub use tidb_planner::privilege_check::*;
use tidb_ast::Stmt;

/// Every `TableRef` reachable from `node`, in traversal order -- the row
/// sources Go's `buildDataSource` visits, and therefore the tables it
/// demands `SELECT` on.
pub fn read_tables(stmt: &Stmt, current_db: &str) -> Vec<(String, String)> {
    tidb_planner::preprocess::collect_physical_table_refs(stmt)
        .iter()
        .filter_map(|path| split_path(path, current_db))
        .collect()
}
