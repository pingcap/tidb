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

//! Runnable final-state table behavior derived from pinned Go
//! `pkg/ddl/db_table_test.go` tests.

use tidb_executor::{ddl, run_create_table_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

// --- TestDropTables (pkg/ddl/db_table_test.go:817) ---

#[test]
fn drop_tables_if_exists_and_partial_missing_table() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table b103_drop_one (a int)", &mut catalog).unwrap();
    let context = ctx();
    ddl::run_drop_table_in(
        "drop table if exists b103_missing, b103_drop_one",
        &mut catalog,
        "test",
        context.sql_mode(),
        context.foreign_key_checks(),
    )
    .unwrap();
    assert!(!catalog.contains_in("test", "b103_drop_one"));
    assert!(catalog.table_in("test", "b103_missing").is_none());
}
