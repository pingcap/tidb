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

//! Runnable rename-table portion of Go
//! `pkg/ddl/masking_policy_test.go::TestMaskingPolicyRenameTableNoPolicy`.
//! Masking-policy DDL and its system-table state are not implemented, so this
//! module makes no claim for those parts of the Go test.

use crate::{
    run_create_table_on, run_insert_on, run_rename_table_in, run_select_on, Catalog, StmtContext,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `masking_policy_test.go:143::TestMaskingPolicyRenameTableNoPolicy`,
/// runnable half. Go renames a table that carries NO policy and asserts
/// the rename succeeds, the data reads back, and the policy count stays 0.
/// The rename + read-back half runs here; the `select count(*) from
/// mysql.tidb_masking_policy` assertion needs the unimplemented sys table.
#[test]
fn masking_policy_rename_table_without_a_policy_renames_and_reads_back() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE old_table (id INT PRIMARY KEY, c VARCHAR(100))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO old_table VALUES (1, 'secret')",
        &mut catalog,
        &ctx(),
    )
    .unwrap();

    // Go: `rename table old_table to new_table` with no policy to update.
    run_rename_table_in(
        "RENAME TABLE old_table TO new_table",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
    )
    .expect("Go: the rename succeeds with no policy present");

    // Go: `select c from new_table` -> 'secret'.
    let rows = run_select_on("SELECT c FROM new_table", &catalog, &ctx()).unwrap();
    assert_eq!(rows.len(), 1);
    let tidb_datatype::Datum::String(text) = &rows[0][0] else {
        panic!("expected the string datum");
    };
    assert_eq!(String::from_utf8_lossy(text.bytes()), "secret");
    // The sys-table-count assertion (Go: count(*) == 0) is the documented
    // masking-store gap; nothing here can observe it.
}
