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

use crate::support::testkit::TestKit;

/// Go `masking_policy_test.go:143::TestMaskingPolicyRenameTableNoPolicy`,
/// runnable half. Go renames a table that carries NO policy and asserts
/// the rename succeeds, the data reads back, and the policy count stays 0.
/// The rename + read-back half runs here; the `select count(*) from
/// mysql.tidb_masking_policy` assertion needs the unimplemented sys table.
#[test]
fn masking_policy_rename_table_without_a_policy_renames_and_reads_back() {
    let mut tk = TestKit::new();
    tk.must_exec("drop table if exists old_table, new_table");

    // Create table without masking policy.
    tk.must_exec("create table old_table(id int primary key, c varchar(100))");
    tk.must_exec("insert into old_table values (1, 'secret')");

    // Rename the table (no policy to update).
    tk.must_exec("rename table old_table to new_table");

    // Verify no error and table works.
    tk.must_query("select c from new_table").check(["secret"]);
    // The sys-table-count assertion (Go: count(*) == 0) is the documented
    // masking-store gap; nothing here can observe it.
}
