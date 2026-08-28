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

//! Ports of the `pkg/ddl/masking_policy_test.go` and
//! `pkg/ddl/masking_policy_internal_test.go` tests in this batch (read from
//! the origin/master snapshot). The Go tests drive masking-policy DDL
//! (`CREATE/ALTER/DROP MASKING POLICY`, `mysql.tidb_masking_policy` rows
//! maintained by the DDL worker, `pkg/ddl/masking_policy.go`): this tier
//! has no masking-policy DDL execution — the metadata struct
//! (`tidb_model::masking_policy::MaskingPolicyInfo`) exists but nothing
//! creates or maintains policy rows — so the SQL-driven tests stay
//! documented `#[ignore]` gaps. The one Go test whose intent is mostly
//! plain rename mechanics (`TestMaskingPolicyRenameTableNoPolicy`) also
//! pins its rename half running below.

use crate::{
    run_create_table_on, run_insert_on, run_rename_table_in, run_select_on, Catalog, StmtContext,
};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `masking_policy_test.go:27::TestMaskingPolicyDDLBasic`. Go creates
/// `masking policy p on t(c) as c`, reads the sys-table row
/// (`p test t c \`c\` ENABLED CUSTOM NONE`), toggles DISABLED/ENABLED via
/// `ALTER TABLE`, replaces the policy with
/// `create or replace ... as concat(c, '_x')` (expression `CONCAT(%`,
/// masking_type CUSTOM), and drops it with `ALTER TABLE ... DROP MASKING
/// POLICY` leaving zero rows. The whole lifecycle runs through the
/// untranscreated masking-policy DDL actions.
// go-parity-gap: masking-policy DDL (CREATE/OR REPLACE/ALTER/DROP) and the
// mysql.tidb_masking_policy store are not transcreated.
#[test]
#[ignore]
fn masking_policy_ddl_basic_lifecycle_updates_the_sys_table() {
}

/// Go `masking_policy_test.go:55::TestMaskingPolicyCaseExpression`. A
/// policy whose expression is a CASE over `current_user()` is stored
/// ENABLED with the expression text normalized to uppercase function
/// spellings (`CASE WHEN %`, `%CURRENT_USER()%`).
// go-parity-gap: masking-policy DDL and expression normalization are not
// transcreated.
#[test]
#[ignore]
fn masking_policy_case_expression_is_stored_normalized_and_enabled() {
}

/// Go `masking_policy_test.go:71::TestMaskingPolicyIfNotExists`. Under the
/// db_change_test parallel-execution harness
/// (`dbChangeTestParallelExecSQL`), `CREATE MASKING POLICY IF NOT EXISTS`
/// enqueues twice and both runs converge to exactly one policy row for
/// (test_db_state, t_mask, p).
// go-parity-gap: masking-policy DDL is not transcreated; the
// double-enqueue convergence needs the job queue besides.
#[test]
#[ignore]
fn masking_policy_if_not_exists_converges_under_parallel_execution() {
}

/// Go `masking_policy_test.go:84::TestMaskingPolicyRenameTable`. Renaming
/// a table with a policy (`rename table old_table to new_table`) rewrites
/// the policy's table_name in the sys table (db_name stays `test`), after
/// which `ALTER TABLE new_table DROP MASKING POLICY p` removes it and the
/// data still reads back (`select c from new_table` → 'secret').
// go-parity-gap: masking-policy DDL and its rename bookkeeping are not
// transcreated.
#[test]
#[ignore]
fn masking_policy_rename_table_updates_the_policy_row() {
}

/// Go `masking_policy_test.go:113::TestMaskingPolicyRenameTableCrossDatabase`.
/// `RENAME TABLE db1.t TO db2.t` must rewrite the policy row's db_name
/// from db1 to db2 (table_name unchanged) while keeping the policy
/// droppable from the new location.
// go-parity-gap: masking-policy DDL and its cross-database rename
// bookkeeping are not transcreated.
#[test]
#[ignore]
fn masking_policy_cross_database_rename_updates_the_policy_row() {
}

/// Go `masking_policy_test.go:143::TestMaskingPolicyRenameTableNoPolicy`,
/// runnable half. Go renames a table that carries NO policy and asserts
/// the rename succeeds, the data reads back, and the policy count stays 0.
/// The rename + read-back half runs here; the `select count(*) from
/// mysql.tidb_masking_policy` assertion needs the sys table (the
/// [`#[ignore]`d] sibling pins it with the lifecycle tests).
#[test]
fn masking_policy_rename_table_without_a_policy_renames_and_reads_back() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE old_table (id INT PRIMARY KEY, c VARCHAR(100))", &mut catalog)
        .unwrap();
    run_insert_on("INSERT INTO old_table VALUES (1, 'secret')", &mut catalog, &ctx()).unwrap();

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

/// Go `masking_policy_test.go:162::TestMaskingPolicyRenameColumn`. Renaming
/// a policy'd column (`ALTER TABLE t RENAME COLUMN c TO c_new`) rewrites
/// BOTH the row's column_name and its stored expression (`` `c` `` →
/// `` `c_new` ``) in the sys table, and reads keep working.
// go-parity-gap: masking-policy DDL and its column-rename bookkeeping are
// not transcreated.
#[test]
#[ignore]
fn masking_policy_rename_column_updates_column_name_and_expression() {
}

/// Go `masking_policy_test.go:186::TestMaskingPolicyModifyColumnRejectUnsupportedType`.
/// MODIFY/CHANGE of a policy'd column to JSON is refused with
/// `errno.ErrUnsupportedDDLOperation` (pkg/errno/errname.go:1013,
/// "Unsupported %s"), leaving the policy row intact. The premise (a policy
/// on the column) cannot be constructed in this tier.
// go-parity-gap: masking-policy-aware MODIFY rejection is not transcreated.
#[test]
#[ignore]
fn masking_policy_modify_column_to_json_is_refused() {
}

/// Go `masking_policy_test.go:205::TestMaskingPolicyExpressionRejectsNonTargetColumn`.
/// CREATE (and CREATE OR REPLACE, and
/// `ALTER TABLE ... MODIFY MASKING POLICY ... SET EXPRESSION`) whose
/// expression references a column other than the policy target is refused
/// with `errno.ErrMaskingPolicyExprInvalidColumn`
/// (pkg/errno/errname.go:1159, "masking policy expression can only
/// reference the target column '%-.64s'"); an expression over only the
/// target column is accepted and stored backticked.
// go-parity-gap: masking-policy expression validation is not transcreated.
#[test]
#[ignore]
fn masking_policy_expression_referencing_non_target_columns_is_refused() {
}

/// Go `masking_policy_test.go:233::TestMaskingPolicyTruncateKeepsPolicy`.
/// `TRUNCATE TABLE` assigns a new table ID; the policy row must survive
/// with its table_id REWRITTEN to the new ID (Go asserts old != new), and
/// the policy stays fully operable afterwards (disable/enable/drop all
/// succeed through the new table).
// go-parity-gap: masking-policy truncate bookkeeping is not transcreated.
#[test]
#[ignore]
fn masking_policy_truncate_keeps_the_policy_with_a_new_table_id() {
}

/// Go `masking_policy_test.go:278::TestMaskingPolicyDropDatabaseCleanup`.
/// Dropping a database removes every policy row belonging to it (Go:
/// count for db_mask_cleanup goes 2 → 0).
// go-parity-gap: masking-policy drop-database cleanup is not transcreated.
#[test]
#[ignore]
fn masking_policy_drop_database_cleans_up_all_policy_rows() {
}

/// Go `masking_policy_internal_test.go:28::TestMaskingPolicyOperationsRequireSysTable`.
/// With the `mockMissingMaskingPolicySysTable` failpoint forcing the
/// masking sys table to be missing, every worker maintenance hook —
/// `dropMaskingPoliciesOnTable` (pkg/ddl/masking_policy.go:537),
/// `dropMaskingPoliciesOnColumn`, `syncMaskingPolicyForModifiedColumn`,
/// `updateMaskingPolicyTableIDAfterTruncate`
/// (pkg/ddl/masking_policy.go:569), and
/// `updateMaskingPolicyNamesAfterRename` — answers
/// `infoschema.ErrTableNotExists` rather than panicking or silently
/// skipping. The worker hooks are internals of the untranscreated
/// masking-policy DDL tier.
// go-parity-gap: the pkg/ddl masking-policy worker hooks are not
// transcreated.
#[test]
#[ignore]
fn masking_policy_worker_hooks_require_the_sys_table() {
}
