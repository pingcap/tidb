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

//! Gap tests for Go `pkg/executor/revoke_test.go` (items 554-559). Every
//! test drives GRANT/REVOKE statements against the `mysql.User` / `mysql.DB`
//! / `mysql.tables_priv` / `mysql.columns_priv` system tables through a
//! live session; this tier has no user accounts, privilege tables, or
//! account-management statements.

/// Go `pkg/executor/revoke_test.go:29::TestRevokeGlobal`: REVOKE against a
/// non-existent user errors; after `GRANT ALL PRIVILEGES ON *.*`, each
/// `REVOKE <priv> ON *.*` flips the matching `mysql.User` column
/// (`mysql.Priv2UserCol`) to `N`. Needs the account-management executor
/// (pkg/executor/revoke.go:63 `RevokeExec.Next`) and the mysql.user table.
#[test]
#[ignore = "go-parity-gap: REVOKE GLOBAL has no executor (pkg/executor/revoke.go:63) and mysql.User privilege columns are unported"]
fn revoke_global_flips_each_user_privilege_column_to_n() {}

/// Go `pkg/executor/revoke_test.go:58::TestRevokeDBScope`: `GRANT ALL ON
/// test.*` sets every `mysql.DB` privilege column to `Y`; each REVOKE flips
/// its column to `N`, and revoking the LAST of `mysql.AllDBPrivs` DELETES
/// the row entirely (issue 38363); REVOKE on a non-existent db errors.
#[test]
#[ignore = "go-parity-gap: REVOKE db-scope with row-deletion-on-last-privilege (pkg/executor/revoke.go:63, issue 38363) and mysql.DB are unported"]
fn revoke_db_scope_deletes_the_row_when_the_last_privilege_goes() {}

/// Go
/// `pkg/executor/revoke_test.go:85::TestRevokeDBScopeCaseInsensitiveWithNewCollationDisabled`
/// (uses `newCollationDisabledBootstrapTestKit`, pkg/executor/main_test.go):
/// with the new collation disabled, `REVOKE SELECT ON TEST.*` revokes the
/// `test.*` grant — the db-name match is case-insensitive and the
/// mysql.db row disappears.
#[test]
#[ignore = "go-parity-gap: needs the new-collation-disabled bootstrap mode and the db-scope REVOKE executor (pkg/executor/revoke.go:63); neither is ported"]
fn revoke_db_scope_matches_db_names_case_insensitively_without_new_collation() {}

/// Go `pkg/executor/revoke_test.go:97::TestRevokeTableScope`: `GRANT ALL ON
/// test.test1` records the full `Table_priv` set
/// (`Select,Insert,Update,Delete,Create,Drop,Index,Alter,Create View,Show
/// View,Trigger,References`) in mysql.tables_priv; each REVOKE removes its
/// member from the SET, and revoking the last privilege deletes the row
/// (issue 38421); REVOKE on a non-existent table errors.
#[test]
#[ignore = "go-parity-gap: table-scope SET membership editing in mysql.tables_priv (pkg/executor/revoke.go:63, SetFromString round-trip) is unported"]
fn revoke_table_scope_removes_set_members_then_deletes_the_row() {}

/// Go
/// `pkg/executor/revoke_test.go:138::TestRevokeTableScopeCaseInsensitiveWithNewCollationDisabled`:
/// three subtests (table-name, schema-name, missing-table fallback) pinning
/// that with the new collation disabled a REVOKE matches
/// tables_priv rows case-insensitively on BOTH name parts and still deletes
/// the row when the referenced table no longer exists.
#[test]
#[ignore = "go-parity-gap: needs the new-collation-disabled bootstrap mode and case-insensitive tables_priv matching in the REVOKE executor (pkg/executor/revoke.go:155); unported"]
fn revoke_table_scope_matches_names_case_insensitively_without_new_collation() {}

/// Go `pkg/executor/revoke_test.go:183::TestRevokeColumnScope`: GRANT/REVOKE
/// of each `mysql.AllColumnPrivs` on a column edits mysql.columns_priv's
/// `Column_priv` SET (grant appends the `Priv2SetStr` member, revoke removes
/// it, revoking the last privilege deletes the row); `GRANT ALL(c2)` seeds
/// every column privilege; the case-insensitive schema-name subtest pins
/// `REVOKE SELECT(id) ON TEST.…` matching the `test` row.
#[test]
#[ignore = "go-parity-gap: column-scope REVOKE over mysql.columns_priv SET values (pkg/executor/revoke.go:155 revokeOneUser) is unported"]
fn revoke_column_scope_edits_column_priv_sets_then_deletes_rows() {}
