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

//! Gap test for Go `pkg/executor/select_test.go:38::TestImportIntoShouldHaveSameFlagsAsInsert`.
//! (`select_test.go:29::BenchmarkResetContextOfStmt` is a Go benchmark, not
//! a unit test — skipped-reason in the receipt, b004/b015/b121 precedent.)

/// Go `pkg/executor/select_test.go:38::TestImportIntoShouldHaveSameFlagsAsInsert`:
/// under each of 10 SQL modes (default, IGNORE_SPACE, STRICT_TRANS_TABLES,
/// STRICT_ALL_TABLES, ALLOW_INVALID_DATES, NO_ZERO_IN_DATE, NO_ZERO_DATE,
/// and three combinations), `executor.ResetContextOfStmt` must derive
/// IDENTICAL TypeCtx flags for a parsed `ast.InsertStmt` and a parsed
/// `ast.ImportIntoStmt` — i.e. IMPORT INTO statements flow through the same
/// SQL-flag derivation as INSERT. Needs `ResetContextOfStmt`
/// (pkg/executor/executor.go, statement-context construction from SQL mode)
/// and IMPORT INTO preprocessing; neither is ported.
#[test]
#[ignore = "go-parity-gap: ResetContextOfStmt SQL-flag derivation (pkg/executor/executor.go) for INSERT vs IMPORT INTO statements is unported; this tier has no statement-context flag surface"]
fn import_into_resolves_the_same_sql_flags_as_insert() {}
