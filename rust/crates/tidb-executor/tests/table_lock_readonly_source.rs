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

//! Ports of Go `pkg/ddl/table_modify_test.go` (master): `TestLockTableReadOnly`
//! (`:37`) and `TestConcurrentLockTables` (`:89`). Go exercises the
//! table-lock metadata (`alter table read only/write`, `lock tables`,
//! `admin cleanup table lock`) through two sessions against a schema-lease
//! mockstore; the table-lock state machine is not transcreated in this tier
//! (the AST shapes exist at `rust/crates/tidb-ast/src/stmt/ddl.rs:228
//! DdlStmt::LockTables`, but no executor consults lock state), so each test
//! is recorded as an explicit gap with the contract re-derived from the Go
//! source. Nothing is approximated.

/// Go `TestLockTableReadOnly` (`pkg/ddl/table_modify_test.go:37`): after
/// `alter table t1 read only`, SELECTs from ANY session succeed while
/// insert/update/delete fail with `infoschema.ErrTableLocked` from both
/// sessions; a second `alter table t1 read only` is idempotent; `alter table
/// t1 read write` releases it; while `lock tables t1 read/write/write local`
/// is held, `alter table t1 read only` fails with ErrTableLocked (both
/// directions, both sessions); and `admin cleanup table lock t1` clears a
/// stranded read-only mark so writes succeed again.
// go-parity-gap: no table-lock state machine (no carrier consults
// TableLockInfo; `alter table read only` and `lock tables` are refused as
// unsupported by the DDL dispatch).
#[test]
#[ignore]
fn read_only_table_lock_gates_writes_across_sessions() {
}

/// Go `TestConcurrentLockTables` (`pkg/ddl/table_modify_test.go:89`): two
/// sessions racing `lock tables t1 read` BOTH succeed; racing `lock tables
/// t1 write` yields exactly one success and one `infoschema.ErrTableLocked`
/// loser; racing `lock tables t1 write local` behaves like plain write; and
/// `unlock tables` clears each winner's state.
// go-parity-gap: same missing table-lock state machine.
#[test]
#[ignore]
fn concurrent_lock_tables_have_read_shared_and_write_exclusive_outcomes() {
}
