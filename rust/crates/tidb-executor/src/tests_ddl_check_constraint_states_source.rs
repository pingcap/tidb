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

//! Port of Go `pkg/ddl/constraint_test.go` (all six `TestXxx` functions,
//! lines 32–219).
//!
//! All six run with `@@global.tidb_enable_check_constraint = 1` and pin how
//! an ADD/DROP/ALTER CHECK constraint behaves ACROSS the online DDL states:
//! a failpoint hook flips `TableCommon.Constraints` mid-job (or reads it at
//! `StateWriteOnly`/`StateWriteReorganization`/`StatePublic`) and interleaves
//! inserts that must fail with `[table:3819]Check constraint '<name>' is
//! violated.` at exactly the states where the constraint is already
//! enforced.
//!
//! This crate's `CREATE TABLE` path models only the variable-OFF behavior
//! (discard + warn, see `run_create_table_in`'s doc): the variable-ON path —
//! storing, printing, enforcing, and state-transitioning check constraints —
//! is refused, so every test here is an ignored parity gap. The session-tier
//! variable-OFF pins live in `tidb-session`'s `tests_check_constraints`.

/// `pkg/ddl/constraint_test.go::TestAlterConstraintAddDrop` (line 32): with
/// the variable ON, inserts against the CREATE-declared checks are enforced
/// (`(4, 3)` violates `a<b`), `ADD CONSTRAINT cc CHECK (b < 5)` verifies
/// existing rows in the job's write-only state (a violating insert from the
/// hook is what makes it fail with 3819), and `DROP CONSTRAINT cc` succeeds.
// go-parity-gap: stored/enforced CHECK constraints under
// tidb_enable_check_constraint=1 with state-interleaved verification are not
// modeled in this crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_constraint_add_drop() {}

/// `pkg/ddl/constraint_test.go::TestAlterAddConstraintStateChange` (line 66):
/// `ADD CONSTRAINT c0 CHECK (a > 10)` observed at StateWriteReorganization —
/// with `Constraints` temporarily emptied the hook's insert of `1` succeeds,
/// then the finished job prints `CONSTRAINT `c0` CHECK ((`a` > 10))` in
/// `SHOW CREATE TABLE` and the table holds `12` and `1`.
// go-parity-gap: mid-state constraint-metadata surgery is not modeled in
// this crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_add_constraint_state_change() {}

/// `pkg/ddl/constraint_test.go::TestAlterAddConstraintStateChange1` (line
/// 110): with `Constraints` emptied at StateWriteOnly, the hook's insert of
/// `1` survives and the ADD CONSTRAINT job FAILS with
/// `[ddl:3819]Check constraint 'c1' is violated.`, leaving no constraint in
/// `SHOW CREATE TABLE`.
// go-parity-gap: mid-state constraint-metadata surgery is not modeled in
// this crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_add_constraint_state_change1() {}

/// `pkg/ddl/constraint_test.go::TestAlterAddConstraintStateChange2` (line
/// 144): with the added constraint forced back to StateWriteOnly while the
/// job is at StateWriteReorganization, the hook's insert fails with
/// `[table:3819]` — enforcement follows the CONSTRAINT's state, not the
/// job's — and the job then completes and prints the constraint.
// go-parity-gap: per-state constraint enforcement is not modeled in this
// crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_add_constraint_state_change2() {}

/// `pkg/ddl/constraint_test.go::TestAlterAddConstraintStateChange3` (line
/// 176, issue #48123): with the constraint forced back to
/// StateWriteReorganization while the job is PUBLIC-and-done, the hook's
/// insert fails with `[table:3819]`, the job still lands, and
/// `SHOW CREATE TABLE` prints the constraint.
// go-parity-gap: per-state constraint enforcement is not modeled in this
// crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_add_constraint_state_change3() {}

/// `pkg/ddl/constraint_test.go::TestAlterEnforcedConstraintStateChange`
/// (line 219): a `NOT ENFORCED` check constraint accepts `12`; forcing it to
/// StateWriteOnly mid-`ALTER TABLE ... ALTER CONSTRAINT c1 ENFORCED` makes
/// the hook's insert of `1` fail with `[table:3819]`, and the job completes
/// with the table still holding only `12`.
// go-parity-gap: NOT ENFORCED/ENFORCED transitions are not modeled in this
// crate.
#[test]
#[ignore = "go-parity-gap: enforced CHECK constraint DDL is not modeled in this crate"]
fn alter_enforced_constraint_state_change() {}
