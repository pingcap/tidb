// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Ports of `pkg/ddl/constraint_test.go` (origin/master): the CHECK
//! constraint DDL state machine under `@@global.tidb_enable_check_constraint = 1`.
//!
//! Every test in the Go file drives a live DDL job through its intermediate
//! `model.SchemaState`s with the `afterWaitSchemaSynced` failpoint hook and
//! flips `tables.TableCommon.Constraints` mid-state to observe which states
//! enforce a check. This tier implements only Go's
//! `tidb_enable_check_constraint = OFF` model — `run_create_table_in` refuses
//! to build CHECK constraints when the variable is ON, ADD/ALTER CHECK are
//! discarded no-ops and DROP CHECK is 3940 (`crate::ddl`'s doc), and no
//! constraint is ever stored or enforced — and it has no DDL job queue or
//! schema states to hook. Each Go test is therefore recorded as an
//! `#[ignore]`d gap carrying its re-derived contract, with the Go symbol
//! locations cited.

use crate::driver::DEFAULT_DATABASE;
use crate::{Catalog, StmtContext, run_alter_table_in};

/// Runs the Go fixture's DDL against this tier to demonstrate the ON-model
/// refusal the gaps below hang on. Kept RUNNING (not ignored) so the boundary
/// the six gaps sit behind is itself regression-pinned: with the check
/// constraint enabled, `create table t (a int check(a>1), ..)` is refused
/// rather than silently storing nothing.
///
/// Derivation: `run_create_table_in` (`ddl.rs:862`) errors with
/// "CHECK constraints are only modelled with tidb_enable_check_constraint
/// off" whenever `enable_check_constraint` is set and the statement carries a
/// check; Go's `constraint_test.go:36` sets the variable to 1 before every
/// fixture.
#[test]
fn check_constraint_on_model_is_refused_not_silently_dropped() {
    let mut catalog = Catalog::default();
    let sql = "create table t (a int check(a>1), b int, constraint a_b check(a<b))";
    let error = crate::run_create_table_in(
        sql,
        &mut catalog,
        DEFAULT_DATABASE,
        crate::CreateTableSettings {
            enable_check_constraint: true,
            ..Default::default()
        },
        &StmtContext::for_query(),
    )
    .expect_err("Go's fixtures run with tidb_enable_check_constraint = 1");
    let message = error.to_string();
    assert!(
        message.contains("tidb_enable_check_constraint"),
        "the refusal names the variable, got: {message}"
    );
    let _ = run_alter_table_in; // part of this module's subject surface
}

/// `constraint_test.go:32::TestAlterConstraintAddDrop`.
#[test]
#[ignore = "go-parity-gap: CHECK enforcement (insert 4,3 rejected) plus a mid-state insert inside the afterWaitSchemaSynced hook need the check-constraint ON model and the DDL job state machine (Go constraint_test.go:32-63)"]
fn alter_constraint_add_drop_enforces_between_states() {
    // Derivation: create t (a int check(a>1), b int, constraint a_b
    // check(a<b)); inserts (2,3) and (3,4) succeed, (4,3) fails [table:3819].
    // During `alter table t add constraint cc check (b < 5)` the hook inserts
    // (5,6) at job.SchemaState == StateWriteOnly; the ALTER then fails with
    // 3819 "Check constraint 'cc' is violated" (verify-remain-data rejects
    // the violating row written mid-state), as does the DROP that follows.
}

/// `constraint_test.go:66::TestAlterAddConstraintStateChange` — StatNone ->
/// StateWriteReorganization, the success path.
#[test]
#[ignore = "go-parity-gap: needs the mockVerifyRemainDataSuccess failpoint, TableCommon.Constraints state surgery and the DDL job machine (Go constraint_test.go:66-107)"]
fn alter_add_constraint_state_change_write_reorg_passes_verification() {
    // Derivation: t(a int) with row 12; during the ADD CONSTRAINT c0 CHECK
    // (a > 10) job's StateWriteReorganization the hook reads
    // `select count(1) from test.t where not a > 10 limit 1` -> 0, clears
    // TableCommon.Constraints, inserts 1 (unconstrained), restores. With
    // mockVerifyRemainDataSuccess returning true the ALTER succeeds; `select
    // * from t` -> "12", "1"; SHOW CREATE TABLE carries
    // `CONSTRAINT `c0` CHECK ((`a` > 10))`; DROP CONSTRAINT c0 succeeds.
}

/// `constraint_test.go:110::TestAlterAddConstraintStateChange1` —
/// StatNone -> StateWriteOnly: the violating insert lands while
/// unconstrained, so the ADD must FAIL and roll back cleanly.
#[test]
#[ignore = "go-parity-gap: mid-job hook + TableCommon.Constraints manipulation; the OFF model this tier implements stores no constraint to fail (Go constraint_test.go:110-141)"]
fn alter_add_constraint_state_change1_fails_on_a_write_only_violation() {
    // Derivation: hook at StateWriteOnly clears the table's constraints and
    // inserts 1 into t (12 present). `alter table t add constraint c1 check
    // (a > 10)` must fail [ddl:3819] "Check constraint 'c1' is violated",
    // `select * from t` -> "12", "1", SHOW CREATE TABLE has NO c1 constraint,
    // and the cleanup `delete from t where a = 1` succeeds.
}

/// `constraint_test.go:144::TestAlterAddConstraintStateChange2` —
/// StateWriteOnly -> StateWriteReorganization: the constraint already
/// ENFORCES during reorganization.
#[test]
#[ignore = "go-parity-gap: needs the job's StateWriteReorganization boundary and per-constraint State mutation on TableCommon (Go constraint_test.go:144-173)"]
fn alter_add_constraint_state_change2_enforces_from_write_reorg_on() {
    // Derivation: hook sets Constraints[0].State = StateWriteOnly during the
    // StateWriteReorganization visit, so `insert into t values(1)` fails
    // [table:3819] naming 'c2'; the ADD succeeds, SHOW CREATE TABLE carries
    // CONSTRAINT c2, and DROP CONSTRAINT c2 succeeds.
}

/// `constraint_test.go:176::TestAlterAddConstraintStateChange3` —
/// StateWriteReorganization -> StatePublic (issue TiDB#48123).
#[test]
#[ignore = "go-parity-gap: pins the job-completion boundary (job.IsDone at StatePublic) with the afterWaitSchemaSynced hook; no job machine here (Go constraint_test.go:176-216)"]
fn alter_add_constraint_state_change3_pins_the_public_boundary() {
    // Derivation: at StatePublic && job.IsDone() for ActionAddCheckConstraint
    // on t, the hook demotes Constraints[0].State to StateWriteReorganization
    // so `insert into t values(1)` fails [table:3819] naming 'c3', then
    // restores. `alter table t add constraint c3 check (a > 10)` succeeds;
    // rows stay "12"; SHOW CREATE TABLE carries CONSTRAINT c3.
}

/// `constraint_test.go:219::TestAlterEnforcedConstraintStateChange` —
/// ALTER CONSTRAINT ... ENFORCED makes a NOT ENFORCED constraint bite.
#[test]
#[ignore = "go-parity-gap: NOT ENFORCED/ENFORCED metadata plus its state machine are outside this tier; enforcement itself needs the ON model (Go constraint_test.go:219-246)"]
fn alter_enforced_constraint_state_change_enforces_on_enforced() {
    // Derivation: t (a int, constraint c1 check (a > 10) not enforced) holds
    // 12. During ALTER CONSTRAINT c1 ENFORCED's StateWriteReorganization the
    // hook sets Constraints[0].State = StateWriteOnly, so `insert into t
    // values(1)` fails [table:3819]; the ALTER succeeds and rows stay "12".
}
