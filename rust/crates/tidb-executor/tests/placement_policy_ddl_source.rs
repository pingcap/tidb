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

//! Ports of Go `pkg/ddl/placement_policy_test.go` (part of the pkg/ddl batch
//! whose executor-side carriers live here: `run_create_placement_policy` /
//! `run_alter_placement_policy` / `run_drop_placement_policy`
//! (`src/ddl/placement_policy.rs`, mirroring `pkg/ddl/executor.go:6802`,
//! `:6871` and the policy builder `pkg/ddl/placement_policy.go:509`), plus
//! `Catalog` policy storage (`src/driver/catalog.rs:599-690`, mirroring
//! `CreatePlacementPolicyWithInfo` at `pkg/ddl/executor.go:1336`).
//!
//! Go drives these tests through a full mockstore + PD stack, so the parts
//! whose assertions live only in PD rule bundles, `SHOW PLACEMENT`,
//! `information_schema` or GC worker state are recorded as `#[ignore]` gap
//! tests with the contract re-derived from the Go source. Nothing here is
//! approximated: a row the Rust side cannot execute is never rewritten into
//! one it can.

use tidb_executor::{
    run_create_table_on, run_drop_table_in, run_truncate_table_in, Catalog, DriverError,
    StmtContext, TableEntry,
};

/// Parses and runs one CREATE/ALTER/DROP PLACEMENT POLICY statement against
/// `catalog`, standing in for Go's testkit session with a stock strict
/// context (the same shape `run_create_table_on` documents for its callers).
fn policy_ddl(sql: &str, catalog: &mut Catalog) -> Result<(), DriverError> {
    let statement = tidb_parser::parse(sql).expect("placement policy statement parses");
    let tidb_ast::Stmt::Ddl(payload) = statement else {
        panic!("expected a DDL envelope for {sql}")
    };
    let context = StmtContext::default().with_strict(true);
    match &*payload {
        tidb_ast::DdlStmt::CreatePlacementPolicy(create) => {
            tidb_executor::run_create_placement_policy(catalog, create, &context)
        }
        tidb_ast::DdlStmt::AlterPlacementPolicy(alter) => {
            tidb_executor::run_alter_placement_policy(catalog, alter, &context)
        }
        tidb_ast::DdlStmt::DropPlacementPolicy(drop) => {
            tidb_executor::run_drop_placement_policy(catalog, drop, &context)
        }
        other => panic!("unexpected DDL payload for {sql}: {other:?}"),
    }
}

/// The `(id, followers, primary_region, regions, schedule)` a policy must
/// carry after a create/alter, checked against Go's `checkFunc` expectations.
fn policy_summary(catalog: &Catalog, name: &str) -> (i64, u64, String, String, String) {
    let policy = catalog
        .policy(name)
        .unwrap_or_else(|| panic!("policy {name} must exist"));
    let settings = policy
        .placement_settings
        .as_ref()
        .expect("policy stores settings")
        .read()
        .clone();
    (
        policy.id,
        settings.followers,
        settings.primary_region,
        settings.regions,
        settings.schedule,
    )
}

fn stored_table<'a>(catalog: &'a Catalog, name: &str) -> &'a tidb_executor::KvTable {
    match catalog.get_table_for_test(name) {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("{name} is not a storage-backed table"),
    }
}

fn drop_table(catalog: &mut Catalog, sql: &str) {
    run_drop_table_in(
        sql,
        catalog,
        "test",
        tidb_parser::SqlMode::default(),
        true,
    )
    .expect("drop table succeeds");
}

/// Go `TestPlacementValidation` (`pkg/ddl/placement_policy_test.go:400`),
/// success rows 1 and 3: a dict-shaped common CONSTRAINTS string is stored
/// verbatim (Go never parses it at DDL time; PD does, `pkg/ddl/placement/
/// errors.go:31`), and a plain PRIMARY_REGION/REGIONS policy round trips.
/// The alter half re-applies each success case over a base policy, which Go
/// restores between rows (`placement_policy_test.go:446-455`).
#[test]
fn placement_validation_success_rows_create_and_alter_settings() {
    let mut catalog = Catalog::default();

    // Row 1: "Dict is not allowed for common constraint" -- in Go this is a
    // SUCCESS row (the dict is only rejected by PD's bundle builder, not by
    // the DDL layer), so the settings must be stored as written.
    policy_ddl(
        "create placement policy x LEARNERS=1 \
         LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" \
         CONSTRAINTS=\"{'+disk=ssd':2}\"",
        &mut catalog,
    )
    .expect("dict constraints are a valid create");
    let (id, followers, primary_region, regions, schedule) = policy_summary(&catalog, "x");
    assert!(id != 0, "create assigns a policy id");
    assert_eq!(followers, 0);
    assert_eq!(primary_region, "");
    assert_eq!(regions, "");
    assert_eq!(schedule, "");
    let policy = catalog.policy("x").unwrap();
    let settings = policy
        .placement_settings
        .as_ref()
        .unwrap()
        .read()
        .clone();
    assert_eq!(settings.learners, 1);
    assert_eq!(settings.learner_constraints, "[+zone=cn-west-1]");
    assert_eq!(settings.constraints, "{'+disk=ssd':2}");
    policy_ddl("drop placement policy if exists x", &mut catalog).unwrap();

    // Row 3: plain region set, created and then re-applied through ALTER
    // (`alter placement policy x ...` with the same options).
    policy_ddl(
        "create placement policy x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\"",
        &mut catalog,
    )
    .unwrap();
    let (_, _, primary_region, regions, _) = policy_summary(&catalog, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1,cn-east-2");

    // Go's alter loop (placement_policy_test.go:445-455) rebuilds the
    // settings FROM THE ALTER's OPTIONS ONLY: fields the alter does not name
    // fall back to zero values (`buildPolicyInfo`,
    // pkg/ddl/placement_policy.go:509), which is what makes row 1 alter
    // cleanly over the region-set base here.
    policy_ddl(
        "alter placement policy x LEARNERS=1 \
         LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" \
         CONSTRAINTS=\"{'+disk=ssd':2}\"",
        &mut catalog,
    )
    .expect("dict constraints are a valid alter");
    let (_, followers, primary_region, regions, _) = policy_summary(&catalog, "x");
    assert_eq!(followers, 0);
    assert_eq!(primary_region, "", "alter resets unnamed fields");
    assert_eq!(regions, "", "alter resets unnamed fields");

    // A FAILED alter must not half-apply. Go pins this by checking
    // `SHOW PLACEMENT` after each error row (placement_policy_test.go:457);
    // the erroring row itself is the gap test below, so here the invariant
    // is pinned through an alter of a MISSING policy instead, which also
    // must leave the stored settings untouched.
    let before = policy_summary(&catalog, "x");
    let error = policy_ddl("alter placement policy x2 REGIONS=\"bj,sh\"", &mut catalog)
        .expect_err("alter of a missing policy reports 8239");
    assert!(matches!(&error, DriverError::PlacementPolicyNotExists(name) if name == "x2"));
    assert_eq!(policy_summary(&catalog, "x"), before);
}

/// Go `TestPlacementValidation` row 2 (`pkg/ddl/placement_policy_test.go:419-
/// 428`): `LEARNER_CONSTRAINTS="[+zone=cn-west-1, +zone=cn-west-2]"` conflicts
/// with itself, and Go reports the full PD constraint-builder message
/// ("invalid label constraints format: should be [constraint1, ...] (error
/// conflicting label constraints: '+zone=cn-west-2' and '+zone=cn-west-1'),
/// ... : invalid LearnerConstraints"), assembled by the placement option
/// checker the DDL layer calls before writing a policy.
// go-parity-gap: the Rust DDL layer stores constraint strings verbatim and
// has no carrier of Go's constraint self-conflict checker
// (pkg/ddl/placement/constraints.go), so the row cannot be executed.
#[test]
#[ignore]
fn placement_validation_conflicting_learner_constraints_reports_go_message() {
}

/// Go `TestResetSchemaPlacement` parser half
/// (`pkg/ddl/placement_policy_test.go:469`): the BARE word `default` is a
/// reserved name in the policy position, so the statement never parses
/// (Go: ErrParse 1064; the bare word is not an identifier-like policy name).
#[test]
fn reset_schema_placement_bare_default_policy_name_is_a_parse_error() {
    assert!(
        tidb_parser::parse("create placement policy default followers=4").is_err(),
        "a bare `default` policy name must not parse"
    );
}

/// Go `TestResetSchemaPlacement` first row
/// (`pkg/ddl/placement_policy_test.go:468`): even QUOTED, `default` is
/// refused by the executor before any storage happens
/// (`pkg/ddl/executor.go:1344-1346` compares `policy.Name.L` against
/// `defaultPlacementPolicyName` and reports ErrReservedSyntax 1382, "The
/// 'default' syntax is reserved for purposes internal to the MySQL server").
// go-parity-gap: run_create_placement_policy has no reserved-name guard, so
// Rust creates a policy literally named `default` where Go refuses with 1382
// (divergence verified this session: the create succeeds against the
// executor carrier).
#[test]
#[ignore]
fn reset_schema_placement_reserved_default_policy_name_reports_1382() {
}

/// Go `TestResetSchemaPlacement` alter half
/// (`pkg/ddl/placement_policy_test.go:471-506`): a database created
/// `PLACEMENT POLICY \`TestReset\`` shows the policy in `SHOW CREATE
/// DATABASE`, and each of `PLACEMENT POLICY=default`, `SET DEFAULT`,
/// `= 'DEFAULT'` and `` = `DEFAULT` `` clears the reference (the four
/// spellings are folded to the default reset by
/// `pkg/ddl/executor.go:622`'s `defaultPlacementPolicyName` comparison), so
/// the show text loses its `/*T![placement]*/` suffix.
// go-parity-gap: the Catalog has no database-level placement reference and
// no SHOW CREATE DATABASE text carrier, so the reset cycle cannot execute.
#[test]
#[ignore]
fn reset_schema_placement_alter_database_reset_cycles_clear_the_ref() {
}

/// Go `TestCreateOrReplacePlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:517-543`): CREATE OR REPLACE on a
/// missing name behaves like CREATE, on an existing name behaves like ALTER
/// (same policy object, new settings), and OR REPLACE together with
/// IF NOT EXISTS is ErrWrongUsage 1221 "Incorrect usage of OR REPLACE and
/// IF NOT EXISTS" (Go reports it before anything else; the Rust carrier
/// checks the same pairing first, `src/ddl/placement_policy.rs:88`).
#[test]
fn create_or_replace_placement_policy_matches_create_then_alter_and_refuses_wrong_usage() {
    let mut catalog = Catalog::default();

    // Missing name: plain create.
    policy_ddl(
        "create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"",
        &mut catalog,
    )
    .expect("or-replace over a missing policy creates it");
    let (_, _, primary_region, regions, _) = policy_summary(&catalog, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1,cn-east");

    // Go next creates `tp` referencing x (table + partition p0) and pins the
    // PD bundles stay after the replace; the bundle half is a gap, but the
    // REFERENCE object must survive the replace with its id intact, which is
    // what Go's OnExistReplace path preserves (`pkg/ddl/executor.go:1336`
    // keeps the policy row and only swaps settings).
    run_create_table_on(
        "CREATE TABLE tp(id int) placement policy x PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY x, \
         PARTITION p1 VALUES LESS THAN (1000))",
        &mut catalog,
    )
    .unwrap();
    let reference_before = stored_table(&catalog, "tp")
        .placement_policy()
        .cloned()
        .expect("table carries a policy reference");

    // Existing name: replace = alter of the stored settings.
    policy_ddl(
        "create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1\"",
        &mut catalog,
    )
    .expect("or-replace over an existing policy replaces the settings");
    let (id_after, _, primary_region, regions, _) = policy_summary(&catalog, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1");
    let reference_after = stored_table(&catalog, "tp")
        .placement_policy()
        .cloned()
        .expect("reference survives");
    assert_eq!(
        reference_before.id, reference_after.id,
        "replace keeps the policy id so references stay resolvable"
    );
    assert_eq!(reference_after.id, id_after);

    // OR REPLACE + IF NOT EXISTS: ErrWrongUsage 1221.
    let error = policy_ddl(
        "create or replace placement policy if not exists x primary_region=\"cn-east-1\" regions=\"cn-east-1\"",
        &mut catalog,
    )
    .expect_err("the two clauses are incompatible");
    let wire = error.to_mysql_error();
    assert_eq!(wire.code, 1221);
    assert_eq!(
        wire.message,
        "Incorrect usage of OR REPLACE and IF NOT EXISTS"
    );
}

/// Go `TestAlterPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:545-614`): each normal alter rebuilds
/// the stored settings from the alter's own options only
/// (`buildPolicyInfo`, `pkg/ddl/placement_policy.go:509`), and an alter of a
/// missing policy (after the table and policy are dropped) reports
/// ErrPlacementPolicyNotExists 8239 for both `x` and a name that never
/// existed (`placement_policy_test.go:604-606`). The PD bundle re-publishing
/// the Go test asserts alongside (`checkExistTableBundlesInPD`) is the gap.
#[test]
fn alter_placement_policy_replaces_settings_and_unknown_policies_report_8239() {
    let mut catalog = Catalog::default();
    policy_ddl(
        "create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"",
        &mut catalog,
    )
    .unwrap();

    // Normal case 1: PRIMARY_REGION/REGIONS replaced. Go's golden
    // information_schema row for this alter ends in `2 0` because the
    // PLACEMENT_POLICIES reader DISPLAYS a default of 2 for an unset
    // FOLLOWERS (`pkg/executor/infoschema_reader.go:3855-3858`); the STORED
    // settings are rebuilt from the alter's options only
    // (`buildPolicyInfo`, pkg/ddl/placement_policy.go:509), so the stored
    // followers stay 0, which is what the carrier holds.
    policy_ddl(
        "alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\"",
        &mut catalog,
    )
    .unwrap();
    let (_, followers, primary_region, regions, schedule) = policy_summary(&catalog, "x");
    assert_eq!(primary_region, "bj");
    assert_eq!(regions, "bj,sh");
    assert_eq!(followers, 0, "stored followers reset (display default 2 is information_schema-only)");
    assert_eq!(schedule, "");

    // Normal case 2: adds SCHEDULE=EVEN.
    policy_ddl(
        "alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj\" SCHEDULE=\"EVEN\"",
        &mut catalog,
    )
    .unwrap();
    let (_, _, primary_region, regions, schedule) = policy_summary(&catalog, "x");
    assert_eq!(primary_region, "bj");
    assert_eq!(regions, "bj");
    assert_eq!(schedule, "EVEN");

    // Normal case 3: leader/follower constraint pair with FOLLOWERS=3.
    policy_ddl(
        "alter placement policy x \
         LEADER_CONSTRAINTS=\"[+region=us-east-1]\" \
         FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" \
         FOLLOWERS=3",
        &mut catalog,
    )
    .unwrap();
    let policy = catalog.policy("x").unwrap();
    let settings = policy.placement_settings.as_ref().unwrap().read();
    assert_eq!(settings.leader_constraints, "[+region=us-east-1]");
    assert_eq!(settings.follower_constraints, "[+region=us-east-2]");
    assert_eq!(settings.followers, 3);
    assert_eq!(settings.primary_region, "", "earlier fields are reset");
    assert_eq!(settings.regions, "");
    drop(settings);

    // Normal case 4: voter/learner constraints + CONSTRAINTS + VOTERS +
    // LEARNERS. Note the Go SQL relies on the grammar gluing the last
    // option of the previous line to the next word
    // (CONSTRAINTS="[+disk=ssd]" VOTERS=5): written out here as separate
    // options with the same meaning.
    policy_ddl(
        "alter placement policy x \
         VOTER_CONSTRAINTS=\"[+region=bj]\" \
         LEARNER_CONSTRAINTS=\"[+region=sh]\" \
         CONSTRAINTS=\"[+disk=ssd]\" \
         VOTERS=5 \
         LEARNERS=3",
        &mut catalog,
    )
    .unwrap();
    let policy = catalog.policy("x").unwrap();
    let settings = policy.placement_settings.as_ref().unwrap().read();
    assert_eq!(settings.voter_constraints, "[+region=bj]");
    assert_eq!(settings.learner_constraints, "[+region=sh]");
    assert_eq!(settings.constraints, "[+disk=ssd]");
    assert_eq!(settings.voters, 5);
    assert_eq!(settings.learners, 3);
    assert_eq!(settings.followers, 0, "followers from case 3 are reset");
    assert_eq!(settings.schedule, "", "schedule from case 2 is reset");
    drop(settings);

    // Missing policies report 8239 (after Go drops the table and the
    // policy; the drop is covered by the dependency test below, so here the
    // policy is dropped directly).
    policy_ddl("drop placement policy x", &mut catalog).unwrap();
    for name in ["x", "x2"] {
        let error = policy_ddl(&format!("alter placement policy {name} REGIONS=\"bj,sh\""), &mut catalog)
            .expect_err("alter of a missing policy reports 8239");
        assert!(matches!(
            &error,
            DriverError::PlacementPolicyNotExists(missing) if missing == name
        ));
        assert_eq!(
            error.to_mysql_error().code, 8239,
            "Go mysql.ErrPlacementPolicyNotExists"
        );
    }
    assert!(catalog.policy("x").is_none());
}

/// Go `TestCreateTableWithPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:616-708`): only a PLACEMENT POLICY
/// option resolves against existing policies (8239 for an unknown name),
/// the table's reference carries the policy's ID and lowercased name, and a
/// partition's own option is stored on that partition alone -- the OTHER
/// partitions of the same table keep a nil reference, exactly as Go's
/// `setPartitionPlacementFromOptions` leaves them
/// (`pkg/ddl/partition.go`; see the `PartitionDef::placement_policy` doc in
/// `src/partition_routing.rs:191`).
#[test]
fn create_table_with_placement_policy_resolves_table_and_partition_refs() {
    let mut catalog = Catalog::default();

    // Only placement policy checks existence: an unknown name is 8239 even
    // though no policy exists at all yet (Go placement_policy_test.go:624).
    let error = run_create_table_on("create table t(a int) PLACEMENT POLICY=\"x\"", &mut catalog)
        .expect_err("unknown policy is refused");
    assert_eq!(error.to_mysql_error().code, 8239);

    policy_ddl(
        "create placement policy x FOLLOWERS=2 CONSTRAINTS=\"[+disk=ssd]\"",
        &mut catalog,
    )
    .unwrap();
    policy_ddl(
        "create placement policy z FOLLOWERS=1 SURVIVAL_PREFERENCES=\"[region, zone]\"",
        &mut catalog,
    )
    .unwrap();
    policy_ddl(
        "create placement policy y FOLLOWERS=3 CONSTRAINTS=\"[+region=bj]\"",
        &mut catalog,
    )
    .unwrap();
    let x_id = catalog.policy("x").unwrap().id;
    let y_id = catalog.policy("y").unwrap().id;
    assert!(x_id != 0 && y_id != 0);

    // Plain table: reference = (policy id, policy name).
    run_create_table_on("create table t(a int) PLACEMENT POLICY=\"x\"", &mut catalog).unwrap();
    let reference = stored_table(&catalog, "t")
        .placement_policy()
        .cloned()
        .expect("t carries a reference");
    assert_eq!(reference.id, x_id);
    assert_eq!(reference.name.lowercase(), "x");
    drop_table(&mut catalog, "drop table if exists t");

    // Plain table against z pins the survival preferences storage.
    run_create_table_on("create table tt(a int) PLACEMENT POLICY=\"z\"", &mut catalog).unwrap();
    let reference = stored_table(&catalog, "tt")
        .placement_policy()
        .cloned()
        .expect("tt carries a reference");
    assert_eq!(reference.name.lowercase(), "z");
    let z_settings = catalog
        .policy("z")
        .unwrap()
        .placement_settings
        .as_ref()
        .unwrap()
        .read()
        .clone();
    assert_eq!(z_settings.survival_preferences, "[region, zone]");
    assert_eq!(z_settings.followers, 1);

    // Range-partitioned: p1's own option is stored, p0/p2 stay nil, and the
    // ids are stamped from the named policies.
    run_create_table_on(
        "create table t_range_p(id int) placement policy x partition by range(id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000) placement policy y, \
         PARTITION p2 VALUES LESS THAN (10000))",
        &mut catalog,
    )
    .unwrap();
    let table = stored_table(&catalog, "t_range_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert_eq!(definitions.len(), 3);
    assert!(definitions[0].placement_policy.is_none(), "p0 inherits nothing at DDL time");
    let p1 = definitions[1].placement_policy.as_ref().unwrap();
    assert_eq!(p1.id, y_id);
    assert_eq!(p1.name.lowercase(), "y");
    assert!(definitions[2].placement_policy.is_none());
    drop_table(&mut catalog, "drop table if exists t_range_p");

    // List-columns partitioned: same shape as Go's t_list_p.
    run_create_table_on(
        "create table t_list_p(name varchar(10)) placement policy x partition by list columns(name) (\
         PARTITION p0 VALUES IN ('a', 'b'), \
         PARTITION p1 VALUES IN ('c', 'd') placement policy y, \
         PARTITION p2 VALUES IN ('e', 'f'))",
        &mut catalog,
    )
    .unwrap();
    let table = stored_table(&catalog, "t_list_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert!(definitions[0].placement_policy.is_none());
    assert_eq!(definitions[1].placement_policy.as_ref().unwrap().id, y_id);
    assert!(definitions[2].placement_policy.is_none());
    drop_table(&mut catalog, "drop table if exists t_list_p");

    // Hash-partitioned with PARTITIONS 4: every generated definition keeps
    // a nil reference while the TABLE still points at x.
    run_create_table_on(
        "create table t_hash_p(id int) placement policy x partition by HASH(id) PARTITIONS 4",
        &mut catalog,
    )
    .unwrap();
    let table = stored_table(&catalog, "t_hash_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert_eq!(definitions.len(), 4);
    assert!(
        definitions.iter().all(|definition| definition.placement_policy.is_none()),
        "generated hash partitions carry no policy reference of their own"
    );
}

/// Go `TestCreateTableWithPlacementPolicy` first row
/// (`pkg/ddl/placement_policy_test.go:623-637`): a DIRECT placement option
/// set whose FOLLOWER_CONSTRAINTS conflicts with the common CONSTRAINTS
/// (`[+zone=cn-east-1]` vs `[-zone=cn-east-1]`) is refused at policy-creation
/// time with Go's "conflicting label constraints" message, while the same
/// text is accepted when written as a PLACEMENT POLICY's own constraint
/// dict (the sibling success row above).
// go-parity-gap: no Rust carrier of the placement constraint conflict
// checker (pkg/ddl/placement/constraints.go), so the pn create cannot be
// replayed; nothing is approximated.
#[test]
#[ignore]
fn create_table_placement_conflicting_constraints_report_conflict() {
}

/// Go `TestCreateTableWithInfoPlacement`
/// (`pkg/ddl/placement_policy_test.go:756-805`): CreateTableWithInfo keeps a
/// reference whose NAME is stale after `drop placement policy p1` +
/// `create placement policy p1 followers=2` and re-points it at the NEW
/// policy (new id) when the table is created in another database; a
/// reference naming a policy that does not exist is refused with
/// "[schema:8239]Unknown placement policy 'pxx'".
// go-parity-gap: the executor has no CreateTableWithInfo entry point (only
// SQL-text creates), so the stale-ref repoint cannot be driven; contract
// re-derived from the Go test body.
#[test]
#[ignore]
fn create_table_with_info_placement_repoints_stale_refs_to_the_new_policy() {
}

/// Go `TestCreateSchemaWithInfoPlacement`
/// (`pkg/ddl/placement_policy_test.go:807-853`): same re-pointing contract
/// for DATABASES created from info -- a stale database placement ref
/// resolves against the current policy of that name, the id is refreshed,
/// and an unknown ref name is 8239.
// go-parity-gap: no database-level placement reference in the Catalog and
// no CreateSchemaWithInfo entry point.
#[test]
#[ignore]
fn create_schema_with_info_placement_repoints_stale_refs_to_the_new_policy() {
}

/// Go `TestAlterRangePlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:855-901`): `ALTER RANGE global|meta
/// PLACEMENT POLICY p` builds the TiDB_GLOBAL / TiDB_META rule bundles
/// whose location labels follow the policy's survival preferences
/// (issue #51712), and the policy is refused by DROP while a range points
/// at it (8241), droppable again after both ranges reset to default
/// (issue #52257's fix).
// go-parity-gap: no ALTER RANGE statement carrier and no rule-bundle store;
// the range bundle builder (pkg/ddl/placement/bundle.go RebuildForRange)
// is not transcreated.
#[test]
#[ignore]
fn alter_range_placement_policy_binds_global_and_meta_ranges_to_the_policy() {
}

/// Go `TestDropPlacementPolicyInUse` (`pkg/ddl/placement_policy_test.go:902-
/// 951`), table halves: a policy referenced by ANY table -- across
/// databases -- cannot be dropped: plain DROP reports
/// "[ddl:8241]Placement policy '<name>' is still in use" and DROP IF EXISTS
/// does NOT suppress it (the policy does exist, so IF EXISTS is not the
/// applicable condition; Go's `CheckPlacementPolicyNotInUseFromInfoSchema`
/// runs after the existence check, `pkg/ddl/executor.go:6829` onward).
#[test]
fn drop_placement_policy_in_use_reports_8241_even_under_if_exists() {
    let mut catalog = Catalog::default();
    catalog.create_database("test2");

    for name in ["p1", "p2", "p3"] {
        policy_ddl(
            "create placement policy p_dummy \
             PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" SCHEDULE=\"EVEN\""
                .replace("p_dummy", name)
                .as_str(),
            &mut catalog,
        )
        .unwrap();
    }
    // p1 is used by test.t11 AND test2.t21 (cross-database dependency).
    run_create_table_on("create table test.t11 (id int) placement policy 'p1'", &mut catalog).unwrap();
    run_create_table_in_db("test2", "create table test2.t21 (id int) placement policy 'p1'", &mut catalog);
    // p2 is used by test.t12.
    run_create_table_on("create table test.t12 (id int) placement policy 'p2'", &mut catalog).unwrap();
    // p3 is used by test.t21 (Go's names are intentionally crosswired).
    run_create_table_on("create table test.t21 (id int) placement policy 'p3'", &mut catalog).unwrap();

    for name in ["p1", "p2", "p3"] {
        let error = policy_ddl(&format!("drop placement policy {name}"), &mut catalog)
            .expect_err("a referenced policy cannot be dropped");
        let wire = error.to_mysql_error();
        assert_eq!(wire.code, 8241, "Go mysql.ErrPlacementPolicyInUse");
        assert_eq!(wire.message, format!("Placement policy '{name}' is still in use"));

        // IF EXISTS does not turn an in-use drop into a note.
        let error = policy_ddl(&format!("drop placement policy if exists {name}"), &mut catalog)
            .expect_err("in-use survives IF EXISTS");
        assert_eq!(error.to_mysql_error().code, 8241);
    }

    // Dropping the referencing tables frees every policy.
    drop_table(&mut catalog, "drop table if exists test.t11");
    drop_table(&mut catalog, "drop table if exists test2.t21");
    drop_table(&mut catalog, "drop table if exists test.t12");
    drop_table(&mut catalog, "drop table if exists test.t21");
    for name in ["p1", "p2", "p3"] {
        policy_ddl(&format!("drop placement policy {name}"), &mut catalog)
            .unwrap_or_else(|error| panic!("{name} should be droppable now: {error:?}"));
    }
}

/// Go `TestDropPlacementPolicyInUse` database half
/// (`pkg/ddl/placement_policy_test.go:945-947`): a policy referenced by a
/// DATABASE (`create database test_p placement policy 'p4'`) is equally
/// in-use (8241).
// go-parity-gap: the Catalog records no database-level placement reference
// (create_database carries only a charset), so the p4 row cannot execute.
#[test]
#[ignore]
fn drop_placement_policy_in_use_by_database_reports_8241() {
}

/// Go `TestPolicyCacheAndPolicyDependency`
/// (`pkg/ddl/placement_policy_test.go:1006-1072`): referencing tables point
/// at the policy OBJECT, not a copy -- an ALTER of the policy is visible
// through every reference while their stored reference (id+name) stays
/// put; DROP of the policy is refused (8241) while any dependent table
/// exists and only succeeds after the last one goes; afterwards the policy
/// is gone from the catalog. Go's per-table dependency list comes from the
// meta KV (`testGetPolicyDependency`); here the catalog's own reference
// tracking stands in for it, which is the same information
// `policy_in_use` answers.
#[test]
fn policy_cache_and_dependency_alter_seen_by_tables_drop_blocked_until_last_use() {
    let mut catalog = Catalog::default();
    policy_ddl(
        "create placement policy x primary_region=\"r1\" regions=\"r1,r2\" schedule=\"EVEN\"",
        &mut catalog,
    )
    .unwrap();
    let x_id = catalog.policy("x").unwrap().id;

    run_create_table_on("create table t (a int) placement policy \"x\"", &mut catalog).unwrap();
    run_create_table_on("create table t2 (a int) placement policy \"x\"", &mut catalog).unwrap();
    for name in ["t", "t2"] {
        let reference = stored_table(&catalog, name)
            .placement_policy()
            .cloned()
            .expect("table carries a reference");
        assert_eq!(reference.id, x_id);
        assert_eq!(reference.name.lowercase(), "x");
    }

    // Alter the policy: every referencing table sees the new settings
    // (they reference by id), while the stored reference is unchanged.
    policy_ddl(
        "alter placement policy x primary_region=\"r3\" regions=\"r3,r4\" schedule=\"ALWAYS\"",
        &mut catalog,
    )
    .unwrap();
    let (id_after, _, primary_region, regions, schedule) = policy_summary(&catalog, "x");
    assert_eq!(id_after, x_id, "alter keeps the policy id");
    assert_eq!(primary_region, "r3");
    assert_eq!(regions, "r3,r4");
    assert_eq!(schedule, "ALWAYS");
    for name in ["t", "t2"] {
        let reference = stored_table(&catalog, name)
            .placement_policy()
            .cloned()
            .expect("reference unchanged by the alter");
        assert_eq!(reference.id, x_id);
    }

    // Refused while any dependent table exists.
    let error = policy_ddl("drop placement policy x", &mut catalog)
        .expect_err("x is still in use");
    assert_eq!(error.to_mysql_error().code, 8241);

    // Drop the first dependent: still in use because of t2.
    drop_table(&mut catalog, "drop table if exists t");
    assert!(catalog.policy_in_use("x"));
    let error = policy_ddl("drop placement policy x", &mut catalog)
        .expect_err("t2 still depends on x");
    assert_eq!(error.to_mysql_error().code, 8241);
    assert!(catalog.policy("x").is_some(), "the policy object survives the failed drops");

    // Drop the last dependent: the policy becomes droppable, and after the
    // drop it is gone.
    drop_table(&mut catalog, "drop table if exists t2");
    assert!(!catalog.policy_in_use("x"));
    policy_ddl("drop placement policy x", &mut catalog).expect("x drops freely now");
    assert!(catalog.policy("x").is_none());
}

/// Go `TestAlterTablePlacement` (`pkg/ddl/placement_policy_test.go:1425-
/// 1492`), executor-visible rows: ALTER TABLE ... PLACEMENT POLICY p stamps
/// the table's reference with the policy's id, a failed alter (unknown
/// policy `px`, 8239) leaves the previous reference in place, and the
/// partitioned table's own partition definitions are untouched by the
/// table-level alter.
#[test]
fn alter_table_placement_updates_ref_and_failed_alter_keeps_old_ref() {
    let mut catalog = Catalog::default();
    policy_ddl("create placement policy p1 primary_region='r1' regions='r1'", &mut catalog).unwrap();
    let p1_id = catalog.policy("p1").unwrap().id;
    run_create_table_on(
        "CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000))",
        &mut catalog,
    )
    .unwrap();
    assert!(stored_table(&catalog, "tp").placement_policy().is_none());

    // Alter with the policy: reference stamped with id + name.
    tidb_executor::run_alter_table_in(
        "alter table tp placement policy p1",
        &mut catalog,
        "test",
        &StmtContext::default().with_strict(true),
    )
    .expect("alter table placement policy p1 succeeds");
    let table = stored_table(&catalog, "tp");
    let reference = table.placement_policy().expect("reference now set").clone();
    assert_eq!(reference.id, p1_id);
    assert_eq!(reference.name.lowercase(), "p1");
    assert!(
        table
            .partition()
            .unwrap()
            .definitions
            .iter()
            .all(|definition| definition.placement_policy.is_none()),
        "a table-level alter does not write partition references"
    );

    // Failed alter: 8239 and no effect on the stored reference.
    let error = tidb_executor::run_alter_table_in(
        "alter table tp placement policy px",
        &mut catalog,
        "test",
        &StmtContext::default().with_strict(true),
    )
    .expect_err("unknown policy");
    let wire = error.to_mysql_error();
    assert_eq!(wire.code, 8239);
    assert_eq!(wire.message, "Unknown placement policy 'px'");
    let reference_after = stored_table(&catalog, "tp")
        .placement_policy()
        .cloned()
        .expect("reference kept");
    assert_eq!(reference_after, reference);
}

/// Go `TestAlterTablePlacement` reset rows
/// (`pkg/ddl/placement_policy_test.go:1457-1466`): `alter table tp placement
/// policy default` clears the table's reference entirely (Go's
/// `AlterTable` arm folds the name `default` into a reset,
/// `pkg/ddl/executor.go:1927` onward), so the following `SHOW CREATE TABLE`
/// no longer prints a placement clause.
// go-parity-gap: the Rust alter arm treats `default` as a literal policy
// name and answers ErrPlacementPolicyNotExists (8239, name "DEFAULT"),
// verified this session, where Go clears the reference. No carrier of the
// reset semantics exists, so the row is recorded, not approximated.
#[test]
#[ignore]
fn alter_table_placement_to_default_clears_the_reference() {
}

/// Go `TestDropPartitionWithPlacement` (`pkg/ddl/placement_policy_test.go:
/// 2051-2147`), executor-visible slice: a policy referenced ONLY by a
/// partition definition is in use until that partition is dropped; the
/// DROP PARTITION itself succeeds and afterwards the policy drops freely.
/// Go additionally pins the PD bundle removal through the GC worker, which
/// is the gap noted in the module doc.
#[test]
fn drop_partition_with_placement_frees_the_policy_only_after_the_partition_goes() {
    let mut catalog = Catalog::default();
    policy_ddl("create placement policy p2 primary_region='r2' regions='r2'", &mut catalog).unwrap();
    run_create_table_on(
        "CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100) placement policy p2, \
         PARTITION p1 VALUES LESS THAN (1000))",
        &mut catalog,
    )
    .unwrap();

    // The partition-level reference blocks the drop.
    let error = policy_ddl("drop placement policy p2", &mut catalog)
        .expect_err("p2 is referenced by partition p0");
    assert_eq!(error.to_mysql_error().code, 8241);

    tidb_executor::run_alter_table_in(
        "alter table tp drop partition p0",
        &mut catalog,
        "test",
        &StmtContext::default().with_strict(true),
    )
    .expect("the partition drops");

    // And with the partition gone, the policy is free.
    assert!(!catalog.policy_in_use("p2"));
    policy_ddl("drop placement policy p2", &mut catalog)
        .expect("p2 drops after its partition is gone");
}

/// Go `TestTruncateTableWithPlacement` (`pkg/ddl/placement_policy_test.go:
/// 1754-1879`), executor-visible slice: TRUNCATE TABLE keeps the table's
/// placement reference AND every partition-level reference exactly as they
/// were (Go re-creates the table info from the old one, so the references
/// ride along), and the policies remain in use afterwards. The rows this
/// slice cannot pin: Go assigns the truncated table a NEW table id
/// (`require.True(t, newT1.Meta().ID != t1.Meta().ID)`) and pins the old
/// bundles as waiting-for-GC; the Rust truncate keeps the same physical
// table entry, and PD bundle state has no carrier here.
#[test]
fn truncate_table_with_placement_keeps_table_and_partition_refs() {
    let mut catalog = Catalog::default();
    policy_ddl("create placement policy p1 primary_region='r1' regions='r1'", &mut catalog).unwrap();
    policy_ddl("create placement policy p2 primary_region='r2' regions='r2'", &mut catalog).unwrap();
    let p1_id = catalog.policy("p1").unwrap().id;
    let p2_id = catalog.policy("p2").unwrap().id;

    run_create_table_on(
        "CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000) placement policy p2, \
         PARTITION p2 VALUES LESS THAN (10000))",
        &mut catalog,
    )
    .unwrap();

    run_truncate_table_in(
        "TRUNCATE TABLE tp",
        &mut catalog,
        "test",
        tidb_parser::SqlMode::default(),
    )
    .expect("truncate succeeds");

    let table = stored_table(&catalog, "tp");
    let reference = table.placement_policy().expect("table ref kept").clone();
    assert_eq!(reference.id, p1_id);
    assert_eq!(reference.name.lowercase(), "p1");
    let definitions = &table.partition().unwrap().definitions;
    assert!(definitions[0].placement_policy.is_none());
    let p1_ref = definitions[1].placement_policy.as_ref().unwrap();
    assert_eq!(p1_ref.id, p2_id);
    assert_eq!(p1_ref.name.lowercase(), "p2");
    assert!(definitions[2].placement_policy.is_none());

    // Both policies are still referenced, so neither drops.
    assert!(catalog.policy_in_use("p1"));
    assert!(catalog.policy_in_use("p2"));
}

/// Go `TestAlterTablePartitionWithPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:1074-1109`): `ALTER TABLE t1 PARTITION
/// p0 PLACEMENT POLICY=\"x\"` refuses an unknown policy with
/// ErrPlacementPolicyNotExists (8239) before anything else ("only placement
/// policy should check the policy existence"), stamps the partition's
/// `PlacementPolicyRef` with the policy id once it exists, and the change
/// is visible in `information_schema.Partitions`.
// go-parity-gap: the executor refuses ALTER TABLE partition SetOptions as
// unsupported (the AST arm `AlterPartitionAction::SetOptions` exists, the
// DDL dispatch does not implement it), so the scenario cannot execute.
#[test]
#[ignore]
fn alter_table_partition_with_placement_policy_checks_existence_then_stamps_ref() {
}

/// Go `TestPolicyInheritance` (`pkg/ddl/placement_policy_test.go:1132`):
/// with a database created `PLACEMENT POLICY p1`, plain tables created in
/// it INHERIT the policy (SHOW CREATE TABLE carries the placement suffix;
/// a table's own `placement policy p2` overrides; CREATE TABLE LIKE does
/// NOT inherit; partitioned tables inherit the table-level policy as their
/// partitions' default while a partition's own option wins), and
/// `first/last partition` split syntax refuses a PLACEMENT POLICY option
/// with Go's parser error 1064 (fix #52257 context).
// go-parity-gap: the Catalog records no database-level placement
// reference, so the inheritance chain has no carrier here.
#[test]
#[ignore]
fn policy_inheritance_from_database_placement_overrides_correctly() {
}

/// Go `TestDatabasePlacement` (`pkg/ddl/placement_policy_test.go:1240`):
/// a database without placement shows a NULL policy name; `ALTER DATABASE
/// db2 PLACEMENT POLICY p1` binds it (visible in information_schema and
/// SHOW CREATE DATABASE), and re-alter swaps the policy.
// go-parity-gap: no ALTER DATABASE placement carrier and no
// information_schema surface in this tier.
#[test]
#[ignore]
fn database_placement_alter_binds_and_swaps_policies() {
}

/// Go `TestDropDatabaseGCPlacement`
/// (`pkg/ddl/placement_policy_test.go:1304`): dropping a database whose
/// tables carried placement policies schedules the delete-range for their
/// bundles; after GC the policies' bundles are gone while the POLICIES
/// themselves remain until dropped explicitly.
// go-parity-gap: no GC worker or PD bundle store; the delete-range
// lifecycle has no Rust carrier.
#[test]
#[ignore]
fn drop_database_gc_placement_removes_bundles_after_gc() {
}

/// Go `TestDropTableGCPlacement` (`pkg/ddl/placement_policy_test.go:1363`):
/// same contract as the database-level GC test, per table: dropping a
/// table whose policy-carrying bundles exist removes them at GC while
/// untouched policies stay.
// go-parity-gap: no GC worker or PD bundle store.
#[test]
#[ignore]
fn drop_table_gc_placement_removes_bundles_after_gc() {
}

/// Go `TestDropTablePartitionGCPlacement`
/// (`pkg/ddl/placement_policy_test.go:1494`): partitions dropped from a
/// placement-carrying partitioned table have their per-partition bundles
/// removed at GC; the surviving partitions and the table keep theirs.
// go-parity-gap: no GC worker or PD bundle store.
#[test]
#[ignore]
fn drop_table_partition_gc_placement_removes_stale_bundles() {
}

/// Go `TestAlterTablePartitionPlacement`
/// (`pkg/ddl/placement_policy_test.go:1584`): `ALTER TABLE tp PARTITION p0
/// PLACEMENT POLICY p1` stamps p0's reference; `PARTITION p1 PLACEMENT
/// POLICY default` on an UNREFERENCED partition is a no-op; `PARTITION p0
/// PLACEMENT POLICY default` clears the written reference; SHOW CREATE
/// TABLE prints the clauses at the right partitions; an unknown policy is
/// 8239.
// go-parity-gap: the DDL dispatch refuses partition SetOptions as
// unsupported, so none of the scenario can execute.
#[test]
#[ignore]
fn alter_table_partition_placement_clears_and_guards_refs() {
}

/// Go `TestAddPartitionWithPlacement`
/// (`pkg/ddl/placement_policy_test.go:1682`): `ALTER TABLE tp ADD
/// PARTITION (partition p3 VALUES LESS THAN (300) PLACEMENT POLICY p2)`
/// stores the new partition's reference with the policy's id; the table's
/// other partitions keep their previous state; SHOW CREATE TABLE renders
/// the new clause; bundles exist in PD for the new partition.
// go-parity-gap: ADD PARTITION with definition options (including
// placement policy) is refused as unsupported by the Rust carrier
// (src/ddl/alter_table.rs add_partition_action).
#[test]
#[ignore]
fn add_partition_with_placement_stamps_the_new_partition_ref() {
}

/// Go `TestTruncateTablePartitionWithPlacement`
/// (`pkg/ddl/placement_policy_test.go:1881-1995`): `ALTER TABLE tp TRUNCATE
/// PARTITION p1,p3` keeps the truncated partitions' own placement
/// references (Go copies the table reference onto truncated partitions that
/// lacked one at `pkg/ddl/executor.go` and keeps written ones) while
/// re-assigning their physical ids, and untouched partitions keep BOTH
/// their id and their reference.
// go-parity-gap: Go's setup pokes `model.PartitionDefinition.
// PlacementPolicyRef` through the meta directly before truncating, and the
// assertions mix bundle lifecycle with physical-id churn; the Rust
// truncate-partition carrier keeps neither physical-id rotation nor
// cross-partition reference copying, so the scenario cannot execute.
#[test]
#[ignore]
fn truncate_table_partition_with_placement_keeps_partition_refs() {
}

/// Go `TestDropTableWithPlacement` (`pkg/ddl/placement_policy_test.go:1997-
/// 2049`): after the table carrying the policies is dropped, its rule
/// bundles are removed from PD (through the GC worker failpoint) and the
/// policies become droppable. The executor-visible "policy droppable after
/// the last referencing table goes" slice is pinned by
/// `policy_cache_and_dependency_alter_seen_by_tables_drop_blocked_until_last_use`
/// above; the bundle/GC assertions have no carrier.
// go-parity-gap: PD bundle deletion via gcWorker.DeleteRanges is not
// transcreated.
#[test]
#[ignore]
fn drop_table_with_placement_removes_bundles_through_gc() {
}

/// Go `TestExchangePartitionWithPlacement`
/// (`pkg/ddl/placement_policy_test.go:2149-2231`): EXCHANGE PARTITION swaps
/// placement state between the partition and the exchanged table -- the
/// partition takes the table's policy reference and vice versa -- and an
/// exchange whose two sides carry DIFFERENT placement metadata is refused
/// with ErrTablesDifferentMetadata (Go: mysql.ErrTablesDifferentMetadata,
/// error code 1736).
// go-parity-gap: the executor has no EXCHANGE PARTITION action (the AST arm
// exists, `tidb_ast::AlterPartitionAction::Exchange`, but the DDL dispatch
// refuses it as unsupported).
#[test]
#[ignore]
fn exchange_partition_with_placement_swaps_and_guards_metadata() {
}

/// Go `TestPDFail` (`pkg/ddl/placement_policy_test.go:2233-2322`): when PD
// rejects the bundle put (`putRuleBundlesError` failpoint), the DDL job
// fails and rolls back instead of committing half the placement change.
// go-parity-gap: no PD client and no bundle publication path; the
// failpoint itself is Go-test-only infrastructure.
#[test]
#[ignore]
fn pd_fail_rolls_back_the_bundle_publication() {
}

/// Go `TestRecoverTableWithPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:2324-2434`): a dropped partitioned
// table recovered via `recover table` regains its table-level and
/// partition-level placement references, and FLASHBACK restores the policy
// links after the safe-point GC window (`tikv_gc_safe_point` SQL poke).
// go-parity-gap: no recover/flashback carrier in the executor and no GC
// safe-point surface.
#[test]
#[ignore]
fn recover_table_with_placement_policy_restores_the_refs() {
}

/// Go `TestAlterPartitioningWithPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:2436-2640`): re-partitioning a table
// that carries a policy (`ALTER TABLE t1 PARTITION BY HASH (id) PARTITIONS
// 3`) keeps the table-level reference across the change, re-balances the
// per-partition bundles, and after GC the stale bundles are gone.
// go-parity-gap: ALTER TABLE ... PARTITION BY (repartition) is refused as
// unsupported by the DDL dispatch, and the bundle assertions have no
// carrier.
#[test]
#[ignore]
fn alter_partitioning_with_placement_policy_keeps_the_ref() {
}

/// Go `TestCheckBundle` (`pkg/ddl/placement_policy_test.go:2642-2760`):
/// invariant checker over completed bundles -- after every placement DDL
/// the set of bundles in PD must exactly mirror the infoschema's references
/// (no orphan bundles for dropped objects, none missing for live ones).
// go-parity-gap: the bundle store and its checker (infosync.GetAllRuleBundles)
// have no Rust counterpart.
#[test]
#[ignore]
fn check_bundle_mirrors_infoschema_references() {
}

/// Creates a table in a database other than the default, for Go's
/// cross-database dependency rows.
fn run_create_table_in_db(database: &str, sql: &str, catalog: &mut Catalog) {
    tidb_executor::run_create_table_in(
        sql,
        catalog,
        database,
        tidb_executor::CreateTableSettings::default(),
        &StmtContext::default().with_strict(true),
    )
    .expect("create in the named database succeeds");
}
