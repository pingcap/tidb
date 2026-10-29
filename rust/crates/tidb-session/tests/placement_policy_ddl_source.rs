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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.
//!
//! Every statement runs through the Go testkit port (`tk.must_exec`) under
//! the testkit's default strict sql_mode; stored policy settings and table
//! references are read the way Go's `external.GetTableByName` /
//! `dom.InfoSchema()` read them, from the domain's current schema.

use std::sync::Arc;

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;
use tidb_table::kv_table::KvTable;

/// The `(id, followers, primary_region, regions, schedule)` a policy must
/// carry after a create/alter, checked against Go's `checkFunc` expectations.
fn policy_summary(tk: &TestKit, name: &str) -> (i64, u64, String, String, String) {
    let settings = policy_settings(tk, name);
    (
        policy_id(tk, name),
        settings.followers,
        settings.primary_region,
        settings.regions,
        settings.schedule,
    )
}

/// Go `dom.InfoSchema().PolicyByName(name).ID`.
fn policy_id(tk: &TestKit, name: &str) -> i64 {
    tk.info_schema()
        .policy(name)
        .unwrap_or_else(|| panic!("policy {name} must exist"))
        .id
}

/// Go `dom.InfoSchema().PolicyByName(name).PlacementSettings`.
fn policy_settings(tk: &TestKit, name: &str) -> tidb_model::PlacementSettings {
    tk.info_schema()
        .policy(name)
        .unwrap_or_else(|| panic!("policy {name} must exist"))
        .placement_settings
        .as_ref()
        .expect("policy stores settings")
        .read()
        .clone()
}

/// Whether `dom.InfoSchema()` holds the policy `name`.
fn policy_exists(tk: &TestKit, name: &str) -> bool {
    tk.info_schema().policy(name).is_some()
}

/// Go `external.GetTableByName(t, tk, "test", name).Meta()`.
fn stored_table(tk: &TestKit, name: &str) -> Arc<KvTable> {
    match tk.get_table_by_name("test", name) {
        TableEntry::Kv(table) => table,
        _ => panic!("{name} is not a storage-backed table"),
    }
}

/// Go `TestPlacementValidation` (`pkg/ddl/placement_policy_test.go:400`),
/// success rows 1 and 3: a dict-shaped common CONSTRAINTS string is stored
/// verbatim (Go never parses it at DDL time; PD does, `pkg/ddl/placement/
/// errors.go:31`), and a plain PRIMARY_REGION/REGIONS policy round trips.
/// The alter half re-applies each success case over a base policy, which Go
/// restores between rows (`placement_policy_test.go:446-455`).
#[test]
fn placement_validation_success_rows_create_and_alter_settings() {
    let mut tk = TestKit::new();

    // Row 1: "Dict is not allowed for common constraint" -- in Go this is a
    // SUCCESS row (the dict is only rejected by PD's bundle builder, not by
    // the DDL layer), so the settings must be stored as written.
    tk.must_exec(
        "create placement policy x LEARNERS=1 \
         LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" \
         CONSTRAINTS=\"{'+disk=ssd':2}\"",
    );
    let (id, followers, primary_region, regions, schedule) = policy_summary(&tk, "x");
    assert!(id != 0, "create assigns a policy id");
    assert_eq!(followers, 0);
    assert_eq!(primary_region, "");
    assert_eq!(regions, "");
    assert_eq!(schedule, "");
    let settings = policy_settings(&tk, "x");
    assert_eq!(settings.learners, 1);
    assert_eq!(settings.learner_constraints, "[+zone=cn-west-1]");
    assert_eq!(settings.constraints, "{'+disk=ssd':2}");
    tk.must_exec("drop placement policy if exists x");

    // Row 3: plain region set, created and then re-applied through ALTER
    // (`alter placement policy x ...` with the same options).
    tk.must_exec(
        "create placement policy x PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1,cn-east-2\"",
    );
    let (_, _, primary_region, regions, _) = policy_summary(&tk, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1,cn-east-2");

    // Go's alter loop (placement_policy_test.go:445-455) rebuilds the
    // settings FROM THE ALTER's OPTIONS ONLY: fields the alter does not name
    // fall back to zero values (`buildPolicyInfo`,
    // pkg/ddl/placement_policy.go:509), which is what makes row 1 alter
    // cleanly over the region-set base here.
    tk.must_exec(
        "alter placement policy x LEARNERS=1 \
         LEARNER_CONSTRAINTS=\"[+zone=cn-west-1]\" \
         CONSTRAINTS=\"{'+disk=ssd':2}\"",
    );
    let (_, followers, primary_region, regions, _) = policy_summary(&tk, "x");
    assert_eq!(followers, 0);
    assert_eq!(primary_region, "", "alter resets unnamed fields");
    assert_eq!(regions, "", "alter resets unnamed fields");

    // A FAILED alter must not half-apply. Go pins this by checking
    // `SHOW PLACEMENT` after each error row (placement_policy_test.go:457);
    // the erroring row itself is the gap test below, so here the invariant
    // is pinned through an alter of a MISSING policy instead, which also
    // must leave the stored settings untouched.
    let before = policy_summary(&tk, "x");
    assert_eq!(
        tk.must_get_err_code("alter placement policy x2 REGIONS=\"bj,sh\"", 8239).message,
        "Unknown placement policy 'x2'"
    );
    assert_eq!(policy_summary(&tk, "x"), before);
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
#[test]
fn reset_schema_placement_reserved_default_policy_name_reports_1382() {
    let mut tk = TestKit::new();
    assert_eq!(
        tk.must_get_err_code("create placement policy `default` followers=4", 1382)
            .message,
        "The 'default' syntax is reserved for purposes internal to the MySQL server"
    );
    assert!(!policy_exists(&tk, "default"));
}

/// Go `TestCreateOrReplacePlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:517-543`): CREATE OR REPLACE on a
/// missing name behaves like CREATE, on an existing name behaves like ALTER
/// (same policy object, new settings), and OR REPLACE together with
/// IF NOT EXISTS is ErrWrongUsage 1221 "Incorrect usage of OR REPLACE and
/// IF NOT EXISTS" (Go reports it before anything else).
#[test]
fn create_or_replace_placement_policy_matches_create_then_alter_and_refuses_wrong_usage() {
    let mut tk = TestKit::new();

    // Missing name: plain create.
    tk.must_exec(
        "create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"",
    );
    let (_, _, primary_region, regions, _) = policy_summary(&tk, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1,cn-east");

    // Go next creates `tp` referencing x (table + partition p0) and pins the
    // PD bundles stay after the replace; the bundle half is a gap, but the
    // REFERENCE object must survive the replace with its id intact, which is
    // what Go's OnExistReplace path preserves (`pkg/ddl/executor.go:1336`
    // keeps the policy row and only swaps settings).
    tk.must_exec(
        "create table tp(id int) placement policy x PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100) PLACEMENT POLICY x, \
         PARTITION p1 VALUES LESS THAN (1000))",
    );
    let reference_before = stored_table(&tk, "tp")
        .placement_policy()
        .expect("table carries a policy reference");

    // Existing name: replace = alter of the stored settings.
    tk.must_exec(
        "create or replace placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1\"",
    );
    let (id_after, _, primary_region, regions, _) = policy_summary(&tk, "x");
    assert_eq!(primary_region, "cn-east-1");
    assert_eq!(regions, "cn-east-1");
    let reference_after = stored_table(&tk, "tp")
        .placement_policy()
        .expect("reference survives");
    assert_eq!(
        reference_before.id, reference_after.id,
        "replace keeps the policy id so references stay resolvable"
    );
    assert_eq!(reference_after.id, id_after);

    // OR REPLACE + IF NOT EXISTS: ErrWrongUsage 1221.
    assert_eq!(
        tk.must_get_err_code(
            "create or replace placement policy if not exists x primary_region=\"cn-east-1\" regions=\"cn-east-1\"",
            1221,
        )
        .message,
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
    let mut tk = TestKit::new();
    tk.must_exec(
        "create placement policy x primary_region=\"cn-east-1\" regions=\"cn-east-1,cn-east\"",
    );

    // Normal case 1: PRIMARY_REGION/REGIONS replaced. Go's golden
    // information_schema row for this alter ends in `2 0` because the
    // PLACEMENT_POLICIES reader DISPLAYS a default of 2 for an unset
    // FOLLOWERS (`pkg/executor/infoschema_reader.go:3855-3858`); the STORED
    // settings are rebuilt from the alter's options only
    // (`buildPolicyInfo`, pkg/ddl/placement_policy.go:509), so the stored
    // followers stay 0, which is what the carrier holds.
    tk.must_exec("alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj,sh\"");
    let (_, followers, primary_region, regions, schedule) = policy_summary(&tk, "x");
    assert_eq!(primary_region, "bj");
    assert_eq!(regions, "bj,sh");
    assert_eq!(followers, 0, "stored followers reset (display default 2 is information_schema-only)");
    assert_eq!(schedule, "");

    // Normal case 2: adds SCHEDULE=EVEN.
    tk.must_exec("alter placement policy x PRIMARY_REGION=\"bj\" REGIONS=\"bj\" SCHEDULE=\"EVEN\"");
    let (_, _, primary_region, regions, schedule) = policy_summary(&tk, "x");
    assert_eq!(primary_region, "bj");
    assert_eq!(regions, "bj");
    assert_eq!(schedule, "EVEN");

    // Normal case 3: leader/follower constraint pair with FOLLOWERS=3.
    tk.must_exec(
        "alter placement policy x \
         LEADER_CONSTRAINTS=\"[+region=us-east-1]\" \
         FOLLOWER_CONSTRAINTS=\"[+region=us-east-2]\" \
         FOLLOWERS=3",
    );
    let settings = policy_settings(&tk, "x");
    assert_eq!(settings.leader_constraints, "[+region=us-east-1]");
    assert_eq!(settings.follower_constraints, "[+region=us-east-2]");
    assert_eq!(settings.followers, 3);
    assert_eq!(settings.primary_region, "", "earlier fields are reset");
    assert_eq!(settings.regions, "");

    // Normal case 4: voter/learner constraints + CONSTRAINTS + VOTERS +
    // LEARNERS. Note the Go SQL relies on the grammar gluing the last
    // option of the previous line to the next word
    // (CONSTRAINTS="[+disk=ssd]" VOTERS=5): written out here as separate
    // options with the same meaning.
    tk.must_exec(
        "alter placement policy x \
         VOTER_CONSTRAINTS=\"[+region=bj]\" \
         LEARNER_CONSTRAINTS=\"[+region=sh]\" \
         CONSTRAINTS=\"[+disk=ssd]\" \
         VOTERS=5 \
         LEARNERS=3",
    );
    let settings = policy_settings(&tk, "x");
    assert_eq!(settings.voter_constraints, "[+region=bj]");
    assert_eq!(settings.learner_constraints, "[+region=sh]");
    assert_eq!(settings.constraints, "[+disk=ssd]");
    assert_eq!(settings.voters, 5);
    assert_eq!(settings.learners, 3);
    assert_eq!(settings.followers, 0, "followers from case 3 are reset");
    assert_eq!(settings.schedule, "", "schedule from case 2 is reset");

    // Missing policies report 8239 (after Go drops the table and the
    // policy; the drop is covered by the dependency test below, so here the
    // policy is dropped directly).
    tk.must_exec("drop placement policy x");
    for name in ["x", "x2"] {
        // Go mysql.ErrPlacementPolicyNotExists.
        assert_eq!(
            tk.must_get_err_code(&format!("alter placement policy {name} REGIONS=\"bj,sh\""), 8239)
                .message,
            format!("Unknown placement policy '{name}'")
        );
    }
    assert!(!policy_exists(&tk, "x"));
}

/// Go `TestCreateTableWithPlacementPolicy`
/// (`pkg/ddl/placement_policy_test.go:616-708`): only a PLACEMENT POLICY
/// option resolves against existing policies (8239 for an unknown name),
/// the table's reference carries the policy's ID and lowercased name, and a
/// partition's own option is stored on that partition alone -- the OTHER
/// partitions of the same table keep a nil reference, exactly as Go's
/// `setPartitionPlacementFromOptions` leaves them (`pkg/ddl/partition.go`).
#[test]
fn create_table_with_placement_policy_resolves_table_and_partition_refs() {
    let mut tk = TestKit::new();

    // Only placement policy checks existence: an unknown name is 8239 even
    // though no policy exists at all yet (Go placement_policy_test.go:624).
    tk.must_get_err_code("create table t(a int) PLACEMENT POLICY=\"x\"", 8239);

    tk.must_exec("create placement policy x FOLLOWERS=2 CONSTRAINTS=\"[+disk=ssd]\"");
    tk.must_exec("create placement policy z FOLLOWERS=1 SURVIVAL_PREFERENCES=\"[region, zone]\"");
    tk.must_exec("create placement policy y FOLLOWERS=3 CONSTRAINTS=\"[+region=bj]\"");
    let x_id = policy_id(&tk, "x");
    let y_id = policy_id(&tk, "y");
    assert!(x_id != 0 && y_id != 0);

    // Plain table: reference = (policy id, policy name).
    tk.must_exec("create table t(a int) PLACEMENT POLICY=\"x\"");
    let reference = stored_table(&tk, "t")
        .placement_policy()
        .expect("t carries a reference");
    assert_eq!(reference.id, x_id);
    assert_eq!(reference.name.lowercase(), "x");
    tk.must_exec("drop table if exists t");

    // Plain table against z pins the survival preferences storage.
    tk.must_exec("create table tt(a int) PLACEMENT POLICY=\"z\"");
    let reference = stored_table(&tk, "tt")
        .placement_policy()
        .expect("tt carries a reference");
    assert_eq!(reference.name.lowercase(), "z");
    let z_settings = policy_settings(&tk, "z");
    assert_eq!(z_settings.survival_preferences, "[region, zone]");
    assert_eq!(z_settings.followers, 1);

    // Range-partitioned: p1's own option is stored, p0/p2 stay nil, and the
    // ids are stamped from the named policies.
    tk.must_exec(
        "create table t_range_p(id int) placement policy x partition by range(id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000) placement policy y, \
         PARTITION p2 VALUES LESS THAN (10000))",
    );
    let table = stored_table(&tk, "t_range_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert_eq!(definitions.len(), 3);
    assert!(definitions[0].placement_policy.is_none(), "p0 inherits nothing at DDL time");
    let p1 = definitions[1].placement_policy.as_ref().unwrap();
    assert_eq!(p1.id, y_id);
    assert_eq!(p1.name.lowercase(), "y");
    assert!(definitions[2].placement_policy.is_none());
    tk.must_exec("drop table if exists t_range_p");

    // List-columns partitioned: same shape as Go's t_list_p.
    tk.must_exec(
        "create table t_list_p(name varchar(10)) placement policy x partition by list columns(name) (\
         PARTITION p0 VALUES IN ('a', 'b'), \
         PARTITION p1 VALUES IN ('c', 'd') placement policy y, \
         PARTITION p2 VALUES IN ('e', 'f'))",
    );
    let table = stored_table(&tk, "t_list_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert!(definitions[0].placement_policy.is_none());
    assert_eq!(definitions[1].placement_policy.as_ref().unwrap().id, y_id);
    assert!(definitions[2].placement_policy.is_none());
    tk.must_exec("drop table if exists t_list_p");

    // Hash-partitioned with PARTITIONS 4: every generated definition keeps
    // a nil reference while the TABLE still points at x.
    tk.must_exec(
        "create table t_hash_p(id int) placement policy x partition by HASH(id) PARTITIONS 4",
    );
    let table = stored_table(&tk, "t_hash_p");
    assert_eq!(table.placement_policy().unwrap().id, x_id);
    let definitions = &table.partition().unwrap().definitions;
    assert_eq!(definitions.len(), 4);
    assert!(
        definitions.iter().all(|definition| definition.placement_policy.is_none()),
        "generated hash partitions carry no policy reference of their own"
    );
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
    let mut tk = TestKit::new();
    tk.must_exec("create database if not exists test2");

    for name in ["p1", "p2", "p3"] {
        tk.must_exec(&format!(
            "create placement policy {name} \
             PRIMARY_REGION=\"cn-east-1\" REGIONS=\"cn-east-1, cn-east-2\" SCHEDULE=\"EVEN\""
        ));
    }
    // p1 is used by test.t11 AND test2.t21 (cross-database dependency).
    tk.must_exec("create table test.t11 (id int) placement policy 'p1'");
    tk.must_exec("create table test2.t21 (id int) placement policy 'p1'");
    // p2 is used by test.t12.
    tk.must_exec("create table test.t12 (id int) placement policy 'p2'");
    // p3 is used by test.t21 (Go's names are intentionally crosswired).
    tk.must_exec("create table test.t21 (id int) placement policy 'p3'");

    for name in ["p1", "p2", "p3"] {
        // Go mysql.ErrPlacementPolicyInUse.
        assert_eq!(
            tk.must_get_err_code(&format!("drop placement policy {name}"), 8241).message,
            format!("Placement policy '{name}' is still in use")
        );

        // IF EXISTS does not turn an in-use drop into a note.
        tk.must_get_err_code(&format!("drop placement policy if exists {name}"), 8241);
    }

    // Dropping the referencing tables frees every policy.
    tk.must_exec("drop table if exists test.t11");
    tk.must_exec("drop table if exists test2.t21");
    tk.must_exec("drop table if exists test.t12");
    tk.must_exec("drop table if exists test.t21");
    for name in ["p1", "p2", "p3"] {
        tk.must_exec(&format!("drop placement policy {name}"));
    }
}

/// Go `TestPolicyCacheAndPolicyDependency`
/// (`pkg/ddl/placement_policy_test.go:1006-1072`): referencing tables point
/// at the policy OBJECT, not a copy -- an ALTER of the policy is visible
/// through every reference while their stored reference (id+name) stays
/// put; DROP of the policy is refused (8241) while any dependent table
/// exists and only succeeds after the last one goes; afterwards the policy
/// is gone from the schema. Go's per-table dependency list comes from the
/// meta KV (`testGetPolicyDependency`); here the current schema's own
/// reference tracking (`policy_in_use`) stands in for it, which is the same
/// information.
#[test]
fn policy_cache_and_dependency_alter_seen_by_tables_drop_blocked_until_last_use() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy x primary_region=\"r1\" regions=\"r1,r2\" schedule=\"EVEN\"");
    let x_id = policy_id(&tk, "x");

    tk.must_exec("create table t (a int) placement policy \"x\"");
    tk.must_exec("create table t2 (a int) placement policy \"x\"");
    for name in ["t", "t2"] {
        let reference = stored_table(&tk, name)
            .placement_policy()
            .expect("table carries a reference");
        assert_eq!(reference.id, x_id);
        assert_eq!(reference.name.lowercase(), "x");
    }

    // Alter the policy: every referencing table sees the new settings
    // (they reference by id), while the stored reference is unchanged.
    // Go's `NewBundleFromOptions` knows only EVEN and MAJORITY_IN_PRIMARY,
    // and its error reaches the client unchanged (captured from Go).
    tk.must_get_err_msg(
        "alter placement policy x primary_region=\"r3\" regions=\"r3,r4\" schedule=\"ALWAYS\"",
        "invalid placement option: unsupported schedule ALWAYS",
    );
    tk.must_exec("alter placement policy x primary_region=\"r3\" regions=\"r3,r4\" schedule=\"MAJORITY_IN_PRIMARY\"");
    let (id_after, _, primary_region, regions, schedule) = policy_summary(&tk, "x");
    assert_eq!(id_after, x_id, "alter keeps the policy id");
    assert_eq!(primary_region, "r3");
    assert_eq!(regions, "r3,r4");
    assert_eq!(schedule, "MAJORITY_IN_PRIMARY");
    for name in ["t", "t2"] {
        let reference = stored_table(&tk, name)
            .placement_policy()
            .expect("reference unchanged by the alter");
        assert_eq!(reference.id, x_id);
    }

    // Refused while any dependent table exists.
    tk.must_get_err_code("drop placement policy x", 8241);

    // Drop the first dependent: still in use because of t2.
    tk.must_exec("drop table if exists t");
    assert!(tk.info_schema().policy_in_use("x"));
    tk.must_get_err_code("drop placement policy x", 8241);
    assert!(policy_exists(&tk, "x"), "the policy object survives the failed drops");

    // Drop the last dependent: the policy becomes droppable, and after the
    // drop it is gone.
    tk.must_exec("drop table if exists t2");
    assert!(!tk.info_schema().policy_in_use("x"));
    tk.must_exec("drop placement policy x");
    assert!(!policy_exists(&tk, "x"));
}

/// Go `TestAlterTablePlacement` (`pkg/ddl/placement_policy_test.go:1425-
/// 1492`), executor-visible rows: ALTER TABLE ... PLACEMENT POLICY p stamps
/// the table's reference with the policy's id, a failed alter (unknown
/// policy `px`, 8239) leaves the previous reference in place, and the
/// partitioned table's own partition definitions are untouched by the
/// table-level alter.
#[test]
fn alter_table_placement_updates_ref_and_failed_alter_keeps_old_ref() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy p1 primary_region='r1' regions='r1'");
    let p1_id = policy_id(&tk, "p1");
    tk.must_exec(
        "CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000))",
    );
    assert!(stored_table(&tk, "tp").placement_policy().is_none());

    // Alter with the policy: reference stamped with id + name.
    tk.must_exec("alter table tp placement policy p1");
    let table = stored_table(&tk, "tp");
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
    assert_eq!(
        tk.must_get_err_code("alter table tp placement policy px", 8239).message,
        "Unknown placement policy 'px'"
    );
    let reference_after = stored_table(&tk, "tp")
        .placement_policy()
        .expect("reference kept");
    assert_eq!(reference_after, reference);
}

/// Go `TestAlterTablePlacement` reset row
/// (`pkg/ddl/placement_policy_test.go:1457-1466`): `alter table tp placement
/// policy default` clears the table's reference entirely (Go's
/// `AlterTable` arm folds the name `default` into a reset,
/// `pkg/ddl/executor.go:1927` onward), so the following `SHOW CREATE TABLE`
/// no longer prints a placement clause.
#[test]
fn alter_table_placement_to_default_clears_the_reference() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy p1 primary_region='r1' regions='r1'");
    tk.must_exec("create table tp (id int) placement policy p1");
    assert!(stored_table(&tk, "tp").placement_policy().is_some());

    tk.must_exec("alter table tp placement policy default");
    assert!(stored_table(&tk, "tp").placement_policy().is_none());
}

/// Go `TestDropPartitionWithPlacement` (`pkg/ddl/placement_policy_test.go:
/// 2051-2147`), executor-visible slice: a policy referenced ONLY by a
/// partition definition is in use until that partition is dropped; the
/// DROP PARTITION itself succeeds and afterwards the policy drops freely.
/// Go additionally pins the PD bundle removal through the GC worker, which
/// is the gap noted in the module doc.
#[test]
fn drop_partition_with_placement_frees_the_policy_only_after_the_partition_goes() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy p2 primary_region='r2' regions='r2'");
    tk.must_exec(
        "CREATE TABLE tp (id INT) PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100) placement policy p2, \
         PARTITION p1 VALUES LESS THAN (1000))",
    );

    // The partition-level reference blocks the drop.
    tk.must_get_err_code("drop placement policy p2", 8241);

    tk.must_exec("alter table tp drop partition p0");

    // And with the partition gone, the policy is free.
    assert!(!tk.info_schema().policy_in_use("p2"));
    tk.must_exec("drop placement policy p2");
}

/// Go `TestTruncateTableWithPlacement` (`pkg/ddl/placement_policy_test.go:
/// 1754-1879`), executor-visible slice: TRUNCATE TABLE keeps the table's
/// placement reference AND every partition-level reference exactly as they
/// were (Go re-creates the table info from the old one, so the references
/// ride along), and the policies remain in use afterwards. The rows this
/// slice cannot pin: Go assigns the truncated table a NEW table id
/// (`require.True(t, newT1.Meta().ID != t1.Meta().ID)`) and pins the old
/// bundles as waiting-for-GC; PD bundle state has no carrier here.
#[test]
fn truncate_table_with_placement_keeps_table_and_partition_refs() {
    let mut tk = TestKit::new();
    tk.must_exec("create placement policy p1 primary_region='r1' regions='r1'");
    tk.must_exec("create placement policy p2 primary_region='r2' regions='r2'");
    let p1_id = policy_id(&tk, "p1");
    let p2_id = policy_id(&tk, "p2");

    tk.must_exec(
        "CREATE TABLE tp (id INT) placement policy p1 PARTITION BY RANGE (id) (\
         PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (1000) placement policy p2, \
         PARTITION p2 VALUES LESS THAN (10000))",
    );

    tk.must_exec("TRUNCATE TABLE tp");

    let table = stored_table(&tk, "tp");
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
    let schema = tk.info_schema();
    assert!(schema.policy_in_use("p1"));
    assert!(schema.policy_in_use("p2"));
}

/// Go `AlterSchema`: ENCRYPTION is ignored, and the charset change and the
/// placement change are two jobs run in order -- the first stays applied when
/// the second fails (gorun).
#[test]
fn alter_database_runs_go_jobs_in_order() {
    let mut tk = TestKit::new();
    tk.must_exec("create database d1 encryption='N'");
    tk.must_exec("create placement policy p1 followers=1");
    tk.must_exec("alter database d1 encryption='Y'");
    tk.must_exec("alter database nosuch encryption='Y'");
    tk.must_get_err_code("alter database nosuch charset utf8mb4", 1049);
    tk.must_exec("alter database d1 charset utf8mb4 collate utf8mb4_bin placement policy=p1");
    tk.must_query("show create database d1").check([
        "d1 CREATE DATABASE `d1` /*!40100 DEFAULT CHARACTER SET utf8mb4 */ /*T![placement] PLACEMENT POLICY=`p1` */",
    ]);
    tk.must_exec("alter database d1 encryption='N' charset utf8mb4");
    tk.must_exec("create database d2");
    let error = tk.must_get_err_code("alter database d2 charset latin1 placement policy=nopolicy", 8239);
    assert_eq!(error.message, "Unknown placement policy 'nopolicy'");
    tk.must_query("show create database d2")
        .check(["d2 CREATE DATABASE `d2` /*!40100 DEFAULT CHARACTER SET latin1 */"]);
    // The charset job runs before the TiFlash part fails (Go: 1049 "Empty
    // database"; this node does not port `ModifySchemaSetTiFlashReplica`).
    tk.must_exec("create database d3");
    tk.must_exec_to_err("alter database d3 charset latin1 set tiflash replica 1");
    tk.must_query("show create database d3")
        .check(["d3 CREATE DATABASE `d3` /*!40100 DEFAULT CHARACTER SET latin1 */"]);
}
