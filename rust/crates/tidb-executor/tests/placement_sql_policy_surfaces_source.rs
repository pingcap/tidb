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

//! Ports of Go `pkg/ddl/placement_sql_test.go` (pkg/ddl batch): the four
//! SQL-surface placement tests. Every one of them asserts through surfaces
//! this crate tier does not carry -- `SHOW PLACEMENT`, `SHOW CREATE
//! DATABASE/TABLE` text, `information_schema.schemata/Tables/Partitions`,
//! the `@@tidb_placement_mode` session variable or the mock TiFlash rule
// store -- so each Go test is recorded as an explicit gap with its contract
// re-derived from the Go source. Nothing is approximated.

/// Go `TestCreateSchemaWithPlacement` (`pkg/ddl/placement_sql_test.go:33`):
/// after `CREATE PLACEMENT POLICY PolicySchemaTest LEADER_CONSTRAINTS=...
/// FOLLOWER_CONSTRAINTS=... FOLLOWERS=4 LEARNER_CONSTRAINTS=... LEARNERS=4`,
/// `SHOW PLACEMENT like 'POLICY %PolicySchemaTest%'` renders the canonical
/// clause order `LEADER_CONSTRAINTS FOLLOWERS FOLLOWER_CONSTRAINTS LEARNERS
/// LEARNER_CONSTRAINTS NULL`; a schema created with `PLACEMENT POLICY =
// \`PolicySchemaTest\`` shows the policy in SHOW CREATE SCHEMA and in
/// `information_schema.schemata.TIDB_PLACEMENT_POLICY_NAME`; a table in
/// that schema INHERITS the policy (`SHOW CREATE TABLE` carries
/// `/*T![placement] PLACEMENT POLICY=\`PolicySchemaTest\` */`), while a
/// table created `PLACEMENT POLICY = "PolicyTableTest"` carries its own;
/// and `dbInfo.PlacementPolicyRef.Name.O` equals the policy name.
// go-parity-gap: no SHOW PLACEMENT / SHOW CREATE text renderers, no
// information_schema surfaces, and no database-level placement reference
// (the inheritance half) in this crate tier.
#[test]
#[ignore]
fn create_schema_with_placement_shows_policy_surfaces_and_inheritance() {
}

/// Go `TestAlterDBPlacement` (`pkg/ddl/placement_sql_test.go:75`):
/// `ALTER DATABASE TestAlterDB PLACEMENT POLICY=\`alter_z\`` with no such
/// policy reports ErrPlacementPolicyNotExists (8239) and leaves the schema
/// row's `TIDB_PLACEMENT_POLICY_NAME` NULL; after
/// `ALTER DATABASE ... PLACEMENT POLICY=\`alter_x\`` the schemata row and
/// SHOW CREATE DATABASE name alter_x, tables created in the database
/// inherit it, and `ALTER DATABASE ... DEFAULT PLACEMENT POLICY=\`alter_y\``
/// changes only FUTURE tables (the old table keeps alter_x, a new one gets
/// alter_y); recreating the database with an inline `PLACEMENT POLICY
/// alter_x` has the same effect, and a table's own `PLACEMENT POLICY=
/// \"alter_y\"` overrides the database default.
// go-parity-gap: no ALTER DATABASE placement carrier (the statement parses,
// but the executor forwards it as unsupported), no information_schema or
// SHOW CREATE surfaces.
#[test]
#[ignore]
fn alter_db_placement_resolves_defaults_and_only_future_tables_follow() {
}

/// Go `TestPlacementMode` (`pkg/ddl/placement_sql_test.go:161`): with
/// `tidb_placement_mode='IGNORE'` every placement-carrying DDL -- create /
/// alter / drop placement policy (including the CreatePlacementPolicyWithInfo
/// API paths with OnExistError/OnExistReplace), create/alter database and
/// table placement options, ADD PARTITION and ALTER PARTITION placement,
/// and CREATE TABLE LIKE -- is demoted to Note 1105 "Placement is ignored
/// when TIDB_PLACEMENT_MODE is 'IGNORE'" while the non-placement remainder
/// of the statement (DEFAULT CHARACTER SET, COMMENT, partition bounds)
/// still applies; `set tidb_placement_mode='aaa'` is
/// "[variable:1231]Variable 'tidb_placement_mode' can't be set to the value
/// of 'aaa'"; the default is STRICT.
// go-parity-gap: the tidb_placement_mode variable and its note-demotion
// path have no Rust carrier; the policy DDL surface itself is pinned by the
// sibling `placement_policy_ddl_source` module in STRICT mode.
#[test]
#[ignore]
fn placement_mode_ignore_demotes_placement_ddl_to_notes() {
}

/// Go `TestPlacementTiflashCheck` (`pkg/ddl/placement_sql_test.go:477`):
/// with the mockTiFlashStoreCount failpoint, `ALTER TABLE tp SET TIFLASH
/// REPLICA 1` plus placement changes (`ALTER TABLE tp PLACEMENT POLICY p1`,
/// `ALTER TABLE tp PARTITION p0 PLACEMENT POLICY p2`, and the same over
/// tables whose policy sits on the table or on individual partitions)
/// keeps `TiFlashReplica` available and the PD placement rules equal to
/// `infosync.MakeNewRule(tbl.Meta().ID, 1, nil)`; SHOW CREATE TABLE prints
/// the policy clauses at the right levels.
// go-parity-gap: no TiFlash replica metadata, no mock rule store, and no
// placement-aware alter surfaces (partition SetOptions is unsupported).
#[test]
#[ignore]
fn placement_tiflash_check_keeps_replica_rules_in_step_with_placement() {
}
