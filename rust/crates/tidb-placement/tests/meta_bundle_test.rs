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

//! Go `meta_bundle_test.go` (`package placement_test`): building table,
//! partition, and full-table bundles through a [`PolicyGetter`].
//!
//! boundary: Go's getter is a `meta.Mutator` over a `mockstore` transaction,
//! reached through `kv.RunInNewTxn`. Only `GetPolicy` is exercised, so the
//! store, the transaction, and the metadata mutator are replaced by an
//! in-memory getter over the same three `PolicyInfo` values; nothing else in
//! this file changes.

use std::collections::HashMap;

use tidb_ast::CiString;
use tidb_model::schema_state::SchemaState;
use tidb_model::{
    GoShared, PartitionDefinition, PartitionInfo, PlacementSettings, PolicyInfo, PolicyRefInfo,
    TableInfo,
};
use tidb_placement::pd::Rule;
use tidb_placement::{
    new_bundle_from_options, new_full_table_bundles, new_partition_bundle,
    new_partition_list_bundles, new_table_bundle, Bundle, PlacementError, PolicyGetter,
    RULE_INDEX_PARTITION, RULE_INDEX_TABLE,
};

/// Go `hex.EncodeToString`.
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Go `hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id)))`.
fn table_prefix_hex(id: i64) -> String {
    let mut buffer = Vec::new();
    tidb_codec::encode_bytes(
        &mut buffer,
        &tidb_tablecodec::table_key::gen_table_prefix(id),
    );
    hex_encode(&buffer)
}

/// The in-memory stand-in for Go's `meta.Mutator`.
struct PolicyStore {
    policies: HashMap<i64, PolicyInfo>,
}

impl PolicyGetter for PolicyStore {
    fn get_policy(&self, policy_id: i64) -> Result<PolicyInfo, PlacementError> {
        Ok(self
            .policies
            .get(&policy_id)
            .unwrap_or_else(|| panic!("policy {policy_id} does not exist"))
            .clone())
    }
}

/// Go `metaBundleSuite`.
struct MetaBundleSuite {
    policy1: PolicyInfo,
    policy2: PolicyInfo,
    policy3: PolicyInfo,
    tbl1: TableInfo,
    tbl2: TableInfo,
    tbl3: TableInfo,
    tbl4: TableInfo,
}

fn policy(id: i64, name: &str, settings: PlacementSettings) -> PolicyInfo {
    PolicyInfo {
        placement_settings: Some(GoShared::new(settings)),
        id,
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
    }
}

fn policy_ref(id: i64, name: &str) -> GoShared<PolicyRefInfo> {
    GoShared::new(PolicyRefInfo {
        id,
        name: CiString::new(name),
    })
}

fn definition(
    id: i64,
    name: &str,
    reference: Option<GoShared<PolicyRefInfo>>,
) -> PartitionDefinition {
    PartitionDefinition {
        id,
        name: CiString::new(name),
        placement_policy_ref: reference,
        ..PartitionDefinition::default()
    }
}

/// Go `createMetaBundleSuite`.
fn create_meta_bundle_suite() -> MetaBundleSuite {
    let policy1 = policy(
        11,
        "p1",
        PlacementSettings {
            primary_region: "r1".to_owned(),
            regions: "r1,r2".to_owned(),
            ..PlacementSettings::default()
        },
    );
    let policy2 = policy(
        12,
        "p2",
        PlacementSettings {
            primary_region: "r2".to_owned(),
            regions: "r1,r2".to_owned(),
            ..PlacementSettings::default()
        },
    );
    let policy3 = policy(
        13,
        "p3",
        PlacementSettings {
            leader_constraints: "[+region=bj]".to_owned(),
            ..PlacementSettings::default()
        },
    );
    let tbl1 = TableInfo {
        id: 101,
        name: CiString::new("t1"),
        placement_policy_ref: Some(policy_ref(11, "p1")),
        partition: Some(GoShared::new(PartitionInfo {
            definitions: vec![
                definition(1000, "par0", None),
                definition(1001, "par1", Some(policy_ref(12, "p2"))),
                definition(1002, "par2", None),
            ]
            .into(),
            ..PartitionInfo::default()
        })),
        ..TableInfo::default()
    };
    let tbl2 = TableInfo {
        id: 102,
        name: CiString::new("t2"),
        partition: Some(GoShared::new(PartitionInfo {
            definitions: vec![
                definition(1000, "par0", Some(policy_ref(11, "p1"))),
                definition(1001, "par1", None),
                definition(1002, "par2", None),
            ]
            .into(),
            ..PartitionInfo::default()
        })),
        ..TableInfo::default()
    };
    let tbl3 = TableInfo {
        id: 103,
        name: CiString::new("t3"),
        placement_policy_ref: Some(policy_ref(13, "p3")),
        ..TableInfo::default()
    };
    let tbl4 = TableInfo {
        id: 104,
        name: CiString::new("t4"),
        ..TableInfo::default()
    };
    MetaBundleSuite {
        policy1,
        policy2,
        policy3,
        tbl1,
        tbl2,
        tbl3,
        tbl4,
    }
}

impl MetaBundleSuite {
    /// Go `(*metaBundleSuite).prepareMeta`.
    fn prepare_meta(&self) -> PolicyStore {
        let mut policies = HashMap::new();
        for policy in [&self.policy1, &self.policy2, &self.policy3] {
            policies.insert(policy.id, policy.clone());
        }
        PolicyStore { policies }
    }

    fn definitions(table: &TableInfo) -> Vec<PartitionDefinition> {
        table
            .partition
            .as_ref()
            .map(|partition| partition.read().definitions.snapshot())
            .unwrap_or_default()
    }

    /// Go `(*metaBundleSuite).checkTwoJSONObjectEquals`.
    fn check_two_json_object_equals(expected: &Bundle, got: &Bundle) {
        let expected_str = serde_json::to_string(expected).expect("bundle marshals");
        let got_str = serde_json::to_string(got).expect("bundle marshals");
        assert_eq!(expected_str, got_str);
    }

    /// Go `(*metaBundleSuite).checkTableBundle`.
    fn check_table_bundle(&self, table: &TableInfo, got: Option<&Bundle>) {
        let Some(reference) = table.placement_policy_ref.as_ref() else {
            assert!(got.is_none());
            return;
        };

        let mut expected = Bundle {
            id: format!("TiDB_DDL_{}", table.id),
            index: RULE_INDEX_TABLE,
            r#override: true,
            rules: self.expected_rules(Some(reference)),
        };

        for (index, rule) in expected.rules.iter_mut().enumerate() {
            rule.group_id = format!("TiDB_DDL_{}", table.id);
            rule.index = RULE_INDEX_TABLE;
            rule.id = format!("table_rule_{}_{index}", table.id);
            rule.start_key_hex = table_prefix_hex(table.id);
            rule.end_key_hex = table_prefix_hex(table.id + 1);
        }

        for partition in Self::definitions(table) {
            let mut rules = self.expected_rules(Some(reference));
            for (index, rule) in rules.iter_mut().enumerate() {
                rule.group_id.clone_from(&expected.id);
                rule.index = RULE_INDEX_PARTITION;
                rule.id = format!("partition_rule_{}_{index}", partition.id);
                rule.start_key_hex = table_prefix_hex(partition.id);
                rule.end_key_hex = table_prefix_hex(partition.id + 1);
            }
            expected.rules.extend(rules);
        }

        Self::check_two_json_object_equals(&expected, got.expect("bundle exists"));
    }

    /// Go `(*metaBundleSuite).checkPartitionBundle`.
    fn check_partition_bundle(&self, def: &PartitionDefinition, got: Option<&Bundle>) {
        let Some(reference) = def.placement_policy_ref.as_ref() else {
            assert!(got.is_none());
            return;
        };

        let mut expected = Bundle {
            id: format!("TiDB_DDL_{}", def.id),
            index: RULE_INDEX_PARTITION,
            r#override: true,
            rules: self.expected_rules(Some(reference)),
        };

        for (index, rule) in expected.rules.iter_mut().enumerate() {
            rule.group_id = format!("TiDB_DDL_{}", def.id);
            rule.index = RULE_INDEX_TABLE;
            rule.id = format!("partition_rule_{}_{index}", def.id);
            rule.start_key_hex = table_prefix_hex(def.id);
            rule.end_key_hex = table_prefix_hex(def.id + 1);
        }

        Self::check_two_json_object_equals(&expected, got.expect("bundle exists"));
    }

    /// Go `(*metaBundleSuite).expectedRules`.
    fn expected_rules(&self, reference: Option<&GoShared<PolicyRefInfo>>) -> Vec<Rule> {
        let Some(reference) = reference else {
            return Vec::new();
        };

        let reference = reference.read();
        let policy = if reference.id == self.policy1.id {
            &self.policy1
        } else if reference.id == self.policy2.id {
            &self.policy2
        } else if reference.id == self.policy3.id {
            &self.policy3
        } else {
            panic!("unknown policy {}", reference.id);
        };

        assert_eq!(policy.name.original(), reference.name.original());
        let settings = policy
            .placement_settings
            .as_ref()
            .map(|shared| shared.read().clone());

        new_bundle_from_options(settings.as_ref())
            .expect("policy settings are valid")
            .rules
    }
}

/// Go `TestNewTableBundle` (`meta_bundle_test.go`).
#[test]
fn test_new_table_bundle() {
    let suite = create_meta_bundle_suite();
    let store = suite.prepare_meta();

    // tbl1
    let bundle = new_table_bundle(&store, &suite.tbl1).expect("table bundle");
    suite.check_table_bundle(&suite.tbl1, bundle.as_ref());

    // tbl2
    let bundle = new_table_bundle(&store, &suite.tbl2).expect("table bundle");
    suite.check_table_bundle(&suite.tbl2, bundle.as_ref());

    // tbl3
    let bundle = new_table_bundle(&store, &suite.tbl3).expect("table bundle");
    suite.check_table_bundle(&suite.tbl3, bundle.as_ref());

    // tbl4
    let bundle = new_table_bundle(&store, &suite.tbl4).expect("table bundle");
    suite.check_table_bundle(&suite.tbl4, bundle.as_ref());
}

/// Go `TestNewPartitionBundle` (`meta_bundle_test.go`).
#[test]
fn test_new_partition_bundle() {
    let suite = create_meta_bundle_suite();
    let store = suite.prepare_meta();
    let definitions = MetaBundleSuite::definitions(&suite.tbl1);

    // tbl1.par0
    let bundle = new_partition_bundle(&store, &definitions[0]).expect("partition bundle");
    suite.check_partition_bundle(&definitions[0], bundle.as_ref());

    // tbl1.par1
    let bundle = new_partition_bundle(&store, &definitions[1]).expect("partition bundle");
    suite.check_partition_bundle(&definitions[1], bundle.as_ref());
}

/// Go `TestNewPartitionListBundles` (`meta_bundle_test.go`).
#[test]
fn test_new_partition_list_bundles() {
    let suite = create_meta_bundle_suite();
    let store = suite.prepare_meta();
    let definitions = MetaBundleSuite::definitions(&suite.tbl1);

    let bundles = new_partition_list_bundles(&store, &definitions).expect("partition bundles");
    assert_eq!(1, bundles.len());
    suite.check_partition_bundle(&definitions[1], bundles.first());

    let bundles = new_partition_list_bundles(&store, &[]).expect("partition bundles");
    assert_eq!(0, bundles.len());

    let bundles =
        new_partition_list_bundles(&store, &[definitions[0].clone(), definitions[2].clone()])
            .expect("partition bundles");
    assert_eq!(0, bundles.len());
}

/// Go `TestNewFullTableBundles` (`meta_bundle_test.go`).
#[test]
fn test_new_full_table_bundles() {
    let suite = create_meta_bundle_suite();
    let store = suite.prepare_meta();

    let bundles = new_full_table_bundles(&store, &suite.tbl1).expect("full table bundles");
    assert_eq!(2, bundles.len());
    suite.check_table_bundle(&suite.tbl1, bundles.first());
    suite.check_partition_bundle(
        &MetaBundleSuite::definitions(&suite.tbl1)[1],
        bundles.get(1),
    );

    let bundles = new_full_table_bundles(&store, &suite.tbl2).expect("full table bundles");
    assert_eq!(1, bundles.len());
    suite.check_partition_bundle(
        &MetaBundleSuite::definitions(&suite.tbl2)[0],
        bundles.first(),
    );

    let bundles = new_full_table_bundles(&store, &suite.tbl3).expect("full table bundles");
    assert_eq!(1, bundles.len());
    suite.check_table_bundle(&suite.tbl3, bundles.first());

    let bundles = new_full_table_bundles(&store, &suite.tbl4).expect("full table bundles");
    assert_eq!(0, bundles.len());
}
