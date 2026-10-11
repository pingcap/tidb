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

//! Go `infosync`'s `mockLabelManager` (`pkg/domain/infosync/label_manager.go`):
//! the label-rule manager a store without PD gets. The in-process catalog owns
//! one, shared by every snapshot of it, as Go's `InfoSyncer` owns one per
//! domain.

use std::collections::BTreeMap;

use crate::ddl_label::{
    use_keyspace_aware_rules, LabelCodec, LabelRulePatch, Rule, KEYSPACE_PREFIX,
};

/// Go `mockLabelManager`. Rules are kept as their JSON, as Go keeps them, so
/// every read hands out a fresh copy that a later `Reset` cannot alias.
///
/// Go keys a map, whose iteration order is random; the ordered map gives
/// [`Self::get_all_label_rules`] rule-ID order.
#[derive(Debug, Default)]
pub struct MockLabelManager {
    label_rules: BTreeMap<String, Vec<u8>>,
}

impl MockLabelManager {
    /// Go `PutLabelRule`.
    pub fn put_label_rule(&mut self, rule: &Rule) -> Result<(), serde_json::Error> {
        let encoded = serde_json::to_vec(rule)?;
        self.label_rules.insert(rule.id.clone(), encoded);
        Ok(())
    }

    /// Go `UpdateLabelRules`: the deletions, then the rules to set.
    pub fn update_label_rules(&mut self, patch: &LabelRulePatch) -> Result<(), serde_json::Error> {
        patch.delete_rules.with_visible(|ids| {
            for id in ids {
                self.label_rules.remove(id);
            }
        });
        for rule in patch.set_rules.snapshot() {
            let encoded = serde_json::to_vec(&rule)?;
            self.label_rules.insert(rule.id.clone(), encoded);
        }
        Ok(())
    }

    /// Go `GetAllLabelRules`.
    pub fn get_all_label_rules(
        &self,
        codec: &dyn LabelCodec,
    ) -> Result<Vec<Rule>, serde_json::Error> {
        let rules = self
            .label_rules
            .values()
            .map(|encoded| serde_json::from_slice(encoded))
            .collect::<Result<Vec<Rule>, _>>()?;
        Ok(filter_rules_by_keyspace(rules, codec))
    }

    /// Go `GetLabelRules`: the stored rules among `rule_ids`, by ID.
    pub fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> Result<BTreeMap<String, Rule>, serde_json::Error> {
        let mut rules = BTreeMap::new();
        for rule_id in rule_ids {
            if let Some(encoded) = self.label_rules.get(rule_id) {
                rules.insert(rule_id.clone(), serde_json::from_slice(encoded)?);
            }
        }
        Ok(rules)
    }
}

/// Go `filterRulesByKeyspace`.
fn filter_rules_by_keyspace(rules: Vec<Rule>, codec: &dyn LabelCodec) -> Vec<Rule> {
    if !use_keyspace_aware_rules(codec) {
        return rules;
    }
    let rule_prefix = format!("{KEYSPACE_PREFIX}/{}/", codec.keyspace_id());
    rules
        .into_iter()
        .filter(|rule| rule.id.starts_with(&rule_prefix))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_label::CodecV1;

    /// A codec with keyspace meta for keyspace 42 (Go `tikv.NewCodecV2`).
    struct KeyspaceCodec;

    impl LabelCodec for KeyspaceCodec {
        fn has_keyspace_meta(&self) -> bool {
            true
        }

        fn keyspace_id(&self) -> u32 {
            42
        }

        fn encode_region_range(&self, start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
            (start.to_vec(), end.to_vec())
        }
    }

    fn rule(id: &str) -> Rule {
        Rule {
            id: id.to_owned(),
            ..Rule::default()
        }
    }

    /// Go `TestFilterLabelRulesByKeyspace`.
    #[test]
    fn filter_label_rules_by_keyspace() {
        let rules = vec![
            rule("schema/test/t1"),
            rule("keyspace/42/schema/test/t2"),
            rule("keyspace/43/schema/test/t3"),
        ];
        assert_eq!(filter_rules_by_keyspace(rules.clone(), &CodecV1), rules);
        if !tidb_config::kerneltype::is_next_gen() {
            assert_eq!(
                filter_rules_by_keyspace(rules.clone(), &KeyspaceCodec),
                rules
            );
            return;
        }
        assert_eq!(
            filter_rules_by_keyspace(rules.clone(), &KeyspaceCodec),
            [rules[1].clone()]
        );
    }

    /// The manager hands out copies: resetting a rule it returned does not
    /// change the stored one, as Go's JSON round trip guarantees.
    #[test]
    fn reads_are_copies_and_patches_delete_before_setting() {
        let mut manager = MockLabelManager::default();
        let mut stored = Rule::new();
        stored
            .apply_attributes_spec(&tidb_ast::AttributesSpec {
                attributes: Some("key=value".to_owned()),
            })
            .unwrap();
        stored.reset(&CodecV1, "db", "t", "", &[1]);
        manager.put_label_rule(&stored).unwrap();

        let mut read = manager.get_label_rules(&[stored.id.clone()]).unwrap()[&stored.id].clone();
        read.reset(&CodecV1, "db2", "t2", "", &[2]);
        assert_eq!(
            manager.get_all_label_rules(&CodecV1).unwrap(),
            [stored.clone()]
        );

        manager
            .update_label_rules(&crate::ddl_label::new_rule_patch(
                vec![read.clone()],
                vec![stored.id.clone(), read.id.clone()],
            ))
            .unwrap();
        assert_eq!(manager.get_all_label_rules(&CodecV1).unwrap(), [read]);
        assert!(manager
            .get_label_rules(&["schema/db/t".to_owned()])
            .unwrap()
            .is_empty());
    }
}
