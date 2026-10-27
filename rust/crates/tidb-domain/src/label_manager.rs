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

//! Go `infosync`'s label-rule manager (`pkg/domain/infosync/label_manager.go`):
//! the `LabelRuleManager` seam, `mockLabelManager` (the manager a store
//! without PD gets), and the package-level `PutLabelRule`, `UpdateLabelRules`,
//! `GetAllLabelRules` and `GetLabelRules` over the node's manager, which
//! `InfoSyncer.initLabelRuleManager` installs. `PDLabelManager` lives with the
//! node's PD HTTP client (`tidb_exec::label_delivery`).
//!
//! The in-process catalog owns a mock of its own, shared by every snapshot of
//! it, as Go's `InfoSyncer` owns one per domain; a production node's catalog
//! is handed the node's manager.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use tidb_ddl_label::{
    use_keyspace_aware_rules, LabelCodec, LabelRulePatch, Rule, KEYSPACE_PREFIX,
};

/// Go `LabelRuleManager`.
pub trait LabelRuleManager: Send + Sync + std::fmt::Debug {
    /// Go `PutLabelRule`.
    fn put_label_rule(&self, rule: &Rule) -> Result<(), String>;
    /// Go `UpdateLabelRules`.
    fn update_label_rules(&self, patch: &LabelRulePatch) -> Result<(), String>;
    /// Go `GetAllLabelRules`.
    fn get_all_label_rules(&self, codec: &dyn LabelCodec) -> Result<Vec<Rule>, String>;
    /// Go `GetLabelRules`: the stored rules among `rule_ids`, by ID.
    fn get_label_rules(&self, rule_ids: &[String]) -> Result<BTreeMap<String, Rule>, String>;
}

/// Go `mockLabelManager`. Rules are kept as their JSON, as Go keeps them, so
/// every read hands out a fresh copy that a later `Reset` cannot alias.
///
/// Go keys a map, whose iteration order is random; the ordered map gives
/// [`LabelRuleManager::get_all_label_rules`] rule-ID order.
#[derive(Debug, Default)]
pub struct MockLabelManager {
    label_rules: RwLock<BTreeMap<String, Vec<u8>>>,
}

impl LabelRuleManager for MockLabelManager {
    fn put_label_rule(&self, rule: &Rule) -> Result<(), String> {
        let encoded = serde_json::to_vec(rule).map_err(|error| error.to_string())?;
        self.label_rules
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(rule.id.clone(), encoded);
        Ok(())
    }

    /// The deletions, then the rules to set.
    fn update_label_rules(&self, patch: &LabelRulePatch) -> Result<(), String> {
        let mut label_rules = self
            .label_rules
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        patch.delete_rules.with_visible(|ids| {
            for id in ids {
                label_rules.remove(id);
            }
        });
        for rule in patch.set_rules.snapshot() {
            let encoded = serde_json::to_vec(&rule).map_err(|error| error.to_string())?;
            label_rules.insert(rule.id.clone(), encoded);
        }
        Ok(())
    }

    fn get_all_label_rules(&self, codec: &dyn LabelCodec) -> Result<Vec<Rule>, String> {
        let rules = self
            .label_rules
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .map(|encoded| serde_json::from_slice(encoded))
            .collect::<Result<Vec<Rule>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(filter_rules_by_keyspace(rules, codec))
    }

    fn get_label_rules(&self, rule_ids: &[String]) -> Result<BTreeMap<String, Rule>, String> {
        let label_rules = self
            .label_rules
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let mut rules = BTreeMap::new();
        for rule_id in rule_ids {
            if let Some(encoded) = label_rules.get(rule_id) {
                rules.insert(
                    rule_id.clone(),
                    serde_json::from_slice(encoded).map_err(|error| error.to_string())?,
                );
            }
        }
        Ok(rules)
    }
}

/// The `InfoSyncer`'s `labelRuleManager`.
static LABEL_RULE_MANAGER: Mutex<Option<Arc<dyn LabelRuleManager>>> = Mutex::new(None);

/// Go `InfoSyncer.initLabelRuleManager`: PD's manager, or a fresh mock when
/// the node has no PD.
pub fn init_label_rule_manager(manager: Option<Arc<dyn LabelRuleManager>>) {
    *LABEL_RULE_MANAGER
        .lock()
        .unwrap_or_else(PoisonError::into_inner) =
        Some(manager.unwrap_or_else(|| Arc::new(MockLabelManager::default())));
}

/// The node's label-rule manager; a node that installed none gets a mock.
pub fn label_rule_manager() -> Arc<dyn LabelRuleManager> {
    Arc::clone(
        LABEL_RULE_MANAGER
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(|| Arc::new(MockLabelManager::default())),
    )
}

/// A job step's view of the label rules while its writes are still pending:
/// reads see the step's own earlier writes over `base`, and writes stay here
/// until the step delivers them. Go's step calls PD write by write, so a
/// later read in the same step (the next pair of a multi-table RENAME) sees
/// the earlier write; a step whose writes are delivered once, before its
/// transaction commits, reads through this to see the same rules.
#[derive(Debug)]
pub struct StagedLabelRules {
    base: Arc<dyn LabelRuleManager>,
    /// Rule ID to its pending rule's JSON (kept encoded, as the mock keeps
    /// it, so every read is a fresh copy); `None` is a pending delete.
    staged: Mutex<BTreeMap<String, Option<Vec<u8>>>>,
}

impl StagedLabelRules {
    /// A step reading through to `base`.
    #[must_use]
    pub fn new(base: Arc<dyn LabelRuleManager>) -> Self {
        Self {
            base,
            staged: Mutex::new(BTreeMap::new()),
        }
    }
}

fn decode_rule(encoded: &[u8]) -> Result<Rule, String> {
    serde_json::from_slice(encoded).map_err(|error| error.to_string())
}

impl LabelRuleManager for StagedLabelRules {
    fn put_label_rule(&self, rule: &Rule) -> Result<(), String> {
        let encoded = serde_json::to_vec(rule).map_err(|error| error.to_string())?;
        self.staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(rule.id.clone(), Some(encoded));
        Ok(())
    }

    fn update_label_rules(&self, patch: &LabelRulePatch) -> Result<(), String> {
        let mut staged = self.staged.lock().unwrap_or_else(PoisonError::into_inner);
        for id in patch.delete_rules.snapshot() {
            staged.insert(id, None);
        }
        for rule in patch.set_rules.snapshot() {
            let encoded = serde_json::to_vec(&rule).map_err(|error| error.to_string())?;
            staged.insert(rule.id.clone(), Some(encoded));
        }
        Ok(())
    }

    fn get_all_label_rules(&self, codec: &dyn LabelCodec) -> Result<Vec<Rule>, String> {
        let mut rules = self
            .base
            .get_all_label_rules(codec)?
            .into_iter()
            .map(|rule| (rule.id.clone(), rule))
            .collect::<BTreeMap<_, _>>();
        for (id, encoded) in self
            .staged
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            match encoded {
                Some(encoded) => {
                    rules.insert(id.clone(), decode_rule(encoded)?);
                }
                None => {
                    rules.remove(id);
                }
            }
        }
        Ok(filter_rules_by_keyspace(
            rules.into_values().collect(),
            codec,
        ))
    }

    fn get_label_rules(&self, rule_ids: &[String]) -> Result<BTreeMap<String, Rule>, String> {
        let staged = self.staged.lock().unwrap_or_else(PoisonError::into_inner);
        let unstaged = rule_ids
            .iter()
            .filter(|id| !staged.contains_key(*id))
            .cloned()
            .collect::<Vec<_>>();
        let mut rules = if unstaged.is_empty() {
            BTreeMap::new()
        } else {
            self.base.get_label_rules(&unstaged)?
        };
        for id in rule_ids {
            if let Some(Some(encoded)) = staged.get(id) {
                rules.insert(id.clone(), decode_rule(encoded)?);
            }
        }
        Ok(rules)
    }
}

/// Go `filterRulesByKeyspace`.
pub fn filter_rules_by_keyspace(rules: Vec<Rule>, codec: &dyn LabelCodec) -> Vec<Rule> {
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
    use tidb_ddl_label::CodecV1;

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

    /// A step's reads see its own pending writes over the base manager, and
    /// a pending delete hides the base rule; the base is untouched until the
    /// step delivers.
    #[test]
    fn staged_reads_see_pending_writes_over_the_base() {
        let base: Arc<dyn LabelRuleManager> = Arc::new(MockLabelManager::default());
        base.put_label_rule(&rule("schema/db/a")).unwrap();
        base.put_label_rule(&rule("schema/db/b")).unwrap();
        let staged = StagedLabelRules::new(Arc::clone(&base));
        staged
            .update_label_rules(&tidb_ddl_label::new_rule_patch(
                vec![rule("schema/db/c")],
                vec!["schema/db/a".to_owned()],
            ))
            .unwrap();
        let ids = ["schema/db/a", "schema/db/b", "schema/db/c"].map(str::to_owned);
        assert_eq!(
            staged
                .get_label_rules(&ids)
                .unwrap()
                .into_keys()
                .collect::<Vec<_>>(),
            ["schema/db/b", "schema/db/c"]
        );
        assert_eq!(
            staged
                .get_all_label_rules(&CodecV1)
                .unwrap()
                .into_iter()
                .map(|rule| rule.id)
                .collect::<Vec<_>>(),
            ["schema/db/b", "schema/db/c"]
        );
        assert_eq!(
            base.get_label_rules(&ids)
                .unwrap()
                .into_keys()
                .collect::<Vec<_>>(),
            ["schema/db/a", "schema/db/b"]
        );
    }

    /// The manager hands out copies: resetting a rule it returned does not
    /// change the stored one, as Go's JSON round trip guarantees.
    #[test]
    fn reads_are_copies_and_patches_delete_before_setting() {
        let manager = MockLabelManager::default();
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
            .update_label_rules(&tidb_ddl_label::new_rule_patch(
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
