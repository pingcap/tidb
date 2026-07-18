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

//! Cascades rule-set filtering from `pkg/planner/cascades/rule/ruleset/rule_set.go`.
//!
//! The Go implementation stores concrete rule interfaces, memo expressions,
//! and logical-plan flags. This leaf preserves the dependency-closed rule-ID
//! mask behavior and the intermediate-Apply special-set switch over opaque
//! IDs; rule construction, memo traversal, and optimizer execution remain
//! external boundaries.

/// A growable rule-ID mask equivalent to the source bitset membership checks.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RuleMask {
    words: Vec<u64>,
}

impl RuleMask {
    /// Creates an empty mask.
    #[must_use]
    pub const fn new() -> Self {
        Self { words: Vec::new() }
    }

    /// Creates a mask containing the supplied rule IDs.
    #[must_use]
    pub fn from_ids(ids: &[u32]) -> Self {
        let mut mask = Self::new();
        for &id in ids {
            mask.insert(id);
        }
        mask
    }

    /// Adds a rule ID to this mask.
    pub fn insert(&mut self, id: u32) {
        let word = (id / 64) as usize;
        if self.words.len() <= word {
            self.words.resize(word + 1, 0);
        }
        self.words[word] |= 1_u64 << (id % 64);
    }

    /// Reports whether the source mask contains a rule ID.
    #[must_use]
    pub fn contains(&self, id: u32) -> bool {
        let word = (id / 64) as usize;
        self.words
            .get(word)
            .is_some_and(|bits| bits & (1_u64 << (id % 64)) != 0)
    }

    /// Filters rule IDs in source order, retaining every masked rule.
    #[must_use]
    pub fn filter(&self, rules: &[u32]) -> Vec<u32> {
        rules
            .iter()
            .copied()
            .filter(|rule_id| self.contains(*rule_id))
            .collect()
    }
}

impl Default for RuleMask {
    fn default() -> Self {
        Self::new()
    }
}

/// Rule lists rooted at one operand, with the source intermediate-Apply set.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OperandRules {
    default_rules: Vec<u32>,
    de_correlate_apply_rules: Vec<u32>,
}

impl OperandRules {
    /// Creates source-shaped default and special rule lists.
    #[must_use]
    pub fn new(default_rules: Vec<u32>, de_correlate_apply_rules: Vec<u32>) -> Self {
        Self {
            default_rules,
            de_correlate_apply_rules,
        }
    }

    /// Returns the special de-correlate list for an intermediate Apply,
    /// otherwise the ordinary operand list.
    #[must_use]
    pub fn filter(&self, apply_generated_from_de_correlate_rule: bool) -> &[u32] {
        if apply_generated_from_de_correlate_rule {
            &self.de_correlate_apply_rules
        } else {
            &self.default_rules
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OperandRules, RuleMask};

    #[test]
    fn mask_filter_preserves_rule_order_and_duplicates() {
        let mask = RuleMask::from_ids(&[2, 65]);
        assert_eq!(mask.filter(&[65, 1, 2, 65]), vec![65, 2, 65]);
        assert!(mask.contains(2));
        assert!(mask.contains(65));
        assert!(!mask.contains(64));
    }

    #[test]
    fn empty_mask_filters_every_rule() {
        let mask = RuleMask::new();
        assert!(mask.filter(&[0, 1, 64]).is_empty());
    }

    #[test]
    fn operand_filter_switches_only_for_intermediate_apply() {
        let rules = OperandRules::new(vec![1, 2], vec![9]);
        assert_eq!(rules.filter(false), &[1, 2]);
        assert_eq!(rules.filter(true), &[9]);
    }

    #[test]
    fn special_set_can_be_empty_without_default_fallback() {
        let rules = OperandRules::new(vec![3], Vec::new());
        assert!(rules.filter(true).is_empty());
        assert_eq!(rules.filter(false), &[3]);
    }
}
