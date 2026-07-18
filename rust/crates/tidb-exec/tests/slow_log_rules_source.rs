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

//! Source-backed tests for slow-log rule metadata ownership.

use std::collections::{BTreeMap, BTreeSet};

use tidb_exec::slow_log_rules::{
    GlobalSlowLogRules, SessionSlowLogRules, SlowLogCondition, SlowLogRule, SlowLogRules,
};
use tidb_exec::slow_log_threshold::SlowLogValue;

fn sample_rules() -> SlowLogRules {
    SlowLogRules {
        raw_rules: "succ: true".to_owned(),
        fields: BTreeSet::from(["succ".to_owned()]),
        rules: vec![SlowLogRule {
            conditions: vec![SlowLogCondition {
                field: "succ".to_owned(),
                threshold: SlowLogValue::Boolean(true),
            }],
        }],
    }
}

#[test]
fn session_rule_metadata_preserves_source_update_marker() {
    // Source: pkg/sessionctx/slowlogrule/rules.go:4-65 and
    // pkg/sessionctx/variable/tests/slowlog/slow_log_test.go:260-300,
    // 485-518.
    let rules = sample_rules();
    let session_rules = SessionSlowLogRules::new(rules.clone());

    assert_eq!(session_rules.slow_log_rules, rules);
    assert!(session_rules.effective_fields.is_empty());
    assert_eq!(session_rules.global_raw_rules_hash, 0);
    assert!(session_rules.need_update_effective_fields);
}

#[test]
fn global_rule_metadata_keeps_connection_index() {
    // Source: pkg/sessionctx/slowlogrule/rules.go:56-65 and
    // pkg/sessionctx/variable/tests/slowlog/slow_log_test.go:622-659.
    let rules = sample_rules();
    let global = GlobalSlowLogRules {
        raw_rules: rules.raw_rules.clone(),
        raw_rules_hash: 0x1234,
        rules_map: BTreeMap::from([(123, rules.clone()), (-1, SlowLogRules::default())]),
    };

    assert_eq!(global.raw_rules, "succ: true");
    assert_eq!(global.raw_rules_hash, 0x1234);
    assert_eq!(global.rules_map.get(&123), Some(&rules));
    assert!(global.rules_map.contains_key(&-1));
}
