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

//! Slow-log rule composition from `pkg/executor/adapter_slow_log.go`.
//!
//! The source combines conditions within one rule using AND and combines
//! rules within a scope using OR. Session rules take precedence over the
//! connection-specific global rule, which takes precedence over the global
//! sentinel rule. This leaf accepts pre-evaluated condition booleans so the
//! logical/hierarchy contract is dependency-closed; parsing, field accessors,
//! threshold matching, SessionVars, and slow-log output remain external.

/// Global-rule connection ID used for the source's cluster-wide sentinel.
pub const UNSET_CONNECTION_ID: i64 = -1;

/// Returns true when any rule matches.
///
/// Each inner vector is one rule's conditions. Conditions are ANDed within a
/// rule, while the outer rule list is ORed. An empty rule therefore matches,
/// exactly like the Go `match := true` loop with zero conditions.
#[must_use]
pub fn match_rules(rules: &[Vec<bool>]) -> bool {
    rules
        .iter()
        .any(|conditions| conditions.iter().all(|matched| *matched))
}

/// Applies source precedence across session and global slow-log rules.
#[must_use]
pub fn should_write_slow_log(
    session_rules: Option<&[Vec<bool>]>,
    specific_connection_rules: Option<&[Vec<bool>]>,
    global_rules: Option<&[Vec<bool>]>,
) -> bool {
    if session_rules.is_some_and(match_rules) {
        return true;
    }
    if specific_connection_rules.is_some_and(match_rules) {
        return true;
    }
    global_rules.is_some_and(match_rules)
}
