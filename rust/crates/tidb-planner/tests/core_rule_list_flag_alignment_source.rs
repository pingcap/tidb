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

//! Port of `pkg/planner.part13` item
//! `pkg/planner/core/optimizer_test.go:650 TestOptRuleListFlagAlignment`,
//! read from origin/master.
//!
//! Go intent: every entry of `optRuleList` (`optimizer.go:88`) is gated by the
//! index-aligned `optRuleFlags` (`optimizer.go:126`) bitmasks, which are
//! stable `1 << iota` values — so rules can be inserted without renumbering.
//! The test pins four invariants (:651-684): same length, no zero flag, each
//! flag exactly one bit, no duplicate bits, and the unique-flag count equals
//! `bits.Len64(rule.FlagFullTextIndexResolveReject)` (35) so no flag exists
//! without a rule or vice versa.
//!
//! The Rust side mirrors both tables verbatim:
//! `logical::rule::OPT_RULE_LIST` (execution order) and
//! `logical::rule::OPT_RULE_FLAGS`, with the raw masks under
//! `logical::rule::flags` (cited to Go `rule/logical_rules.go:20-55`).

use tidb_planner::logical::rule::{flags, OPT_RULE_FLAGS, OPT_RULE_LIST};

/// GO PORT of `pkg/planner/core/optimizer_test.go:650
/// TestOptRuleListFlagAlignment`.
#[test]
fn opt_rule_list_and_flag_tables_stay_aligned_with_one_unique_bit_per_rule() {
    // require.Equal(len(optRuleList), len(optRuleFlags)) (:657-661).
    assert_eq!(
        OPT_RULE_LIST.len(),
        OPT_RULE_FLAGS.len(),
        "optRuleList length does not match optRuleFlags; \
         did you add a rule without a flag or vice versa?"
    );

    // Every flag nonzero, exactly one bit set, no duplicates (:663-673).
    let mut seen_flags: Vec<u64> = Vec::with_capacity(OPT_RULE_FLAGS.len());
    for (i, &flag) in OPT_RULE_FLAGS.iter().enumerate() {
        assert_ne!(flag, 0, "optRuleFlags[{i}] must not be zero");
        assert_eq!(
            flag & (flag - 1),
            0,
            "optRuleFlags[{i}] must contain exactly one bit"
        );
        assert!(
            !seen_flags.contains(&flag),
            "optRuleFlags[{i}] duplicates flag {flag}"
        );
        seen_flags.push(flag);
    }

    // Unique count == bits.Len64(FlagFullTextIndexResolveReject)
    // (:675-684). The highest mask in Go's namespace defines how many flags
    // exist; FULL_TEXT_INDEX_RESOLVE_REJECT carries that top bit here too.
    let top_flag = flags::FULL_TEXT_INDEX_RESOLVE_REJECT;
    let num_flags = (64 - top_flag.leading_zeros()) as usize;
    assert_eq!(
        seen_flags.len(),
        num_flags,
        "unique optRuleFlags count does not match Flag* count; \
         did you add a flag without mapping it to a rule or vice versa?"
    );
}
