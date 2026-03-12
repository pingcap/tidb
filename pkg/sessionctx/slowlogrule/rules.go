// Copyright 2025 PingCAP, Inc.
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

package slowlogrule

// SlowLogCondition defines a single condition within a slow log rule.
type SlowLogCondition struct {
	Field     string // Name of the slow log field to check (e.g., "Conn_ID", "Query_time").
	Threshold any    // Threshold value for triggering the condition.
}

// SlowLogRule represents a single slow log rule.
// A rule is triggered only if **all** of its Conditions are satisfied (logical AND).
type SlowLogRule struct {
	Conditions []SlowLogCondition // List of conditions combined with logical AND.
}

// SlowLogRules represents all slow log rules defined for the current scope (e.g., session/global).
// The rules are evaluated using logical OR between them: if any rule matches, it triggers the slow log.
type SlowLogRules struct {
	RawRules string              // Raw rule string before parsing.
	Fields   map[string]struct{} // All unique fields used in this rule set.
	Rules    []*SlowLogRule      // List of rules combined with logical OR.
}

// SessionSlowLogRules represents the slow log rules effective for a specific session.
// It embeds SlowLogRules and tracks additional session-level information.
type SessionSlowLogRules struct {
	*SlowLogRules
	EffectiveFields           map[string]struct{} // All unique fields visible to this session (session + global rules).
	GlobalRawRulesHash        uint64
	NeedUpdateEffectiveFields bool // NeedUpdateEffectiveFields indicates whether EffectiveFields needs to be updated.
}

// NewSessionSlowLogRules creates a new SessionSlowLogRules instance from the given SlowLogRules.
func NewSessionSlowLogRules(slRules *SlowLogRules) *SessionSlowLogRules {
	return &SessionSlowLogRules{
		SlowLogRules:              slRules,
		EffectiveFields:           make(map[string]struct{}),
		NeedUpdateEffectiveFields: true,
	}
}

// GlobalSlowLogRules represents all slow log rules defined at the global scope.
// It maintains a mapping from ConnID to a set of SlowLogRules:
//   - Key = specific ConnID: rules that only apply to the given connection.
//   - Key = UnsetConnID (-1): rules that apply globally to all sessions.
//
// Rule evaluation is logical OR across all matched rule sets.
type GlobalSlowLogRules struct {
	RawRules     string                  // Raw rule string before parsing.
	RawRulesHash uint64                  // Hash of RawRules for fast comparison.
	RulesMap     map[int64]*SlowLogRules // Mapping of ConnID → SlowLogRules.
}
