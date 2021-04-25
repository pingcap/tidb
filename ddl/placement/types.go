// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"encoding/json"
)

// Refer to https://github.com/tikv/pd/issues/2701 .
// IMO, it is indeed not bad to have a copy of definition.
// After all, placement rules are communicated using an HTTP API. Loose
//  coupling is a good feature.

// PeerRoleType is the expected peer type of the placement rule.
type PeerRoleType string

const (
	// Voter can either match a leader peer or follower peer.
	Voter PeerRoleType = "voter"
	// Leader matches a leader.
	Leader PeerRoleType = "leader"
	// Follower matches a follower.
	Follower PeerRoleType = "follower"
	// Learner matches a learner.
	Learner PeerRoleType = "learner"
)

// Rule is the placement rule. Check https://github.com/tikv/pd/blob/master/server/schedule/placement/rule.go.
type Rule struct {
	GroupID          string       `json:"group_id"`
	ID               string       `json:"id"`
	Index            int          `json:"index,omitempty"`
	Override         bool         `json:"override,omitempty"`
	StartKeyHex      string       `json:"start_key"`
	EndKeyHex        string       `json:"end_key"`
	Role             PeerRoleType `json:"role"`
	Count            int          `json:"count"`
	LabelConstraints Constraints  `json:"label_constraints,omitempty"`
	LocationLabels   []string     `json:"location_labels,omitempty"`
	IsolationLevel   string       `json:"isolation_level,omitempty"`
}

// Clone is used to duplicate a RuleOp for safe modification.
func (r *Rule) Clone() *Rule {
	n := &Rule{}
	*n = *r
	return n
}

// Bundle is a group of all rules and configurations. It is used to support rule cache.
type Bundle struct {
	ID       string  `json:"group_id"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
	Rules    []*Rule `json:"rules"`
}

func (b *Bundle) String() string {
	t, err := json.Marshal(b)
	if err != nil {
		return ""
	}
	return string(t)
}

// Clone is used to duplicate a bundle.
func (b *Bundle) Clone() *Bundle {
	newBundle := &Bundle{}
	*newBundle = *b
	if len(b.Rules) > 0 {
		newBundle.Rules = make([]*Rule, 0, len(b.Rules))
		for i := range b.Rules {
			newBundle.Rules = append(newBundle.Rules, b.Rules[i].Clone())
		}
	}
	return newBundle
}

// IsEmpty is used to check if a bundle is empty.
func (b *Bundle) IsEmpty() bool {
	return len(b.Rules) == 0 && b.Index == 0 && !b.Override
}

// RuleOpType indicates the operation type.
type RuleOpType string

const (
	// RuleOpAdd a placement rule, only need to specify the field *Rule.
	RuleOpAdd RuleOpType = "add"
	// RuleOpDel a placement rule, only need to specify the field `GroupID`, `ID`, `MatchID`.
	RuleOpDel RuleOpType = "del"
)

// RuleOp is for batching placement rule actions.
type RuleOp struct {
	*Rule
	Action           RuleOpType `json:"action"`
	DeleteByIDPrefix bool       `json:"delete_by_id_prefix"`
}

// Clone is used to clone a RuleOp that is safe to modify, without affecting the old RuleOp.
func (op *RuleOp) Clone() *RuleOp {
	newOp := &RuleOp{}
	*newOp = *op
	newOp.Rule = &Rule{}
	*newOp.Rule = *op.Rule
	return newOp
}

func (op *RuleOp) String() string {
	b, err := json.Marshal(op)
	if err != nil {
		return ""
	}
	return string(b)
}
