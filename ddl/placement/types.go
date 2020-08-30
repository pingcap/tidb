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

// LabelConstraintOp defines how a LabelConstraint matches a store.
type LabelConstraintOp string

const (
	// In restricts the store label value should in the value list.
	// If label does not exist, `in` is always false.
	In LabelConstraintOp = "in"
	// NotIn restricts the store label value should not in the value list.
	// If label does not exist, `notIn` is always true.
	NotIn LabelConstraintOp = "notIn"
	// Exists restricts the store should have the label.
	Exists LabelConstraintOp = "exists"
	// NotExists restricts the store should not have the label.
	NotExists LabelConstraintOp = "notExists"
)

// LabelConstraint is used to filter store when trying to place peer of a region.
type LabelConstraint struct {
	Key    string            `json:"key,omitempty"`
	Op     LabelConstraintOp `json:"op,omitempty"`
	Values []string          `json:"values,omitempty"`
}

// Rule is the placement rule. Check https://github.com/tikv/pd/blob/master/server/schedule/placement/rule.go.
type Rule struct {
	GroupID          string            `json:"group_id"`
	ID               string            `json:"id"`
	Index            int               `json:"index,omitempty"`
	Override         bool              `json:"override,omitempty"`
	StartKey         []byte            `json:"-"`
	StartKeyHex      string            `json:"start_key"`
	EndKey           []byte            `json:"-"`
	EndKeyHex        string            `json:"end_key"`
	Role             PeerRoleType      `json:"role"`
	Count            int               `json:"count"`
	LabelConstraints []LabelConstraint `json:"label_constraints,omitempty"`
	LocationLabels   []string          `json:"location_labels,omitempty"`
	IsolationLevel   string            `json:"isolation_level,omitempty"`
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
