// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

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

// Rule is the core placement rule struct. Check https://github.com/tikv/pd/blob/master/server/schedule/placement/rule.go.
type Rule struct {
	GroupID        string       `json:"group_id"`
	ID             string       `json:"id"`
	Index          int          `json:"index,omitempty"`
	Override       bool         `json:"override,omitempty"`
	StartKeyHex    string       `json:"start_key"`
	EndKeyHex      string       `json:"end_key"`
	Role           PeerRoleType `json:"role"`
	Count          int          `json:"count"`
	Constraints    Constraints  `json:"label_constraints,omitempty"`
	LocationLabels []string     `json:"location_labels,omitempty"`
	IsolationLevel string       `json:"isolation_level,omitempty"`
}

// NewRules constructs []*Rule from a yaml-compatible representation of
// array or map of constraints. It converts 'CONSTRAINTS' field in RFC
// https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md to structs.
func NewRules(replicas uint64, cnstr string) ([]*Rule, error) {
	rules := []*Rule{}

	cnstbytes := []byte(cnstr)

	constraints1 := []string{}
	err1 := yaml.UnmarshalStrict(cnstbytes, &constraints1)
	if err1 == nil {
		// can not emit REPLICAS with an array or empty label
		if replicas == 0 {
			return rules, fmt.Errorf("%w: should be positive", ErrInvalidConstraintsRelicas)
		}

		labelConstraints, err := NewConstraints(constraints1)
		if err != nil {
			return rules, err
		}

		rules = append(rules, &Rule{
			Count:       int(replicas),
			Constraints: labelConstraints,
		})

		return rules, nil
	}

	constraints2 := map[string]int{}
	err2 := yaml.UnmarshalStrict(cnstbytes, &constraints2)
	if err2 == nil {
		ruleCnt := 0
		for labels, cnt := range constraints2 {
			if cnt <= 0 {
				return rules, fmt.Errorf("%w: count of labels '%s' should be positive, but got %d", ErrInvalidConstraintsMapcnt, labels, cnt)
			}
			ruleCnt += cnt
		}

		if replicas == 0 {
			replicas = uint64(ruleCnt)
		}

		if int(replicas) < ruleCnt {
			return rules, fmt.Errorf("%w: should be larger or equal to the number of total replicas, but REPLICAS=%d < total=%d", ErrInvalidConstraintsRelicas, replicas, ruleCnt)
		}

		for labels, cnt := range constraints2 {
			labelConstraints, err := NewConstraints(strings.Split(labels, ","))
			if err != nil {
				return rules, err
			}

			rules = append(rules, &Rule{
				Count:       cnt,
				Constraints: labelConstraints,
			})
		}

		remain := int(replicas) - ruleCnt
		if remain > 0 {
			rules = append(rules, &Rule{
				Count: remain,
			})
		}

		return rules, nil
	}

	return nil, fmt.Errorf("%w: should be [constraint1, ...] (error %s), {constraint1: cnt1, ...} (error %s), or any yaml compatible representation", ErrInvalidConstraintsFormat, err1, err2)
}

// Clone is used to duplicate a RuleOp for safe modification.
// Note that it is a shallow copy: LocationLabels and Constraints
// is not cloned.
func (r *Rule) Clone() *Rule {
	n := &Rule{}
	*n = *r
	return n
}
