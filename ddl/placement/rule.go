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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"fmt"
	"regexp"
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
	GroupID     string       `json:"group_id"`
	ID          string       `json:"id"`
	Index       int          `json:"index,omitempty"`
	Override    bool         `json:"override,omitempty"`
	StartKeyHex string       `json:"start_key"`
	EndKeyHex   string       `json:"end_key"`
	Role        PeerRoleType `json:"role"`
	Count       int          `json:"count"`
	Constraints Constraints  `json:"label_constraints,omitempty"`
}

// TiFlashRule extends Rule with other necessary fields.
type TiFlashRule struct {
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

// NewRule constructs *Rule from role, count, and constraints. It is here to
// consistent the behavior of creating new rules.
func NewRule(role PeerRoleType, replicas uint64, cnst Constraints) *Rule {
	return &Rule{
		Role:        role,
		Count:       int(replicas),
		Constraints: cnst,
	}
}

var wrongSeparatorRegexp = regexp.MustCompile(`[^"':]+:\d`)

func getYamlMapFormatError(str string) error {
	if !strings.Contains(str, ":") {
		return ErrInvalidConstraintsMappingNoColonFound
	}
	if wrongSeparatorRegexp.MatchString(str) {
		return ErrInvalidConstraintsMappingWrongSeparator
	}
	return nil
}

// NewRules constructs []*Rule from a yaml-compatible representation of
// 'array' or 'dict' constraints.
// Refer to https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md.
func NewRules(role PeerRoleType, replicas uint64, cnstr string) ([]*Rule, error) {
	rules := []*Rule{}

	cnstbytes := []byte(cnstr)

	constraints1, err1 := NewConstraintsFromYaml(cnstbytes)
	if err1 == nil {
		rules = append(rules, NewRule(role, replicas, constraints1))
		return rules, nil
	}

	constraints2 := map[string]int{}
	err2 := yaml.UnmarshalStrict(cnstbytes, &constraints2)
	if err2 == nil {
		if replicas != 0 {
			return rules, fmt.Errorf("%w: should not specify replicas=%d when using dict syntax", ErrInvalidConstraintsRelicas, replicas)
		}

		for labels, cnt := range constraints2 {
			if cnt <= 0 {
				if err := getYamlMapFormatError(string(cnstbytes)); err != nil {
					return rules, err
				}
				return rules, fmt.Errorf("%w: count of labels '%s' should be positive, but got %d", ErrInvalidConstraintsMapcnt, labels, cnt)
			}
		}

		for labels, cnt := range constraints2 {
			labelConstraints, err := NewConstraints(strings.Split(labels, ","))
			if err != nil {
				return rules, err
			}

			rules = append(rules, NewRule(role, uint64(cnt), labelConstraints))
		}
		return rules, nil
	}

	return nil, fmt.Errorf("%w: should be [constraint1, ...] (error %s), {constraint1: cnt1, ...} (error %s), or any yaml compatible representation", ErrInvalidConstraintsFormat, err1, err2)
}

// Clone is used to duplicate a RuleOp for safe modification.
// Note that it is a shallow copy: Constraints is not cloned.
func (r *Rule) Clone() *Rule {
	n := &Rule{}
	*n = *r
	return n
}

func (r *Rule) String() string {
	return fmt.Sprintf("%+v", *r)
}
