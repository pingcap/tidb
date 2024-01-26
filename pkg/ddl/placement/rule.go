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

	pd "github.com/tikv/pd/client/http"
	"gopkg.in/yaml.v2"
)

const (
	attributePrefix = "#"
	// AttributeEvictLeader is used to evict leader from a store.
	attributeEvictLeader = "evict-leader"
)

// RuleBuilder is used to build the Rules from a constraint string.
type RuleBuilder struct {
	role                        pd.PeerRoleType
	replicasNum                 uint64
	skipCheckReplicasConsistent bool
	constraintStr               string
}

// NewRuleBuilder creates a new RuleBuilder.
func NewRuleBuilder() *RuleBuilder {
	return &RuleBuilder{}
}

// SetRole sets the role of the rule.
func (b *RuleBuilder) SetRole(role pd.PeerRoleType) *RuleBuilder {
	b.role = role
	return b
}

// SetReplicasNum sets the replicas number in the rule.
func (b *RuleBuilder) SetReplicasNum(num uint64) *RuleBuilder {
	b.replicasNum = num
	return b
}

// SetSkipCheckReplicasConsistent sets the skipCheckReplicasConsistent flag.
func (b *RuleBuilder) SetSkipCheckReplicasConsistent(skip bool) *RuleBuilder {
	b.skipCheckReplicasConsistent = skip
	return b
}

// SetConstraintStr sets the constraint string.
func (b *RuleBuilder) SetConstraintStr(constraintStr string) *RuleBuilder {
	b.constraintStr = constraintStr
	return b
}

// BuildRulesWithDictConstraintsOnly constructs []*Rule from a yaml-compatible representation of
// 'dict' constraints.
func (b *RuleBuilder) BuildRulesWithDictConstraintsOnly() ([]*pd.Rule, error) {
	return newRulesWithDictConstraints(b.role, b.constraintStr)
}

// BuildRules constructs []*Rule from a yaml-compatible representation of
// 'array' or 'dict' constraints.
// Refer to https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md.
func (b *RuleBuilder) BuildRules() ([]*pd.Rule, error) {
	rules, err := newRules(b.role, b.replicasNum, b.constraintStr)
	// check if replicas is consistent
	if err == nil {
		if b.skipCheckReplicasConsistent {
			return rules, err
		}
		totalCnt := 0
		for _, rule := range rules {
			totalCnt += rule.Count
		}
		if b.replicasNum != 0 && b.replicasNum != uint64(totalCnt) {
			err = fmt.Errorf("%w: count of replicas in dict constrains is %d, but got %d", ErrInvalidConstraintsReplicas, totalCnt, b.replicasNum)
		}
	}
	return rules, err
}

// NewRule constructs *Rule from role, count, and constraints. It is here to
// consistent the behavior of creating new rules.
func NewRule(role pd.PeerRoleType, replicas uint64, cnst []pd.LabelConstraint) *pd.Rule {
	return &pd.Rule{
		Role:             role,
		Count:            int(replicas),
		LabelConstraints: cnst,
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

// newRules constructs []*Rule from a yaml-compatible representation of
// 'array' or 'dict' constraints.
// Refer to https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md.
func newRules(role pd.PeerRoleType, replicas uint64, cnstr string) (rules []*pd.Rule, err error) {
	cnstbytes := []byte(cnstr)
	constraints1, err1 := NewConstraintsFromYaml(cnstbytes)
	if err1 == nil {
		if replicas == 0 {
			if len(cnstr) > 0 {
				return nil, fmt.Errorf("%w: count of replicas should be positive, but got %d, constraint %s", ErrInvalidConstraintsReplicas, replicas, cnstr)
			}
			return nil, nil
		}
		rules = append(rules, NewRule(role, replicas, constraints1))
		err = err1
		return
	}
	// check if is dict constraints
	constraints2 := map[string]int{}
	if err2 := yaml.UnmarshalStrict(cnstbytes, &constraints2); err2 != nil {
		err = fmt.Errorf("%w: should be [constraint1, ...] (error %s), {constraint1: cnt1, ...} (error %s), or any yaml compatible representation", ErrInvalidConstraintsFormat, err1, err2)
		return
	}

	return newRulesWithDictConstraints(role, cnstr)
}

// newRulesWithDictConstraints constructs []*Rule from a yaml-compatible representation of
// 'dict' constraints.
func newRulesWithDictConstraints(role pd.PeerRoleType, cnstr string) ([]*pd.Rule, error) {
	rules := []*pd.Rule{}
	cnstbytes := []byte(cnstr)
	constraints2 := map[string]int{}
	err2 := yaml.UnmarshalStrict(cnstbytes, &constraints2)
	if err2 == nil {
		for labels, cnt := range constraints2 {
			if cnt <= 0 {
				if err := getYamlMapFormatError(string(cnstbytes)); err != nil {
					return rules, err
				}
				return rules, fmt.Errorf("%w: count of labels '%s' should be positive, but got %d", ErrInvalidConstraintsMapcnt, labels, cnt)
			}
		}

		for labels, cnt := range constraints2 {
			lbs, overrideRole, err := preCheckDictConstraintStr(labels, role)
			if err != nil {
				return rules, err
			}
			labelConstraints, err := NewConstraints(lbs)
			if err != nil {
				return rules, err
			}
			if cnt == 0 {
				return nil, fmt.Errorf("%w: count of replicas should be positive, but got %d", ErrInvalidConstraintsReplicas, cnt)
			}
			rules = append(rules, NewRule(overrideRole, uint64(cnt), labelConstraints))
		}
		return rules, nil
	}

	return nil, fmt.Errorf("%w: should be [constraint1, ...] or {constraint1: cnt1, ...}, error %s, or any yaml compatible representation", ErrInvalidConstraintsFormat, err2)
}
