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

package ddl

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl/placement"
)

var _ = Suite(&testPlacementSuite{})

type testPlacementSuite struct {
}

func (s *testPlacementSuite) compareRuleOp(n, o *placement.RuleOp) bool {
	ok1, _ := DeepEquals.Check([]interface{}{n.Action, o.Action}, nil)
	ok2, _ := DeepEquals.Check([]interface{}{n.DeleteByIDPrefix, o.DeleteByIDPrefix}, nil)
	ok3, _ := DeepEquals.Check([]interface{}{n.Rule, o.Rule}, nil)
	return ok1 && ok2 && ok3
}

func (s *testPlacementSuite) TestPlacementBuild(c *C) {
	tests := []struct {
		input  []*ast.PlacementSpec
		output []*placement.RuleOp
		err    string
	}{
		{
			input:  []*ast.PlacementSpec{},
			output: []*placement.RuleOp{},
		},

		{
			input: []*ast.PlacementSpec{{
				Role:        ast.PlacementRoleVoter,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["+  zone=sh", "-zone = bj"]`,
			}},
			output: []*placement.RuleOp{
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Voter,
						Override: true,
						Count:    3,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				}},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleFollower,
					Tp:          ast.PlacementAdd,
					Replicas:    2,
					Constraints: `["-  zone=sh", "+zone = bj"]`,
				},
			},
			output: []*placement.RuleOp{
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Voter,
						Override: true,
						Count:    3,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Follower,
						Override: true,
						Count:    2,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "notIn", Values: []string{"sh"}},
							{Key: "zone", Op: "in", Values: []string{"bj"}},
						},
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAlter,
					Replicas:    2,
					Constraints: `["-  zone=sh", "+zone = bj"]`,
				},
			},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Voter,
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Voter,
						Override: true,
						Count:    2,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "notIn", Values: []string{"sh"}},
							{Key: "zone", Op: "in", Values: []string{"bj"}},
						},
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAlter,
					Replicas:    3,
					Constraints: `{"-  zone=sh":1, "+zone = bj":1}`,
				},
			},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Voter,
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:          placement.RuleDefaultGroupID,
						Role:             placement.Voter,
						Override:         true,
						Count:            1,
						LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"bj"}}},
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:          placement.RuleDefaultGroupID,
						Role:             placement.Voter,
						Override:         true,
						Count:            1,
						LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "notIn", Values: []string{"sh"}}},
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Voter,
						Override: true,
						Count:    1,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Voter,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role: ast.PlacementRoleLearner,
					Tp:   ast.PlacementDrop,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Voter,
					},
				},
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Learner,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleLearner,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						Role:    placement.Voter,
					},
				},
				{
					Action: placement.RuleOpAdd,
					Rule: &placement.Rule{
						GroupID:  placement.RuleDefaultGroupID,
						Role:     placement.Learner,
						Override: true,
						Count:    3,
						LabelConstraints: []placement.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				},
			},
		},
	}
	for k, t := range tests {
		out, err := buildPlacementSpecs(t.input)
		if err == nil {
			for i := range t.output {
				found := false
				for j := range out {
					if s.compareRuleOp(out[j], t.output[i]) {
						found = true
						break
					}
				}
				if !found {
					c.Logf("test %d, %d-th output", k, i)
					c.Logf("\texcept %+v\n\tbut got", t.output[i])
					for j := range out {
						c.Logf("\t%+v", out[j])
					}
					c.Fail()
				}
			}
		} else {
			c.Assert(err.Error(), ErrorMatches, t.err)
		}
	}
}

func (s *testPlacementSuite) TestPlacementBuildDrop(c *C) {
	tests := []struct {
		input  []int64
		output []*placement.RuleOp
	}{
		{
			input: []int64{2},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						ID:      "0_t0_p2",
					},
				},
			},
		},
		{
			input: []int64{1, 2},
			output: []*placement.RuleOp{
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						ID:      "0_t0_p1",
					},
				},
				{
					Action:           placement.RuleOpDel,
					DeleteByIDPrefix: true,
					Rule: &placement.Rule{
						GroupID: placement.RuleDefaultGroupID,
						ID:      "0_t0_p2",
					},
				},
			},
		},
	}
	for _, t := range tests {
		out := buildPlacementDropRules(0, 0, t.input)
		c.Assert(len(out), Equals, len(t.output))
		for i := range t.output {
			c.Assert(s.compareRuleOp(out[i], t.output[i]), IsTrue, Commentf("except: %+v, obtained: %+v", t.output[i], out[i]))
		}
	}
}

func (s *testPlacementSuite) TestPlacementBuildTruncate(c *C) {
	rules := []*placement.RuleOp{
		{Rule: &placement.Rule{ID: "0_t0_p1"}},
		{Rule: &placement.Rule{ID: "0_t0_p94"}},
		{Rule: &placement.Rule{ID: "0_t0_p48"}},
	}

	tests := []struct {
		input  []int64
		output []*placement.RuleOp
	}{
		{
			input: []int64{1},
			output: []*placement.RuleOp{
				{Action: placement.RuleOpDel, Rule: &placement.Rule{GroupID: placement.RuleDefaultGroupID, ID: "0_t0_p1"}},
				{Action: placement.RuleOpAdd, Rule: &placement.Rule{ID: "0_t0_p2__0_0"}},
			},
		},
		{
			input: []int64{94, 48},
			output: []*placement.RuleOp{
				{Action: placement.RuleOpDel, Rule: &placement.Rule{GroupID: placement.RuleDefaultGroupID, ID: "0_t0_p94"}},
				{Action: placement.RuleOpAdd, Rule: &placement.Rule{ID: "0_t0_p95__0_0"}},
				{Action: placement.RuleOpDel, Rule: &placement.Rule{GroupID: placement.RuleDefaultGroupID, ID: "0_t0_p48"}},
				{Action: placement.RuleOpAdd, Rule: &placement.Rule{ID: "0_t0_p49__0_1"}},
			},
		},
	}
	for _, t := range tests {
		copyRules := make([]*placement.RuleOp, len(rules))
		for _, rule := range rules {
			copyRules = append(copyRules, rule.Clone())
		}

		newPartitions := make([]model.PartitionDefinition, 0, len(t.input))
		for _, j := range t.input {
			newPartitions = append(newPartitions, model.PartitionDefinition{ID: j + 1})
		}

		out := buildPlacementTruncateRules(rules, 0, 0, 0, t.input, newPartitions)

		c.Assert(len(out), Equals, len(t.output))
		for i := range t.output {
			c.Assert(s.compareRuleOp(out[i], t.output[i]), IsTrue, Commentf("except: %+v, obtained: %+v", t.output[i], out[i]))
		}
	}
}
