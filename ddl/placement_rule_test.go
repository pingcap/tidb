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
	"github.com/pingcap/tidb/ddl/placement"
)

var _ = Suite(&testPlacementSuite{})

type testPlacementSuite struct {
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
	}
	for k, t := range tests {
		out, err := buildPlacementSpecs(t.input)
		c.Logf("test %d\n", k)
		if err == nil {
			for i := range t.output {
				c.Logf("\t%d-th output\n", i)
				found := false
				for j := range out {
					ok1, _ := DeepEquals.Check([]interface{}{out[j].Action, t.output[i].Action}, nil)
					ok2, _ := DeepEquals.Check([]interface{}{out[j].DeleteByIDPrefix, t.output[i].DeleteByIDPrefix}, nil)
					ok3, _ := DeepEquals.Check([]interface{}{out[j].Rule, t.output[i].Rule}, nil)
					if ok1 && ok2 && ok3 {
						found = true
						break
					}
				}
				if !found {
					c.Fatalf("\texcept %+v, but got %+v\n", t.output[i], out)
				}
			}
		} else {
			c.Assert(err.Error(), ErrorMatches, t.err)
		}
	}
}
