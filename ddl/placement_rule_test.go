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
	"encoding/hex"
	"encoding/json"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var _ = Suite(&testPlacementSuite{})

type testPlacementSuite struct {
}

func (s *testPlacementSuite) TestPlacementBuild(c *C) {
	tests := []struct {
		input  []*ast.PlacementSpec
		bundle *placement.Bundle
		output []*placement.Rule
		err    string
	}{
		{
			input:  []*ast.PlacementSpec{},
			output: []*placement.Rule{},
		},

		{
			input: []*ast.PlacementSpec{{
				Role:        ast.PlacementRoleVoter,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["+  zone=sh", "-zone = bj"]`,
			}},
			output: []*placement.Rule{
				{
					Role:  placement.Voter,
					Count: 3,
					LabelConstraints: []placement.LabelConstraint{
						{Key: "zone", Op: "in", Values: []string{"sh"}},
						{Key: "zone", Op: "notIn", Values: []string{"bj"}},
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
					Role:        ast.PlacementRoleFollower,
					Tp:          ast.PlacementAdd,
					Replicas:    2,
					Constraints: `["-  zone=sh", "+zone = bj"]`,
				},
			},
			output: []*placement.Rule{
				{
					Role:  placement.Voter,
					Count: 3,
					LabelConstraints: []placement.LabelConstraint{
						{Key: "zone", Op: "in", Values: []string{"sh"}},
						{Key: "zone", Op: "notIn", Values: []string{"bj"}},
					},
				},
				{
					Role:  placement.Follower,
					Count: 2,
					LabelConstraints: []placement.LabelConstraint{
						{Key: "zone", Op: "notIn", Values: []string{"sh"}},
						{Key: "zone", Op: "in", Values: []string{"bj"}},
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
			output: []*placement.Rule{
				{
					Role:  placement.Voter,
					Count: 2,
					LabelConstraints: []placement.LabelConstraint{
						{Key: "zone", Op: "notIn", Values: []string{"sh"}},
						{Key: "zone", Op: "in", Values: []string{"bj"}},
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
			output: []*placement.Rule{
				{
					Role:             placement.Voter,
					Count:            1,
					LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "notIn", Values: []string{"sh"}}},
				},
				{
					Role:             placement.Voter,
					Count:            1,
					LabelConstraints: []placement.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"bj"}}},
				},
				{
					Role:  placement.Voter,
					Count: 1,
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
			output: []*placement.Rule{},
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
			bundle: &placement.Bundle{Rules: []*placement.Rule{
				{Role: placement.Learner},
				{Role: placement.Voter},
				{Role: placement.Learner},
				{Role: placement.Voter},
			}},
			output: []*placement.Rule{},
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
			output: []*placement.Rule{
				{
					Role:  placement.Learner,
					Count: 3,
					LabelConstraints: []placement.LabelConstraint{
						{Key: "zone", Op: "in", Values: []string{"sh"}},
						{Key: "zone", Op: "notIn", Values: []string{"bj"}},
					},
				},
			},
		},
	}
	for i, t := range tests {
		var bundle *placement.Bundle
		if t.bundle == nil {
			bundle = &placement.Bundle{Rules: []*placement.Rule{}}
		} else {
			bundle = t.bundle
		}
		out, err := buildPlacementSpecs(bundle, t.input)
		if err == nil {
			expected, err := json.Marshal(t.output)
			c.Assert(err, IsNil)
			got, err := json.Marshal(out.Rules)
			c.Assert(err, IsNil)
			c.Assert(len(t.output), Equals, len(out.Rules))
			for _, r1 := range t.output {
				found := false
				for _, r2 := range out.Rules {
					if ok, _ := DeepEquals.Check([]interface{}{r1, r2}, nil); ok {
						found = true
						break
					}
				}
				c.Assert(found, IsTrue, Commentf("%d test\nexpected %s\nbut got %s", i, expected, got))
			}
		} else {
			c.Assert(err.Error(), ErrorMatches, t.err)
		}
	}
}

func (s *testPlacementSuite) TestPlacementBuildDrop(c *C) {
	tests := []struct {
		input  int64
		output *placement.Bundle
	}{
		{
			input:  2,
			output: &placement.Bundle{ID: placement.GroupID(2)},
		},
		{
			input:  1,
			output: &placement.Bundle{ID: placement.GroupID(1)},
		},
	}
	for _, t := range tests {
		out := buildPlacementDropBundle(t.input)
		c.Assert(t.output, DeepEquals, out)
	}
}

func (s *testPlacementSuite) TestPlacementBuildTruncate(c *C) {
	bundle := &placement.Bundle{
		ID:    placement.GroupID(-1),
		Rules: []*placement.Rule{{GroupID: placement.GroupID(-1)}},
	}

	tests := []struct {
		input  int64
		output *placement.Bundle
	}{
		{
			input: 1,
			output: &placement.Bundle{
				ID: placement.GroupID(1),
				Rules: []*placement.Rule{{
					GroupID:     placement.GroupID(1),
					StartKeyHex: hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(1))),
					EndKeyHex:   hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2))),
				}},
			},
		},
		{
			input: 2,
			output: &placement.Bundle{
				ID: placement.GroupID(2),
				Rules: []*placement.Rule{{
					GroupID:     placement.GroupID(2),
					StartKeyHex: hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2))),
					EndKeyHex:   hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3))),
				}},
			},
		},
	}
	for _, t := range tests {
		out := buildPlacementTruncateBundle(bundle, t.input)
		c.Assert(t.output, DeepEquals, out)
		c.Assert(bundle.ID, Equals, placement.GroupID(-1))
		c.Assert(bundle.Rules, HasLen, 1)
		c.Assert(bundle.Rules[0].GroupID, Equals, placement.GroupID(-1))
	}
}
