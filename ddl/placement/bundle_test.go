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
	"encoding/hex"
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

var _ = Suite(&testBundleSuite{})

type testBundleSuite struct{}

func (s *testBundleSuite) TestEmpty(c *C) {
	bundle := &Bundle{ID: GroupID(1)}
	c.Assert(bundle.IsEmpty(), IsTrue)

	bundle = &Bundle{ID: GroupID(1), Index: 1}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Override: true}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}
	c.Assert(bundle.IsEmpty(), IsFalse)

	bundle = &Bundle{ID: GroupID(1), Index: 1, Override: true}
	c.Assert(bundle.IsEmpty(), IsFalse)
}

func (s *testBundleSuite) TestClone(c *C) {
	bundle := &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}

	newBundle := bundle.Clone()
	newBundle.ID = GroupID(2)
	newBundle.Rules[0] = &Rule{ID: "121"}

	c.Assert(bundle, DeepEquals, &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}})
	c.Assert(newBundle, DeepEquals, &Bundle{ID: GroupID(2), Rules: []*Rule{{ID: "121"}}})
}

func (s *testBundleSuite) TestObjectID(c *C) {
	type TestCase struct {
		name       string
		bundleID   string
		expectedID int64
		err        error
	}
	tests := []TestCase{
		{"non tidb bundle", "pd", 0, ErrInvalidBundleIDFormat},
		{"id of words", "TiDB_DDL_foo", 0, ErrInvalidBundleID},
		{"id of words and nums", "TiDB_DDL_3x", 0, ErrInvalidBundleID},
		{"id of floats", "TiDB_DDL_3.0", 0, ErrInvalidBundleID},
		{"id of negatives", "TiDB_DDL_-10", 0, ErrInvalidBundleID},
		{"id of positive integer", "TiDB_DDL_10", 10, nil},
	}
	for _, t := range tests {
		comment := Commentf("%s", t.name)
		bundle := Bundle{ID: t.bundleID}
		id, err := bundle.ObjectID()
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			c.Assert(id, Equals, t.expectedID, comment)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}

func (s *testBundleSuite) TestGetLeaderDCByBundle(c *C) {
	testcases := []struct {
		name       string
		bundle     *Bundle
		expectedDC string
	}{
		{
			name: "only leader",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "12",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "bj",
		},
		{
			name: "no leader",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "12",
						Role: Voter,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
						},
						Count: 3,
					},
				},
			},
			expectedDC: "",
		},
		{
			name: "voter and leader",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "11",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
					{
						ID:   "12",
						Role: Voter,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
						},
						Count: 3,
					},
				},
			},
			expectedDC: "sh",
		},
		{
			name: "wrong label key",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "11",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "fake",
								Op:     In,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "",
		},
		{
			name: "wrong operator",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "11",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     NotIn,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "",
		},
		{
			name: "leader have multi values",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "11",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh", "bj"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "",
		},
		{
			name: "irrelvant rules",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "15",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
							},
						},
						Count: 1,
					},
					{
						ID:   "14",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "disk",
								Op:     NotIn,
								Values: []string{"ssd", "hdd"},
							},
						},
						Count: 1,
					},
					{
						ID:   "13",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "bj",
		},
		{
			name: "multi leaders 1",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "16",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh"},
							},
						},
						Count: 2,
					},
				},
			},
			expectedDC: "",
		},
		{
			name: "multi leaders 2",
			bundle: &Bundle{
				ID: GroupID(1),
				Rules: []*Rule{
					{
						ID:   "17",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
					{
						ID:   "18",
						Role: Leader,
						Constraints: Constraints{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "sh",
		},
	}
	for _, testcase := range testcases {
		comment := Commentf("%s", testcase.name)
		result, ok := testcase.bundle.GetLeaderDC("zone")
		if len(testcase.expectedDC) > 0 {
			c.Assert(ok, Equals, true, comment)
		} else {
			c.Assert(ok, Equals, false, comment)
		}
		c.Assert(result, Equals, testcase.expectedDC, comment)
	}
}

func (s *testBundleSuite) TestApplyPlacmentSpec(c *C) {
	type TestCase struct {
		name   string
		input  []*ast.PlacementSpec
		output []*Rule
		err    error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		name:   "empty",
		input:  []*ast.PlacementSpec{},
		output: []*Rule{},
	})

	rules, err := NewRules(3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	rules[0].Role = Voter
	tests = append(tests, TestCase{
		name: "add voter array",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleVoter,
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: `["+zone=sh", "+zone=sh"]`,
		}},
		output: rules,
	})

	rules, err = NewRules(3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	rules[0].Role = Learner
	tests = append(tests, TestCase{
		name: "add learner array",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleLearner,
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: `["+zone=sh", "+zone=sh"]`,
		}},
		output: rules,
	})

	rules, err = NewRules(3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	rules[0].Role = Follower
	tests = append(tests, TestCase{
		name: "add follower array",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleFollower,
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: `["+zone=sh", "+zone=sh"]`,
		}},
		output: rules,
	})

	tests = append(tests, TestCase{
		name: "add invalid constraints",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleVoter,
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: "ne",
		}},
		err: ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name: "add empty role",
		input: []*ast.PlacementSpec{{
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: "",
		}},
		err: ErrMissingRoleField,
	})

	tests = append(tests, TestCase{
		name: "add multiple leaders",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleLeader,
			Tp:          ast.PlacementAdd,
			Replicas:    3,
			Constraints: "",
		}},
		err: ErrLeaderReplicasMustOne,
	})

	rules, err = NewRules(1, "")
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	rules[0].Role = Leader
	tests = append(tests, TestCase{
		name: "omit leader field",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleLeader,
			Tp:          ast.PlacementAdd,
			Constraints: "",
		}},
		output: rules,
	})

	rules, err = NewRules(3, `["-zone=sh","+zone=bj"]`)
	c.Assert(err, IsNil)
	c.Assert(rules, HasLen, 1)
	rules[0].Role = Follower
	tests = append(tests, TestCase{
		name: "drop",
		input: []*ast.PlacementSpec{
			{
				Role:        ast.PlacementRoleFollower,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["-  zone=sh", "+zone = bj"]`,
			},
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
		output: rules,
	})

	tests = append(tests, TestCase{
		name: "drop unexisted",
		input: []*ast.PlacementSpec{{
			Role:        ast.PlacementRoleLeader,
			Tp:          ast.PlacementDrop,
			Constraints: "",
		}},
		err: ErrNoRulesToDrop,
	})

	rules1, err := NewRules(3, `["-zone=sh","+zone=bj"]`)
	c.Assert(err, IsNil)
	c.Assert(rules1, HasLen, 1)
	rules1[0].Role = Follower
	rules2, err := NewRules(3, `["+zone=sh","-zone=bj"]`)
	c.Assert(err, IsNil)
	c.Assert(rules2, HasLen, 1)
	rules2[0].Role = Voter
	tests = append(tests, TestCase{
		name: "alter",
		input: []*ast.PlacementSpec{
			{
				Role:        ast.PlacementRoleFollower,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["-  zone=sh", "+zone = bj"]`,
			},
			{
				Role:        ast.PlacementRoleVoter,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["-  zone=sh", "+zone = bj"]`,
			},
			{
				Role:        ast.PlacementRoleVoter,
				Tp:          ast.PlacementAlter,
				Replicas:    3,
				Constraints: `["+  zone=sh", "-zone = bj"]`,
			},
		},
		output: append(rules1, rules2...),
	})

	for _, t := range tests {
		comment := Commentf("%s", t.name)
		bundle := &Bundle{}
		err := bundle.ApplyPlacementSpec(t.input)
		if t.err == nil {
			c.Assert(err, IsNil)
			matchRules(t.output, bundle.Rules, comment.CheckCommentString(), c)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}

func (s *testBundleSuite) TestString(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules1, err := NewRules(3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	rules2, err := NewRules(4, `["-zone=sh", "+zone=bj"]`)
	c.Assert(err, IsNil)
	bundle.Rules = append(rules1, rules2...)

	c.Assert(bundle.String(), Equals, `{"group_id":"TiDB_DDL_1","group_index":0,"group_override":false,"rules":[{"group_id":"","id":"","start_key":"","end_key":"","role":"","count":3,"label_constraints":[{"key":"zone","op":"in","values":["sh"]}]},{"group_id":"","id":"","start_key":"","end_key":"","role":"","count":4,"label_constraints":[{"key":"zone","op":"notIn","values":["sh"]},{"key":"zone","op":"in","values":["bj"]}]}]}`)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure"), IsNil)
	}()
	c.Assert(bundle.String(), Equals, "")
}

func (s *testBundleSuite) TestNew(c *C) {
	c.Assert(NewBundle(3), DeepEquals, &Bundle{ID: GroupID(3)})
	c.Assert(NewBundle(-1), DeepEquals, &Bundle{ID: GroupID(-1)})
}

func (s *testBundleSuite) TestReset(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules, err := NewRules(3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	bundle.Rules = rules

	bundle.Reset(3)
	c.Assert(bundle.ID, Equals, GroupID(3))
	c.Assert(bundle.Rules, HasLen, 1)
	c.Assert(bundle.Rules[0].GroupID, Equals, bundle.ID)

	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(3)))
	c.Assert(bundle.Rules[0].StartKeyHex, Equals, startKey)

	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(4)))
	c.Assert(bundle.Rules[0].EndKeyHex, Equals, endKey)
}

func (s *testBundleSuite) TestTidy(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules0, err := NewRules(1, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	c.Assert(rules0, HasLen, 1)
	rules0[0].Count = 0
	rules1, err := NewRules(4, `["-zone=sh", "+zone=bj"]`)
	c.Assert(err, IsNil)
	c.Assert(rules1, HasLen, 1)
	rules2, err := NewRules(4, `["-zone=sh", "+zone=bj"]`)
	c.Assert(err, IsNil)
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules1...)
	bundle.Rules = append(bundle.Rules, rules2...)

	err = bundle.Tidy()
	c.Assert(err, IsNil)
	c.Assert(bundle.Rules, HasLen, 2)
	c.Assert(bundle.Rules[0].ID, Equals, "1")
	c.Assert(bundle.Rules[0].Constraints, HasLen, 3)
	c.Assert(bundle.Rules[0].Constraints[2], DeepEquals, Constraint{
		Op:     NotIn,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	})
	c.Assert(bundle.Rules[1].ID, Equals, "2")

	// merge
	rules3, err := NewRules(4, "")
	c.Assert(err, IsNil)
	c.Assert(rules3, HasLen, 1)
	rules3[0].Role = Follower

	rules4, err := NewRules(5, "")
	c.Assert(err, IsNil)
	c.Assert(rules4, HasLen, 1)
	rules4[0].Role = Follower

	rules0[0].Role = Voter
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules3...)
	bundle.Rules = append(bundle.Rules, rules4...)

	chkfunc := func() {
		c.Assert(err, IsNil)
		c.Assert(bundle.Rules, HasLen, 3)
		c.Assert(bundle.Rules[0].ID, Equals, "0")
		c.Assert(bundle.Rules[1].ID, Equals, "1")
		c.Assert(bundle.Rules[2].ID, Equals, "follower")
		c.Assert(bundle.Rules[2].Count, Equals, 9)
		c.Assert(bundle.Rules[2].Constraints, DeepEquals, Constraints{
			{
				Op:     NotIn,
				Key:    EngineLabelKey,
				Values: []string{EngineLabelTiFlash},
			},
		})
	}
	err = bundle.Tidy()
	chkfunc()

	// tidy again
	// it should be stable
	err = bundle.Tidy()
	chkfunc()

	// tidy again
	// it should be stable
	bundle2 := bundle.Clone()
	err = bundle2.Tidy()
	c.Assert(err, IsNil)
	c.Assert(bundle2, DeepEquals, bundle)

	bundle.Rules[2].Constraints = append(bundle.Rules[2].Constraints, Constraint{
		Op:     In,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	})
	c.Log(bundle.Rules[2])
	err = bundle.Tidy()
	c.Assert(errors.Is(err, ErrConflictingConstraints), IsTrue)
}
