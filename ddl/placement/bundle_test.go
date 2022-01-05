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
	"encoding/hex"
	"errors"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/model"
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

func (s *testBundleSuite) TestString(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules1, err := NewRules(Voter, 3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	rules2, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
	c.Assert(err, IsNil)
	bundle.Rules = append(rules1, rules2...)

	c.Assert(bundle.String(), Equals, "{\"group_id\":\"TiDB_DDL_1\",\"group_index\":0,\"group_override\":false,\"rules\":[{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"count\":3,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"sh\"]}]},{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"count\":4,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"notIn\",\"values\":[\"sh\"]},{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"bj\"]}]}]}")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure"), IsNil)
	}()
	c.Assert(bundle.String(), Equals, "")
}

func (s *testBundleSuite) TestNew(c *C) {
	c.Assert(NewBundle(3), DeepEquals, &Bundle{ID: GroupID(3)})
	c.Assert(NewBundle(-1), DeepEquals, &Bundle{ID: GroupID(-1)})
	_, err := NewBundleFromConstraintsOptions(nil)
	c.Assert(err, NotNil)
	_, err = NewBundleFromSugarOptions(nil)
	c.Assert(err, NotNil)
	_, err = NewBundleFromOptions(nil)
	c.Assert(err, NotNil)
}

func (s *testBundleSuite) TestNewBundleFromOptions(c *C) {
	type TestCase struct {
		name   string
		input  *model.PlacementSettings
		output []*Rule
		err    error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		name:  "empty 1",
		input: &model.PlacementSettings{},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name:  "empty 2",
		input: nil,
		err:   ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "empty 3",
		input: &model.PlacementSettings{
			LearnerConstraints: "[+region=us]",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: normal case 1",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "us",
		},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: normal case 2",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "us",
			Schedule:      "majority_in_primary",
		},
		output: []*Rule{
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Follower, 1, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: few followers",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "bj,sh,us",
			Followers:     1,
		},
		output: []*Rule{
			NewRule(Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Follower, 1, NewConstraintsDirect(
				NewConstraintDirect("region", In, "bj", "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: omit regions 1",
		input: &model.PlacementSettings{
			Followers: 2,
			Schedule:  "even",
		},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: omit regions 2",
		input: &model.PlacementSettings{
			Followers: 2,
			Schedule:  "majority_in_primary",
		},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: wrong schedule prop",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "us",
			Schedule:      "wrong",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: invalid region name 1",
		input: &model.PlacementSettings{
			PrimaryRegion: ",=,",
			Regions:       ",=,",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: invalid region name 2",
		input: &model.PlacementSettings{
			PrimaryRegion: "f",
			Regions:       ",=",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: invalid region name 4",
		input: &model.PlacementSettings{
			PrimaryRegion: "",
			Regions:       "g",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: normal case 2",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "sh,us",
			Followers:     5,
		},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Follower, 3, NewConstraintsDirect(
				NewConstraintDirect("region", In, "sh"),
			)),
		},
	})
	tests = append(tests, tests[len(tests)-1])
	tests[len(tests)-1].name = "sugar syntax: explicit schedule"
	tests[len(tests)-1].input.Schedule = "even"

	tests = append(tests, TestCase{
		name: "sugar syntax: majority schedule",
		input: &model.PlacementSettings{
			PrimaryRegion: "sh",
			Regions:       "bj,sh",
			Followers:     4,
			Schedule:      "majority_in_primary",
		},
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("region", In, "sh"),
			)),
			NewRule(Follower, 2, NewConstraintsDirect(
				NewConstraintDirect("region", In, "bj"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: normal case 1",
		input: &model.PlacementSettings{
			Constraints: "[+region=us]",
		},
		output: []*Rule{
			NewRule(Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: normal case 3",
		input: &model.PlacementSettings{
			Constraints: "[+region=us]",
			Followers:   2,
			Learners:    2,
		},
		output: []*Rule{
			NewRule(Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
			NewRule(Learner, 2, NewConstraintsDirect(
				NewConstraintDirect("region", In, "us"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: lack count 1",
		input: &model.PlacementSettings{
			LeaderConstraints:   "[+region=as]",
			FollowerConstraints: "[-region=us]",
		},
		output: []*Rule{
			NewRule(Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", In, "as"))),
			NewRule(Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", NotIn, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: lack count 2",
		input: &model.PlacementSettings{
			LeaderConstraints:  "[+region=as]",
			LearnerConstraints: "[-region=us]",
		},
		err: ErrInvalidPlacementOptions,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: omit leader",
		input: &model.PlacementSettings{
			Followers:           2,
			FollowerConstraints: "[+region=bj]",
		},
		output: []*Rule{
			NewRule(Leader, 1, NewConstraintsDirect()),
			NewRule(Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", In, "bj"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: conflicts 1",
		input: &model.PlacementSettings{
			Constraints:       "[+region=us]",
			LeaderConstraints: "[-region=us]",
			Followers:         2,
		},
		err: ErrConflictingConstraints,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: conflicts 3",
		input: &model.PlacementSettings{
			Constraints:         "[+region=us]",
			FollowerConstraints: "[-region=us]",
			Followers:           2,
		},
		err: ErrConflictingConstraints,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: conflicts 4",
		input: &model.PlacementSettings{
			Constraints:        "[+region=us]",
			LearnerConstraints: "[-region=us]",
			Followers:          2,
			Learners:           2,
		},
		err: ErrConflictingConstraints,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: invalid format 1",
		input: &model.PlacementSettings{
			Constraints:       "[+region=us]",
			LeaderConstraints: "-region=us]",
			Followers:         2,
		},
		err: ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: invalid format 2",
		input: &model.PlacementSettings{
			Constraints:       "+region=us]",
			LeaderConstraints: "[-region=us]",
			Followers:         2,
		},
		err: ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: invalid format 4",
		input: &model.PlacementSettings{
			Constraints:         "[+region=us]",
			FollowerConstraints: "-region=us]",
			Followers:           2,
		},
		err: ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: invalid format 5",
		input: &model.PlacementSettings{
			Constraints:       "[+region=us]",
			LeaderConstraints: "-region=us]",
			Learners:          2,
			Followers:         2,
		},
		err: ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: learner dict constraints",
		input: &model.PlacementSettings{
			LearnerConstraints: `{"+region=us": 2}`,
		},
		output: []*Rule{
			NewRule(Leader, 1, NewConstraintsDirect()),
			NewRule(Voter, 2, NewConstraintsDirect()),
			NewRule(Learner, 2, NewConstraintsDirect(NewConstraintDirect("region", In, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: learner dict constraints, with count",
		input: &model.PlacementSettings{
			LearnerConstraints: `{"+region=us": 2}`,
			Learners:           4,
		},
		err: ErrInvalidConstraintsRelicas,
	})

	for _, t := range tests {
		bundle, err := newBundleFromOptions(t.input)
		comment := Commentf("[%s]\nerr1 %s\nerr2 %s", t.name, err, t.err)
		if t.err != nil {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		} else {
			c.Assert(err, IsNil, comment)
			matchRules(t.output, bundle.Rules, comment.CheckCommentString(), c)
		}
	}
}

func (s *testBundleSuite) TestResetBundleWithSingleRule(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules, err := NewRules(Voter, 3, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	bundle.Rules = rules

	bundle.Reset(RuleIndexTable, []int64{3})
	c.Assert(bundle.ID, Equals, GroupID(3))
	c.Assert(bundle.Override, Equals, true)
	c.Assert(bundle.Index, Equals, RuleIndexTable)
	c.Assert(bundle.Rules, HasLen, 1)
	c.Assert(bundle.Rules[0].GroupID, Equals, bundle.ID)

	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	c.Assert(bundle.Rules[0].StartKeyHex, Equals, startKey)

	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	c.Assert(bundle.Rules[0].EndKeyHex, Equals, endKey)
}

func (s *testBundleSuite) TestResetBundleWithMultiRules(c *C) {
	// build a bundle with three rules.
	bundle, err := NewBundleFromOptions(&model.PlacementSettings{
		LeaderConstraints:   `["+zone=bj"]`,
		Followers:           2,
		FollowerConstraints: `["+zone=hz"]`,
		Learners:            1,
		LearnerConstraints:  `["+zone=cd"]`,
		Constraints:         `["+disk=ssd"]`,
	})
	c.Assert(err, IsNil)
	c.Assert(len(bundle.Rules), Equals, 3)

	// test if all the three rules are basic rules even the start key are not set.
	bundle.Reset(RuleIndexTable, []int64{1, 2, 3})
	c.Assert(bundle.ID, Equals, GroupID(1))
	c.Assert(bundle.Index, Equals, RuleIndexTable)
	c.Assert(bundle.Override, Equals, true)
	c.Assert(len(bundle.Rules), Equals, 3*3)
	// for id 1.
	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(1)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	c.Assert(bundle.Rules[0].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[0].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[1].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[1].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[2].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[2].EndKeyHex, Equals, endKey)
	// for id 2.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	c.Assert(bundle.Rules[3].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[3].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[4].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[4].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[5].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[5].EndKeyHex, Equals, endKey)
	// for id 3.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	c.Assert(bundle.Rules[6].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[6].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[7].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[7].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[8].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[8].EndKeyHex, Equals, endKey)

	// test if bundle has redundant rules.
	// for now, the bundle has 9 rules, each table id or partition id has the three with them.
	// once we reset this bundle for another ids, for example, adding partitions. we should
	// extend the basic rules(3 of them) to the new partition id.
	bundle.Reset(RuleIndexTable, []int64{1, 3, 4, 5})
	c.Assert(bundle.ID, Equals, GroupID(1))
	c.Assert(bundle.Index, Equals, RuleIndexTable)
	c.Assert(bundle.Override, Equals, true)
	c.Assert(len(bundle.Rules), Equals, 3*4)
	// for id 1.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(1)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	c.Assert(bundle.Rules[0].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[0].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[1].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[1].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[2].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[2].EndKeyHex, Equals, endKey)
	// for id 3.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	c.Assert(bundle.Rules[3].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[3].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[4].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[4].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[5].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[5].EndKeyHex, Equals, endKey)
	// for id 4.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(5)))
	c.Assert(bundle.Rules[6].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[6].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[7].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[7].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[8].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[8].EndKeyHex, Equals, endKey)
	// for id 5.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(5)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(6)))
	c.Assert(bundle.Rules[9].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[9].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[10].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[10].EndKeyHex, Equals, endKey)
	c.Assert(bundle.Rules[11].StartKeyHex, Equals, startKey)
	c.Assert(bundle.Rules[11].EndKeyHex, Equals, endKey)
}

func (s *testBundleSuite) TestTidy(c *C) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules0, err := NewRules(Voter, 1, `["+zone=sh", "+zone=sh"]`)
	c.Assert(err, IsNil)
	c.Assert(rules0, HasLen, 1)
	rules0[0].Count = 0 // test prune useless rules

	rules1, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
	c.Assert(err, IsNil)
	c.Assert(rules1, HasLen, 1)
	rules2, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
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
	rules3, err := NewRules(Follower, 4, "")
	c.Assert(err, IsNil)
	c.Assert(rules3, HasLen, 1)

	rules4, err := NewRules(Follower, 5, "")
	c.Assert(err, IsNil)
	c.Assert(rules4, HasLen, 1)

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
