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
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func TestEmpty(t *testing.T) {
	bundle := &Bundle{ID: GroupID(1)}
	require.True(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Index: 1}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Override: true}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Index: 1, Override: true}
	require.False(t, bundle.IsEmpty())
}

func TestCloneBundle(t *testing.T) {
	bundle := &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}

	newBundle := bundle.Clone()
	newBundle.ID = GroupID(2)
	newBundle.Rules[0] = &Rule{ID: "121"}

	require.Equal(t, &Bundle{ID: GroupID(1), Rules: []*Rule{{ID: "434"}}}, bundle)
	require.Equal(t, &Bundle{ID: GroupID(2), Rules: []*Rule{{ID: "121"}}}, newBundle)
}

func TestObjectID(t *testing.T) {
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
	for _, test := range tests {
		bundle := Bundle{ID: test.bundleID}
		id, err := bundle.ObjectID()
		if test.err == nil {
			require.NoError(t, err, test.name)
			require.Equal(t, test.expectedID, id, test.name)
		} else {
			require.ErrorIs(t, err, test.err, test.name)
		}
	}
}

func TestGetLeaderDCByBundle(t *testing.T) {
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
		result, ok := testcase.bundle.GetLeaderDC("zone")
		if len(testcase.expectedDC) > 0 {
			require.True(t, ok, testcase.name)
		} else {
			require.False(t, ok, testcase.name)
		}
		require.Equal(t, testcase.expectedDC, result, testcase.name)
	}
}

func TestString(t *testing.T) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules1, err := NewRules(Voter, 3, `["+zone=sh", "+zone=sh"]`)
	require.NoError(t, err)
	rules2, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
	require.NoError(t, err)
	bundle.Rules = append(rules1, rules2...)

	require.Equal(t, "{\"group_id\":\"TiDB_DDL_1\",\"group_index\":0,\"group_override\":false,\"rules\":[{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"count\":3,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"sh\"]}]},{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"count\":4,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"notIn\",\"values\":[\"sh\"]},{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"bj\"]}]}]}", bundle.String())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/placement/MockMarshalFailure"))
	}()
	require.Equal(t, "", bundle.String())
}

func TestNewBundle(t *testing.T) {
	require.Equal(t, &Bundle{ID: GroupID(3)}, NewBundle(3))
	require.Equal(t, &Bundle{ID: GroupID(-1)}, NewBundle(-1))
	_, err := NewBundleFromConstraintsOptions(nil)
	require.Error(t, err)
	_, err = NewBundleFromSugarOptions(nil)
	require.Error(t, err)
	_, err = NewBundleFromOptions(nil)
	require.Error(t, err)
}

func TestNewBundleFromOptions(t *testing.T) {
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

	for _, test := range tests {
		bundle, err := newBundleFromOptions(test.input)
		comment := fmt.Sprintf("[%s]\nerr1 %s\nerr2 %s", test.name, err, test.err)
		if test.err != nil {
			require.ErrorIs(t, err, test.err, comment)
		} else {
			require.NoError(t, err, comment)
			matchRules(test.output, bundle.Rules, comment, t)
		}
	}
}

func TestResetBundleWithSingleRule(t *testing.T) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules, err := NewRules(Voter, 3, `["+zone=sh", "+zone=sh"]`)
	require.NoError(t, err)
	bundle.Rules = rules

	bundle.Reset(RuleIndexTable, []int64{3})
	require.Equal(t, GroupID(3), bundle.ID)
	require.Equal(t, true, bundle.Override)
	require.Equal(t, RuleIndexTable, bundle.Index)
	require.Len(t, bundle.Rules, 1)
	require.Equal(t, bundle.ID, bundle.Rules[0].GroupID)

	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	require.Equal(t, startKey, bundle.Rules[0].StartKeyHex)

	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	require.Equal(t, endKey, bundle.Rules[0].EndKeyHex)
}

func TestResetBundleWithMultiRules(t *testing.T) {
	// build a bundle with three rules.
	bundle, err := NewBundleFromOptions(&model.PlacementSettings{
		LeaderConstraints:   `["+zone=bj"]`,
		Followers:           2,
		FollowerConstraints: `["+zone=hz"]`,
		Learners:            1,
		LearnerConstraints:  `["+zone=cd"]`,
		Constraints:         `["+disk=ssd"]`,
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(bundle.Rules))

	// test if all the three rules are basic rules even the start key are not set.
	bundle.Reset(RuleIndexTable, []int64{1, 2, 3})
	require.Equal(t, GroupID(1), bundle.ID)
	require.Equal(t, RuleIndexTable, bundle.Index)
	require.Equal(t, true, bundle.Override)
	require.Equal(t, 3*3, len(bundle.Rules))
	// for id 1.
	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(1)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	require.Equal(t, startKey, bundle.Rules[0].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[0].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[1].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[1].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[2].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[2].EndKeyHex)
	// for id 2.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	require.Equal(t, startKey, bundle.Rules[3].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[3].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[4].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[4].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[5].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[5].EndKeyHex)
	// for id 3.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	require.Equal(t, startKey, bundle.Rules[6].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[6].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[7].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[7].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[8].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[8].EndKeyHex)

	// test if bundle has redundant rules.
	// for now, the bundle has 9 rules, each table id or partition id has the three with them.
	// once we reset this bundle for another ids, for example, adding partitions. we should
	// extend the basic rules(3 of them) to the new partition id.
	bundle.Reset(RuleIndexTable, []int64{1, 3, 4, 5})
	require.Equal(t, GroupID(1), bundle.ID)
	require.Equal(t, RuleIndexTable, bundle.Index)
	require.Equal(t, true, bundle.Override)
	require.Equal(t, 3*4, len(bundle.Rules))
	// for id 1.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(1)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(2)))
	require.Equal(t, startKey, bundle.Rules[0].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[0].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[1].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[1].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[2].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[2].EndKeyHex)
	// for id 3.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(3)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	require.Equal(t, startKey, bundle.Rules[3].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[3].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[4].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[4].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[5].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[5].EndKeyHex)
	// for id 4.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(4)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(5)))
	require.Equal(t, startKey, bundle.Rules[6].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[6].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[7].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[7].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[8].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[8].EndKeyHex)
	// for id 5.
	startKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(5)))
	endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(6)))
	require.Equal(t, startKey, bundle.Rules[9].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[9].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[10].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[10].EndKeyHex)
	require.Equal(t, startKey, bundle.Rules[11].StartKeyHex)
	require.Equal(t, endKey, bundle.Rules[11].EndKeyHex)
}

func TestTidy(t *testing.T) {
	bundle := &Bundle{
		ID: GroupID(1),
	}

	rules0, err := NewRules(Voter, 1, `["+zone=sh", "+zone=sh"]`)
	require.NoError(t, err)
	require.Len(t, rules0, 1)
	rules0[0].Count = 0 // test prune useless rules

	rules1, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
	require.NoError(t, err)
	require.Len(t, rules1, 1)
	rules2, err := NewRules(Voter, 4, `["-zone=sh", "+zone=bj"]`)
	require.NoError(t, err)
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules1...)
	bundle.Rules = append(bundle.Rules, rules2...)

	err = bundle.Tidy()
	require.NoError(t, err)
	require.Len(t, bundle.Rules, 2)
	require.Equal(t, "1", bundle.Rules[0].ID)
	require.Len(t, bundle.Rules[0].Constraints, 3)
	require.Equal(t, Constraint{
		Op:     NotIn,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	}, bundle.Rules[0].Constraints[2])
	require.Equal(t, "2", bundle.Rules[1].ID)

	// merge
	rules3, err := NewRules(Follower, 4, "")
	require.NoError(t, err)
	require.Len(t, rules3, 1)

	rules4, err := NewRules(Follower, 5, "")
	require.NoError(t, err)
	require.Len(t, rules4, 1)

	rules0[0].Role = Voter
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules3...)
	bundle.Rules = append(bundle.Rules, rules4...)

	chkfunc := func() {
		require.NoError(t, err)
		require.Len(t, bundle.Rules, 3)
		require.Equal(t, "0", bundle.Rules[0].ID)
		require.Equal(t, "1", bundle.Rules[1].ID)
		require.Equal(t, "follower", bundle.Rules[2].ID)
		require.Equal(t, 9, bundle.Rules[2].Count)
		require.Equal(t, Constraints{
			{
				Op:     NotIn,
				Key:    EngineLabelKey,
				Values: []string{EngineLabelTiFlash},
			},
		}, bundle.Rules[2].Constraints)
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
	require.NoError(t, err)
	require.Equal(t, bundle, bundle2)

	bundle.Rules[2].Constraints = append(bundle.Rules[2].Constraints, Constraint{
		Op:     In,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	})
	require.ErrorIs(t, bundle.Tidy(), ErrConflictingConstraints)
}
