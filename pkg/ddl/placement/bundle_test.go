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
	"reflect"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
)

func TestEmpty(t *testing.T) {
	bundle := &Bundle{ID: GroupID(1)}
	require.True(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Index: 1}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Override: true}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Rules: []*pd.Rule{{ID: "434"}}}
	require.False(t, bundle.IsEmpty())

	bundle = &Bundle{ID: GroupID(1), Index: 1, Override: true}
	require.False(t, bundle.IsEmpty())
}

func TestCloneBundle(t *testing.T) {
	bundle := &Bundle{ID: GroupID(1), Rules: []*pd.Rule{{ID: "434"}}}

	newBundle := bundle.Clone()
	newBundle.ID = GroupID(2)
	newBundle.Rules[0] = &pd.Rule{ID: "121"}

	require.Equal(t, &Bundle{ID: GroupID(1), Rules: []*pd.Rule{{ID: "434"}}}, bundle)
	require.Equal(t, &Bundle{ID: GroupID(2), Rules: []*pd.Rule{{ID: "121"}}}, newBundle)
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
				Rules: []*pd.Rule{
					{
						ID:   "12",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "12",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "11",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
					{
						ID:   "12",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "11",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "fake",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "11",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.NotIn,
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
				Rules: []*pd.Rule{
					{
						ID:   "11",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "15",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    EngineLabelKey,
								Op:     pd.NotIn,
								Values: []string{EngineLabelTiFlash},
							},
						},
						Count: 1,
					},
					{
						ID:   "14",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "disk",
								Op:     pd.NotIn,
								Values: []string{"ssd", "hdd"},
							},
						},
						Count: 1,
					},
					{
						ID:   "13",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "16",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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
				Rules: []*pd.Rule{
					{
						ID:   "17",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
								Values: []string{"sh"},
							},
						},
						Count: 1,
					},
					{
						ID:   "18",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{
								Key:    "zone",
								Op:     pd.In,
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

	rules1, err := newRules(pd.Voter, 3, `["+zone=sh", "+zone=sh"]`)
	require.NoError(t, err)
	rules2, err := newRules(pd.Voter, 4, `["-zone=sh", "+zone=bj"]`)
	require.NoError(t, err)
	bundle.Rules = append(rules1, rules2...)

	require.Equal(t, "{\"group_id\":\"TiDB_DDL_1\",\"group_index\":0,\"group_override\":false,\"rules\":[{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"is_witness\":false,\"count\":3,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"sh\"]}]},{\"group_id\":\"\",\"id\":\"\",\"start_key\":\"\",\"end_key\":\"\",\"role\":\"voter\",\"is_witness\":false,\"count\":4,\"label_constraints\":[{\"key\":\"zone\",\"op\":\"notIn\",\"values\":[\"sh\"]},{\"key\":\"zone\",\"op\":\"in\",\"values\":[\"bj\"]}]}]}", bundle.String())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/placement/MockMarshalFailure", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/placement/MockMarshalFailure"))
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
		output []*pd.Rule
		err    error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		name:  "empty 1",
		input: &model.PlacementSettings{},
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect()),
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
		err: ErrInvalidConstraintsReplicas,
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: normal case 1",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "us",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
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
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 1, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: few followers",
		input: &model.PlacementSettings{
			PrimaryRegion: "us",
			Regions:       "bj,sh,us",
			Followers:     1,
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "bj", "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: omit regions 1",
		input: &model.PlacementSettings{
			Followers: 2,
			Schedule:  "even",
		},
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "sugar syntax: omit regions 2",
		input: &model.PlacementSettings{
			Followers: 2,
			Schedule:  "majority_in_primary",
		},
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect()),
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
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "sh"),
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
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "sh"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "sh"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "bj"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: normal case 1",
		input: &model.PlacementSettings{
			Constraints: "[+region=us]",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
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
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
			NewRule(pd.Learner, 2, NewConstraintsDirect(
				NewConstraintDirect("region", pd.In, "us"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: only leader constraints",
		input: &model.PlacementSettings{
			LeaderConstraints: "[+region=as]",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "as"))),
			NewRule(pd.Voter, 2, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: only leader constraints",
		input: &model.PlacementSettings{
			LeaderConstraints: "[+region=as]",
			Followers:         4,
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "as"))),
			NewRule(pd.Voter, 4, NewConstraintsDirect()),
		},
	})
	tests = append(tests, TestCase{
		name: "direct syntax: leader and follower constraints",
		input: &model.PlacementSettings{
			LeaderConstraints:   "[+region=as]",
			FollowerConstraints: `{"+region=us": 2}`,
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "as"))),
			NewRule(pd.Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: lack count 1",
		input: &model.PlacementSettings{
			LeaderConstraints:   "[+region=as]",
			FollowerConstraints: "[-region=us]",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "as"))),
			NewRule(pd.Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.NotIn, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: lack count 2",
		input: &model.PlacementSettings{
			LeaderConstraints:  "[+region=as]",
			LearnerConstraints: "[-region=us]",
		},
		err: ErrInvalidConstraintsReplicas,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: omit leader",
		input: &model.PlacementSettings{
			Followers:           2,
			FollowerConstraints: "[+region=bj]",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect()),
			NewRule(pd.Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "bj"))),
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
		name: "direct syntax: follower dict constraints",
		input: &model.PlacementSettings{
			FollowerConstraints: "{+disk=ssd: 1}",
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect()),
			NewRule(pd.Voter, 1, NewConstraintsDirect(NewConstraintDirect("disk", pd.In, "ssd"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: invalid follower dict constraints",
		input: &model.PlacementSettings{
			FollowerConstraints: "{+disk=ssd: 1}",
			Followers:           2,
		},
		err: ErrInvalidConstraintsReplicas,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: learner dict constraints",
		input: &model.PlacementSettings{
			LearnerConstraints: `{"+region=us": 2}`,
		},
		output: []*pd.Rule{
			NewRule(pd.Leader, 1, NewConstraintsDirect()),
			NewRule(pd.Voter, 2, NewConstraintsDirect()),
			NewRule(pd.Learner, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: learner dict constraints, with count",
		input: &model.PlacementSettings{
			LearnerConstraints: `{"+region=us": 2}`,
			Learners:           4,
		},
		err: ErrInvalidConstraintsReplicas,
	})

	tests = append(tests, TestCase{
		name: "direct syntax: dict constraints",
		input: &model.PlacementSettings{
			Constraints: `{"+region=us": 3}`,
		},
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: dict constraints, 2:2:1",
		input: &model.PlacementSettings{
			Constraints: `{ "+region=us-east-1":2, "+region=us-east-2": 2, "+region=us-west-1": 1}`,
		},
		output: []*pd.Rule{
			NewRule(pd.Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us-east-1"))),
			NewRule(pd.Voter, 2, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us-east-2"))),
			NewRule(pd.Voter, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us-west-1"))),
		},
	})

	tests = append(tests, TestCase{
		name: "direct syntax: dict constraints",
		input: &model.PlacementSettings{
			Constraints:        `{"+region=us-east": 3}`,
			LearnerConstraints: `{"+region=us-west": 1}`,
		},
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us-east"))),
			NewRule(pd.Learner, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, "us-west"))),
		},
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

	rules, err := newRules(pd.Voter, 3, `["+zone=sh", "+zone=sh"]`)
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

	rules0, err := newRules(pd.Voter, 1, `["+zone=sh", "+zone=sh"]`)
	require.NoError(t, err)
	require.Len(t, rules0, 1)
	rules0[0].Count = 0 // test prune useless rules

	rules1, err := newRules(pd.Voter, 4, `["-zone=sh", "+zone=bj"]`)
	require.NoError(t, err)
	require.Len(t, rules1, 1)
	rules2, err := newRules(pd.Voter, 0, `{"-zone=sh,+zone=bj": 4}}`)
	require.NoError(t, err)
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules1...)
	bundle.Rules = append(bundle.Rules, rules2...)

	require.Len(t, bundle.Rules, 3)
	err = bundle.Tidy()
	require.NoError(t, err)
	require.Len(t, bundle.Rules, 1)
	require.Equal(t, "0", bundle.Rules[0].ID)
	require.Len(t, bundle.Rules[0].LabelConstraints, 3)
	require.Equal(t, pd.LabelConstraint{
		Op:     pd.NotIn,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	}, bundle.Rules[0].LabelConstraints[2])

	// merge
	rules3, err := newRules(pd.Follower, 4, "")
	require.NoError(t, err)
	require.Len(t, rules3, 1)

	rules4, err := newRules(pd.Follower, 5, "")
	require.NoError(t, err)
	require.Len(t, rules4, 1)

	rules0[0].Role = pd.Voter
	bundle.Rules = append(bundle.Rules, rules0...)
	bundle.Rules = append(bundle.Rules, rules3...)
	bundle.Rules = append(bundle.Rules, rules4...)

	for _, r := range bundle.Rules {
		r.LocationLabels = []string{"zone", "host"}
	}
	chkfunc := func() {
		require.NoError(t, err)
		require.Len(t, bundle.Rules, 2)
		require.Equal(t, "0", bundle.Rules[0].ID)
		require.Equal(t, "1", bundle.Rules[1].ID)
		require.Equal(t, 9, bundle.Rules[1].Count)
		require.Equal(t, []pd.LabelConstraint{
			{
				Op:     pd.NotIn,
				Key:    EngineLabelKey,
				Values: []string{EngineLabelTiFlash},
			},
		}, bundle.Rules[1].LabelConstraints)
		require.Equal(t, []string{"zone", "host"}, bundle.Rules[1].LocationLabels)
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

	bundle.Rules[1].LabelConstraints = append(bundle.Rules[1].LabelConstraints, pd.LabelConstraint{
		Op:     pd.In,
		Key:    EngineLabelKey,
		Values: []string{EngineLabelTiFlash},
	})
	require.ErrorIs(t, bundle.Tidy(), ErrConflictingConstraints)
}

func TestTidy2(t *testing.T) {
	tests := []struct {
		name     string
		bundle   Bundle
		expected Bundle
	}{
		{
			name: "Empty bundle",
			bundle: Bundle{
				Rules: []*pd.Rule{},
			},
			expected: Bundle{
				Rules: []*pd.Rule{},
			},
		},
		{
			name: "Rules with empty constraints are merged",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:               "1",
						Role:             pd.Leader,
						Count:            1,
						LabelConstraints: []pd.LabelConstraint{},
						LocationLabels:   []string{"region"},
					},
					{
						ID:               "2",
						Role:             pd.Voter,
						Count:            2,
						LabelConstraints: []pd.LabelConstraint{},
						LocationLabels:   []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:               "0",
						Role:             pd.Voter,
						Count:            3,
						LabelConstraints: []pd.LabelConstraint{},
						LocationLabels:   []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints are merged, Leader + Follower",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          2,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          3,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints are merged, Leader + Voter",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          2,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          3,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints and role are merged,  Leader + Follower + Voter",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          3,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints and role are merged,  Leader + Follower + Voter + Learner",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "4",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          2,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          3,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          2,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints and role are merged,  Leader + Follower + Learner | Follower",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "4",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          2,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with same constraints and role are merged,  Leader + Follower + Learner | Voter",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "4",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "1",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Learner,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "3",
						Role: pd.Voter,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
		{
			name: "Rules with different constraints are kept separate",
			bundle: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "1",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "2",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
			expected: Bundle{
				Rules: []*pd.Rule{
					{
						ID:   "0",
						Role: pd.Leader,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"1"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
					{
						ID:   "1",
						Role: pd.Follower,
						LabelConstraints: []pd.LabelConstraint{
							{Op: pd.In, Key: "rack", Values: []string{"2"}},
						},
						Count:          1,
						LocationLabels: []string{"region"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.bundle.Tidy()
			require.NoError(t, err)

			require.Equal(t, len(tt.expected.Rules), len(tt.bundle.Rules))

			for i, rule := range tt.bundle.Rules {
				expectedRule := tt.expected.Rules[i]
				// Tiflash is always excluded from the constraints.
				AddConstraint(&expectedRule.LabelConstraints, pd.LabelConstraint{
					Op:     pd.NotIn,
					Key:    EngineLabelKey,
					Values: []string{EngineLabelTiFlash},
				})
				if !reflect.DeepEqual(rule, expectedRule) {
					t.Errorf("unexpected rule at index %d:\nactual=%#v,\nexpected=%#v\n", i, rule, expectedRule)
				}
			}
		})
	}
}
