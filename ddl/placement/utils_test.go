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

package placement

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testUtilsSuite{})

type testUtilsSuite struct{}

func (t *testUtilsSuite) TestRestoreConstraints(c *C) {
	testCases := []struct {
		constraints    []LabelConstraint
		expectedResult string
		expectErr      bool
	}{
		{
			constraints:    []LabelConstraint{},
			expectedResult: ``,
		},
		{
			constraints: []LabelConstraint{
				{
					Key:    "zone",
					Op:     "in",
					Values: []string{"bj"},
				},
			},
			expectedResult: `"+zone=bj"`,
		},
		{
			constraints: []LabelConstraint{
				{
					Key:    "zone",
					Op:     "notIn",
					Values: []string{"bj"},
				},
			},
			expectedResult: `"-zone=bj"`,
		},
		{
			constraints: []LabelConstraint{
				{
					Key:    "zone",
					Op:     "exists",
					Values: []string{"bj"},
				},
			},
			expectErr: true,
		},
		{
			constraints: []LabelConstraint{
				{
					Key:    "zone",
					Op:     "in",
					Values: []string{"bj", "sh"},
				},
			},
			expectedResult: `"+zone=bj,+zone=sh"`,
		},
		{
			constraints: []LabelConstraint{
				{
					Key:    "zone",
					Op:     "in",
					Values: []string{"bj", "sh"},
				},
				{
					Key:    "disk",
					Op:     "in",
					Values: []string{"ssd"},
				},
			},
			expectedResult: `"+zone=bj,+zone=sh","+disk=ssd"`,
		},
	}
	for _, testCase := range testCases {
		rs, err := RestoreLabelConstraintList(testCase.constraints)
		if testCase.expectErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(rs, Equals, testCase.expectedResult)
		}
	}
}

func (t *testUtilsSuite) TestObjectIDFromGroupID(c *C) {
	testCases := []struct {
		bundleID   string
		expectedID int64
		expectErr  bool
	}{
		{"pd", 0, false},
		{"TiDB_DDL_foo", 0, true},
		{"TiDB_DDL_3x", 0, true},
		{"TiDB_DDL_3.0", 0, true},
		{"TiDB_DDL_-10", 0, true},
		{"TiDB_DDL_10", 10, false},
	}
	for _, testCase := range testCases {
		id, err := ObjectIDFromGroupID(testCase.bundleID)
		if testCase.expectErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(id, Equals, testCase.expectedID)
		}
	}
}

func (t *testUtilsSuite) TestGetLeaderDCByBundle(c *C) {
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
							},
						},
						Count: 1,
					},
					{
						ID:   "12",
						Role: Voter,
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"bj"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "fake",
								Op:     In,
								Values: []string{"sh"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     NotIn,
								Values: []string{"sh"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
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
						LabelConstraints: []LabelConstraint{
							{
								Key:    "zone",
								Op:     In,
								Values: []string{"sh", "bj"},
							},
							{
								Key:    EngineLabelKey,
								Op:     NotIn,
								Values: []string{EngineLabelTiFlash},
							},
						},
						Count: 1,
					},
				},
			},
			expectedDC: "",
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.name)
		result, ok := GetLeaderDCByBundle(testcase.bundle, "zone")
		if len(testcase.expectedDC) > 0 {
			c.Assert(ok, Equals, true)
		} else {
			c.Assert(ok, Equals, false)
		}
		c.Assert(result, Equals, testcase.expectedDC)
	}
}
