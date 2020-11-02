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
