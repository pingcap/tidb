// Copyright 2017 PingCAP, Inc.
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

package json

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsAnyAsterisk(t *testing.T) {
	var tests = []struct {
		expression        string
		containsAsterisks bool
	}{
		{"$.a[b]", false},
		{"$.a[*]", true},
		{"$.*[b]", true},
		{"$**.a[b]", true},
	}

	for _, test := range tests {
		// copy iterator variable into a new variable, see issue #27779
		test := test
		t.Run(test.expression, func(t *testing.T) {
			pe, err := ParseJSONPathExpr(test.expression)
			require.NoError(t, err)
			require.Equal(t, test.containsAsterisks, pe.flags.containsAnyAsterisk())
		})
	}
}

func TestValidatePathExpr(t *testing.T) {
	var tests = []struct {
		expression string
		success    bool
		legs       int
	}{
		{`   $  `, true, 0},
		{"   $ .   key1  [  3  ]\t[*].*.key3", true, 5},
		{"   $ .   key1  [  3  ]**[*].*.key3", true, 6},
		{`$."key1 string"[  3  ][*].*.key3`, true, 5},
		{`$."hello \"escaped quotes\" world\\n"[3][*].*.key3`, true, 5},

		{`$.\"escaped quotes\"[3][*].*.key3`, false, 0},
		{`$.hello \"escaped quotes\" world[3][*].*.key3`, false, 0},
		{`$NoValidLegsHere`, false, 0},
		{`$        No Valid Legs Here .a.b.c`, false, 0},
	}

	for _, test := range tests {
		// copy iterator variable into a new variable, see issue #27779
		test := test
		t.Run(test.expression, func(t *testing.T) {
			pe, err := ParseJSONPathExpr(test.expression)
			if test.success {
				require.NoError(t, err)
				require.Len(t, pe.legs, test.legs)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestPathExprToString(t *testing.T) {
	var tests = []struct {
		expression string
	}{
		{"$.a[1]"},
		{"$.a[*]"},
		{"$.*[2]"},
		{"$**.a[3]"},
		{`$."\"hello\""`},
	}
	for _, test := range tests {
		// copy iterator variable into a new variable, see issue #27779
		test := test
		t.Run(test.expression, func(t *testing.T) {
			pe, err := ParseJSONPathExpr(test.expression)
			require.NoError(t, err)
			require.Equal(t, test.expression, pe.String())
		})
	}
}

func TestPushBackOneIndexLeg(t *testing.T) {
	var tests = []struct {
		expression          string
		index               int
		expected            string
		containsAnyAsterisk bool
	}{
		{"$", 1, "$[1]", false},
		{"$.a[1]", 1, "$.a[1][1]", false},
		{"$.a[1]", -1, "$.a[1][*]", true},
		{"$.a[*]", 10, "$.a[*][10]", true},
		{"$.*[2]", 2, "$.*[2][2]", true},
		{"$**.a[3]", 3, "$**.a[3][3]", true},
	}

	for _, test := range tests {
		// copy iterator variable into a new variable, see issue #27779
		test := test
		t.Run(test.expression, func(t *testing.T) {
			pe, err := ParseJSONPathExpr(test.expression)
			require.NoError(t, err)

			pe = pe.pushBackOneIndexLeg(test.index)
			require.Equal(t, test.expected, pe.String())
			require.Equal(t, test.containsAnyAsterisk, pe.ContainsAnyAsterisk())
		})
	}
}

func TestPushBackOneKeyLeg(t *testing.T) {
	var tests = []struct {
		expression          string
		key                 string
		expected            string
		containsAnyAsterisk bool
	}{
		{"$", "aa", "$.aa", false},
		{"$.a[1]", "aa", "$.a[1].aa", false},
		{"$.a[1]", "*", "$.a[1].*", true},
		{"$.a[*]", "k", "$.a[*].k", true},
		{"$.*[2]", "bb", "$.*[2].bb", true},
		{"$**.a[3]", "cc", "$**.a[3].cc", true},
	}

	for _, test := range tests {
		t.Run(test.expression, func(t *testing.T) {
			pe, err := ParseJSONPathExpr(test.expression)
			require.NoError(t, err)

			pe = pe.pushBackOneKeyLeg(test.key)
			require.Equal(t, test.expected, pe.String())
			require.Equal(t, test.containsAnyAsterisk, pe.ContainsAnyAsterisk())
		})
	}
}
