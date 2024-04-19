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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client/http"
)

func TestClone(t *testing.T) {
	rule := &pd.Rule{ID: "434"}
	newRule := rule.Clone()
	newRule.ID = "121"

	require.Equal(t, &pd.Rule{ID: "434"}, rule)
	require.Equal(t, &pd.Rule{ID: "121"}, newRule)
}

func matchRules(t1, t2 []*pd.Rule, prefix string, t *testing.T) {
	require.Equal(t, len(t2), len(t1), prefix)
	for i := range t1 {
		found := false
		for j := range t2 {
			ok := reflect.DeepEqual(t2[j], t1[i])
			if ok {
				found = true
				break
			}
		}
		require.True(t, found, "%s\n\ncan not found %d rule\n%+v\n%+v", prefix, i, t1[i], t2)
	}
}

func TestNewRuleAndNewRules(t *testing.T) {
	type TestCase struct {
		name     string
		input    string
		replicas uint64
		output   []*pd.Rule
		err      error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		name:     "empty constraints",
		input:    "",
		replicas: 3,
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name:     "zero replicas",
		input:    "",
		replicas: 0,
		output:   nil,
	})

	tests = append(tests, TestCase{
		name:     "normal list constraints",
		input:    `["+zone=sh", "+region=sh"]`,
		replicas: 3,
		output: []*pd.Rule{
			NewRule(pd.Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
				NewConstraintDirect("region", pd.In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:  "normal dict constraints",
		input: `{"+zone=sh,-zone=bj":2, "+zone=sh": 1}`,
		output: []*pd.Rule{
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
				NewConstraintDirect("zone", pd.NotIn, "bj"),
			)),
			NewRule(pd.Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:  "normal dict constraints, with count",
		input: "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
		output: []*pd.Rule{
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
				NewConstraintDirect("zone", pd.NotIn, "bj"),
			)),
			NewRule(pd.Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:  "zero count in dict constraints",
		input: `{"+zone=sh,-zone=bj":0, "+zone=sh": 1}`,
		err:   ErrInvalidConstraintsMapcnt,
	})

	tests = append(tests, TestCase{
		name:     "invalid list constraints",
		input:    `["ne=sh", "+zone=sh"]`,
		replicas: 3,
		err:      ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name:  "invalid dict constraints",
		input: `{+ne=sh,-zone=bj:1, "+zone=sh": 4`,
		err:   ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name:  "invalid dict constraints",
		input: `{"nesh,-zone=bj":1, "+zone=sh": 4}`,
		err:   ErrInvalidConstraintFormat,
	})

	tests = append(tests, TestCase{
		name:  "invalid dict separator",
		input: `{+region=us-east-2:2}`,
		err:   ErrInvalidConstraintsMappingWrongSeparator,
	})

	tests = append(tests, TestCase{
		name:  "normal dict constraint with evict leader attribute",
		input: `{"+zone=sh,-zone=bj":2, "+zone=sh,#evict-leader": 1}`,
		output: []*pd.Rule{
			NewRule(pd.Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
				NewConstraintDirect("zone", pd.NotIn, "bj"),
			)),
			NewRule(pd.Follower, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", pd.In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:  "invalid constraints with invalid format",
		input: `{"+zone=sh,-zone=bj":2, "+zone=sh,evict-leader": 1}`,
		err:   ErrInvalidConstraintFormat,
	})

	tests = append(tests, TestCase{
		name:  "invalid constraints with undetermined attribute",
		input: `{"+zone=sh,-zone=bj":2, "+zone=sh,#reject-follower": 1}`,
		err:   ErrUnsupportedConstraint,
	})

	for _, tt := range tests {
		comment := fmt.Sprintf("[%s]", tt.name)
		output, err := newRules(pd.Voter, tt.replicas, tt.input)
		if tt.err == nil {
			require.NoError(t, err, comment)
			matchRules(tt.output, output, comment, t)
		} else {
			require.True(t, errors.Is(err, tt.err), "[%s]\n%s\n%s\n", tt.name, err, tt.err)
		}
	}
}
