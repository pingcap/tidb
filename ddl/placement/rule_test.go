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

	. "github.com/pingcap/check"
)

var _ = Suite(&testRuleSuite{})

type testRuleSuite struct{}

func (t *testRuleSuite) TestClone(c *C) {
	rule := &Rule{ID: "434"}
	newRule := rule.Clone()
	newRule.ID = "121"

	c.Assert(rule, DeepEquals, &Rule{ID: "434"})
	c.Assert(newRule, DeepEquals, &Rule{ID: "121"})
}

func matchRules(t1, t2 []*Rule, prefix string, c *C) {
	c.Assert(len(t2), Equals, len(t1), Commentf(prefix))
	for i := range t1 {
		found := false
		for j := range t2 {
			ok, _ := DeepEquals.Check([]interface{}{t2[j], t1[i]}, []string{})
			if ok {
				found = true
				break
			}
		}
		if !found {
			c.Errorf("%s\n\ncan not found %d rule\n%+v\n%+v", prefix, i, t1[i], t2)
		}
	}
}

func (t *testRuleSuite) TestNewRuleAndNewRules(c *C) {
	type TestCase struct {
		name     string
		input    string
		replicas uint64
		output   []*Rule
		err      error
	}
	tests := []TestCase{}

	tests = append(tests, TestCase{
		name:     "empty constraints",
		input:    "",
		replicas: 3,
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name:     "zero replicas",
		input:    "",
		replicas: 0,
		err:      ErrInvalidConstraintsRelicas,
	})

	tests = append(tests, TestCase{
		name:     "normal array constraints",
		input:    `["+zone=sh", "+region=sh"]`,
		replicas: 3,
		output: []*Rule{
			NewRule(Voter, 3, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
				NewConstraintDirect("region", In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:     "normal object constraints",
		input:    `{"+zone=sh,-zone=bj":2, "+zone=sh": 1}`,
		replicas: 3,
		output: []*Rule{
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
				NewConstraintDirect("zone", NotIn, "bj"),
			)),
			NewRule(Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:     "normal object constraints, with extra count",
		input:    "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
		replicas: 4,
		output: []*Rule{
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
				NewConstraintDirect("zone", NotIn, "bj"),
			)),
			NewRule(Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
			)),
			NewRule(Voter, 1, NewConstraintsDirect()),
		},
	})

	tests = append(tests, TestCase{
		name:  "normal object constraints, without count",
		input: "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
		output: []*Rule{
			NewRule(Voter, 2, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
				NewConstraintDirect("zone", NotIn, "bj"),
			)),
			NewRule(Voter, 1, NewConstraintsDirect(
				NewConstraintDirect("zone", In, "sh"),
			)),
		},
	})

	tests = append(tests, TestCase{
		name:     "zero count in object constraints",
		input:    `{"+zone=sh,-zone=bj":0, "+zone=sh": 1}`,
		replicas: 3,
		err:      ErrInvalidConstraintsMapcnt,
	})

	tests = append(tests, TestCase{
		name:     "overlarge total count in object constraints",
		input:    `{"+ne=sh,-zone=bj":1, "+zone=sh": 4}`,
		replicas: 3,
		err:      ErrInvalidConstraintsRelicas,
	})

	tests = append(tests, TestCase{
		name:     "invalid array",
		input:    `["+ne=sh", "+zone=sh"`,
		replicas: 3,
		err:      ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name:     "invalid array constraints",
		input:    `["ne=sh", "+zone=sh"]`,
		replicas: 3,
		err:      ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name:     "invalid map",
		input:    `{+ne=sh,-zone=bj:1, "+zone=sh": 4`,
		replicas: 5,
		err:      ErrInvalidConstraintsFormat,
	})

	tests = append(tests, TestCase{
		name:     "invalid map constraints",
		input:    `{"nesh,-zone=bj":1, "+zone=sh": 4}`,
		replicas: 6,
		err:      ErrInvalidConstraintFormat,
	})

	for _, t := range tests {
		comment := Commentf("[%s]", t.name)
		output, err := NewRules(Voter, t.replicas, t.input)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			matchRules(t.output, output, comment.CheckCommentString(), c)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, Commentf("[%s]\n%s\n%s\n", t.name, err, t.err))
		}
	}
}
