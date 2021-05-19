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
	"encoding/json"
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

func matchRule(r1 *Rule, t2 []*Rule) bool {
	for _, r2 := range t2 {
		if ok, _ := DeepEquals.Check([]interface{}{r1, r2}, nil); ok {
			return true
		}
	}
	return false
}

func matchRules(t1, t2 []*Rule, prefix string, c *C) {
	expected, err := json.Marshal(t1)
	c.Assert(err, IsNil)
	got, err := json.Marshal(t2)
	c.Assert(err, IsNil)
	comment := Commentf("%s, expected %s\nbut got %s", prefix, expected, got)
	c.Assert(len(t1), Equals, len(t2), comment)
	for _, r1 := range t1 {
		c.Assert(matchRule(r1, t2), IsTrue, comment)
	}
}

func (t *testRuleSuite) TestNewRules(c *C) {
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
			{
				Count:       3,
				Constraints: Constraints{},
			},
		},
	})

	tests = append(tests, TestCase{
		name:     "zero replicas",
		input:    "",
		replicas: 0,
		err:      ErrInvalidConstraintsRelicas,
	})

	labels, err := NewConstraints([]string{"+zone=sh", "+zone=sh"})
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		name:     "normal array constraints",
		input:    `["+zone=sh", "+zone=sh"]`,
		replicas: 3,
		output: []*Rule{
			{
				Count:       3,
				Constraints: labels,
			},
		},
	})

	labels1, err := NewConstraints([]string{"+zone=sh", "-zone=bj"})
	c.Assert(err, IsNil)
	labels2, err := NewConstraints([]string{"+zone=sh"})
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		name:     "normal object constraints",
		input:    `{"+zone=sh,-zone=bj":2, "+zone=sh": 1}`,
		replicas: 3,
		output: []*Rule{
			{
				Count:       2,
				Constraints: labels1,
			},
			{
				Count:       1,
				Constraints: labels2,
			},
		},
	})

	tests = append(tests, TestCase{
		name:     "normal object constraints, with extra count",
		input:    "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
		replicas: 4,
		output: []*Rule{
			{
				Count:       2,
				Constraints: labels1,
			},
			{
				Count:       1,
				Constraints: labels2,
			},
			{
				Count: 1,
			},
		},
	})

	tests = append(tests, TestCase{
		name:  "normal object constraints, without count",
		input: "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
		output: []*Rule{
			{
				Count:       2,
				Constraints: labels1,
			},
			{
				Count:       1,
				Constraints: labels2,
			},
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
		err:      ErrInvalidConstraintFormat,
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
		comment := Commentf("%s", t.name)
		output, err := NewRules(t.replicas, t.input)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			matchRules(t.output, output, comment.CheckCommentString(), c)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}
