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

var _ = Suite(&testConstraintsSuite{})

type testConstraintsSuite struct{}

func (t *testConstraintsSuite) TestNew(c *C) {
	_, err := NewConstraints(nil)
	c.Assert(err, IsNil)

	_, err = NewConstraints([]string{})
	c.Assert(err, IsNil)

	_, err = NewConstraints([]string{"+zonesh"})
	c.Assert(errors.Is(err, ErrInvalidConstraintFormat), IsTrue)

	_, err = NewConstraints([]string{"+zone=sh", "-zone=sh"})
	c.Assert(errors.Is(err, ErrConflictingConstraints), IsTrue)
}

func (t *testConstraintsSuite) TestAdd(c *C) {
	type TestCase struct {
		name   string
		labels Constraints
		label  Constraint
		err    error
	}
	var tests []TestCase

	labels, err := NewConstraints([]string{"+zone=sh"})
	c.Assert(err, IsNil)
	label, err := NewConstraint("-zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"always false match",
		labels, label,
		ErrConflictingConstraints,
	})

	labels, err = NewConstraints([]string{"+zone=sh"})
	c.Assert(err, IsNil)
	label, err = NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"duplicated constraints, skip",
		labels, label,
		nil,
	})

	tests = append(tests, TestCase{
		"duplicated constraints should not stop conflicting constraints check",
		append(labels, Constraint{
			Op:     NotIn,
			Key:    "zone",
			Values: []string{"sh"},
		}), label,
		ErrConflictingConstraints,
	})

	labels, err = NewConstraints([]string{"+zone=sh"})
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"invalid label in operand",
		labels, Constraint{Op: "["},
		nil,
	})

	tests = append(tests, TestCase{
		"invalid label in operator",
		Constraints{{Op: "["}}, label,
		nil,
	})

	tests = append(tests, TestCase{
		"invalid label in both, same key",
		Constraints{{Op: "[", Key: "dc"}}, Constraint{Op: "]", Key: "dc"},
		ErrConflictingConstraints,
	})

	labels, err = NewConstraints([]string{"+zone=sh"})
	c.Assert(err, IsNil)
	label, err = NewConstraint("-zone=bj")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"normal",
		labels, label,
		nil,
	})

	for _, t := range tests {
		err := t.labels.Add(t.label)
		comment := Commentf("%s: %v", t.name, err)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			c.Assert(t.labels[len(t.labels)-1], DeepEquals, t.label, comment)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}

func (t *testConstraintsSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Constraints
		output string
		err    error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		"normal1",
		Constraints{},
		"",
		nil,
	})

	input1, err := NewConstraint("+zone=bj")
	c.Assert(err, IsNil)
	input2, err := NewConstraint("-zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"normal2",
		Constraints{input1, input2},
		`"+zone=bj","-zone=sh"`,
		nil,
	})

	tests = append(tests, TestCase{
		"error",
		Constraints{{
			Op:     "[",
			Key:    "dc",
			Values: []string{"dc1"},
		}},
		"",
		ErrInvalidConstraintFormat,
	})

	for _, t := range tests {
		res, err := t.input.Restore()
		comment := Commentf("%s: %v", t.name, err)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			c.Assert(res, Equals, t.output, comment)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}
