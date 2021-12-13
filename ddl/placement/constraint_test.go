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

var _ = Suite(&testConstraintSuite{})

type testConstraintSuite struct{}

func (t *testConstraintSuite) TestNewFromYaml(c *C) {
	_, err := NewConstraintsFromYaml([]byte("[]"))
	c.Assert(err, IsNil)
	_, err = NewConstraintsFromYaml([]byte("]"))
	c.Assert(err, NotNil)
}

func (t *testConstraintSuite) TestNew(c *C) {
	type TestCase struct {
		name  string
		input string
		label Constraint
		err   error
	}
	tests := []TestCase{
		{
			name:  "normal",
			input: "+zone=bj",
			label: Constraint{
				Key:    "zone",
				Op:     In,
				Values: []string{"bj"},
			},
		},
		{
			name:  "normal with spaces",
			input: "-  dc  =  sh  ",
			label: Constraint{
				Key:    "dc",
				Op:     NotIn,
				Values: []string{"sh"},
			},
		},
		{
			name:  "not tiflash",
			input: "-engine  =  tiflash  ",
			label: Constraint{
				Key:    "engine",
				Op:     NotIn,
				Values: []string{"tiflash"},
			},
		},
		{
			name:  "disallow tiflash",
			input: "+engine=Tiflash",
			err:   ErrUnsupportedConstraint,
		},
		// invalid
		{
			name:  "invalid length",
			input: ",,,",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "invalid, lack = 1",
			input: "+    ",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "invalid, lack = 2",
			input: "+000",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "invalid op",
			input: "0000",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "empty key 1",
			input: "+ =zone1",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "empty key 2",
			input: "+  =   z",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "empty value 1",
			input: "+zone=",
			err:   ErrInvalidConstraintFormat,
		},
		{
			name:  "empty value 2",
			input: "+z  =   ",
			err:   ErrInvalidConstraintFormat,
		},
	}

	for _, t := range tests {
		label, err := NewConstraint(t.input)
		comment := Commentf("%s: %v", t.name, err)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			c.Assert(label, DeepEquals, t.label, comment)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}

func (t *testConstraintSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Constraint
		output string
		err    error
	}
	var tests []TestCase

	input, err := NewConstraint("+zone=bj")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		name:   "normal, op in",
		input:  input,
		output: "+zone=bj",
	})

	input, err = NewConstraint("+  zone = bj  ")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		name:   "normal with spaces, op in",
		input:  input,
		output: "+zone=bj",
	})

	input, err = NewConstraint("-  zone = bj  ")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		name:   "normal with spaces, op not in",
		input:  input,
		output: "-zone=bj",
	})

	tests = append(tests, TestCase{
		name: "no values",
		input: Constraint{
			Op:     In,
			Key:    "dc",
			Values: []string{},
		},
		err: ErrInvalidConstraintFormat,
	})

	tests = append(tests, TestCase{
		name: "multiple values",
		input: Constraint{
			Op:     In,
			Key:    "dc",
			Values: []string{"dc1", "dc2"},
		},
		err: ErrInvalidConstraintFormat,
	})

	tests = append(tests, TestCase{
		name: "invalid op",
		input: Constraint{
			Op:     "[",
			Key:    "dc",
			Values: []string{},
		},
		err: ErrInvalidConstraintFormat,
	})

	for _, t := range tests {
		output, err := t.input.Restore()
		comment := Commentf("%s: %v", t.name, err)
		if t.err == nil {
			c.Assert(err, IsNil, comment)
			c.Assert(output, Equals, t.output, comment)
		} else {
			c.Assert(errors.Is(err, t.err), IsTrue, comment)
		}
	}
}

func (t *testConstraintSuite) TestCompatibleWith(c *C) {
	type TestCase struct {
		name   string
		i1     Constraint
		i2     Constraint
		output ConstraintCompatibility
	}
	var tests []TestCase

	i1, err := NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	i2, err := NewConstraint("-zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"case 2",
		i1, i2,
		ConstraintIncompatible,
	})

	i1, err = NewConstraint("+zone=bj")
	c.Assert(err, IsNil)
	i2, err = NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"case 3",
		i1, i2,
		ConstraintIncompatible,
	})

	i1, err = NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	i2, err = NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"case 1",
		i1, i2,
		ConstraintDuplicated,
	})

	i1, err = NewConstraint("+zone=sh")
	c.Assert(err, IsNil)
	i2, err = NewConstraint("+dc=sh")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"normal 1",
		i1, i2,
		ConstraintCompatible,
	})

	i1, err = NewConstraint("-zone=sh")
	c.Assert(err, IsNil)
	i2, err = NewConstraint("-zone=bj")
	c.Assert(err, IsNil)
	tests = append(tests, TestCase{
		"normal 2",
		i1, i2,
		ConstraintCompatible,
	})

	for _, t := range tests {
		comment := Commentf("%s", t.name)
		c.Assert(t.i1.CompatibleWith(&t.i2), Equals, t.output, comment)
	}
}
