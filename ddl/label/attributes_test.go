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

package label

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLabelSuite{})

type testLabelSuite struct{}

func (t *testLabelSuite) TestNew(c *C) {
	type TestCase struct {
		name  string
		input string
		label Label
	}
	tests := []TestCase{
		{
			name:  "normal",
			input: "merge_option=allow",
			label: Label{
				Key:   "merge_option",
				Value: "allow",
			},
		},
		{
			name:  "normal with space",
			input: " merge_option=allow ",
			label: Label{
				Key:   "merge_option",
				Value: "allow",
			},
		},
	}

	for _, t := range tests {
		label, err := NewLabel(t.input)
		c.Assert(err, IsNil)
		c.Assert(label, DeepEquals, t.label, Commentf("%s", t.name))
	}
}

func (t *testLabelSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Label
		output string
	}

	input, err := NewLabel("merge_option=allow")
	c.Assert(err, IsNil)
	input1, err := NewLabel(" merge_option=allow  ")
	c.Assert(err, IsNil)
	tests := []TestCase{
		{
			name:   "normal",
			input:  input,
			output: "merge_option=allow",
		},
		{
			name:   "normal with spaces",
			input:  input1,
			output: "merge_option=allow",
		},
	}

	for _, t := range tests {
		output := t.input.Restore()
		c.Assert(output, Equals, t.output, Commentf("%s", t.name))
	}
}

var _ = Suite(&testLabelsSuite{})

type testLabelsSuite struct{}

func (t *testLabelsSuite) TestNew(c *C) {
	labels, err := NewLabels(nil)
	c.Assert(err, IsNil)
	c.Assert(labels, HasLen, 0)

	labels, err = NewLabels([]string{})
	c.Assert(err, IsNil)
	c.Assert(labels, HasLen, 0)

	labels, err = NewLabels([]string{"merge_option=allow"})
	c.Assert(err, IsNil)
	c.Assert(labels, HasLen, 1)
	c.Assert(labels[0].Key, Equals, "merge_option")
	c.Assert(labels[0].Value, Equals, "allow")

	// test multiple attributes
	labels, err = NewLabels([]string{"merge_option=allow", "key=value"})
	c.Assert(err, IsNil)
	c.Assert(labels, HasLen, 2)
	c.Assert(labels[0].Key, Equals, "merge_option")
	c.Assert(labels[0].Value, Equals, "allow")
	c.Assert(labels[1].Key, Equals, "key")
	c.Assert(labels[1].Value, Equals, "value")

	// test duplicated attributes
	labels, err = NewLabels([]string{"merge_option=allow", "merge_option=allow"})
	c.Assert(err, IsNil)
	c.Assert(labels, HasLen, 1)
	c.Assert(labels[0].Key, Equals, "merge_option")
	c.Assert(labels[0].Value, Equals, "allow")
}

func (t *testLabelsSuite) TestAdd(c *C) {
	type TestCase struct {
		name   string
		labels Labels
		label  Label
		err    error
	}

	labels, err := NewLabels([]string{"merge_option=allow"})
	c.Assert(err, IsNil)
	label, err := NewLabel("somethingelse=true")
	c.Assert(err, IsNil)
	l1, err := NewLabels([]string{"key=value"})
	c.Assert(err, IsNil)
	l2, err := NewLabel("key=value")
	c.Assert(err, IsNil)
	l3, err := NewLabels([]string{"key=value1"})
	c.Assert(err, IsNil)
	tests := []TestCase{
		{
			"normal",
			labels, label,
			nil,
		},
		{
			"duplicated attributes, skip",
			l1, l2,
			nil,
		},
		{
			"duplicated attributes, skip",
			append(labels, Label{
				Key:   "merge_option",
				Value: "allow",
			}), label,
			nil,
		},
		{
			"conflict attributes",
			l3, l2,
			ErrConflictingAttributes,
		},
	}

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

func (t *testLabelsSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Labels
		output string
	}

	input1, err := NewLabel("merge_option=allow")
	c.Assert(err, IsNil)
	input2, err := NewLabel("key=value")
	c.Assert(err, IsNil)
	input3, err := NewLabel("db=d1")
	c.Assert(err, IsNil)
	input4, err := NewLabel("table=t1")
	c.Assert(err, IsNil)
	input5, err := NewLabel("partition=p1")
	c.Assert(err, IsNil)

	tests := []TestCase{
		{
			"normal1",
			Labels{},
			"",
		},
		{
			"normal2",
			Labels{input1, input2},
			`"merge_option=allow","key=value"`,
		},
		{
			"normal3",
			Labels{input3, input4, input5},
			"",
		},
		{
			"normal4",
			Labels{input1, input2, input3},
			`"merge_option=allow","key=value"`,
		},
	}

	for _, t := range tests {
		res := t.input.Restore()
		c.Assert(res, Equals, t.output, Commentf("%s", t.name))
	}
}
