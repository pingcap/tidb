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

package label

import (
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
			input: "nomerge",
			label: Label{
				Key:   "nomerge",
				Value: "true",
			},
		},
		{
			name:  "normal with space",
			input: " nomerge ",
			label: Label{
				Key:   "nomerge",
				Value: "true",
			},
		},
	}

	for _, t := range tests {
		label := NewLabel(t.input)
		c.Assert(label, DeepEquals, t.label, Commentf("%s", t.name))
	}
}

func (t *testLabelSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Label
		output string
	}

	input := NewLabel("nomerge")
	input1 := NewLabel(" nomerge  ")
	tests := []TestCase{
		{
			name:   "normal",
			input:  input,
			output: "nomerge",
		},
		{
			name:   "normal with spaces",
			input:  input1,
			output: "nomerge",
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
	labels := NewLabels(nil)
	c.Assert(labels, HasLen, 0)

	labels = NewLabels([]string{})
	c.Assert(labels, HasLen, 0)

	labels = NewLabels([]string{"nomerge"})
	c.Assert(labels, HasLen, 1)
	c.Assert(labels[0].Key, Equals, "nomerge")

	// test multiple attributes
	labels = NewLabels([]string{"nomerge", "somethingelse"})
	c.Assert(labels, HasLen, 2)
	c.Assert(labels[0].Key, Equals, "nomerge")
	c.Assert(labels[1].Key, Equals, "somethingelse")

	// test duplicated attributes
	labels = NewLabels([]string{"nomerge", "nomerge"})
	c.Assert(labels, HasLen, 1)
	c.Assert(labels[0].Key, Equals, "nomerge")
}

func (t *testLabelsSuite) TestAdd(c *C) {
	type TestCase struct {
		name   string
		labels Labels
		label  Label
	}

	labels := NewLabels([]string{"nomerge"})
	label := NewLabel("somethingelse")
	tests := []TestCase{
		{
			"normal",
			labels, label,
		},
		{
			"duplicated attributes, skip",
			NewLabels([]string{"nomerge"}), NewLabel("nomerge"),
		},
		{
			"duplicated attributes, skip",
			append(labels, Label{
				Key:   "nomerge",
				Value: "true",
			}), label,
		},
	}

	for _, t := range tests {
		t.labels.Add(t.label)
		c.Assert(t.labels[len(t.labels)-1], DeepEquals, t.label, Commentf("%s", t.name))
	}
}

func (t *testLabelsSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Labels
		output string
	}

	input1 := NewLabel("nomerge")
	input2 := NewLabel("somethingelse")
	input3 := NewLabel("db")
	input4 := NewLabel("table")

	tests := []TestCase{
		{
			"normal1",
			Labels{},
			"",
		},
		{
			"normal2",
			Labels{input1, input2},
			`"nomerge","somethingelse"`,
		},
		{
			"normal3",
			Labels{input3, input4},
			"",
		},
		{
			"normal4",
			Labels{input1, input2, input3},
			`"nomerge","somethingelse"`,
		},
	}

	for _, t := range tests {
		res := t.input.Restore()
		c.Assert(res, Equals, t.output, Commentf("%s", t.name))
	}
}
