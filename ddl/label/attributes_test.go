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
		c.Log(t.name)
		label := NewLabel(t.input)
		c.Assert(label, DeepEquals, t.label)
	}
}

func (t *testLabelSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Label
		output string
	}
	var tests []TestCase

	input := NewLabel("nomerge")
	tests = append(tests, TestCase{
		name:   "normal",
		input:  input,
		output: "nomerge",
	})

	input = NewLabel(" nomerge  ")
	tests = append(tests, TestCase{
		name:   "normal with spaces",
		input:  input,
		output: "nomerge",
	})

	for _, t := range tests {
		c.Log(t.name)
		output := t.input.Restore()
		c.Assert(output, Equals, t.output)
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
	var tests []TestCase

	labels := NewLabels([]string{"nomerge"})
	label := NewLabel("somethingelse")
	tests = append(tests, TestCase{
		"normal",
		labels, label,
	})

	labels = NewLabels([]string{"nomerge"})
	label = NewLabel("nomerge")
	tests = append(tests, TestCase{
		"duplicated attributes, skip",
		labels, label,
	})

	tests = append(tests, TestCase{
		"duplicated attributes, skip",
		append(labels, Label{
			Key:   "nomerge",
			Value: "true",
		}), label,
	})

	for _, t := range tests {
		c.Log(t.name)
		t.labels.Add(t.label)
		c.Assert(t.labels[len(t.labels)-1], DeepEquals, t.label)
	}
}

func (t *testLabelsSuite) TestRestore(c *C) {
	type TestCase struct {
		name   string
		input  Labels
		output string
		err    error
	}
	var tests []TestCase

	tests = append(tests, TestCase{
		"normal1",
		Labels{},
		"",
		nil,
	})

	input1 := NewLabel("nomerge")
	input2 := NewLabel("somethingelse")
	tests = append(tests, TestCase{
		"normal2",
		Labels{input1, input2},
		`"nomerge","somethingelse"`,
		nil,
	})

	input4 := NewLabel("db")
	input5 := NewLabel("table")
	tests = append(tests, TestCase{
		"normal3",
		Labels{input4, input5},
		"",
		nil,
	})

	tests = append(tests, TestCase{
		"normal4",
		Labels{input1, input2, input4},
		`"nomerge","somethingelse"`,
		nil,
	})

	for _, t := range tests {
		c.Log(t.name)
		res := t.input.Restore()
		c.Assert(res, Equals, t.output)
	}
}
