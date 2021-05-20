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

// +build hyperscan

package expression

import (
	"encoding/json"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
)

func generateSource(patterns []string, sourceType string) string {
	switch sourceType {
	case "lines":
		return strings.Join(patterns, "\n")
	case "json":
		pats := []hsPatternObj{}
		for id, pat := range patterns {
			patObj := hsPatternObj{
				ID:      id + 1,
				Pattern: pat,
			}
			pats = append(pats, patObj)
		}
		ret, err := json.Marshal(pats)
		if err != nil {
			return ""
		}
		return string(ret)
	}
	return ""
}

func (s *testEvaluatorSuite) TestHyperscanMatch(c *C) {
	tests := []struct {
		input    string
		patterns []string
		match    int
	}{
		{"test def", []string{"test$", "abc$"}, 0},
		{"test abc", []string{"test$", "abc$"}, 1},
		{"test ABC", []string{"test$", "/abc$/i"}, 1},
		{"test ABC", []string{"test$", "/abc$/"}, 0},
		{"this is a test", []string{"test$", "abc$"}, 1},
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", patterns = "%v"`, tt.input, tt.patterns)
		fc := funcs[HSMatch]
		source := generateSource(tt.patterns, "lines")
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source)))
		c.Assert(err, IsNil, commentf)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)

		source = generateSource(tt.patterns, "json")
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source, "json")))
		c.Assert(err, IsNil, commentf)
		r, err = evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)
	}
}

func (s *testEvaluatorSuite) TestHyperscanMatchAll(c *C) {
	tests := []struct {
		input    string
		patterns []string
		match    int
	}{
		{"test def", []string{"test", "abc"}, 0},
		{"test abc", []string{"test", "abc"}, 1},
		{"this is a test", []string{"test", "abc"}, 0},
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", patterns = "%v"`, tt.input, tt.patterns)
		fc := funcs[HSMatchAll]
		source := generateSource(tt.patterns, "lines")
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source)))
		c.Assert(err, IsNil, commentf)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)

		source = generateSource(tt.patterns, "json")
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source, "json")))
		c.Assert(err, IsNil, commentf)
		r, err = evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)
	}
}

func (s *testEvaluatorSuite) TestHyperscanMatchIds(c *C) {
	tests := []struct {
		input    string
		patterns []string
		matchIds string
	}{
		{"test def", []string{"abc", "test"}, "2"},
		{"test abc", []string{"abc", "test"}, "2,1"},
		{"this is a test", []string{"abc", "test"}, "2"},
		{"this is empty result", []string{"abc", "test"}, ""},
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", patterns = "%v"`, tt.input, tt.patterns)
		fc := funcs[HSMatchIds]
		source := generateSource(tt.patterns, "lines")
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source)))
		c.Assert(err, IsNil, commentf)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.matchIds), commentf)

		source = generateSource(tt.patterns, "json")
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, source, "json")))
		c.Assert(err, IsNil, commentf)
		r, err = evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.matchIds), commentf)
	}
}
