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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestLike(c *C) {
	tests := []struct {
		input   string
		pattern string
		match   int
	}{
		{"a", "", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 0},
		{"aAb", `Aa%`, 0},
		{"aAb", "aA_", 1},
		{"baab", "b_%b", 1},
		{"baab", "b%_b", 1},
		{"bab", "b_%b", 1},
		{"bab", "b%_b", 1},
		{"bb", "b_%b", 0},
		{"bb", "b%_b", 0},
		{"baabccc", "b_%b%", 1},
	}

	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0)))
		c.Assert(err, IsNil, commentf)
		r, err := evalBuiltinFuncConcurrent(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)
	}
}

func (s *testEvaluatorSerialSuites) TestCILike(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tests := []struct {
		input   string
		pattern string
		match   int
	}{
		{"a", "", 0},
		{"a", "a", 1},
		{"a", "á", 1},
		{"a", "b", 0},
		{"aA", "Aa", 1},
		{"áAb", `Aa%`, 1},
		{"áAb", `%ab%`, 1},
		{"áAb", `%ab`, 1},
		{"ÀAb", "aA_", 1},
		{"áééá", "a_%a", 1},
		{"áééá", "a%_a", 1},
		{"áéá", "a_%a", 1},
		{"áéá", "a%_a", 1},
		{"áá", "a_%a", 0},
		{"áá", "a%_a", 0},
		{"áééáííí", "a_%a%", 1},
	}
	for _, tt := range tests {
		commentf := Commentf(`for input = "%s", pattern = "%s"`, tt.input, tt.pattern)
		fc := funcs[ast.Like]
		inputs := s.datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0))
		f, err := fc.getFunction(s.ctx, inputs)
		c.Assert(err, IsNil, commentf)
		f.setCollator(collate.GetCollator("utf8mb4_general_ci"))
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil, commentf)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match), commentf)
	}
}

func (s *testEvaluatorSuite) TestRegexp(c *C) {
	tests := []struct {
		pattern string
		input   string
		match   int64
		err     error
	}{
		{"^$", "a", 0, nil},
		{"a", "a", 1, nil},
		{"a", "b", 0, nil},
		{"aA", "aA", 1, nil},
		{".", "a", 1, nil},
		{"^.$", "ab", 0, nil},
		{"..", "b", 0, nil},
		{".ab", "aab", 1, nil},
		{".*", "abcd", 1, nil},
		{"(", "", 0, ErrRegexp},
		{"(*", "", 0, ErrRegexp},
		{"[a", "", 0, ErrRegexp},
		{"\\", "", 0, ErrRegexp},
	}
	for _, tt := range tests {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		c.Assert(err, IsNil)
		match, err := evalBuiltinFunc(f, chunk.Row{})
		if tt.err == nil {
			c.Assert(err, IsNil)
			c.Assert(match, testutil.DatumEquals, types.NewDatum(tt.match), Commentf("%v", tt))
		} else {
			c.Assert(terror.ErrorEqual(err, tt.err), IsTrue)
		}
	}
}
