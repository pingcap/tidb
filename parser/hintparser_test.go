// Copyright 2020 PingCAP, Inc.
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

package parser_test

import (
	. "github.com/pingcap/check"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
)

var _ = Suite(&testHintParserSuite{})

type testHintParserSuite struct{}

func (s *testHintParserSuite) TestParseHint(c *C) {
	testCases := []struct {
		input  string
		mode   mysql.SQLMode
		output []*ast.TableOptimizerHint
		errs   []string
	}{
		{
			input: "",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "MEMORY_QUOTA(8 MB) MEMORY_QUOTA(6 GB)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("MEMORY_QUOTA"),
					HintData: int64(8 * 1024 * 1024),
				},
				{
					HintName: model.NewCIStr("MEMORY_QUOTA"),
					HintData: int64(6 * 1024 * 1024 * 1024),
				},
			},
		},
		{
			input: "QB_NAME(qb1) QB_NAME(`qb2`), QB_NAME(TRUE) QB_NAME(\"ANSI quoted\") QB_NAME(_utf8), QB_NAME(0b10) QB_NAME(0x1a)",
			mode:  mysql.ModeANSIQuotes,
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("qb1"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("qb2"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("TRUE"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("ANSI quoted"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("_utf8"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("0b10"),
				},
				{
					HintName: model.NewCIStr("QB_NAME"),
					QBName:   model.NewCIStr("0x1a"),
				},
			},
		},
		{
			input: "QB_NAME(1)",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "QB_NAME('string literal')",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "QB_NAME(many identifiers)",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "QB_NAME(@qb1)",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "QB_NAME(b'10')",
			errs: []string{
				`.*Cannot use bit-value literal.*`,
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "QB_NAME(x'1a')",
			errs: []string{
				`.*Cannot use hexadecimal literal.*`,
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "JOIN_FIXED_ORDER() BKA()",
			errs: []string{
				`.*Optimizer hint JOIN_FIXED_ORDER is not supported.*`,
				`.*Optimizer hint BKA is not supported.*`,
			},
		},
		{
			input: "HASH_JOIN() TIDB_HJ(@qb1) INL_JOIN(x, `y y`.z) MERGE_JOIN(w@`First QB`)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("HASH_JOIN"),
				},
				{
					HintName: model.NewCIStr("TIDB_HJ"),
					QBName:   model.NewCIStr("qb1"),
				},
				{
					HintName: model.NewCIStr("INL_JOIN"),
					Tables: []ast.HintTable{
						{TableName: model.NewCIStr("x")},
						{DBName: model.NewCIStr("y y"), TableName: model.NewCIStr("z")},
					},
				},
				{
					HintName: model.NewCIStr("MERGE_JOIN"),
					Tables: []ast.HintTable{
						{TableName: model.NewCIStr("w"), QBName: model.NewCIStr("First QB")},
					},
				},
			},
		},
		{
			input: "USE_INDEX_MERGE(@qb1 tbl1 x, y, z) IGNORE_INDEX(tbl2@qb2) USE_INDEX(tbl3 PRIMARY)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("USE_INDEX_MERGE"),
					Tables:   []ast.HintTable{{TableName: model.NewCIStr("tbl1")}},
					QBName:   model.NewCIStr("qb1"),
					Indexes:  []model.CIStr{model.NewCIStr("x"), model.NewCIStr("y"), model.NewCIStr("z")},
				},
				{
					HintName: model.NewCIStr("IGNORE_INDEX"),
					Tables:   []ast.HintTable{{TableName: model.NewCIStr("tbl2"), QBName: model.NewCIStr("qb2")}},
				},
				{
					HintName: model.NewCIStr("USE_INDEX"),
					Tables:   []ast.HintTable{{TableName: model.NewCIStr("tbl3")}},
					Indexes:  []model.CIStr{model.NewCIStr("PRIMARY")},
				},
			},
		},
		{
			input: `SET_VAR(sbs = 16M) SET_VAR(fkc=OFF) SET_VAR(os="mcb=off") set_var(abc=1)`,
			errs: []string{
				`.*Optimizer hint SET_VAR is not supported.*`,
				`.*Optimizer hint SET_VAR is not supported.*`,
				`.*Optimizer hint SET_VAR is not supported.*`,
				`.*Optimizer hint set_var is not supported.*`,
			},
		},
		{
			input: "USE_TOJA(TRUE) IGNORE_PLAN_CACHE() USE_CASCADES(TRUE) QUERY_TYPE(@qb1 OLAP) QUERY_TYPE(OLTP) NO_INDEX_MERGE()",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("USE_TOJA"),
					HintData: true,
				},
				{
					HintName: model.NewCIStr("IGNORE_PLAN_CACHE"),
				},
				{
					HintName: model.NewCIStr("USE_CASCADES"),
					HintData: true,
				},
				{
					HintName: model.NewCIStr("QUERY_TYPE"),
					QBName:   model.NewCIStr("qb1"),
					HintData: model.NewCIStr("OLAP"),
				},
				{
					HintName: model.NewCIStr("QUERY_TYPE"),
					HintData: model.NewCIStr("OLTP"),
				},
				{
					HintName: model.NewCIStr("NO_INDEX_MERGE"),
				},
			},
		},
		{
			input: "READ_FROM_STORAGE(@foo TIKV[a, b], TIFLASH[c, d]) HASH_AGG() READ_FROM_STORAGE(TIKV[e])",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("READ_FROM_STORAGE"),
					HintData: model.NewCIStr("TIKV"),
					QBName:   model.NewCIStr("foo"),
					Tables: []ast.HintTable{
						{TableName: model.NewCIStr("a")},
						{TableName: model.NewCIStr("b")},
					},
				},
				{
					HintName: model.NewCIStr("READ_FROM_STORAGE"),
					HintData: model.NewCIStr("TIFLASH"),
					QBName:   model.NewCIStr("foo"),
					Tables: []ast.HintTable{
						{TableName: model.NewCIStr("c")},
						{TableName: model.NewCIStr("d")},
					},
				},
				{
					HintName: model.NewCIStr("HASH_AGG"),
				},
				{
					HintName: model.NewCIStr("READ_FROM_STORAGE"),
					HintData: model.NewCIStr("TIKV"),
					Tables: []ast.HintTable{
						{TableName: model.NewCIStr("e")},
					},
				},
			},
		},
		{
			input: "unknown_hint()",
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "set_var(timestamp = 1.5)",
			errs: []string{
				`.*Cannot use decimal number.*`,
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "set_var(timestamp = _utf8mb4'1234')", // Optimizer hint doesn't recognize _charset'strings'.
			errs:  []string{`.*Optimizer hint syntax error at line 1 .*`},
		},
		{
			input: "set_var(timestamp = 9999999999999999999999999999999999999)",
			errs: []string{
				`.*integer value is out of range.*`,
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "time_range('2020-02-20 12:12:12',456)",
			errs: []string{
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "time_range(456,'2020-02-20 12:12:12')",
			errs: []string{
				`.*Optimizer hint syntax error at line 1 .*`,
			},
		},
		{
			input: "TIME_RANGE('2020-02-20 12:12:12','2020-02-20 13:12:12')",
			output: []*ast.TableOptimizerHint{
				{
					HintName: model.NewCIStr("TIME_RANGE"),
					HintData: ast.HintTimeRange{
						From: "2020-02-20 12:12:12",
						To:   "2020-02-20 13:12:12",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		output, errs := parser.ParseHint("/*+"+tc.input+"*/", tc.mode, parser.Pos{Line: 1})
		c.Assert(errs, HasLen, len(tc.errs), Commentf("input = %s,\n... errs = %q", tc.input, errs))
		for i, err := range errs {
			c.Assert(err, ErrorMatches, tc.errs[i], Commentf("input = %s, i = %d", tc.input, i))
		}
		c.Assert(output, DeepEquals, tc.output, Commentf("input = %s,\n... output = %q", tc.input, output))
	}
}
