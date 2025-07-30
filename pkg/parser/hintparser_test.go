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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestParseHint(t *testing.T) {
	testCases := []struct {
		input  string
		mode   mysql.SQLMode
		output []*ast.TableOptimizerHint
		errs   []string
	}{
		{
			input: "",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "MEMORY_QUOTA(8 MB) MEMORY_QUOTA(6 GB)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("MEMORY_QUOTA"),
					HintData: int64(8 * 1024 * 1024),
				},
				{
					HintName: ast.NewCIStr("MEMORY_QUOTA"),
					HintData: int64(6 * 1024 * 1024 * 1024),
				},
			},
		},
		{
			input: "QB_NAME(qb1) QB_NAME(`qb2`), QB_NAME(TRUE) QB_NAME(\"ANSI quoted\") QB_NAME(_utf8), QB_NAME(0b10) QB_NAME(0x1a)",
			mode:  mysql.ModeANSIQuotes,
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("qb1"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("qb2"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("TRUE"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("ANSI quoted"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("_utf8"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("0b10"),
				},
				{
					HintName: ast.NewCIStr("QB_NAME"),
					QBName:   ast.NewCIStr("0x1a"),
				},
			},
		},
		{
			input: "QB_NAME(1)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME('string literal')",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(many identifiers)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(@qb1)",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "QB_NAME(b'10')",
			errs: []string{
				`Cannot use bit-value literal`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "QB_NAME(x'1a')",
			errs: []string{
				`Cannot use hexadecimal literal`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "JOIN_FIXED_ORDER() BKA()",
			errs: []string{
				`Optimizer hint JOIN_FIXED_ORDER is not supported`,
				`Optimizer hint BKA is not supported`,
			},
		},
		{
			input: "HASH_JOIN() TIDB_HJ(@qb1) INL_JOIN(x, `y y`.z) MERGE_JOIN(w@`First QB`)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("HASH_JOIN"),
				},
				{
					HintName: ast.NewCIStr("TIDB_HJ"),
					QBName:   ast.NewCIStr("qb1"),
				},
				{
					HintName: ast.NewCIStr("INL_JOIN"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("x")},
						{DBName: ast.NewCIStr("y y"), TableName: ast.NewCIStr("z")},
					},
				},
				{
					HintName: ast.NewCIStr("MERGE_JOIN"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("w"), QBName: ast.NewCIStr("First QB")},
					},
				},
			},
		},
		{
			input: "USE_INDEX_MERGE(@qb1 tbl1 x, y, z) IGNORE_INDEX(tbl2@qb2) USE_INDEX(tbl3 PRIMARY) FORCE_INDEX(tbl4@qb3 c1)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("USE_INDEX_MERGE"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl1")}},
					QBName:   ast.NewCIStr("qb1"),
					Indexes:  []ast.CIStr{ast.NewCIStr("x"), ast.NewCIStr("y"), ast.NewCIStr("z")},
				},
				{
					HintName: ast.NewCIStr("IGNORE_INDEX"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl2"), QBName: ast.NewCIStr("qb2")}},
				},
				{
					HintName: ast.NewCIStr("USE_INDEX"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl3")}},
					Indexes:  []ast.CIStr{ast.NewCIStr("PRIMARY")},
				},
				{
					HintName: ast.NewCIStr("FORCE_INDEX"),
					Tables:   []ast.HintTable{{TableName: ast.NewCIStr("tbl4"), QBName: ast.NewCIStr("qb3")}},
					Indexes:  []ast.CIStr{ast.NewCIStr("c1")},
				},
			},
		},
		{
			input: "USE_INDEX(@qb1 tbl1 partition(p0) x) USE_INDEX_MERGE(@qb2 tbl2@qb2 partition(p0, p1) x, y, z)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("USE_INDEX"),
					Tables: []ast.HintTable{{
						TableName:     ast.NewCIStr("tbl1"),
						PartitionList: []ast.CIStr{ast.NewCIStr("p0")},
					}},
					QBName:  ast.NewCIStr("qb1"),
					Indexes: []ast.CIStr{ast.NewCIStr("x")},
				},
				{
					HintName: ast.NewCIStr("USE_INDEX_MERGE"),
					Tables: []ast.HintTable{{
						TableName:     ast.NewCIStr("tbl2"),
						QBName:        ast.NewCIStr("qb2"),
						PartitionList: []ast.CIStr{ast.NewCIStr("p0"), ast.NewCIStr("p1")},
					}},
					QBName:  ast.NewCIStr("qb2"),
					Indexes: []ast.CIStr{ast.NewCIStr("x"), ast.NewCIStr("y"), ast.NewCIStr("z")},
				},
			},
		},
		{
			input: `SET_VAR(sbs = 16M) SET_VAR(fkc=OFF) SET_VAR(os="mcb=off") set_var(abc=1) set_var(os2='mcb2=off')`,
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "sbs",
						Value:   "16M",
					},
				},
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "fkc",
						Value:   "OFF",
					},
				},
				{
					HintName: ast.NewCIStr("SET_VAR"),
					HintData: ast.HintSetVar{
						VarName: "os",
						Value:   "mcb=off",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "abc",
						Value:   "1",
					},
				},
				{
					HintName: ast.NewCIStr("set_var"),
					HintData: ast.HintSetVar{
						VarName: "os2",
						Value:   "mcb2=off",
					},
				},
			},
		},
		{
			input: "USE_TOJA(TRUE) IGNORE_PLAN_CACHE() USE_CASCADES(TRUE) QUERY_TYPE(@qb1 OLAP) QUERY_TYPE(OLTP) NO_INDEX_MERGE() RESOURCE_GROUP(rg1)",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("USE_TOJA"),
					HintData: true,
				},
				{
					HintName: ast.NewCIStr("IGNORE_PLAN_CACHE"),
				},
				{
					HintName: ast.NewCIStr("USE_CASCADES"),
					HintData: true,
				},
				{
					HintName: ast.NewCIStr("QUERY_TYPE"),
					QBName:   ast.NewCIStr("qb1"),
					HintData: ast.NewCIStr("OLAP"),
				},
				{
					HintName: ast.NewCIStr("QUERY_TYPE"),
					HintData: ast.NewCIStr("OLTP"),
				},
				{
					HintName: ast.NewCIStr("NO_INDEX_MERGE"),
				},
				{
					HintName: ast.NewCIStr("RESOURCE_GROUP"),
					HintData: "rg1",
				},
			},
		},
		{
			input: "READ_FROM_STORAGE(@foo TIKV[a, b], TIFLASH[c, d]) HASH_AGG() SEMI_JOIN_REWRITE() READ_FROM_STORAGE(TIKV[e])",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("READ_FROM_STORAGE"),
					HintData: ast.NewCIStr("TIKV"),
					QBName:   ast.NewCIStr("foo"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("a")},
						{TableName: ast.NewCIStr("b")},
					},
				},
				{
					HintName: ast.NewCIStr("READ_FROM_STORAGE"),
					HintData: ast.NewCIStr("TIFLASH"),
					QBName:   ast.NewCIStr("foo"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("c")},
						{TableName: ast.NewCIStr("d")},
					},
				},
				{
					HintName: ast.NewCIStr("HASH_AGG"),
				},
				{
					HintName: ast.NewCIStr("SEMI_JOIN_REWRITE"),
				},
				{
					HintName: ast.NewCIStr("READ_FROM_STORAGE"),
					HintData: ast.NewCIStr("TIKV"),
					Tables: []ast.HintTable{
						{TableName: ast.NewCIStr("e")},
					},
				},
			},
		},
		{
			input: "unknown_hint()",
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "set_var(timestamp = 1.5)",
			errs: []string{
				`Cannot use decimal number`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "set_var(timestamp = _utf8mb4'1234')", // Optimizer hint doesn't recognize _charset'strings'.
			errs:  []string{`Optimizer hint syntax error at line 1 `},
		},
		{
			input: "set_var(timestamp = 9999999999999999999999999999999999999)",
			errs: []string{
				`integer value is out of range`,
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "time_range('2020-02-20 12:12:12',456)",
			errs: []string{
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "time_range(456,'2020-02-20 12:12:12')",
			errs: []string{
				`Optimizer hint syntax error at line 1 `,
			},
		},
		{
			input: "TIME_RANGE('2020-02-20 12:12:12','2020-02-20 13:12:12')",
			output: []*ast.TableOptimizerHint{
				{
					HintName: ast.NewCIStr("TIME_RANGE"),
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
		require.Lenf(t, errs, len(tc.errs), "input = %s,\n... errs = %q", tc.input, errs)
		for i, err := range errs {
			require.Errorf(t, err, "input = %s, i = %d", tc.input, i)
			require.Containsf(t, err.Error(), tc.errs[i], "input = %s, i = %d", tc.input, i)
		}
		require.Equalf(t, tc.output, output, "input = %s,\n... output = %q", tc.input, output)
	}
}
