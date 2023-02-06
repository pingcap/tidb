// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int, c int, j json,
index(a), index(b),
index idx(a, b, (cast(j as signed array)), c),
index idx2(a, b, (cast(j->'$.str' as char(10) array)), c))`)

	tk.MustExec("set tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx2"))
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx2"))

	tk.MustExec("set tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx2"))
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx"))
	tk.MustExec("analyze table t index a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows())
	tk.MustExec("analyze table t index a, idx, idx2")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx",
		"Warning 1105 analyzing multi-valued indexes is not supported, skip idx2"))
}

func TestIndexMergeJSONMemberOf(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(
a int, j0 json, j1 json,
index j0_0((cast(j0->'$.path0' as signed array))),
index j0_1((cast(j0->'$.path1' as signed array))),
index j0_string((cast(j0->'$.path_string' as char(10) array))),
index j0_date((cast(j0->'$.path_date' as date array))),
index j1((cast(j1 as signed array))))`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, query := range input {
		testdata.OnRecord(func() {
			output[i].SQL = query
		})
		result := tk.MustQuery("explain format = 'brief' " + query)
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(result.Rows())
		})
		result.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestDNFOnMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int, c int, j json,
index idx1((cast(j as signed array))),
index idx2(a, b, (cast(j as signed array)), c))`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, query := range input {
		testdata.OnRecord(func() {
			output[i].SQL = query
		})
		result := tk.MustQuery("explain format = 'brief' " + query)
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(result.Rows())
		})
		result.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestCompositeMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, b int , c int, j json,
index idx(a, b, (cast(j as signed array)), c),
index idx2(a, b, (cast(j->'$.str' as char(10) array)), c))`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, query := range input {
		testdata.OnRecord(func() {
			output[i].SQL = query
		})
		result := tk.MustQuery("explain format = 'brief' " + query)
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(result.Rows())
		})
		result.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestMVIndexSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, j json,
index i_int((cast(j->'$.int' as signed array))))`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, query := range input {
		testdata.OnRecord(func() {
			output[i].SQL = query
		})
		result := tk.MustQuery("explain format = 'brief' " + query)
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(result.Rows())
		})
		result.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestMVIndexIndexMergePlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(j json, index kj((cast(j as signed array))))`)

	tk.MustExec("prepare st from 'select /*+ use_index_merge(t, kj) */ * from t where (1 member of (j))'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip plan-cache: query accesses generated columns is un-cacheable"))
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestMVIndexPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(j json, unique kj((cast(j as signed array))))`)

	for _, sql := range []string{
		"select j from t where j=1",
		"select j from t where j=1 or j=2",
		"select j from t where j in (1, 2)",
	} {
		plan := tk.MustQuery("explain " + sql).Rows()
		hasPointGet := false
		for _, line := range plan {
			if strings.Contains(strings.ToLower(line[0].(string)), "point") {
				hasPointGet = true
			}
		}
		require.True(t, !hasPointGet) // no point-get plan
	}
}

func TestEnforceMVIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(a int, j json, index kj((cast(j as signed array))))`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Err  string
	}
	planSuiteData := core.GetIndexMergeSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, query := range input {
		testdata.OnRecord(func() {
			output[i].SQL = query
		})
		rs, err := tk.Exec("explain format = 'brief' " + query)
		if err != nil {
			testdata.OnRecord(func() {
				output[i].Err = err.Error()
				output[i].Plan = nil
			})
			require.Equal(t, output[i].Err, err.Error())
		} else {
			result := tk.ResultSetToResultWithCtx(context.Background(), rs, "")
			testdata.OnRecord(func() {
				output[i].Err = ""
				output[i].Plan = testdata.ConvertRowsToStrings(result.Rows())
			})
			result.Check(testkit.Rows(output[i].Plan...))
		}
	}
}

func TestMVIndexInvisible(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t(a int, j json, index kj((cast(j as signed array))))`)
	tk.MustQuery(`explain format='brief' select /*+ use_index(t, kj) */ * from t where (1 member of (j))`).Check(testkit.Rows(
		`Selection 8000.00 root  json_memberof(cast(1, json BINARY), test.t.j)`,
		`└─IndexMerge 10.00 root  type: union`,
		"  ├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:kj(cast(`j` as signed array)) range:[1,1], keep order:false, stats:pseudo",
		`  └─TableRowIDScan(Probe) 10.00 cop[tikv] table:t keep order:false, stats:pseudo`))

	tk.MustExec(`ALTER TABLE t ALTER INDEX kj INVISIBLE`)
	tk.MustQuery(`explain format='brief' select /*+ use_index(t, kj) */ * from t where (1 member of (j))`).Check(testkit.Rows(
		"Selection 8000.00 root  json_memberof(cast(1, json BINARY), test.t.j)",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery(`explain format='brief' select /*+ use_index_merge(t, kj) */ * from t where (1 member of (j))`).Check(testkit.Rows(
		"Selection 8000.00 root  json_memberof(cast(1, json BINARY), test.t.j)",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))

	tk.MustExec(`ALTER TABLE t ALTER INDEX kj VISIBLE`)
	tk.MustQuery(`explain format='brief' select /*+ use_index(t, kj) */ * from t where (1 member of (j))`).Check(testkit.Rows(
		`Selection 8000.00 root  json_memberof(cast(1, json BINARY), test.t.j)`,
		`└─IndexMerge 10.00 root  type: union`,
		"  ├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:kj(cast(`j` as signed array)) range:[1,1], keep order:false, stats:pseudo",
		`  └─TableRowIDScan(Probe) 10.00 cop[tikv] table:t keep order:false, stats:pseudo`))
}

func TestMVIndexFullScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t(j json, index kj((cast(j as signed array))))`)
	tk.MustExec(`insert into t values ('[1]')`)
	tk.MustExec(`insert into t values ('[1, 2]')`)
	tk.MustExec(`insert into t values ('[]')`)
	tk.MustExec(`insert into t values (NULL)`)

	tk.MustQuery(`select /*+ use_index_merge(t, kj) */ count(*) from t`).Check(testkit.Rows("4"))
	tk.MustQuery(`select /*+ use_index_merge(t, kj) */ count(*) from t where (1 member of (j))`).Check(testkit.Rows("2"))
	tk.MustQuery(`select /*+ use_index_merge(t, kj) */ count(*) from t where json_contains((j), '[1]')`).Check(testkit.Rows("2"))
	tk.MustQuery(`select /*+ use_index_merge(t, kj) */ count(*) from t where json_overlaps((j), '[1]')`).Check(testkit.Rows("2"))

	// Forbid IndexMerge+IndexFullScan since IndexFullScan on MVIndex cannot read all rows some cases.
	tk.MustGetErrMsg(`select /*+ use_index(t, kj) */ count(*) from t`, "[planner:1815]Internal : Can't find a proper physical plan for this query")
}

func TestMVIndexEmptyArray(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t(j json, index kj((cast(j as signed array))))`)
	tk.MustExec(`insert into t values ('[1]')`)
	tk.MustExec(`insert into t values ('[1, 2]')`)
	tk.MustExec(`insert into t values ('[]')`)
	tk.MustExec(`insert into t values (NULL)`)

	for _, cond := range []string{
		"json_contains(j, '[]')",
		"json_contains(j, '[1]')",
		"json_contains(j, '[1, 2]')",
		"json_contains(j, '[1, 10]')",
		"json_overlaps(j, '[]')",
		"json_overlaps(j, '[1]')",
		"json_overlaps(j, '[1, 2]')",
		"json_overlaps(j, '[1, 10]')",
	} {
		tk.MustQuery(fmt.Sprintf("select /*+ use_index_merge(t) */ * from t where %v", cond)).Sort().Check(
			tk.MustQuery(fmt.Sprintf("select /*+ ignore_index(t, kj) */ * from t where %v", cond)).Sort().Rows())
	}
}

func TestMVIndexRandom(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, testCase := range []struct {
		indexType     string
		insertValOpts randMVIndexValOpts
		queryValsOpts randMVIndexValOpts
	}{
		{"signed", randMVIndexValOpts{"signed", 0, 3}, randMVIndexValOpts{"signed", 0, 3}},
		{"unsigned", randMVIndexValOpts{"unsigned", 0, 3}, randMVIndexValOpts{"unsigned", 0, 3}}, // unsigned-index + unsigned-values
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 3, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 1, 3}},
		{"char(3)", randMVIndexValOpts{"string", 3, 3}, randMVIndexValOpts{"string", 5, 3}},
		{"date", randMVIndexValOpts{"date", 0, 3}, randMVIndexValOpts{"date", 0, 3}},
	} {
		tk.MustExec("drop table if exists t")
		tk.MustExec(fmt.Sprintf(`create table t(a int, j json, index kj((cast(j as %v array))))`, testCase.indexType))

		nRows := 20
		rows := make([]string, 0, nRows)
		for i := 0; i < nRows; i++ {
			va, v1, v2 := rand.Intn(testCase.insertValOpts.distinct), randMVIndexValue(testCase.insertValOpts), randMVIndexValue(testCase.insertValOpts)
			if testCase.indexType == "date" {
				rows = append(rows, fmt.Sprintf(`(%v, json_array(cast(%v as date), cast(%v as date)))`, va, v1, v2))
			} else {
				rows = append(rows, fmt.Sprintf(`(%v, '[%v, %v]')`, va, v1, v2))
			}
		}
		tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(rows, ", ")))

		nQueries := 20
		for i := 0; i < nQueries; i++ {
			conds := randMVIndexConds(rand.Intn(3)+1, testCase.queryValsOpts)
			r1 := tk.MustQuery("select /*+ ignore_index(t, kj) */ * from t where " + conds).Sort()
			tk.MustQuery("select /*+ use_index_merge(t, kj) */ * from t where " + conds).Sort().Check(r1.Rows())
		}
	}
}

func randMVIndexConds(nConds int, valOpts randMVIndexValOpts) string {
	var conds string
	for i := 0; i < nConds; i++ {
		if i > 0 {
			if rand.Intn(5) < 1 { // OR
				conds += " OR "
			} else { // AND
				conds += " AND "
			}
		}
		cond := randMVIndexCond(rand.Intn(4), valOpts)
		conds += cond
	}
	return conds
}

func randMVIndexCond(condType int, valOpts randMVIndexValOpts) string {
	switch condType {
	case 0: // member_of
		return fmt.Sprintf(`(%v member of (j))`, randMVIndexValue(valOpts))
	case 1: // json_contains
		return fmt.Sprintf(`json_contains(j, '%v')`, randArray(valOpts))
	case 2: // json_overlaps
		return fmt.Sprintf(`json_overlaps(j, '%v')`, randArray(valOpts))
	default: // others
		return fmt.Sprintf(`a < %v`, rand.Intn(valOpts.distinct))
	}
}

func randArray(opts randMVIndexValOpts) string {
	n := rand.Intn(5) // n can be 0
	var vals []string
	for i := 0; i < n; i++ {
		vals = append(vals, randMVIndexValue(opts))
	}
	return "[" + strings.Join(vals, ", ") + "]"
}

type randMVIndexValOpts struct {
	valType   string // INT, UNSIGNED, STR, DATE
	maxStrLen int
	distinct  int
}

func randMVIndexValue(opts randMVIndexValOpts) string {
	switch strings.ToLower(opts.valType) {
	case "signed":
		return fmt.Sprintf("%v", rand.Intn(opts.distinct)-(opts.distinct/2))
	case "unsigned":
		return fmt.Sprintf("%v", rand.Intn(opts.distinct))
	case "string":
		return fmt.Sprintf(`"%v"`, strings.Repeat(fmt.Sprintf("%v", rand.Intn(opts.distinct)), rand.Intn(opts.maxStrLen)+1))
	case "date":
		return fmt.Sprintf(`"2000-01-%v"`, rand.Intn(opts.distinct)+1)
	}
	return ""
}
