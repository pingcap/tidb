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
