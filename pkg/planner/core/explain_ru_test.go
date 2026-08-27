// Copyright 2026 PingCAP, Inc.
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
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestExplainAnalyzeRUFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")

	var input []struct {
		SQL string
	}
	var output []struct {
		SQL  string
		Rows [][]string
	}
	suiteData := core.GetExplainAnalyzeRUSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	require.Equal(t, len(input), len(output))

	toStringRows := func(rows [][]any) [][]string {
		stringRows := make([][]string, len(rows))
		for i, row := range rows {
			stringRows[i] = make([]string, len(row))
			for j, col := range row {
				stringRows[i][j] = col.(string)
			}
		}
		return stringRows
	}

	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt.SQL
			output[i].Rows = toStringRows(tk.MustQuery(tt.SQL).Rows())
		})
		require.Equal(t, tt.SQL, output[i].SQL)
		require.Equal(t, output[i].Rows, toStringRows(tk.MustQuery(tt.SQL).Rows()))
	}
}

func TestExplainAnalyzeRUFormatEndToEndMonotonicity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	const (
		selfRUColumn = 3
		cumRUColumn  = 4
	)

	explainRU := func(tb testing.TB, sql string) [][]any {
		tb.Helper()
		rows := tk.MustQuery("explain analyze format = 'ru' " + sql).Rows()
		require.NotEmpty(tb, rows)
		return rows
	}
	getOperatorRU := func(tb testing.TB, rows [][]any, operator string, column int) float64 {
		tb.Helper()
		for _, row := range rows {
			require.Greater(tb, len(row), column)
			id, ok := row[0].(string)
			require.True(tb, ok)
			if !strings.Contains(id, operator) {
				continue
			}
			ruText, ok := row[column].(string)
			require.True(tb, ok)
			require.NotEmpty(tb, ruText)
			ru, err := strconv.ParseFloat(ruText, 64)
			require.NoError(tb, err)
			return ru
		}
		require.FailNowf(tb, "operator not found", "operator %s not found in rows %v", operator, rows)
		return 0
	}
	insertIntRows := func(table string, start, count int) {
		var sql strings.Builder
		sql.WriteString("insert into ")
		sql.WriteString(table)
		sql.WriteString(" values ")
		for i := 0; i < count; i++ {
			if i > 0 {
				sql.WriteString(", ")
			}
			a := start + i
			sql.WriteString("(")
			sql.WriteString(strconv.Itoa(a))
			sql.WriteString(", ")
			sql.WriteString(strconv.Itoa(a + 1000))
			sql.WriteString(")")
		}
		tk.MustExec(sql.String())
	}
	insertIndexedRows := func(table string, start, count int) {
		var sql strings.Builder
		sql.WriteString("insert into ")
		sql.WriteString(table)
		sql.WriteString(" values ")
		for i := 0; i < count; i++ {
			if i > 0 {
				sql.WriteString(", ")
			}
			a := start + i
			aText := strconv.Itoa(a)
			sql.WriteString("(")
			sql.WriteString(aText)
			sql.WriteString(", concat(lpad(")
			sql.WriteString(aText)
			sql.WriteString(", 6, '0'), repeat('x', 256)), repeat('y', 256))")
		}
		tk.MustExec(sql.String())
	}

	t.Run("Reader cumRU increases with scanned bytes", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_scan_bytes")
		tk.MustExec("create table t_unistore_ru_scan_bytes(a int primary key, b varchar(4096))")
		getReaderCumRU := func() float64 {
			rows := explainRU(t, "select * from t_unistore_ru_scan_bytes")
			return getOperatorRU(t, rows, "TableReader", cumRUColumn)
		}

		previousRU := getReaderCumRU()
		for i := 0; i < 20; i++ {
			firstID := i*2 + 1
			tk.MustExec("insert into t_unistore_ru_scan_bytes values (" + strconv.Itoa(firstID) + ", repeat('a', 4096)), (" + strconv.Itoa(firstID+1) + ", repeat('b', 4096))")
			currentRU := getReaderCumRU()
			require.Greater(t, currentRU, previousRU)
			previousRU = currentRU
		}
	})

	t.Run("Scan RU is attributed to the owning Reader", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_scan_attribution")
		tk.MustExec("create table t_unistore_ru_scan_attribution(a int primary key, b varchar(4096))")
		tk.MustExec("insert into t_unistore_ru_scan_attribution values (1, repeat('a', 4096)), (2, repeat('b', 4096))")

		rows := explainRU(t, "select * from t_unistore_ru_scan_attribution")
		require.Positive(t, getOperatorRU(t, rows, "TableReader", cumRUColumn))
		require.Zero(t, getOperatorRU(t, rows, "TableFullScan", selfRUColumn))
		require.Zero(t, getOperatorRU(t, rows, "TableFullScan", cumRUColumn))
	})

	t.Run("Selection selfRU increases with input rows", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_selection_rows")
		tk.MustExec("create table t_unistore_ru_selection_rows(a int, b int)")
		insertIntRows("t_unistore_ru_selection_rows", 0, 20)
		smallRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_selection_rows where a >= 0 and b >= 0"), "Selection", selfRUColumn)

		insertIntRows("t_unistore_ru_selection_rows", 20, 80)
		largeRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_selection_rows where a >= 0 and b >= 0"), "Selection", selfRUColumn)
		require.Greater(t, largeRU, smallRU)
	})

	t.Run("Selection selfRU increases with condition count", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_selection_conditions")
		tk.MustExec("create table t_unistore_ru_selection_conditions(a int, b int)")
		insertIntRows("t_unistore_ru_selection_conditions", 0, 100)

		oneConditionRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_selection_conditions where a >= 0"), "Selection", selfRUColumn)
		threeConditionsRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_selection_conditions where a >= 0 and b >= 0 and a < 100000"), "Selection", selfRUColumn)
		require.Greater(t, threeConditionsRU, oneConditionRU)
	})

	t.Run("Sort selfRU increases with input rows", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_sort_rows")
		tk.MustExec("create table t_unistore_ru_sort_rows(a int, b int)")
		insertIntRows("t_unistore_ru_sort_rows", 0, 20)
		smallRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_sort_rows order by b"), "Sort", selfRUColumn)

		insertIntRows("t_unistore_ru_sort_rows", 20, 80)
		largeRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_sort_rows order by b"), "Sort", selfRUColumn)
		require.Greater(t, largeRU, smallRU)
	})

	t.Run("TopN selfRU increases with retained rows", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_topn_retained_rows")
		tk.MustExec("create table t_unistore_ru_topn_retained_rows(a int, b int)")
		insertIntRows("t_unistore_ru_topn_retained_rows", 0, 100)

		limitOneRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_topn_retained_rows order by b limit 1"), "TopN", selfRUColumn)
		limitTenRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_topn_retained_rows order by b limit 10"), "TopN", selfRUColumn)
		require.Greater(t, limitTenRU, limitOneRU)
	})

	t.Run("Limit selfRU increases with retained rows", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_limit_retained_rows")
		tk.MustExec("create table t_unistore_ru_limit_retained_rows(a int, b int)")
		insertIntRows("t_unistore_ru_limit_retained_rows", 0, 100)

		limitOneRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_limit_retained_rows limit 1"), "Limit", selfRUColumn)
		limitTenRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_limit_retained_rows limit 10"), "Limit", selfRUColumn)
		require.Greater(t, limitTenRU, limitOneRU)
	})

	t.Run("Full Sort selfRU is greater than TopN selfRU for the same input", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_sort_topn")
		tk.MustExec("create table t_unistore_ru_sort_topn(a int, b int)")
		insertIntRows("t_unistore_ru_sort_topn", 0, 100)

		sortRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_sort_topn order by b"), "Sort", selfRUColumn)
		topNRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_sort_topn order by b limit 10"), "TopN", selfRUColumn)
		require.Greater(t, sortRU, topNRU)
	})

	t.Run("IndexReader cumRU increases with scanned bytes", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_index_reader")
		tk.MustExec("create table t_unistore_ru_index_reader(a int primary key, b varchar(512), c varchar(512), key idx_b(b))")
		insertIndexedRows("t_unistore_ru_index_reader", 0, 20)
		smallRU := getOperatorRU(t, explainRU(t, "select b from t_unistore_ru_index_reader use index(idx_b) where b >= ''"), "IndexReader", cumRUColumn)

		insertIndexedRows("t_unistore_ru_index_reader", 20, 80)
		largeRU := getOperatorRU(t, explainRU(t, "select b from t_unistore_ru_index_reader use index(idx_b) where b >= ''"), "IndexReader", cumRUColumn)
		require.Greater(t, largeRU, smallRU)
	})

	t.Run("IndexLookup cumRU increases with scanned bytes", func(t *testing.T) {
		tk.MustExec("drop table if exists t_unistore_ru_index_lookup")
		tk.MustExec("create table t_unistore_ru_index_lookup(a int primary key, b varchar(512), c varchar(512), key idx_b(b))")
		insertIndexedRows("t_unistore_ru_index_lookup", 0, 20)
		smallRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_index_lookup use index(idx_b) where b >= ''"), "IndexLookUp", cumRUColumn)

		insertIndexedRows("t_unistore_ru_index_lookup", 20, 80)
		largeRU := getOperatorRU(t, explainRU(t, "select * from t_unistore_ru_index_lookup use index(idx_b) where b >= ''"), "IndexLookUp", cumRUColumn)
		require.Greater(t, largeRU, smallRU)
	})
}
