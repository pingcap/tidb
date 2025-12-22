// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pushdowntest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type IndexLookUpPushDownRunVerifier struct {
	*testing.T
	tk          *testkit.TestKit
	tableName   string
	indexName   string
	primaryRows []int
	msg         string
}

type RunSelectWithCheckResult struct {
	SQL         string
	Rows        [][]any
	AnalyzeRows [][]any
}

func (t *IndexLookUpPushDownRunVerifier) RunSelectWithCheck(where string, skip, limit int) RunSelectWithCheckResult {
	require.NotNil(t, t.tk)
	require.NotEmpty(t, t.tableName)
	require.NotEmpty(t, t.indexName)
	require.NotEmpty(t, t.primaryRows)
	require.GreaterOrEqual(t, skip, 0)
	if skip > 0 {
		require.GreaterOrEqual(t, limit, 0)
	}

	message := fmt.Sprintf("%s, table: %s, where: %s, limit: %d", t.msg, t.tableName, where, limit)
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("select /*+ index_lookup_pushdown(%s, %s)*/ * from %s where ", t.tableName, t.indexName, t.tableName))
	sb.WriteString(where)
	if skip > 0 {
		sb.WriteString(fmt.Sprintf(" limit %d, %d", skip, limit))
	} else if limit >= 0 {
		sb.WriteString(fmt.Sprintf(" limit %d", limit))
	}

	// make sure the query uses index lookup
	analyzeSQL := "explain analyze " + sb.String()
	analyzeResult := t.tk.MustQuery(analyzeSQL)
	require.Contains(t, analyzeResult.String(), "LocalIndexLookUp", analyzeSQL+"\n"+analyzeResult.String())

	// get actual result
	rs := t.tk.MustQuery(sb.String())
	actual := rs.Rows()
	idSets := make(map[string]struct{}, len(actual))
	for _, row := range actual {
		var primaryKey strings.Builder
		require.Greater(t, len(t.primaryRows), 0)
		for i, idx := range t.primaryRows {
			if i > 0 {
				primaryKey.WriteString("#")
			}
			primaryKey.WriteString(row[idx].(string))
		}
		id := primaryKey.String()
		_, dup := idSets[id]
		require.False(t, dup, "dupID: "+id+", "+message)
		idSets[row[0].(string)] = struct{}{}
	}

	// use table scan
	matchCondList := t.tk.MustQuery(fmt.Sprintf("select /*+ use_index(%s) */* from %s where "+where, t.tableName, t.tableName)).Rows()
	if limit == 0 || skip >= len(matchCondList) {
		require.Len(t, actual, 0, message)
	} else if limit < 0 {
		// no limit two results should have same members
		require.ElementsMatch(t, matchCondList, actual, message)
	} else {
		expectRowCnt := limit
		if skip+limit > len(matchCondList) {
			expectRowCnt = len(matchCondList) - skip
		}
		require.Len(t, actual, expectRowCnt, message)
		require.Subset(t, matchCondList, actual, message)
	}

	// check in analyze the index is lookup locally
	message = fmt.Sprintf("%s\n%s\n%s", message, analyzeSQL, analyzeResult.String())
	analyzeVerified := false
	localIndexLookUpIndex := -1
	totalIndexScanCnt := -1
	localIndexLookUpRowCnt := -1
	analyzeRows := analyzeResult.Rows()
	metTableRowIDScan := false
	for i, row := range analyzeRows {
		if strings.Contains(row[0].(string), "LocalIndexLookUp") {
			localIndexLookUpIndex = i
			continue
		}

		if strings.Contains(row[0].(string), "TableRowIDScan") && strings.Contains(row[3].(string), "cop[tikv]") {
			var err error
			if !metTableRowIDScan {
				localIndexLookUpRowCnt, err = strconv.Atoi(row[2].(string))
				require.NoError(t, err, message)
				require.GreaterOrEqual(t, localIndexLookUpRowCnt, 0)
				// check actRows for LocalIndexLookUp
				require.Equal(t, analyzeRows[localIndexLookUpIndex][2], row[2], message)
				// get index scan row count
				totalIndexScanCnt, err = strconv.Atoi(analyzeRows[localIndexLookUpIndex+1][2].(string))
				require.NoError(t, err, message)
				require.GreaterOrEqual(t, totalIndexScanCnt, localIndexLookUpRowCnt)
				metTableRowIDScan = true
				continue
			}

			tidbIndexLookUpRowCnt, err := strconv.Atoi(row[2].(string))
			require.NoError(t, err, message)
			if limit < 0 {
				require.Equal(t, totalIndexScanCnt, localIndexLookUpRowCnt+tidbIndexLookUpRowCnt, message)
			} else {
				require.LessOrEqual(t, localIndexLookUpRowCnt+tidbIndexLookUpRowCnt, totalIndexScanCnt, message)
			}
			analyzeVerified = true
			break
		}
	}
	require.True(t, analyzeVerified, analyzeResult.String())
	return RunSelectWithCheckResult{
		SQL:         sb.String(),
		Rows:        actual,
		AnalyzeRows: analyzeRows,
	}
}

func TestRealTiKVIndexLookUpPushDown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id bigint primary key, a bigint, b bigint, index a(a))")
	seed := time.Now().UnixNano()
	logutil.BgLogger().Info("Run TestRealTiKVIndexLookUpPushDown with seed", zap.Int64("seed", seed))
	r := rand.New(rand.NewSource(seed))
	batch := 100
	total := batch * 20
	indexValEnd := 100
	randIndexVal := func() int {
		return r.Intn(indexValEnd)
	}
	for i := 0; i < total; i += batch {
		values := make([]string, 0, batch)
		for j := 0; j < batch; j++ {
			values = append(values, fmt.Sprintf("(%d, %d, %d)", i+j, randIndexVal(), r.Int63()))
		}
		tk.MustExec("insert into t values " + strings.Join(values, ","))
	}

	v := &IndexLookUpPushDownRunVerifier{
		T:           t,
		tk:          tk,
		tableName:   "t",
		indexName:   "a",
		primaryRows: []int{0},
		msg:         fmt.Sprintf("seed: %d", seed),
	}

	v.RunSelectWithCheck("1", 0, -1)
	v.RunSelectWithCheck("1", 0, r.Intn(total*2))
	v.RunSelectWithCheck("1", total/2, r.Intn(total))
	v.RunSelectWithCheck("1", total-10, 20)
	v.RunSelectWithCheck("1", total, 10)
	v.RunSelectWithCheck("1", 10, 0)
	v.RunSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), 0, -1)
	v.RunSelectWithCheck(fmt.Sprintf("a = %d", randIndexVal()), 0, 25)
	v.RunSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), 0, -1)
	v.RunSelectWithCheck(fmt.Sprintf("a < %d", randIndexVal()), 0, r.Intn(100)+1)
	v.RunSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), 0, -1)
	v.RunSelectWithCheck(fmt.Sprintf("a > %d", randIndexVal()), 0, r.Intn(100)+1)
	start := randIndexVal()
	v.RunSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), 0, -1)
	start = randIndexVal()
	v.RunSelectWithCheck(fmt.Sprintf("a >= %d and a < %d", start, start+r.Intn(5)+1), 0, r.Intn(50)+1)
	v.RunSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), 0, -1)
	v.RunSelectWithCheck(fmt.Sprintf("a > %d and b < %d", randIndexVal(), r.Int63()), 0, r.Intn(50)+1)
}

func TestRealTiKVCommonHandleIndexLookUpPushDown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	collations := [][]string{
		{"binary", "binary"},
		{"ascii", "ascii_bin"},
		{"latin1", "latin1_bin"},
		{"gbk", "gbk_bin"},
		{"gbk", "gbk_chinese_ci"},
		{"utf8mb4", "utf8mb4_bin"},
		{"utf8mb4", "utf8mb4_general_ci"},
		{"utf8mb4", "utf8mb4_unicode_ci"},
		{"utf8mb4", "utf8mb4_0900_ai_ci"},
		{"utf8mb4", "utf8mb4_0900_bin"},
	}

	prepareTable := func(v *IndexLookUpPushDownRunVerifier, uniqueIndex bool, charset, collation, primaryKey string) {
		uniquePrefix := ""
		if uniqueIndex {
			uniquePrefix = "unique "
		}
		tk.MustExec("drop table if exists " + v.tableName)
		tk.MustExec("create table " + v.tableName + " (" +
			"id1 varchar(64), " +
			"id2 bigint, " +
			"a bigint, " +
			"b bigint, " +
			"primary key(" + primaryKey + "), " +
			uniquePrefix + "index " + v.indexName + "(a)" +
			") charset=" + charset + " collate=" + collation)
		tk.MustExec("insert into " + v.tableName + " values " +
			"('abcA', 1, 99, 199), " +
			"('abCE', 2, 98, 198), " +
			"('ABdd', 1, 97, 197), " +
			"('aBdc', 2, 96, 196), " +
			"('Defb', 1, 95, 195), " +
			"('defa', 2, 94, 194), " +
			"('efga', 1, 93, 193), " +
			"('aabb', 1, NULL, 192), " +
			"('bbaa', 2, NULL, 191)",
		)
	}

	for i, cs := range collations {
		for j, unique := range []bool{true, false} {
			charset := cs[0]
			collation := cs[1]
			caseName := fmt.Sprintf("%s-%s-unique-%v", charset, collation, unique)
			t.Run(caseName, func(t *testing.T) {
				v := &IndexLookUpPushDownRunVerifier{
					T:           t,
					tk:          tk,
					tableName:   fmt.Sprintf("t_common_handle_%d_%d", i, j),
					indexName:   "idx_a",
					primaryRows: []int{0, 1},
					msg:         fmt.Sprintf("case: %s", caseName),
				}
				prepareTable(v, unique, charset, collation, "id1, id2")
				v.RunSelectWithCheck("1", 0, -1)
				v.RunSelectWithCheck("a > 93 and b < 199", 0, 10)
				v.RunSelectWithCheck("a > 93 and b < 199 and id1 != 'abdc'", 0, 10)
				// check the TopN push down
				result := v.RunSelectWithCheck("a > 0 and id1 not in ('efga', 'ABdd') order by id2, id1", 0, 4)
				require.Contains(t, result.AnalyzeRows[2][0], "LocalIndexLookUp")
				require.Contains(t, result.AnalyzeRows[3][0], "TopN")
				require.Contains(t, result.AnalyzeRows[4][0], "Selection")
				require.Equal(t, "cop[tikv]", result.AnalyzeRows[3][3])
				if strings.Contains(collation, "_ci") {
					require.Equal(t, [][]any{
						{"abcA", "1", "99", "199"},
						{"Defb", "1", "95", "195"},
						{"abCE", "2", "98", "198"},
						{"aBdc", "2", "96", "196"},
					}, result.Rows)
				} else {
					require.Equal(t, [][]any{
						{"Defb", "1", "95", "195"},
						{"abcA", "1", "99", "199"},
						{"aBdc", "2", "96", "196"},
						{"abCE", "2", "98", "198"},
					}, result.Rows)
				}
			})
		}

		// test prefix index column
		v := &IndexLookUpPushDownRunVerifier{
			T:           t,
			tk:          tk,
			tableName:   "t_common_handle_prefix_primary_index",
			indexName:   "idx_a",
			primaryRows: []int{0, 1},
			msg:         "case: t_common_handle_prefix_primary_index",
		}
		prepareTable(v, false, "utf8mb4", "utf8mb4_general_ci", "id1(3), id2")
		v.RunSelectWithCheck("1", 0, -1)

		// test two int column primary key
		v = &IndexLookUpPushDownRunVerifier{
			T:           t,
			tk:          tk,
			tableName:   "t_common_handle_two_int_pk",
			indexName:   "idx_a",
			primaryRows: []int{0, 1},
			msg:         "case: t_common_handle_two_int_pk",
		}
		prepareTable(v, false, "utf8mb4", "utf8mb4_general_ci", "b, id2")
		v.RunSelectWithCheck("1", 0, -1)
	}
}

func TestRealTiKVPartitionIndexLookUpPushDown(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// int handle
	tk.MustExec("create table tp1 (\n" +
		"    a varchar(32),\n" +
		"    b int,\n" +
		"    c int,\n" +
		"    d int,\n" +
		"    primary key(b) CLUSTERED,\n" +
		"    index c(c)\n" +
		")\n" +
		"PARTITION BY RANGE (b) (\n" +
		"    PARTITION p0 VALUES LESS THAN (100),\n" +
		"    PARTITION p1 VALUES LESS THAN (200),\n" +
		"    PARTITION p2 VALUES LESS THAN (300),\n" +
		"    PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
		")")

	// common handle
	tk.MustExec("create table tp2 (\n" +
		"    a varchar(32),\n" +
		"    b int,\n" +
		"    c int,\n" +
		"    d int,\n" +
		"    primary key(a, b) CLUSTERED,\n" +
		"    index c(c)\n" +
		")\n" +
		"PARTITION BY RANGE COLUMNS (a) (\n" +
		"    PARTITION p0 VALUES LESS THAN ('c'),\n" +
		"    PARTITION p1 VALUES LESS THAN ('e'),\n" +
		"    PARTITION p2 VALUES LESS THAN ('g'),\n" +
		"    PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
		")")

	// extra handle
	tk.MustExec("create table tp3 (\n" +
		"    a varchar(32),\n" +
		"    b int,\n" +
		"    c int,\n" +
		"    d int,\n" +
		"    primary key(a, b) NONCLUSTERED,\n" +
		"    index c(c)\n" +
		")\n" +
		"PARTITION BY RANGE COLUMNS (a) (\n" +
		"    PARTITION p0 VALUES LESS THAN ('c'),\n" +
		"    PARTITION p1 VALUES LESS THAN ('e'),\n" +
		"    PARTITION p2 VALUES LESS THAN ('g'),\n" +
		"    PARTITION p3 VALUES LESS THAN MAXVALUE\n" +
		")")

	tableNames := []string{"tp1", "tp2", "tp3"}
	// prepare data
	for _, tableName := range tableNames {
		tk.MustExec("insert into " + tableName + " values " +
			"('a', 10, 1, 100), " +
			"('b', 20, 2, 200), " +
			"('c', 110, 3, 300), " +
			"('d', 120, 4, 400), " +
			"('e', 210, 5, 500), " +
			"('f', 220, 6, 600), " +
			"('g', 330, 5, 700), " +
			"('h', 340, 5, 800), " +
			"('i', 450, 5, 900), " +
			"('j', 550, 6, 1000) ",
		)

		v := &IndexLookUpPushDownRunVerifier{
			T:           t,
			tk:          tk,
			tableName:   tableName,
			indexName:   "c",
			primaryRows: []int{0, 1},
			msg:         tableName,
		}

		if tableName == "tp1" {
			v.primaryRows = []int{1}
		}
		v.RunSelectWithCheck("1", 0, -1)

		// test with uncommitted data
		func() {
			defer tk.MustExec("rollback")
			tk.MustExec("begin")
			tk.MustExec(fmt.Sprintf("insert into %s values ('b2', 22, 2, 202),  ('d3', 123, 4, 203)", tableName))
			result := v.RunSelectWithCheck("1 order by d", 0, 5)
			require.Equal(t, [][]any{
				{"a", "10", "1", "100"},
				{"b", "20", "2", "200"},
				{"b2", "22", "2", "202"},
				{"d3", "123", "4", "203"},
				{"c", "110", "3", "300"},
			}, result.Rows)
		}()
	}
}
