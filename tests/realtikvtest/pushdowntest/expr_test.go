// Copyright 2024 PingCAP, Inc.
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

package pushdowntest

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// TestBitCastInTiKV see issue: https://github.com/pingcap/tidb/issues/56494
func TestBitCastInTiKV(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	defer tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a bit(24))")
	tk.MustExec("insert into t1 values(0xffffff)")
	err := tk.QueryToErr("select a from t1 where false not like convert(a, char)")
	require.EqualError(t, err, "[tikv:3854]Cannot convert string '\\xFF\\xFF\\xFF' from binary to utf8mb4")
}

func TestTrimPushDownToTiKV(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	resetTrimPushdownBlacklist := func() {
		tk.MustExec("delete from mysql.expr_pushdown_blacklist where name='trim' and store_type='tikv' and reason='for trim realtikv test'")
		tk.MustExec("admin reload expr_pushdown_blacklist")
	}
	checkTrimExplainPushed := func(sql string) {
		rows := tk.MustQuery("explain analyze format='brief' " + sql).Rows()
		found := false
		for _, row := range rows {
			op, ok := row[0].(string)
			require.True(t, ok, sql)
			task, ok := row[3].(string)
			require.True(t, ok, sql)
			info, ok := row[6].(string)
			require.True(t, ok, sql)
			if strings.Contains(op, "Selection") && task == "cop[tikv]" && strings.Contains(info, "trim(") {
				found = true
				break
			}
		}
		require.True(t, found, sql)
	}
	checkTrimResultConsistency := func(sql string, expected []string) {
		resetTrimPushdownBlacklist()
		rowsOn := tk.MustQuery(sql).Rows()
		require.EqualValues(t, testkit.Rows(expected...), rowsOn, sql)

		tk.MustExec("insert into mysql.expr_pushdown_blacklist values('trim', 'tikv', 'for trim realtikv test')")
		tk.MustExec("admin reload expr_pushdown_blacklist")

		rowsOff := tk.MustQuery(sql).Rows()
		require.EqualValues(t, rowsOn, rowsOff, sql)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_trim_pushdown")
	t.Cleanup(func() {
		resetTrimPushdownBlacklist()
		tk.MustExec("drop table if exists t_trim_pushdown")
	})

	tk.MustExec(`create table t_trim_pushdown(
		id int primary key,
		c varchar(64) charset utf8mb4 collate utf8mb4_general_ci,
		c_bin varchar(64) charset utf8mb4 collate utf8mb4_bin,
		c_ascii varchar(64) charset ascii collate ascii_bin,
		vb varbinary(64)
	)`)
	tk.MustExec(`insert into t_trim_pushdown values
		(1, '  x  ', '  x  ', '  x  ', x'207820'),
		(2, 'xybarxy', 'xybarxy', 'xybarxy', x'20787920'),
		(3, 'xyxybarxy', 'xyxybarxy', 'xyxybarxy', x'2078797820'),
		(4, 'xybarxyxy', 'xybarxyxy', 'xybarxyxy', x'207879787920'),
		(5, 'xyxybarxyxy', 'xyxybarxyxy', 'xyxybarxyxy', x'207879787920'),
		(6, '', '', '', x''),
		(7, null, null, null, null),
		(8, '   ', '   ', '   ', x'202020'),
		(9, '好好bar好好', '好好bar好好', null, x''),
		(10, 'bar', 'bar', 'bar', x'626172'),
		(11, 'x', 'x', 'x', x'78'),
		(12, 'ff', 'ff', 'ff', x'20ff20'),
		(13, null, null, null, x'20ff7820'),
		(14, null, null, null, x'00207820')`)

	explainCases := []string{
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(c) = 'x'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim('xy' from c) = 'bar'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(leading 'xy' from c) = 'barxy'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(trailing 'xy' from c) = 'xybar'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(both 'xy' from c) = 'bar'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(c) is null",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(c) = ''",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim('' from c) = 'bar'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim('xy' from c) = 'x'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim('xy' from c) is null",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(c_ascii) = 'x'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(vb) = x''",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(vb) = x'ff78'",
		"select /*+ read_from_storage(tikv[t_trim_pushdown]) */ * from t_trim_pushdown where trim(vb) = x'002078'",
	}
	for _, sql := range explainCases {
		checkTrimExplainPushed(sql)
	}

	resultCases := []struct {
		sql      string
		expected []string
	}{
		{
			sql:      "select id from t_trim_pushdown where trim(c) = 'x' order by id",
			expected: []string{"1", "11"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim('xy' from c) = 'bar' order by id",
			expected: []string{"2", "3", "4", "5", "10"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(leading 'xy' from c) = 'barxy' order by id",
			expected: []string{"2", "3"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(trailing 'xy' from c) = 'xybar' order by id",
			expected: []string{"2", "4"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(both 'xy' from c) = 'bar' order by id",
			expected: []string{"2", "3", "4", "5", "10"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(c) = '' order by id",
			expected: []string{"6", "8"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim('' from c) = 'bar' order by id",
			expected: []string{"10"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim('xy' from c) = 'x' order by id",
			expected: []string{"11"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim('xy' from c) is null order by id",
			expected: []string{"7", "13", "14"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(c_ascii) = 'x' order by id",
			expected: []string{"1", "11"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(vb) = x'' order by id",
			expected: []string{"6", "8", "9"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim('好' from c_bin) = 'bar' and c_bin like '%好%' order by id",
			expected: []string{"9"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(vb) = x'ff' order by id",
			expected: []string{"12"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(vb) = x'ff78' order by id",
			expected: []string{"13"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(vb) = x'002078' order by id",
			expected: []string{"14"},
		},
		{
			sql:      "select id from t_trim_pushdown where trim(c) is null order by id",
			expected: []string{"7", "13", "14"},
		},
	}
	for _, tc := range resultCases {
		checkTrimResultConsistency(tc.sql, tc.expected)
	}
}
