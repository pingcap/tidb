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
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestTiDBRowIDBindingAndEvaluation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a bigint primary key clustered, b int)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")

	rows := tk.MustQuery("select tidb_row_id(), tidb_row_id(t), a from t order by a").Rows()
	require.Len(t, rows, 2)
	var tableID int64
	for i, row := range rows {
		implicitToken := row[0].(string)
		require.Equal(t, implicitToken, row[1])
		require.Equal(t, strings.ToLower(implicitToken), implicitToken)
		physicalID, handle, _, canonical, err := tablecodec.DecodeTiDBRowID(implicitToken)
		require.NoError(t, err)
		require.Equal(t, implicitToken, canonical)
		require.True(t, handle.IsInt())
		require.Equal(t, int64(i+1), handle.IntValue())
		if i == 0 {
			tableID = physicalID
		} else {
			require.Equal(t, tableID, physicalID)
		}
	}
	foreignToken, err := tablecodec.EncodeTiDBRowID(tableID+1, kv.IntHandle(1))
	require.NoError(t, err)
	requireInvalidToken := func(sql string, args ...any) {
		t.Helper()
		err := tk.ExecToErr(sql, args...)
		require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	}

	tk.MustExec("set @tidb_row_id_token = (select tidb_row_id() from t where a = 2)")
	tk.MustQuery("select a from t where tidb_row_id() = @tidb_row_id_token").Check(testkit.Rows("2"))
	tk.MustQuery("select a from t where tidb_row_id(t) = ?", rows[1][0]).Check(testkit.Rows("2"))
	tk.MustQuery("explain format = 'brief' select a from t where tidb_row_id(t) = ?", rows[1][0]).CheckContain("Point_Get")
	tk.MustQuery("select a from t where ? = tidb_row_id(t)", strings.ToUpper(rows[1][0].(string))).Check(testkit.Rows("2"))
	requireInvalidToken("select a from t where tidb_row_id() = 'malformed'")
	requireInvalidToken(
		"select a from t where tidb_row_id() in ('" + rows[0][0].(string) + "', 'malformed')",
	)
	tk.MustQuery("select a from t where tidb_row_id() in (?, null) order by a", rows[1][0]).Check(testkit.Rows("2"))
	tk.MustQuery("explain format = 'brief' select a from t where tidb_row_id() in (?, ?)", rows[0][0], rows[1][0]).CheckContain("Batch_Point_Get")
	tk.MustExec(`set @@tidb_opt_fix_control = "52592:ON"`)
	tk.MustQuery("select a from t where tidb_row_id() = ?", rows[1][0]).Check(testkit.Rows("2"))
	tk.MustQuery("explain format = 'brief' select a from t where tidb_row_id() = ?", rows[1][0]).CheckNotContain("Point_Get")
	tk.MustExec(`set @@tidb_opt_fix_control = ""`)
	tk.MustQuery("select a from t where tidb_row_id() in (?, null)", foreignToken).Check(testkit.Rows())
	tk.MustQuery("select a from t where tidb_row_id() not in (?, null)", rows[1][0]).Check(testkit.Rows())
	tk.MustQuery("select a from t where tidb_row_id() not in (?) order by a", rows[1][0]).Check(testkit.Rows("1"))

	tk.MustExec("prepare tidb_row_id_stmt from 'select a from t where tidb_row_id() = ?'")
	tk.MustExec("set @tidb_row_id_token = ?", rows[0][0])
	tk.MustQuery("execute tidb_row_id_stmt using @tidb_row_id_token").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("set @tidb_row_id_token = 'malformed'")
	requireInvalidToken("execute tidb_row_id_stmt using @tidb_row_id_token")
	tk.MustExec("set @tidb_row_id_token = ?", strings.ToUpper(rows[1][0].(string)))
	tk.MustQuery("execute tidb_row_id_stmt using @tidb_row_id_token").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare tidb_row_id_stmt")
	tk.MustQuery("select tidb_row_id() from t group by tidb_row_id() order by tidb_row_id()").Check(
		testkit.Rows(rows[0][0].(string), rows[1][0].(string)),
	)
	tk.MustQuery("select tidb_row_id(), count(*) from t group by a order by a").Check(
		testkit.Rows(rows[0][0].(string)+" 1", rows[1][0].(string)+" 1"),
	)
	tk.MustQuery("select tidb_row_id() as rid, count(*) as row_count from t group by rid order by rid").Check(
		testkit.Rows(rows[0][0].(string)+" 1", rows[1][0].(string)+" 1"),
	)
	tk.MustExec("set sql_mode = ''")
	tk.MustQuery("select tidb_row_id(), count(*) from t").Check(testkit.Rows(rows[0][0].(string) + " 2"))
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustGetErrCode("select tidb_row_id(), count(*) from t", errno.ErrMixOfGroupFuncAndFieldsIncompatible)
	tk.MustExec("update t set b = 21 where tidb_row_id(t) = ?", rows[1][0])
	tk.MustQuery("select b from t where a = 2").Check(testkit.Rows("21"))
	tk.MustQuery("select tidb_row_id() from t where a = 2").Check(testkit.Rows(rows[1][0].(string)))
	tk.MustExec("delete from t where tidb_row_id() = ?", rows[0][0])
	tk.MustQuery("select a, b from t").Check(testkit.Rows("2 21"))
	testTiDBRowIDIndexPaths(t, tk)
	testTiDBRowIDMultiTableDML(t, tk)
}

func testTiDBRowIDIndexPaths(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("create table locator_index (a bigint primary key clustered, b int, c int, index idx_b (b), index idx_c (c))")
	tk.MustExec("insert into locator_index values (1, 10, 100), (2, 20, 200), (3, 30, 300)")
	rows := tk.MustQuery("select tidb_row_id(), a from locator_index order by a").Rows()
	tk.MustQuery("select tidb_row_id(), b from locator_index force index (idx_b) order by a").Check(
		testkit.Rows(rows[0][0].(string)+" 10", rows[1][0].(string)+" 20", rows[2][0].(string)+" 30"),
	)
	tk.MustQuery("explain format = 'brief' select tidb_row_id(), b from locator_index force index (idx_b) where b >= 10").CheckContain("IndexReader")
	tk.MustQuery("select tidb_row_id(), c from locator_index force index (idx_b) where b >= 20 order by a").Check(
		testkit.Rows(rows[1][0].(string)+" 200", rows[2][0].(string)+" 300"),
	)
	tk.MustQuery("explain format = 'brief' select tidb_row_id(), c from locator_index force index (idx_b) where b >= 20").CheckContain("IndexLookUp")
	tk.MustQuery("select /*+ use_index_merge(locator_index, idx_b, idx_c) */ tidb_row_id(), a from locator_index where b = 10 or c = 300 order by a").Check(
		testkit.Rows(rows[0][0].(string)+" 1", rows[2][0].(string)+" 3"),
	)
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(locator_index, idx_b, idx_c) */ tidb_row_id(), a from locator_index where b = 10 or c = 300").CheckContain("IndexMerge")

	tk.MustExec("create table locator_partition_index (a int, b int, c int, primary key (a) nonclustered, index idx_b (b), index idx_c (c) global) partition by range (a) (partition p0 values less than (3), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into locator_partition_index values (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400)")
	partitionRows := tk.MustQuery("select tidb_row_id(), a from locator_partition_index order by a").Rows()
	tk.MustQuery("select tidb_row_id(), b from locator_partition_index force index (idx_b) order by a").Check(
		testkit.Rows(
			partitionRows[0][0].(string)+" 10",
			partitionRows[1][0].(string)+" 20",
			partitionRows[2][0].(string)+" 30",
			partitionRows[3][0].(string)+" 40",
		),
	)
	tk.MustQuery("select tidb_row_id(), c from locator_partition_index force index (idx_c) order by a").Check(
		testkit.Rows(
			partitionRows[0][0].(string)+" 100",
			partitionRows[1][0].(string)+" 200",
			partitionRows[2][0].(string)+" 300",
			partitionRows[3][0].(string)+" 400",
		),
	)
}

func testTiDBRowIDMultiTableDML(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("create table locator_dml_left (a bigint primary key clustered, b int)")
	tk.MustExec("create table locator_dml_right (a bigint primary key clustered, b int)")
	tk.MustExec("insert into locator_dml_left values (1, 10), (2, 20)")
	tk.MustExec("insert into locator_dml_right values (1, 100), (2, 200)")
	leftTokens := tk.MustQuery("select tidb_row_id(), a from locator_dml_left order by a").Rows()
	tk.MustExec(
		"update locator_dml_left l join locator_dml_right r on l.a = r.a set l.b = l.b + 1 where tidb_row_id(l) = ?",
		leftTokens[1][0],
	)
	tk.MustQuery("select a, b from locator_dml_left order by a").Check(testkit.Rows("1 10", "2 21"))
	tk.MustExec(
		"delete l from locator_dml_left l join locator_dml_right r on l.a = r.a where tidb_row_id(l) = ?",
		leftTokens[0][0],
	)
	tk.MustQuery("select a, b from locator_dml_left").Check(testkit.Rows("2 21"))

	tk.MustExec("create table locator_self_dml (a bigint primary key clustered, b int)")
	tk.MustExec("insert into locator_self_dml values (1, 10), (2, 20)")
	selfTokens := tk.MustQuery("select tidb_row_id() from locator_self_dml order by a").Rows()
	tk.MustExec(
		"update locator_self_dml target join locator_self_dml peer on target.a < peer.a "+
			"set target.b = target.b + 1 where tidb_row_id(peer) = ?",
		selfTokens[0][0],
	)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.MustExec(
		"update locator_self_dml target join locator_self_dml peer on target.a < peer.a "+
			"set target.b = target.b + 1 where tidb_row_id(peer) = ?",
		selfTokens[1][0],
	)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select a, b from locator_self_dml order by a").Check(testkit.Rows("1 11", "2 20"))
	tk.MustExec(
		"delete target from locator_self_dml target join locator_self_dml peer on target.a < peer.a "+
			"where tidb_row_id(peer) = ?",
		selfTokens[0][0],
	)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.MustExec(
		"delete target from locator_self_dml target join locator_self_dml peer on target.a < peer.a "+
			"where tidb_row_id(peer) = ?",
		selfTokens[1][0],
	)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select a, b from locator_self_dml").Check(testkit.Rows("2 20"))
}

func TestTiDBRowIDOrderedRanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table range_t (a bigint primary key clustered, b int)")
	tk.MustExec("insert into range_t values (1, 10), (2, 20), (3, 30), (4, 40)")
	tokens := tk.MustQuery("select tidb_row_id() from range_t order by a").Rows()

	tk.MustQuery("select a from range_t where tidb_row_id() > ? order by a", tokens[0][0]).Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select a from range_t where ? < tidb_row_id() order by a", tokens[1][0]).Check(testkit.Rows("3", "4"))
	tk.MustQuery("select a from range_t where tidb_row_id() < ? order by a", tokens[2][0]).Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from range_t where tidb_row_id() <= ? order by a", tokens[1][0]).Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from range_t where tidb_row_id() >= ? order by a", tokens[1][0]).Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select a from range_t where ? >= tidb_row_id() order by a", tokens[1][0]).Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from range_t where tidb_row_id() >= ? order by a", strings.ToUpper(tokens[1][0].(string))).Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select a from range_t where tidb_row_id() between ? and ? order by a", tokens[1][0], tokens[2][0]).Check(testkit.Rows("2", "3"))
	tk.MustQuery("select a from range_t where tidb_row_id() between ? and ?", tokens[1][0], tokens[1][0]).Check(testkit.Rows("2"))
	tk.MustQuery("select a from range_t where tidb_row_id() between ? and ?", tokens[2][0], tokens[1][0]).Check(testkit.Rows())
	tk.MustQuery("select a from range_t where tidb_row_id() > ? and tidb_row_id() <= ? order by a", tokens[0][0], tokens[2][0]).Check(testkit.Rows("2", "3"))
	tk.MustQuery("select a from range_t where tidb_row_id() > null").Check(testkit.Rows())
	tk.MustQuery("select a from range_t where tidb_row_id() not between ? and ? order by a", tokens[1][0], tokens[2][0]).Check(testkit.Rows("1", "4"))
	tk.MustQuery("select a from range_t where tidb_row_id() > concat('', ?) order by a", tokens[1][0]).Check(testkit.Rows("3", "4"))
	tk.MustQuery("explain format = 'brief' select a from range_t where tidb_row_id() > ?", tokens[1][0]).CheckContain("TableRangeScan")

	physicalID, _, _, _, err := tablecodec.DecodeTiDBRowID(tokens[0][0].(string))
	require.NoError(t, err)
	foreign, err := tablecodec.EncodeTiDBRowID(physicalID+1, kv.IntHandle(1))
	require.NoError(t, err)
	tk.MustExec("create table incompatible_range (a varchar(10), primary key (a) clustered)")
	tk.MustExec("insert into incompatible_range values ('x')")
	incompatible := tk.MustQuery("select tidb_row_id() from incompatible_range").Rows()[0][0].(string)

	tk.MustQuery("select a from range_t where tidb_row_id() = null").Check(testkit.Rows())
	tk.MustQuery("select a from range_t where tidb_row_id() = ?", incompatible).Check(testkit.Rows())
	tk.MustQuery("select a from range_t where tidb_row_id() in (?, ?, ?) order by a", strings.ToUpper(tokens[1][0].(string)), incompatible, foreign).Check(testkit.Rows("2"))
	tk.MustQuery("select a from range_t where tidb_row_id() in (?, ?, ?) order by a", tokens[0][0], tokens[0][0], tokens[1][0]).Check(testkit.Rows("1", "2"))

	for _, testCase := range []struct {
		op            string
		directCount   string
		reversedCount string
	}{
		{op: "<", directCount: "1", reversedCount: "2"},
		{op: "<=", directCount: "2", reversedCount: "3"},
		{op: ">", directCount: "2", reversedCount: "1"},
		{op: ">=", directCount: "3", reversedCount: "2"},
	} {
		upperToken := strings.ToUpper(tokens[1][0].(string))
		tk.MustQuery("select count(*) from range_t where tidb_row_id() "+testCase.op+" ?", upperToken).Check(
			testkit.Rows(testCase.directCount),
		)
		tk.MustQuery("select count(*) from range_t where ? "+testCase.op+" tidb_row_id()", upperToken).Check(
			testkit.Rows(testCase.reversedCount),
		)
		tk.MustQuery("select a from range_t where tidb_row_id() " + testCase.op + " null").Check(testkit.Rows())
		tk.MustQuery("select a from range_t where null " + testCase.op + " tidb_row_id()").Check(testkit.Rows())

		for _, invalid := range []string{"malformed", foreign, incompatible} {
			err = tk.ExecToErr("select a from range_t where tidb_row_id() "+testCase.op+" ?", invalid)
			require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), "%s direct with %q: %v", testCase.op, invalid, err)
			err = tk.ExecToErr("select a from range_t where ? "+testCase.op+" tidb_row_id()", invalid)
			require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), "%s reversed with %q: %v", testCase.op, invalid, err)
		}
	}

	tk.MustQuery(
		"select a from range_t where tidb_row_id() between ? and ? order by a",
		strings.ToUpper(tokens[1][0].(string)), strings.ToUpper(tokens[2][0].(string)),
	).Check(testkit.Rows("2", "3"))
	for _, bounds := range [][2]any{
		{"malformed", tokens[2][0]},
		{tokens[1][0], "malformed"},
		{foreign, tokens[2][0]},
		{tokens[1][0], foreign},
		{incompatible, tokens[2][0]},
		{tokens[1][0], incompatible},
		{nil, "malformed"},
		{"malformed", nil},
	} {
		err = tk.ExecToErr("select a from range_t where tidb_row_id() between ? and ?", bounds[0], bounds[1])
		require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), "BETWEEN bounds %v: %v", bounds, err)
	}
	err = tk.ExecToErr(
		"select a from range_t where tidb_row_id() not between ? and ?",
		tokens[0][0], "malformed",
	)
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustQuery("select a from range_t where tidb_row_id() = ? or a = 4 order by a", tokens[1][0]).Check(testkit.Rows("2", "4"))
	err = tk.QueryToErr("select a from range_t where tidb_row_id() = 'malformed' or a = 1")
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("create table range_tie_break (a bigint primary key clustered, b int)")
	tk.MustExec("insert into range_tie_break values (1, 10), (2, 10), (3, 20), (4, 20)")
	tieBreakRows := tk.MustQuery("select tidb_row_id(), a, b from range_tie_break order by b, tidb_row_id()").Rows()
	firstTieBreakPage := tk.MustQuery("select tidb_row_id(), a, b from range_tie_break order by b, tidb_row_id() limit 3").Rows()
	secondTieBreakPage := tk.MustQuery(
		"select tidb_row_id(), a, b from range_tie_break where b > ? or (b = ? and tidb_row_id() > ?) order by b, tidb_row_id() limit 3",
		firstTieBreakPage[2][2],
		firstTieBreakPage[2][2],
		firstTieBreakPage[2][0],
	).Rows()
	require.Equal(t, tieBreakRows, append(firstTieBreakPage, secondTieBreakPage...))

	for _, sql := range []string{
		"select a from range_t where tidb_row_id() > 'malformed'",
		"select a from range_t where tidb_row_id() <= '" + foreign + "'",
		"select a from range_t where tidb_row_id() between '" + tokens[0][0].(string) + "' and 'malformed'",
	} {
		err := tk.ExecToErr(sql)
		require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	}

	tk.MustExec("prepare range_stmt from 'select a from range_t where tidb_row_id() > ? order by a'")
	tk.MustExec("set @range_token = ?", tokens[0][0])
	tk.MustQuery("execute range_stmt using @range_token").Check(testkit.Rows("2", "3", "4"))
	tk.MustExec("set @range_token = ?", tokens[2][0])
	tk.MustQuery("execute range_stmt using @range_token").Check(testkit.Rows("4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @range_token = 'malformed'")
	err = tk.ExecToErr("execute range_stmt using @range_token")
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("deallocate prepare range_stmt")
	tk.MustExec("prepare between_stmt from 'select a from range_t where tidb_row_id() between ? and ?'")
	tk.MustExec("set @range_low = null, @range_high = ?", tokens[2][0])
	tk.MustQuery("execute between_stmt using @range_low, @range_high").Check(testkit.Rows())
	tk.MustExec("set @range_high = 'malformed'")
	err = tk.ExecToErr("execute between_stmt using @range_low, @range_high")
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("deallocate prepare between_stmt")

	tk.MustExec("set tidb_enable_non_prepared_plan_cache = 1")
	nonPreparedRangeSQL := func(token string) string {
		return "select a from range_t where tidb_row_id() > '" + token + "' and a >= 0 order by a"
	}
	tk.MustQuery(nonPreparedRangeSQL(tokens[0][0].(string))).Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery(nonPreparedRangeSQL(tokens[2][0].(string))).Check(testkit.Rows("4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	err = tk.ExecToErr(nonPreparedRangeSQL("malformed"))
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustQuery(nonPreparedRangeSQL(strings.ToUpper(tokens[1][0].(string)))).Check(testkit.Rows("3", "4"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("update range_t set b = b + 1 where tidb_row_id() >= ?", tokens[2][0])
	tk.MustQuery("select a, b from range_t order by a").Check(testkit.Rows("1 10", "2 20", "3 31", "4 41"))
	tk.MustExec("delete from range_t where tidb_row_id() > ?", tokens[2][0])
	tk.MustQuery("select a from range_t order by a").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("begin")
	tk.MustExec("insert into range_t values (4, 40)")
	tk.MustExec("update range_t set b = 131 where a = 3")
	tk.MustExec("delete from range_t where a = 2")
	tk.MustQuery("select a, b from range_t where tidb_row_id() > ? order by a", tokens[0][0]).Check(
		testkit.Rows("3 131", "4 40"),
	)
	tk.MustQuery("explain format = 'brief' select a from range_t where tidb_row_id() > ?", tokens[1][0]).CheckContain("UnionScan")
	tk.MustExec("rollback")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select a from range_t where tidb_row_id() >= ? order by a for update", tokens[1][0]).Check(testkit.Rows("2", "3"))
	tk.MustExec("rollback")

	tk.MustExec("create table common_range (a varchar(10), b int, primary key (a, b) clustered)")
	tk.MustExec("insert into common_range values ('a', 1), ('b', 2), ('c', 3)")
	commonTokens := tk.MustQuery("select tidb_row_id(), a from common_range order by tidb_row_id()").Rows()
	tk.MustQuery("select a from common_range where tidb_row_id() > ? order by tidb_row_id()", commonTokens[0][0]).Check(
		testkit.Rows(commonTokens[1][1].(string), commonTokens[2][1].(string)),
	)
	err = tk.ExecToErr("select a from range_t where tidb_row_id() > ?", commonTokens[0][0])
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)

	tk.MustExec("create table range_lifecycle (a bigint primary key clustered)")
	tk.MustExec("insert into range_lifecycle values (1), (2)")
	lifecycleToken := tk.MustQuery("select tidb_row_id() from range_lifecycle where a = 1").Rows()[0][0]
	tk.MustExec("rename table range_lifecycle to range_renamed")
	tk.MustQuery("select a from range_renamed where tidb_row_id() > ?", lifecycleToken).Check(testkit.Rows("2"))
	tk.MustExec("prepare lifecycle_stmt from 'select a from range_renamed where tidb_row_id() > ?'")
	tk.MustExec("set @lifecycle_token = ?", lifecycleToken)
	tk.MustQuery("execute lifecycle_stmt using @lifecycle_token").Check(testkit.Rows("2"))
	tk.MustExec("truncate table range_renamed")
	err = tk.ExecToErr("execute lifecycle_stmt using @lifecycle_token")
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("insert into range_renamed values (1), (2)")
	newLifecycleToken := tk.MustQuery("select tidb_row_id() from range_renamed where a = 1").Rows()[0][0]
	tk.MustExec("set @lifecycle_token = ?", newLifecycleToken)
	tk.MustQuery("execute lifecycle_stmt using @lifecycle_token").Check(testkit.Rows("2"))
	tk.MustExec("deallocate prepare lifecycle_stmt")
	tk.MustExec("drop table range_renamed")
	tk.MustExec("create table range_renamed (a bigint primary key clustered)")
	tk.MustExec("insert into range_renamed values (1), (2)")
	tk.MustQuery("select a from range_renamed where tidb_row_id() = ?", newLifecycleToken).Check(testkit.Rows())
	err = tk.ExecToErr("select a from range_renamed where tidb_row_id() > ?", newLifecycleToken)
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
}

func TestTiDBRowIDPartitionRanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table range_partitioned (a bigint primary key clustered) partition by range (a) (partition p0 values less than (3), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into range_partitioned values (1), (2), (3), (4)")
	tokens := tk.MustQuery("select tidb_row_id(), a from range_partitioned order by tidb_row_id()").Rows()
	firstPage := tk.MustQuery("select tidb_row_id(), a from range_partitioned order by tidb_row_id() limit 2").Rows()
	require.Equal(t, tokens[:2], firstPage)
	secondPage := tk.MustQuery("select tidb_row_id(), a from range_partitioned where tidb_row_id() > ? order by tidb_row_id() limit 2", firstPage[1][0]).Rows()
	require.Equal(t, tokens[2:], secondPage)
	descendingFirstPage := tk.MustQuery("select tidb_row_id(), a from range_partitioned order by tidb_row_id() desc limit 2").Rows()
	require.Equal(t, [][]any{tokens[3], tokens[2]}, descendingFirstPage)
	descendingSecondPage := tk.MustQuery("select tidb_row_id(), a from range_partitioned where tidb_row_id() < ? order by tidb_row_id() desc limit 2", descendingFirstPage[1][0]).Rows()
	require.Equal(t, [][]any{tokens[1], tokens[0]}, descendingSecondPage)
	paginationPlan := tk.MustQuery("explain format = 'brief' select tidb_row_id(), a from range_partitioned where tidb_row_id() > ? order by tidb_row_id() limit 2", firstPage[1][0])
	paginationPlan.CheckContain("TopN")

	tk.MustQuery("select a from range_partitioned where tidb_row_id() > ? order by tidb_row_id()", tokens[0][0]).Check(
		testkit.Rows(tokens[1][1].(string), tokens[2][1].(string), tokens[3][1].(string)),
	)
	tk.MustQuery("select a from range_partitioned partition (p0) where tidb_row_id() >= ?", tokens[2][0]).Check(testkit.Rows())
	tk.MustQuery("select a from range_partitioned partition (p1) where tidb_row_id() <= ?", tokens[1][0]).Check(testkit.Rows())

	lowerPID, _, lowerKey, _, err := tablecodec.DecodeTiDBRowID(tokens[1][0].(string))
	require.NoError(t, err)
	upperPID, _, upperKey, _, err := tablecodec.DecodeTiDBRowID(tokens[2][0].(string))
	require.NoError(t, err)
	require.NotEqual(t, lowerPID, upperPID)
	expectedRanges := []kv.KeyRange{
		{StartKey: lowerKey, EndKey: tablecodec.EncodeRowKeyWithHandle(lowerPID, kv.IntHandle(math.MaxInt64)).Next()},
		{StartKey: tablecodec.EncodeRowKeyWithHandle(upperPID, kv.IntHandle(math.MinInt64)), EndKey: upperKey.Next()},
	}
	dispatchedRanges := make([]kv.KeyRange, 0, len(expectedRanges))
	requestCtx := context.WithValue(context.Background(), "CheckSelectRequestHook", func(req *kv.Request) {
		req.KeyRanges.ForEachPartition(func(ranges []kv.KeyRange) {
			dispatchedRanges = append(dispatchedRanges, ranges...)
		})
	})
	tk.MustQueryWithContext(
		requestCtx,
		"select a from range_partitioned where tidb_row_id() between ? and ? order by a",
		tokens[1][0],
		tokens[2][0],
	).Check(testkit.Rows("2", "3"))
	require.ElementsMatch(t, expectedRanges, dispatchedRanges)

	tk.MustExec("set tidb_partition_prune_mode = 'static'")
	tk.MustQuery("select a from range_partitioned where tidb_row_id() between ? and ? order by tidb_row_id()", tokens[1][0], tokens[2][0]).Check(
		testkit.Rows(tokens[1][1].(string), tokens[2][1].(string)),
	)

	tk.MustExec("create table range_snapshot (a bigint primary key clustered) partition by range (a) (" +
		"partition p0 values less than (10), partition p1 values less than (20), " +
		"partition p2 values less than (30), partition p3 values less than (40), " +
		"partition p4 values less than (50), partition p5 values less than (maxvalue))")
	tk.MustExec("insert into range_snapshot values (1), (2), (11), (12), (21), (22), (31), (32), (41), (42), (51), (52)")
	tk.MustExec("begin")
	snapshotRows := tk.MustQuery("select tidb_row_id(), a from range_snapshot order by tidb_row_id()").Rows()
	ascendingRows := tk.MustQuery("select tidb_row_id(), a from range_snapshot order by tidb_row_id() limit 3").Rows()

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec("insert into range_snapshot values (53)")
	for range len(snapshotRows) {
		page := tk.MustQuery(
			"select tidb_row_id(), a from range_snapshot where tidb_row_id() > ? order by tidb_row_id() limit 3",
			ascendingRows[len(ascendingRows)-1][0],
		).Rows()
		if len(page) == 0 {
			break
		}
		ascendingRows = append(ascendingRows, page...)
	}
	require.Equal(t, snapshotRows, ascendingRows)

	descendingRows := tk.MustQuery("select tidb_row_id(), a from range_snapshot order by tidb_row_id() desc limit 3").Rows()
	for range len(snapshotRows) {
		page := tk.MustQuery(
			"select tidb_row_id(), a from range_snapshot where tidb_row_id() < ? order by tidb_row_id() desc limit 3",
			descendingRows[len(descendingRows)-1][0],
		).Rows()
		if len(page) == 0 {
			break
		}
		descendingRows = append(descendingRows, page...)
	}
	descendingSnapshotRows := make([][]any, len(snapshotRows))
	for i := range snapshotRows {
		descendingSnapshotRows[i] = snapshotRows[len(snapshotRows)-1-i]
	}
	require.Equal(t, descendingSnapshotRows, descendingRows)
	tk.MustExec("rollback")
	tk.MustQuery("select count(*) from range_snapshot").Check(testkit.Rows("13"))

	tk.MustExec("create table range_stale (a bigint primary key clustered)")
	tk.MustExec("insert into range_stale values (1), (2)")
	staleRows := tk.MustQuery("select tidb_row_id(), a from range_stale order by a").Rows()
	staleVersion, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	tk.MustExec("insert into range_stale values (3)")
	latestToken := tk.MustQuery("select tidb_row_id() from range_stale where a = 3").Rows()[0][0]
	tk.MustExec("prepare stale_range from 'select a from range_stale where tidb_row_id() > ? order by a'")
	tk.MustExec("set @stale_token = ?", staleRows[0][0])
	tk.MustQuery("execute stale_range using @stale_token").Check(testkit.Rows("2", "3"))
	tk.MustQuery("execute stale_range using @stale_token").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("replace into mysql.tidb(variable_name, variable_value) values ('tikv_gc_safe_point', '20060102-15:04:05 -0700')")
	tk.MustExec("set tidb_snapshot = '" + strconv.FormatUint(staleVersion.Ver, 10) + "'")
	tk.MustQuery("select tidb_row_id(), a from range_stale order by a").Check(staleRows)
	tk.MustQuery("select a from range_stale where tidb_row_id() = ?", latestToken).Check(testkit.Rows())
	tk.MustQuery("execute stale_range using @stale_token").Check(testkit.Rows("2"))
	tk.MustExec("set tidb_snapshot = ''")
	tk.MustQuery("execute stale_range using @stale_token").Check(testkit.Rows("2", "3"))
	tk.MustExec("deallocate prepare stale_range")
}

func TestTiDBRowIDRangeTiFlashPlan(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table range_tiflash (a bigint primary key clustered)")
	tk.MustExec("insert into range_tiflash values (1), (2)")
	token := tk.MustQuery("select tidb_row_id() from range_tiflash where a = 1").Rows()[0][0].(string)
	testkit.SetTiFlashReplica(t, dom, "test", "range_tiflash")
	tk.MustExec("set tidb_allow_mpp = 1")
	tk.MustExec("set tidb_enforce_mpp = 1")
	tk.MustExec("set tidb_isolation_read_engines = 'tiflash'")
	plan := tk.MustQuery("explain format = 'brief' select a from range_tiflash where tidb_row_id() > ?", token)
	plan.CheckContain("TableRangeScan")
	plan.CheckContain("mpp[tiflash]")
}

func TestTiDBRowIDEmptyRangeTiFlashExecution(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t, mockstore.WithMockTiFlash(1))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table empty_range_tiflash (a bigint primary key clustered)")
	tk.MustExec("insert into empty_range_tiflash values (1), (2)")
	tokens := tk.MustQuery("select tidb_row_id() from empty_range_tiflash order by a").Rows()
	testkit.SetTiFlashReplica(t, dom, "test", "empty_range_tiflash")
	tk.MustExec("set tidb_allow_mpp = 1")
	tk.MustExec("set tidb_enforce_mpp = 1")
	tk.MustExec("set tidb_isolation_read_engines = 'tiflash'")
	tk.MustQuery(
		"select a from empty_range_tiflash where tidb_row_id() between ? and ?",
		tokens[1][0],
		tokens[0][0],
	).Check(testkit.Rows())
	tk.MustQuery("select a from empty_range_tiflash where tidb_row_id() > ?", nil).Check(testkit.Rows())

	tk.MustExec("prepare empty_range_stmt from 'select a from empty_range_tiflash where tidb_row_id() between ? and ? order by a'")
	tk.MustExec("set @range_low = ?, @range_high = ?", tokens[0][0], tokens[1][0])
	tk.MustQuery("execute empty_range_stmt using @range_low, @range_high").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @range_low = ?, @range_high = ?", tokens[1][0], tokens[0][0])
	tk.MustQuery("execute empty_range_stmt using @range_low, @range_high").Check(testkit.Rows())
	tk.MustExec("set @range_low = NULL, @range_high = ?", tokens[1][0])
	tk.MustQuery("execute empty_range_stmt using @range_low, @range_high").Check(testkit.Rows())
	tk.MustExec("set @range_low = ?, @range_high = ?", tokens[0][0], tokens[1][0])
	tk.MustQuery("execute empty_range_stmt using @range_low, @range_high").Check(testkit.Rows("1", "2"))
	tk.MustExec("deallocate prepare empty_range_stmt")

	tk.MustExec("set tidb_isolation_read_engines = 'tikv'")
	tk.MustExec("create table empty_range_partitioned (a bigint primary key clustered) partition by range (a) (partition p0 values less than (2), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into empty_range_partitioned values (1), (2)")
	partitionTokens := tk.MustQuery("select tidb_row_id() from empty_range_partitioned order by a").Rows()
	testkit.SetTiFlashReplica(t, dom, "test", "empty_range_partitioned")
	tk.MustExec("set tidb_isolation_read_engines = 'tiflash'")
	for _, pruneMode := range []string{"dynamic", "static"} {
		tk.MustExec("set tidb_partition_prune_mode = ?", pruneMode)
		tk.MustQuery(
			"select a from empty_range_partitioned where tidb_row_id() between ? and ?",
			partitionTokens[1][0],
			partitionTokens[0][0],
		).Check(testkit.Rows())
	}

	tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery(
		"select a from empty_range_partitioned where a < 2 and tidb_row_id() >= ?",
		partitionTokens[1][0],
	).Check(testkit.Rows())
	tk.MustQuery(
		"select count(*) from empty_range_partitioned where a < 2 and tidb_row_id() >= ?",
		partitionTokens[1][0],
	).Check(testkit.Rows("0"))

	tk.MustExec("prepare dynamically_pruned_range from 'select a from empty_range_partitioned where a < ? and tidb_row_id() >= ? order by a'")
	tk.MustExec("set @partition_bound = 2, @partition_token = ?", partitionTokens[1][0])
	tk.MustQuery("execute dynamically_pruned_range using @partition_bound, @partition_token").Check(testkit.Rows())
	tk.MustExec("set @partition_bound = 3")
	tk.MustQuery("execute dynamically_pruned_range using @partition_bound, @partition_token").Check(testkit.Rows("2"))
	tk.MustExec("set @partition_bound = 2")
	tk.MustQuery("execute dynamically_pruned_range using @partition_bound, @partition_token").Check(testkit.Rows())
	tk.MustExec("deallocate prepare dynamically_pruned_range")

	tk.MustExec("set tidb_isolation_read_engines = 'tikv'")
	tk.MustExec("create table empty_range_join_side (a bigint primary key clustered)")
	tk.MustExec("insert into empty_range_join_side values (1), (2)")
	testkit.SetTiFlashReplica(t, dom, "test", "empty_range_join_side")
	tk.MustExec("set tidb_isolation_read_engines = 'tiflash'")
	tk.MustQuery(
		"select l.a, r.a from empty_range_join_side l left join empty_range_partitioned r on l.a = r.a and r.a < 2 and tidb_row_id(r) >= ? order by l.a",
		partitionTokens[1][0],
	).Check(testkit.Rows("1 <nil>", "2 <nil>"))
	tk.MustQuery(
		"select r.a from empty_range_partitioned r left join empty_range_join_side l on r.a = l.a where r.a < 2 and tidb_row_id(r) >= ? order by r.a",
		partitionTokens[1][0],
	).Check(testkit.Rows())
}

func TestTiDBRowIDHandleAndPartitionKinds(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table extra_handle (a int)")
	tk.MustExec("insert into extra_handle values (1)")
	extraToken := tk.MustQuery("select tidb_row_id() from extra_handle").Rows()[0][0].(string)
	_, extraHandle, _, _, err := tablecodec.DecodeTiDBRowID(extraToken)
	require.NoError(t, err)
	require.True(t, extraHandle.IsInt())
	tk.MustQuery("select a, tidb_row_id() from extra_handle where tidb_row_id() = ?", extraToken).Check(
		testkit.Rows("1 " + extraToken),
	)
	tk.MustQuery("explain format = 'brief' select a from extra_handle where tidb_row_id() = ?", extraToken).CheckContain("Point_Get")

	tk.MustExec("create table signed_handle_extrema (a bigint primary key clustered)")
	tk.MustExec("insert into signed_handle_extrema values (-9223372036854775808), (9223372036854775807)")
	for _, row := range tk.MustQuery("select tidb_row_id(), a from signed_handle_extrema order by a").Rows() {
		tk.MustQuery("select a from signed_handle_extrema where tidb_row_id() = ?", row[0]).Check(
			testkit.Rows(row[1].(string)),
		)
	}

	tk.MustExec("create table unsigned_handle_extrema (a bigint unsigned primary key clustered)")
	tk.MustExec("insert into unsigned_handle_extrema values (0), (9223372036854775808), (18446744073709551615)")
	unsignedRows := tk.MustQuery("select tidb_row_id(), a from unsigned_handle_extrema order by a").Rows()
	for _, row := range unsignedRows {
		_, handle, _, _, err := tablecodec.DecodeTiDBRowID(row[0].(string))
		require.NoError(t, err)
		expected, err := strconv.ParseUint(row[1].(string), 10, 64)
		require.NoError(t, err)
		require.Equal(t, expected, uint64(handle.IntValue()))
		tk.MustQuery("select a from unsigned_handle_extrema where tidb_row_id() = ?", row[0]).Check(
			testkit.Rows(row[1].(string)),
		)
	}
	unsignedRowsByLocator := tk.MustQuery("select tidb_row_id(), a from unsigned_handle_extrema order by tidb_row_id()").Rows()
	tk.MustQuery(
		"select tidb_row_id(), a from unsigned_handle_extrema where tidb_row_id() > ? order by tidb_row_id()",
		unsignedRowsByLocator[0][0],
	).Check(unsignedRowsByLocator[1:])

	tk.MustExec("create table common_handle (a varchar(20), b int, primary key (a, b) clustered)")
	tk.MustExec("insert into common_handle values ('x', 7)")
	commonToken := tk.MustQuery("select tidb_row_id() from common_handle").Rows()[0][0].(string)
	_, commonHandle, _, _, err := tablecodec.DecodeTiDBRowID(commonToken)
	require.NoError(t, err)
	require.False(t, commonHandle.IsInt())
	tk.MustQuery("select a, b, tidb_row_id() from common_handle where tidb_row_id() = ?", commonToken).Check(
		testkit.Rows("x 7 " + commonToken),
	)
	tk.MustQuery("explain format = 'brief' select a from common_handle where tidb_row_id() = ?", commonToken).CheckContain("Point_Get")

	tk.MustExec("create table collated_common_handle (a varchar(20) collate utf8mb4_general_ci, b int, primary key (a, b) clustered)")
	tk.MustExec("insert into collated_common_handle values ('Common', 8)")
	collatedToken := tk.MustQuery("select tidb_row_id() from collated_common_handle").Rows()[0][0]
	tk.MustQuery("select a, b from collated_common_handle where tidb_row_id() = ?", collatedToken).Check(
		testkit.Rows("Common 8"),
	)

	maxCommonValue := strings.Repeat("x", 3072)
	tk.MustExec("create table max_common_handle (a varchar(3072), primary key (a) clustered) charset ascii")
	tk.MustExec("insert into max_common_handle values (?)", maxCommonValue)
	maxCommonToken := tk.MustQuery("select tidb_row_id() from max_common_handle").Rows()[0][0]
	tk.MustQuery("select length(a) from max_common_handle where tidb_row_id() = ?", maxCommonToken).Check(
		testkit.Rows("3072"),
	)

	tk.MustExec("create table partitioned (a bigint primary key clustered, b int default 0) partition by range (a) (partition p0 values less than (2), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into partitioned (a) values (1), (2)")
	partitionRows := tk.MustQuery("select tidb_row_id(), a from partitioned order by a").Rows()
	firstPID, firstHandle, _, _, err := tablecodec.DecodeTiDBRowID(partitionRows[0][0].(string))
	require.NoError(t, err)
	secondPID, secondHandle, _, _, err := tablecodec.DecodeTiDBRowID(partitionRows[1][0].(string))
	require.NoError(t, err)
	require.NotEqual(t, firstPID, secondPID)
	require.Equal(t, int64(1), firstHandle.IntValue())
	require.Equal(t, int64(2), secondHandle.IntValue())
	tk.MustQuery("select a, tidb_row_id() from partitioned where tidb_row_id() = ?", partitionRows[0][0]).Check(
		testkit.Rows("1 " + partitionRows[0][0].(string)),
	)
	tk.MustQuery("select a from partitioned where tidb_row_id() in (?, ?) order by a", partitionRows[1][0], partitionRows[0][0]).Check(
		testkit.Rows("1", "2"),
	)
	tk.MustQuery(
		"select a from partitioned partition (p0) where tidb_row_id() in (?, ?) order by a",
		partitionRows[1][0], partitionRows[0][0],
	).Check(testkit.Rows("1"))
	tk.MustQuery(
		"select a from partitioned where a < 2 and tidb_row_id() in (?, ?) order by a",
		partitionRows[1][0], partitionRows[0][0],
	).Check(testkit.Rows("1"))
	tk.MustQuery("explain format = 'brief' select a from partitioned where tidb_row_id() in (?, ?)", partitionRows[0][0], partitionRows[1][0]).CheckContain("Batch_Point_Get")
	tk.MustQuery("select a from partitioned partition (p0) where tidb_row_id() = ?", partitionRows[1][0]).Check(testkit.Rows())

	tk.MustExec("create table list_partitioned (a bigint primary key clustered) partition by list (a) (partition p0 values in (1, 3), partition p1 values in (2, 4))")
	tk.MustExec("insert into list_partitioned values (1), (2), (3), (4)")
	listRows := tk.MustQuery("select tidb_row_id(), a from list_partitioned order by tidb_row_id()").Rows()
	listFirstPage := tk.MustQuery("select tidb_row_id(), a from list_partitioned order by tidb_row_id() limit 2").Rows()
	listSecondPage := tk.MustQuery(
		"select tidb_row_id(), a from list_partitioned where tidb_row_id() > ? order by tidb_row_id() limit 2",
		listFirstPage[1][0],
	).Rows()
	require.Equal(t, listRows, append(listFirstPage, listSecondPage...))
	listP1Token := tk.MustQuery("select tidb_row_id() from list_partitioned where a = 2").Rows()[0][0]
	tk.MustQuery("select a from list_partitioned where tidb_row_id() = ?", listP1Token).Check(testkit.Rows("2"))
	tk.MustQuery("select a from list_partitioned partition (p0) where tidb_row_id() = ?", listP1Token).Check(testkit.Rows())

	tk.MustExec(`set @@tidb_opt_fix_control = "52592:ON"`)
	tk.MustQuery("select a from partitioned where tidb_row_id() = ?", partitionRows[0][0]).Check(testkit.Rows("1"))
	tk.MustExec(`set @@tidb_opt_fix_control = ""`)
	tk.MustExec("prepare partition_locator from 'select a from partitioned where tidb_row_id() = ?'")
	tk.MustExec("set @partition_locator_token = ?", partitionRows[0][0])
	tk.MustQuery("execute partition_locator using @partition_locator_token").Check(testkit.Rows("1"))
	tk.MustExec("set @partition_locator_token = ?", partitionRows[1][0])
	tk.MustQuery("execute partition_locator using @partition_locator_token").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare partition_locator")

	tk.MustExec("create table duplicate_handle (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (maxvalue))")
	tk.MustExec("set tidb_opt_write_row_id = 1")
	tk.MustExec("insert into duplicate_handle (_tidb_rowid, a) values (7, 1), (7, 11)")
	duplicateRows := tk.MustQuery("select tidb_row_id(), a from duplicate_handle order by a").Rows()
	require.NotEqual(t, duplicateRows[0][0], duplicateRows[1][0])
	tk.MustQuery(
		"select a from duplicate_handle where tidb_row_id() in (?, ?) order by a",
		duplicateRows[1][0], duplicateRows[0][0],
	).Check(testkit.Rows("1", "11"))
	tk.MustQuery("select a from duplicate_handle where tidb_row_id() = ?", duplicateRows[1][0]).Check(testkit.Rows("11"))

	tk.MustExec("set tidb_partition_prune_mode = 'static'")
	tk.MustQuery("select a from partitioned where tidb_row_id() = ?", partitionRows[0][0]).Check(testkit.Rows("1"))
	tk.MustQuery(
		"select a from partitioned partition (p0) where tidb_row_id() in (?, ?) order by a",
		partitionRows[1][0], partitionRows[0][0],
	).Check(testkit.Rows("1"))
	tk.MustQuery(
		"select a from partitioned where a < 2 and tidb_row_id() in (?, ?) order by a",
		partitionRows[1][0], partitionRows[0][0],
	).Check(testkit.Rows("1"))
	tk.MustQuery(
		"select a from duplicate_handle where tidb_row_id() in (?, ?) order by a",
		duplicateRows[1][0], duplicateRows[0][0],
	).Check(testkit.Rows("1", "11"))
	tk.MustExec("update partitioned set b = 5 where tidb_row_id() = ?", partitionRows[1][0])
	tk.MustQuery("select a, b from partitioned order by a").Check(testkit.Rows("1 0", "2 5"))
	tk.MustExec("delete from partitioned where tidb_row_id() = ?", partitionRows[0][0])
	tk.MustQuery("select a, b from partitioned").Check(testkit.Rows("2 5"))

	tk.MustExec("set tidb_row_format_version = 1")
	tk.MustExec("create table old_row_format (a bigint primary key clustered) partition by hash (a) partitions 2")
	tk.MustExec("insert into old_row_format values (1)")
	oldFormatToken := tk.MustQuery("select tidb_row_id() from old_row_format").Rows()[0][0]
	tk.MustQuery("select a, tidb_row_id() from old_row_format where tidb_row_id() = ?", oldFormatToken).Check(
		testkit.Rows("1 " + oldFormatToken.(string)),
	)

	tk.MustExec("set tidb_enable_exchange_partition = 1")
	tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("create table locator_exchange (a int, b int, primary key (a) nonclustered) partition by range (a) (partition p0 values less than (100), partition p1 values less than (200))")
	tk.MustExec("create table locator_plain (a int, b int, primary key (a) nonclustered)")
	tk.MustExec("insert into locator_exchange values (1, 10), (2, 20)")
	tk.MustExec("insert into locator_plain values (101, 110), (102, 120)")
	partitionTokensBeforeExchange := tk.MustQuery("select tidb_row_id(), a from locator_exchange order by a").Rows()
	plainTokensBeforeExchange := tk.MustQuery("select tidb_row_id(), a from locator_plain order by a").Rows()
	tk.MustExec("prepare exchange_locator from 'select a from locator_exchange where tidb_row_id() = ?'")
	tk.MustExec("set @exchange_token = ?", partitionTokensBeforeExchange[0][0])
	tk.MustQuery("execute exchange_locator using @exchange_token").Check(testkit.Rows("1"))
	tk.MustQuery("execute exchange_locator using @exchange_token").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @exchange_token = ?", plainTokensBeforeExchange[0][0])
	tk.MustQuery("execute exchange_locator using @exchange_token").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("alter table locator_exchange exchange partition p1 with table locator_plain")
	tk.MustQuery("execute exchange_locator using @exchange_token").Check(testkit.Rows("101"))
	tk.MustQuery("select a from locator_plain where tidb_row_id() = ?", plainTokensBeforeExchange[0][0]).Check(testkit.Rows())
	rowsAfterExchange := tk.MustQuery("select tidb_row_id(), a from locator_exchange order by a").Rows()
	require.Equal(t, partitionTokensBeforeExchange[0][0], rowsAfterExchange[0][0])
	require.Equal(t, partitionTokensBeforeExchange[1][0], rowsAfterExchange[1][0])
	require.Equal(t, plainTokensBeforeExchange[0][0], rowsAfterExchange[2][0])
	require.Equal(t, plainTokensBeforeExchange[1][0], rowsAfterExchange[3][0])
	tk.MustQuery("select _tidb_rowid, a from locator_exchange order by a").Check(
		testkit.Rows("1 1", "2 2", "1 101", "2 102"),
	)
	uniqueLocators := make(map[string]struct{}, len(rowsAfterExchange))
	for _, row := range rowsAfterExchange {
		uniqueLocators[row[0].(string)] = struct{}{}
	}
	require.Len(t, uniqueLocators, len(rowsAfterExchange))

	orderedRowsAfterExchange := tk.MustQuery("select tidb_row_id(), a from locator_exchange order by tidb_row_id()").Rows()
	firstExchangePage := tk.MustQuery("select tidb_row_id(), a from locator_exchange order by tidb_row_id() limit 2").Rows()
	secondExchangePage := tk.MustQuery(
		"select tidb_row_id(), a from locator_exchange where tidb_row_id() > ? order by tidb_row_id() limit 2",
		firstExchangePage[1][0],
	).Rows()
	require.Equal(t, orderedRowsAfterExchange, append(firstExchangePage, secondExchangePage...))
	tk.MustExec("update locator_exchange set b = 111 where tidb_row_id() = ?", plainTokensBeforeExchange[0][0])
	tk.MustQuery("select a, b from locator_exchange order by a").Check(testkit.Rows("1 10", "2 20", "101 111", "102 120"))
	tk.MustExec("delete from locator_exchange where tidb_row_id() = ?", partitionTokensBeforeExchange[1][0])
	tk.MustQuery("select a from locator_exchange order by a").Check(testkit.Rows("1", "101", "102"))
	tk.MustExec("deallocate prepare exchange_locator")
	testTiDBRowIDPartitionLifecycle(t, tk)
	testTiDBRowIDAcceptanceScenario(t, tk)
}

func testTiDBRowIDAcceptanceScenario(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("set @@session.tidb_enable_clustered_index = off")
	tk.MustExec("create table locator_acceptance_tp (a bigint, b bigint, primary key (a) nonclustered) partition by hash(a) partitions 2")
	tk.MustExec("create table locator_acceptance_exchange (a bigint, b bigint, primary key (a) nonclustered)")
	tk.MustExec("insert into locator_acceptance_tp values (1, 10), (3, 30)")
	tk.MustExec("insert into locator_acceptance_exchange values (2, 20), (4, 40)")
	tk.MustExec("alter table locator_acceptance_tp exchange partition p0 with table locator_acceptance_exchange")

	tk.MustQuery(
		"select count(*), count(distinct _tidb_rowid), count(distinct tidb_row_id()) from locator_acceptance_tp",
	).Check(testkit.Rows("4 2 4"))

	p0Row := tk.MustQuery("select tidb_row_id(), _tidb_rowid from locator_acceptance_tp where a = 2").Rows()[0]
	p1Row := tk.MustQuery("select tidb_row_id(), _tidb_rowid from locator_acceptance_tp where a = 1").Rows()[0]
	require.Equal(t, p0Row[1], p1Row[1])
	p0PID, err := strconv.ParseInt(tk.MustQuery(
		"select tidb_partition_id from information_schema.partitions " +
			"where table_schema = 'test' and table_name = 'locator_acceptance_tp' and partition_name = 'p0'",
	).Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)
	p1PID, err := strconv.ParseInt(tk.MustQuery(
		"select tidb_partition_id from information_schema.partitions " +
			"where table_schema = 'test' and table_name = 'locator_acceptance_tp' and partition_name = 'p1'",
	).Rows()[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.NotEqual(t, p0PID, p1PID)

	type decodedRecordKey struct {
		PartitionID int64 `json:"partition_id"`
		RowID       int64 `json:"_tidb_rowid"`
	}
	decode := func(token string) decodedRecordKey {
		t.Helper()
		decoded := tk.MustQuery("select tidb_decode_key(?)", token).Rows()[0][0].(string)
		var result decodedRecordKey
		require.NoError(t, json.Unmarshal([]byte(decoded), &result), decoded)
		return result
	}
	p0Decoded := decode(p0Row[0].(string))
	p1Decoded := decode(p1Row[0].(string))
	require.Equal(t, p0PID, p0Decoded.PartitionID)
	require.Equal(t, p1PID, p1Decoded.PartitionID)
	require.Equal(t, p0Row[1].(string), strconv.FormatInt(p0Decoded.RowID, 10))
	require.Equal(t, p1Row[1].(string), strconv.FormatInt(p1Decoded.RowID, 10))

	tk.MustExec("begin")
	allRows := tk.MustQuery("select tidb_row_id(), a, b from locator_acceptance_tp order by tidb_row_id()").Rows()
	firstPage := tk.MustQuery("select tidb_row_id(), a, b from locator_acceptance_tp order by tidb_row_id() limit 2").Rows()
	secondPage := tk.MustQuery(
		"select tidb_row_id(), a, b from locator_acceptance_tp where tidb_row_id() > ? order by tidb_row_id() limit 2",
		firstPage[1][0],
	).Rows()
	require.Equal(t, allRows, append(firstPage, secondPage...))

	tk.MustQuery(
		"select a, b from locator_acceptance_tp where tidb_row_id() = ? for update",
		p0Row[0],
	).Check(testkit.Rows("2 20"))
	tk.MustExec("update locator_acceptance_tp set b = 21 where tidb_row_id() = ?", p0Row[0])
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select a, b from locator_acceptance_tp order by a").Check(
		testkit.Rows("1 10", "2 21", "3 30", "4 40"),
	)
	tk.MustExec("delete from locator_acceptance_tp where tidb_row_id() = ?", p0Row[0])
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select a, b from locator_acceptance_tp order by a").Check(
		testkit.Rows("1 10", "3 30", "4 40"),
	)
	tk.MustExec("commit")
}

func testTiDBRowIDPartitionLifecycle(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("create table locator_partition_lifecycle (a bigint primary key clustered) partition by range (a) (partition p0 values less than (3), partition p1 values less than (5), partition p2 values less than (maxvalue))")
	tk.MustExec("insert into locator_partition_lifecycle values (1), (3), (5)")
	tokens := tk.MustQuery("select tidb_row_id(), a from locator_partition_lifecycle order by a").Rows()
	tk.MustExec("prepare partition_lifecycle_locator from 'select a from locator_partition_lifecycle where tidb_row_id() = ?'")
	tk.MustExec("set @partition_lifecycle_token = ?", tokens[2][0])
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows("5"))
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows("5"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("alter table locator_partition_lifecycle drop partition p2")
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows())
	err := tk.ExecToErr("select a from locator_partition_lifecycle where tidb_row_id() >= ?", tokens[2][0])
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("set @partition_lifecycle_token = 'malformed'")
	err = tk.ExecToErr("execute partition_lifecycle_locator using @partition_lifecycle_token")
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err), err)
	tk.MustExec("set @partition_lifecycle_token = ?", tokens[0][0])
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows("1"))
	tk.MustExec("set @partition_lifecycle_token = ?", tokens[1][0])
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows("3"))
	tk.MustExec("alter table locator_partition_lifecycle truncate partition p1")
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows())
	tk.MustExec("insert into locator_partition_lifecycle values (3)")
	newTruncatedToken := tk.MustQuery("select tidb_row_id() from locator_partition_lifecycle where a = 3").Rows()[0][0]
	require.NotEqual(t, tokens[1][0], newTruncatedToken)
	tk.MustExec("set @partition_lifecycle_token = ?", newTruncatedToken)
	tk.MustQuery("execute partition_lifecycle_locator using @partition_lifecycle_token").Check(testkit.Rows("3"))
	tk.MustExec("deallocate prepare partition_lifecycle_locator")

	tk.MustExec("create table locator_reorganize (a bigint primary key clustered) partition by range (a) (partition p0 values less than (3), partition p1 values less than (5), partition p2 values less than (maxvalue))")
	tk.MustExec("insert into locator_reorganize values (1), (3), (5)")
	oldReorganizeToken := tk.MustQuery("select tidb_row_id() from locator_reorganize where a = 1").Rows()[0][0]
	tk.MustExec("prepare reorganize_locator from 'select a from locator_reorganize where tidb_row_id() = ?'")
	tk.MustExec("set @reorganize_token = ?", oldReorganizeToken)
	tk.MustQuery("execute reorganize_locator using @reorganize_token").Check(testkit.Rows("1"))
	tk.MustQuery("execute reorganize_locator using @reorganize_token").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("alter table locator_reorganize reorganize partition p0, p1 into (partition p01 values less than (5))")
	tk.MustQuery("execute reorganize_locator using @reorganize_token").Check(testkit.Rows())
	newReorganizeToken := tk.MustQuery("select tidb_row_id() from locator_reorganize where a = 1").Rows()[0][0]
	require.NotEqual(t, oldReorganizeToken, newReorganizeToken)
	tk.MustExec("set @reorganize_token = ?", newReorganizeToken)
	tk.MustQuery("execute reorganize_locator using @reorganize_token").Check(testkit.Rows("1"))
	tk.MustExec("deallocate prepare reorganize_locator")

	tk.MustExec("create table locator_handle_update (a bigint primary key clustered, b int)")
	tk.MustExec("insert into locator_handle_update values (1, 10)")
	oldHandleToken := tk.MustQuery("select tidb_row_id() from locator_handle_update").Rows()[0][0]
	tk.MustExec("update locator_handle_update set a = 2 where tidb_row_id() = ?", oldHandleToken)
	tk.MustQuery("select a from locator_handle_update where tidb_row_id() = ?", oldHandleToken).Check(testkit.Rows())
	newHandleToken := tk.MustQuery("select tidb_row_id() from locator_handle_update").Rows()[0][0]
	require.NotEqual(t, oldHandleToken, newHandleToken)
}

func BenchmarkRowLocatorLookup(b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	populateRowLocatorBenchmarkTable(tk, "row_locator_lookup", 1024)
	token := tk.MustQuery("select tidb_row_id() from row_locator_lookup where a = 777").Rows()[0][0]

	tk.MustQuery("explain format = 'brief' select b from row_locator_lookup where a = ?", 777).CheckContain("Point_Get")
	tk.MustQuery("explain format = 'brief' select b from row_locator_lookup where tidb_row_id() = ?", token).CheckContain("Point_Get")

	benchmarkRowLocatorQuery(b, tk, "PrimaryKey", "select b from row_locator_lookup where a = ?", 777)
	benchmarkRowLocatorQuery(b, tk, "Locator", "select b from row_locator_lookup where tidb_row_id() = ?", token)

	const candidateCount = 16
	primaryKeyArgs := make([]any, 0, candidateCount)
	locatorArgs := make([]any, 0, candidateCount)
	for i := 1; i <= candidateCount; i++ {
		primaryKeyArgs = append(primaryKeyArgs, i*32)
	}
	tokenRows := tk.MustQuery(
		"select tidb_row_id() from row_locator_lookup where a in ("+
			strings.TrimSuffix(strings.Repeat("?,", candidateCount), ",")+") order by a",
		primaryKeyArgs...,
	).Rows()
	for _, row := range tokenRows {
		locatorArgs = append(locatorArgs, row[0])
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", candidateCount), ",")
	primaryKeyINSQL := "select b from row_locator_lookup where a in (" + placeholders + ")"
	locatorINSQL := "select b from row_locator_lookup where tidb_row_id() in (" + placeholders + ")"
	tk.MustQuery("explain format = 'brief' "+primaryKeyINSQL, primaryKeyArgs...).CheckContain("Batch_Point_Get")
	tk.MustQuery("explain format = 'brief' "+locatorINSQL, locatorArgs...).CheckContain("Batch_Point_Get")
	benchmarkRowLocatorQuery(b, tk, "PrimaryKeyIN16", primaryKeyINSQL, primaryKeyArgs...)
	benchmarkRowLocatorQuery(b, tk, "LocatorIN16", locatorINSQL, locatorArgs...)
}

func BenchmarkRowLocatorPagination(b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	populateRowLocatorBenchmarkTable(tk, "row_locator_pagination", 2048)
	token := tk.MustQuery("select tidb_row_id() from row_locator_pagination where a = 1024").Rows()[0][0]

	directSQL := "select a from row_locator_pagination where tidb_row_id() > ? order by tidb_row_id() limit 32"
	residualSQL := "select a from row_locator_pagination where tidb_row_id() > if(a >= 0, ?, ?) order by tidb_row_id() limit 32"
	tk.MustQuery("explain format = 'brief' "+directSQL, token).CheckContain("TableRangeScan")
	tk.MustQuery("explain format = 'brief' "+residualSQL, token, token).CheckNotContain("TableRangeScan")
	benchmarkRowLocatorQuery(b, tk, "ExtractedRange", directSQL, token)
	benchmarkRowLocatorQuery(b, tk, "RootResidual", residualSQL, token, token)

	tk.MustExec("create table row_locator_partition_pagination (a bigint primary key clustered, b bigint) partition by hash(a) partitions 16")
	populateRowLocatorBenchmarkRows(tk, "row_locator_partition_pagination", 2048)
	partitionToken := tk.MustQuery("select tidb_row_id() from row_locator_partition_pagination where a = 1024").Rows()[0][0]
	partitionSQL := "select a from row_locator_partition_pagination where tidb_row_id() > ? order by tidb_row_id() limit 32"
	partitionPlan := tk.MustQuery("explain format = 'brief' "+partitionSQL, partitionToken)
	partitionPlan.CheckContain("TopN")
	partitionPlan.CheckContain("TableRangeScan")
	benchmarkRowLocatorQuery(b, tk, "PartitionedGlobalSort16", partitionSQL, partitionToken)
}

func populateRowLocatorBenchmarkTable(tk *testkit.TestKit, tableName string, rowCount int) {
	tk.MustExec("create table " + tableName + " (a bigint primary key clustered, b bigint)")
	populateRowLocatorBenchmarkRows(tk, tableName, rowCount)
}

func populateRowLocatorBenchmarkRows(tk *testkit.TestKit, tableName string, rowCount int) {
	tk.MustExec("insert into " + tableName + " values (1, 1)")
	for offset := 1; offset < rowCount; offset *= 2 {
		offsetText := strconv.Itoa(offset)
		tk.MustExec(
			"insert into " + tableName + " select a + " + offsetText + ", b + " + offsetText + " from " + tableName,
		)
	}
}

func benchmarkRowLocatorQuery(b *testing.B, tk *testkit.TestKit, name, sql string, args ...any) {
	b.Helper()
	b.Run(name, func(b *testing.B) {
		recordKeys := benchmarkRowLocatorRecordKeys(b, tk, sql, args...)
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(recordKeys, "record_keys/op")
		for range b.N {
			if rows := tk.MustQuery(sql, args...).Rows(); len(rows) == 0 {
				b.Fatal("row locator benchmark unexpectedly returned no rows")
			}
		}
	})
}

func benchmarkRowLocatorRecordKeys(b *testing.B, tk *testkit.TestKit, sql string, args ...any) float64 {
	b.Helper()
	// These PK-only benchmark tables have one live row per record key, so a
	// storage operator's actual rows measure record keys read by the query.
	var recordKeys float64
	foundStorageOperator := false
	for _, row := range tk.MustQuery("explain analyze "+sql, args...).Rows() {
		operator, ok := row[0].(string)
		if !ok {
			b.Fatalf("unexpected EXPLAIN ANALYZE operator type %T", row[0])
		}
		if !strings.Contains(operator, "Point_Get") &&
			!strings.Contains(operator, "TableRangeScan") &&
			!strings.Contains(operator, "TableFullScan") {
			continue
		}
		actualRows, ok := row[2].(string)
		if !ok {
			b.Fatalf("unexpected EXPLAIN ANALYZE actual-row type %T", row[2])
		}
		keys, err := strconv.ParseFloat(actualRows, 64)
		if err != nil {
			b.Fatalf("parse EXPLAIN ANALYZE actual rows %q: %v", actualRows, err)
		}
		recordKeys += keys
		foundStorageOperator = true
	}
	if !foundStorageOperator {
		b.Fatal("row locator benchmark plan has no record-key storage operator")
	}
	return recordKeys
}

func TestTiDBRowIDQueryBlockBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a bigint primary key clustered)")
	tk.MustExec("create table t2 (a bigint primary key clustered)")
	tk.MustExec("create table t3 (a bigint primary key clustered)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("insert into t3 values (1)")

	joinRow := tk.MustQuery("select tidb_row_id(x), tidb_row_id(y) from t1 x join t2 y on x.a = y.a").Rows()[0]
	require.NotEqual(t, joinRow[0], joinRow[1])
	tk.MustGetErrCode("select tidb_row_id() from t1 x join t2 y on x.a = y.a", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("select tidb_row_id(t1) from t1 x", errno.ErrTiDBRowIDTableBinding)
	tk.MustQuery("select tidb_row_id(x) from t1 x").Check(testkit.Rows(joinRow[0].(string)))
	tk.MustQuery("select tidb_row_id(mixed) from t1 MiXeD").Check(testkit.Rows(joinRow[0].(string)))
	tk.MustExec("set @@sql_select_limit = 1")
	tk.MustQuery("select tidb_row_id() from t1").Check(testkit.Rows(joinRow[0].(string)))
	tk.MustExec("set @@sql_select_limit = default")

	nestedJoinRow := tk.MustQuery(
		"select tidb_row_id(x), tidb_row_id(y), tidb_row_id(z) " +
			"from (t1 x join t2 y on tidb_row_id(x) <> tidb_row_id(y)) " +
			"join t3 z on tidb_row_id(y) <> tidb_row_id(z) " +
			"where tidb_row_id(x) is not null",
	).Rows()[0]
	require.NotEqual(t, nestedJoinRow[0], nestedJoinRow[1])
	require.NotEqual(t, nestedJoinRow[0], nestedJoinRow[2])
	require.NotEqual(t, nestedJoinRow[1], nestedJoinRow[2])
	tk.MustGetErrCode(
		"select 1 from t1 x join t2 y on tidb_row_id() is not null",
		errno.ErrTiDBRowIDTableBinding,
	)
	tk.MustQuery(
		"select tidb_row_id() from t1 where exists (select 1 from t2 where tidb_row_id() is not null)",
	).Check(testkit.Rows(joinRow[0].(string)))

	tk.MustExec("create table self_t (a bigint primary key clustered)")
	tk.MustExec("insert into self_t values (1), (2)")
	selfJoinRow := tk.MustQuery(
		"select tidb_row_id(x), tidb_row_id(y) from self_t x join self_t y on x.a < y.a",
	).Rows()[0]
	require.NotEqual(t, selfJoinRow[0], selfJoinRow[1])
	tk.MustGetErrCode(
		"select tidb_row_id() from self_t x join self_t y on x.a < y.a",
		errno.ErrTiDBRowIDTableBinding,
	)

	innerToken := tk.MustQuery("select rid from (select tidb_row_id() rid from t1) q").Rows()[0][0]
	require.Equal(t, joinRow[0], innerToken)
	tk.MustQuery(
		"select a from (select tidb_row_id() rid, a from t1) q where rid = ?",
		innerToken,
	).Check(testkit.Rows("1"))
	tk.MustQuery(
		"select a from (select tidb_row_id() rid, a from t1) q where rid = ?",
		strings.ToUpper(innerToken.(string)),
	).Check(testkit.Rows())
	tk.MustGetErrCode("select tidb_row_id(q) from (select a from t1) q", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode(
		"select tidb_row_id(t1) from (select /*+ merge() */ a from t1) q",
		errno.ErrTiDBRowIDTableBinding,
	)
	tk.MustGetErrCode("select (select tidb_row_id(t1)) from t1", errno.ErrTiDBRowIDTableBinding)

	tk.MustQuery("with c as (select tidb_row_id() rid from t1) select rid from c").Check(testkit.Rows(innerToken.(string)))
	tk.MustGetErrCode("with c as (select a from t1) select tidb_row_id(c) from c", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode(
		"with c as (select /*+ merge() */ a from t1) select tidb_row_id(t1) from c",
		errno.ErrTiDBRowIDTableBinding,
	)
	tk.MustQuery("select tidb_row_id() from t1 union all select tidb_row_id() from t2").Sort().Check(
		testkit.Rows(joinRow[0].(string), joinRow[1].(string)),
	)
	testTiDBRowIDOnlyFullGroupByInJoin(t, tk)
}

func testTiDBRowIDOnlyFullGroupByInJoin(t *testing.T, tk *testkit.TestKit) {
	t.Helper()
	tk.MustExec("create table locator_group_left (a bigint primary key clustered)")
	tk.MustExec("create table locator_group_right (a bigint, index (a))")
	tk.MustExec("insert into locator_group_left values (1), (2)")
	tk.MustExec("insert into locator_group_right values (1), (1), (2)")
	tokens := tk.MustQuery("select tidb_row_id(), a from locator_group_left order by a").Rows()

	for _, newCheck := range []string{"off", "on"} {
		tk.MustExec("set @@session.tidb_enable_new_only_full_group_by_check = " + newCheck)
		tk.MustExec("set sql_mode = ''")
		tk.MustQuery(
			"select count(*), tidb_row_id(l) is not null from locator_group_left l " +
				"join locator_group_right r on l.a = r.a",
		).Check(testkit.Rows("3 1"))
		tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
		tk.MustQuery(
			"select tidb_row_id(l) as rid, count(*) from locator_group_left l " +
				"join locator_group_right r on l.a = r.a group by l.a order by rid",
		).Check(testkit.Rows(tokens[0][0].(string)+" 2", tokens[1][0].(string)+" 1"))
		tk.MustGetErrCode(
			"select tidb_row_id(l), count(*) from locator_group_left l "+
				"join locator_group_right r on l.a = r.a",
			errno.ErrMixOfGroupFuncAndFieldsIncompatible,
		)
	}
}

func TestTiDBRowIDInUpdateAssignmentSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table locator_source (a bigint primary key clustered)")
	tk.MustExec("create table locator_sink (id int primary key, locator varchar(255))")
	tk.MustExec("insert into locator_source values (1)")
	tk.MustExec("insert into locator_sink values (1, null)")

	token := tk.MustQuery("select tidb_row_id() from locator_source").Rows()[0][0]
	tk.MustExec("update locator_sink set locator = (select tidb_row_id() from locator_source)")
	tk.MustQuery("select locator from locator_sink").Check(testkit.Rows(token.(string)))
}

func TestTiDBRowIDForNullExtendedOuterJoinRow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table locator_outer_left (a bigint primary key clustered)")
	tk.MustExec("create table locator_outer_right (a bigint primary key clustered)")
	tk.MustExec("insert into locator_outer_left values (1)")

	tk.MustQuery(`
		select tidb_row_id(r)
		from locator_outer_left l
		left join locator_outer_right r on l.a = r.a
	`).Check(testkit.Rows("<nil>"))
}

func TestTiDBRowIDRejectedFormsAndContexts(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a bigint primary key clustered)")
	tk.MustExec("insert into t values (1)")

	tk.MustGetErrCode("select tidb_row_id()", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("select tidb_row_id('t') from t", errno.ErrWrongArguments)
	tk.MustGetErrCode("select tidb_row_id(t.a) from t", errno.ErrWrongArguments)
	tk.MustGetErrCode("select tidb_row_id(test.t) from t", errno.ErrWrongArguments)
	tk.MustGetErrCode("select tidb_row_id(t, t) from t", errno.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select tidb_row_id(t, t), count(*) from t", errno.ErrWrongParamcountToNativeFct)
	tk.MustGetErrCode("select count(*) from t having tidb_row_id() is not null", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("update t set a = length(tidb_row_id())", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("create view v as select tidb_row_id() from t", errno.ErrTiDBRowIDTableBinding)
	tk.MustExec("create view plain_v as select a from t")
	tk.MustGetErrCode("select tidb_row_id(plain_v) from plain_v", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("select tidb_row_id(t) from plain_v", errno.ErrTiDBRowIDTableBinding)

	tk.MustExec("create global temporary table temp_t (a int) on commit delete rows")
	tk.MustGetErrCode("select tidb_row_id() from temp_t", errno.ErrTiDBRowIDTableBinding)
	tk.MustExec("create temporary table local_temp_t (a int)")
	tk.MustGetErrCode("select tidb_row_id() from local_temp_t", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode(
		"select tidb_row_id(tables) from information_schema.tables",
		errno.ErrTiDBRowIDTableBinding,
	)
	tk.MustGetErrCode(
		"select tidb_row_id(cluster_info) from information_schema.cluster_info",
		errno.ErrTiDBRowIDTableBinding,
	)
	tk.MustExec("create sequence locator_sequence")
	tk.MustGetErrCode(
		"select tidb_row_id(locator_sequence) from locator_sequence",
		errno.ErrTiDBRowIDTableBinding,
	)

	tk.MustExec("create table locator_sink (rid varchar(255) primary key)")
	tk.MustGetErrCode("insert into locator_sink values (tidb_row_id())", errno.ErrTiDBRowIDTableBinding)
	tk.MustGetErrCode("replace into locator_sink values (tidb_row_id())", errno.ErrTiDBRowIDTableBinding)
	token := tk.MustQuery("select tidb_row_id() from t").Rows()[0][0].(string)
	tk.MustExec("insert into locator_sink select tidb_row_id() from t")
	tk.MustQuery("select rid from locator_sink").Check(testkit.Rows(token))
	tk.MustExec("replace into locator_sink select tidb_row_id() from t")
	tk.MustExec("create table locator_ctas as select tidb_row_id() rid from t")
	tk.MustQuery("select rid from locator_ctas").Check(testkit.Rows(token))

	tk.MustGetErrCode(
		"create table locator_generated (a bigint, rid varchar(255) as (tidb_row_id()))",
		errno.ErrGeneratedColumnFunctionIsNotAllowed,
	)
	tk.MustGetErrCode(
		"create table locator_functional (a bigint, index idx ((tidb_row_id())))",
		errno.ErrFunctionalIndexFunctionIsNotAllowed,
	)
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustGetErrCode(
		"create table locator_check (a bigint, constraint chk check (length(tidb_row_id()) > 0))",
		errno.ErrCheckConstraintNamedFunctionIsNotAllowed,
	)
	tk.MustGetErrCode(
		"create table locator_default (rid varchar(255) default (tidb_row_id()))",
		errno.ErrDefValGeneratedNamedFunctionIsNotAllowed,
	)
	tk.MustGetErrCode(
		"create table locator_partition (a bigint) partition by hash(tidb_row_id()) partitions 2",
		errno.ErrPartitionFunctionIsNotAllowed,
	)
}

func TestTiDBRowIDUsesTableSelectPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	root.MustExec("create table test.t (a bigint primary key clustered, b int)")
	root.MustExec("create table test.u (c int)")
	root.MustExec("insert into test.t values (1, 10), (2, 20)")
	root.MustExec("insert into test.u values (1)")
	root.MustExec("create user 'row_id_user'@'localhost'")

	user := testkit.NewTestKit(t, store)
	err := user.Session().Auth(&auth.UserIdentity{Username: "row_id_user", Hostname: "localhost"}, nil, nil, nil)
	require.NoError(t, err)
	user.MustGetErrCode("select tidb_row_id() from test.t", errno.ErrTableaccessDenied)
	user.MustGetErrCode("select count(*) from test.t", errno.ErrTableaccessDenied)

	root.MustExec("grant select(b) on test.t to 'row_id_user'@'localhost'")
	user.MustQuery("select count(*) from test.t").Check(testkit.Rows("2"))
	user.MustGetErrCode("select tidb_row_id() from test.t", errno.ErrTableaccessDenied)
	user.MustGetErrCode("select tidb_decode_key(tidb_row_id()) from test.t", errno.ErrTableaccessDenied)

	root.MustExec("revoke select(b) on test.t from 'row_id_user'@'localhost'")
	root.MustExec("grant select on test.t to 'row_id_user'@'localhost'")
	user.MustGetErrCode("select tidb_row_id(t) from test.t t join test.u u", errno.ErrTableaccessDenied)
	token := root.MustQuery("select tidb_row_id() from test.t where a = 1").Rows()[0][0]
	user.MustGetErrCode(
		"update test.t set b = 11 where tidb_row_id() = '"+token.(string)+"'",
		errno.ErrColumnaccessDenied,
	)
	root.MustExec("grant update on test.t to 'row_id_user'@'localhost'")
	user.MustExec("update test.t set b = 11 where tidb_row_id() = ?", token)
	root.MustQuery("select b from test.t where a = 1").Check(testkit.Rows("11"))
	user.MustGetErrCode(
		"delete from test.t where tidb_row_id() = '"+token.(string)+"'",
		errno.ErrTableaccessDenied,
	)
	root.MustExec("grant delete on test.t to 'row_id_user'@'localhost'")
	user.MustExec("delete from test.t where tidb_row_id() = ?", token)
	root.MustQuery("select a from test.t order by a").Check(testkit.Rows("2"))
}
