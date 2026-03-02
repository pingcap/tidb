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

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlsess "github.com/pingcap/tidb/pkg/ddl/session"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewBuildReadTSQueryTypeAlignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1)")

	ddlSe := ddlsess.NewSession(tk.Session())
	ctx := context.Background()

	tk.MustExec("select * from t")
	expected := tk.Session().GetSessionVars().LastQueryInfo.StartTS
	require.NotZero(t, expected)

	rows, err := ddlSe.Execute(ctx,
		"SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
		"create-materialized-view-build-read-ts-ut",
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	readTS := rows[0].GetUint64(0)
	require.Equal(t, expected, readTS)
}

func TestMaterializedViewDDLBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originSplit := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, originSplit)
	tk.MustExec("set @@session.tidb_scatter_region='table'")
	tk.MustExec("create table t (a int not null, b int not null)")

	// Base table must have MV LOG first.
	err := tk.ExecToErr("create materialized view mv_no_log (a, cnt) as select a, count(1) from t group by a")
	require.ErrorContains(t, err, "materialized view log does not exist")

	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	// Physical table created.
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='mv'").Check(testkit.Rows("1"))

	showCreate := tk.MustQuery("show create table mv").Rows()[0][1].(string)
	require.Contains(t, showCreate, "PRIMARY KEY (`a`)")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mlogTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
	require.NoError(t, err)

	require.NotNil(t, mvTable.Meta().MaterializedView)
	require.Equal(t, []int64{baseTable.Meta().ID}, mvTable.Meta().MaterializedView.BaseTableIDs)
	require.Equal(t, "FAST", mvTable.Meta().MaterializedView.RefreshMethod)
	require.Equal(t, "", mvTable.Meta().MaterializedView.RefreshStartWith)
	require.Equal(t, "NOW()", mvTable.Meta().MaterializedView.RefreshNext)
	expectedTZName, expectedTZOffset := ddlutil.GetTimeZone(tk.Session())
	require.Equal(t, tk.Session().GetSessionVars().SQLMode, mvTable.Meta().MaterializedView.DefinitionSQLMode)
	require.Equal(t, expectedTZName, mvTable.Meta().MaterializedView.DefinitionTimeZone.Name)
	require.Equal(t, expectedTZOffset, mvTable.Meta().MaterializedView.DefinitionTimeZone.Offset)
	tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO > 0 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.Meta().ID)).
		Check(testkit.Rows("1"))

	// Base table reverse mapping maintained by DDL.
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogTable.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Contains(t, baseTable.Meta().MaterializedViewBase.MViewIDs, mvTable.Meta().ID)

	// MV must contain count(*|1).
	err = tk.ExecToErr("create materialized view mv_bad (a, s) as select a, sum(b) from t group by a")
	require.ErrorContains(t, err, "must contain count(*)/count(1)")

	// count(column) is supported (but CREATE MATERIALIZED VIEW still requires count(*|1) in the SELECT list).
	tk.MustExec("create materialized view mv_count_col (a, cnt_b, cnt) as select a, count(b), count(1) from t group by a")
	tk.MustQuery("select a, cnt_b, cnt from mv_count_col order by a").Check(testkit.Rows("1 2 2", "2 1 1"))
	is = dom.InfoSchema()
	mvCountColTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_count_col"))
	require.NoError(t, err)
	require.Equal(t, "FAST", mvCountColTable.Meta().MaterializedView.RefreshMethod)
	require.Equal(t, "", mvCountColTable.Meta().MaterializedView.RefreshStartWith)
	require.Equal(t, "", mvCountColTable.Meta().MaterializedView.RefreshNext)

	// Aggregate function names are case-insensitive in CREATE MATERIALIZED VIEW.
	tk.MustExec("create materialized view mv_upper_agg (a, s, cnt) as select a, SUM(b), COUNT(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv_upper_agg order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	// Non-deterministic WHERE and unsupported aggregate should fail.
	err = tk.ExecToErr("create materialized view mv_bad_avg (a, avgv, c) as select a, avg(b), count(1) from t group by a")
	require.ErrorContains(t, err, "unsupported aggregate function")
	err = tk.ExecToErr("create materialized view mv_bad_where (a, c) as select a, count(1) from t where rand() > 0 group by a")
	require.ErrorContains(t, err, "WHERE clause must be deterministic")

	// SUM/MIN/MAX currently require NOT NULL argument columns.
	tk.MustExec("create table t_sum_nullable (a int not null, b int)")
	tk.MustExec("create materialized view log on t_sum_nullable (a, b) purge next date_add(now(), interval 1 hour)")
	err = tk.ExecToErr("create materialized view mv_bad_sum_nullable (a, s, c) as select a, sum(b), count(1) from t_sum_nullable group by a")
	require.ErrorContains(t, err, "only supports SUM/MIN/MAX on NOT NULL column")

	// MIN/MAX requires a base-table index whose leading columns cover all GROUP BY columns.
	tk.MustExec("create table t_minmax_bad (a int not null, b int not null, c int not null, index idx_cab(c, a, b))")
	tk.MustExec("create materialized view log on t_minmax_bad (a, b, c) purge next date_add(now(), interval 1 hour)")
	err = tk.ExecToErr("create materialized view mv_bad_minmax_index (a, b, minc, c1) as select a, b, min(c), count(1) from t_minmax_bad group by a, b")
	require.ErrorContains(t, err, "requires base table index whose leading columns cover all GROUP BY columns")
	err = tk.ExecToErr("create materialized view mv_bad_minmax_index_upper (a, b, minc, c1) as select a, b, MIN(c), COUNT(1) from t_minmax_bad group by a, b")
	require.ErrorContains(t, err, "requires base table index whose leading columns cover all GROUP BY columns")
	tk.MustExec("create table t_minmax_ok (a int not null, b int not null, c int not null, index idx_bac(b, a, c))")
	tk.MustExec("create materialized view log on t_minmax_ok (a, b, c) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_minmax_ok (a, b, minc, c1) as select a, b, min(c), count(1) from t_minmax_ok group by a, b")

	// SELECT clauses outside Stage-1 scope should be rejected.
	err = tk.ExecToErr("create materialized view mv_bad_distinct (a, c) as select distinct a, count(1) from t group by a")
	require.ErrorContains(t, err, "does not support SELECT DISTINCT")
	err = tk.ExecToErr("create materialized view mv_bad_having (a, c) as select a, count(1) from t group by a having count(1) > 0")
	require.ErrorContains(t, err, "does not support HAVING clause")
	err = tk.ExecToErr("create materialized view mv_bad_order (a, c) as select a, count(1) from t group by a order by a")
	require.ErrorContains(t, err, "does not support ORDER BY clause")
	err = tk.ExecToErr("create materialized view mv_bad_limit (a, c) as select a, count(1) from t group by a limit 1")
	require.ErrorContains(t, err, "does not support LIMIT clause")
	err = tk.ExecToErr("create materialized view mv_bad_rollup (a, c) as select a, count(1) from t group by a with rollup")
	require.ErrorContains(t, err, "does not support GROUP BY WITH ROLLUP")

	// Aliased base table in WHERE should work.
	tk.MustExec("create materialized view mv_alias (a, c) as select x.a, count(1) from t x where x.b > 0 group by x.a")
	tk.MustQuery("select a, c from mv_alias order by a").Check(testkit.Rows("1 2", "2 1"))

	// MV LOG must contain all referenced columns.
	tk.MustExec("create table t_mlog_missing (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mlog_missing (a) purge next date_add(now(), interval 1 hour)")
	err = tk.ExecToErr("create materialized view mv_bad_mlog_cols (a, s, c) as select a, sum(b), count(1) from t_mlog_missing group by a")
	require.ErrorContains(t, err, "does not contain column b")
	err = tk.ExecToErr("create materialized view mv_bad_mlog_cols_count (a, cnt_b, cnt) as select a, count(b), count(1) from t_mlog_missing group by a")
	require.ErrorContains(t, err, "does not contain column b")

	// Nullable group-by key should use UNIQUE KEY.
	tk.MustExec("create table t_nullable (a int, b int)")
	tk.MustExec("create materialized view log on t_nullable (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_nullable (a, c) as select a, count(1) from t_nullable group by a")
	showCreate = tk.MustQuery("show create table mv_nullable").Rows()[0][1].(string)
	require.Contains(t, showCreate, "UNIQUE KEY")
	require.False(t, strings.Contains(showCreate, "PRIMARY KEY (`a`)"))

	// CREATE MATERIALIZED VIEW supports SHARD_ROW_ID_BITS/PRE_SPLIT_REGIONS and follows table pre-split flow.
	tk.MustExec("create table t_presplit (a int, b int not null)")
	tk.MustExec("create materialized view log on t_presplit (a) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_presplit (a, cnt) shard_row_id_bits = 2 pre_split_regions = 2 as select a, count(1) from t_presplit group by a")
	showCreate = tk.MustQuery("show create table mv_presplit").Rows()[0][1].(string)
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS=2")
	require.Contains(t, showCreate, "PRE_SPLIT_REGIONS=2")

	is = dom.InfoSchema()
	mvPreSplit, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_presplit"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mvPreSplit.Meta().ShardRowIDBits)
	require.Equal(t, uint64(2), mvPreSplit.Meta().PreSplitRegions)

	regions := tk.MustQuery("show table mv_presplit regions").Rows()
	regionNames := make([]string, 0, len(regions))
	for _, row := range regions {
		regionNames = append(regionNames, fmt.Sprint(row[1]))
	}
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_2305843009213693952", mvPreSplit.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_4611686018427387904", mvPreSplit.Meta().ID))
	require.Contains(t, regionNames, fmt.Sprintf("t_%d_r_6917529027641081856", mvPreSplit.Meta().ID))

	// Index DDL on MV-related tables:
	// base table and MV table are allowed, MV LOG table is disallowed.
	tk.MustExec("create index idx_base_b on t (b)")
	tk.MustExec("drop index idx_base_b on t")
	tk.MustExec("create index idx_mv_s on mv (s)")
	tk.MustExec("drop index idx_mv_s on mv")
	tk.MustExec("alter table t add column c int")
	tk.MustExec("alter table t add index idx_base_c_alter (c)")
	err = tk.ExecToErr("alter table t modify column a bigint")
	require.ErrorContains(t, err, "referenced by materialized view log")
	tk.MustExec("alter table t drop index idx_base_c_alter")
	tk.MustExec("alter table mv add index idx_mv_s_alter (s)")
	tk.MustExec("alter table mv drop index idx_mv_s_alter")

	// ALTER TABLE ... SET TIFLASH REPLICA is allowed on both base table and MV table.
	err = tk.ExecToErr("alter table t set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on base table with materialized view dependencies")
	}
	err = tk.ExecToErr("alter table mv set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on materialized view table")
	}

	// MV LOG cannot be dropped while dependent MVs exist.
	err = tk.ExecToErr("drop materialized view log on t")
	require.ErrorContains(t, err, "dependent materialized views exist")

	// Drop MV and then drop MV LOG.
	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view mv_count_col")
	tk.MustExec("drop materialized view mv_upper_agg")
	tk.MustExec("drop materialized view mv_alias")
	tk.MustExec("drop materialized view mv_nullable")
	tk.MustExec("drop materialized view mv_minmax_ok")
	tk.MustExec("drop materialized view mv_presplit")
	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop materialized view log on t_nullable")
	tk.MustExec("drop materialized view log on t_sum_nullable")
	tk.MustExec("drop materialized view log on t_minmax_bad")
	tk.MustExec("drop materialized view log on t_minmax_ok")
	tk.MustExec("drop materialized view log on t_presplit")
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	// Reverse mapping cleared.
	is = dom.InfoSchema()
	baseTable, err = is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestShowCreateMaterializedView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_show_mv (a int, b int not null)")
	tk.MustExec("create materialized view log on t_show_mv (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_show_mv (a, s, cnt) comment = 'c1' refresh fast next now() shard_row_id_bits = 2 pre_split_regions = 2 as select a, sum(b), count(1) from t_show_mv group by a")

	rows := tk.MustQuery("show create materialized view mv_show_mv").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "mv_show_mv", rows[0][0])
	showCreate, ok := rows[0][1].(string)
	require.True(t, ok)
	require.Contains(t, showCreate, "CREATE MATERIALIZED VIEW `mv_show_mv` (`a`, `s`, `cnt`)")
	require.Contains(t, showCreate, "COMMENT = 'c1'")
	require.Contains(t, showCreate, "REFRESH FAST NEXT NOW()")
	require.Contains(t, showCreate, "SHARD_ROW_ID_BITS = 2 PRE_SPLIT_REGIONS = 2")
	require.Contains(t, showCreate, "AS SELECT `a`,SUM(`b`),COUNT(1) FROM `test`.`t_show_mv` GROUP BY `a`")
	require.Equal(t, "utf8mb4", rows[0][2])
	require.Equal(t, "utf8mb4_bin", rows[0][3])
}

func TestCreateMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	err := tk.ExecToErr("create materialized view mv_bad_next (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("create materialized view mv_bad_start (a, s, cnt) refresh fast start with 1 next now() as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")

	tk.MustExec("create materialized view mv_ok (a, s, cnt) refresh fast start with now() next now() as select a, sum(b), count(1) from t group by a")
	tk.MustExec("drop materialized view mv_ok")
	tk.MustExec("drop materialized view log on t")
}

func TestAlterMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_ok (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	err := tk.ExecToErr("alter materialized view mv_ok refresh next 300")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view mv_ok refresh start with 1 next now()")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view mv_ok refresh start with now() next now()")
	tk.MustExec("drop materialized view mv_ok")
	tk.MustExec("drop materialized view log on t")
}

func TestAlterMaterializedViewRefreshUpdatesMetaAndNextTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a")

	getMViewMeta := func() (int64, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, mvTable.Meta().MaterializedView)
		return mvTable.Meta().ID, mvTable.Meta().MaterializedView
	}

	mviewID, mvInfo := getMViewMeta()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv refresh start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, mvInfo = getMViewMeta()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 30 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("alter materialized view mv refresh next date_add(now(), interval 25 minute)")
	_, mvInfo = getMViewMeta()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 15 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 1 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Check(testkit.Rows("1 1 1"))

	beforeRows := tk.MustQuery(fmt.Sprintf(
		"select TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Rows()
	tk.MustExec("alter materialized view mv refresh")
	_, mvInfo = getMViewMeta()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "", mvInfo.RefreshStartWith)
	require.Equal(t, "", mvInfo.RefreshNext)
	afterRows := tk.MustQuery(fmt.Sprintf(
		"select TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mviewID,
	)).Rows()
	require.Equal(t, beforeRows, afterRows)

	tk.MustExec("drop materialized view mv")
	tk.MustExec("drop materialized view log on t")
}

func TestCreateMaterializedViewRefreshInfoNextTimeDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	// START WITH and NEXT both present, START WITH is not near-now: NEXT_TIME should use START WITH.
	tk.MustExec("create materialized view mv_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvStartOnlyID := getMViewID("mv_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 30 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// NEXT only: NEXT_TIME should use evaluated NEXT.
	tk.MustExec("create materialized view mv_next_only (a, s, cnt) refresh fast next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvNextOnlyID := getMViewID("mv_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 10 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 1 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	// Neither START WITH nor NEXT: NEXT_TIME should stay unchanged (create path: NULL).
	tk.MustExec("create materialized view mv_no_schedule (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	mvNoScheduleID := getMViewID("mv_no_schedule")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNoScheduleID,
	)).Check(testkit.Rows("1"))

	// START WITH near-now and NEXT present: NEXT_TIME should use NEXT.
	tk.MustExec("create materialized view mv_near_now (a, s, cnt) refresh fast start with now() next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t group by a")
	mvNearNowID := getMViewID("mv_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, NEXT_TIME > UTC_TIMESTAMP() + interval 20 minute, NEXT_TIME < UTC_TIMESTAMP() + interval 2 hour from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNearNowID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("drop materialized view mv_start_only")
	tk.MustExec("drop materialized view mv_next_only")
	tk.MustExec("drop materialized view mv_no_schedule")
	tk.MustExec("drop materialized view mv_near_now")
	tk.MustExec("drop materialized view log on t")
}

func TestCreateMaterializedViewRefreshInfoNextTimeUsesUTC(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	tk.MustExec("create materialized view mv_utc_next (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	mvID := getMViewID("mv_utc_next")

	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, "+
			"NEXT_TIME > UTC_TIMESTAMP(6) - interval 5 minute, "+
			"NEXT_TIME < UTC_TIMESTAMP(6) + interval 5 minute, "+
			"NEXT_TIME < NOW(6) - interval 7 hour "+
			"from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows("1 1 1 1"))

	// START WITH should also be evaluated in UTC even when session timezone is +08:00.
	tk.MustExec("create materialized view mv_utc_start (a, s, cnt) refresh fast start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvStartID := getMViewID("mv_utc_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_TIME is not null, "+
			"NEXT_TIME > UTC_TIMESTAMP(6) + interval 20 minute, "+
			"NEXT_TIME < UTC_TIMESTAMP(6) + interval 2 hour, "+
			"NEXT_TIME < NOW(6) - interval 7 hour "+
			"from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartID,
	)).Check(testkit.Rows("1 1 1 1"))

	tk.MustExec("drop materialized view mv_utc_next")
	tk.MustExec("drop materialized view mv_utc_start")
	tk.MustExec("drop materialized view log on t")
}

func TestCreateMaterializedViewRefreshInfoRunningAndSuccess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_state (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	}()

	var initTS uint64
	var mviewID int64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select MVIEW_ID, LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		id, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || id == 0 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][1]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		mviewID = id
		initTS = ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	err := <-ddlDone
	require.NoError(t, err)
	tk.MustQuery("select a, s, cnt from mv_state order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	rows := tk.MustQuery(fmt.Sprintf("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mviewID)).Rows()
	require.Len(t, rows, 1)
	finalTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	// finalTS must be greater than initTS when the build is successful.
	require.Greater(t, finalTS, initTS)
}

func TestCreateMaterializedViewSuccessRefreshInfoVisibilityBeforeCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const afterUpsertFailpoint = "github.com/pingcap/tidb/pkg/ddl/afterCreateMaterializedViewSuccessRefreshInfoUpsert"
	const postUpsertRetryableErr = "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr"

	paused := make(chan struct{})
	resume := make(chan struct{})
	var pausedOnce sync.Once
	var resumeOnce sync.Once
	release := func() {
		resumeOnce.Do(func() {
			close(resume)
		})
	}
	testfailpoint.EnableCall(t, afterUpsertFailpoint, func() {
		pausedOnce.Do(func() {
			close(paused)
		})
		<-resume
	})

	require.NoError(t, failpoint.Enable(postUpsertRetryableErr, "1*return(true)"))
	defer func() {
		release()
		require.NoError(t, failpoint.Disable(postUpsertRetryableErr))
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_upsert_visibility (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	}()

	var prewriteTS uint64
	require.Eventually(t, func() bool {
		rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
		if len(rows) != 1 {
			return false
		}
		ts, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
		if err != nil || ts == 0 {
			return false
		}
		prewriteTS = ts
		return true
	}, 30*time.Second, 100*time.Millisecond)

	select {
	case <-paused:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for post-upsert failpoint")
	}

	// The post-build success upsert should stay invisible before this DDL step commits.
	rows := tk.MustQuery("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info").Rows()
	require.Len(t, rows, 1)
	visibleTS, err := strconv.ParseUint(fmt.Sprint(rows[0][0]), 10, 64)
	require.NoError(t, err)
	require.Equal(t, prewriteTS, visibleTS)

	release()

	err = <-ddlDone
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")
	tk.MustQuery("show tables like 'mv_upsert_visibility'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewBuildFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_fail (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewBuildContextCanceledRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", `return("context-canceled")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_ctx_cancel (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_ctx_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create view v as select a from t")

	err := tk.ExecToErr("create materialized view mv_v (a, c) as select a, count(1) from v group by a")
	require.ErrorContains(t, err, "is not BASE TABLE")
}

func TestCreateMaterializedViewCancelRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	tkCancel := testkit.NewTestKit(t, store)
	tkCancel.MustExec("use test")

	const pauseBuildFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_cancel (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	}()

	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCancel.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 {
			return false
		}
		if len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = rows[0][0].(string)
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCancel.MustExec("admin cancel ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	err := <-ddlDone
	require.ErrorContains(t, err, "Cancelled DDL job")

	rows := tkCancel.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, "rollback done", rows[0][len(rows[0])-2])

	tk.MustQuery("show tables like 'mv_cancel'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewPauseAndResume(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	pauseBuildFailpoint := "github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild"
	require.NoError(t, failpoint.Enable(pauseBuildFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
		}
	}()

	ddlDone := make(chan error, 1)
	go func() {
		tkDDL := testkit.NewTestKit(t, store)
		tkDDL.MustExec("use test")
		ddlDone <- tkDDL.ExecToErr("create materialized view mv_pause (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	}()

	tkCtl := testkit.NewTestKit(t, store)
	tkCtl.MustExec("use test")
	jobID := ""
	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
		if len(rows) == 0 {
			return false
		}
		if len(rows[0]) < 5 || strings.ToLower(fmt.Sprint(rows[0][4])) != "write reorganization" {
			return false
		}
		jobID = fmt.Sprint(rows[0][0])
		return jobID != ""
	}, 30*time.Second, 100*time.Millisecond)

	tkCtl.MustExec("admin pause ddl jobs " + jobID)
	require.NoError(t, failpoint.Disable(pauseBuildFailpoint))
	enabled = false

	require.Eventually(t, func() bool {
		rows := tkCtl.MustQuery("admin show ddl jobs where JOB_ID=" + jobID).Rows()
		if len(rows) == 0 {
			return false
		}
		state := strings.ToLower(fmt.Sprint(rows[0][len(rows[0])-2]))
		return state == "paused" || state == "pausing"
	}, 30*time.Second, 100*time.Millisecond)

	// Current Stage-1 semantics: MV table is visible once phase-1 finishes, even before job completion.
	tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows("mv_pause"))

	tkCtl.MustExec("admin resume ddl jobs " + jobID)
	select {
	case err := <-ddlDone:
		if err != nil {
			require.ErrorContains(t, err, "detected residual build rows on retry")
			tk.MustQuery("show tables like 'mv_pause'").Check(testkit.Rows())
			return
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out waiting CREATE MATERIALIZED VIEW to finish after resume")
	}

	tk.MustQuery("select a, s, cnt from mv_pause order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateMaterializedViewRollbackIgnoreMissingRefreshInfoTable(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_missing_refresh_meta (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)

	tk.MustQuery("show tables like 'mv_missing_refresh_meta'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoUpsertFailureRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_upsert_fail (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	require.NotContains(t, err.Error(), "Duplicate entry")

	tk.MustQuery("show tables like 'mv_upsert_fail'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))
	rows := tk.MustQuery("admin show ddl jobs where JOB_TYPE='create materialized view'").Rows()
	require.NotEmpty(t, rows)
	jobID := fmt.Sprint(rows[0][0])
	tk.MustQuery("select ((select count(*) from mysql.gc_delete_range where job_id=" + jobID + ") + (select count(*) from mysql.gc_delete_range_done where job_id=" + jobID + ")) > 0").Check(testkit.Rows("1"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRetryWithResidualBuildRowsRollback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (null, 7), (null, 3)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry_residual (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "detected residual build rows on retry")
	require.NotContains(t, err.Error(), "Duplicate entry")

	tk.MustQuery("show tables like 'mv_retry_residual'").Check(testkit.Rows())
	tk.MustQuery("select count(*) from mysql.tidb_mview_refresh_info").Check(testkit.Rows("0"))

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.NotZero(t, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestDropMaterializedViewLogRecheckWithConcurrentCreateMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck"
	require.NoError(t, failpoint.Enable(pauseDropFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseDropFailpoint))
		}
	}()

	dropErrCh := make(chan error, 1)
	go func() {
		tkDrop := testkit.NewTestKit(t, store)
		tkDrop.MustExec("use test")
		dropErrCh <- tkDrop.ExecToErr("drop materialized view log on t")
	}()

	// Let the drop SQL pass executor pre-check and pause before DDL job submit.
	time.Sleep(500 * time.Millisecond)

	tk.MustExec("create materialized view mv_dep (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	require.NoError(t, failpoint.Disable(pauseDropFailpoint))
	enabled = false

	err := <-dropErrCh
	require.ErrorContains(t, err, "dependent materialized views exist")
	tk.MustQuery("show tables like '$mlog$t'").Check(testkit.Rows("$mlog$t"))

	tk.MustExec("drop materialized view mv_dep")
	tk.MustExec("drop materialized view log on t")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestCreateMaterializedViewRetryAfterUpsertFailure(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists", "1*return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists"))
	}()

	err := tk.ExecToErr("create materialized view mv_retry (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	require.Error(t, err)
	require.ErrorContains(t, err, "tidb_mview_refresh_info")
	require.NotContains(t, err.Error(), "Information schema is changed")
	tk.MustQuery("show tables like 'mv_retry'").Check(testkit.Rows())

	tk.MustExec("create materialized view mv_retry (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")
	tk.MustQuery("select a, s, cnt from mv_retry order by a").Check(testkit.Rows("1 15 2", "2 7 1"))
}

func TestCreateTableLikeShouldNotCarryMaterializedViewMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_src (a, s, cnt) refresh fast next now() as select a, sum(b), count(1) from t group by a")

	tk.MustExec("create table t_like like t")
	tk.MustExec("create table mv_like like mv_src")
	tk.MustExec("create table mlog_like like `$mlog$t`")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t"))
	require.NoError(t, err)
	mvSrc, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_src"))
	require.NoError(t, err)
	mvLike, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mv_like"))
	require.NoError(t, err)
	mlogSrc, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mlogLike, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("mlog_like"))
	require.NoError(t, err)

	require.NotNil(t, mvSrc.Meta().MaterializedView)
	require.NotNil(t, mlogSrc.Meta().MaterializedViewLog)

	require.Nil(t, mvLike.Meta().MaterializedView)
	require.Nil(t, mvLike.Meta().MaterializedViewLog)
	require.Nil(t, mvLike.Meta().MaterializedViewBase)
	require.Nil(t, mlogLike.Meta().MaterializedView)
	require.Nil(t, mlogLike.Meta().MaterializedViewLog)
	require.Nil(t, mlogLike.Meta().MaterializedViewBase)

	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogSrc.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, []int64{mvSrc.Meta().ID}, baseTable.Meta().MaterializedViewBase.MViewIDs)
}
