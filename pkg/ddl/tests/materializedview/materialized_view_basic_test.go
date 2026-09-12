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

package materializedview

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCreateMaterializedViewValidationCoverage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_validation (a int not null, b int not null)")
	tk.MustExec("insert into t_mv_validation values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_mv_validation (a, b)")

	// COUNT(column) is supported when the required COUNT(*)/COUNT(1) is also present.
	tk.MustExec("create materialized view mv_count_column (a, cnt_b, cnt) as select a, count(b), count(1) from t_mv_validation group by a")
	tk.MustQuery("select a, cnt_b, cnt from mv_count_column order by a").Check(testkit.Rows("1 2 2", "2 1 1"))

	// Aggregate function names are case-insensitive.
	tk.MustExec("create materialized view mv_upper_aggregate (a, s, cnt) as select a, SUM(b), COUNT(1) from t_mv_validation group by a")
	tk.MustQuery("select a, s, cnt from mv_upper_aggregate order by a").Check(testkit.Rows("1 15 2", "2 7 1"))

	// MIN/MAX still require a supporting base-table index, regardless of function case.
	tk.MustExec("create table t_mv_minmax_validation (a int not null, b int not null, c int not null, index idx_cab(c, a, b))")
	tk.MustExec("create materialized view log on t_mv_minmax_validation (a, b, c)")
	err := tk.ExecToErr("create materialized view mv_upper_min (a, b, minc, cnt) as select a, b, MIN(c), COUNT(1) from t_mv_minmax_validation group by a, b")
	require.ErrorContains(t, err, "requires base table index whose leading columns cover all GROUP BY columns")

	// Every referenced column must be present in the materialized view log.
	tk.MustExec("create table t_mv_missing_column (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_missing_column (a)")
	err = tk.ExecToErr("create materialized view mv_missing_count_column (a, cnt_b, cnt) as select a, count(b), count(1) from t_mv_missing_column group by a")
	require.ErrorContains(t, err, "does not contain column b")
}

func TestCreateMaterializedViewNullableAggregateValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_mv_nullable_sum (a int not null, b int)")
	tk.MustExec("create materialized view log on t_mv_nullable_sum (a, b)")
	err := tk.ExecToErr("create materialized view mv_nullable_sum_bad (a, s, cnt) as select a, sum(b), count(1) from t_mv_nullable_sum group by a")
	require.ErrorContains(t, err, "requires matching COUNT")
	tk.MustExec("create materialized view mv_nullable_sum (a, s, cnt_b, cnt) as select a, sum(b), count(b), count(1) from t_mv_nullable_sum group by a")
	tk.MustExec("create materialized view mv_nullable_sum_duplicate_count (a, s, cnt_b1, cnt_b2, cnt) as select a, sum(b), count(b) as cnt_b1, count(b) as cnt_b2, count(1) from t_mv_nullable_sum group by a")

	tk.MustExec("create table t_mv_nullable_minmax (a int not null, b int, index idx_ab(a, b))")
	tk.MustExec("create materialized view log on t_mv_nullable_minmax (a, b)")
	tk.MustExec("create materialized view mv_nullable_minmax (a, minb, maxb, cnt) as select a, min(b), max(b), count(1) from t_mv_nullable_minmax group by a")
}

func TestMaterializedViewBaseSetTiFlashReplica(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_tiflash (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_tiflash (a, b)")
	tk.MustExec("create materialized view mv_tiflash (a, cnt) as select a, count(1) from t_mv_tiflash group by a")

	// TiFlash availability is environment-dependent in mock storage. The MV dependency
	// guard must not reject this operation; a storage-related error is acceptable here.
	err := tk.ExecToErr("alter table t_mv_tiflash set tiflash replica 1")
	if err != nil {
		require.NotContains(t, err.Error(), "ALTER TABLE on base table with materialized view dependencies")
	}
}

func TestMaterializedViewCommentOnlyBaseColumnModify(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_comment (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_comment (a, b)")
	tk.MustExec("create materialized view mv_comment (a, cnt) as select a, count(1) from t_mv_comment group by a")

	tk.MustExec("alter table t_mv_comment modify column b int not null comment 'comment-only change'")
	tk.MustQuery("select column_comment from information_schema.columns where table_schema = 'test' and table_name = 't_mv_comment' and column_name = 'b'").Check(testkit.Rows("comment-only change"))
	tk.MustExec("alter table t_mv_comment modify column b int not null comment 'comment-only change'")
}

func TestCreateMaterializedViewColumnFlags(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table base_mv_flags(id bigint not null auto_increment primary key, g1 int not null, v1 bigint not null, key idx_g1_id(g1, id))")
	tk.MustExec("create materialized view log on base_mv_flags(id, g1, v1) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_flags (g1, cnt, s_v1, min_id, max_id) as select g1, count(1), sum(v1), min(id), max(id) from base_mv_flags group by g1")
	for _, col := range []string{"min_id", "max_id"} {
		tk.MustQuery(fmt.Sprintf("select column_key from information_schema.columns where table_schema='test' and table_name='mv_flags' and column_name='%s'", col)).Check(testkit.Rows(""))
		tk.MustQuery(fmt.Sprintf("select extra from information_schema.columns where table_schema='test' and table_name='mv_flags' and column_name='%s'", col)).Check(testkit.Rows(""))
	}
}

func TestMaterializedViewCommentLength(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_comment_len (id bigint not null primary key, g1 int not null, v1 bigint not null, key idx_g1(g1))")
	tk.MustExec("create materialized view log on t_mv_comment_len (id, g1, v1)")

	maxTableCommentLength := ddl.MaxCommentLength * 2
	commentMaxLen := strings.Repeat("y", maxTableCommentLength)
	commentTooLong := strings.Repeat("y", maxTableCommentLength+1)
	createMVSQL := func(name, comment string) string {
		return fmt.Sprintf("create materialized view %s (g1, cnt) comment = '%s' refresh fast as select g1, count(1) from t_mv_comment_len group by g1", name, comment)
	}
	errTooLongComment := func(name string) string {
		return fmt.Sprintf("Comment for table '%s' is too long (max = %d)", name, maxTableCommentLength)
	}

	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES'")
	tk.MustExec(createMVSQL("mv_comment_max", commentMaxLen))
	err := tk.ExecToErr(createMVSQL("mv_comment_too_long", commentTooLong))
	require.ErrorContains(t, err, errTooLongComment("mv_comment_too_long"))

	tk.MustExec("set @@sql_mode=''")
	tk.MustExec(createMVSQL("mv_comment_truncated", commentTooLong))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1628|"+errTooLongComment("mv_comment_truncated")))
	tk.MustQuery("select length(table_comment) from information_schema.tables where table_schema = 'test' and table_name = 'mv_comment_truncated'").Check(testkit.Rows(strconv.Itoa(maxTableCommentLength)))
}

func TestCreateMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	err := tk.ExecToErr("create materialized view mv_bad_next (a, s, cnt) refresh fast next 300 as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")
	err = tk.ExecToErr("create materialized view mv_bad_start (a, s, cnt) refresh fast start with 1 next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")
	tk.MustExec("create materialized view mv_ok (a, s, cnt) refresh fast start with now() next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
}

func TestDropMaterializedViewWhenDisabled(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_when_disabled (a int not null)")
	tk.MustExec("create materialized view log on t_drop_when_disabled (a)")
	tk.MustExec("create materialized view mv_drop_when_disabled (a, cnt) as select a, count(1) from t_drop_when_disabled group by a")

	tk.MustExec("set tidb_mview_enable = off")
	tk.MustExec("drop materialized view mv_drop_when_disabled")
	tk.MustExec("drop materialized view log on t_drop_when_disabled")
}

func TestDropMaterializedViewIfExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop materialized view if exists missing_mv")
	tk.MustExec("drop materialized view if exists missing_schema.missing_mv")

	err := tk.ExecToErr("drop materialized view log if exists on missing_base")
	require.ErrorContains(t, err, "Table 'test.missing_base' doesn't exist")
	err = tk.ExecToErr("drop materialized view log if exists on missing_schema.missing_base")
	require.ErrorContains(t, err, "Table 'missing_schema.missing_base' doesn't exist")

	tk.MustExec("create table t_drop_if_exists (a int not null)")
	tk.MustExec("create table t_no_mlog_drop_if_exists (a int not null)")
	tk.MustExec("create materialized view log on t_drop_if_exists (a)")
	tk.MustExec("create materialized view mv_drop_if_exists (a, cnt) as select a, count(1) from t_drop_if_exists group by a")

	err = tk.ExecToErr("drop materialized view if exists t_drop_if_exists")
	require.ErrorContains(t, err, "is not MATERIALIZED VIEW")
	err = tk.ExecToErr("drop materialized view log if exists on mv_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view if exists mv_drop_if_exists")
	tk.MustExec("drop materialized view if exists mv_drop_if_exists")

	tk.MustExec("create view v_drop_if_exists as select * from t_drop_if_exists")
	err = tk.ExecToErr("drop materialized view log if exists on v_drop_if_exists")
	require.ErrorContains(t, err, "is not BASE TABLE")

	tk.MustExec("drop materialized view log if exists on t_no_mlog_drop_if_exists")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1051 Unknown table 'test.t_no_mlog_drop_if_exists'"))
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
	tk.MustExec("drop materialized view log if exists on t_drop_if_exists")
}

func TestAlterMaterializedViewRefreshExprTypeValidation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")

	err := tk.ExecToErr("alter materialized view mv refresh next 300")
	require.ErrorContains(t, err, "REFRESH NEXT expression must return DATETIME/TIMESTAMP")

	err = tk.ExecToErr("alter materialized view mv refresh start with 1 next date_add(now(), interval 1 hour)")
	require.ErrorContains(t, err, "REFRESH START WITH expression must return DATETIME/TIMESTAMP")

	tk.MustExec("alter materialized view mv refresh start with now() next date_add(now(), interval 1 hour)")
}

func TestCreateMaterializedViewRejectsUnsupportedSelectClauses(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t (a, b)")

	tests := []struct {
		name    string
		sql     string
		errPart string
	}{
		{
			name:    "cte",
			sql:     "create materialized view mv_cte (a, s, cnt) as with cte as (select a from t) select a, sum(b), count(1) from t group by a",
			errPart: "common table expressions",
		},
		{
			name:    "locking clause",
			sql:     "create materialized view mv_lock (a, s, cnt) as select a, sum(b), count(1) from t group by a for update",
			errPart: "locking clauses",
		},
		{
			name:    "select into",
			sql:     "create materialized view mv_into (a, s, cnt) as select a, sum(b), count(1) from t group by a into outfile '/tmp/mv.out'",
			errPart: "SELECT INTO",
		},
		{
			name:    "as of",
			sql:     "create materialized view mv_as_of (a, s, cnt) as select a, sum(b), count(1) from t as of timestamp now() group by a",
			errPart: "AS OF",
		},
		{
			name:    "table sample",
			sql:     "create materialized view mv_sample (a, s, cnt) as select a, sum(b), count(1) from t tablesample system (50) group by a",
			errPart: "TABLESAMPLE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tk.ExecToErr(tt.sql)
			require.ErrorContains(t, err, tt.errPart)
		})
	}
}

func TestCreateTableLikeShouldNotCarryMaterializedViewMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv_src (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a")
	tk.MustExec("create table t_like like t")
	tk.MustExec("create table mv_like like mv_src")
	tk.MustExec("create table mlog_like like `$mlog$t`")

	is := dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	mvSrc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_src"))
	require.NoError(t, err)
	mvLike, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_like"))
	require.NoError(t, err)
	mlogSrc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t"))
	require.NoError(t, err)
	mlogLike, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mlog_like"))
	require.NoError(t, err)

	require.NotNil(t, mvSrc.Meta().MaterializedView)
	require.NotNil(t, mlogSrc.Meta().MaterializedViewLog)
	for _, tbl := range []*model.TableInfo{mvLike.Meta(), mlogLike.Meta()} {
		require.Nil(t, tbl.MaterializedView)
		require.Nil(t, tbl.MaterializedViewLog)
		require.Nil(t, tbl.MaterializedViewBase)
	}
	require.NotNil(t, baseTable.Meta().MaterializedViewBase)
	require.Equal(t, mlogSrc.Meta().ID, baseTable.Meta().MaterializedViewBase.MLogID)
	require.Equal(t, []int64{mvSrc.Meta().ID}, baseTable.Meta().MaterializedViewBase.MViewIDs)
}

func TestCreateMaterializedViewRefreshInfoNextUnixSecondsDerivation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	tk.MustExec("create materialized view mv_start_only (a, s, cnt) refresh fast start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvStartOnlyID := getMViewID("mv_start_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 30 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create materialized view mv_next_only (a, s, cnt) refresh fast next date_add(now(), interval 20 minute) as select a, sum(b), count(1) from t group by a")
	mvNextOnlyID := getMViewID("mv_next_only")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 10 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 1 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNextOnlyID,
	)).Check(testkit.Rows("1 1 1"))

	tk.MustExec("create materialized view mv_no_schedule (a, s, cnt) refresh fast as select a, sum(b), count(1) from t group by a")
	mvNoScheduleID := getMViewID("mv_no_schedule")
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvNoScheduleID)).Check(testkit.Rows("1"))

	tk.MustExec("create materialized view mv_near_now (a, s, cnt) refresh fast start with now() next date_add(now(), interval 40 minute) as select a, sum(b), count(1) from t group by a")
	mvNearNowID := getMViewID("mv_near_now")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS is not null, NEXT_REFRESH_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_REFRESH_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvNearNowID,
	)).Check(testkit.Rows("1 1 1"))
}

func TestCreateMaterializedViewRefreshInfoNextUnixSecondsUsesScheduleTimeZone(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")

	getMViewID := func(name string) int64 {
		is := dom.InfoSchema()
		mvTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
		require.NoError(t, err)
		return mvTable.Meta().ID
	}

	tk.MustExec("create materialized view mv_schedule_next (a, s, cnt) refresh fast next cast('2030-01-02 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")
	mvID := getMViewID("mv_schedule_next")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS = 1893549600, NEXT_REFRESH_UNIX_SECONDS = 1893578400 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvID,
	)).Check(testkit.Rows("1 0"))

	tk.MustExec("create materialized view mv_schedule_start (a, s, cnt) refresh fast start with cast('2030-01-02 10:00:00' as datetime) next cast('2030-01-03 10:00:00' as datetime) as select a, sum(b), count(1) from t group by a")
	mvStartID := getMViewID("mv_schedule_start")
	tk.MustQuery(fmt.Sprintf(
		"select NEXT_REFRESH_UNIX_SECONDS = 1893549600, NEXT_REFRESH_UNIX_SECONDS = 1893636000 from mysql.tidb_mview_refresh_info where MVIEW_ID = %d",
		mvStartID,
	)).Check(testkit.Rows("1 0"))
}

func TestCreateMaterializedViewRejectNonBaseObject(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")
	tk.MustExec("create view v as select a from t")

	err := tk.ExecToErr("create materialized view mv_v (a, c) as select a, count(1) from v group by a")
	require.ErrorContains(t, err, "is not BASE TABLE")
}

func TestDropTableMaterializedViewConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_constraints (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_drop_constraints (a, b)")
	tk.MustExec("create materialized view mv_drop_constraints (a, s, cnt) as select a, sum(b), count(1) from t_drop_constraints group by a")
	err := tk.ExecToErr("drop table mv_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on materialized view table")
	err = tk.ExecToErr("drop table `$mlog$t_drop_constraints`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")

	err = tk.ExecToErr("truncate table mv_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view table")
	err = tk.ExecToErr("truncate table `$mlog$t_drop_constraints`")
	require.ErrorContains(t, err, "TRUNCATE TABLE on materialized view log table")
	err = tk.ExecToErr("truncate table t_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view dependencies")
	tk.MustExec("drop materialized view mv_drop_constraints")
	err = tk.ExecToErr("drop table t_drop_constraints")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("truncate table t_drop_constraints")
	require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")
	tk.MustExec("drop materialized view log on t_drop_constraints")
	tk.MustExec("truncate table t_drop_constraints")
	tk.MustExec("drop table t_drop_constraints")
}

func TestMaterializedViewPartitionDDLConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t_partition_mlog (id bigint not null, v int not null)")
	tk.MustExec("create materialized view log on t_partition_mlog (id, v)")
	err := tk.ExecToErr(`alter table t_partition_mlog
partition by range (id) (
  partition p0 values less than (100),
  partition p1 values less than maxvalue
)`)
	require.ErrorContains(t, err, "ALTER TABLE ... PARTITION BY with materialized view log")

	tk.MustExec(`create table t_partitioned (
  id bigint not null primary key,
  v int not null
)
partition by range (id) (
  partition p0 values less than (100),
  partition p1 values less than maxvalue
)`)
	tk.MustExec("create table t_exchange_base (id bigint not null, v int not null)")
	tk.MustExec("create materialized view log on t_exchange_base (id, v)")
	tk.MustExec("create materialized view mv_exchange (v, cnt) as select v, count(*) from t_exchange_base group by v")
	tk.MustExec("set @@tidb_enable_exchange_partition = 1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition = 0")

	err = tk.ExecToErr("alter table t_partitioned exchange partition p0 with table t_exchange_base")
	require.ErrorContains(t, err, "EXCHANGE PARTITION on non-partitioned table with materialized view dependencies")

	err = tk.ExecToErr("alter table t_partitioned exchange partition p0 with table `$mlog$t_exchange_base`")
	require.ErrorContains(t, err, "EXCHANGE PARTITION on non-partitioned table with materialized view log")

	err = tk.ExecToErr("alter table t_partitioned exchange partition p0 with table mv_exchange")
	require.ErrorContains(t, err, "EXCHANGE PARTITION on non-partitioned table materialized view table")
}

func TestAlterMaterializedViewMetadataDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int not null, b int not null)")
	tk.MustExec("insert into t values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)")
	tk.MustExec("create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) as select a, sum(b), count(1) from t group by a")

	getMView := func() (*model.TableInfo, *model.MaterializedViewInfo) {
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta(), tbl.Meta().MaterializedView
	}

	tk.MustExec("alter materialized view mv comment = 'updated comment'")
	mvTable, _ := getMView()
	require.Equal(t, "updated comment", mvTable.Comment)

	tk.MustExec("alter materialized view mv refresh start with date_add(now(), interval 40 minute) next date_add(now(), interval 20 minute)")
	_, mvInfo := getMView()
	require.Equal(t, "FAST", mvInfo.RefreshMethod)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 40 MINUTE)", mvInfo.RefreshStartWith)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 20 MINUTE)", mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is not null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view mv attributes='mview_alert_warning=5,mview_alert_overdue=10,mview_alert_refresh_failed=yes'")
	_, mvInfo = getMView()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(10), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)

	err := tk.ExecToErr("alter materialized view mv attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")

	tk.MustExec("alter materialized view mv refresh")
	_, mvInfo = getMView()
	require.Empty(t, mvInfo.RefreshStartWith)
	require.Empty(t, mvInfo.RefreshNext)
	tk.MustQuery(fmt.Sprintf("select NEXT_REFRESH_UNIX_SECONDS is null from mysql.tidb_mview_refresh_info where MVIEW_ID = %d", mvTable.ID)).Check(testkit.Rows("1"))
}

func TestAlterMaterializedViewAttributesUpdatesAlertThresholds(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mv_attr (a int not null, b int not null)")
	tk.MustExec("create materialized view log on t_mv_attr (a, b)")
	tk.MustExec("create materialized view mv_attr (a, s, cnt) refresh fast next date_add(now(), interval 2 hour) attributes='mview_alert_warning=300,mview_alert_overdue=600,mview_alert_refresh_failed=no' as select a, sum(b), count(1) from t_mv_attr group by a")

	getMViewInfo := func() *model.MaterializedViewInfo {
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("mv_attr"))
		require.NoError(t, err)
		require.NotNil(t, tbl.Meta().MaterializedView)
		return tbl.Meta().MaterializedView
	}

	mvInfo := getMViewInfo()
	require.Equal(t, int64(300), mvInfo.AlertWarningSec)
	require.Equal(t, int64(600), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=5,mview_alert_overdue=5,mview_alert_refresh_failed=yes'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(5), mvInfo.AlertWarningSec)
	require.Equal(t, int64(5), mvInfo.AlertOverdueSec)
	require.True(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 2 HOUR)", mvInfo.RefreshNext)

	tk.MustExec("alter materialized view mv_attr refresh next date_add(now(), interval 25 minute)")
	tk.MustExec("alter materialized view mv_attr attributes='mview_alert_warning=10,mview_alert_overdue=20,mview_alert_refresh_failed=no'")
	mvInfo = getMViewInfo()
	require.Equal(t, int64(10), mvInfo.AlertWarningSec)
	require.Equal(t, int64(20), mvInfo.AlertOverdueSec)
	require.False(t, mvInfo.AlertRefreshFailed)
	require.Equal(t, "DATE_ADD(NOW(), INTERVAL 25 MINUTE)", mvInfo.RefreshNext)

	err := tk.ExecToErr("alter materialized view mv_attr attributes='unknown=1'")
	require.ErrorContains(t, err, "unsupported ATTRIBUTES key")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_refresh_failed=maybe'")
	require.ErrorContains(t, err, "must be yes or no")
	err = tk.ExecToErr("alter materialized view mv_attr attributes='mview_alert_warning=20,mview_alert_overdue=10'")
	require.ErrorContains(t, err, "must be less than or equal")
}

func TestMaterializedViewRelatedTablesDDLRejected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ddl_mv (a int not null, b int)")
	tk.MustExec("create materialized view log on t_ddl_mv (a, b)")

	err := tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
	err = tk.ExecToErr("rename table t_ddl_mv to t_ddl_mv2")
	require.ErrorContains(t, err, "RENAME TABLE on base table with materialized view log")
	err = tk.ExecToErr("drop table `$mlog$t_ddl_mv`")
	require.ErrorContains(t, err, "DROP TABLE on materialized view log table")

	tk.MustExec("create materialized view mv_ddl_mv (a, cnt) as select a, count(1) from t_ddl_mv group by a")
	tk.MustExec("alter table t_ddl_mv add column c int")
	err = tk.ExecToErr("alter table t_ddl_mv modify column a bigint")
	require.ErrorContains(t, err, "does not support changing charset/collation/nullability of group keys")
	err = tk.ExecToErr("drop table t_ddl_mv")
	require.ErrorContains(t, err, "DROP TABLE on base table with materialized view dependencies")
	err = tk.ExecToErr("alter table mv_ddl_mv add column x int")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view table")
	err = tk.ExecToErr("alter table `$mlog$t_ddl_mv` add index idx_mlog_b(b)")
	require.ErrorContains(t, err, "ALTER TABLE on materialized view log table")
	err = tk.ExecToErr("create index idx_mlog_b_create on `$mlog$t_ddl_mv`(b)")
	require.ErrorContains(t, err, "CREATE INDEX on materialized view log table")
}
