package ddl

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaterializedViewMetadataDDLNoCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustGetErrCode("create materialized view mv1 (a) as select 1", errno.ErrNoDB)
	tk.MustGetErrCode("alter materialized view mv1 comment = 'c1'", errno.ErrNoDB)
	tk.MustGetErrCode("drop materialized view mv1", errno.ErrNoDB)

	tk.MustExec("create database mview_no_db")
	tk.MustExec("create materialized view mview_no_db.mv1 (a) as select 1")
	tk.MustQuery(`select count(*) from mysql.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("1"))

	tk.MustExec("alter materialized view mview_no_db.mv1 comment = 'c1'")
	tk.MustQuery(`select mview_comment from mysql.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("c1"))

	tk.MustExec("drop materialized view mview_no_db.mv1")
	tk.MustQuery(`select count(*) from mysql.tidb_mviews where table_schema='mview_no_db' and mview_name='mv1'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewMetadataDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_meta")
	tk.MustExec("use mview_meta")

	// default refresh clause: REFRESH FAST ON DEMAND START WITH NOW() NEXT 300
	tk.MustExec("create materialized view mv1 (a) as select 1")
	rows := tk.MustQuery(`
		select table_schema, mview_id, mview_owner, mview_definition, mview_comment, mview_tiflash_replicas,
		       refresh_method, refresh_mode, staleness
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv1'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "mview_meta", rows[0][0])
	require.Len(t, rows[0][1].(string), 36) // UUID
	require.IsType(t, "", rows[0][2]) // owner may be empty in test sessions
	require.Contains(t, rows[0][3].(string), "CREATE MATERIALIZED VIEW")
	require.Equal(t, "<nil>", fmt.Sprint(rows[0][4]))
	require.Equal(t, "0", fmt.Sprint(rows[0][5]))
	require.Equal(t, "REFRESH FAST", rows[0][6])
	require.Equal(t, "ON DEMAND START WITH NOW() NEXT 300", rows[0][7])
	require.Equal(t, "FRESH", rows[0][8])

	// comment + tiflash replica are persisted in mysql.tidb_mviews
	tk.MustExec("create materialized view mv2 (a) comment = 'c1' tiflash replica 1 as select 1")
	rows = tk.MustQuery(`
		select mview_comment, mview_tiflash_replicas
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv2'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "c1", rows[0][0])
	require.Equal(t, "1", fmt.Sprint(rows[0][1]))

	// NEVER REFRESH stores refresh_mode as NULL.
	tk.MustExec("create materialized view mv3 (a) never refresh as select 1")
	rows = tk.MustQuery(`
		select refresh_method, refresh_mode
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv3'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "NEVER REFRESH", rows[0][0])
	require.Equal(t, "<nil>", fmt.Sprint(rows[0][1]))

	// explicit refresh schedule (FAST always implies ON DEMAND in parser)
	tk.MustExec("create materialized view mv4 (a) refresh fast start with now() next 600 as select 1")
	rows = tk.MustQuery(`
		select refresh_method, refresh_mode
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv4'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "REFRESH FAST", rows[0][0])
	require.Equal(t, "ON DEMAND START WITH NOW() NEXT 600", rows[0][1])

	// duplicate name in same schema
	tk.MustGetErrCode("create materialized view mv1 (a) as select 1", errno.ErrTableExists)

	// ALTER comment / refresh schedule
	tk.MustExec("alter materialized view mv1 comment = 'c2'")
	rows = tk.MustQuery(`
		select mview_comment
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv1'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "c2", rows[0][0])

	tk.MustExec("alter materialized view mv1 refresh start with now() next 900")
	rows = tk.MustQuery(`
		select refresh_method, refresh_mode
		from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv1'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "REFRESH FAST", rows[0][0])
	require.Equal(t, "START WITH NOW() NEXT 900", rows[0][1])

	// DROP removes metadata row.
	tk.MustExec("drop materialized view mv2")
	tk.MustQuery(`
		select count(*) from mysql.tidb_mviews
		where table_schema='mview_meta' and mview_name='mv2'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewMetadataDDLErrorCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_err")
	tk.MustExec("use mview_err")

	tk.MustGetErrCode("alter materialized view mv_not_exist comment = 'c1'", errno.ErrNoSuchTable)
	tk.MustGetErrCode("drop materialized view mv_not_exist", errno.ErrNoSuchTable)
}

func TestMaterializedViewMetadataDDLDatabaseNotExists(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustGetErrCode("create materialized view not_exist.mv1 (a) as select 1", errno.ErrBadDB)
	tk.MustGetErrCode("alter materialized view not_exist.mv1 comment = 'c1'", errno.ErrBadDB)
	tk.MustGetErrCode("drop materialized view not_exist.mv1", errno.ErrBadDB)
}

func TestMaterializedViewLogMetadataDDLNoCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_no_db")
	tk.MustExec("use mlog_no_db")
	tk.MustExec("create table t (a int)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustGetErrCode("create materialized view log on t (a)", errno.ErrNoDB)
	tk2.MustGetErrCode("alter materialized view log on t purge immediate", errno.ErrNoDB)
	tk2.MustGetErrCode("drop materialized view log on t", errno.ErrNoDB)

	tk2.MustExec("create materialized view log on mlog_no_db.t (a)")
	tk2.MustQuery(`select count(*) from mysql.tidb_mlogs where base_table_schema='mlog_no_db' and base_table_name='t'`).Check(testkit.Rows("1"))

	tk2.MustExec("alter materialized view log on mlog_no_db.t purge immediate")
	tk2.MustExec("drop materialized view log on mlog_no_db.t")
	tk2.MustQuery(`select count(*) from mysql.tidb_mlogs where base_table_schema='mlog_no_db' and base_table_name='t'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewLogMetadataDDL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_meta")
	tk.MustExec("use mlog_meta")
	tk.MustExec("create table t (a int, b int)")

	// default purge policy: IMMEDIATE (interval 0).
	tk.MustExec("create materialized view log on t (a,b)")
	rows := tk.MustQuery(`
		select base_table_id, mlog_name, mlog_columns, purge_method, purge_interval
		from mysql.tidb_mlogs
		where base_table_schema='mlog_meta' and base_table_name='t'`).Rows()
	require.Len(t, rows, 1)
	baseTableID := rows[0][0].(string)
	require.NotEmpty(t, baseTableID)
	require.Equal(t, fmt.Sprintf("__tidb_mlog_%s", baseTableID), rows[0][1])
	require.Equal(t, "a,b", rows[0][2])
	require.Equal(t, "IMMEDIATE", rows[0][3])
	require.Equal(t, "0", fmt.Sprint(rows[0][4]))

	// duplicate mlog on same base table
	tk.MustGetErrMsg(
		"create materialized view log on t (a)",
		"[schema:1050]Table 'materialized view log on mlog_meta.t' already exists",
	)

	// columns must exist on base table
	tk.MustExec("create table t2 (a int)") // ensure table exists for the next statement
	tk.MustGetErrMsg(
		"create materialized view log on t2 (b)",
		"[schema:1054]Unknown column 'b' in 't2'",
	)

	// ALTER purge to deferred with deterministic start/interval.
	tk.MustExec("alter materialized view log on t purge start with '2026-01-01 00:00:00' next 600")
	rows = tk.MustQuery(`
		select purge_method, purge_start, purge_interval
		from mysql.tidb_mlogs
		where base_table_schema='mlog_meta' and base_table_name='t'`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "DEFERRED", rows[0][0])
	require.Equal(t, "2026-01-01 00:00:00", fmt.Sprint(rows[0][1]))
	require.Equal(t, "600", fmt.Sprint(rows[0][2]))

	// DROP removes metadata row.
	tk.MustExec("drop materialized view log on t")
	tk.MustQuery(`
		select count(*) from mysql.tidb_mlogs
		where base_table_schema='mlog_meta' and base_table_name='t'`).Check(testkit.Rows("0"))
}

func TestMaterializedViewLogMetadataDDLErrorCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_err")
	tk.MustExec("use mlog_err")

	// base table missing
	tk.MustGetErrCode("create materialized view log on t_not_exist (a)", errno.ErrNoSuchTable)

	// mlog missing on existing base table
	tk.MustExec("create table t (a int)")
	tk.MustGetErrCode("alter materialized view log on t purge immediate", errno.ErrNoSuchTable)
	tk.MustGetErrCode("drop materialized view log on t", errno.ErrNoSuchTable)

	// column case-insensitive match
	tk.MustExec("create table t2 (A int, b int)")
	tk.MustExec("create materialized view log on t2 (a, b)")
	tk.MustExec("drop materialized view log on t2")
}

func TestRefreshMaterializedViewUnsupported(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mview_refresh_unsupported")
	tk.MustExec("use mview_refresh_unsupported")
	tk.MustExec("create materialized view mv1 (a) as select 1")

	err := tk.ExecToErr("refresh materialized view mv1")
	require.Error(t, err)
	require.ErrorContains(t, err, "REFRESH MATERIALIZED VIEW is not supported")
}

func TestDropTableBlockedByMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard")
	tk.MustExec("use mlog_drop_guard")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")

	tk.MustGetErrMsg(
		"drop table t",
		"can't drop table mlog_drop_guard.t: materialized view log exists, drop it first",
	)

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table t")
}

func TestDropTableBlockedByMaterializedViewLogMultiTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard_multi")
	tk.MustExec("use mlog_drop_guard_multi")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create materialized view log on t1 (a)")

	tk.MustGetErrMsg(
		"drop table t1, t2",
		"can't drop table mlog_drop_guard_multi.t1: materialized view log exists, drop it first",
	)
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='mlog_drop_guard_multi' and table_name='t1'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from information_schema.tables where table_schema='mlog_drop_guard_multi' and table_name='t2'").Check(testkit.Rows("1"))
}

func TestDropTableIfExistsBlockedByMaterializedViewLog(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database mlog_drop_guard_if_exists")
	tk.MustExec("use mlog_drop_guard_if_exists")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create materialized view log on t (a)")

	tk.MustGetErrMsg(
		"drop table if exists not_exist, t",
		"can't drop table mlog_drop_guard_if_exists.t: materialized view log exists, drop it first",
	)

	tk.MustExec("drop materialized view log on t")
	tk.MustExec("drop table if exists not_exist, t")
}
