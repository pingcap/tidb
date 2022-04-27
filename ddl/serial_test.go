// Copyright 2019 PingCAP, Inc.
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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/util"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestTruncateAllPartitions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table partition_table (v int) partition by hash (v) partitions 10")
	tk.MustExec("insert into partition_table values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	tk.MustExec("alter table partition_table truncate partition all")
	tk.MustQuery("select count(*) from partition_table").Check(testkit.Rows("0"))
}

func TestIssue23872(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for _, test := range []struct {
		sql  string
		flag uint
	}{
		{
			"create table t(id smallint,id1 int, primary key (id))",
			mysql.NotNullFlag | mysql.PriKeyFlag | mysql.NoDefaultValueFlag,
		},
		{
			"create table t(a int default 1, primary key(a))",
			mysql.NotNullFlag | mysql.PriKeyFlag,
		},
	} {
		tk.MustExec("drop table if exists t")
		tk.MustExec(test.sql)
		rs, err := tk.Exec("select * from t")
		require.NoError(t, err)
		cols := rs.Fields()
		require.NoError(t, rs.Close())
		require.Equal(t, test.flag, cols[0].Column.GetFlag())
	}
}

func TestChangeMaxIndexLength(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.MaxIndexLength = config.DefMaxOfMaxIndexLength
	})

	tk.MustExec("use test")
	tk.MustExec("create table t (c1 varchar(3073), index(c1)) charset = ascii")
	tk.MustExec(fmt.Sprintf("create table t1 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength))
	err := tk.ExecToErr(fmt.Sprintf("create table t2 (c1 varchar(%d), index(c1)) charset = ascii;", config.DefMaxOfMaxIndexLength+1))
	require.EqualError(t, err, "[ddl:1071]Specified key was too long; max key length is 12288 bytes")
}

func TestCreateTableWithLike(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	// for the same database
	tk.MustExec("create database ctwl_db")
	tk.MustExec("use ctwl_db")
	tk.MustExec("create table tt(id int primary key)")
	tk.MustExec("create table t (c1 int not null auto_increment, c2 int, constraint cc foreign key (c2) references tt(id), primary key(c1)) auto_increment = 10")
	tk.MustExec("insert into t set c2=1")
	tk.MustExec("create table t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustExec("create table t2 (like ctwl_db.t1)")
	tk.MustExec("insert into t2 set c2=12")
	tk.MustQuery("select * from t").Check(testkit.Rows("10 1"))
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 12"))
	is := domain.GetDomain(tk.Session()).InfoSchema()
	tbl1, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1Info := tbl1.Meta()
	require.Nil(t, tbl1Info.ForeignKeys)
	require.True(t, tbl1Info.PKIsHandle)
	col := tbl1Info.Columns[0]
	hasNotNull := mysql.HasNotNullFlag(col.GetFlag())
	require.True(t, hasNotNull)
	tbl2, err := is.TableByName(model.NewCIStr("ctwl_db"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tbl2Info := tbl2.Meta()
	require.Nil(t, tbl2Info.ForeignKeys)
	require.True(t, tbl2Info.PKIsHandle)
	require.True(t, mysql.HasNotNullFlag(tbl2Info.Columns[0].GetFlag()))

	// for different databases
	tk.MustExec("create database ctwl_db1")
	tk.MustExec("use ctwl_db1")
	tk.MustExec("create table t1 like ctwl_db.t")
	tk.MustExec("insert into t1 set c2=11")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 11"))
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl1, err = is.TableByName(model.NewCIStr("ctwl_db1"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.Nil(t, tbl1.Meta().ForeignKeys)

	// for table partition
	tk.MustExec("use ctwl_db")
	tk.MustExec("create table pt1 (id int) partition by range columns (id) (partition p0 values less than (10))")
	tk.MustExec("insert into pt1 values (1),(2),(3),(4)")
	tk.MustExec("create table ctwl_db1.pt1 like ctwl_db.pt1")
	tk.MustQuery("select * from ctwl_db1.pt1").Check(testkit.Rows())

	// Test create table like for partition table.
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_scatter_region=1")
	tk.MustExec("drop table if exists partition_t")
	tk.MustExec("create table partition_t (a int, b int,index(a)) partition by hash (a) partitions 3")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 like partition_t")
	re := tk.MustQuery("show table t1 regions")
	rows := re.Rows()
	require.Len(t, rows, 3)
	tbl := external.GetTableByName(t, tk, "test", "t1")
	partitionDef := tbl.Meta().GetPartitionInfo().Definitions
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[0].ID), rows[0][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[1].ID), rows[1][1])
	require.Regexp(t, fmt.Sprintf("t_%d_.*", partitionDef[2].ID), rows[2][1])

	// Test pre-split table region when create table like.
	tk.MustExec("drop table if exists t_pre")
	tk.MustExec("create table t_pre (a int, b int) shard_row_id_bits = 2 pre_split_regions=2")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 like t_pre")
	re = tk.MustQuery("show table t2 regions")
	rows = re.Rows()
	// Table t2 which create like t_pre should have 4 regions now.
	require.Len(t, rows, 4)
	tbl = external.GetTableByName(t, tk, "test", "t2")
	require.Equal(t, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID), rows[3][1])
	// Test after truncate table the region is also splited.
	tk.MustExec("truncate table t2")
	re = tk.MustQuery("show table t2 regions")
	rows = re.Rows()
	require.Equal(t, 4, len(rows))
	tbl = external.GetTableByName(t, tk, "test", "t2")
	require.Equal(t, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID), rows[3][1])

	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)

	// for failure table cases
	tk.MustExec("use ctwl_db")
	failSQL := "create table t1 like test_not_exist.t"
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = "create table t1 like test.t_not_exist"
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = "create table t1 (like test_not_exist.t)"
	tk.MustGetErrCode(failSQL, mysql.ErrNoSuchTable)
	failSQL = "create table test_not_exis.t1 like ctwl_db.t"
	tk.MustGetErrCode(failSQL, mysql.ErrBadDB)
	failSQL = "create table t1 like ctwl_db.t"
	tk.MustGetErrCode(failSQL, mysql.ErrTableExists)

	// test failure for wrong object cases
	tk.MustExec("drop view if exists v")
	tk.MustExec("create view v as select 1 from dual")
	tk.MustGetErrCode("create table viewTable like v", mysql.ErrWrongObject)
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq")
	tk.MustGetErrCode("create table sequenceTable like seq", mysql.ErrWrongObject)

	tk.MustExec("drop database ctwl_db")
	tk.MustExec("drop database ctwl_db1")
}

func TestCreateTableWithLikeAtTemporaryMode(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// Test create table like at temporary mode.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists temporary_table")
	tk.MustExec("create global temporary table temporary_table (a int, b int,index(a)) on commit delete rows")
	tk.MustExec("drop table if exists temporary_table_t1")
	err := tk.ExecToErr("create table temporary_table_t1 like temporary_table")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like").Error(), err.Error())
	tk.MustExec("drop table if exists temporary_table")

	// Test create temporary table like.
	// Test auto_random.
	tk.MustExec("drop table if exists auto_random_table")
	err = tk.ExecToErr("create table auto_random_table (a bigint primary key auto_random(3), b varchar(255))")
	defer tk.MustExec("drop table if exists auto_random_table")
	tk.MustExec("drop table if exists auto_random_temporary_global")
	err = tk.ExecToErr("create global temporary table auto_random_temporary_global like auto_random_table on commit delete rows")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random").Error(), err.Error())

	// Test pre split regions.
	tk.MustExec("drop table if exists table_pre_split")
	err = tk.ExecToErr("create table table_pre_split(id int) shard_row_id_bits = 2 pre_split_regions=2")
	defer tk.MustExec("drop table if exists table_pre_split")
	tk.MustExec("drop table if exists temporary_table_pre_split")
	err = tk.ExecToErr("create global temporary table temporary_table_pre_split like table_pre_split ON COMMIT DELETE ROWS")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions").Error(), err.Error())

	// Test shard_row_id_bits.
	tk.MustExec("drop table if exists shard_row_id_table, shard_row_id_temporary_table, shard_row_id_table_plus, shard_row_id_temporary_table_plus")
	err = tk.ExecToErr("create table shard_row_id_table (a int) shard_row_id_bits = 5")
	err = tk.ExecToErr("create global temporary table shard_row_id_temporary_table like shard_row_id_table on commit delete rows")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error(), err.Error())
	tk.MustExec("create table shard_row_id_table_plus (a int)")
	tk.MustExec("create global temporary table shard_row_id_temporary_table_plus (a int) on commit delete rows")
	defer tk.MustExec("drop table if exists shard_row_id_table, shard_row_id_temporary_table, shard_row_id_table_plus, shard_row_id_temporary_table_plus")
	err = tk.ExecToErr("alter table shard_row_id_temporary_table_plus shard_row_id_bits = 4")
	require.Equal(t, dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error(), err.Error())

	// Test partition.
	tk.MustExec("drop table if exists global_partition_table")
	tk.MustExec("create table global_partition_table (a int, b int) partition by hash(a) partitions 3")
	defer tk.MustExec("drop table if exists global_partition_table")
	tk.MustGetErrCode("create global temporary table global_partition_temp_table like global_partition_table ON COMMIT DELETE ROWS;", errno.ErrPartitionNoTemporary)
	// Test virtual columns.
	tk.MustExec("drop table if exists test_gv_ddl, test_gv_ddl_temp")
	tk.MustExec(`create table test_gv_ddl(a int, b int as (a+8) virtual, c int as (b + 2) stored)`)
	tk.MustExec(`create global temporary table test_gv_ddl_temp like test_gv_ddl on commit delete rows;`)
	defer tk.MustExec("drop table if exists test_gv_ddl_temp, test_gv_ddl")
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_gv_ddl"))
	require.NoError(t, err)
	testCases := []struct {
		generatedExprString string
		generatedStored     bool
	}{
		{"", false},
		{"`a` + 8", false},
		{"`b` + 2", true},
	}
	for i, column := range table.Meta().Columns {
		require.Equal(t, testCases[i].generatedExprString, column.GeneratedExprString)
		require.Equal(t, testCases[i].generatedStored, column.GeneratedStored)
	}
	result := tk.MustQuery(`DESC test_gv_ddl_temp`)
	result.Check(testkit.Rows(`a int(11) YES  <nil> `, `b int(11) YES  <nil> VIRTUAL GENERATED`, `c int(11) YES  <nil> STORED GENERATED`))
	tk.MustExec("begin")
	tk.MustExec("insert into test_gv_ddl_temp values (1, default, default)")
	tk.MustQuery("select * from test_gv_ddl_temp").Check(testkit.Rows("1 9 11"))
	err = tk.ExecToErr("commit")
	require.NoError(t, err)

	// Test foreign key.
	tk.MustExec("drop table if exists test_foreign_key, t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table test_foreign_key (c int,d int,foreign key (d) references t1 (b))")
	defer tk.MustExec("drop table if exists test_foreign_key, t1")
	tk.MustExec("create global temporary table test_foreign_key_temp like test_foreign_key on commit delete rows")
	is = tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("test_foreign_key_temp"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	require.Equal(t, 0, len(tableInfo.ForeignKeys))

	// Issue 25613.
	// Test from->normal, to->normal.
	tk.MustExec("drop table if exists tb1, tb2")
	tk.MustExec("create table tb1(id int)")
	tk.MustExec("create table tb2 like tb1")
	defer tk.MustExec("drop table if exists tb1, tb2")
	tk.MustQuery("show create table tb2").Check(testkit.Rows("tb2 CREATE TABLE `tb2` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test from->normal, to->global temporary.
	tk.MustExec("drop table if exists tb3, tb4")
	tk.MustExec("create table tb3(id int)")
	tk.MustExec("create global temporary table tb4 like tb3 on commit delete rows")
	defer tk.MustExec("drop table if exists tb3, tb4")
	tk.MustQuery("show create table tb4").Check(testkit.Rows("tb4 CREATE GLOBAL TEMPORARY TABLE `tb4` (\n" +
		"  `id` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ON COMMIT DELETE ROWS"))

	// Test from->global temporary, to->normal.
	tk.MustExec("drop table if exists tb5, tb6")
	tk.MustExec("create global temporary table tb5(id int) on commit delete rows")
	err = tk.ExecToErr("create table tb6 like tb5")
	require.EqualError(t, err, core.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like").Error())
	defer tk.MustExec("drop table if exists tb5, tb6")

	// Test from->global temporary, to->global temporary.
	tk.MustExec("drop table if exists tb7, tb8")
	tk.MustExec("create global temporary table tb7(id int) on commit delete rows")
	err = tk.ExecToErr("create global temporary table tb8 like tb7 on commit delete rows")
	require.EqualError(t, err, core.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like").Error())
	defer tk.MustExec("drop table if exists tb7, tb8")

	// Test from->normal, to->local temporary
	tk.MustExec("drop table if exists tb11, tb12")
	tk.MustExec("create table tb11 (i int primary key, j int)")
	tk.MustExec("create temporary table tb12 like tb11")
	tk.MustQuery("show create table tb12").Check(testkit.Rows("tb12 CREATE TEMPORARY TABLE `tb12` (\n" +
		"  `i` int(11) NOT NULL,\n  `j` int(11) DEFAULT NULL,\n  PRIMARY KEY (`i`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("create temporary table if not exists tb12 like tb11")
	err = infoschema.ErrTableExists.GenWithStackByArgs("test.tb12")
	require.EqualError(t, err, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err.Error())
	defer tk.MustExec("drop table if exists tb11, tb12")
	// Test from->local temporary, to->local temporary
	tk.MustExec("drop table if exists tb13, tb14")
	tk.MustExec("create temporary table tb13 (i int primary key, j int)")
	err = tk.ExecToErr("create temporary table tb14 like tb13")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like").Error(), err.Error())
	defer tk.MustExec("drop table if exists tb13, tb14")
	// Test from->local temporary, to->normal
	tk.MustExec("drop table if exists tb15, tb16")
	tk.MustExec("create temporary table tb15 (i int primary key, j int)")
	err = tk.ExecToErr("create table tb16 like tb15")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("create table like").Error(), err.Error())
	defer tk.MustExec("drop table if exists tb15, tb16")

	tk.MustExec("drop table if exists table_pre_split, tmp_pre_split")
	tk.MustExec("create table table_pre_split(id int) shard_row_id_bits=2 pre_split_regions=2")
	err = tk.ExecToErr("create temporary table tmp_pre_split like table_pre_split")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("pre split regions").Error(), err.Error())
	defer tk.MustExec("drop table if exists table_pre_split, tmp_pre_split")

	tk.MustExec("drop table if exists table_shard_row_id, tmp_shard_row_id")
	tk.MustExec("create table table_shard_row_id(id int) shard_row_id_bits=2")
	err = tk.ExecToErr("create temporary table tmp_shard_row_id like table_shard_row_id")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("shard_row_id_bits").Error(), err.Error())
	defer tk.MustExec("drop table if exists table_shard_row_id, tmp_shard_row_id")

	tk.MustExec("drop table if exists partition_table, tmp_partition_table")
	tk.MustExec("create table partition_table (a int, b int) partition by hash(a) partitions 3")
	tk.MustGetErrCode("create temporary table tmp_partition_table like partition_table", errno.ErrPartitionNoTemporary)
	defer tk.MustExec("drop table if exists partition_table, tmp_partition_table")

	tk.MustExec("drop table if exists foreign_key_table1, foreign_key_table2, foreign_key_tmp")
	tk.MustExec("create table foreign_key_table1 (a int, b int)")
	tk.MustExec("create table foreign_key_table2 (c int,d int,foreign key (d) references foreign_key_table1 (b))")
	tk.MustExec("create temporary table foreign_key_tmp like foreign_key_table2")
	is = tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("foreign_key_tmp"))
	require.NoError(t, err)
	tableInfo = table.Meta()
	require.Equal(t, 0, len(tableInfo.ForeignKeys))
	defer tk.MustExec("drop table if exists foreign_key_table1, foreign_key_table2, foreign_key_tmp")

	// Test for placement
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("create placement policy p1 primary_region='r1' regions='r1,r2'")
	defer tk.MustExec("drop placement policy p1")
	tk.MustExec("drop table if exists placement_table1")
	tk.MustExec("create table placement_table1(id int) placement policy p1")
	defer tk.MustExec("drop table if exists placement_table1")

	err = tk.ExecToErr("create global temporary table g_tmp_placement1 like placement_table1 on commit delete rows")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("placement").Error(), err.Error())
	err = tk.ExecToErr("create temporary table l_tmp_placement1 like placement_table1")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("placement").Error(), err.Error())
}

func createMockStoreAndDomain(t *testing.T) (store kv.Storage, dom *domain.Domain, clean func()) {
	session.SetSchemaLease(200 * time.Millisecond)
	session.DisableStats4Test()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)

	var err error
	store, err = mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	clean = func() {
		dom.Close()
		require.NoError(t, store.Close())
	}
	return
}

// TestCancelAddIndex1 tests canceling ddl job when the add index worker is not started.
func TestCancelAddIndexPanic(t *testing.T) {
	store, dom, clean := createMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/errorMockPanic", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/errorMockPanic"))
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int)")

	tkCancel := testkit.NewTestKit(t, store)
	defer tk.MustExec("drop table t")
	for i := 0; i < 5; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	var checkErr error
	oldReorgWaitTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 50 * time.Millisecond
	defer func() { ddl.ReorgWaitTimeout = oldReorgWaitTimeout }()
	hook := &ddl.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.State == model.JobStateRunning && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer != 0 {
			tkCancel.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
		}
	}
	dom.DDL().SetHook(hook)
	rs, err := tk.Exec("alter table t add index idx_c2(c2)")
	if rs != nil {
		require.NoError(t, rs.Close())
	}
	require.NoError(t, checkErr)
	require.Error(t, err)
	errMsg := err.Error()
	require.True(t, strings.HasPrefix(errMsg, "[ddl:8214]Cancelled DDL job"))
}

func TestRecoverTableByJobID(t *testing.T) {
	store, _, clean := createMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int)")
	defer func(originGC bool) {
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	util.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	getDDLJobID := func(table, tp string) int64 {
		rs, err := tk.Exec("admin show ddl jobs")
		require.NoError(t, err)
		rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
		require.NoError(t, err)
		for _, row := range rows {
			if row.GetString(1) == table && row.GetString(3) == tp {
				return row.GetInt64(0)
			}
		}
		require.FailNowf(t, "can't find %s table of %s", tp, table)
		return -1
	}
	jobID := getDDLJobID("test_recover", "drop table")

	// if GC safe point is not exists in mysql.tidb
	err := tk.ExecToErr(fmt.Sprintf("recover table by job %d", jobID))
	require.EqualError(t, err, "can not get 'tikv_gc_safe_point'")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// if GC enable is not exists in mysql.tidb
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
	tk.MustExec("DROP TABLE t_recover")

	err = gcutil.EnableGC(tk.Session())
	require.NoError(t, err)

	// recover job is before GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeAfterDrop))
	err = tk.ExecToErr(fmt.Sprintf("recover table by job %d", jobID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot is older than GC safe point")

	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// if there is a new table with the same name, should return failed.
	tk.MustExec("create table t_recover (a int)")
	err = tk.ExecToErr(fmt.Sprintf("recover table by job %d", jobID))
	require.EqualError(t, err, infoschema.ErrTableExists.GenWithStackByArgs("t_recover").Error())

	// drop the new table with the same name, then recover table.
	tk.MustExec("drop table t_recover")

	// do recover table.
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))

	// recover table by none exits job.
	err = tk.ExecToErr(fmt.Sprintf("recover table by job %d", 10000000))
	require.Error(t, err)

	// Disable GC by manual first, then after recover table, the GC enable status should also be disabled.
	err = gcutil.DisableGC(tk.Session())
	require.NoError(t, err)

	tk.MustExec("delete from t_recover where a > 1")
	tk.MustExec("drop table t_recover")
	jobID = getDDLJobID("test_recover", "drop table")

	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (7),(8),(9)")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "7", "8", "9"))

	// Test for recover truncate table.
	tk.MustExec("truncate table t_recover")
	tk.MustExec("rename table t_recover to t_recover_new")
	jobID = getDDLJobID("test_recover", "truncate table")
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
	tk.MustExec("insert into t_recover values (10)")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "7", "8", "9", "10"))

	gcEnable, err := gcutil.CheckGCEnable(tk.Session())
	require.NoError(t, err)
	require.Equal(t, false, gcEnable)
}

func TestRecoverTableByJobIDFail(t *testing.T) {
	store, dom, clean := createMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int)")
	defer func(originGC bool) {
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise, emulator GC will delete table record as soon as possible after execute drop table util.
	util.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	rs, err := tk.Exec("admin show ddl jobs")
	require.NoError(t, err)
	rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	row := rows[0]
	require.Equal(t, "test_recover", row.GetString(1))
	require.Equal(t, "drop table", row.GetString(3))
	jobID := row.GetInt64(0)

	// enableGC first
	err = gcutil.EnableGC(tk.Session())
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// set hook
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRecoverTable {
			require.NoError(t, failpoint.Enable("tikvclient/mockCommitError", `return(true)`))
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr", `return(true)`))
		}
	}
	dom.DDL().SetHook(hook)

	// do recover table.
	tk.MustExec(fmt.Sprintf("recover table by job %d", jobID))
	require.NoError(t, failpoint.Disable("tikvclient/mockCommitError"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr"))

	// make sure enable GC after recover table.
	enable, err := gcutil.CheckGCEnable(tk.Session())
	require.NoError(t, err)
	require.Equal(t, true, enable)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func TestRecoverTableByTableNameFail(t *testing.T) {
	store, dom, clean := createMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_recover")
	tk.MustExec("use test_recover")
	tk.MustExec("drop table if exists t_recover")
	tk.MustExec("create table t_recover (a int)")
	defer func(originGC bool) {
		if originGC {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	}(util.IsEmulatorGCEnable())

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	util.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	tk.MustExec("insert into t_recover values (1),(2),(3)")
	tk.MustExec("drop table t_recover")

	// enableGC first
	err := gcutil.EnableGC(tk.Session())
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// set hook
	hook := &ddl.TestDDLCallback{}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if job.Type == model.ActionRecoverTable {
			require.NoError(t, failpoint.Enable("tikvclient/mockCommitError", `return(true)`))
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr", `return(true)`))
		}
	}
	dom.DDL().SetHook(hook)

	// do recover table.
	tk.MustExec("recover table t_recover")
	require.NoError(t, failpoint.Disable("tikvclient/mockCommitError"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockRecoverTableCommitErr"))

	// make sure enable GC after recover table.
	enable, err := gcutil.CheckGCEnable(tk.Session())
	require.NoError(t, err)
	require.True(t, enable)

	// check recover table meta and data record.
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3"))
	// check recover table autoID.
	tk.MustExec("insert into t_recover values (4),(5),(6)")
	tk.MustQuery("select * from t_recover").Check(testkit.Rows("1", "2", "3", "4", "5", "6"))
}

func TestCancelJobByErrorCountLimit(t *testing.T) {
	store, _, clean := createMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockExceedErrorLimit"))
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 16")
	err := ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))

	err = tk.ExecToErr("create table t (a int)")
	require.EqualError(t, err, "[ddl:-1]DDL job rollback, error msg: mock do job error")
}

func TestTruncateTableUpdateSchemaVersionErr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockTruncateTableUpdateVersionError", `return(true)`))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	limit := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 5")
	err := ddlutil.LoadDDLVars(tk.Session())
	require.NoError(t, err)
	defer tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %d", limit))

	tk.MustExec("create table t (a int)")
	err = tk.ExecToErr("truncate table t")
	require.EqualError(t, err, "[ddl:-1]DDL job rollback, error msg: mock update version error")
	// Disable fail point.
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockTruncateTableUpdateVersionError"))
	tk.MustExec("truncate table t")
}

func TestCanceledJobTakeTime(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cjtt(a int)")

	hook := &ddl.TestDDLCallback{}
	once := sync.Once{}
	hook.OnJobUpdatedExported = func(job *model.Job) {
		once.Do(func() {
			err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
				m := meta.NewMeta(txn)
				err := m.GetAutoIDAccessors(job.SchemaID, job.TableID).Del()
				if err != nil {
					return err
				}
				return m.DropTableOrView(job.SchemaID, job.TableID)
			})
			require.NoError(t, err)
		})
	}
	dom.DDL().SetHook(hook)

	originalWT := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Second)
	defer func() { ddl.SetWaitTimeWhenErrorOccurred(originalWT) }()
	startTime := time.Now()
	tk.MustGetErrCode("alter table t_cjtt add column b int", mysql.ErrNoSuchTable)
	sub := time.Since(startTime)
	require.Less(t, sub, ddl.GetWaitTimeWhenErrorOccurred())
}

func TestTableLocksEnable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int)")

	// Test for enable table lock config.
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableTableLock = false
	})

	tk.MustExec("lock tables t1 write")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1235 LOCK TABLES is not supported. To enable this experimental feature, set 'enable-table-lock' in the configuration file."))
	tbl := external.GetTableByName(t, tk, "test", "t1")
	dom := domain.GetDomain(tk.Session())
	require.NoError(t, dom.Reload())
	require.Nil(t, tbl.Meta().Lock)
	tk.MustExec("unlock tables")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1235 UNLOCK TABLES is not supported. To enable this experimental feature, set 'enable-table-lock' in the configuration file."))
}

func TestAutoRandomOnTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists auto_random_temporary")
	err := tk.ExecToErr("create global temporary table auto_random_temporary (a bigint primary key auto_random(3), b varchar(255)) on commit delete rows")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random").Error(), err.Error())
	err = tk.ExecToErr("create temporary table t(a bigint key auto_random)")
	require.Equal(t, core.ErrOptOnTemporaryTable.GenWithStackByArgs("auto_random").Error(), err.Error())
}

func TestAutoRandom(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists auto_random_db")
	tk.MustExec("use auto_random_db")
	databaseName, tableName := "auto_random_db", "t"
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	assertInvalidAutoRandomErr := func(sql string, errMsg string, args ...interface{}) {
		err := tk.ExecToErr(sql)
		require.EqualError(t, err, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(errMsg, args...)).Error())
	}

	assertPKIsNotHandle := func(sql, errCol string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomPKisNotHandleErrMsg, errCol)
	}
	assertAlterValue := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomAlterErrMsg)
	}
	assertOnlyChangeFromAutoIncPK := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomAlterChangeFromAutoInc)
	}
	assertDecreaseBitErr := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomDecreaseBitErrMsg)
	}
	assertWithAutoInc := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
	}
	assertOverflow := func(sql, colName string, maxAutoRandBits, actualAutoRandBits uint64) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomOverflowErrMsg, maxAutoRandBits, actualAutoRandBits, colName)
	}
	assertMaxOverflow := func(sql, colName string, autoRandBits uint64) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomOverflowErrMsg, autoid.MaxAutoRandomBits, autoRandBits, colName)
	}
	assertModifyColType := func(sql string) {
		tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	}
	assertDefault := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
	}
	assertNonPositive := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomNonPositive)
	}
	assertBigIntOnly := func(sql, colType string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomOnNonBigIntColumn, colType)
	}
	assertAddColumn := func(sql, colName string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomAlterAddColumn, colName, databaseName, tableName)
	}
	mustExecAndDrop := func(sql string, fns ...func()) {
		tk.MustExec(sql)
		for _, f := range fns {
			f()
		}
		tk.MustExec("drop table t")
	}

	// Only bigint column can set auto_random
	assertBigIntOnly("create table t (a char primary key auto_random(3), b int)", "char")
	assertBigIntOnly("create table t (a varchar(255) primary key auto_random(3), b int)", "varchar")
	assertBigIntOnly("create table t (a timestamp primary key auto_random(3), b int)", "timestamp")

	// PKIsHandle, but auto_random is defined on non-primary key.
	assertPKIsNotHandle("create table t (a bigint auto_random (3) primary key, b bigint auto_random (3))", "b")
	assertPKIsNotHandle("create table t (a bigint auto_random (3), b bigint auto_random(3), primary key(a))", "b")
	assertPKIsNotHandle("create table t (a bigint auto_random (3), b bigint auto_random(3) primary key)", "a")

	// PKIsNotHandle: no primary key.
	assertPKIsNotHandle("create table t (a bigint auto_random(3), b int)", "a")
	// PKIsNotHandle: primary key is not a single column.
	assertPKIsNotHandle("create table t (a bigint auto_random(3), b bigint, primary key (a, b))", "a")
	assertPKIsNotHandle("create table t (a bigint auto_random(3), b int, c char, primary key (a, c))", "a")

	// PKIsNotHandle: nonclustered integer primary key.
	assertPKIsNotHandle("create table t (a bigint auto_random(3) primary key nonclustered, b int)", "a")
	assertPKIsNotHandle("create table t (a bigint auto_random(3) primary key nonclustered, b int)", "a")
	assertPKIsNotHandle("create table t (a int, b bigint auto_random(3) primary key nonclustered)", "b")

	// Can not set auto_random along with auto_increment.
	assertWithAutoInc("create table t (a bigint auto_random(3) primary key auto_increment)")
	assertWithAutoInc("create table t (a bigint primary key auto_increment auto_random(3))")
	assertWithAutoInc("create table t (a bigint auto_increment primary key auto_random(3))")
	assertWithAutoInc("create table t (a bigint auto_random(3) auto_increment, primary key (a))")

	// Can not set auto_random along with default.
	assertDefault("create table t (a bigint auto_random primary key default 3)")
	assertDefault("create table t (a bigint auto_random(2) primary key default 5)")
	mustExecAndDrop("create table t (a bigint auto_random primary key)", func() {
		assertDefault("alter table t modify column a bigint auto_random default 3")
		assertDefault("alter table t alter column a set default 3")
	})

	// Overflow data type max length.
	assertMaxOverflow("create table t (a bigint auto_random(64) primary key)", "a", 64)
	assertMaxOverflow("create table t (a bigint auto_random(16) primary key)", "a", 16)
	mustExecAndDrop("create table t (a bigint auto_random(5) primary key)", func() {
		assertMaxOverflow("alter table t modify a bigint auto_random(64)", "a", 64)
		assertMaxOverflow("alter table t modify a bigint auto_random(16)", "a", 16)
	})

	assertNonPositive("create table t (a bigint auto_random(0) primary key)")
	tk.MustGetErrMsg("create table t (a bigint auto_random(-1) primary key)",
		`[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 38 near "-1) primary key)" `)

	// Basic usage.
	mustExecAndDrop("create table t (a bigint auto_random(1) primary key)")
	mustExecAndDrop("create table t (a bigint auto_random(4) primary key)")
	mustExecAndDrop("create table t (a bigint auto_random(15) primary key)")
	mustExecAndDrop("create table t (a bigint primary key auto_random(4))")
	mustExecAndDrop("create table t (a bigint auto_random(4), primary key (a))")

	// Increase auto_random bits.
	mustExecAndDrop("create table t (a bigint auto_random(5) primary key)", func() {
		tk.MustExec("alter table t modify a bigint auto_random(8)")
		tk.MustExec("alter table t modify a bigint auto_random(10)")
		tk.MustExec("alter table t modify a bigint auto_random(12)")
	})

	// Auto_random can occur multiple times like other column attributes.
	mustExecAndDrop("create table t (a bigint auto_random(3) auto_random(2) primary key)")
	mustExecAndDrop("create table t (a bigint, b bigint auto_random(3) primary key auto_random(2))")
	mustExecAndDrop("create table t (a bigint auto_random(1) auto_random(2) auto_random(3), primary key (a))")

	// Add/drop the auto_random attribute is not allowed.
	mustExecAndDrop("create table t (a bigint auto_random(3) primary key)", func() {
		assertAlterValue("alter table t modify column a bigint")
		assertAlterValue("alter table t modify column a bigint auto_random(0)")
		assertAlterValue("alter table t change column a b bigint")
	})
	mustExecAndDrop("create table t (a bigint, b char, c bigint auto_random(3), primary key(c))", func() {
		assertAlterValue("alter table t modify column c bigint")
		assertAlterValue("alter table t change column c d bigint")
	})
	mustExecAndDrop("create table t (a bigint primary key)", func() {
		assertOnlyChangeFromAutoIncPK("alter table t modify column a bigint auto_random(3)")
	})
	mustExecAndDrop("create table t (a bigint, b bigint, primary key(a, b))", func() {
		assertOnlyChangeFromAutoIncPK("alter table t modify column a bigint auto_random(3)")
		assertOnlyChangeFromAutoIncPK("alter table t modify column b bigint auto_random(3)")
	})

	// Add auto_random column is not allowed.
	mustExecAndDrop("create table t (a bigint)", func() {
		assertAddColumn("alter table t add column b int auto_random", "b")
		assertAddColumn("alter table t add column b bigint auto_random", "b")
		assertAddColumn("alter table t add column b bigint auto_random primary key", "b")
	})
	mustExecAndDrop("create table t (a bigint, b bigint primary key)", func() {
		assertAddColumn("alter table t add column c int auto_random", "c")
		assertAddColumn("alter table t add column c bigint auto_random", "c")
		assertAddColumn("alter table t add column c bigint auto_random primary key", "c")
	})

	// Decrease auto_random bits is not allowed.
	mustExecAndDrop("create table t (a bigint auto_random(10) primary key)", func() {
		assertDecreaseBitErr("alter table t modify column a bigint auto_random(6)")
	})
	mustExecAndDrop("create table t (a bigint auto_random(10) primary key)", func() {
		assertDecreaseBitErr("alter table t modify column a bigint auto_random(1)")
	})

	originStep := autoid.GetStep()
	autoid.SetStep(1)
	// Increase auto_random bits but it will overlap with incremental bits.
	mustExecAndDrop("create table t (a bigint unsigned auto_random(5) primary key)", func() {
		const alterTryCnt, rebaseOffset = 3, 1
		insertSQL := fmt.Sprintf("insert into t values (%d)", ((1<<(64-10))-1)-rebaseOffset-alterTryCnt)
		tk.MustExec(insertSQL)
		// Try to rebase to 0..0011..1111 (54 `1`s).
		tk.MustExec("alter table t modify a bigint unsigned auto_random(6)")
		tk.MustExec("alter table t modify a bigint unsigned auto_random(10)")
		assertOverflow("alter table t modify a bigint unsigned auto_random(11)", "a", 10, 11)
	})
	autoid.SetStep(originStep)

	// Modifying the field type of a auto_random column is not allowed.
	// Here the throw error is `ERROR 8200 (HY000): Unsupported modify column: length 11 is less than origin 20`,
	// instead of `ERROR 8216 (HY000): Invalid auto random: modifying the auto_random column type is not supported`
	// Because the origin column is `bigint`, it can not change to any other column type in TiDB limitation.
	mustExecAndDrop("create table t (a bigint primary key auto_random(3), b int)", func() {
		assertModifyColType("alter table t modify column a int auto_random(3)")
		assertModifyColType("alter table t modify column a mediumint auto_random(3)")
		assertModifyColType("alter table t modify column a smallint auto_random(3)")
		tk.MustExec("alter table t modify column b int")
		tk.MustExec("alter table t modify column b bigint")
		tk.MustExec("alter table t modify column a bigint auto_random(3)")
	})

	// Test show warnings when create auto_random table.
	assertShowWarningCorrect := func(sql string, times int) {
		mustExecAndDrop(sql, func() {
			note := fmt.Sprintf(autoid.AutoRandomAvailableAllocTimesNote, times)
			result := fmt.Sprintf("Note|1105|%s", note)
			tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", result))
			require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
		})
	}
	assertShowWarningCorrect("create table t (a bigint auto_random(15) primary key)", 281474976710655)
	assertShowWarningCorrect("create table t (a bigint unsigned auto_random(15) primary key)", 562949953421311)
	assertShowWarningCorrect("create table t (a bigint auto_random(1) primary key)", 4611686018427387903)

	// Test insert into auto_random column explicitly is not allowed by default.
	assertExplicitInsertDisallowed := func(sql string) {
		assertInvalidAutoRandomErr(sql, autoid.AutoRandomExplicitInsertDisabledErrMsg)
	}
	tk.MustExec("set @@allow_auto_random_explicit_insert = false")
	mustExecAndDrop("create table t (a bigint auto_random primary key)", func() {
		assertExplicitInsertDisallowed("insert into t values (1)")
		assertExplicitInsertDisallowed("insert into t values (3)")
		tk.MustExec("insert into t values()")
	})
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	mustExecAndDrop("create table t (a bigint auto_random primary key)", func() {
		tk.MustExec("insert into t values(1)")
		tk.MustExec("insert into t values(3)")
		tk.MustExec("insert into t values()")
	})
}

func TestAutoRandomWithPreSplitRegion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists auto_random_db")
	tk.MustExec("use auto_random_db")

	origin := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, origin)
	tk.MustExec("set @@global.tidb_scatter_region=1")

	// Test pre-split table region for auto_random table.
	tk.MustExec("create table t (a bigint auto_random(2) primary key clustered, b int) pre_split_regions=2")
	re := tk.MustQuery("show table t regions")
	rows := re.Rows()
	require.Len(t, rows, 4)
	tbl := external.GetTableByName(t, tk, "auto_random_db", "t") //nolint:typecheck
	require.Equal(t, fmt.Sprintf("t_%d_r_2305843009213693952", tbl.Meta().ID), rows[1][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_4611686018427387904", tbl.Meta().ID), rows[2][1])
	require.Equal(t, fmt.Sprintf("t_%d_r_6917529027641081856", tbl.Meta().ID), rows[3][1])
}

func TestModifyingColumn4NewCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database dct")
	tk.MustExec("use dct")
	tk.MustExec("create table t(b varchar(10) collate utf8_bin, c varchar(10) collate utf8_general_ci) collate utf8_bin")
	// Column collation can be changed as long as there is no index defined.
	tk.MustExec("alter table t modify b varchar(10) collate utf8_general_ci")
	tk.MustExec("alter table t modify c varchar(10) collate utf8_bin")
	tk.MustExec("alter table t modify c varchar(10) collate utf8_unicode_ci")
	tk.MustExec("alter table t charset utf8 collate utf8_general_ci")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_bin")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_unicode_ci")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_general_ci")
	tk.MustExec("alter table t modify b varchar(10) collate utf8_unicode_ci")
	tk.MustExec("alter table t modify b varchar(10) collate utf8_bin")

	tk.MustExec("alter table t add index b_idx(b)")
	tk.MustExec("alter table t add index c_idx(c)")
	tk.MustGetErrMsg("alter table t modify b varchar(10) collate utf8_general_ci", "[ddl:8200]Unsupported modifying collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	tk.MustGetErrMsg("alter table t modify c varchar(10) collate utf8_bin", "[ddl:8200]Unsupported modifying collation of column 'c' from 'utf8_general_ci' to 'utf8_bin' when index is defined on it.")
	tk.MustGetErrMsg("alter table t modify c varchar(10) collate utf8_unicode_ci", "[ddl:8200]Unsupported modifying collation of column 'c' from 'utf8_general_ci' to 'utf8_unicode_ci' when index is defined on it.")
	tk.MustGetErrMsg("alter table t convert to charset utf8 collate utf8_general_ci", "[ddl:8200]Unsupported converting collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.")
	// Change to a compatible collation is allowed.
	tk.MustExec("alter table t modify c varchar(10) collate utf8mb4_general_ci")
	// Change the default collation of table is allowed.
	tk.MustExec("alter table t collate utf8mb4_general_ci")
	tk.MustExec("alter table t charset utf8mb4 collate utf8mb4_bin")
	tk.MustExec("alter table t charset utf8mb4 collate utf8mb4_unicode_ci")
	tk.MustExec("alter table t charset utf8mb4 collate utf8mb4_zh_pinyin_tidb_as_cs")
	// Change the default collation of database is allowed.
	tk.MustExec("alter database dct charset utf8mb4 collate utf8mb4_general_ci")
}

func TestForbidUnsupportedCollations(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mustGetUnsupportedCollation := func(sql string, coll string) {
		tk.MustGetErrMsg(sql, fmt.Sprintf("[ddl:1273]Unsupported collation when new collation is enabled: '%s'", coll))
	}

	// Test default collation of database.
	mustGetUnsupportedCollation("create database ucd charset utf8mb4 collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("create database ucd charset utf8 collate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create database ucd")
	mustGetUnsupportedCollation("alter database ucd charset utf8mb4 collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("alter database ucd collate utf8mb4_roman_ci", "utf8mb4_roman_ci")

	// Test default collation of table.
	tk.MustExec("use ucd")
	mustGetUnsupportedCollation("create table t(a varchar(20)) charset utf8mb4 collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("create table t(a varchar(20)) collate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create table t(a varchar(20)) collate utf8mb4_general_ci")
	mustGetUnsupportedCollation("alter table t default collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("alter table t convert to charset utf8mb4 collate utf8mb4_roman_ci", "utf8mb4_roman_ci")

	// Test collation of columns.
	mustGetUnsupportedCollation("create table t1(a varchar(20)) collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("create table t1(a varchar(20)) charset utf8 collate utf8_roman_ci", "utf8_roman_ci")
	tk.MustExec("create table t1(a varchar(20))")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) collate utf8mb4_roman_ci", "utf8mb4_roman_ci")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) charset utf8 collate utf8_roman_ci", "utf8_roman_ci")
	mustGetUnsupportedCollation("alter table t1 modify a varchar(20) charset utf8 collate utf8_roman_ci", "utf8_roman_ci")

	// TODO(bb7133): fix the following cases by setting charset from collate firstly.
	// mustGetUnsupportedCollation("create database ucd collate utf8mb4_unicode_ci", errMsgUnsupportedUnicodeCI)
	// mustGetUnsupportedCollation("alter table t convert to collate utf8mb4_unicode_ci", "utf8mb4_unicode_ci")
}

func TestCreateTableNoBlock(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/checkOwnerCheckAllVersionsWaitTime", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/checkOwnerCheckAllVersionsWaitTime"))
	}()
	save := variable.GetDDLErrorCountLimit()
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", save))
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	require.Error(t, tk.ExecToErr("create table t(a int)"))
}

func TestCheckEnumLength(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustGetErrCode("create table t1 (a enum('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))", errno.ErrTooLongValueForType)
	tk.MustGetErrCode("create table t1 (a set('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))", errno.ErrTooLongValueForType)
	tk.MustExec("create table t2 (id int primary key)")
	tk.MustGetErrCode("alter table t2 add a enum('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", errno.ErrTooLongValueForType)
	tk.MustGetErrCode("alter table t2 add a set('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')", errno.ErrTooLongValueForType)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableEnumLengthLimit = false
	})
	tk.MustExec("create table t3 (a enum('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))")
	tk.MustExec("insert into t3 values(1)")
	tk.MustQuery("select a from t3").Check(testkit.Rows("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	tk.MustExec("create table t4 (a set('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableEnumLengthLimit = true
	})
	tk.MustGetErrCode("create table t5 (a enum('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))", errno.ErrTooLongValueForType)
	tk.MustGetErrCode("create table t5 (a set('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))", errno.ErrTooLongValueForType)
	tk.MustExec("drop table if exists t1,t2,t3,t4,t5")
}

func TestGetReverseKey(t *testing.T) {
	var cluster testutils.Cluster
	store, dom, clean := testkit.CreateMockStoreAndDomain(t,
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cluster = c
		}))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database db_get")
	tk.MustExec("use db_get")
	tk.MustExec("create table test_get(a bigint not null primary key, b bigint)")

	insertVal := func(val int) {
		sql := fmt.Sprintf("insert into test_get value(%d, %d)", val, val)
		tk.MustExec(sql)
	}
	insertVal(math.MinInt64)
	insertVal(math.MinInt64 + 1)
	insertVal(1 << 61)
	insertVal(3 << 61)
	insertVal(math.MaxInt64)
	insertVal(math.MaxInt64 - 1)

	// Get table ID for split.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("db_get"), model.NewCIStr("test_get"))
	require.NoError(t, err)
	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 4)

	tk.MustQuery("select * from test_get order by a").Check(testkit.Rows("-9223372036854775808 -9223372036854775808",
		"-9223372036854775807 -9223372036854775807",
		"2305843009213693952 2305843009213693952",
		"6917529027641081856 6917529027641081856",
		"9223372036854775806 9223372036854775806",
		"9223372036854775807 9223372036854775807",
	))

	minKey := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(math.MinInt64))
	maxKey := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(math.MaxInt64))
	checkRet := func(startKey, endKey, retKey kv.Key) {
		h, err := ddl.GetMaxRowID(store, 0, tbl, startKey, endKey)
		require.NoError(t, err)
		require.Equal(t, 0, h.Cmp(retKey))
	}

	// [minInt64, minInt64]
	checkRet(minKey, minKey, minKey)
	// [minInt64, 1<<64-1]
	endKey := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(1<<61-1))
	retKey := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(math.MinInt64+1))
	checkRet(minKey, endKey, retKey)
	// [1<<64, 2<<64]
	startKey := tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(1<<61))
	endKey = tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(2<<61))
	checkRet(startKey, endKey, startKey)
	// [3<<64, maxInt64]
	startKey = tablecodec.EncodeRecordKey(tbl.RecordPrefix(), kv.IntHandle(3<<61))
	endKey = maxKey
	checkRet(startKey, endKey, endKey)
}

func TestLocalTemporaryTableBlockedDDL(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create temporary table tmp1 (id int primary key, a int unique, b int)")
	require.ErrorIs(t, tk.ExecToErr("rename table tmp1 to tmp2"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("alter table tmp1 add column c int"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("alter table tmp1 add index b(b)"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("create index a on tmp1(b)"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("drop index a on tmp1"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("lock tables tmp1 read"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("lock tables tmp1 write"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("lock tables t1 read, tmp1 read"), dbterror.ErrUnsupportedLocalTempTableDDL)
	require.ErrorIs(t, tk.ExecToErr("admin cleanup table lock tmp1"), dbterror.ErrUnsupportedLocalTempTableDDL)
}
