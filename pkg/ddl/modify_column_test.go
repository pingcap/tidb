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

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func batchInsert(tk *testkit.TestKit, tbl string, start, end int) {
	dml := fmt.Sprintf("insert into %s values", tbl)
	for i := start; i < end; i++ {
		dml += fmt.Sprintf("(%d, %d, %d)", i, i, i)
		if i != end-1 {
			dml += ","
		}
	}
	tk.MustExec(dml)
}

func TestModifyColumnReorgInfo(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	originalTimeout := ddl.ReorgWaitTimeout
	ddl.ReorgWaitTimeout = 10 * time.Millisecond
	limit := variable.GetDDLErrorCountLimit()
	variable.SetDDLErrorCountLimit(5)
	defer func() {
		ddl.ReorgWaitTimeout = originalTimeout
		variable.SetDDLErrorCountLimit(limit)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int, c2 int, c3 int, index idx(c2), index idx1(c1, c2));")

	sql := "alter table t1 change c2 c2 mediumint;"
	// defaultBatchSize is equal to ddl.defaultBatchSize
	base := defaultBatchSize * 8
	// add some rows
	batchInsert(tk, "t1", 0, base)
	// Make sure the count of regions more than backfill workers.
	tk.MustQuery("split table t1 between (0) and (8192) regions 8;").Check(testkit.Rows("8 1"))

	tbl := external.GetTableByName(t, tk, "test", "t1")

	// Check insert null before job first update.
	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr error
	var currJob *model.Job
	var elements []*meta.Element
	ctx := mock.NewContext()
	ctx.Store = store
	times := 0
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID || checkErr != nil || job.SchemaState != model.StateWriteReorganization {
			return
		}
		if job.Type == model.ActionModifyColumn {
			if times == 0 {
				times++
				return
			}
			currJob = job
			var (
				_newCol                *model.ColumnInfo
				_oldColName            *model.CIStr
				_pos                   = &ast.ColumnPosition{}
				_modifyColumnTp        byte
				_updatedAutoRandomBits uint64
				changingCol            *model.ColumnInfo
				changingIdxs           []*model.IndexInfo
			)
			checkErr = job.DecodeArgs(&_newCol, &_oldColName, _pos, &_modifyColumnTp, &_updatedAutoRandomBits, &changingCol, &changingIdxs)
			elements = ddl.BuildElements(changingCol, changingIdxs)
		}
		if job.Type == model.ActionAddIndex {
			if times == 1 {
				times++
				return
			}
			tbl := external.GetTableByName(t, tk, "test", "t1")
			indexInfo := tbl.Meta().FindIndexByName("idx2")
			elements = []*meta.Element{{ID: indexInfo.ID, TypeKey: meta.IndexElementKey}}
		}
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr", `return("cantDecodeRecordErr")`))
	dom.DDL().SetHook(hook)
	err := tk.ExecToErr(sql)
	require.EqualError(t, err, "[ddl:8202]Cannot decode index value, because mock can't decode record error")
	require.NoError(t, checkErr)
	// Check whether the reorg information is cleaned up when executing "modify column" failed.
	checkReorgHandle := func(gotElements, expectedElements []*meta.Element) {
		require.Equal(t, len(expectedElements), len(gotElements))
		for i, e := range gotElements {
			require.Equal(t, expectedElements[i], e)
		}
		// check the consistency of the tables.
		currJobID := strconv.FormatInt(currJob.ID, 10)
		tk.MustQuery("select job_id, reorg, schema_ids, table_ids, type, processing from mysql.tidb_ddl_job where job_id = " + currJobID).Check(testkit.Rows())
		tk.MustQuery("select job_id from mysql.tidb_ddl_history where job_id = " + currJobID).Check(testkit.Rows(currJobID))
		tk.MustQuery("select job_id, ele_id, ele_type, physical_id from mysql.tidb_ddl_reorg where job_id = " + currJobID).Check(testkit.Rows())
		require.NoError(t, sessiontxn.NewTxn(context.Background(), ctx))
		e, start, end, physicalID, err := ddl.NewReorgHandlerForTest(testkit.NewTestKit(t, store).Session()).GetDDLReorgHandle(currJob)
		require.Error(t, err, "Error not ErrDDLReorgElementNotExists, found orphan row in tidb_ddl_reorg for job.ID %d: e: '%s', physicalID: %d, start: 0x%x end: 0x%x", currJob.ID, e, physicalID, start, end)
		require.True(t, meta.ErrDDLReorgElementNotExist.Equal(err))
		require.Nil(t, e)
		require.Nil(t, start)
		require.Nil(t, end)
		require.Zero(t, physicalID)
	}
	expectedElements := []*meta.Element{
		{ID: 4, TypeKey: meta.ColumnElementKey},
		{ID: 3, TypeKey: meta.IndexElementKey},
		{ID: 4, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedElements)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr"))
	tk.MustExec("admin check table t1")

	// Check whether the reorg information is cleaned up when executing "modify column" successfully.
	// Test encountering a "notOwnerErr" error which caused the processing backfill job to exit halfway.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr", `return("modifyColumnNotOwnerErr")`))
	tk.MustExec(sql)
	expectedElements = []*meta.Element{
		{ID: 5, TypeKey: meta.ColumnElementKey},
		{ID: 5, TypeKey: meta.IndexElementKey},
		{ID: 6, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedElements)
	tk.MustExec("admin check table t1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr"))

	// Test encountering a "notOwnerErr" error which caused the processing backfill job to exit halfway.
	// During the period, the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr", `return("addIdxNotOwnerErr")`))
	tk.MustExec("alter table t1 add index idx2(c1)")
	expectedElements = []*meta.Element{
		{ID: 7, TypeKey: meta.IndexElementKey}}
	checkReorgHandle(elements, expectedElements)
	tk.MustExec("admin check table t1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockGetIndexRecordErr"))
}

func TestModifyColumnNullToNotNullWithChangingVal2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockInsertValueAfterCheckNull", `return("insert into test.tt values (NULL, NULL)")`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockInsertValueAfterCheckNull"))
	}()

	tk.MustExec("drop table if exists tt;")
	tk.MustExec(`create table tt (a bigint, b int, unique index idx(a));`)
	tk.MustExec("insert into tt values (1,1),(2,2),(3,3);")
	err := tk.ExecToErr("alter table tt modify a int not null;")
	require.EqualError(t, err, "[ddl:1265]Data truncated for column 'a' at row 1")
	tk.MustExec("drop table tt")
}

func TestModifyColumnNullToNotNull(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 600*time.Millisecond)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t1 (c1 int, c2 int)")

	tbl := external.GetTableByName(t, tk1, "test", "t1")

	// Check insert null before job first update.
	hook := &callback.TestDDLCallback{Do: dom}
	tk1.MustExec("delete from t1")
	once := sync.Once{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		once.Do(func() {
			checkErr = tk2.ExecToErr("insert into t1 values ()")
		})
	}
	dom.DDL().SetHook(hook)
	err := tk1.ExecToErr("alter table t1 change c2 c2 int not null")
	require.NoError(t, checkErr)
	require.EqualError(t, err, "[ddl:1138]Invalid use of NULL value")
	tk1.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil>"))

	// Check insert error when column has PreventNullInsertFlag.
	tk1.MustExec("delete from t1")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}

		if job.State != model.JobStateRunning {
			return
		}
		// now c2 has PreventNullInsertFlag, an error is expected.
		checkErr = tk2.ExecToErr("insert into t1 values ()")
	}
	dom.DDL().SetHook(hook)
	tk1.MustExec("alter table t1 change c2 c2 int not null")
	require.EqualError(t, checkErr, "[table:1048]Column 'c2' cannot be null")

	c2 := external.GetModifyColumn(t, tk1, "test", "t1", "c2", false)
	require.True(t, mysql.HasNotNullFlag(c2.GetFlag()))
	require.False(t, mysql.HasPreventNullInsertFlag(c2.GetFlag()))
	err = tk1.ExecToErr("insert into t1 values ();")
	require.EqualError(t, err, "[table:1364]Field 'c2' doesn't have a default value")
}

func TestModifyColumnNullToNotNullWithChangingVal(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 600*time.Millisecond)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t1 (c1 int, c2 int)")

	tbl := external.GetTableByName(t, tk1, "test", "t1")

	// Check insert null before job first update.
	hook := &callback.TestDDLCallback{Do: dom}
	tk1.MustExec("delete from t1")
	once := sync.Once{}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		once.Do(func() {
			checkErr = tk2.ExecToErr("insert into t1 values ()")
		})
	}
	dom.DDL().SetHook(hook)
	err := tk1.ExecToErr("alter table t1 change c2 c2 tinyint not null")
	require.NoError(t, checkErr)
	require.EqualError(t, err, "[ddl:1265]Data truncated for column 'c2' at row 1")
	tk1.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil>"))

	// Check insert error when column has PreventNullInsertFlag.
	tk1.MustExec("delete from t1")
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}

		if job.State != model.JobStateRunning {
			return
		}
		// now c2 has PreventNullInsertFlag, an error is expected.
		checkErr = tk2.ExecToErr("insert into t1 values ()")
	}
	dom.DDL().SetHook(hook)
	tk1.MustExec("alter table t1 change c2 c2 tinyint not null")
	require.EqualError(t, checkErr, "[table:1048]Column 'c2' cannot be null")

	c2 := external.GetModifyColumn(t, tk1, "test", "t1", "c2", false)
	require.True(t, mysql.HasNotNullFlag(c2.GetFlag()))
	require.False(t, mysql.HasPreventNullInsertFlag(c2.GetFlag()))
	require.EqualError(t, tk1.ExecToErr("insert into t1 values ()"), "[table:1364]Field 'c2' doesn't have a default value")

	c2 = external.GetModifyColumn(t, tk1, "test", "t1", "c2", false)
	require.Equal(t, mysql.TypeTiny, c2.FieldType.GetType())
}

func TestModifyColumnBetweenStringTypes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// varchar to varchar
	tk.MustExec("create table tt (a varchar(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a varchar(5);")
	mvc := external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, 5, mvc.FieldType.GetFlen())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a varchar(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a varchar(100);")
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// char to char
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a char(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a char(5);")
	mc := external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, 5, mc.FieldType.GetFlen())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a char(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a char(100);")
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// binary to binary
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a binary(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustGetErrMsg("alter table tt change a a binary(5);", "[types:1265]Data truncated for column 'a', value is '111\x00\x00\x00\x00\x00\x00\x00'")
	mb := external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, 10, mb.FieldType.GetFlen())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00", "10000\x00\x00\x00\x00\x00"))
	tk.MustGetErrMsg("alter table tt change a a binary(4);", "[types:1265]Data truncated for column 'a', value is '111\x00\x00\x00\x00\x00\x00\x00'")
	tk.MustExec("alter table tt change a a binary(12);")
	tk.MustQuery("select * from tt").Check(testkit.Rows("111\x00\x00\x00\x00\x00\x00\x00\x00\x00", "10000\x00\x00\x00\x00\x00\x00\x00"))
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("12", "12"))

	// varbinary to varbinary
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a varbinary(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")
	tk.MustExec("alter table tt change a a varbinary(5);")
	mvb := external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, 5, mvb.FieldType.GetFlen())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a varbinary(4);", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a varbinary(12);")
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustQuery("select length(a) from tt").Check(testkit.Rows("3", "5"))

	// varchar to char
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt (a varchar(10));")
	tk.MustExec("insert into tt values ('111'),('10000');")

	tk.MustExec("alter table tt change a a char(10);")
	c2 := external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, mysql.TypeString, c2.FieldType.GetType())
	require.Equal(t, 10, c2.FieldType.GetFlen())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustGetErrMsg("alter table tt change a a char(4);", "[types:1265]Data truncated for column 'a', value is '10000'")

	// char to text
	tk.MustExec("alter table tt change a a text;")
	c2 = external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, mysql.TypeBlob, c2.FieldType.GetType())

	// text to set
	tk.MustGetErrMsg("alter table tt change a a set('111', '2222');", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a set('111', '10000');")
	c2 = external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, mysql.TypeSet, c2.FieldType.GetType())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))

	// set to set
	tk.MustExec("alter table tt change a a set('10000', '111');")
	c2 = external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, mysql.TypeSet, c2.FieldType.GetType())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))

	// set to enum
	tk.MustGetErrMsg("alter table tt change a a enum('111', '2222');", "[types:1265]Data truncated for column 'a', value is '10000'")
	tk.MustExec("alter table tt change a a enum('111', '10000');")
	c2 = external.GetModifyColumn(t, tk, "test", "tt", "a", false)
	require.Equal(t, mysql.TypeEnum, c2.FieldType.GetType())
	tk.MustQuery("select * from tt").Check(testkit.Rows("111", "10000"))
	tk.MustExec("alter table tt change a a enum('10000', '111');")
	tk.MustQuery("select * from tt where a = 1").Check(testkit.Rows("10000"))
	tk.MustQuery("select * from tt where a = 2").Check(testkit.Rows("111"))

	// no-strict mode
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustExec("alter table tt change a a enum('111', '2222');")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1265|Data truncated for column 'a', value is '10000'"))

	tk.MustExec("drop table tt;")
}

func TestModifyColumnCharset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mcc(a varchar(8) charset utf8, b varchar(8) charset utf8)")

	result := tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("alter table t_mcc modify column a varchar(8);")
	tbl := external.GetTableByName(t, tk, "test", "t_mcc")
	tbl.Meta().Version = model.TableInfoVersion0
	// When the table version is TableInfoVersion0, the following statement don't change "b" charset.
	// So the behavior is not compatible with MySQL.
	tk.MustExec("alter table t_mcc modify column b varchar(8);")
	result = tk.MustQuery(`show create table t_mcc`)
	result.Check(testkit.Rows(
		"t_mcc CREATE TABLE `t_mcc` (\n" +
			"  `a` varchar(8) DEFAULT NULL,\n" +
			"  `b` varchar(8) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestModifyColumnTime(t *testing.T) {
	now := time.Now().UTC()
	now = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	timeToDate1 := now.Format("2006-01-02")
	timeToDate2 := now.AddDate(0, 0, 30).Format("2006-01-02")
	timeToDatetime1 := now.Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime2 := now.Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	timeToDatetime3 := now.Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime4 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToDatetime5 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	timeToTimestamp1 := now.Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp2 := now.Add(20 * time.Hour).Format("2006-01-02 15:04:05")
	timeToTimestamp3 := now.Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp4 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Add(12 * time.Second).Format("2006-01-02 15:04:05")
	timeToTimestamp5 := now.AddDate(0, 0, 30).Add(20 * time.Hour).Format("2006-01-02 15:04:05")

	tests := []testModifyColumnTimeCase{
		// time to date
		{"time", `"30 20:00:12"`, "date", timeToDate2, 0},
		{"time", `"30 20:00"`, "date", timeToDate2, 0},
		{"time", `"30 20"`, "date", timeToDate2, 0},
		{"time", `"20:00:12"`, "date", timeToDate1, 0},
		{"time", `"20:00"`, "date", timeToDate1, 0},
		{"time", `"12"`, "date", timeToDate1, 0},
		{"time", `"200012"`, "date", timeToDate1, 0},
		{"time", `200012`, "date", timeToDate1, 0},
		{"time", `0012`, "date", timeToDate1, 0},
		{"time", `12`, "date", timeToDate1, 0},
		{"time", `"30 20:00:12.498"`, "date", timeToDate2, 0},
		{"time", `"20:00:12.498"`, "date", timeToDate1, 0},
		{"time", `"200012.498"`, "date", timeToDate1, 0},
		{"time", `200012.498`, "date", timeToDate1, 0},
		// time to datetime
		{"time", `"30 20:00:12"`, "datetime", timeToDatetime4, 0},
		{"time", `"30 20:00"`, "datetime", timeToDatetime5, 0},
		{"time", `"30 20"`, "datetime", timeToDatetime5, 0},
		{"time", `"20:00:12"`, "datetime", timeToDatetime1, 0},
		{"time", `"20:00"`, "datetime", timeToDatetime2, 0},
		{"time", `"12"`, "datetime", timeToDatetime3, 0},
		{"time", `"200012"`, "datetime", timeToDatetime1, 0},
		{"time", `200012`, "datetime", timeToDatetime1, 0},
		{"time", `0012`, "datetime", timeToDatetime3, 0},
		{"time", `12`, "datetime", timeToDatetime3, 0},
		{"time", `"30 20:00:12.498"`, "datetime", timeToDatetime4, 0},
		{"time", `"20:00:12.498"`, "datetime", timeToDatetime1, 0},
		{"time", `"200012.498"`, "datetime", timeToDatetime1, 0},
		{"time", `200012.498`, "datetime", timeToDatetime1, 0},
		// time to timestamp
		{"time", `"30 20:00:12"`, "timestamp", timeToTimestamp4, 0},
		{"time", `"30 20:00"`, "timestamp", timeToTimestamp5, 0},
		{"time", `"30 20"`, "timestamp", timeToTimestamp5, 0},
		{"time", `"20:00:12"`, "timestamp", timeToTimestamp1, 0},
		{"time", `"20:00"`, "timestamp", timeToTimestamp2, 0},
		{"time", `"12"`, "timestamp", timeToTimestamp3, 0},
		{"time", `"200012"`, "timestamp", timeToTimestamp1, 0},
		{"time", `200012`, "timestamp", timeToTimestamp1, 0},
		{"time", `0012`, "timestamp", timeToTimestamp3, 0},
		{"time", `12`, "timestamp", timeToTimestamp3, 0},
		{"time", `"30 20:00:12.498"`, "timestamp", timeToTimestamp4, 0},
		{"time", `"20:00:12.498"`, "timestamp", timeToTimestamp1, 0},
		{"time", `"200012.498"`, "timestamp", timeToTimestamp1, 0},
		{"time", `200012.498`, "timestamp", timeToTimestamp1, 0},
	}
	testModifyColumnTime(t, tests)
}

type testModifyColumnTimeCase struct {
	from   string
	value  string
	to     string
	expect string
	err    uint16
}

func testModifyColumnTime(t *testing.T, tests []testModifyColumnTimeCase) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 3")
	tk.MustExec("set @@time_zone=UTC")

	defer func() {
		tk.MustExec("set @@global.tidb_ddl_error_count_limit = default")
		tk.MustExec("set @@time_zone=default")
	}()

	for _, test := range tests {
		comment := fmt.Sprintf("%+v", test)
		tk.MustExec("drop table if exists t_mc")
		tk.MustExec(fmt.Sprintf("create table t_mc(a %s)", test.from))
		tk.MustExec(fmt.Sprintf(`insert into t_mc (a) values (%s)`, test.value))
		_, err := tk.Exec(fmt.Sprintf(`alter table t_mc modify a %s`, test.to))
		if test.err != 0 {
			require.Error(t, err, comment)
			require.Regexp(t, fmt.Sprintf(".*[ddl:%d].*", test.err), err.Error(), comment)
			continue
		}
		require.NoError(t, err, comment)
		tk.MustQuery("select a from t_mc").Check(testkit.Rows(test.expect))
	}
}

// TestModifyColumnTypeWhenInterception is to test modifying column type with warnings intercepted by
// reorg timeout, not owner error and so on.
func TestModifyColumnTypeWhenInterception(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test normal warnings.
	tk.MustExec("create table t(a int primary key, b decimal(4,2))")

	count := defaultBatchSize * 4
	// Add some rows.
	dml := "insert into t values"
	for i := 1; i <= count; i++ {
		dml += fmt.Sprintf("(%d, %f)", i, 11.22)
		if i != count {
			dml += ","
		}
	}
	tk.MustExec(dml)
	// Make the regions scale like: [1, 1024), [1024, 2048), [2048, 3072), [3072, 4096]
	tk.MustQuery("split table t between(0) and (4096) regions 4")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/MockReorgTimeoutInOneRegion", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/MockReorgTimeoutInOneRegion"))
	}()
	tk.MustExec("alter table t modify column b decimal(3,1)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 4096 warnings with this error code, first warning: Truncated incorrect DECIMAL value: '11.22'"))
}
