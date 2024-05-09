// Copyright 2020 PingCAP, Inc.
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
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestColumnTypeChangeStateBetweenInteger(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")

	// use new session to check meta in callback function.
	internalTK := testkit.NewTestKit(t, store)
	internalTK.MustExec("use test")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	require.NotNil(t, external.GetModifyColumn(t, tk, "test", "t", "c2", false))

	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr error
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateNone:
			tbl = external.GetTableByName(t, internalTK, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.Cols()) != 2 {
				checkErr = errors.New("len(cols) is not right")
			}
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			tbl = external.GetTableByName(t, internalTK, "test", "t")
			if tbl == nil {
				checkErr = errors.New("tbl is nil")
			} else if len(tbl.(*tables.TableCommon).Columns) != 3 {
				// changingCols has been added into meta.
				checkErr = errors.New("len(cols) is not right")
			} else if external.GetModifyColumn(t, internalTK, "test", "t", "c2", true).GetFlag()&mysql.PreventNullInsertFlag == uint(0) {
				checkErr = errors.New("old col's flag is not right")
			} else if external.GetModifyColumn(t, internalTK, "test", "t", "_Col$_c2_0", true) == nil {
				checkErr = errors.New("changingCol is nil")
			}
		}
	}
	dom.DDL().SetHook(hook)
	// Alter sql will modify column c2 to tinyint not null.
	SQL := "alter table t modify column c2 tinyint not null"
	tk.MustExec(SQL)
	// Assert the checkErr in the job of every state.
	require.NoError(t, checkErr)

	// Check the col meta after the column type change.
	tbl = external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	col := external.GetModifyColumn(t, tk, "test", "t", "c2", false)
	require.NotNil(t, col)
	require.Equal(t, true, mysql.HasNotNullFlag(col.GetFlag()))
	require.NotEqual(t, 0, col.GetFlag()&mysql.NoDefaultValueFlag)
	require.Equal(t, mysql.TypeTiny, col.GetType())
	require.Nil(t, col.ChangeStateInfo)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

func TestRollbackColumnTypeChangeBetweenInteger(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bigint, c2 bigint)")
	tk.MustExec("insert into t(c1, c2) values (1, 1)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	require.NotNil(t, external.GetModifyColumn(t, tk, "test", "t", "c2", false))

	hook := &callback.TestDDLCallback{Do: dom}
	// Mock roll back at model.StateNone.
	customizeHookRollbackAtState(hook, tbl, model.StateNone)
	dom.DDL().SetHook(hook)
	// Alter sql will modify column c2 to bigint not null.
	SQL := "alter table t modify column c2 int not null"
	err := tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-none")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateDeleteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateDeleteOnly)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-delete only")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateWriteOnly.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteOnly)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-write only")
	assertRollBackedColUnchanged(t, tk)

	// Mock roll back at model.StateWriteReorg.
	customizeHookRollbackAtState(hook, tbl, model.StateWriteReorganization)
	dom.DDL().SetHook(hook)
	err = tk.ExecToErr(SQL)
	require.EqualError(t, err, "[ddl:1]MockRollingBackInCallBack-write reorganization")
	assertRollBackedColUnchanged(t, tk)
}

func customizeHookRollbackAtState(hook *callback.TestDDLCallback, tbl table.Table, state model.SchemaState) {
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.SchemaState == state {
			job.State = model.JobStateRollingback
			job.Error = mockTerrorMap[state.String()]
		}
	}
}

func assertRollBackedColUnchanged(t *testing.T, tk *testkit.TestKit) {
	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Equal(t, 2, len(tbl.Cols()))
	col := external.GetModifyColumn(t, tk, "test", "t", "c2", false)
	require.NotNil(t, col)
	require.Equal(t, uint(0), col.GetFlag())
	require.Equal(t, mysql.TypeLonglong, col.GetType())
	require.Nil(t, col.ChangeStateInfo)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
}

var mockTerrorMap = make(map[string]*terror.Error)

func init() {
	// Since terror new action will cause data race with other test suite (getTerrorCode) in parallel, we init it all here.
	mockTerrorMap[model.StateNone.String()] = dbterror.ClassDDL.NewStdErr(1, mysql.Message("MockRollingBackInCallBack-"+model.StateNone.String(), nil))
	mockTerrorMap[model.StateDeleteOnly.String()] = dbterror.ClassDDL.NewStdErr(1, mysql.Message("MockRollingBackInCallBack-"+model.StateDeleteOnly.String(), nil))
	mockTerrorMap[model.StateWriteOnly.String()] = dbterror.ClassDDL.NewStdErr(1, mysql.Message("MockRollingBackInCallBack-"+model.StateWriteOnly.String(), nil))
	mockTerrorMap[model.StateWriteReorganization.String()] = dbterror.ClassDDL.NewStdErr(1, mysql.Message("MockRollingBackInCallBack-"+model.StateWriteReorganization.String(), nil))
}

// Test issue #20529.
func TestColumnTypeChangeIgnoreDisplayLength(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	var assertResult bool
	assertHasAlterWriteReorg := func(tbl table.Table) {
		// Restore assertResult to false.
		assertResult = false
		hook := &callback.TestDDLCallback{Do: dom}
		hook.OnJobRunBeforeExported = func(job *model.Job) {
			if tbl.Meta().ID != job.TableID {
				return
			}
			if job.SchemaState == model.StateWriteReorganization {
				assertResult = true
			}
		}
		dom.DDL().SetHook(hook)
	}

	// Change int to tinyint.
	// Although display length is increased, the default flen is decreased, reorg is needed.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(1))")
	tbl := external.GetTableByName(t, tk, "test", "t")
	assertHasAlterWriteReorg(tbl)
	tk.MustExec("alter table t modify column a tinyint(3)")
	require.True(t, assertResult)

	// Change tinyint to tinyint
	// Although display length is decreased, default flen is the same, reorg is not needed.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a tinyint(3))")
	tbl = external.GetTableByName(t, tk, "test", "t")
	assertHasAlterWriteReorg(tbl)
	tk.MustExec("alter table t modify column a tinyint(1)")
	require.False(t, assertResult)
	tk.MustExec("drop table if exists t")
}

// TestRowFormat is used to close issue #21391, the encoded row in column type change should be aware of the new row format.
func TestRowFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v varchar(10))")
	tk.MustExec("insert into t values (1, \"123\");")
	tk.MustExec("alter table t modify column v varchar(5);")

	tbl := external.GetTableByName(t, tk, "test", "t")
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))

	h := helper.NewHelper(store.(helper.Storage))
	data, err := h.GetMvccByEncodedKey(encodedKey)
	require.NoError(t, err)
	// The new format will start with CodecVer = 128 (0x80).
	require.Equal(t, []byte{0x80, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x4, 0x0, 0x7, 0x0, 0x1, 0x31, 0x32, 0x33, 0x31, 0x32, 0x33}, data.Info.Writes[0].ShortValue)
	tk.MustExec("drop table if exists t")
}

func TestRowFormatWithChecksums(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v varchar(10))")
	tk.MustExec("insert into t values (1, \"123\");")
	tk.MustExec("alter table t modify column v varchar(5);")

	tbl := external.GetTableByName(t, tk, "test", "t")
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))

	h := helper.NewHelper(store.(helper.Storage))
	data, err := h.GetMvccByEncodedKey(encodedKey)
	require.NoError(t, err)
	// row value with checksums
	require.Equal(t, []byte{0x80, 0x2, 0x3, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x1, 0x0, 0x4, 0x0, 0x7, 0x0, 0x1, 0x31, 0x32, 0x33, 0x31, 0x32, 0x33, 0x8, 0x52, 0x78, 0xc9, 0x28, 0x52, 0x78, 0xc9, 0x28}, data.Info.Writes[0].ShortValue)
	tk.MustExec("drop table if exists t")
}

func TestRowLevelChecksumWithMultiSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v varchar(10))")
	tk.MustExec("insert into t values (1, \"123\")")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/forceRowLevelChecksumOnUpdateColumnBackfill", "return"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/forceRowLevelChecksumOnUpdateColumnBackfill")
	tk.MustExec("alter table t add column vv int, modify column v varchar(5)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))

	h := helper.NewHelper(store.(helper.Storage))
	data, err := h.GetMvccByEncodedKey(encodedKey)
	require.NoError(t, err)
	// checksum skipped and with a null col vv
	require.Equal(t, []byte{0x80, 0x0, 0x3, 0x0, 0x1, 0x0, 0x1, 0x2, 0x4, 0x3, 0x1, 0x0, 0x4, 0x0, 0x7, 0x0, 0x1, 0x31, 0x32, 0x33, 0x31, 0x32, 0x33}, data.Info.Writes[0].ShortValue)
	tk.MustExec("drop table if exists t")
}

// Close issue #22395
// Background:
// Since the changing column is implemented as adding a new column and substitute the old one when it finished.
// The added column with NOT-NULL option will be fetched with error when it's origin default value is not set.
// It's good because the insert / update logic will cast the related column to changing column rather than use
// origin default value directly.
func TestChangingColOriginDefaultValue(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, unique key(a))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(2, 2)")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &callback.TestDDLCallback{Do: dom}
	var (
		once     bool
		checkErr error
	)
	i := 0
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if (job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization) && i < 3 {
			if !once {
				once = true
				tbl := external.GetTableByName(t, tk, "test", "t")
				if len(tbl.WritableCols()) != 3 {
					checkErr = errors.New("assert the writable column number error")
					return
				}
				if tbl.WritableCols()[2].OriginDefaultValue.(string) != "0" {
					checkErr = errors.New("assert the write only column origin default value error")
					return
				}
			}
			// For writable column:
			// Insert/ Update should set the column with the casted-related column value.
			sql := fmt.Sprintf("insert into t values(%d, %d)", i+3, i+3)
			_, err := tk1.Exec(sql)
			if err != nil {
				checkErr = err
				return
			}
			if job.SchemaState == model.StateWriteOnly {
				// The casted value will be inserted into changing column too.
				_, err := tk1.Exec("update t set b = -1 where a = 1")
				if err != nil {
					checkErr = err
					return
				}
			} else {
				// The casted value will be inserted into changing column too.
				_, err := tk1.Exec("update t set b = -2 where a = 2")
				if err != nil {
					checkErr = err
					return
				}
			}
			i++
		}
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column b tinyint NOT NULL")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	// Since getReorgInfo will stagnate StateWriteReorganization for a ddl round, so insert should exec 3 times.
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 -1", "2 -2", "3 3", "4 4", "5 5"))
	tk.MustExec("drop table if exists t")
}

func TestChangingColOriginDefaultValueAfterAddColAndCastSucc(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set time_zone = 'UTC'")

	tk.MustExec("set time_zone = 'UTC'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, unique key(a))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("insert into t values(2, 2)")
	tk.MustExec("alter table t add column c timestamp default '1971-06-09' not null")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &callback.TestDDLCallback{Do: dom}
	var (
		once     bool
		checkErr error
	)
	i, stableTimes := 0, 0
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if (job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization) && stableTimes < 3 {
			if !once {
				once = true
				tbl := external.GetTableByName(t, tk, "test", "t")
				if len(tbl.WritableCols()) != 4 {
					checkErr = errors.New("assert the writable column number error")
					return
				}
				originalDV := fmt.Sprintf("%v", tbl.WritableCols()[3].OriginDefaultValue)
				expectVal := "1971-06-09"
				if originalDV != expectVal {
					errMsg := fmt.Sprintf("expect: %v, got: %v", expectVal, originalDV)
					checkErr = errors.New("assert the write only column origin default value error" + errMsg)
					return
				}
			}
			// For writable column:
			// Insert / Update should set the column with the casted-related column value.
			sql := fmt.Sprintf("insert into t values(%d, %d, '2021-06-06 12:13:14')", i+3, i+3)
			err := tk1.ExecToErr(sql)
			if err != nil {
				checkErr = err
				return
			}
			if job.SchemaState == model.StateWriteOnly {
				// The casted value will be inserted into changing column too.
				// for point get
				err := tk1.ExecToErr("update t set b = -1 where a = 1")
				if err != nil {
					checkErr = err
					return
				}
			} else {
				// The casted value will be inserted into changing column too.
				// for point get
				err := tk1.ExecToErr("update t set b = -2 where a = 2")
				if err != nil {
					checkErr = err
					return
				}
			}
			stableTimes++
		}
		i++
	}

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column c date NOT NULL")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	// Since getReorgInfo will stagnate StateWriteReorganization for a ddl round, so insert should exec 3 times.
	tk.MustQuery("select * from t order by a").Check(
		testkit.Rows("1 -1 1971-06-09", "2 -2 1971-06-09", "5 5 2021-06-06", "6 6 2021-06-06", "7 7 2021-06-06"))
	tk.MustExec("drop table if exists t")
}

// TestChangingColOriginDefaultValueAfterAddColAndCastFail tests #25383.
func TestChangingColOriginDefaultValueAfterAddColAndCastFail(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk.MustExec("set time_zone = 'UTC'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a VARCHAR(31) NULL DEFAULT 'wwrzfwzb01j6ddj', b DECIMAL(12,0) NULL DEFAULT '-729850476163')")
	tk.MustExec("ALTER TABLE t ADD COLUMN x CHAR(218) NULL DEFAULT 'lkittuae'")

	tbl := external.GetTableByName(t, tk, "test", "t")
	originalHook := dom.DDL().GetHook()
	hook := &callback.TestDDLCallback{Do: dom}
	var checkErr error
	var firstJobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}

		if firstJobID == 0 {
			firstJobID = job.ID
		}
		if job.SchemaState == model.StateWriteOnly || job.SchemaState == model.StateWriteReorganization {
			tbl := external.GetTableByName(t, tk, "test", "t")
			if len(tbl.WritableCols()) != 4 {
				errMsg := fmt.Sprintf("job ID:%d, cols len:%v", job.ID, len(tbl.WritableCols()))
				checkErr = errors.New("assert the writable column number error" + errMsg)
				return
			}
			// modify column x
			if job.ID == firstJobID {
				originalDV := fmt.Sprintf("%v", tbl.WritableCols()[3].OriginDefaultValue)
				expectVal := "0000-00-00 00:00:00"
				if originalDV != expectVal {
					errMsg := fmt.Sprintf("job ID:%d, expect: %v, got: %v", job.ID, expectVal, originalDV)
					checkErr = errors.New("assert the write only column origin default value error" + errMsg)
					return
				}
				// The cast value will be inserted into changing column too.
				_, err := tk1.Exec("UPDATE t SET a = '18apf' WHERE x = '' AND a = 'mul'")
				if err != nil {
					checkErr = err
					return
				}
			}
			// modify column b
			if job.ID == firstJobID+1 {
				originalDV := fmt.Sprintf("%v", tbl.WritableCols()[3].OriginDefaultValue)
				expectVal := ""
				if originalDV != expectVal {
					errMsg := fmt.Sprintf("job ID:%d, expect: %v, got: %v", job.ID, expectVal, originalDV)
					checkErr = errors.New("assert the write only column origin default value error" + errMsg)
					return
				}
				// The cast value will be inserted into changing column too.
				_, err := tk1.Exec("UPDATE t SET a = '18apf' WHERE a = '1'")
				if err != nil {
					checkErr = err
					return
				}
			}
		}
	}

	dom.DDL().SetHook(hook)
	tk.MustExec("alter table t modify column x DATETIME NULL DEFAULT '3771-02-28 13:00:11' AFTER b;")
	tk.MustExec("insert into t(a) value('1')")
	tk.MustExec("alter table t modify column b varchar(256) default (REPLACE(UPPER(UUID()), '-', ''));")
	dom.DDL().SetHook(originalHook)
	require.NoError(t, checkErr)
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("18apf -729850476163 3771-02-28 13:00:11"))
	tk.MustExec("drop table if exists t")
}

// Close issue #23202
func TestDDLExitWhenCancelMeetPanic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("alter table t add index(b)")
	tk.MustExec("set @@global.tidb_ddl_error_count_limit=3")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockExceedErrorLimit", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockExceedErrorLimit"))
	}()

	hook := &callback.TestDDLCallback{Do: dom}
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if jobID != 0 {
			return
		}
		if job.Type == model.ActionDropIndex {
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)

	// when it panics in write-reorg state, the job will be pulled up as a cancelling job. Since drop-index with
	// write-reorg can't be cancelled, so it will be converted to running state and try again (dead loop).
	err := tk.ExecToErr("alter table t drop index b")
	require.EqualError(t, err, "[ddl:-1]panic in handling DDL logic and error count beyond the limitation 3, cancelled")
	require.Less(t, int64(0), jobID)

	// Verification of the history job state.
	job, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.Equal(t, int64(4), job.ErrorCount)
	require.Equal(t, "[ddl:-1]panic in handling DDL logic and error count beyond the limitation 3, cancelled", job.Error.Error())
}

// Close issue #24584
func TestCancelCTCInReorgStateWillCauseGoroutineLeak(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockInfiniteReorgLogic", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockInfiniteReorgLogic"))
	}()

	// set ddl hook
	originalHook := dom.DDL().GetHook()
	defer dom.DDL().SetHook(originalHook)

	tk.MustExec("drop table if exists ctc_goroutine_leak")
	tk.MustExec("create table ctc_goroutine_leak (a int)")
	tk.MustExec("insert into ctc_goroutine_leak values(1),(2),(3)")
	tbl := external.GetTableByName(t, tk, "test", "ctc_goroutine_leak")

	hook := &callback.TestDDLCallback{Do: dom}
	var jobID int64
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if jobID != 0 {
			return
		}
		if tbl.Meta().ID != job.TableID {
			return
		}
		if job.Query == "alter table ctc_goroutine_leak modify column a tinyint" {
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	var (
		wg       util.WaitGroupWrapper
		alterErr error
	)
	wg.Run(func() {
		// This ddl will be hang over in the failpoint loop, waiting for outside cancel.
		_, alterErr = tk1.Exec("alter table ctc_goroutine_leak modify column a tinyint")
	})

	<-ddl.TestReorgGoroutineRunning
	tk.MustExec("admin cancel ddl jobs " + strconv.Itoa(int(jobID)))
	wg.Wait()
	require.Equal(t, "[ddl:8214]Cancelled DDL job", alterErr.Error())
}

// Fix issue: https://github.com/pingcap/tidb/issues/26292
// Cast date to timestamp has two kind behavior: cast("3977-02-22" as date)
// For select statement, it truncates the string and return no errors. (which is 3977-02-22 00:00:00 here)
// For ddl reorging or changing column in ctc, it needs report some errors.
func TestCastDateToTimestampInReorgAttribute(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 600*time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_enable_row_level_checksum = 1")
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE `t` (`a` DATE NULL DEFAULT '8497-01-06')")
	tk.MustExec("insert into t values(now())")

	tbl := external.GetTableByName(t, tk, "test", "t")
	require.NotNil(t, tbl)
	require.Len(t, tbl.Cols(), 1)
	var checkErr1 error
	var checkErr2 error

	hook := &callback.TestDDLCallback{Do: dom}
	hook.OnJobRunBeforeExported = func(job *model.Job) {
		if checkErr1 != nil || checkErr2 != nil || tbl.Meta().ID != job.TableID {
			return
		}
		switch job.SchemaState {
		case model.StateWriteOnly:
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			checkErr1 = tk.ExecToErr("insert into `t` set  `a` = '3977-02-22'") // this(string) will be cast to a as date, then cast a(date) as timestamp to changing column.
			checkErr2 = tk.ExecToErr("update t set `a` = '3977-02-22'")
		}
	}
	dom.DDL().SetHook(hook)

	tk.MustExec("alter table t modify column a  TIMESTAMP NULL DEFAULT '2021-04-28 03:35:11' FIRST")
	require.EqualError(t, checkErr1, "[types:1292]Incorrect timestamp value: '3977-02-22'")
	require.EqualError(t, checkErr2, "[types:1292]Incorrect timestamp value: '3977-02-22'")
}
