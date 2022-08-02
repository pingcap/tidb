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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/stretchr/testify/require"
)

// TestModifyColumnTypeArgs test job raw args won't be updated when error occurs in `updateVersionAndTableInfo`.
func TestModifyColumnTypeArgs(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr", `return(2)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr"))
	}()

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_modify_column_args")
	tk.MustExec("create table t_modify_column_args(a int, unique(a))")

	err := tk.ExecToErr("alter table t_modify_column_args modify column a tinyint")
	require.Error(t, err)
	// error goes like `mock update version and tableInfo error,jobID=xx`
	strs := strings.Split(err.Error(), ",")
	require.Equal(t, "[ddl:-1]mock update version and tableInfo error", strs[0])

	jobID := strings.Split(strs[1], "=")[1]
	tbl := external.GetTableByName(t, tk, "test", "t_modify_column_args")
	require.Len(t, tbl.Meta().Columns, 1)
	require.Len(t, tbl.Meta().Indices, 1)

	id, err := strconv.Atoi(jobID)
	require.NoError(t, err)
	historyJob, err := ddl.GetHistoryJobByID(tk.Session(), int64(id))
	require.NoError(t, err)
	require.NotNil(t, historyJob)

	var (
		newCol                *model.ColumnInfo
		oldColName            *model.CIStr
		modifyColumnTp        byte
		updatedAutoRandomBits uint64
		changingCol           *model.ColumnInfo
		changingIdxs          []*model.IndexInfo
	)
	pos := &ast.ColumnPosition{}
	err = historyJob.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
	require.NoError(t, err)
	require.Nil(t, changingCol)
	require.Nil(t, changingIdxs)
}

func TestParallelUpdateTableReplica(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount"))
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_db_state default charset utf8 default collate utf8_bin")
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")

	tk1, tk2, ch, originalCallback := prepareTestControlParallelExecSQL(t, store, dom)
	defer dom.DDL().SetHook(originalCallback)

	t1 := external.GetTableByName(t, tk, "test_db_state", "t1")

	var err1 error
	var err2 error
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		// Mock for table tiflash replica was available.
		err1 = domain.GetDomain(tk1.Session()).DDL().UpdateTableReplicaInfo(tk1.Session(), t1.Meta().ID, true)
	})
	wg.Run(func() {
		<-ch
		// Mock for table tiflash replica was available.
		err2 = domain.GetDomain(tk2.Session()).DDL().UpdateTableReplicaInfo(tk2.Session(), t1.Meta().ID, true)
	})
	wg.Wait()
	require.NoError(t, err1)
	require.EqualError(t, err2, "[ddl:-1]the replica available status of table t1 is already updated")
}

// TestParallelFlashbackTable tests parallel flashback table.
func TestParallelFlashbackTable(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func(originGC bool) {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}(ddlutil.IsEmulatorGCEnable())

	// disable emulator GC.
	// Disable emulator GC, otherwise, emulator GC will delete table record as soon as possible after executing drop table DDL.
	ddlutil.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// set GC enable.
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	// prepare dropped table.
	tk.MustExec("create database test_db_state default charset utf8 default collate utf8_bin")
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("drop table if exists t")
	// Test parallel flashback table.
	sql1 := "flashback table t to t_flashback"
	f := func(err1, err2 error) {
		require.NoError(t, err1)
		require.EqualError(t, err2, "[schema:1050]Table 't_flashback' already exists")
	}
	testControlParallelExecSQL(t, tk, store, dom, "", sql1, sql1, f)

	// Test parallel flashback table with different name
	tk.MustExec("drop table t_flashback")
	sql1 = "flashback table t_flashback"
	sql2 := "flashback table t_flashback to t_flashback2"
	testControlParallelExecSQL(t, tk, store, dom, "", sql1, sql2, f)
}
