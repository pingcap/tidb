// Copyright 2016 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLitAddIndex(tk *testkit.TestKit, t *testing.T, ctx sessionctx.Context, tblID int64, unique bool, indexName string, colName string, dom *domain.Domain) int64 {
	uniqueStr := ""
	if unique {
		uniqueStr = "unique"
	}
	sql := fmt.Sprintf("alter table t add %s index %s(%s)", uniqueStr, indexName, colName)
	tk.MustExec(sql)

	idi, _ := strconv.Atoi(tk.MustQuery("admin show ddl jobs 1;").Rows()[0][0].(string))
	id := int64(idi)
	v := getSchemaVer(t, ctx)
	require.NoError(t, dom.Reload())
	tblInfo, exist := dom.InfoSchema().TableByID(tblID)
	require.True(t, exist)
	checkHistoryJobArgs(t, ctx, id, &historyJobArgs{ver: v, tbl: tblInfo.Meta()})
	return id
}

func TestEnableLightning(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Check default value, Current is off
	allow := ddl.IsEnableFastReorg()
	require.Equal(t, variable.DefTiDBDDLEnableFastReorg, allow)
	// Set an illegal value.
	err := tk.ExecToErr("set @@global.tidb_ddl_enable_fast_reorg = abc")
	require.Error(t, err)
	allow = ddl.IsEnableFastReorg()
	require.Equal(t, variable.DefTiDBDDLEnableFastReorg, allow)

	// Set the variable to on.
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on")
	allow = ddl.IsEnableFastReorg()
	require.Equal(t, true, allow)
	// Set the variable to off.
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = off")
	allow = ddl.IsEnableFastReorg()
	require.Equal(t, false, allow)
}

func TestAddIndexLit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert t values (1, 1, 1), (2, 2, 2), (3, 3, 1);")
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = on")

	var tableID int64
	rs := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_name='t' and table_schema='test';")
	tableIDi, _ := strconv.Atoi(rs.Rows()[0][0].(string))
	tableID = int64(tableIDi)

	// Non-unique secondary index
	jobID := testLitAddIndex(tk, t, testNewContext(store), tableID, false, "idx1", "c2", dom)
	testCheckJobDone(t, store, jobID, true)

	// Unique secondary index
	jobID = testLitAddIndex(tk, t, testNewContext(store), tableID, true, "idx2", "c2", dom)
	testCheckJobDone(t, store, jobID, true)

	// Unique duplicate key
	err := tk.ExecToErr("alter table t1 add index unique idx3(c3)")
	require.Error(t, err)
}

func TestAddIndexMergeProcess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6);")
	// Force onCreateIndex use the backfill-merge process.
	lightning.GlobalEnv.IsInited = false
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")

	var checkErr error
	var runDML, backfillDone bool
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(&ddl.TestDDLCallback{
		Do: dom,
		OnJobUpdatedExported: func(job *model.Job) {
			if !runDML && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
				idx := findIdxInfo(dom, "test", "t", "idx")
				if idx == nil || idx.BackfillState != model.BackfillStateRunning {
					return
				}
				if !backfillDone {
					// Wait another round so that the backfill range is determined(1-4).
					backfillDone = true
					return
				}
				runDML = true
				// Write record 7 to the temporary index.
				_, checkErr = tk2.Exec("insert into t values (7, 8, 9);")
			}
		},
	})
	tk.MustExec("alter table t add index idx(c1);")
	dom.DDL().SetHook(originHook)
	require.True(t, backfillDone)
	require.True(t, runDML)
	require.NoError(t, checkErr)
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t use index (idx);").Check(testkit.Rows("1 2 3", "4 5 6", "7 8 9"))
	tk.MustQuery("select * from t ignore index (idx);").Check(testkit.Rows("1 2 3", "4 5 6", "7 8 9"))
}

func TestAddPrimaryKeyMergeProcess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6);")
	// Force onCreateIndex use the backfill-merge process.
	lightning.GlobalEnv.IsInited = false
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")

	var checkErr error
	var runDML, backfillDone bool
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(&ddl.TestDDLCallback{
		Do: nil, // We'll reload the schema manually.
		OnJobUpdatedExported: func(job *model.Job) {
			if !runDML && job.Type == model.ActionAddPrimaryKey && job.SchemaState == model.StateWriteReorganization {
				idx := findIdxInfo(dom, "test", "t", "primary")
				if idx == nil || idx.BackfillState != model.BackfillStateRunning || job.SnapshotVer == 0 {
					return
				}
				if !backfillDone {
					// Wait another round so that the backfill process is finished, but
					// the info schema is not updated.
					backfillDone = true
					return
				}
				runDML = true
				// Add delete record 4 to the temporary index.
				_, checkErr = tk2.Exec("delete from t where c1 = 4;")
			}
			assert.NoError(t, dom.Reload())
		},
	})
	tk.MustExec("alter table t add primary key idx(c1);")
	dom.DDL().SetHook(originHook)
	require.True(t, backfillDone)
	require.True(t, runDML)
	require.NoError(t, checkErr)
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t use index (primary);").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t ignore index (primary);").Check(testkit.Rows("1 2 3"))
}

func findIdxInfo(dom *domain.Domain, dbName, tbName, idxName string) *model.IndexInfo {
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	if err != nil {
		logutil.BgLogger().Warn("cannot find table", zap.String("dbName", dbName), zap.String("tbName", tbName))
		return nil
	}
	return tbl.Meta().FindIndexByName(idxName)
}
