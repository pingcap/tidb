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
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAddIndexMergeProcess(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int, c3 int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6);")
	// Force onCreateIndex use the txn-merge process.
	ingest.LitInitialized = false
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
	// Disable auto schema reload.
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 0)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6);")
	// Force onCreateIndex use the backfill-merge process.
	ingest.LitInitialized = false
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

func TestAddIndexMergeVersionIndexValue(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk.MustExec("create table t (c1 int);")
	// Force onCreateIndex use the txn-merge process.
	ingest.LitInitialized = false
	tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg = 1;")

	var checkErr error
	var runDML bool
	var tblID, idxID int64
	originHook := dom.DDL().GetHook()
	dom.DDL().SetHook(&ddl.TestDDLCallback{
		Do: dom,
		OnJobUpdatedExported: func(job *model.Job) {
			if !runDML && job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization {
				idx := findIdxInfo(dom, "test", "t", "idx")
				if idx == nil || idx.BackfillState != model.BackfillStateReadyToMerge {
					return
				}
				runDML = true
				tblID = job.TableID
				idxID = idx.ID
				_, checkErr = tk2.Exec("insert into t values (1);")
			}
		},
	})
	tk.MustExec("alter table t add unique index idx(c1);")
	dom.DDL().SetHook(originHook)
	require.True(t, runDML)
	require.NoError(t, checkErr)
	tk.MustExec("admin check table t;")
	tk.MustQuery("select * from t use index (idx);").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t ignore index (idx);").Check(testkit.Rows("1"))

	snap := store.GetSnapshot(kv.MaxVersion)
	iter, err := snap.Iter(tablecodec.GetTableIndexKeyRange(tblID, idxID))
	require.NoError(t, err)
	require.True(t, iter.Valid())
	// The origin index value should not have 'm' version appended.
	require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}, iter.Value())
}

func findIdxInfo(dom *domain.Domain, dbName, tbName, idxName string) *model.IndexInfo {
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tbName))
	if err != nil {
		logutil.BgLogger().Warn("cannot find table", zap.String("dbName", dbName), zap.String("tbName", tbName))
		return nil
	}
	return tbl.Meta().FindIndexByName(idxName)
}
