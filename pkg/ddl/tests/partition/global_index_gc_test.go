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

package partition

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestGlobalIndexDeleteRangeColumn(t *testing.T) {
	for _, alter := range []string{"drop column a", "modify column a varchar(20)"} {
		t.Run(alter, func(t *testing.T) {
			store, tk := prepareGlobalIndexGC(t)
			tk.MustExec(`create table t (a int, b int, c int, key g(a) global, key l(a),
				key keep_g(c) global, key keep_l(c)) partition by hash(b) partitions 4`)
			tk.MustExec("insert into t values (1,1,10),(2,2,20),(3,3,30),(4,4,40)")
			old := external.GetTableByName(t, tk, "test", "t").Meta()
			tk.MustExec("alter table t " + alter)
			checkGlobalIndexGCRanges(t, store, tk, old, []*model.IndexInfo{old.FindIndexByName("g"), old.FindIndexByName("l")}, false)
		})
	}
}

func TestGlobalIndexDeleteRangeAddRollback(t *testing.T) {
	for _, state := range []model.SchemaState{model.StateWriteOnly, model.StateWriteReorganization} {
		for _, indexes := range []string{"index g(a) global", "unique index g(a) global", "index g((a+1)) global"} {
			t.Run(fmt.Sprintf("%s/%s", state, indexes), func(t *testing.T) {
				store, tk := prepareGlobalIndexGC(t)
				tk.MustExec(`create table t (a int, b int, c int, key keep_g(c) global, key keep_l(c))
					partition by hash(b) partitions 4`)
				tk.MustExec("insert into t values (1,1,10),(2,2,20),(3,3,30),(4,4,40)")
				tkDML := testkit.NewTestKit(t, store)
				tkDML.MustExec("use test")
				var dropping []*model.IndexInfo
				var old *model.TableInfo
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
					if job.Type != model.ActionMultiSchemaChange || old != nil {
						return
					}
					ready := false
					for _, sub := range job.MultiSchemaInfo.SubJobs {
						if sub.Type == model.ActionAddIndex && sub.SchemaState == state {
							ready = true
						}
					}
					if !ready {
						return
					}
					old = external.GetTableByName(t, tkDML, "test", "t").Meta()
					dropping = []*model.IndexInfo{old.FindIndexByName("g"), old.FindIndexByName("l")}
					// DML writes both global and local temp-index keys before cancellation.
					tkDML.MustExec("insert into t values (5,5,50)")
					tkDML.MustQuery(fmt.Sprintf("admin cancel ddl jobs %d", job.ID)).Check(testkit.Rows(fmt.Sprintf("%d successful", job.ID)))
				})
				tk.MustGetErrCode("alter table t add "+indexes+", add index l(a)", errno.ErrCancelledDDLJob)
				require.NotNil(t, old, "cancellation hook did not run")
				checkGlobalIndexGCRanges(t, store, tk, old, dropping, true)
			})
		}
	}
}

func TestGlobalIndexDeleteRangeAddFailure(t *testing.T) {
	store, tk := prepareGlobalIndexGC(t)
	tk.MustExec(`create table t (a int, b int, c int, key keep_g(c) global, key keep_l(c))
		partition by hash(b) partitions 4`)
	tk.MustExec("insert into t values (1,1,10),(1,2,20),(2,3,30),(2,4,40)")
	var old *model.TableInfo
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && old == nil {
			observer := testkit.NewTestKit(t, store)
			old = external.GetTableByName(t, observer, "test", "t").Meta()
		}
	})
	tk.MustGetErrCode("alter table t add unique index g(a) global", errno.ErrDupEntry)
	require.NotNil(t, old)
	checkGlobalIndexGCRanges(t, store, tk, old, []*model.IndexInfo{old.FindIndexByName("g")}, true)
}

func prepareGlobalIndexGC(t *testing.T) (kv.Storage, *testkit.TestKit) {
	enabled := util.IsEmulatorGCEnable()
	util.EmulatorGCEnable()
	t.Cleanup(func() {
		if enabled {
			util.EmulatorGCEnable()
		} else {
			util.EmulatorGCDisable()
		}
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return store, tk
}

func checkGlobalIndexGCRanges(t *testing.T, store kv.Storage, tk *testkit.TestKit, old *model.TableInfo, indexes []*model.IndexInfo, temp bool) {
	type indexRange struct {
		tableID int64
		indexID int64
	}
	var ranges []indexRange
	for _, idx := range indexes {
		require.NotNil(t, idx)
		physicalIDs := []int64{old.ID}
		if !idx.Global {
			physicalIDs = physicalIDs[:0]
			for _, def := range old.Partition.Definitions {
				physicalIDs = append(physicalIDs, def.ID)
			}
		}
		indexIDs := []int64{idx.ID}
		if temp {
			indexIDs = append(indexIDs, tablecodec.TempIndexPrefix|idx.ID)
		}
		for _, pid := range physicalIDs {
			for _, id := range indexIDs {
				ranges = append(ranges, indexRange{pid, id})
			}
		}
	}
	for _, r := range ranges {
		start := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(r.tableID, r.indexID))
		end := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(r.tableID, r.indexID+1))
		// GC advances start_key as it deletes batches, including in completed ranges.
		tk.MustQuery(`select count(*) from (
			select start_key, end_key from mysql.gc_delete_range union
			select start_key, end_key from mysql.gc_delete_range_done
		) r where start_key >= ? and start_key < ? and end_key = ?`, start, end, end).Check(testkit.Rows("1"))
	}
	rows := tk.MustQuery("select c from t order by c").Rows()
	require.GreaterOrEqual(t, len(rows), 4)
	// The mock store's emulator deletes the actual keys before completing ranges.
	require.Eventually(t, func() bool {
		return tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()[0][0] == "0"
	}, 10*time.Second, 10*time.Millisecond)
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() { require.NoError(t, txn.Rollback()) }()
	for _, r := range ranges {
		start := tablecodec.EncodeTableIndexPrefix(r.tableID, r.indexID)
		end := tablecodec.EncodeTableIndexPrefix(r.tableID, r.indexID+1)
		it, err := txn.Iter(start, end)
		require.NoError(t, err)
		require.False(t, it.Valid(), "obsolete keys for table %d index %d", r.tableID, r.indexID)
		it.Close()
	}
	tk.MustExec("set tidb_enable_fast_table_check = off")
	tk.MustExec("admin check table t")
	tk.MustQuery("select c from t force index(keep_g) order by c").Check(rows)
	tk.MustQuery("select c from t force index(keep_l) order by c").Check(rows)
}
