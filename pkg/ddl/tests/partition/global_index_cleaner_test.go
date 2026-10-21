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
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

// createLeftoverGlobalIndexes adds two copies of the public global index idx_g in different
// non-public states, plus a non-public global index without a public twin, to table test.t.
func createLeftoverGlobalIndexes(t *testing.T, store kv.Storage, dom *domain.Domain, tk *testkit.TestKit) (
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, leftoverIDs []int64, addingID int64) {
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, s int, ct int not null,
		unique key uk_id (id, ct),
		key idx_g (a, s, ct) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 0, 1, 5), (2, 0, 1, 15), (3, 0, 1, 25), (4, 0, 1, 35)")

	tblInfo = external.GetTableByName(t, tk, "test", "t").Meta().Clone()
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	publicGlobal := tblInfo.FindIndexByName("idx_g")
	addIndex := func(name string, state model.SchemaState) int64 {
		idx := publicGlobal.Clone()
		tblInfo.MaxIndexID++
		idx.ID = tblInfo.MaxIndexID
		idx.Name = ast.NewCIStr(name)
		idx.State = state
		tblInfo.Indices = append(tblInfo.Indices, idx)
		return idx.ID
	}
	leftoverIDs = []int64{
		addIndex("idx_g", model.StateDeleteOnly),
		addIndex("idx_g", model.StateWriteReorganization),
	}
	// A non-public global index without a public twin, like one being added, must be kept.
	addingID = addIndex("idx_adding", model.StateWriteReorganization)
	updateTableMeta(t, store, dom, dbInfo.ID, tblInfo)
	require.Len(t, external.GetTableByName(t, tk, "test", "t").Meta().Indices, 5)
	return dbInfo, tblInfo, leftoverIDs, addingID
}

// addIndexCopies adds a copy of the public index of test.t in each given state, like a cancelled
// REORGANIZE PARTITION leaves behind.
func addIndexCopies(t *testing.T, store kv.Storage, dom *domain.Domain, tk *testkit.TestKit, idxName string, states ...model.SchemaState) (
	dbInfo *model.DBInfo, tblInfo *model.TableInfo, copyIDs []int64) {
	tblInfo = external.GetTableByName(t, tk, "test", "t").Meta().Clone()
	dbInfo, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	public := tblInfo.FindIndexByName(idxName)
	require.Equal(t, model.StatePublic, public.State)
	for _, state := range states {
		idx := public.Clone()
		tblInfo.MaxIndexID++
		idx.ID = tblInfo.MaxIndexID
		idx.State = state
		tblInfo.Indices = append(tblInfo.Indices, idx)
		copyIDs = append(copyIDs, idx.ID)
	}
	updateTableMeta(t, store, dom, dbInfo.ID, tblInfo)
	return dbInfo, tblInfo, copyIDs
}

func updateTableMeta(t *testing.T, store kv.Storage, dom *domain.Domain, dbID int64, tblInfo *model.TableInfo) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		if err := m.UpdateTable(dbID, tblInfo); err != nil {
			return err
		}
		ver, err := m.GenSchemaVersion()
		if err != nil {
			return err
		}
		return m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionAddIndex, SchemaID: dbID, TableID: tblInfo.ID})
	}))
	require.NoError(t, dom.Reload())
}

func historyJobID(t *testing.T, tk *testkit.TestKit, tableID int64, jobType string) int64 {
	rows := tk.MustQuery(fmt.Sprintf("select job_id from information_schema.ddl_jobs where table_id = %d and job_type = '%s'", tableID, jobType)).Rows()
	require.Len(t, rows, 1)
	jobID, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	return jobID
}

// deleteRangesOfIndexes returns the job ID and element ID of the delete range of each index.
func deleteRangesOfIndexes(t *testing.T, tk *testkit.TestKit, tableID int64, indexIDs []int64) (jobIDs, elementIDs []int64) {
	for _, id := range indexIDs {
		idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, id))
		rows := tk.MustQuery(fmt.Sprintf(`select job_id, element_id from (select job_id, element_id, start_key from mysql.gc_delete_range
			union all select job_id, element_id, start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Rows()
		require.Len(t, rows, 1)
		jobID, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
		require.NoError(t, err)
		elementID, err := strconv.ParseInt(rows[0][1].(string), 10, 64)
		require.NoError(t, err)
		jobIDs = append(jobIDs, jobID)
		elementIDs = append(elementIDs, elementID)
	}
	return jobIDs, elementIDs
}

func checkLeftoverGlobalIndexesDropped(t *testing.T, tk *testkit.TestKit, tblInfo *model.TableInfo, leftoverIDs []int64, addingID, gcJobID int64) {
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Len(t, after.Indices, 3)
	for _, id := range leftoverIDs {
		require.Nil(t, after.FindIndexByID(id))
	}
	require.NotNil(t, after.FindIndexByID(addingID))
	require.Equal(t, tblInfo.FindIndexByName("idx_g").ID, after.FindIndexByName("idx_g").ID)
	require.Equal(t, model.StatePublic, after.FindIndexByName("idx_g").State)
	tk.MustExec("admin check table t")
	tk.MustQuery("select count(*) from t use index(idx_g) where a >= 0").Check(testkit.Rows("4"))
	jobIDs, _ := deleteRangesOfIndexes(t, tk, tblInfo.ID, leftoverIDs)
	for _, jobID := range jobIDs {
		require.Equal(t, gcJobID, jobID)
	}
	// The GC worker looks up this job and does not complete the delete range if it is missing.
	job, err := ddl.GetHistoryJobByID(tk.Session(), gcJobID)
	require.NoError(t, err)
	require.NotNil(t, job, "delete range job %d has no history job", gcJobID)
}

func TestCleanupLeftoverIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	dbInfo, tblInfo, leftoverIDs, addingID := createLeftoverGlobalIndexes(t, store, dom, tk)
	createJobID := historyJobID(t, tk, tblInfo.ID, "create table")
	// Newer jobs of types not in the allowlist are skipped.
	tk.MustExec("rename table t to t_tmp")
	tk.MustExec("rename table t_tmp to t")

	ctx := context.Background()
	indexIDs, ver, gcJobID, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.Greater(t, ver, int64(0))
	require.Equal(t, createJobID, gcJobID)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo, leftoverIDs, addingID, gcJobID)

	indexIDs, _, gcJobID, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Empty(t, indexIDs)
	require.Zero(t, gcJobID)
}

func TestCleanupLeftoverIndexesElementIDs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("alter table t add index idx_b (b)")
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	addIndexJobID := historyJobID(t, tk, tblID, "add index")

	emptyStart := hex.EncodeToString(tablecodec.EncodeTablePrefix(1 << 40))
	emptyEnd := hex.EncodeToString(tablecodec.EncodeTablePrefix(1<<40 + 1))
	tk.MustExec(fmt.Sprintf("insert ignore into mysql.gc_delete_range values (%d, 1, '%s', '%s', 0)", addIndexJobID, emptyStart, emptyEnd))
	tk.MustExec(fmt.Sprintf("insert ignore into mysql.gc_delete_range_done values (%d, 2, '%s', '%s', 0)", addIndexJobID, emptyStart, emptyEnd))
	jobRanges := func() (cnt int, maxElementID int64) {
		rows := tk.MustQuery(fmt.Sprintf(`select count(*), max(element_id) from (select element_id from mysql.gc_delete_range where job_id = %d
			union all select element_id from mysql.gc_delete_range_done where job_id = %d) r`, addIndexJobID, addIndexJobID)).Rows()
		cnt, err := strconv.Atoi(rows[0][0].(string))
		require.NoError(t, err)
		maxElementID, err = strconv.ParseInt(rows[0][1].(string), 10, 64)
		require.NoError(t, err)
		return cnt, maxElementID
	}
	before, maxElementID := jobRanges()
	require.GreaterOrEqual(t, maxElementID, int64(2))
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization, model.StateDeleteOnly)

	indexIDs, _, gcJobID, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, copyIDs, indexIDs)
	require.Equal(t, addIndexJobID, gcJobID)
	after, _ := jobRanges()
	require.Equal(t, before+len(copyIDs), after)
	jobIDs, elementIDs := deleteRangesOfIndexes(t, tk, tblID, copyIDs)
	require.ElementsMatch(t, []int64{gcJobID, gcJobID}, jobIDs)
	require.ElementsMatch(t, []int64{maxElementID + 1, maxElementID + 2}, elementIDs)
	require.NoError(t, dom.Reload())
	tk.MustExec("admin check table t")
}

func TestCleanupLeftoverIndexesNoGCJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_ddl_history where table_ids = '%d'", tblID))
	tk.MustExec("rename table t to t_tmp")
	tk.MustExec("rename table t_tmp to t")
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	ver := dom.InfoSchema().SchemaMetaVersion()

	indexIDs, _, gcJobID, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.ErrorContains(t, err, "has no finished DDL history job")
	require.Empty(t, indexIDs)
	require.Zero(t, gcJobID)
	require.NoError(t, dom.Reload())
	require.Equal(t, ver, dom.InfoSchema().SchemaMetaVersion())
	require.NotNil(t, external.GetTableByName(t, tk, "test", "t").Meta().FindIndexByID(copyIDs[0]))
	idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tblID, copyIDs[0]))
	tk.MustQuery(fmt.Sprintf(`select count(*) from (select start_key from mysql.gc_delete_range
		union all select start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Check(testkit.Rows("0"))
}
func TestCleanupLeftoverIndexesAfterReorganizePartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	// A successful REORGANIZE PARTITION keeps DDLChangedIndex.
	tk.MustExec("use test")
	tk.MustExec(`create table t2 (a int, b int, unique key uk_b (b) global)
		partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))`)
	tk.MustExec("alter table t2 reorganize partition p0, p1 into (partition p01 values less than (20))")
	pi := external.GetTableByName(t, tk, "test", "t2").Meta().Partition
	require.Equal(t, model.StateNone, pi.DDLState)
	require.Empty(t, pi.AddingDefinitions)
	require.Empty(t, pi.DroppingDefinitions)
	require.NotEmpty(t, pi.DDLChangedIndex)

	dbInfo, tblInfo, leftoverIDs, addingID := createLeftoverGlobalIndexes(t, store, dom, tk)
	tblInfo.Partition.DDLChangedIndex = map[int64]bool{tblInfo.FindIndexByName("idx_g").ID: true}
	updateTableMeta(t, store, dom, dbInfo.ID, tblInfo)

	indexIDs, _, gcJobID, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo, leftoverIDs, addingID, gcJobID)
}

func TestCleanupLeftoverIndexesSkipsUnique(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (id bigint not null, a int, ct int not null,
		unique key uk_g (id) global,
		key idx_g (a) global)
		partition by range (ct) (
		partition p0 values less than (10),
		partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 0, 5), (2, 0, 15)")
	_, _, uniqueCopyIDs := addIndexCopies(t, store, dom, tk, "uk_g", model.StateWriteReorganization)
	dbInfo, tblInfo, leftoverIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)

	indexIDs, _, _, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, indexIDs)
	require.NoError(t, dom.Reload())
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Nil(t, after.FindIndexByID(leftoverIDs[0]))
	require.NotNil(t, after.FindIndexByID(uniqueCopyIDs[0]))
}
