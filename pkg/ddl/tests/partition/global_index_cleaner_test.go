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
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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

// cancelReorganizePartition runs a REORGANIZE PARTITION on test.t, cancels it in delete only and
// returns its job ID.
func cancelReorganizePartition(t *testing.T, store kv.Storage, tk *testkit.TestKit, alter string) int64 {
	var jobID int64
	tkCancel := testkit.NewTestKit(t, store)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if jobID == 0 && job.Type == model.ActionReorganizePartition && job.SchemaState == model.StateDeleteOnly {
			jobID = job.ID
			tkCancel.MustExec(fmt.Sprintf("admin cancel ddl jobs %d", job.ID))
		}
	})
	tk.MustGetErrCode(alter, errno.ErrCancelledDDLJob)
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	require.NotZero(t, jobID)
	return jobID
}

const reorgPMax = "alter table t reorganize partition pMax into (partition p1 values less than (30), partition pMax values less than (maxvalue))"

func updateHistoryJob(t *testing.T, store kv.Storage, jobID int64, update func(job *model.Job)) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		job, err := m.GetHistoryDDLJob(jobID)
		if err != nil {
			return err
		}
		update(job)
		return m.AddHistoryDDLJob(job, false)
	}))
}

func checkGCJob(t *testing.T, tk *testkit.TestKit, gcJob *model.Job, wantID int64, wantType model.ActionType) {
	require.NotNil(t, gcJob)
	require.Equal(t, wantID, gcJob.ID)
	require.Equal(t, wantType, gcJob.Type)
	// The GC worker looks up this job and does not complete the delete range if it is missing.
	job, err := ddl.GetHistoryJobByID(tk.Session(), gcJob.ID)
	require.NoError(t, err)
	require.NotNil(t, job, "delete range job %d has no history job", gcJob.ID)
}

// checkDeleteRanges checks that each index ID run of the table is queued as one delete range of the job.
func checkDeleteRanges(t *testing.T, tk *testkit.TestKit, tableID, jobID int64, ranges [][2]int64) (elementIDs []int64) {
	for _, r := range ranges {
		startKey := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, r[0]))
		endKey := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tableID, r[1]+1))
		rows := tk.MustQuery(fmt.Sprintf(`select element_id from (select job_id, element_id, start_key, end_key from mysql.gc_delete_range
			union all select job_id, element_id, start_key, end_key from mysql.gc_delete_range_done) r
			where job_id = %d and start_key = '%s' and end_key = '%s'`, jobID, startKey, endKey)).Rows()
		require.Len(t, rows, 1, "range %v", r)
		elementID, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
		require.NoError(t, err)
		elementIDs = append(elementIDs, elementID)
	}
	return elementIDs
}

func countIndexKeys(t *testing.T, store kv.Storage, tableID, indexID int64) int {
	prefix := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
	txn, err := store.Begin()
	require.NoError(t, err)
	defer func() { require.NoError(t, txn.Rollback()) }()
	iter, err := txn.Iter(prefix, prefix.PrefixNext())
	require.NoError(t, err)
	defer iter.Close()
	cnt := 0
	for iter.Valid() {
		cnt++
		require.NoError(t, iter.Next())
	}
	return cnt
}

func checkLeftoverGlobalIndexesDropped(t *testing.T, tk *testkit.TestKit, tblInfo *model.TableInfo, leftoverIDs []int64, addingID int64, res *ddl.LeftoverIndexesCleanup) {
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
	// The copies have consecutive IDs, so they are queued as one delete range.
	require.Equal(t, [][2]int64{{leftoverIDs[0], leftoverIDs[1]}}, res.DeleteRanges)
	checkDeleteRanges(t, tk, tblInfo.ID, res.GCJob.ID, res.DeleteRanges)
}

func TestCleanupLeftoverGlobalIndexesOnOwnerChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, tblInfo, leftoverIDs, addingID := createLeftoverGlobalIndexes(t, store, dom, tk)
	createJobID := historyJobID(t, tk, tblInfo.ID, "create table")

	// The new owner scans the partitioned tables once it is elected.
	require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
	require.Eventually(t, func() bool {
		require.NoError(t, dom.Reload())
		after := external.GetTableByName(t, tk, "test", "t").Meta()
		return after.FindIndexByID(leftoverIDs[0]) == nil && after.FindIndexByID(leftoverIDs[1]) == nil
	}, 30*time.Second, 100*time.Millisecond)
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo, leftoverIDs, addingID, &ddl.LeftoverIndexesCleanup{
		GCJob:        &model.Job{ID: createJobID},
		DeleteRanges: [][2]int64{{leftoverIDs[0], leftoverIDs[1]}},
	})
}

func TestCleanupLeftoverGlobalIndexesOnOwnerChangeTableNotInMeta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	dbInfo, tblInfo, _ := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	// Drop the table from meta without a new schema version, so the new owner still finds it in its infoschema.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).DropTableOrView(dbInfo.ID, tblInfo.ID)
	}))
	require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))

	tk2 := testkit.NewTestKit(t, store)
	done := make(chan error, 1)
	go func() {
		done <- tk2.ExecToErr("create table test.t2 (a int)")
	}()
	var (
		err      error
		finished bool
	)
	select {
	case err = <-done:
		finished = true
	case <-time.After(20 * time.Second):
	}
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).CreateTableOrView(dbInfo.ID, tblInfo)
	}))
	if !finished {
		// Let a new owner run the DDL, so the test can tear down.
		require.NoError(t, dom.DDL().OwnerManager().ResignOwner(context.Background()))
		<-done
	}
	require.True(t, finished, "DDL is not scheduled after the cleanup on election")
	require.NoError(t, err)
}

func TestCleanupLeftoverIndexes(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	dbInfo, tblInfo, leftoverIDs, addingID := createLeftoverGlobalIndexes(t, store, dom, tk)
	createJobID := historyJobID(t, tk, tblInfo.ID, "create table")
	// Newer jobs of other types are skipped.
	tk.MustExec("rename table t to t_tmp")
	tk.MustExec("rename table t_tmp to t")

	ctx := context.Background()
	res, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, res.IndexIDs)
	require.Greater(t, res.SchemaVersion, int64(0))
	checkGCJob(t, tk, res.GCJob, createJobID, model.ActionCreateTable)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo, leftoverIDs, addingID, res)

	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Empty(t, res.IndexIDs)
	require.Nil(t, res.GCJob)
	require.Empty(t, res.DeleteRanges)
}

func TestCleanupLeftoverIndexesDeleteRanges(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global)
		partition by range (b) (partition p0 values less than (10), partition pMax values less than (maxvalue))`)
	_, _, copies1 := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	tk.MustExec("alter table t add index idx_dropped (b)")
	tk.MustExec("alter table t drop index idx_dropped")
	_, _, copies2 := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	tk.MustExec("alter table t add index idx_mid (b) global")
	dbInfo, tblInfo, copies3 := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	midID := tblInfo.FindIndexByName("idx_mid").ID
	require.Equal(t, copies1[0]+2, copies2[0])
	require.Equal(t, copies2[0]+1, midID)
	require.Equal(t, midID+1, copies3[0])
	tk.MustExec("insert into t values (1, 1), (2, 20), (3, 30)")
	for _, id := range []int64{copies1[0], copies2[0], midID, copies3[0]} {
		require.Equal(t, 3, countIndexKeys(t, store, tblInfo.ID, id))
	}

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{copies1[0], copies2[0], copies3[0]}, res.IndexIDs)
	// The dropped idx_dropped does not split a run, while idx_mid, which stays, does.
	require.Equal(t, [][2]int64{{copies1[0], copies2[0]}, {copies3[0], copies3[0]}}, res.DeleteRanges)
	checkDeleteRanges(t, tk, tblInfo.ID, res.GCJob.ID, res.DeleteRanges)
	require.NoError(t, dom.Reload())

	// The delRange emulator of mockstore runs when a DDL job adds delete ranges.
	tk.MustExec("create table t_trigger (a int)")
	tk.MustExec("drop table t_trigger")
	require.Eventually(t, func() bool {
		return tk.MustQuery(fmt.Sprintf("select count(*) from mysql.gc_delete_range where job_id = %d", res.GCJob.ID)).Rows()[0][0] == "0"
	}, 30*time.Second, 100*time.Millisecond)
	for _, id := range res.IndexIDs {
		require.Zero(t, countIndexKeys(t, store, tblInfo.ID, id))
	}
	require.Equal(t, 3, countIndexKeys(t, store, tblInfo.ID, midID))
	tk.MustExec("admin check table t")
	tk.MustQuery("select count(*) from t use index(idx_mid)").Check(testkit.Rows("3"))
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

	jobRanges := func() (cnt int, maxElementID int64) {
		rows := tk.MustQuery(fmt.Sprintf(`select count(*), ifnull(max(element_id), 0) from (select element_id from mysql.gc_delete_range where job_id = %d
			union all select element_id from mysql.gc_delete_range_done where job_id = %d) r`, addIndexJobID, addIndexJobID)).Rows()
		cnt, err := strconv.Atoi(rows[0][0].(string))
		require.NoError(t, err)
		maxElementID, err = strconv.ParseInt(rows[0][1].(string), 10, 64)
		require.NoError(t, err)
		return cnt, maxElementID
	}
	_, maxElementID := jobRanges()
	emptyStart := hex.EncodeToString(tablecodec.EncodeTablePrefix(1 << 40))
	emptyEnd := hex.EncodeToString(tablecodec.EncodeTablePrefix(1<<40 + 1))
	tk.MustExec(fmt.Sprintf("insert into mysql.gc_delete_range values (%d, %d, '%s', '%s', 0)", addIndexJobID, maxElementID+1, emptyStart, emptyEnd))
	tk.MustExec(fmt.Sprintf("insert into mysql.gc_delete_range_done values (%d, %d, '%s', '%s', 0)", addIndexJobID, maxElementID+2, emptyStart, emptyEnd))
	before, maxElementID := jobRanges()
	_, _, copies1 := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	tk.MustExec("alter table t add index idx_mid (b) global")
	dbInfo, tblInfo, copies2 := addIndexCopies(t, store, dom, tk, "idx_g", model.StateDeleteOnly)
	// Keep the ADD INDEX of idx_b the newest usable job.
	tk.MustExec(fmt.Sprintf("delete from mysql.tidb_ddl_history where table_ids = '%d' and job_id > %d", tblID, addIndexJobID))

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{copies1[0], copies2[0]}, res.IndexIDs)
	checkGCJob(t, tk, res.GCJob, addIndexJobID, model.ActionAddIndex)
	require.Len(t, res.DeleteRanges, 2)
	after, _ := jobRanges()
	require.Equal(t, before+len(res.DeleteRanges), after)
	elementIDs := checkDeleteRanges(t, tk, tblID, res.GCJob.ID, res.DeleteRanges)
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

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.ErrorContains(t, err, "no DDL history job")
	require.Nil(t, res)
	require.NoError(t, dom.Reload())
	require.Equal(t, ver, dom.InfoSchema().SchemaMetaVersion())
	require.NotNil(t, external.GetTableByName(t, tk, "test", "t").Meta().FindIndexByID(copyIDs[0]))
	idxPrefix := hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(tblID, copyIDs[0]))
	tk.MustQuery(fmt.Sprintf(`select count(*) from (select start_key from mysql.gc_delete_range
		union all select start_key from mysql.gc_delete_range_done) r where start_key = '%s'`, idxPrefix)).Check(testkit.Rows("0"))
}

func TestCleanupLeftoverIndexesNewestJob(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global)
		partition by range (b) (partition p0 values less than (10), partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 1), (2, 20)")
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	reorgJobID := cancelReorganizePartition(t, store, tk, reorgPMax)
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	ctx := context.Background()

	// A rolled back REORGANIZE PARTITION newer than CREATE TABLE is used.
	res, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Subset(t, res.IndexIDs, copyIDs)
	checkGCJob(t, tk, res.GCJob, reorgJobID, model.ActionReorganizePartition)
	require.True(t, res.GCJob.IsRollbackDone())

	// Newer jobs of other types are skipped.
	tk.MustExec("rename table t to t_tmp")
	tk.MustExec("rename table t_tmp to t")
	_, _, copyIDs = addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblID)
	require.NoError(t, err)
	require.Equal(t, copyIDs, res.IndexIDs)
	checkGCJob(t, tk, res.GCJob, reorgJobID, model.ActionReorganizePartition)

	// A newer job of a type GC changes no rules for is used.
	tk.MustExec("alter table t add column c int")
	addColumnJobID := historyJobID(t, tk, tblID, "add column")
	_, _, copyIDs = addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblID)
	require.NoError(t, err)
	require.Equal(t, copyIDs, res.IndexIDs)
	checkGCJob(t, tk, res.GCJob, addColumnJobID, model.ActionAddColumn)
}

func TestCleanupLeftoverIndexesHistoryBatches(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	createJobID := historyJobID(t, tk, tblID, "create table")
	// Rows without a history job in meta are skipped, and they span more than one batch.
	for i := range 70 {
		tk.MustExec(fmt.Sprintf("insert into mysql.tidb_ddl_history (job_id, table_ids) values (%d, '%d')", createJobID+1000+int64(i), tblID))
	}
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Equal(t, copyIDs, res.IndexIDs)
	checkGCJob(t, tk, res.GCJob, createJobID, model.ActionCreateTable)
}

func TestCleanupLeftoverIndexesRolledBackReorg(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global)
		partition by range (b) (partition p0 values less than (10), partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 1), (2, 20)")
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	createJobID := historyJobID(t, tk, tblID, "create table")
	reorgJobID := cancelReorganizePartition(t, store, tk, reorgPMax)
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	ctx := context.Background()

	// GC decodes the args of a rolled back REORGANIZE PARTITION, so an older job is used when they cannot be decoded.
	var rawArgs []byte
	updateHistoryJob(t, store, reorgJobID, func(job *model.Job) {
		rawArgs = job.RawArgs
		job.RawArgs = []byte("[1]")
	})
	res, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Subset(t, res.IndexIDs, copyIDs)
	checkGCJob(t, tk, res.GCJob, createJobID, model.ActionCreateTable)
	checkDeleteRanges(t, tk, tblID, createJobID, res.DeleteRanges)
	require.NoError(t, dom.Reload())

	updateHistoryJob(t, store, reorgJobID, func(job *model.Job) {
		job.RawArgs = rawArgs
	})
	_, _, copyIDs = addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Equal(t, copyIDs, res.IndexIDs)
	checkGCJob(t, tk, res.GCJob, reorgJobID, model.ActionReorganizePartition)
	checkDeleteRanges(t, tk, tblID, reorgJobID, res.DeleteRanges)
	require.NoError(t, dom.Reload())
	tk.MustExec("admin check table t")
}

func TestCleanupLeftoverIndexesTableNotExists(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	dbInfo, tblInfo, _ := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	ctx := context.Background()

	tk.MustExec("create database test2")
	tk.MustExec("rename table t to test2.t")
	res, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.True(t, infoschema.ErrTableNotExists.Equal(err), "%v", err)
	require.Nil(t, res)

	db2, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test2"))
	require.True(t, ok)
	tk.MustExec("truncate table test2.t")
	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), db2.ID, tblInfo.ID)
	require.True(t, infoschema.ErrTableNotExists.Equal(err), "%v", err)
	require.Nil(t, res)

	res, err = ddl.CleanupLeftoverIndexes(ctx, tk.Session(), 1<<40, tblInfo.ID)
	require.True(t, infoschema.ErrDatabaseNotExists.Equal(err), "%v", err)
	require.Nil(t, res)
}

func TestCleanupLeftoverIndexesSchemaDiffInFlight(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tkClean := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global) partition by hash (b) partitions 2`)
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)
	tk.MustExec("create table t2 (a int, b int)")
	ctx := context.Background()

	var (
		cleanupErr        error
		fired, loadedStep bool
	)
	// The step of ADD INDEX has allocated its schema version but not committed its schema diff.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, _ *error) {
		if fired || job.Type != model.ActionAddIndex || job.SchemaState != model.StateDeleteOnly {
			return
		}
		fired = true
		_, cleanupErr = ddl.CleanupLeftoverIndexes(ctx, tkClean.Session(), dbInfo.ID, tblInfo.ID)
		// Another node loads the schema before the step commits.
		_ = dom.Reload()
	})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeWaitSchemaSynced", func(job *model.Job, _ int64) {
		if loadedStep || job.Type != model.ActionAddIndex || job.SchemaState != model.StateDeleteOnly {
			return
		}
		if dom.Reload() != nil {
			return
		}
		tbl, ok := dom.InfoSchema().TableByID(ctx, job.TableID)
		loadedStep = ok && tbl.Meta().FindIndexByName("i2") != nil
	})
	tk.MustExec("alter table t2 add index i2 (a)")
	require.True(t, fired)
	require.ErrorContains(t, cleanupErr, "has no schema diff yet")
	require.True(t, loadedStep, "the schema loader skipped the schema diff of the in-flight step")
	tk.MustExec("admin check table t2")

	res, err := ddl.CleanupLeftoverIndexes(ctx, tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Equal(t, copyIDs, res.IndexIDs)
}

func TestCleanupLeftoverIndexesSkipsPartitionBy(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, key idx_g (a) global)
		partition by range (b) (partition p0 values less than (10), partition pMax values less than (maxvalue))`)
	tk.MustExec("insert into t values (1, 1), (2, 20)")
	oldTblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	tk.MustExec(`alter table t partition by range (b) (partition p0 values less than (5), partition pMax values less than (maxvalue))`)
	tblID := external.GetTableByName(t, tk, "test", "t").Meta().ID
	require.NotEqual(t, oldTblID, tblID)
	partitionByJobID := historyJobID(t, tk, oldTblID, "alter table partition by")
	// Make the PARTITION BY job a candidate of the new table ID.
	tk.MustExec(fmt.Sprintf("update mysql.tidb_ddl_history set table_ids = '%d' where job_id = %d", tblID, partitionByJobID))
	dbInfo, tblInfo, copyIDs := addIndexCopies(t, store, dom, tk, "idx_g", model.StateWriteReorganization)

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.ErrorContains(t, err, "no DDL history job")
	require.Nil(t, res)

	reorgJobID := cancelReorganizePartition(t, store, tk, reorgPMax)
	require.Greater(t, reorgJobID, partitionByJobID)
	res, err = ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.Subset(t, res.IndexIDs, copyIDs)
	checkGCJob(t, tk, res.GCJob, reorgJobID, model.ActionReorganizePartition)
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

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, res.IndexIDs)
	require.NoError(t, dom.Reload())
	checkLeftoverGlobalIndexesDropped(t, tk, tblInfo, leftoverIDs, addingID, res)
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

	res, err := ddl.CleanupLeftoverIndexes(context.Background(), tk.Session(), dbInfo.ID, tblInfo.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, leftoverIDs, res.IndexIDs)
	require.Equal(t, [][2]int64{{leftoverIDs[0], leftoverIDs[0]}}, res.DeleteRanges)
	require.NoError(t, dom.Reload())
	after := external.GetTableByName(t, tk, "test", "t").Meta()
	require.Nil(t, after.FindIndexByID(leftoverIDs[0]))
	require.NotNil(t, after.FindIndexByID(uniqueCopyIDs[0]))
}
