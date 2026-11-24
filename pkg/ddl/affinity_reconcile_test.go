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

package ddl_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

type reconcilePDClient struct {
	pdhttp.Client
	mu              sync.Mutex
	groups          map[string]*pdhttp.AffinityGroupState
	getAllErr       error
	createErr       error
	deleteErr       error
	deleteErrOnceOn string
	deleteBatches   [][]string
	onAfterCreate   func(ids []string)
	onBeforeCreate  func(ids []string)
	onBeforeDelete  func(ids []string)
}

func newReconcilePDClient() *reconcilePDClient {
	return &reconcilePDClient{
		groups: make(map[string]*pdhttp.AffinityGroupState),
	}
}

func (c *reconcilePDClient) seedGroup(id string, createTS uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groups[id] = &pdhttp.AffinityGroupState{
		AffinityGroup: pdhttp.AffinityGroup{
			ID:              id,
			CreateTimestamp: createTS,
		},
		RangeCount: 1,
	}
}

func (c *reconcilePDClient) hasGroup(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.groups[id]
	return ok
}

func (c *reconcilePDClient) deleteGroupDirect(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.groups, id)
}

func (c *reconcilePDClient) CreateAffinityGroups(_ context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, _ ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	c.mu.Lock()
	beforeHook := c.onBeforeCreate
	c.mu.Unlock()
	if beforeHook != nil {
		beforeHook(ids)
	}

	c.mu.Lock()
	if c.createErr != nil {
		err := c.createErr
		c.mu.Unlock()
		return nil, err
	}
	now := uint64(time.Now().Unix())
	res := make(map[string]*pdhttp.AffinityGroupState, len(groups))
	for id, ranges := range groups {
		if _, exists := c.groups[id]; !exists {
			c.groups[id] = &pdhttp.AffinityGroupState{
				AffinityGroup: pdhttp.AffinityGroup{
					ID:              id,
					CreateTimestamp: now,
				},
				RangeCount: len(ranges),
			}
		}
		res[id] = c.groups[id]
	}
	afterHook := c.onAfterCreate
	c.mu.Unlock()

	if afterHook != nil {
		afterHook(ids)
	}
	return res, nil
}

func (c *reconcilePDClient) BatchDeleteAffinityGroups(_ context.Context, ids []string, _ bool) error {
	c.mu.Lock()
	beforeHook := c.onBeforeDelete
	c.mu.Unlock()
	if beforeHook != nil {
		beforeHook(slices.Clone(ids))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteBatches = append(c.deleteBatches, slices.Clone(ids))
	if c.deleteErr != nil {
		return c.deleteErr
	}
	if c.deleteErrOnceOn != "" {
		for _, id := range ids {
			if id == c.deleteErrOnceOn {
				c.deleteErrOnceOn = ""
				return fmt.Errorf("injected batch delete failure on %s", id)
			}
		}
	}
	for _, id := range ids {
		delete(c.groups, id)
	}
	return nil
}

func (c *reconcilePDClient) GetAffinityGroups(_ context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make(map[string]*pdhttp.AffinityGroupState, len(ids))
	for _, id := range ids {
		if st, ok := c.groups[id]; ok {
			copied := *st
			res[id] = &copied
		}
	}
	return res, nil
}

func (c *reconcilePDClient) GetAllAffinityGroups(_ context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getAllErr != nil {
		return nil, c.getAllErr
	}
	res := make(map[string]*pdhttp.AffinityGroupState, len(c.groups))
	for id, st := range c.groups {
		copied := *st
		res[id] = &copied
	}
	return res, nil
}

type mockReloadErrLoader struct {
	err error
}

func (m mockReloadErrLoader) Reload() error {
	return m.err
}

func insertFakeDDLJob(t *testing.T, tk *testkit.TestKit, job *model.Job) {
	t.Helper()
	raw, err := job.Encode(true)
	require.NoError(t, err)
	tk.MustExec("insert into mysql.tidb_ddl_job (job_id, reorg, schema_ids, table_ids, job_meta, type, processing) values (?, 0, ?, ?, ?, ?, 0)",
		job.ID, fmt.Sprint(job.SchemaID), fmt.Sprint(job.TableID), raw, int(job.Type))
}

func deleteFakeDDLJob(t *testing.T, tk *testkit.TestKit, jobID int64) {
	t.Helper()
	tk.MustExec("delete from mysql.tidb_ddl_job where job_id = ?", jobID)
}

func TestAffinityReconcileRepairAndUnsupportedPD(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int) affinity='table'")
	tk.MustExec("create table tp1(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")

	t1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tp1, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp1"))
	require.NoError(t, err)

	codec := store.GetCodec()
	t1Group := ddl.GetTableAffinityGroupID(codec, t1.Meta().ID)
	p0Group := ddl.GetPartitionAffinityGroupID(codec, tp1.Meta().ID, tp1.Meta().Partition.Definitions[0].ID)
	p1Group := ddl.GetPartitionAffinityGroupID(codec, tp1.Meta().ID, tp1.Meta().Partition.Definitions[1].ID)

	// Simulate PD losing groups; reconcile must recreate them.
	cli.deleteGroupDirect(t1Group)
	cli.deleteGroupDirect(p0Group)
	require.False(t, cli.hasGroup(t1Group))
	require.False(t, cli.hasGroup(p0Group))
	require.True(t, cli.hasGroup(p1Group))

	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(t1Group))
	require.True(t, cli.hasGroup(p0Group))
	require.True(t, cli.hasGroup(p1Group))

	// Old PD returning 404 unsupported must be silently skipped without error.
	cli.mu.Lock()
	cli.getAllErr = fmt.Errorf("request pd http api failed with status: '404 Not Found', body: '404 page not found'")
	cli.mu.Unlock()
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
}

func TestAffinityReconcileTwoRoundGracePeriodOwnerSwitchAndBatching(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	baseNow := time.Unix(1_700_000_000, 0)
	curNow := baseNow
	grace := 5 * time.Minute
	batchSize := 2
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &grace,
		BatchSize:   &batchSize,
		Now:         func() time.Time { return curNow },
	})
	t.Cleanup(restore)

	// Seed 3 old orphans + 1 fresh orphan (within grace period).
	oldOrphans := []string{
		ddl.GetTableAffinityGroupID(codec, 900001),
		ddl.GetTableAffinityGroupID(codec, 900002),
		ddl.GetTableAffinityGroupID(codec, 900003),
	}
	for _, id := range oldOrphans {
		cli.seedGroup(id, uint64(baseNow.Add(-10*time.Minute).Unix()))
	}
	freshOrphan := ddl.GetTableAffinityGroupID(codec, 900004)
	cli.seedGroup(freshOrphan, uint64(baseNow.Add(-1*time.Minute).Unix()))

	// Round 1: old orphans recorded as suspect, none deleted yet.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 3, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	for _, id := range oldOrphans {
		require.True(t, cli.hasGroup(id))
	}
	require.True(t, cli.hasGroup(freshOrphan))

	// Owner retirement clears suspectOrphans, so the next round is treated as Round 1 again.
	ddl.ClearAffinityReconcilerForTest(dom.DDL())
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 3, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	for _, id := range oldOrphans {
		require.True(t, cli.hasGroup(id))
	}

	// Round 2 with batch failure on the first batch (size 2): batch 1 stays in suspectOrphans, batch 2 succeeds.
	cli.mu.Lock()
	cli.deleteErrOnceOn = oldOrphans[0]
	cli.mu.Unlock()
	err := ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session())
	require.Error(t, err)
	cli.mu.Lock()
	require.Equal(t, [][]string{{oldOrphans[0], oldOrphans[1]}, {oldOrphans[2]}}, cli.deleteBatches)
	cli.mu.Unlock()
	require.True(t, cli.hasGroup(oldOrphans[0]))
	require.True(t, cli.hasGroup(oldOrphans[1]))
	require.False(t, cli.hasGroup(oldOrphans[2]))
	require.Equal(t, 2, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	require.True(t, cli.hasGroup(freshOrphan))

	// Round 3: failed batch retries immediately and succeeds.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(oldOrphans[0]))
	require.False(t, cli.hasGroup(oldOrphans[1]))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	// Fresh orphan is still protected by grace period.
	require.True(t, cli.hasGroup(freshOrphan))

	// Advance clock past grace period: takes 2 consecutive rounds to delete freshOrphan.
	curNow = baseNow.Add(6 * time.Minute)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(freshOrphan))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(freshOrphan))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
}

func TestAffinityReconcileKeyspaceIsolation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	ks42, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)
	ks43, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 43},
	})
	require.NoError(t, err)

	zeroGrace := time.Duration(0)
	cfg := ddl.AffinityReconcilerConfigForTest{GracePeriod: &zeroGrace}
	activeCodec := store.GetCodec()
	if kerneltype.IsNextGen() {
		cfg.Codec = ks42
		activeCodec = ks42
	}
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), cfg)
	t.Cleanup(restore)

	ownOrphanTable := ddl.GetTableAffinityGroupID(activeCodec, 880001)
	ownOrphanPart := ddl.GetPartitionAffinityGroupID(activeCodec, 880002, 880003)
	thirdParty := "custom_app_group_1"
	otherClassic := "_tidb_t_880004"
	otherKS := "_tidb_ks43_t_880005"
	if kerneltype.IsNextGen() {
		otherKS = ddl.GetTableAffinityGroupID(ks43, 880005)
	}

	cli.seedGroup(ownOrphanTable, 0)
	cli.seedGroup(ownOrphanPart, 0)
	cli.seedGroup(thirdParty, 0)
	cli.seedGroup(otherKS, 0)
	if kerneltype.IsNextGen() {
		cli.seedGroup(otherClassic, 0)
	} else {
		// In Classic mode, NextGen-prefixed groups must never be touched.
		cli.seedGroup("_tidb_ks42_t_880006", 0)
	}

	// Two rounds delete ONLY current namespace orphans.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))

	require.False(t, cli.hasGroup(ownOrphanTable))
	require.False(t, cli.hasGroup(ownOrphanPart))
	require.True(t, cli.hasGroup(thirdParty), "third-party group must never be deleted")
	require.True(t, cli.hasGroup(otherKS), "other keyspace group must never be deleted")
	if kerneltype.IsNextGen() {
		require.True(t, cli.hasGroup(otherClassic), "classic group must never be deleted by NextGen keyspace")
	} else {
		require.True(t, cli.hasGroup("_tidb_ks42_t_880006"), "NextGen keyspace group must never be deleted by Classic TiDB")
	}
}

func TestAffinityReconcileProtectsPendingGCDeleteRange(t *testing.T) {
	ddlutil.EmulatorGCDisable()
	t.Cleanup(ddlutil.EmulatorGCEnable)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	// Table and partition in mysql.gc_delete_range (pending GC).
	pendingTblID := int64(910001)
	pendingPartParentID := int64(910002)
	pendingPartID := int64(910003)
	// Table only in mysql.gc_delete_range_done (already GC'ed, NOT protected).
	doneTblID := int64(910004)

	tk.MustExec("insert into mysql.gc_delete_range (job_id, element_id, start_key, end_key, ts) values (?, ?, ?, ?, ?)",
		99001, 1, hex.EncodeToString(tablecodec.EncodeTablePrefix(pendingTblID)), hex.EncodeToString(tablecodec.EncodeTablePrefix(pendingTblID+1)), uint64(100))
	tk.MustExec("insert into mysql.gc_delete_range (job_id, element_id, start_key, end_key, ts) values (?, ?, ?, ?, ?)",
		99002, 1, hex.EncodeToString(tablecodec.EncodeTablePrefix(pendingPartID)), hex.EncodeToString(tablecodec.EncodeTablePrefix(pendingPartID+1)), uint64(100))
	tk.MustExec("insert into mysql.gc_delete_range_done (job_id, element_id, start_key, end_key, ts) values (?, ?, ?, ?, ?)",
		99003, 1, hex.EncodeToString(tablecodec.EncodeTablePrefix(doneTblID)), hex.EncodeToString(tablecodec.EncodeTablePrefix(doneTblID+1)), uint64(100))
	t.Cleanup(func() {
		tk.MustExec("delete from mysql.gc_delete_range where job_id in (99001, 99002)")
		tk.MustExec("delete from mysql.gc_delete_range_done where job_id = 99003")
	})

	pendingTblGroup := ddl.GetTableAffinityGroupID(codec, pendingTblID)
	pendingPartGroup := ddl.GetPartitionAffinityGroupID(codec, pendingPartParentID, pendingPartID)
	doneTblGroup := ddl.GetTableAffinityGroupID(codec, doneTblID)
	cli.seedGroup(pendingTblGroup, 0)
	cli.seedGroup(pendingPartGroup, 0)
	cli.seedGroup(doneTblGroup, 0)

	// Two rounds: pending GC groups stay protected; gc_delete_range_done group is deleted.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))

	require.True(t, cli.hasGroup(pendingTblGroup))
	require.True(t, cli.hasGroup(pendingPartGroup))
	require.False(t, cli.hasGroup(doneTblGroup))
}

func TestAffinityReconcileProtectsInFlightDDLAndConcurrentCommit(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
		IsOwner:     func() bool { return true },
	})
	t.Cleanup(restore)

	// Stop DDL owner scheduler so our synthetic jobs in mysql.tidb_ddl_job stay in-flight.
	require.NoError(t, dom.DDL().Stop())

	// 1. In-flight CREATE TABLE, TRUNCATE TABLE, TRUNCATE PARTITION, ALTER AFFINITY, RECOVER TABLE.
	createGroup := ddl.GetTableAffinityGroupID(codec, 920001)
	truncNewGroup := ddl.GetTableAffinityGroupID(codec, 920003)
	truncPartNewGroup := ddl.GetPartitionAffinityGroupID(codec, 920004, 920005)
	recoverGroup := ddl.GetTableAffinityGroupID(codec, 920006)
	realOrphan := ddl.GetTableAffinityGroupID(codec, 920099)

	for _, id := range []string{createGroup, truncNewGroup, truncPartNewGroup, recoverGroup, realOrphan} {
		cli.seedGroup(id, 0)
	}

	jCreate := &model.Job{ID: 880001, Version: model.JobVersion2, Type: model.ActionCreateTable, SchemaID: 1, TableID: 920001}
	jCreate.FillArgs(&model.CreateTableArgs{TableInfo: &model.TableInfo{ID: 920001}})
	jTrunc := &model.Job{ID: 880002, Version: model.JobVersion2, Type: model.ActionTruncateTable, SchemaID: 1, TableID: 920002}
	jTrunc.FillArgs(&model.TruncateTableArgs{NewTableID: 920003})
	jTruncPart := &model.Job{ID: 880003, Version: model.JobVersion2, Type: model.ActionTruncateTablePartition, SchemaID: 1, TableID: 920004}
	jTruncPart.FillArgs(&model.TruncateTableArgs{NewPartitionIDs: []int64{920005}, OldPartitionIDs: []int64{920015}})
	jRecover := &model.Job{ID: 880004, Version: model.JobVersion2, Type: model.ActionRecoverTable, SchemaID: 1, TableID: 920006}
	jRecover.FillArgs(&model.RecoverArgs{RecoverInfo: &model.RecoverSchemaInfo{
		RecoverTableInfos: []*model.RecoverTableInfo{{TableInfo: &model.TableInfo{ID: 920006}}},
	}})

	for _, j := range []*model.Job{jCreate, jTrunc, jTruncPart, jRecover} {
		insertFakeDDLJob(t, tk, j)
		id := j.ID
		t.Cleanup(func() { deleteFakeDDLJob(t, tk, id) })
	}

	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))

	require.True(t, cli.hasGroup(createGroup))
	require.True(t, cli.hasGroup(truncNewGroup))
	require.True(t, cli.hasGroup(truncPartNewGroup))
	require.True(t, cli.hasGroup(recoverGroup))
	require.False(t, cli.hasGroup(realOrphan))

	// 2. FlashbackCluster skips the entire round (neither creates nor deletes).
	flashbackOrphan := ddl.GetTableAffinityGroupID(codec, 920100)
	cli.seedGroup(flashbackOrphan, 0)
	jFlashback := &model.Job{ID: 880005, Version: model.JobVersion2, Type: model.ActionFlashbackCluster}
	insertFakeDDLJob(t, tk, jFlashback)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(flashbackOrphan))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	deleteFakeDDLJob(t, tk, jFlashback.ID)
}

func TestAffinityReconcileReverseLifecycleConcurrentCommit(t *testing.T) {
	ddlutil.EmulatorGCDisable()
	t.Cleanup(ddlutil.EmulatorGCEnable)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	// Scenario B: DROP TABLE commits between T2 (tidb_ddl_job) and T3 (InfoSchema) ->
	// its group was in PD at T1, not in InfoSchema at T3, and T4 gc_delete_range protects it.
	tk.MustExec("create table t_drop_race(a int) affinity='table'")
	tDrop, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop_race"))
	require.NoError(t, err)
	dropGroupID := ddl.GetTableAffinityGroupID(codec, tDrop.Meta().ID)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	var droppedOnce atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAffinityReconcileReadDDLJobs", func() {
		if droppedOnce.CompareAndSwap(false, true) {
			tk2.MustExec("drop table t_drop_race")
		}
	})

	// Run two rounds: even though DROP committed between T2 and T3, T4 reads gc_delete_range and protects it.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, droppedOnce.Load())
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(dropGroupID), "table dropped between T2 and T3 must stay protected by T4 gc_delete_range")
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Scenario A (CR 1): CREATE TABLE creates its PD group and pauses BEFORE the
	// job finishes; reconciler reads that group in its FIRST PD snapshot (T1),
	// then the hook releases CREATE TABLE and waits for it to finish BEFORE T2
	// (tidb_ddl_job) so the job is gone by T2 and T3 InfoSchema must protect it.
	pdGroupCreatedCh := make(chan string, 1)
	releaseCreateCh := make(chan struct{})
	createDoneCh := make(chan error, 1)
	var pausedOnce atomic.Bool
	cli.mu.Lock()
	cli.onAfterCreate = func(ids []string) {
		if pausedOnce.CompareAndSwap(false, true) && len(ids) == 1 {
			pdGroupCreatedCh <- ids[0]
			<-releaseCreateCh
		}
	}
	cli.mu.Unlock()
	t.Cleanup(func() {
		cli.mu.Lock()
		cli.onAfterCreate = nil
		cli.mu.Unlock()
	})

	go func() {
		_, execErr := tk2.Exec("create table t_create_race(a int) affinity='table'")
		createDoneCh <- execErr
	}()

	createGroupID := <-pdGroupCreatedCh
	require.True(t, cli.hasGroup(createGroupID), "PD group must exist before T1 snapshot")

	var releasedOnce atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAffinityReconcileReadPD", func() {
		if releasedOnce.CompareAndSwap(false, true) {
			close(releaseCreateCh)
			require.NoError(t, <-createDoneCh)
		}
	})

	// Round 1 sees createGroupID in T1, CREATE finishes before T2, and T3 protects it.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, releasedOnce.Load())
	require.True(t, cli.hasGroup(createGroupID), "table created before T2 must stay protected by T3 InfoSchema")
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()), "round 1 must NOT mark the group suspect")

	// Round 2 also keeps the group intact.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(createGroupID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
}

func TestAffinityReconcileFailClosedAndAlterFallback(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	// Pre-seed a confirmed suspect orphan (1 round already observed).
	orphanID := ddl.GetTableAffinityGroupID(codec, 930001)
	cli.seedGroup(orphanID, 0)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// 1. tidb_ddl_job query error -> fail-closed, 0 deletions, invalidates prior suspects.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileGetDDLJobsErr", "return(true)")
	require.Error(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileGetDDLJobsErr")
	require.True(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Re-observe orphanID in round 1 before testing reload failure.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// 2. schemaLoader.Reload() error -> fail-closed, 0 deletions, invalidates prior suspects.
	restoreLoader := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		SchemaLoader: mockReloadErrLoader{err: errors.New("injected reload failure")},
	})
	require.Error(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	restoreLoader()
	require.True(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Re-observe orphanID in round 1 before testing gc_delete_range failure.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// 3. gc_delete_range query error -> fail-closed, 0 deletions, invalidates prior suspects.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr", "return(true)")
	require.Error(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr")
	require.True(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// 4. ALTER TABLE ... AFFINITY = '' with PD delete failure -> DDL still succeeds, reconcile cleans up.
	tk.MustExec("create table t_alter(a int) affinity='table'")
	tAlter, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_alter"))
	require.NoError(t, err)
	alterGroupID := ddl.GetTableAffinityGroupID(codec, tAlter.Meta().ID)
	require.True(t, cli.hasGroup(alterGroupID))

	cli.mu.Lock()
	cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'temporary error'")
	cli.mu.Unlock()
	tk.MustExec("alter table t_alter affinity=''")
	// Group remains because immediate best-effort delete failed.
	require.True(t, cli.hasGroup(alterGroupID))

	// Recover PD and run two reconcile rounds: first re-observation must NOT delete
	// orphanID (its prior suspect state was invalidated by the gc_delete_range error).
	cli.mu.Lock()
	cli.deleteErr = nil
	cli.mu.Unlock()
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(alterGroupID))
	require.True(t, cli.hasGroup(orphanID), "orphan must NOT be deleted on first re-observation after an unclassified round")
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(alterGroupID))
	require.False(t, cli.hasGroup(orphanID))
}

func TestAffinityReconcileSuppressesRepairDuringAlterAffinity(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mut(a int) affinity='table'")
	tMut, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_mut"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tMut.Meta().ID)

	// Stop DDL owner scheduler so our synthetic ALTER job remains in tidb_ddl_job.
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		IsOwner: func() bool { return true },
	})
	t.Cleanup(restore)
	require.NoError(t, dom.DDL().Stop())
	cli.deleteGroupDirect(groupID)
	jAlter := &model.Job{ID: 880010, Version: model.JobVersion2, Type: model.ActionAlterTableAffinity, SchemaID: 1, TableID: tMut.Meta().ID}
	insertFakeDDLJob(t, tk, jAlter)
	t.Cleanup(func() { deleteFakeDDLJob(t, tk, jAlter.ID) })

	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(groupID), "reconcile must suppress repair while ALTER AFFINITY is in-flight")
}

func TestAffinityReconcileRepairPreventsZombieResurrection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutation  string
		expectHas bool
	}{
		{name: "alter affinity off after T3", mutation: "alter table t_zombie affinity=''", expectHas: false},
		{name: "drop table after T3", mutation: "drop table t_zombie", expectHas: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ddlutil.EmulatorGCDisable()
			t.Cleanup(ddlutil.EmulatorGCEnable)

			store, dom := testkit.CreateMockStoreAndDomain(t)
			cli := newReconcilePDClient()
			affinity.InitManager(cli)
			t.Cleanup(func() { affinity.InitManager(nil) })

			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_zombie(a int) affinity='table'")
			tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_zombie"))
			require.NoError(t, err)
			groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)
			// Simulate PD missing the group at T1 so reconcile selects it as a repair candidate.
			cli.deleteGroupDirect(groupID)
			require.False(t, cli.hasGroup(groupID))

			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec("use test")
			var mutated atomic.Bool
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAffinityReconcileReadInfoSchema", func() {
				if mutated.CompareAndSwap(false, true) {
					tk2.MustExec(tc.mutation)
				}
			})

			require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
			require.True(t, mutated.Load())
			require.Equal(t, tc.expectHas, cli.hasGroup(groupID), "repair must not resurrect a group disabled or dropped after T3")
		})
	}
}

func TestAffinityReconcileEmptyPDSkipsT4AndNilSchemaLoaderFailsClosed(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	// Seed an orphan and mark it suspect in round 1.
	orphanID := ddl.GetTableAffinityGroupID(codec, 940001)
	cli.seedGroup(orphanID, 0)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Remove all PD groups; with len(pdGroups)==0, reconcile must clear suspectOrphans
	// and skip T4 (even if T4 is configured to fail).
	cli.deleteGroupDirect(orphanID)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr", "return(true)")
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr")
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Nil schemaLoader must fail closed.
	restoreLoader := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		ClearSchemaLoader: true,
	})
	err := ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session())
	restoreLoader()
	require.ErrorContains(t, err, "schemaLoader is nil")
}

func TestAffinityReconcileFlashbackOnSecondDDLReadAbortsEntirePass(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_fb(a int) affinity='table'")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_fb"))
	require.NoError(t, err)
	codec := store.GetCodec()
	tableGroup := ddl.GetTableAffinityGroupID(codec, tbl.Meta().ID)

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
		IsOwner:     func() bool { return true },
	})
	t.Cleanup(restore)

	// Stop DDL owner scheduler so our synthetic FLASHBACK CLUSTER job stays in tidb_ddl_job.
	require.NoError(t, dom.DDL().Stop())

	// Round 1 marks an orphan suspect (eligible for deletion in round 2).
	orphanID := ddl.GetTableAffinityGroupID(codec, 950001)
	cli.seedGroup(orphanID, 0)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Delete t_fb's group so round 2 enters repairMissingGroups (second DDL job read).
	cli.deleteGroupDirect(tableGroup)

	// Inject FLASHBACK CLUSTER after T3 InfoSchema read, right before repair's second DDL read.
	tk2 := testkit.NewTestKit(t, store)
	jFlashback := &model.Job{ID: 880099, Version: model.JobVersion2, Type: model.ActionFlashbackCluster}
	var injected atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAffinityReconcileReadInfoSchema", func() {
		if injected.CompareAndSwap(false, true) {
			insertFakeDDLJob(t, tk2, jFlashback)
		}
	})
	t.Cleanup(func() { deleteFakeDDLJob(t, tk2, jFlashback.ID) })

	// Round 2 detects FLASHBACK CLUSTER on repair's second DDL read: must abort the
	// entire pass (neither repair tableGroup nor delete orphanID) and clear suspectOrphans.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, injected.Load())
	require.False(t, cli.hasGroup(tableGroup), "repair must be aborted when FLASHBACK CLUSTER is queued")
	require.True(t, cli.hasGroup(orphanID), "orphan deletion must be aborted when FLASHBACK CLUSTER is detected on second DDL read")
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()), "suspectOrphans must be cleared on FLASHBACK CLUSTER abort")
}

func TestAffinityReconcileDropIndexDoesNotProtectTable(t *testing.T) {
	ddlutil.EmulatorGCDisable()
	t.Cleanup(ddlutil.EmulatorGCEnable)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	tk.MustExec("create table t_idx_gc(a int, b int, index idx_b(b)) affinity='table'")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_idx_gc"))
	require.NoError(t, err)
	tblGroup := ddl.GetTableAffinityGroupID(codec, tbl.Meta().ID)
	require.True(t, cli.hasGroup(tblGroup))

	// DROP INDEX inserts an index-only range into mysql.gc_delete_range for t_idx_gc.
	tk.MustExec("alter table t_idx_gc drop index idx_b")
	rows := tk.MustQuery("select count(*) from mysql.gc_delete_range").Rows()
	require.NotEqual(t, "0", rows[0][0])

	// Also insert a partially-resumed table delete range (start_key inside _i, end_key at t<id+1>)
	// to verify it STILL protects its retired table ID.
	partialTblID := int64(960001)
	partialGroup := ddl.GetTableAffinityGroupID(codec, partialTblID)
	cli.seedGroup(partialGroup, 0)
	tk.MustExec("insert into mysql.gc_delete_range (job_id, element_id, start_key, end_key, ts) values (?, ?, ?, ?, ?)",
		99050, 1,
		hex.EncodeToString(tablecodec.EncodeTableIndexPrefix(partialTblID, 1)),
		hex.EncodeToString(tablecodec.EncodeTablePrefix(partialTblID+1)),
		uint64(100))
	t.Cleanup(func() {
		tk.MustExec("delete from mysql.gc_delete_range where job_id = 99050")
	})

	// Disable affinity while PD delete transiently fails -> group stays orphaned.
	cli.mu.Lock()
	cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'temporary'")
	cli.mu.Unlock()
	tk.MustExec("alter table t_idx_gc affinity=''")
	require.True(t, cli.hasGroup(tblGroup))

	// Recover PD and run two reconcile rounds: DROP INDEX range must NOT protect t_idx_gc,
	// whereas the partially-resumed table delete range MUST remain protected.
	cli.mu.Lock()
	cli.deleteErr = nil
	cli.mu.Unlock()
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(tblGroup))
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(tblGroup), "pending DROP INDEX range must not block orphan cleanup")
	require.True(t, cli.hasGroup(partialGroup), "partially-resumed table delete range must remain protected")
}

func TestAffinityReconcileInvalidatesSuspectsOnDeletionSuppression(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	codec := store.GetCodec()

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
		IsOwner:     func() bool { return true },
	})
	t.Cleanup(restore)

	require.NoError(t, dom.DDL().Stop())

	orphanID := ddl.GetTableAffinityGroupID(codec, 970001)
	cli.seedGroup(orphanID, 0)

	// Round 1 marks orphan suspect.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Round 2 has running DROP SCHEMA -> skipDeletion exit must clear suspectOrphans.
	jDropDB := &model.Job{ID: 880111, Version: model.JobVersion2, Type: model.ActionDropSchema, SchemaID: 77, State: model.JobStateRunning}
	insertFakeDDLJob(t, tk, jDropDB)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	deleteFakeDDLJob(t, tk, jDropDB.ID)

	// Round 3 is first re-observation -> must NOT delete yet.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(orphanID))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Round 4 confirms and deletes.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
}

func TestAffinityReconcileSerializedWithAlterTableAffinity(t *testing.T) {
	t.Run("repair vs alter disable: alter commits first", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_race1(a int) affinity='table'")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_race1"))
		require.NoError(t, err)
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)
		cli.deleteGroupDirect(groupID)

		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		var altered atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityReconcileRepairGate", func() {
			if altered.CompareAndSwap(false, true) {
				tk2.MustExec("alter table t_race1 affinity=''")
			}
		})

		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.True(t, altered.Load())
		require.False(t, cli.hasGroup(groupID), "repair must recheck under gate and skip creating stale group")
	})

	t.Run("repair vs alter disable: repair enters gate first", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_race2(a int) affinity='table'")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_race2"))
		require.NoError(t, err)
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)
		cli.deleteGroupDirect(groupID)

		repairInCreate := make(chan struct{})
		releaseRepair := make(chan struct{})
		var paused atomic.Bool
		cli.mu.Lock()
		cli.onBeforeCreate = func(ids []string) {
			if slices.Contains(ids, groupID) && paused.CompareAndSwap(false, true) {
				close(repairInCreate)
				<-releaseRepair
			}
		}
		cli.mu.Unlock()
		t.Cleanup(func() {
			cli.mu.Lock()
			cli.onBeforeCreate = nil
			cli.mu.Unlock()
		})

		reconcileDone := make(chan error, 1)
		go func() {
			reconcileDone <- ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session())
		}()

		<-repairInCreate
		alterAtGate := make(chan struct{})
		var alterSignaled atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityDDLPDOp", func() {
			if alterSignaled.CompareAndSwap(false, true) {
				close(alterAtGate)
			}
		})
		alterDone := make(chan error, 1)
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		go func() {
			_, execErr := tk2.Exec("alter table t_race2 affinity=''")
			alterDone <- execErr
		}()

		<-alterAtGate
		close(releaseRepair)
		require.NoError(t, <-reconcileDone)
		require.NoError(t, <-alterDone)
		require.False(t, cli.hasGroup(groupID), "ALTER post-commit cleanup must delete group created by repair")
	})

	t.Run("orphan delete vs alter re-enable: alter commits first", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_race3(a int)")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_race3"))
		require.NoError(t, err)
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)

		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
		})
		t.Cleanup(restore)

		// Seed a stale orphan using t_race3's table ID and confirm it in round 1.
		cli.seedGroup(groupID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		// In round 2, right before orphan delete acquires pdGate, ALTER re-enables affinity.
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		var altered atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityReconcileDeleteGate", func() {
			if altered.CompareAndSwap(false, true) {
				tk2.MustExec("alter table t_race3 affinity='table'")
			}
		})

		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.True(t, altered.Load())
		require.True(t, cli.hasGroup(groupID), "orphan delete must recheck under gate and skip deleting newly active group")
		require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	})

	t.Run("orphan delete vs alter re-enable: delete enters gate first", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_race4(a int)")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_race4"))
		require.NoError(t, err)
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)

		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
		})
		t.Cleanup(restore)

		// Seed stale orphan and mark suspect in round 1.
		cli.seedGroup(groupID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		deleteInProgress := make(chan struct{})
		releaseDelete := make(chan struct{})
		var paused atomic.Bool
		cli.mu.Lock()
		cli.onBeforeDelete = func(ids []string) {
			if slices.Contains(ids, groupID) && paused.CompareAndSwap(false, true) {
				close(deleteInProgress)
				<-releaseDelete
			}
		}
		cli.mu.Unlock()
		t.Cleanup(func() {
			cli.mu.Lock()
			cli.onBeforeDelete = nil
			cli.mu.Unlock()
		})

		reconcileDone := make(chan error, 1)
		go func() {
			reconcileDone <- ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session())
		}()

		<-deleteInProgress
		alterAtGate := make(chan struct{})
		var alterSignaled atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityDDLPDOp", func() {
			if alterSignaled.CompareAndSwap(false, true) {
				close(alterAtGate)
			}
		})
		alterDone := make(chan error, 1)
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		go func() {
			_, execErr := tk2.Exec("alter table t_race4 affinity='table'")
			alterDone <- execErr
		}()

		<-alterAtGate
		close(releaseDelete)
		require.NoError(t, <-reconcileDone)
		require.NoError(t, <-alterDone)
		require.True(t, cli.hasGroup(groupID), "ALTER must create group after orphan delete releases gate")
	})
}

type hookSchemaLoader struct {
	base     ddl.SchemaLoader
	onReload func()
}

func (h *hookSchemaLoader) Reload() error {
	if h.onReload != nil {
		h.onReload()
	}
	if h.base != nil {
		return h.base.Reload()
	}
	return nil
}

func TestAffinityReconcileRechecksPendingGCUnderGate(t *testing.T) {
	t.Run("drop after T4 before delete gate preserves group for RECOVER TABLE", func(t *testing.T) {
		ddlutil.EmulatorGCDisable()
		t.Cleanup(ddlutil.EmulatorGCEnable)

		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		safePoint := time.Now().Add(-time.Hour).Format("20060102-15:04:05 -0700")
		tk.MustExec("insert into mysql.tidb values ('tikv_gc_safe_point', ?, '') on duplicate key update variable_value = values(variable_value)", safePoint)
		t.Cleanup(func() {
			tk.MustExec("delete from mysql.tidb where variable_name = 'tikv_gc_safe_point'")
		})
		tk.MustExec("create table t_drop_after_t4(a int)")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop_after_t4"))
		require.NoError(t, err)
		tableID := tbl.Meta().ID
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tableID)

		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
		})
		t.Cleanup(restore)

		// Seed a stale orphan group for t_drop_after_t4's ID and observe it in round 1.
		cli.seedGroup(groupID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		// In round 2, after T4 classifies groupID into toDelete and right before
		// deleteOrphanBatchUnderGate acquires pdGate, enable affinity and DROP TABLE
		// so the table leaves InfoSchema/tidb_ddl_job and gains a new mysql.gc_delete_range row.
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		var dropped atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityReconcileDeleteGate", func() {
			if dropped.CompareAndSwap(false, true) {
				tk2.MustExec("alter table t_drop_after_t4 affinity='table'")
				tk2.MustExec("drop table t_drop_after_t4")
			}
		})

		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.True(t, dropped.Load())
		require.True(t, cli.hasGroup(groupID), "group protected by fresh gc_delete_range row under gate must not be deleted")
		require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		// RECOVER TABLE restores the same physical table ID with its PD affinity group intact.
		tk.MustExec("recover table t_drop_after_t4")
		recovered, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop_after_t4"))
		require.NoError(t, err)
		require.Equal(t, tableID, recovered.Meta().ID)
		require.NotNil(t, recovered.Meta().Affinity)
		require.True(t, cli.hasGroup(groupID))
	})

	t.Run("gc_delete_range read failure under delete gate fails closed and clears suspects", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		orphanID := ddl.GetTableAffinityGroupID(store.GetCodec(), 980001)

		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
		})
		t.Cleanup(restore)

		// Round 1 marks orphanID suspect.
		cli.seedGroup(orphanID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		// Round 2 passes T4, then fails loadPendingGCPhysicalIDs inside deleteOrphanBatchUnderGate.
		var injected atomic.Bool
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeAffinityReconcileDeleteGate", func() {
			if injected.CompareAndSwap(false, true) {
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr", "return(true)")
			}
		})
		err := ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session())
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/mockAffinityReconcileLoadGCRangesErr")
		require.Error(t, err)
		require.True(t, injected.Load())
		require.True(t, cli.hasGroup(orphanID), "orphan must not be deleted when gate gc_delete_range recheck fails")
		require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()), "suspectOrphans must be cleared when gate gc_delete_range recheck fails")
	})
}

func TestAffinityReconcileSessionAcquireFailureClearsSuspects(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := newReconcilePDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	orphanID := ddl.GetTableAffinityGroupID(store.GetCodec(), 981001)

	zeroGrace := time.Duration(0)
	restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
		GracePeriod: &zeroGrace,
	})
	t.Cleanup(restore)

	// Round 1 marks orphan suspect.
	cli.seedGroup(orphanID, 0)
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Round 2 fails sessPool.Get() -> suspectOrphans must be cleared.
	ddl.RunAffinityReconcileTickWithClosedPoolForTest(context.Background(), dom.DDL())
	require.True(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Round 3 is the first re-observation after the cleared state -> must NOT delete yet.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.True(t, cli.hasGroup(orphanID))
	require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

	// Round 4 confirms two consecutive observations and deletes.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
	require.False(t, cli.hasGroup(orphanID))
	require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
}

func TestAffinityReconcileOwnerLossInsideGateAbortsPDWrite(t *testing.T) {
	t.Run("repair aborts without calling PD when ownership is lost inside gate", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t_owner_repair(a int) affinity='table'")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_owner_repair"))
		require.NoError(t, err)
		groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)
		orphanID := ddl.GetTableAffinityGroupID(store.GetCodec(), 982001)

		var isOwner atomic.Bool
		isOwner.Store(true)
		var reloadCount atomic.Int32
		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
			IsOwner:     func() bool { return isOwner.Load() },
			SchemaLoader: &hookSchemaLoader{
				base: dom,
				onReload: func() {
					// Reload #1 is round 1 T3, #2 is round 2 T3, #3 is inside repairMissingBatchUnderGate.
					if reloadCount.Add(1) == 3 {
						isOwner.Store(false)
					}
				},
			},
		})
		t.Cleanup(restore)

		// Round 1 seeds a suspect orphan so we also verify suspectOrphans is cleared on abort.
		cli.seedGroup(orphanID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		// Delete t_owner_repair's group so round 2 enters repairMissingBatchUnderGate.
		cli.deleteGroupDirect(groupID)
		var createCalled atomic.Bool
		cli.mu.Lock()
		cli.onBeforeCreate = func(_ []string) { createCalled.Store(true) }
		cli.mu.Unlock()
		t.Cleanup(func() {
			cli.mu.Lock()
			cli.onBeforeCreate = nil
			cli.mu.Unlock()
		})

		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.False(t, isOwner.Load())
		require.False(t, createCalled.Load(), "PD CreateAffinityGroups must not be called after ownership loss inside gate")
		require.False(t, cli.hasGroup(groupID))
		require.True(t, cli.hasGroup(orphanID))
		require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	})

	t.Run("delete aborts without calling PD when ownership is lost inside gate", func(t *testing.T) {
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		orphanID := ddl.GetTableAffinityGroupID(store.GetCodec(), 982002)

		var isOwner atomic.Bool
		isOwner.Store(true)
		var reloadCount atomic.Int32
		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
			IsOwner:     func() bool { return isOwner.Load() },
			SchemaLoader: &hookSchemaLoader{
				base: dom,
				onReload: func() {
					// Reload #1 is round 1 T3, #2 is round 2 T3, #3 is inside deleteOrphanBatchUnderGate.
					if reloadCount.Add(1) == 3 {
						isOwner.Store(false)
					}
				},
			},
		})
		t.Cleanup(restore)

		// Round 1 marks orphan suspect.
		cli.seedGroup(orphanID, 0)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.Equal(t, 1, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))

		var deleteCalled atomic.Bool
		cli.mu.Lock()
		cli.onBeforeDelete = func(_ []string) { deleteCalled.Store(true) }
		cli.mu.Unlock()
		t.Cleanup(func() {
			cli.mu.Lock()
			cli.onBeforeDelete = nil
			cli.mu.Unlock()
		})

		// Round 2 loses ownership inside deleteOrphanBatchUnderGate right before PD delete.
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.False(t, isOwner.Load())
		require.False(t, deleteCalled.Load(), "PD BatchDeleteAffinityGroups must not be called after ownership loss inside gate")
		require.True(t, cli.hasGroup(orphanID))
		require.Zero(t, ddl.AffinityReconcilerSuspectCountForTest(dom.DDL()))
	})
}
