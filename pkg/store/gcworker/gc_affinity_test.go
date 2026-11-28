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

package gcworker

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	tikvutil "github.com/tikv/client-go/v2/util"
	"github.com/tikv/pd/client/clients/gc"
	pdhttp "github.com/tikv/pd/client/http"
)

type gcAffinityMockPDClient struct {
	pdhttp.Client
	mu             sync.Mutex
	groups         map[string]*pdhttp.AffinityGroupState
	deleteErr      error
	deleteCalls    int
	deletedBatches [][]string
}

func newGCAffinityMockPDClient() *gcAffinityMockPDClient {
	return &gcAffinityMockPDClient{
		groups: make(map[string]*pdhttp.AffinityGroupState),
	}
}

func (c *gcAffinityMockPDClient) CreateAffinityGroups(_ context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, _ ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make(map[string]*pdhttp.AffinityGroupState, len(groups))
	for id, ranges := range groups {
		st := &pdhttp.AffinityGroupState{
			AffinityGroup: pdhttp.AffinityGroup{ID: id},
			RangeCount:    len(ranges),
		}
		c.groups[id] = st
		copied := *st
		res[id] = &copied
	}
	return res, nil
}

func (c *gcAffinityMockPDClient) BatchDeleteAffinityGroups(_ context.Context, ids []string, _ bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteCalls++
	c.deletedBatches = append(c.deletedBatches, slices.Clone(ids))
	if c.deleteErr != nil {
		return c.deleteErr
	}
	for _, id := range ids {
		delete(c.groups, id)
	}
	return nil
}

func (c *gcAffinityMockPDClient) GetAffinityGroups(_ context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
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

func (c *gcAffinityMockPDClient) GetAllAffinityGroups(_ context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make(map[string]*pdhttp.AffinityGroupState, len(c.groups))
	for id, st := range c.groups {
		copied := *st
		res[id] = &copied
	}
	return res, nil
}

func (c *gcAffinityMockPDClient) seed(ids ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range ids {
		c.groups[id] = &pdhttp.AffinityGroupState{AffinityGroup: pdhttp.AffinityGroup{ID: id}}
	}
}

func (c *gcAffinityMockPDClient) has(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.groups[id]
	return ok
}

func encodeDecodeJob(t *testing.T, j *model.Job) *model.Job {
	t.Helper()
	b, err := j.Encode(true)
	require.NoError(t, err)
	out := &model.Job{}
	require.NoError(t, out.Decode(b))
	return out
}

func mustExecSQL(t *testing.T, se sessionapi.Session, sql string) {
	t.Helper()
	rs, err := se.Execute(gcContext(), sql)
	require.NoError(t, err, "sql=%s", sql)
	for _, r := range rs {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}
}

func TestDoGCAffinityGroups(t *testing.T) {
	s := createGCWorkerSuite(t)
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	se := createSession(s.store)
	defer se.Close()

	var currentJob *model.Job
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/store/gcworker/mockGCAffinityHistoryJob", func(_ int64, dst **model.Job) {
		*dst = currentJob
	}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/gcworker/mockGCAffinityHistoryJob"))
	})

	t.Run("non-affinity table makes zero PD calls", func(t *testing.T) {
		j := &model.Job{Version: model.JobVersion2, ID: 101, Type: model.ActionDropTable, TableID: 10}
		j.FillFinishedArgs(&model.DropTableArgs{OldPartitionIDs: []int64{11}})
		currentJob = encodeDecodeJob(t, j)

		var cache sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 101, ElementID: 1}, &cache))
		require.Zero(t, cli.deleteCalls)
		_, cached := cache.Load(int64(101))
		require.True(t, cached)
	})

	t.Run("drop table and concurrent dedup by jobID", func(t *testing.T) {
		cli.seed("_tidb_pt_20_p21", "_tidb_pt_20_p22")
		j := &model.Job{Version: model.JobVersion2, ID: 102, Type: model.ActionDropTable, TableID: 20}
		j.FillFinishedArgs(&model.DropTableArgs{
			OldPartitionIDs:     []int64{21, 22},
			OldAffinityGroupIDs: []string{"_tidb_pt_20_p21", "_tidb_pt_20_p22"},
		})
		currentJob = encodeDecodeJob(t, j)

		var cache sync.Map
		beforeCalls := cli.deleteCalls
		const workers = 8
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, workers)
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startCh
				errs[idx] = s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 102, ElementID: int64(idx + 1)}, &cache)
			}(i)
		}
		close(startCh)
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, beforeCalls+1, cli.deleteCalls)
		require.False(t, cli.has("_tidb_pt_20_p21"))
		require.False(t, cli.has("_tidb_pt_20_p22"))
	})

	t.Run("drop schema batches over 100 groups", func(t *testing.T) {
		ids := make([]string, 0, 125)
		for i := 1; i <= 125; i++ {
			id := fmt.Sprintf("_tidb_t_%d", 5000+i)
			ids = append(ids, id)
			cli.seed(id)
		}
		j := &model.Job{Version: model.JobVersion2, ID: 103, Type: model.ActionDropSchema}
		j.FillFinishedArgs(&model.DropSchemaArgs{OldAffinityGroupIDs: ids})
		currentJob = encodeDecodeJob(t, j)

		beforeCalls := cli.deleteCalls
		beforeBatches := len(cli.deletedBatches)
		var cache sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 103, ElementID: 1}, &cache))
		require.Equal(t, beforeCalls+2, cli.deleteCalls)
		require.Len(t, cli.deletedBatches[beforeBatches], maxGCAffinityGroupBatchSize)
		require.Len(t, cli.deletedBatches[beforeBatches+1], 25)
		for _, id := range ids {
			require.False(t, cli.has(id))
		}
	})

	t.Run("truncate table, truncate partition, and mview cutover", func(t *testing.T) {
		cases := []struct {
			jobID int64
			job   *model.Job
			id    string
		}{
			{
				jobID: 104,
				id:    "_tidb_t_30",
				job: func() *model.Job {
					j := &model.Job{Version: model.JobVersion2, ID: 104, Type: model.ActionTruncateTable, TableID: 30}
					j.FillFinishedArgs(&model.TruncateTableArgs{OldAffinityGroupIDs: []string{"_tidb_t_30"}})
					return j
				}(),
			},
			{
				jobID: 105,
				id:    "_tidb_pt_31_p32",
				job: func() *model.Job {
					j := &model.Job{Version: model.JobVersion2, ID: 105, Type: model.ActionTruncateTablePartition, TableID: 31}
					j.FillFinishedArgs(&model.TruncateTableArgs{OldPartitionIDs: []int64{32}, OldAffinityGroupIDs: []string{"_tidb_pt_31_p32"}})
					return j
				}(),
			},
			{
				jobID: 106,
				id:    "_tidb_t_33",
				job: func() *model.Job {
					j := &model.Job{Version: model.JobVersion2, ID: 106, Type: model.ActionMViewRefreshOutOfPlaceCutover, TableID: 33}
					j.FillArgs(&model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs{
						OldMViewID:          33,
						ShadowTableID:       34,
						BuildReadTSO:        1,
						OldAffinityGroupIDs: []string{"_tidb_t_33"},
					})
					return j
				}(),
			},
		}
		for _, tc := range cases {
			cli.seed(tc.id)
			currentJob = encodeDecodeJob(t, tc.job)
			var cache sync.Map
			require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: tc.jobID, ElementID: 1}, &cache))
			require.False(t, cli.has(tc.id))
		}
	})

	t.Run("transient PD error deduplicates within round and retries next round", func(t *testing.T) {
		cli.seed("_tidb_t_40")
		j := &model.Job{Version: model.JobVersion2, ID: 107, Type: model.ActionDropTable, TableID: 40}
		j.FillFinishedArgs(&model.DropTableArgs{OldAffinityGroupIDs: []string{"_tidb_t_40"}})
		currentJob = encodeDecodeJob(t, j)

		cli.mu.Lock()
		cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'busy'")
		beforeCalls := cli.deleteCalls
		cli.mu.Unlock()
		var sameRoundCache sync.Map
		err := s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 107, ElementID: 1}, &sameRoundCache)
		require.Error(t, err)
		v, loaded := sameRoundCache.Load(int64(107))
		require.True(t, loaded)
		require.True(t, v.(*gcAffinityJobState).tried)
		require.Error(t, v.(*gcAffinityJobState).err)
		require.True(t, cli.has("_tidb_t_40"))

		// Second range of the same JobID in the same deleteRanges round must reuse
		// the cached failure without issuing another PD call.
		err2 := s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 107, ElementID: 2}, &sameRoundCache)
		require.Error(t, err2)
		cli.mu.Lock()
		require.Equal(t, beforeCalls+1, cli.deleteCalls)
		cli.mu.Unlock()

		// Next GC round uses a fresh per-round cache and succeeds after PD recovers.
		cli.mu.Lock()
		cli.deleteErr = nil
		cli.mu.Unlock()
		var nextRoundCache sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 107, ElementID: 1}, &nextRoundCache))
		v, loaded = nextRoundCache.Load(int64(107))
		require.True(t, loaded)
		require.True(t, v.(*gcAffinityJobState).tried)
		require.NoError(t, v.(*gcAffinityJobState).err)
		require.False(t, cli.has("_tidb_t_40"))
	})

	t.Run("unsupported old PD 404 returns nil and does not block GC", func(t *testing.T) {
		cli.seed("_tidb_t_41")
		j := &model.Job{Version: model.JobVersion2, ID: 108, Type: model.ActionDropTable, TableID: 41}
		j.FillFinishedArgs(&model.DropTableArgs{OldAffinityGroupIDs: []string{"_tidb_t_41"}})
		currentJob = encodeDecodeJob(t, j)

		cli.mu.Lock()
		cli.deleteErr = fmt.Errorf("request pd http api failed with status: '%d %s', body: '404 page not found'", http.StatusNotFound, http.StatusText(http.StatusNotFound))
		cli.mu.Unlock()
		t.Cleanup(func() {
			cli.mu.Lock()
			cli.deleteErr = nil
			cli.mu.Unlock()
		})
		var cache sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 108, ElementID: 1}, &cache))
		v, cached := cache.Load(int64(108))
		require.True(t, cached)
		require.True(t, v.(*gcAffinityJobState).tried)
		require.NoError(t, v.(*gcAffinityJobState).err)
	})

	t.Run("legacy #73720 cutover V1 slot 12 compatibility", func(t *testing.T) {
		oldMVID := int64(3301)
		shadowID := int64(3302)
		oldMLogID := int64(3303)
		oldMVGroup := ddl.GetTableAffinityGroupID(s.store.GetCodec(), oldMVID)
		shadowGroup := ddl.GetTableAffinityGroupID(s.store.GetCodec(), shadowID)
		oldMLogGroup := ddl.GetTableAffinityGroupID(s.store.GetCodec(), oldMLogID)

		// Case 1: #73720 V1 12-slot job with OldMViewHasAffinity=true and empty OldAffinityGroupIDs.
		// Transient PD 503 fails and preserves group; next round deletes ONLY oldMVGroup while
		// preserving shadowGroup and oldMLogGroup (cannot be inferred from OldMViewHasAffinity).
		cli.seed(oldMVGroup, shadowGroup, oldMLogGroup)
		legacyTrue := &model.Job{
			Version: model.JobVersion1,
			ID:      109,
			Type:    model.ActionMViewRefreshOutOfPlaceCutover,
			State:   model.JobStateDone,
			TableID: oldMVID,
		}
		legacyTrue.FillArgs(&model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs{
			OldMViewID:          oldMVID,
			ShadowTableID:       shadowID,
			OldMLogID:           oldMLogID,
			BuildReadTSO:        1,
			OldMViewHasAffinity: true,
		})
		currentJob = encodeDecodeJob(t, legacyTrue)

		cli.mu.Lock()
		cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'busy'")
		cli.mu.Unlock()
		var cache1 sync.Map
		require.Error(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 109, ElementID: 1}, &cache1))
		require.True(t, cli.has(oldMVGroup))

		cli.mu.Lock()
		cli.deleteErr = nil
		cli.mu.Unlock()
		var cache2 sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 109, ElementID: 1}, &cache2))
		require.False(t, cli.has(oldMVGroup), "legacy #73720 true job must delete retired MV group")
		require.True(t, cli.has(shadowGroup), "shadow MV group must be preserved")
		require.True(t, cli.has(oldMLogGroup), "old MLog group must not be inferred from OldMViewHasAffinity")

		// Case 2: Already-deleted old MV group stays idempotent with no error.
		var cache3 sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 109, ElementID: 1}, &cache3))

		// Case 3: OldMViewHasAffinity=false must NOT issue any PD delete RPC.
		cli.seed(oldMVGroup)
		legacyFalse := &model.Job{
			Version: model.JobVersion1,
			ID:      110,
			Type:    model.ActionMViewRefreshOutOfPlaceCutover,
			State:   model.JobStateDone,
			TableID: oldMVID,
		}
		legacyFalse.FillArgs(&model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs{
			OldMViewID:          oldMVID,
			ShadowTableID:       shadowID,
			BuildReadTSO:        1,
			OldMViewHasAffinity: false,
		})
		currentJob = encodeDecodeJob(t, legacyFalse)
		cli.mu.Lock()
		callsBeforeFalse := cli.deleteCalls
		cli.mu.Unlock()
		var cache4 sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 110, ElementID: 1}, &cache4))
		cli.mu.Lock()
		require.Equal(t, callsBeforeFalse, cli.deleteCalls)
		cli.mu.Unlock()
		require.True(t, cli.has(oldMVGroup))

		// Case 4: Cancelled job with OldMViewHasAffinity=true must NOT issue any PD delete RPC.
		legacyCancelled := &model.Job{
			Version: model.JobVersion1,
			ID:      111,
			Type:    model.ActionMViewRefreshOutOfPlaceCutover,
			State:   model.JobStateCancelled,
			TableID: oldMVID,
		}
		legacyCancelled.FillArgs(&model.RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs{
			OldMViewID:          oldMVID,
			ShadowTableID:       shadowID,
			BuildReadTSO:        1,
			OldMViewHasAffinity: true,
		})
		currentJob = encodeDecodeJob(t, legacyCancelled)
		cli.mu.Lock()
		callsBeforeCancel := cli.deleteCalls
		cli.mu.Unlock()
		var cache5 sync.Map
		require.NoError(t, s.gcWorker.doGCAffinityGroups(context.Background(), se, util.DelRangeTask{JobID: 111, ElementID: 1}, &cache5))
		cli.mu.Lock()
		require.Equal(t, callsBeforeCancel, cli.deleteCalls)
		cli.mu.Unlock()
		require.True(t, cli.has(oldMVGroup))
	})
}

func TestDeleteRangesGCAffinityEndToEnd(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "use test")
	mustExecSQL(t, se, "create table t_drop(a int) affinity='table'")
	mustExecSQL(t, se, "create table tp_drop(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	mustExecSQL(t, se, "create table t_trunc(a int) affinity='table'")
	mustExecSQL(t, se, "create table tp_trunc(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	mustExecSQL(t, se, "create database db_drop_aff")
	mustExecSQL(t, se, "create table db_drop_aff.td1(a int) affinity='table'")

	is := s.dom.InfoSchema()
	tDrop, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop"))
	require.NoError(t, err)
	tpDrop, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_drop"))
	require.NoError(t, err)
	tTrunc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_trunc"))
	require.NoError(t, err)
	tpTrunc, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_trunc"))
	require.NoError(t, err)
	td1, err := is.TableByName(context.Background(), ast.NewCIStr("db_drop_aff"), ast.NewCIStr("td1"))
	require.NoError(t, err)

	codec := s.store.GetCodec()
	dropGroup := ddl.GetTableAffinityGroupID(codec, tDrop.Meta().ID)
	tpDropGroups := make([]string, 0, len(tpDrop.Meta().Partition.Definitions))
	for _, def := range tpDrop.Meta().Partition.Definitions {
		tpDropGroups = append(tpDropGroups, ddl.GetPartitionAffinityGroupID(codec, tpDrop.Meta().ID, def.ID))
	}
	truncOldGroup := ddl.GetTableAffinityGroupID(codec, tTrunc.Meta().ID)
	partOldGroup := ddl.GetPartitionAffinityGroupID(codec, tpTrunc.Meta().ID, tpTrunc.Meta().Partition.Definitions[0].ID)
	dropDBGroup := ddl.GetTableAffinityGroupID(codec, td1.Meta().ID)

	mustExecSQL(t, se, "drop table t_drop")
	mustExecSQL(t, se, "drop table tp_drop")
	mustExecSQL(t, se, "truncate table t_trunc")
	mustExecSQL(t, se, "alter table tp_trunc truncate partition p0")
	mustExecSQL(t, se, "drop database db_drop_aff")

	// Before GC safe point passes, all old affinity groups remain in PD.
	require.True(t, cli.has(dropGroup))
	for _, g := range tpDropGroups {
		require.True(t, cli.has(g))
	}
	require.True(t, cli.has(truncOldGroup))
	require.True(t, cli.has(partOldGroup))
	require.True(t, cli.has(dropDBGroup))

	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })

	ctx := gcContext()
	futureSP := s.mustAllocTs(t) + 1000
	cli.mu.Lock()
	cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'temporary'")
	cli.mu.Unlock()
	require.NoError(t, s.gcWorker.deleteRanges(ctx, futureSP, gcConcurrency{v: 1}))
	require.True(t, cli.has(dropGroup))
	for _, g := range tpDropGroups {
		require.True(t, cli.has(g))
	}
	require.True(t, cli.has(truncOldGroup))
	require.True(t, cli.has(partOldGroup))
	require.True(t, cli.has(dropDBGroup))
	pendingRanges, err := util.LoadDeleteRanges(ctx, se, futureSP)
	require.NoError(t, err)
	require.NotEmpty(t, pendingRanges, "failed affinity GC must keep rows in mysql.gc_delete_range for retry")

	// Clear error and rerun deleteRanges: old affinity groups are deleted and ranges completed.
	cli.mu.Lock()
	cli.deleteErr = nil
	cli.mu.Unlock()
	require.NoError(t, s.gcWorker.deleteRanges(ctx, futureSP, gcConcurrency{v: 1}))
	require.False(t, cli.has(dropGroup))
	for _, g := range tpDropGroups {
		require.False(t, cli.has(g))
	}
	require.False(t, cli.has(truncOldGroup))
	require.False(t, cli.has(partOldGroup))
	require.False(t, cli.has(dropDBGroup))
	pendingRanges, err = util.LoadDeleteRanges(ctx, se, futureSP)
	require.NoError(t, err)
	require.Empty(t, pendingRanges)

	// Active replacement groups must still exist.
	is = s.dom.InfoSchema()
	tTruncNew, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_trunc"))
	require.NoError(t, err)
	tpTruncNew, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp_trunc"))
	require.NoError(t, err)
	require.True(t, cli.has(ddl.GetTableAffinityGroupID(codec, tTruncNew.Meta().ID)))
	for _, def := range tpTruncNew.Meta().Partition.Definitions {
		require.True(t, cli.has(ddl.GetPartitionAffinityGroupID(codec, tpTruncNew.Meta().ID, def.ID)))
	}
}

func TestCheckPrepareRetriesPendingAffinityCleanupUnderGCBarrier(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })

	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "use test")
	mustExecSQL(t, se, "create table t_barrier_old(a int) affinity='table'")
	mustExecSQL(t, se, "create table t_barrier_new(a int) affinity='table'")

	is := s.dom.InfoSchema()
	tOld, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_barrier_old"))
	require.NoError(t, err)
	tNew, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_barrier_new"))
	require.NoError(t, err)
	codec := s.store.GetCodec()
	oldGroup := ddl.GetTableAffinityGroupID(codec, tOld.Meta().ID)
	newGroup := ddl.GetTableAffinityGroupID(codec, tNew.Meta().ID)

	// Drop t_barrier_old before advancing safePoint to SP1.
	mustExecSQL(t, se, "drop table t_barrier_old")

	// Advance oracle past GC lifetime + run interval and prepare SP1.
	s.oracle.AddOffset(30 * time.Minute)
	ok, sp1, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotZero(t, sp1)

	// Run deleteRanges at SP1 while PD returns transient 503 on affinity delete.
	cli.mu.Lock()
	cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'temporary'")
	cli.mu.Unlock()
	require.NoError(t, s.gcWorker.deleteRanges(gcContext(), sp1, gcConcurrency{v: 1}))
	require.True(t, cli.has(oldGroup))

	// Place a GC barrier at SP1 so advanceTxnSafePoint holds steady (returns 0),
	// and drop t_barrier_new AFTER SP1 (so its delete-range ts >= SP1).
	gcStatesCli := s.pdClient.GetGCStatesClient(uint32(s.store.GetCodec().GetKeyspaceID()))
	_, err = gcStatesCli.SetGCBarrier(context.Background(), "barrier-affinity-retry", sp1, gc.TTLNeverExpire)
	require.NoError(t, err)
	mustExecSQL(t, se, "drop table t_barrier_new")

	// Simulate GC leader failover: a brand-new GCWorker instance with zero in-memory
	// retry state takes over leadership while the GC barrier still holds safePoint at SP1.
	failoverWorker := newTestGCWorker(t, s.store, s.pdClient)

	// Advance oracle past gcRunInterval: even though txnSafePoint cannot advance
	// due to the GC barrier, failoverWorker.prepare() must use the persisted safePoint
	// SP1 and schedule a retry for pending ranges strictly below SP1 without touching
	// newer ranges (t_barrier_new).
	s.oracle.AddOffset(30 * time.Minute)
	ok, retrySP, err := failoverWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sp1, retrySP)

	// Recover PD and run deleteRanges with retrySP: t_barrier_old is cleaned up,
	// while t_barrier_new (ts >= sp1) remains untouched in PD and gc_delete_range.
	cli.mu.Lock()
	cli.deleteErr = nil
	cli.mu.Unlock()
	require.NoError(t, failoverWorker.deleteRanges(gcContext(), retrySP, gcConcurrency{v: 1}))
	require.False(t, cli.has(oldGroup))
	require.True(t, cli.has(newGroup), "newer range dropped after original cutoff must not be processed early")

	// Next prepare under the same barrier must skip GC because no pending ranges remain < sp1.
	s.oracle.AddOffset(30 * time.Minute)
	ok, nextSP, err := failoverWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, nextSP)
}

func TestCheckPrepareRetriesAfterTransientTiKVErrorUnderStalledSafePoint(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "use test")
	mustExecSQL(t, se, "create table t_tikv_err(a int) affinity='table'")
	tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_tikv_err"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(s.store.GetCodec(), tbl.Meta().ID)
	mustExecSQL(t, se, "drop table t_tikv_err")

	s.oracle.AddOffset(30 * time.Minute)
	ok, sp1, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotZero(t, sp1)

	// Fail deleteRanges on the TiKV UnsafeDestroyRange step (before affinity cleanup).
	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		return nil, fmt.Errorf("injected transient tikv error")
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })
	require.NoError(t, s.gcWorker.deleteRanges(gcContext(), sp1, gcConcurrency{v: 1}))
	require.True(t, cli.has(groupID))
	pending, err := util.LoadDeleteRanges(gcContext(), se, sp1)
	require.NoError(t, err)
	require.NotEmpty(t, pending)

	// Hold safePoint at SP1 with a GC barrier.
	gcStatesCli := s.pdClient.GetGCStatesClient(uint32(s.store.GetCodec().GetKeyspaceID()))
	_, err = gcStatesCli.SetGCBarrier(context.Background(), "barrier-tikv-retry", sp1, gc.TTLNeverExpire)
	require.NoError(t, err)

	// Next prepare under stalled safePoint must still schedule a retry for pending ranges < SP1.
	s.oracle.AddOffset(30 * time.Minute)
	ok, retrySP, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sp1, retrySP)

	// Once TiKV recovers, deleteRanges completes the pending range and deletes the affinity group.
	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	require.NoError(t, s.gcWorker.deleteRanges(gcContext(), retrySP, gcConcurrency{v: 1}))
	require.False(t, cli.has(groupID))
	pending, err = util.LoadDeleteRanges(gcContext(), se, sp1)
	require.NoError(t, err)
	require.Empty(t, pending)

	// Next prepare skips GC because nothing remains pending below SP1.
	s.oracle.AddOffset(30 * time.Minute)
	ok, nextSP, err := s.gcWorker.prepare(gcContext())
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, nextSP)
}

func TestDeleteRangesDedupsSameJobAffinityFailurePerRound(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })

	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "create database db_drop_12")
	const tableCount = 12
	codec := s.store.GetCodec()
	groupIDs := make([]string, 0, tableCount)
	for i := range tableCount {
		tblName := fmt.Sprintf("t%d", i)
		mustExecSQL(t, se, fmt.Sprintf("create table db_drop_12.%s(a int) affinity='table'", tblName))
		tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("db_drop_12"), ast.NewCIStr(tblName))
		require.NoError(t, err)
		groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, tbl.Meta().ID))
	}
	mustExecSQL(t, se, "drop database db_drop_12")

	ctx := gcContext()
	futureSP := s.mustAllocTs(t) + 1000
	pendingBefore, err := util.LoadDeleteRanges(ctx, se, futureSP)
	require.NoError(t, err)
	require.Len(t, pendingBefore, tableCount)

	// Persistent PD 503 during round 1: all 12 ranges share one JobID, so only 1 PD
	// batch delete call may be issued in this round, and all 12 rows stay pending.
	cli.mu.Lock()
	cli.deleteErr = fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'temporary'")
	callsBefore := cli.deleteCalls
	cli.mu.Unlock()
	require.NoError(t, s.gcWorker.deleteRanges(ctx, futureSP, gcConcurrency{v: 4}))
	cli.mu.Lock()
	require.Equal(t, callsBefore+1, cli.deleteCalls, "12 ranges of same JobID must issue only 1 PD batch delete call per round on 503")
	cli.mu.Unlock()
	pendingAfterErr, err := util.LoadDeleteRanges(ctx, se, futureSP)
	require.NoError(t, err)
	require.Len(t, pendingAfterErr, tableCount)
	for _, g := range groupIDs {
		require.True(t, cli.has(g))
	}

	// Recover PD in round 2: 1 PD batch delete call completes all 12 ranges.
	cli.mu.Lock()
	cli.deleteErr = nil
	callsBeforeRecover := cli.deleteCalls
	cli.mu.Unlock()
	require.NoError(t, s.gcWorker.deleteRanges(ctx, futureSP, gcConcurrency{v: 4}))
	cli.mu.Lock()
	require.Equal(t, callsBeforeRecover+1, cli.deleteCalls, "recovery round must issue only 1 PD batch delete call for all 12 ranges")
	cli.mu.Unlock()
	pendingAfterRecover, err := util.LoadDeleteRanges(ctx, se, futureSP)
	require.NoError(t, err)
	require.Empty(t, pendingAfterRecover)
	for _, g := range groupIDs {
		require.False(t, cli.has(g))
	}
}

func TestCheckPrepareReplacementLeaderPersistsSQLSafePointBeforeRetry(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	oldWait := txnSafePointSyncWaitTime
	txnSafePointSyncWaitTime = 0
	t.Cleanup(func() { txnSafePointSyncWaitTime = oldWait })

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	ctx := gcContext()
	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "use test")
	mustExecSQL(t, se, "create table t_failover_sync(a int) affinity='table'")
	tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_failover_sync"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(s.store.GetCodec(), tbl.Meta().ID)

	// Record an older SQL safe point (SP0) before dropping the table.
	oldSQLSafePointTime, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	oldSQLSafePointTime = oldSQLSafePointTime.Truncate(time.Millisecond)
	s.oracle.AddOffset(time.Minute)
	oldSnapTS := s.mustAllocTs(t)
	s.oracle.AddOffset(time.Minute)
	mustExecSQL(t, se, "drop table t_failover_sync")

	// Advance oracle so SP1 is strictly after the drop, then simulate the original
	// GC leader advancing PD's TxnSafePoint to SP1 and exiting before persisting
	// tikv_gc_safe_point in mysql.tidb.
	s.oracle.AddOffset(20 * time.Minute)
	now, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	sp1Time := now.Add(-gcDefaultLifeTime).Truncate(time.Millisecond)
	sp1 := oracle.GoTimeToTS(sp1Time)
	require.Greater(t, sp1, oldSnapTS)

	keyspaceID := uint32(s.store.GetCodec().GetKeyspaceID())
	_, err = s.pdClient.GetGCInternalController(keyspaceID).AdvanceTxnSafePoint(ctx, sp1)
	require.NoError(t, err)
	_, err = s.pdClient.GetGCStatesClient(keyspaceID).SetGCBarrier(ctx, "barrier-failover-sync", sp1, gc.TTLNeverExpire)
	require.NoError(t, err)

	oldLastRun := now.Add(-30 * time.Minute).Truncate(time.Millisecond)
	require.NoError(t, s.gcWorker.saveTime(gcSafePointKey, oldSQLSafePointTime))
	require.NoError(t, s.gcWorker.saveTime(gcLastRunTimeKey, oldLastRun))

	// Before takeover, SQL still allows a snapshot older than SP1 because
	// tikv_gc_safe_point is still at SP0.
	preCheckSe := createSession(s.store)
	_, err = preCheckSe.Execute(ctx, fmt.Sprintf("set @@tidb_snapshot = '%d'", oldSnapTS))
	preCheckSe.Close()
	require.NoError(t, err)

	// Replacement GC leader takes over with no in-memory state. Verify that by the
	// time UnsafeDestroyRange executes, tikv_gc_safe_point is already persisted to SP1.
	failoverWorker := newTestGCWorker(t, s.store, s.pdClient)
	failoverWorker.lastFinish = time.Now().Add(-gcWaitTime)

	var (
		destroyMu          sync.Mutex
		destroyCalls       int
		sqlSafePointAtCall *time.Time
		loadErrAtCall      error
	)
	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		sp, loadErr := failoverWorker.loadTime(gcSafePointKey)
		destroyMu.Lock()
		destroyCalls++
		if sqlSafePointAtCall == nil {
			sqlSafePointAtCall = sp
			loadErrAtCall = loadErr
		}
		destroyMu.Unlock()
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })

	require.NoError(t, failoverWorker.leaderTick(ctx))
	require.True(t, failoverWorker.gcIsRunning)
	select {
	case err := <-failoverWorker.done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for replacement GC job")
	}

	destroyMu.Lock()
	require.Positive(t, destroyCalls)
	require.NoError(t, loadErrAtCall)
	require.NotNil(t, sqlSafePointAtCall)
	require.Equal(t, sp1, oracle.GoTimeToTS(*sqlSafePointAtCall), "tikv_gc_safe_point must be persisted before range cleanup starts")
	destroyMu.Unlock()

	require.False(t, cli.has(groupID))
	pending, err := util.LoadDeleteRanges(ctx, se, sp1)
	require.NoError(t, err)
	require.Empty(t, pending)

	// A new SQL session attempting to set a snapshot older than SP1 must be rejected
	// with ErrSnapshotTooOld (error 8055).
	postCheckSe := createSession(s.store)
	defer postCheckSe.Close()
	_, err = postCheckSe.Execute(ctx, fmt.Sprintf("set @@tidb_snapshot = '%d'", oldSnapTS))
	require.Error(t, err)
	require.True(t, variable.ErrSnapshotTooOld.Equal(err), "expected ErrSnapshotTooOld (8055), got: %v", err)
}

func TestCheckPrepareReplacementLeaderAbortsRetryWhenSQLSafePointPersistFails(t *testing.T) {
	util.EmulatorGCDisable()
	t.Cleanup(util.EmulatorGCEnable)

	oldWait := txnSafePointSyncWaitTime
	txnSafePointSyncWaitTime = 0
	t.Cleanup(func() { txnSafePointSyncWaitTime = oldWait })

	s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
	cli := newGCAffinityMockPDClient()
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })

	ctx := gcContext()
	se := createSession(s.store)
	defer se.Close()
	mustExecSQL(t, se, "use test")
	mustExecSQL(t, se, "create table t_failover_err(a int) affinity='table'")
	tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_failover_err"))
	require.NoError(t, err)
	groupID := ddl.GetTableAffinityGroupID(s.store.GetCodec(), tbl.Meta().ID)

	oldSQLSafePointTime, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	oldSQLSafePointTime = oldSQLSafePointTime.Truncate(time.Millisecond)
	s.oracle.AddOffset(time.Minute)
	oldSnapTS := s.mustAllocTs(t)
	s.oracle.AddOffset(time.Minute)
	mustExecSQL(t, se, "drop table t_failover_err")

	s.oracle.AddOffset(20 * time.Minute)
	now, err := s.gcWorker.getOracleTime()
	require.NoError(t, err)
	sp1Time := now.Add(-gcDefaultLifeTime).Truncate(time.Millisecond)
	sp1 := oracle.GoTimeToTS(sp1Time)

	keyspaceID := uint32(s.store.GetCodec().GetKeyspaceID())
	_, err = s.pdClient.GetGCInternalController(keyspaceID).AdvanceTxnSafePoint(ctx, sp1)
	require.NoError(t, err)
	_, err = s.pdClient.GetGCStatesClient(keyspaceID).SetGCBarrier(ctx, "barrier-failover-err", sp1, gc.TTLNeverExpire)
	require.NoError(t, err)

	oldLastRun := now.Add(-30 * time.Minute).Truncate(time.Millisecond)
	require.NoError(t, s.gcWorker.saveTime(gcSafePointKey, oldSQLSafePointTime))
	require.NoError(t, s.gcWorker.saveTime(gcLastRunTimeKey, oldLastRun))

	var destroyCalled atomic.Bool
	s.client.unsafeDestroyRangeHandler = func(_ string, _ *tikvrpc.Request) (*tikvrpc.Response, error) {
		destroyCalled.Store(true)
		return &tikvrpc.Response{Resp: &kvrpcpb.UnsafeDestroyRangeResponse{}}, nil
	}
	t.Cleanup(func() { s.client.unsafeDestroyRangeHandler = nil })

	// Inject a write failure specifically when persisting tikv_gc_safe_point.
	var injectedHits atomic.Int32
	failSaveSafePoint := func(_ string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
		for _, m := range req.Prewrite().GetMutations() {
			if bytes.Contains(m.GetKey(), []byte(gcSafePointKey)) || bytes.Contains(m.GetValue(), []byte(gcSafePointKey)) {
				injectedHits.Add(1)
				return &tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{
						Errors: []*kvrpcpb.KeyError{{Abort: "injected tikv_gc_safe_point persistence failure"}},
					},
				}, nil
			}
		}
		return nil, nil
	}
	s.client.prewriteHandler.Store(&failSaveSafePoint)
	t.Cleanup(func() { s.client.prewriteHandler.Store(nil) })

	failoverWorker := newTestGCWorker(t, s.store, s.pdClient)
	failoverWorker.lastFinish = time.Now().Add(-gcWaitTime)

	// Persisting tikv_gc_safe_point fails: leaderTick must return an error and must
	// not launch range cleanup or advance gcLastRunTimeKey.
	err = failoverWorker.leaderTick(ctx)
	require.Error(t, err)
	require.Positive(t, injectedHits.Load(), "expected tikv_gc_safe_point write to be attempted")
	require.False(t, failoverWorker.gcIsRunning)
	require.False(t, destroyCalled.Load(), "range cleanup must not run when persisting tikv_gc_safe_point fails")
	require.True(t, cli.has(groupID))

	pending, err := util.LoadDeleteRanges(ctx, se, sp1)
	require.NoError(t, err)
	require.NotEmpty(t, pending)

	savedSP, err := failoverWorker.loadTime(gcSafePointKey)
	require.NoError(t, err)
	require.NotNil(t, savedSP)
	require.True(t, savedSP.Equal(oldSQLSafePointTime))

	savedLastRun, err := failoverWorker.loadTime(gcLastRunTimeKey)
	require.NoError(t, err)
	require.NotNil(t, savedLastRun)
	require.True(t, savedLastRun.Equal(oldLastRun), "gcLastRunTimeKey must not advance when persisting tikv_gc_safe_point fails")

	// Once persistence recovers, the next tick immediately retries without waiting
	// for another gcRunInterval, persists tikv_gc_safe_point, and cleans up ranges.
	s.client.prewriteHandler.Store(nil)
	require.NoError(t, failoverWorker.leaderTick(ctx))
	require.True(t, failoverWorker.gcIsRunning)
	select {
	case err := <-failoverWorker.done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for recovered GC job")
	}

	require.True(t, destroyCalled.Load())
	require.False(t, cli.has(groupID))
	pending, err = util.LoadDeleteRanges(ctx, se, sp1)
	require.NoError(t, err)
	require.Empty(t, pending)

	checkSe := createSession(s.store)
	defer checkSe.Close()
	_, err = checkSe.Execute(ctx, fmt.Sprintf("set @@tidb_snapshot = '%d'", oldSnapTS))
	require.Error(t, err)
	require.True(t, variable.ErrSnapshotTooOld.Equal(err), "expected ErrSnapshotTooOld (8055), got: %v", err)

	t.Run("overlapping workers never regress SQL safe point", func(t *testing.T) {
		s := createGCWorkerSuite(t, withSchemaLease(220*time.Minute))
		ctx := gcContext()
		se := createSession(s.store)
		defer se.Close()

		baseTime, err := s.gcWorker.getOracleTime()
		require.NoError(t, err)
		sp0Time := baseTime.Truncate(time.Millisecond)
		s.oracle.AddOffset(2 * time.Minute)
		spATime := sp0Time.Add(2 * time.Minute)
		s.oracle.AddOffset(2 * time.Minute)
		midSnapTS := s.mustAllocTs(t)
		s.oracle.AddOffset(2 * time.Minute)
		spBTime := spATime.Add(4 * time.Minute)
		spA := oracle.GoTimeToTS(spATime)
		spB := oracle.GoTimeToTS(spBTime)
		require.Less(t, spA, midSnapTS)
		require.Less(t, midSnapTS, spB)

		assertMidSnapshotRejected := func() {
			checkSe := createSession(s.store)
			defer checkSe.Close()
			_, execErr := checkSe.Execute(ctx, fmt.Sprintf("set @@tidb_snapshot = '%d'", midSnapTS))
			require.Error(t, execErr)
			require.True(t, variable.ErrSnapshotTooOld.Equal(execErr), "expected ErrSnapshotTooOld (8055), got: %v", execErr)
		}

		oldWorker := newTestGCWorker(t, s.store, s.pdClient)
		newWorker := newTestGCWorker(t, s.store, s.pdClient)

		// Subcase 1: Existing row at SP0. Old worker starts saveTimeIfNewer(SP_A) and
		// pauses at CmdPrewrite while new leader concurrently advances to SP_B.
		// Pessimistic row locking serializes both writes so SQL ends at SP_B and never
		// regresses to SP_A.
		require.NoError(t, oldWorker.saveTime(gcSafePointKey, sp0Time))

		spAVal := []byte(spATime.Format(tikvutil.GCTimeFormat))
		pausedOld := make(chan struct{})
		releaseOld := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseOld) }) }
		// Unblock a writer paused in the hook even if the subtest fails early.
		t.Cleanup(release)
		var pausedOnce atomic.Bool
		var h handler = func(_ string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if prewrite, ok := req.Req.(*kvrpcpb.PrewriteRequest); ok {
				for _, m := range prewrite.Mutations {
					if bytes.Contains(m.Value, spAVal) && pausedOnce.CompareAndSwap(false, true) {
						close(pausedOld)
						<-releaseOld
					}
				}
			}
			return nil, nil
		}
		s.client.prewriteHandler.Store(&h)
		t.Cleanup(func() { s.client.prewriteHandler.Store(nil) })

		writeCtx, cancelWrites := context.WithCancel(ctx)
		t.Cleanup(cancelWrites)
		oldDone := make(chan error, 1)
		go func() {
			oldDone <- oldWorker.saveTimeIfNewer(writeCtx, gcSafePointKey, spATime)
		}()

		select {
		case <-pausedOld:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for old worker prewrite pause")
		}

		newDone := make(chan error, 1)
		go func() {
			newDone <- newWorker.saveTimeIfNewer(writeCtx, gcSafePointKey, spBTime)
		}()

		// Give newWorker time to contend on the locked mysql.tidb row before releasing oldWorker.
		time.Sleep(50 * time.Millisecond)
		release()

		for _, done := range []chan error{oldDone, newDone} {
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(30 * time.Second):
				t.Fatal("timed out waiting for concurrent saveTimeIfNewer")
			}
		}
		s.client.prewriteHandler.Store(nil)

		savedSP, err := newWorker.loadTime(gcSafePointKey)
		require.NoError(t, err)
		require.NotNil(t, savedSP)
		require.True(t, savedSP.Equal(spBTime), "expected SQL safe point %v, got %v", spBTime, *savedSP)
		assertMidSnapshotRejected()

		// Subcase 2: New leader has already committed SP_B; a delayed old worker
		// resuming with lower SP_A must keep SP_B intact and midSnapTS must stay rejected.
		require.NoError(t, oldWorker.saveTimeIfNewer(ctx, gcSafePointKey, spATime))
		savedSP, err = newWorker.loadTime(gcSafePointKey)
		require.NoError(t, err)
		require.NotNil(t, savedSP)
		require.True(t, savedSP.Equal(spBTime), "delayed older worker must not regress SQL safe point from %v to %v", spBTime, *savedSP)
		assertMidSnapshotRejected()

		// Subcase 3: Missing row initially. Two workers concurrently insert SP_A and SP_B
		// via saveTimeIfNewer; plain INSERT + ErrKeyExists retry ensures final SQL safe
		// point is strictly SP_B regardless of goroutine scheduling order.
		for range 5 {
			mustExecSQL(t, se, fmt.Sprintf("delete from mysql.tidb where variable_name = '%s'", gcSafePointKey))
			start := make(chan struct{})
			errA := make(chan error, 1)
			errB := make(chan error, 1)
			go func() {
				<-start
				errA <- oldWorker.saveTimeIfNewer(ctx, gcSafePointKey, spATime)
			}()
			go func() {
				<-start
				errB <- newWorker.saveTimeIfNewer(ctx, gcSafePointKey, spBTime)
			}()
			close(start)
			require.NoError(t, <-errA)
			require.NoError(t, <-errB)

			savedSP, err = newWorker.loadTime(gcSafePointKey)
			require.NoError(t, err)
			require.NotNil(t, savedSP)
			require.True(t, savedSP.Equal(spBTime), "concurrent empty-row insert regressed SQL safe point to %v", *savedSP)
			assertMidSnapshotRejected()
		}
	})
}
