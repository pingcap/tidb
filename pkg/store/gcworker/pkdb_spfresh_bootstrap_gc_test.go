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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestGCPlacementForCanceledSPFreshTruncate(t *testing.T) {
	s := createGCWorkerSuite(t)
	const oldID, newID = int64(20000), int64(20001)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	write := func(jobID int64, finished, succeeded, intent bool) {
		require.NoError(t, kv.RunInNewTxn(ctx, s.store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			if intent {
				_, err := m.RegisterSPFreshBootstrap(jobID, meta.SPFreshBootstrapTarget{
					TableID: newID, IndexIDs: []int64{1}, ClusterID: 1, KeyspaceID: ^uint32(0),
					WorkerEndpoints: []string{"http://worker"}, Owner: "http://worker", BaseSnapshotTS: 100,
				})
				if err != nil {
					return err
				}
				if finished {
					if err := m.FinishSPFreshBootstrap(jobID, succeeded, true); err != nil {
						return err
					}
				}
			}
			// Reproduce actual canceled history, including cleared arguments.
			job := &model.Job{ID: jobID, Type: model.ActionTruncateTable, TableID: oldID,
				Version: model.JobVersion2, State: model.JobStateCancelled, RawArgs: []byte("null")}
			return m.AddHistoryDDLJob(job, false)
		}))
	}
	write(10000, true, false, true)
	for _, id := range []int64{oldID, newID} {
		bundle, err := placement.NewBundleFromOptions(&model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1, r2"})
		require.NoError(t, err)
		bundle.ID = fmt.Sprintf("TiDB_DDL_%d", id)
		require.NoError(t, infosync.PutRuleBundles(ctx, []*placement.Bundle{bundle}))
	}
	prefix := tablecodec.GenTablePrefix(newID)
	task := util.DelRangeTask{JobID: 10000, ElementID: 1, StartKey: prefix, EndKey: prefix.PrefixNext()}
	se := createSession(s.store)
	defer se.Close()
	// Standby replication enters through the DDL-job callback. A job ID alone
	// cannot identify the abandoned replacement; carry the destroyed range.
	require.ErrorContains(t, doGCPlacementRulesForDDLJob(s.store, task.JobID), "does not match")
	require.NoError(t, doGCPlacementRulesForDDLJob(s.store, task.JobID,
		kv.KeyRange{StartKey: task.StartKey, EndKey: task.EndKey}))
	var cache sync.Map
	require.NoError(t, doGCPlacementRules(se, 0, task, &cache))
	for id, deleted := range map[int64]bool{oldID: false, newID: true} {
		bundle, err := infosync.GetRuleBundle(ctx, fmt.Sprintf("TiDB_DDL_%d", id))
		require.NoError(t, err)
		require.Equal(t, deleted, bundle.IsEmpty(), "table %d", id)
		_, cached := cache.Load(id)
		require.Equal(t, deleted, cached)
	}
	// Repeated cleanup remains idempotent, and must never target the live old ID.
	require.NoError(t, doGCPlacementRules(se, 0, task, &cache))
	bad := task
	bad.StartKey = tablecodec.GenTablePrefix(oldID)
	bad.EndKey = kv.Key(bad.StartKey).PrefixNext()
	require.ErrorContains(t, doGCPlacementRules(se, 0, bad, &sync.Map{}), "does not match")
	bad = task
	bad.StartKey = append(append([]byte(nil), prefix...), 0)
	require.ErrorContains(t, doGCPlacementRules(se, 0, bad, &sync.Map{}), "does not match")
	for _, state := range []struct {
		jobID                       int64
		finished, succeeded, intent bool
	}{
		{10001, false, false, true},
		{10002, true, true, true},
		{10003, false, false, false},
	} {
		write(state.jobID, state.finished, state.succeeded, state.intent)
		bad = task
		bad.JobID = state.jobID
		require.ErrorContains(t, doGCPlacementRules(se, 0, bad, &sync.Map{}), "requires a failed SPFresh bootstrap intent")
	}
}
