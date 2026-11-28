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

package ddl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/ngaut/pools"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockStoreWithCodec struct {
	kv.Storage
	codec tikv.Codec
}

func (m *mockStoreWithCodec) GetCodec() tikv.Codec {
	return m.codec
}

type contextAffinityPDClient struct {
	pdhttp.Client
	calls       int
	hasDeadline bool
}

func (c *contextAffinityPDClient) BatchDeleteAffinityGroups(ctx context.Context, _ []string, _ bool) error {
	c.calls++
	if _, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://pd/affinity-groups", nil); err != nil {
		return err
	}
	_, c.hasDeadline = ctx.Deadline()
	return ctx.Err()
}

func TestAffinityDeleteWithTimeoutContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "active_ctx", true: "cancelled_ctx"}[cancelled], func(t *testing.T) {
			cli := &contextAffinityPDClient{}
			affinity.InitManager(cli)
			t.Cleanup(func() { affinity.InitManager(nil) })
			ctx, cancel := context.WithCancel(context.Background())
			if cancelled {
				cancel()
			} else {
				defer cancel()
			}
			jobCtx := &jobContext{ctx: ctx, store: &mockStoreWithCodec{}}
			tblInfo := &model.TableInfo{
				ID:       10,
				Name:     ast.NewCIStr("t"),
				Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable},
			}
			err := deleteTableAffinityGroupsWithTimeout(jobCtx, tblInfo, nil)
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, cli.calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, cli.calls)
				require.True(t, cli.hasDeadline)
			}
		})
	}
}

func TestCollectInFlightJobProtection(t *testing.T) {
	t.Run("flashback skips round", func(t *testing.T) {
		prot := newInFlightAffinityProtection()
		err := collectInFlightJobProtection(&model.Job{Type: model.ActionFlashbackCluster}, prot)
		require.NoError(t, err)
		require.True(t, prot.skipRound)
	})

	t.Run("create table and batch create tables", func(t *testing.T) {
		j1 := &model.Job{Version: model.JobVersion2, Type: model.ActionCreateTable, TableID: 100}
		j1.FillArgs(&model.CreateTableArgs{
			TableInfo: &model.TableInfo{
				ID: 100,
				Partition: &model.PartitionInfo{
					Definitions: []model.PartitionDefinition{{ID: 101}, {ID: 102}},
				},
			},
		})
		j2 := &model.Job{Version: model.JobVersion2, Type: model.ActionCreateTables}
		j2.FillArgs(&model.BatchCreateTableArgs{
			Tables: []*model.CreateTableArgs{
				{TableInfo: &model.TableInfo{ID: 200}},
				{TableInfo: &model.TableInfo{ID: 201, Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{{ID: 202}}}}},
			},
		})
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(j1, prot))
		require.NoError(t, collectInFlightJobProtection(j2, prot))
		for _, id := range []int64{100, 101, 102, 200, 201, 202} {
			require.Contains(t, prot.protectedTableIDs, id)
		}
	})

	t.Run("truncate table and partition", func(t *testing.T) {
		j1 := &model.Job{Version: model.JobVersion2, Type: model.ActionTruncateTable, TableID: 300}
		j1.FillArgs(&model.TruncateTableArgs{
			NewTableID:      301,
			NewPartitionIDs: []int64{302, 303},
			OldPartitionIDs: []int64{304, 305},
		})
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(j1, prot))
		for _, id := range []int64{300, 301, 302, 303, 304, 305} {
			require.Contains(t, prot.protectedTableIDs, id)
		}
		require.Contains(t, prot.mutatingTableIDs, int64(300))
		require.Contains(t, prot.mutatingTableIDs, int64(301))
	})

	t.Run("alter table affinity protects and suppresses repair", func(t *testing.T) {
		j := &model.Job{Version: model.JobVersion2, Type: model.ActionAlterTableAffinity, TableID: 400}
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(j, prot))
		require.Contains(t, prot.protectedTableIDs, int64(400))
		require.Contains(t, prot.mutatingTableIDs, int64(400))
	})

	t.Run("recover table and schema", func(t *testing.T) {
		j1 := &model.Job{Version: model.JobVersion2, Type: model.ActionRecoverTable, TableID: 500}
		j1.FillArgs(&model.RecoverArgs{
			RecoverInfo: &model.RecoverSchemaInfo{
				RecoverTableInfos: []*model.RecoverTableInfo{
					{TableInfo: &model.TableInfo{ID: 500, Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{{ID: 501}}}}},
				},
			},
		})
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(j1, prot))
		require.Contains(t, prot.protectedTableIDs, int64(500))
		require.Contains(t, prot.protectedTableIDs, int64(501))
		require.False(t, prot.skipDeletion)

		j2 := &model.Job{Version: model.JobVersion2, Type: model.ActionRecoverSchema, SchemaID: 50}
		j2.FillArgs(&model.RecoverArgs{
			RecoverInfo: &model.RecoverSchemaInfo{LoadTablesOnExecute: true},
		})
		prot2 := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(j2, prot2))
		require.True(t, prot2.skipDeletion)
	})

	t.Run("drop schema unfinished vs finished", func(t *testing.T) {
		jRunning := &model.Job{Version: model.JobVersion2, Type: model.ActionDropSchema, SchemaID: 60, State: model.JobStateRunning}
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(jRunning, prot))
		require.Contains(t, prot.mutatingSchemaIDs, int64(60))
		require.True(t, prot.skipDeletion)

		jDone := &model.Job{Version: model.JobVersion2, Type: model.ActionDropSchema, SchemaID: 60, State: model.JobStateDone}
		jDone.FillFinishedArgs(&model.DropSchemaArgs{AllDroppedTableIDs: []int64{601, 602}})
		protDone := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(jDone, protDone))
		require.Contains(t, protDone.mutatingSchemaIDs, int64(60))
		require.Contains(t, protDone.protectedTableIDs, int64(601))
		require.Contains(t, protDone.protectedTableIDs, int64(602))
		require.False(t, protDone.skipDeletion)
	})

	t.Run("multi schema change recurses into subjobs", func(t *testing.T) {
		parent := &model.Job{
			Version: model.JobVersion2,
			Type:    model.ActionMultiSchemaChange,
			TableID: 700,
			MultiSchemaInfo: &model.MultiSchemaInfo{
				SubJobs: []*model.SubJob{
					{Type: model.ActionAlterTableAffinity},
				},
			},
		}
		prot := newInFlightAffinityProtection()
		require.NoError(t, collectInFlightJobProtection(parent, prot))
		require.Contains(t, prot.protectedTableIDs, int64(700))
		require.Contains(t, prot.mutatingTableIDs, int64(700))
	})

	t.Run("corrupted args returns error fail-closed", func(t *testing.T) {
		bad := &model.Job{
			Version: model.JobVersion2,
			Type:    model.ActionCreateTable,
			RawArgs: json.RawMessage(`{invalid-json`),
		}
		prot := newInFlightAffinityProtection()
		require.Error(t, collectInFlightJobProtection(bad, prot))
	})
}

// AffinityReconcilerConfigForTest configures an affinityReconciler in tests.
type AffinityReconcilerConfigForTest struct {
	GracePeriod       *time.Duration
	BatchSize         *int
	Now               func() time.Time
	SchemaLoader      SchemaLoader
	ClearSchemaLoader bool
	Codec             tikv.Codec
	IsOwner           func() bool
}

// ConfigureAffinityReconcilerForTest mutates test settings on d's affinityReconciler
// and returns a restore callback.
func ConfigureAffinityReconcilerForTest(d DDL, cfg AffinityReconcilerConfigForTest) func() {
	dd := d.(*ddl)
	r := dd.affinityReconciler
	origGrace := r.gracePeriod
	origBatch := r.batchSize
	origNow := r.now
	origLoader := r.schemaLoader
	origStore := r.store
	origIsOwner := r.isOwner

	if cfg.GracePeriod != nil {
		r.gracePeriod = *cfg.GracePeriod
	}
	if cfg.BatchSize != nil {
		r.batchSize = *cfg.BatchSize
	}
	if cfg.Now != nil {
		r.now = cfg.Now
	}
	if cfg.ClearSchemaLoader {
		r.schemaLoader = nil
	} else if cfg.SchemaLoader != nil {
		r.schemaLoader = cfg.SchemaLoader
	}
	if cfg.Codec != nil {
		r.store = &mockStoreWithCodec{Storage: r.store, codec: cfg.Codec}
	}
	if cfg.IsOwner != nil {
		r.isOwner = cfg.IsOwner
	}

	return func() {
		r.gracePeriod = origGrace
		r.batchSize = origBatch
		r.now = origNow
		r.schemaLoader = origLoader
		r.store = origStore
		r.isOwner = origIsOwner
	}
}

// ReconcileAffinityOnceForTest triggers one reconciliation pass synchronously.
func ReconcileAffinityOnceForTest(ctx context.Context, d DDL, sctx sessionctx.Context) error {
	return d.(*ddl).affinityReconciler.reconcileOnce(ctx, sctx)
}

// RunAffinityReconcileTickWithClosedPoolForTest runs one reconciler tick against
// a closed session pool so sessPool.Get() returns an error.
func RunAffinityReconcileTickWithClosedPoolForTest(ctx context.Context, d DDL) {
	pool := sess.NewSessionPool(pools.NewResourcePool(func() (pools.Resource, error) {
		return nil, errors.New("unused")
	}, 1, 1, 0))
	pool.Close()
	d.(*ddl).affinityReconciler.runOneTick(ctx, pool)
}

// ClearAffinityReconcilerForTest clears suspect orphans (simulating owner retirement).
func ClearAffinityReconcilerForTest(d DDL) {
	d.(*ddl).affinityReconciler.clear()
}

// AffinityReconcilerSuspectCountForTest returns the current suspect orphan count.
func AffinityReconcilerSuspectCountForTest(d DDL) int {
	r := d.(*ddl).affinityReconciler
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.suspectOrphans)
}

type recordingDelRangeWrapper struct {
	batches int
	params  []any
}

func (*recordingDelRangeWrapper) UpdateTSOForJob() error                { return nil }
func (w *recordingDelRangeWrapper) PrepareParamsList(sz int)            { w.params = make([]any, 0, sz) }
func (*recordingDelRangeWrapper) RewriteTableID(id int64) (int64, bool) { return id, true }
func (w *recordingDelRangeWrapper) AppendParamsList(jobID, elemID int64, startKey, endKey string) {
	w.params = append(w.params, jobID, elemID, startKey, endKey, uint64(100))
}
func (w *recordingDelRangeWrapper) ConsumeDeleteRange(_ context.Context, _ string) error {
	w.batches++
	return nil
}

func TestAddDelRangeJobInternalV1TruncatePartition(t *testing.T) {
	job := &model.Job{
		ID:      777,
		Version: model.JobVersion1,
		Type:    model.ActionTruncateTablePartition,
		State:   model.JobStateDone,
	}
	job.FillFinishedArgs(&model.TruncateTableArgs{
		OldPartitionIDs:        []int64{101, 102},
		NewPartitionIDs:        []int64{201, 202},
		AffinityGroupsPrepared: true,
		OldAffinityGroupIDs:    []string{"_tidb_pt_100_p101", "_tidb_pt_100_p102"},
	})
	raw, err := job.Encode(true)
	require.NoError(t, err)

	decoded := &model.Job{}
	require.NoError(t, decoded.Decode(raw))

	// Both GetTruncateTableArgs and AddDelRangeJobInternal must decode the V1
	// finished payload cleanly and register the delete-range batch for both old partitions.
	trArgs, err := model.GetTruncateTableArgs(decoded)
	require.NoError(t, err)
	require.Equal(t, []int64{101, 102}, trArgs.OldPartitionIDs)
	require.True(t, trArgs.AffinityGroupsPrepared)
	require.Equal(t, []string{"_tidb_pt_100_p101", "_tidb_pt_100_p102"}, trArgs.OldAffinityGroupIDs)

	wrapper := &recordingDelRangeWrapper{}
	require.NoError(t, AddDelRangeJobInternal(context.Background(), wrapper, decoded))
	require.Equal(t, 1, wrapper.batches)
	require.Len(t, wrapper.params, 2*5)
}
