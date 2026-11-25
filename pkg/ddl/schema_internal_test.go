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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/ddl/label"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestDropPartitionPropagatesMetadataUpdateError(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr", "return(1)")

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		metaMut := meta.NewMutator(txn)
		require.NoError(t, metaMut.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
		definitions := []model.PartitionDefinition{
			{ID: 3, Name: ast.NewCIStr("p0"), LessThan: []string{"10"}},
			{ID: 4, Name: ast.NewCIStr("p1"), LessThan: []string{"20"}},
		}
		tblInfo := &model.TableInfo{
			ID: 2, Name: ast.NewCIStr("t"), State: model.StatePublic,
			Partition: &model.PartitionInfo{
				Type: ast.PartitionTypeRange, Enable: true, Num: 2,
				Definitions: definitions, DroppingDefinitions: definitions[:1],
				DDLState: model.StateWriteOnly, DDLAction: model.ActionDropTablePartition,
			},
		}
		require.NoError(t, metaMut.CreateTableOrView(1, tblInfo))
		job := &model.Job{
			Version: model.GetJobVerInUse(), Type: model.ActionDropTablePartition,
			SchemaID: 1, SchemaName: "test", TableID: 2, TableName: "t",
			State: model.JobStateRunning, SchemaState: model.StateWriteOnly,
		}
		job.FillArgs(&model.TablePartitionArgs{PartNames: []string{"p0"}})
		_, err := job.Encode(true)
		require.NoError(t, err)
		jobCtx := &jobContext{metaMut: metaMut, store: store, stepCtx: ctx}

		_, err = (&worker{}).onDropTablePartition(jobCtx, job)
		require.ErrorContains(t, err, "mock update version and tableInfo error")
		return nil
	})
	require.NoError(t, err)
}

func TestAppendPhysicalTableIDs(t *testing.T) {
	table := &model.TableInfo{
		ID: 100,
		Partition: &model.PartitionInfo{
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 101},
				{ID: 102},
			},
		},
	}

	ids := appendPhysicalTableIDs([]int64{1}, table)

	require.Equal(t, []int64{1, 100, 101, 102}, ids)
}

func TestBuildTableLabelRuleOwnership(t *testing.T) {
	codec := tikv.NewCodecV1(tikv.ModeTxn)
	tbl := &model.TableInfo{Name: ast.NewCIStr("t")}
	id := label.NewRuleID(codec, "db", "t", "")
	rule := (&label.Rule{Labels: []pdhttp.RegionLabel{{Key: "a", Value: "table"}}}).Reset(codec, "old_db", "old_t", "", 100)
	before := rule.String()
	ids := []int64{30, 10, 20}
	result := buildTableLabelRule(codec, "db", tbl, ids, map[string]*label.Rule{id: rule})
	require.Equal(t, id, result.ID)
	require.Equal(t, before, rule.String())
	require.Equal(t, []int64{30, 10, 20}, ids)
	require.Nil(t, buildTableLabelRule(codec, "db", tbl, ids, nil))
}

func TestRestoreOriginalPartitionDefinitions(t *testing.T) {
	t.Run("nonadjacent LIST partitions with reused names", func(t *testing.T) {
		current := []model.PartitionDefinition{
			{ID: 100, Name: ast.NewCIStr("p1"), InValues: [][]string{{"10", "20"}}},
			{ID: 10, Name: ast.NewCIStr("p0"), InValues: [][]string{{"0"}}},
			{ID: 30, Name: ast.NewCIStr("p2"), InValues: [][]string{{"2"}}},
			{ID: 101, Name: ast.NewCIStr("p3"), InValues: [][]string{{"30", "40"}}},
			{ID: 50, Name: ast.NewCIStr("p4"), InValues: [][]string{{"4"}}},
			{ID: 102, Name: ast.NewCIStr("p5"), InValues: [][]string{{"50"}}},
		}
		dropping := []model.PartitionDefinition{
			{ID: 20, Name: ast.NewCIStr("p1"), InValues: [][]string{{"1"}}},
			{ID: 40, Name: ast.NewCIStr("p3"), InValues: [][]string{{"3"}}},
		}
		originalIDs := []int64{10, 20, 30, 40, 50}

		restored, err := restoreOriginalPartitionDefinitions(originalIDs, current, dropping)

		require.NoError(t, err)
		require.Equal(t, []model.PartitionDefinition{
			{ID: 10, Name: ast.NewCIStr("p0"), InValues: [][]string{{"0"}}},
			{ID: 20, Name: ast.NewCIStr("p1"), InValues: [][]string{{"1"}}},
			{ID: 30, Name: ast.NewCIStr("p2"), InValues: [][]string{{"2"}}},
			{ID: 40, Name: ast.NewCIStr("p3"), InValues: [][]string{{"3"}}},
			{ID: 50, Name: ast.NewCIStr("p4"), InValues: [][]string{{"4"}}},
		}, restored)
		require.Equal(t, []int64{10, 20, 30, 40, 50}, originalIDs)
	})

	t.Run("missing original ID", func(t *testing.T) {
		current := []model.PartitionDefinition{{ID: 1}}
		dropping := []model.PartitionDefinition{{ID: 3}}

		restored, err := restoreOriginalPartitionDefinitions([]int64{1, 2, 3}, current, dropping)

		require.ErrorContains(t, err, "missing partition ID 2")
		require.Nil(t, restored)
	})
}

func TestClonePartitionDefinitions(t *testing.T) {
	t.Run("RANGE", func(t *testing.T) {
		definitions := []model.PartitionDefinition{{ID: 1, LessThan: []string{"10"}}}
		cloned := clonePartitionDefinitions(definitions)
		cloned[0].ID = 2
		cloned[0].LessThan[0] = "20"
		require.Equal(t, []model.PartitionDefinition{{ID: 1, LessThan: []string{"10"}}}, definitions)
	})
	t.Run("LIST", func(t *testing.T) {
		definitions := []model.PartitionDefinition{{ID: 1, InValues: [][]string{{"1", "2"}, {"3", "4"}}}}
		cloned := clonePartitionDefinitions(definitions)
		cloned[0].InValues[0][0] = "changed"
		cloned[0].InValues[1] = []string{"replaced"}
		require.Equal(t, []model.PartitionDefinition{{ID: 1, InValues: [][]string{{"1", "2"}, {"3", "4"}}}}, definitions)
	})
}

func TestRemovePartitionIndexes(t *testing.T) {
	columns := []*model.ColumnInfo{
		{ID: 1, Name: ast.NewCIStr("a"), Offset: 0},
		{ID: 2, Name: ast.NewCIStr("b"), Offset: 1},
		{ID: 3, Name: ast.NewCIStr("_shared"), Offset: 2, Hidden: true},
		{ID: 4, Name: ast.NewCIStr("_private"), Offset: 3, Hidden: true},
		{ID: 5, Name: ast.NewCIStr("tail"), Offset: 4},
	}
	index := func(id int64, name string, column *model.ColumnInfo, global, unique bool) *model.IndexInfo {
		return &model.IndexInfo{
			ID: id, Name: ast.NewCIStr(name), State: model.StatePublic,
			Global: global, Unique: unique,
			Columns: []*model.IndexColumn{{Name: column.Name, Offset: column.Offset}},
		}
	}
	oldGlobal := index(1, "idx_a", columns[0], true, false)
	newGlobal := index(2, "idx_a", columns[0], true, false)
	localUnique := index(3, "idx_b", columns[1], false, true)
	privateExpression := index(4, "idx_private", columns[3], false, false)
	oldExpression := index(5, "idx_shared", columns[2], true, false)
	newExpression := index(6, "idx_shared", columns[2], true, false)
	unrelated := index(7, "idx_tail", columns[4], false, false)
	unrelated.State = model.StateDeleteOnly
	tblInfo := &model.TableInfo{
		ID: 100, Columns: append([]*model.ColumnInfo(nil), columns...),
		Indices: []*model.IndexInfo{oldGlobal, newGlobal, localUnique, privateExpression, oldExpression, newExpression, unrelated},
	}
	for _, idx := range tblInfo.Indices {
		AddIndexColumnFlag(tblInfo, idx)
	}

	cleanup := removePartitionIndexes(tblInfo, []*model.IndexInfo{oldGlobal, localUnique, privateExpression, oldExpression})
	// A partitioning change may replace the table ID after collecting cleanup IDs.
	tblInfo.ID = 200

	require.Equal(t, []model.TableIDIndexID{{TableID: 100, IndexID: 1}, {TableID: 100, IndexID: 5}}, cleanup)
	require.Equal(t, []*model.IndexInfo{newGlobal, newExpression, unrelated}, tblInfo.Indices)
	require.Equal(t, model.StateDeleteOnly, unrelated.State)
	require.Equal(t, []*model.ColumnInfo{columns[0], columns[1], columns[2], columns[4]}, tblInfo.Columns)
	require.True(t, mysql.HasMultipleKeyFlag(columns[0].GetFlag()))
	require.False(t, mysql.HasUniKeyFlag(columns[1].GetFlag()))
	require.True(t, mysql.HasMultipleKeyFlag(columns[2].GetFlag()))
	require.Equal(t, 3, columns[4].Offset)
	require.Equal(t, 3, unrelated.Columns[0].Offset)
	for offset, col := range tblInfo.Columns {
		require.Equal(t, offset, col.Offset)
	}
}

type keyspacedPDStoreForTest struct {
	kv.Storage
	kv.StorageWithPD
	codec tikv.Codec
}

func (s *keyspacedPDStoreForTest) GetCodec() tikv.Codec {
	return s.codec
}

type recordingDelRangeManagerForTest struct {
	mockDelRange
	jobIDs []int64
}

func (m *recordingDelRangeManagerForTest) removeFromGCDeleteRangeWithSession(_ context.Context, _ sessionctx.Context, jobID int64) error {
	m.jobIDs = append(m.jobIDs, jobID)
	return nil
}

func TestRecoverGCBarrierUsesActualKeyspaceAndRenews(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	pdStore, ok := store.(kv.StorageWithPD)
	require.True(t, ok)

	codecV2, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)

	ctx := context.Background()
	nullKeyspaceID := uint32(store.GetCodec().GetKeyspaceID())
	// Advance NullKeyspace's txn safe point past snapshotTS so using NullKeyspaceID would fail.
	_, err = pdStore.GetPDClient().GetGCInternalController(nullKeyspaceID).AdvanceTxnSafePoint(ctx, 200)
	require.NoError(t, err)

	ksStore := &keyspacedPDStoreForTest{
		Storage:       store,
		StorageWithPD: pdStore,
		codec:         codecV2,
	}
	w := &worker{ddlCtx: &ddlCtx{store: ksStore}}
	job := &model.Job{ID: 1001}

	require.NoError(t, setRecoverGCBarrier(ctx, w, job.ID, 100))

	ksState, err := pdStore.GetPDClient().GetGCStatesClient(42).GetGCState(ctx, pdgc.ExcludeGCBarriers(false))
	require.NoError(t, err)
	require.True(t, ksState.HasGCBarriers())
	barriers, err := ksState.GetGCBarriers()
	require.NoError(t, err)
	require.Len(t, barriers, 1)
	require.Equal(t, recoverGCBarrierID(42, job.ID), barriers[0].BarrierID)
	require.Equal(t, uint64(100), barriers[0].BarrierTS)

	nullState, err := pdStore.GetPDClient().GetGCStatesClient(nullKeyspaceID).GetGCState(ctx, pdgc.ExcludeGCBarriers(false))
	require.NoError(t, err)
	require.True(t, nullState.HasGCBarriers())
	nullBarriers, err := nullState.GetGCBarriers()
	require.NoError(t, err)
	require.Empty(t, nullBarriers)

	// Background renewer updates the barrier independently while work runs.
	origRenewInterval := recoverGCBarrierRenewInterval
	recoverGCBarrierRenewInterval = 15 * time.Millisecond
	t.Cleanup(func() { recoverGCBarrierRenewInterval = origRenewInterval })

	_, stopRenew, renewErr := startRecoverGCBarrierRenewer(ctx, w, job.ID, 150)
	require.Eventually(t, func() bool {
		ksState, err := pdStore.GetPDClient().GetGCStatesClient(42).GetGCState(ctx, pdgc.ExcludeGCBarriers(false))
		if err != nil {
			return false
		}
		barriers, err := ksState.GetGCBarriers()
		return err == nil && len(barriers) == 1 && barriers[0].BarrierTS == 150
	}, 2*time.Second, 10*time.Millisecond)
	stopRenew()
	require.NoError(t, renewErr())

	require.NoError(t, deleteRecoverGCBarrier(ctx, w, job.ID))
	ksState, err = pdStore.GetPDClient().GetGCStatesClient(42).GetGCState(ctx, pdgc.ExcludeGCBarriers(false))
	require.NoError(t, err)
	barriers, err = ksState.GetGCBarriers()
	require.NoError(t, err)
	require.Empty(t, barriers)
}

type mockSafePointContext struct {
	*mock.Context
	safePoint string
}

func (c *mockSafePointContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return c
}

func (c *mockSafePointContext) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, _ string, _ ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeVarchar)}, 1)
	chk.AppendString(0, c.safePoint)
	return []chunk.Row{chk.GetRow(0)}, nil, nil
}

type recordingGCBarrierPDClient struct {
	pd.Client
	onSetBarrier func(call int, barrierID string, barrierTS uint64, ttl time.Duration) error
	calls        atomic.Int64
}

func (c *recordingGCBarrierPDClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &recordingGCStatesClient{
		GCStatesClient: c.Client.GetGCStatesClient(keyspaceID),
		parent:         c,
	}
}

type recordingGCStatesClient struct {
	pdgc.GCStatesClient
	parent *recordingGCBarrierPDClient
}

func (c *recordingGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	call := int(c.parent.calls.Add(1))
	if c.parent.onSetBarrier != nil {
		if err := c.parent.onSetBarrier(call, barrierID, barrierTS, ttl); err != nil {
			return nil, err
		}
	}
	return c.GCStatesClient.SetGCBarrier(ctx, barrierID, barrierTS, ttl)
}

type blockingListTablesStore struct {
	kv.Storage
	kv.StorageWithPD
	pdCli         pd.Client
	targetSnapTS  uint64
	enteredListCh chan struct{}
	releaseListCh chan struct{}
	enteredOnce   sync.Once
}

func (s *blockingListTablesStore) GetPDClient() pd.Client {
	return s.pdCli
}

func (s *blockingListTablesStore) GetSnapshot(ver kv.Version) kv.Snapshot {
	snap := s.Storage.GetSnapshot(ver)
	if ver.Ver != s.targetSnapTS {
		return snap
	}
	return &blockingSnapshot{
		Snapshot: snap,
		store:    s,
	}
}

type blockingSnapshot struct {
	kv.Snapshot
	store *blockingListTablesStore
}

func (s *blockingSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	s.store.enteredOnce.Do(func() {
		close(s.store.enteredListCh)
	})
	<-s.store.releaseListCh
	return s.Snapshot.Iter(k, upperBound)
}

func TestRecoverSchemaRenewsGCBarrierWhilePreparationBlocked(t *testing.T) {
	origRenewInterval := recoverGCBarrierRenewInterval
	recoverGCBarrierRenewInterval = 20 * time.Millisecond
	t.Cleanup(func() { recoverGCBarrierRenewInterval = origRenewInterval })

	setupRecoverSchemaEnv := func(t *testing.T, onSetBarrier func(call int, barrierID string, barrierTS uint64, ttl time.Duration) error) (
		*worker,
		*blockingListTablesStore,
		*recordingGCBarrierPDClient,
		*recordingDelRangeManagerForTest,
		uint64,
	) {
		store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		pdStore, ok := store.(kv.StorageWithPD)
		require.True(t, ok)

		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		dbInfo := &model.DBInfo{ID: 100, Name: ast.NewCIStr("recover_db"), State: model.StatePublic}
		tblInfo := &model.TableInfo{ID: 101, Name: ast.NewCIStr("t1"), State: model.StatePublic}
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			require.NoError(t, m.CreateDatabase(dbInfo))
			return m.CreateTableOrView(dbInfo.ID, tblInfo)
		}))

		snapTS, err := store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		require.NoError(t, err)

		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			return meta.NewMutator(txn).DropDatabase(dbInfo.ID)
		}))

		recPD := &recordingGCBarrierPDClient{
			Client:       pdStore.GetPDClient(),
			onSetBarrier: onSetBarrier,
		}
		wrappedStore := &blockingListTablesStore{
			Storage:       store,
			StorageWithPD: pdStore,
			pdCli:         recPD,
			targetSnapTS:  snapTS,
			enteredListCh: make(chan struct{}),
			releaseListCh: make(chan struct{}),
		}

		rs := pools.NewResourcePool(func() (pools.Resource, error) {
			mctx := mock.NewContext()
			mctx.GetSessionVars().GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
			require.NoError(t, mctx.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVarOnly(context.Background(), vardef.TiDBGCEnable, vardef.Off, false))
			return &mockSafePointContext{
				Context:   mctx,
				safePoint: time.Unix(0, 0).UTC().Format("20060102-15:04:05 -0700 MST"),
			}, nil
		}, 2, 2, 0)
		t.Cleanup(rs.Close)

		delMgr := &recordingDelRangeManagerForTest{}
		w := &worker{
			ddlCtx:          &ddlCtx{store: wrappedStore},
			sessPool:        sess.NewSessionPool(rs),
			sess:            sess.NewSession(mock.NewContext()),
			delRangeManager: delMgr,
		}
		return w, wrappedStore, recPD, delMgr, snapTS
	}

	buildJob := func(snapTS uint64) *model.Job {
		job := &model.Job{
			Version:     model.GetJobVerInUse(),
			Type:        model.ActionRecoverSchema,
			SchemaID:    100,
			SchemaName:  "recover_db",
			State:       model.JobStateRunning,
			SchemaState: model.StateWriteOnly,
			ID:          2001,
			BinlogInfo:  &model.HistoryInfo{},
		}
		job.FillArgs(&model.RecoverArgs{
			RecoverInfo: &model.RecoverSchemaInfo{
				DBInfo: &model.DBInfo{
					ID:    100,
					Name:  ast.NewCIStr("recover_db"),
					State: model.StateWriteOnly,
				},
				DropJobID:           777,
				SnapshotTS:          snapTS,
				OldSchemaName:       ast.NewCIStr("recover_db"),
				LoadTablesOnExecute: true,
			},
			CheckFlag: recoverCheckFlagDisableGC,
		})
		_, err := job.Encode(true)
		require.NoError(t, err)
		return job
	}

	t.Run("renews while ListTables is blocked", func(t *testing.T) {
		w, wrappedStore, recPD, delMgr, snapTS := setupRecoverSchemaEnv(t, nil)
		job := buildJob(snapTS)

		type recoverResult struct {
			err error
		}
		doneCh := make(chan recoverResult, 1)
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		go func() {
			err := kv.RunInNewTxn(ctx, wrappedStore, false, func(txnCtx context.Context, txn kv.Transaction) error {
				jobCtx := &jobContext{
					metaMut:              meta.NewMutator(txn),
					store:                wrappedStore,
					stepCtx:              txnCtx,
					schemaVersionManager: newSchemaVersionManager(wrappedStore),
				}
				defer jobCtx.unlockSchemaVersion(jobCtx, job.ID)
				_, err := w.onRecoverSchema(jobCtx, job)
				return err
			})
			doneCh <- recoverResult{err: err}
		}()

		select {
		case <-wrappedStore.enteredListCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for snapMeta.ListTables")
		}

		// Wait for at least two background renewals while ListTables remains blocked.
		require.Eventually(t, func() bool {
			return recPD.calls.Load() >= 3
		}, 5*time.Second, 5*time.Millisecond)

		keyspaceID := uint32(wrappedStore.GetCodec().GetKeyspaceID())
		gcState, err := recPD.GetGCStatesClient(keyspaceID).GetGCState(ctx, pdgc.ExcludeGCBarriers(false))
		require.NoError(t, err)
		barriers, err := gcState.GetGCBarriers()
		require.NoError(t, err)
		require.Len(t, barriers, 1)
		require.Equal(t, recoverGCBarrierID(keyspaceID, job.ID), barriers[0].BarrierID)
		require.Equal(t, snapTS, barriers[0].BarrierTS)

		close(wrappedStore.releaseListCh)
		res := <-doneCh
		require.NoError(t, res.err)
		require.Equal(t, model.JobStateDone, job.State)
		require.Equal(t, model.StatePublic, job.SchemaState)
		require.Equal(t, []int64{777}, delMgr.jobIDs)

		callsAfterFinish := recPD.calls.Load()
		time.Sleep(60 * time.Millisecond)
		require.Equal(t, callsAfterFinish, recPD.calls.Load())
	})

	t.Run("cancels preparation and job when renewal fails", func(t *testing.T) {
		renewErr := errors.New("mock PD barrier renewal error")
		w, wrappedStore, recPD, delMgr, snapTS := setupRecoverSchemaEnv(t, func(call int, _ string, _ uint64, _ time.Duration) error {
			if call >= 2 {
				return renewErr
			}
			return nil
		})
		job := buildJob(snapTS)

		type recoverResult struct {
			err error
		}
		doneCh := make(chan recoverResult, 1)
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		go func() {
			err := kv.RunInNewTxn(ctx, wrappedStore, false, func(txnCtx context.Context, txn kv.Transaction) error {
				jobCtx := &jobContext{
					metaMut:              meta.NewMutator(txn),
					store:                wrappedStore,
					stepCtx:              txnCtx,
					schemaVersionManager: newSchemaVersionManager(wrappedStore),
				}
				defer jobCtx.unlockSchemaVersion(jobCtx, job.ID)
				_, err := w.onRecoverSchema(jobCtx, job)
				return err
			})
			doneCh <- recoverResult{err: err}
		}()

		select {
		case <-wrappedStore.enteredListCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for snapMeta.ListTables")
		}

		require.Eventually(t, func() bool {
			return recPD.calls.Load() >= 2
		}, 5*time.Second, 5*time.Millisecond)

		close(wrappedStore.releaseListCh)
		res := <-doneCh
		require.ErrorContains(t, res.err, "mock PD barrier renewal error")
		require.Equal(t, model.JobStateCancelled, job.State)
		require.Empty(t, delMgr.jobIDs)
	})
}

func TestRecoverSchemaRemovesGCDeleteRangeOnce(t *testing.T) {
	delMgr := &recordingDelRangeManagerForTest{}
	w := &worker{
		sess:            sess.NewSession(mock.NewContext()),
		delRangeManager: delMgr,
	}
	jobCtx := &jobContext{stepCtx: context.Background()}

	recoverTbls := []*model.RecoverTableInfo{
		{DropJobID: 777, TableInfo: &model.TableInfo{ID: 1}},
		{DropJobID: 777, TableInfo: &model.TableInfo{ID: 2}},
		{DropJobID: 777, TableInfo: &model.TableInfo{ID: 3}},
	}
	require.NoError(t, w.removeRecoverSchemaGCDeleteRange(jobCtx, 777, recoverTbls))
	require.Equal(t, []int64{777}, delMgr.jobIDs)
	require.True(t, jobCtx.mustRollbackTxnOnError)

	// Legacy fallback when schema DropJobID is 0 still deletes shared DropJobID only once.
	delMgr.jobIDs = nil
	jobCtx.mustRollbackTxnOnError = false
	require.NoError(t, w.removeRecoverSchemaGCDeleteRange(jobCtx, 0, recoverTbls))
	require.Equal(t, []int64{777}, delMgr.jobIDs)
	require.True(t, jobCtx.mustRollbackTxnOnError)
}
