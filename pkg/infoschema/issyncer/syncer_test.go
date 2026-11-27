// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issyncer

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestSyncerSkipMDLCheck(t *testing.T) {
	syncer := New(nil, nil, 0, nil, nil, nil)
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, 456: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{metadef.ReservedGlobalIDUpperBound: {}}, nil))
	require.False(t, syncer.skipMDLCheck(map[int64]struct{}{123: {}, metadef.ReservedGlobalIDUpperBound: {}}, nil))

	cache := infoschema.NewCache(nil, 1)
	db := &model.DBInfo{ID: 2, Name: ast.NewCIStr("mysql"), State: model.StatePublic}
	db.Deprecated.Tables = []*model.TableInfo{
		{ID: 9999, DBID: db.ID, Name: ast.NewCIStr("stats_meta"), State: model.StatePublic},
		{ID: 10000, DBID: db.ID, Name: ast.NewCIStr("custom_table"), State: model.StatePublic},
	}
	builder := infoschema.NewBuilder(nil, 0, nil, infoschema.NewData(), false)
	require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{db}, nil, nil, nil, 1))
	cache.Insert(builder.Build(0), 0)
	syncer = NewCrossKSSyncer(testStoreWithKS{}, cache, 0, nil, nil, "ks1")
	for _, isLegacyNextGen := range []bool{false, true} {
		syncer.loader.legacyNextGen.Store(&isLegacyNextGen)
		require.True(t, syncer.skipMDLCheck(map[int64]struct{}{}, cache.GetLatest()))
		for _, tc := range []struct {
			id                     int64
			skipNative, skipLegacy bool
		}{
			{0, true, true},
			{123, true, true},
			{9999, true, false},
			{10000, true, true},
			{metadef.GlobalPrivTableID, false, true},
		} {
			id, wantSkip := tc.id, tc.skipNative
			if isLegacyNextGen {
				wantSkip = tc.skipLegacy
			}
			if kerneltype.IsClassic() {
				wantSkip = true
			}
			require.Equal(t, wantSkip, syncer.skipMDLCheck(map[int64]struct{}{id: {}}, cache.GetLatest()), "isLegacyNextGen=%t id=%d", isLegacyNextGen, id)
			require.Equal(t, wantSkip, syncer.skipMDLCheck(map[int64]struct{}{123: {}, id: {}}, cache.GetLatest()))
		}
	}
}

func TestSyncerMDLForNewLegacySystemTable(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross-keyspace schema synchronization requires NextGen")
	}
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnAdmin)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		require.NoError(t, m.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr(mysql.SystemDB), State: model.StatePublic}))
		require.NoError(t, m.CreateTableOrView(1, &model.TableInfo{ID: 100, Name: ast.NewCIStr("user"), State: model.StatePublic}))
		require.NoError(t, m.CreateTableOrView(1, &model.TableInfo{ID: 200, Name: ast.NewCIStr("custom_table"), State: model.StatePublic}))
		version, err := m.GenSchemaVersion()
		require.NoError(t, err)
		return m.SetSchemaDiff(&model.SchemaDiff{Version: version, Type: model.ActionCreateTable, SchemaID: 1, TableID: 100})
	}))
	cache := infoschema.NewCache(nil, 1)
	follower := NewCrossKSSyncer(store, cache, 0, nil, nil, "legacy-ks")
	version, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	_, _, _, _, err = follower.LoadWithTS(version.Ver, false)
	require.NoError(t, err)
	require.True(t, follower.loader.isLegacyNextGen())

	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		require.NoError(t, m.CreateTableOrView(1, &model.TableInfo{ID: 101, Name: ast.NewCIStr("global_priv"), State: model.StatePublic}))
		version, err := m.GenSchemaVersion()
		require.NoError(t, err)
		return m.SetSchemaDiff(&model.SchemaDiff{Version: version, Type: model.ActionCreateTable, SchemaID: 1, TableID: 101})
	}))
	version, err = store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	// The owner can discover the built-in and start waiting for assumed servers
	// before their cached schemas contain it.
	owner := newLoader(store, infoschema.NewCache(nil, 1), nil, nil)
	owner.mode = LoadModeFull
	ownerIS, _, _, _, err := owner.LoadWithTS(version.Ver, false)
	require.NoError(t, err)
	require.True(t, infoschema.IsSystemTableInNextGen(ownerIS, true, 101))
	_, exists := cache.GetLatest().TableItemByID(101)
	require.False(t, exists)
	_, hitCache, oldVersion, changes, err := follower.LoadWithTS(version.Ver, false)
	require.NoError(t, err)
	require.False(t, hitCache)
	require.EqualValues(t, 1, oldVersion)
	require.NotNil(t, changes, "the follower must use incremental schema loading")

	const builtinJobID, customJobID = int64(10), int64(11)
	se := &mdlTestSession{rows: &sqlexec.SimpleRecordSet{
		ResultFields: []*resolve.ResultField{
			{Column: &model.ColumnInfo{FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
			{Column: &model.ColumnInfo{FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
			{Column: &model.ColumnInfo{FieldType: *types.NewFieldType(mysql.TypeVarchar)}},
		},
		Rows:         [][]any{{builtinJobID, int64(2), "101"}, {customJobID, int64(2), "200"}},
		MaxChunkSize: 2,
	}}
	pool := util.NewSessionPool(1, func() (pools.Resource, error) { return se, nil }, nil, nil, nil)
	t.Cleanup(pool.Close)
	versionSyncer := schemaver.NewMemSyncer()
	require.NoError(t, versionSyncer.Init(ctx))
	follower.sysSessionPool = pool
	follower.SetMinJobIDRefresher(systable.NewMinJobIDRefresher(nil))
	follower.InitRequiredFields(func() sessmgr.InfoSchemaCoordinator { return nil }, versionSyncer, nil, nil)
	follower.refreshMDLCheckTableInfo(ctx)
	require.Contains(t, follower.mdlCheckTableInfo.jobs, builtinJobID)
	require.NotContains(t, follower.mdlCheckTableInfo.jobs, customJobID)

	loopCtx, cancel := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		follower.MDLCheckLoop(loopCtx)
	}()
	t.Cleanup(func() {
		cancel()
		<-loopDone
	})
	select {
	case follower.mdlCheckCh <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("MDL checker did not start")
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	_, err = versionSyncer.WaitVersionSynced(waitCtx, builtinJobID, 2, true)
	require.NoError(t, err, "the assumed server must acknowledge the newly discovered built-in")
}

// mdlTestSession supplies the persisted MDL rows without starting a SQL server.
type mdlTestSession struct {
	sessionctx.Context
	sqlexec.SQLExecutor
	rows *sqlexec.SimpleRecordSet
}

func (s *mdlTestSession) GetSQLExecutor() sqlexec.SQLExecutor { return s }

func (s *mdlTestSession) ExecuteInternal(_ context.Context, sql string, _ ...any) (sqlexec.RecordSet, error) {
	if sql == "rollback" {
		return nil, nil
	}
	return s.rows, nil
}

func (*mdlTestSession) Close() {}
