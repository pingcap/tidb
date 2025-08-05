// Copyright 2025 PingCAP, Inc.
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

package issyncer

import (
	"context"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestLoadFromTS(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	l := newLoader(store, infoschema.NewCache(nil, 1), nil)
	ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err := l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas := is.AllSchemas()
	// only 2 memory schemas are there
	require.Len(t, allSchemas, 2)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)

	// hit cache
	_, hitCache, _, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	require.True(t, hitCache)
	require.Nil(t, changes)

	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
		require.NoError(t, mu.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: ast.NewCIStr("t"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: 1,
			TableID:  1,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	l = newLoader(store, infoschema.NewCache(nil, 1), nil)
	is, hitCache, oldSchemaVersion, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas = is.AllSchemas()
	require.EqualValues(t, 1, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 3)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "test" }))
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)
	tbls, err := is.SchemaTableInfos(ctx, ast.NewCIStr("test"))
	require.NoError(t, err)
	require.Len(t, tbls, 1)
	require.Equal(t, "t", tbls[0].Name.L)

	// load from diff
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		require.NoError(t, mu.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: ast.NewCIStr("t1"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: 1,
			TableID:  2,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas = is.AllSchemas()
	require.EqualValues(t, 2, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 3)
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "information_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "metrics_schema" }))
	require.True(t, slices.ContainsFunc(allSchemas, func(s *model.DBInfo) bool { return s.Name.L == "test" }))
	require.False(t, hitCache)
	require.EqualValues(t, 1, oldSchemaVersion)
	require.Len(t, changes.PhyTblIDS, 1)
	tbls, err = is.SchemaTableInfos(ctx, ast.NewCIStr("test"))
	require.NoError(t, err)
	require.Len(t, tbls, 2)
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t" }))
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t1" }))
}

func TestLoadFromTSForCrossKS(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	l := NewLoaderForCrossKS(store, infoschema.NewCache(nil, 1))
	ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	_, _, _, _, err = l.LoadWithTS(ver.Ver, false)
	require.ErrorContains(t, err, "system database not found")

	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)
	systemDBID := metadef.SystemDatabaseID
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: systemDBID, Name: ast.NewCIStr(mysql.SystemDB)}))
		testTblID := metadef.ReservedGlobalIDUpperBound - 1
		require.NoError(t, mu.CreateTableOrView(systemDBID, &model.TableInfo{ID: testTblID, Name: ast.NewCIStr("t"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: systemDBID,
			TableID:  testTblID,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err := l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas := is.AllSchemas()
	require.EqualValues(t, 1, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 1)
	require.Equal(t, mysql.SystemDB, allSchemas[0].Name.L)
	require.Equal(t, systemDBID, allSchemas[0].ID)
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)
	tbls, err := is.SchemaTableInfos(ctx, ast.NewCIStr(mysql.SystemDB))
	require.NoError(t, err)
	require.Len(t, tbls, 1)
	require.Equal(t, "t", tbls[0].Name.L)

	// load from diff, diff of non-reserved table ID is not loaded
	require.NoError(t, tidbkv.RunInNewTxn(ctx, l.store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		testTblID := metadef.ReservedGlobalIDUpperBound - 2
		require.NoError(t, mu.CreateTableOrView(systemDBID, &model.TableInfo{ID: testTblID, Name: ast.NewCIStr("t1"), State: model.StatePublic}))
		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: systemDBID,
			TableID:  testTblID,
		}))

		require.NoError(t, mu.CreateTableOrView(systemDBID, &model.TableInfo{ID: 100, Name: ast.NewCIStr("t100"), State: model.StatePublic}))
		schVer, err = mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: systemDBID,
			TableID:  100,
		}))
		return nil
	}))
	ver, err = store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)
	is, hitCache, oldSchemaVersion, changes, err = l.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	allSchemas = is.AllSchemas()
	require.EqualValues(t, 3, is.SchemaMetaVersion())
	require.Len(t, allSchemas, 1)
	require.Equal(t, mysql.SystemDB, allSchemas[0].Name.L)
	require.Equal(t, systemDBID, allSchemas[0].ID)
	require.False(t, hitCache)
	require.EqualValues(t, 1, oldSchemaVersion)
	require.Len(t, changes.PhyTblIDS, 1)
	tbls, err = is.SchemaTableInfos(ctx, ast.NewCIStr(mysql.SystemDB))
	require.NoError(t, err)
	require.Len(t, tbls, 2)
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t" }))
	require.True(t, slices.ContainsFunc(tbls, func(t *model.TableInfo) bool { return t.Name.L == "t1" }))
}

type testStoreWithKS struct {
	tidbkv.Storage
}

func (testStoreWithKS) GetKeyspace() string {
	return "test_ks"
}

func TestLoaderSkipLoadingDiff(t *testing.T) {
	syncer := New(nil, nil, 0, nil, nil)
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{TableID: 100}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{OldTableID: 100}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{TableID: 100, OldTableID: 100}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{TableID: metadef.ReservedGlobalIDUpperBound}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{OldTableID: metadef.ReservedGlobalIDUpperBound}))
	require.False(t, syncer.loader.skipLoadingDiff(&model.SchemaDiff{TableID: metadef.ReservedGlobalIDUpperBound,
		OldTableID: metadef.ReservedGlobalIDUpperBound}))

	loaderForCrossKS := NewLoaderForCrossKS(testStoreWithKS{}, nil)
	require.True(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{}))
	require.True(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{TableID: 100}))
	require.True(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{OldTableID: 100}))
	require.True(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{TableID: 100, OldTableID: 100}))
	require.False(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{TableID: metadef.ReservedGlobalIDUpperBound}))
	require.False(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{OldTableID: metadef.ReservedGlobalIDUpperBound}))
	require.False(t, loaderForCrossKS.skipLoadingDiff(&model.SchemaDiff{TableID: metadef.ReservedGlobalIDUpperBound,
		OldTableID: metadef.ReservedGlobalIDUpperBound}))
}
