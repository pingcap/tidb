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

// testNameFilter is a lightweight Filter implementation for tests, mimicking
// BR behaviour without importing BR packages to avoid cycles.
type testNameFilter struct {
	allow func(ast.CIStr) bool
}

func newTestNameFilter(fn func(ast.CIStr) bool) Filter {
	if fn == nil {
		return nil
	}
	return &testNameFilter{allow: fn}
}

func (f *testNameFilter) SkipLoadDiff(diff *model.SchemaDiff, latestIS infoschema.InfoSchema) bool {
	if f == nil || f.allow == nil {
		return false
	}
	if diff.Type == model.ActionCreateSchema || diff.Type == model.ActionCreatePlacementPolicy {
		return false
	}
	if diff.SchemaID == 0 {
		return false
	}
	if latestIS == nil {
		return true
	}
	schema, ok := latestIS.SchemaByID(diff.SchemaID)
	selected := ok && f.allow(schema.Name)
	return !selected
}

func (f *testNameFilter) SkipLoadSchema(dbInfo *model.DBInfo) bool {
	if f == nil || f.allow == nil || dbInfo == nil {
		return false
	}
	return !f.allow(dbInfo.Name)
}

func (f *testNameFilter) SkipMDLCheck(tableIDs map[int64]struct{}, latestIS infoschema.InfoSchema) bool {
	if f == nil || f.allow == nil || latestIS == nil {
		return false
	}
	for id := range tableIDs {
		db, ok := latestIS.SchemaByID(id)
		if !(ok && f.allow(db.Name)) {
			return false
		}
	}
	return true
}

func TestLoadFromTS(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	l := newLoader(store, infoschema.NewCache(nil, 1), nil, nil)
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
	l = newLoader(store, infoschema.NewCache(nil, 1), nil, nil)
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
	syncer := New(nil, nil, 0, nil, nil, nil)
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

func TestLoaderSkipLoadingDiffForBR(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)

	// Create databases: system database, BR-related database, and user database
	require.NoError(t, tidbkv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		// Create mysql (system database) with ID 1
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr(mysql.SystemDB)}))
		// Create BR-related database with ID 2
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 2, Name: ast.NewCIStr("__TiDB_BR_Temporary_test")}))
		// Create user database with ID 3
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 3, Name: ast.NewCIStr("userdb")}))
		return nil
	}))

	ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)

	// Create loader for BR with a filter function that only loads system and BR-related databases
	brFilter := newTestNameFilter(func(dbName ast.CIStr) bool {
		return metadef.IsSystemDB(dbName.L) || metadef.IsBRRelatedDB(dbName.O)
	})
	loaderForBR := newLoader(store, infoschema.NewCache(nil, 1), nil, brFilter)
	require.NotNil(t, loaderForBR.filter)

	// Load initial schema to populate the cache
	_, _, _, _, err = loaderForBR.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)

	// Test case 1: Schema diff for system database - should NOT skip
	systemDiff := &model.SchemaDiff{
		SchemaID: 1, // mysql system database
		TableID:  10,
	}
	require.False(t, loaderForBR.skipLoadingDiff(systemDiff), "should NOT skip diff for system database")

	// Test case 1.1: CREATE DATABASE diff should always pass through the filter
	createDBDiff := &model.SchemaDiff{
		Type:     model.ActionCreateSchema,
		SchemaID: 4,
	}
	require.False(t, loaderForBR.skipLoadingDiff(createDBDiff), "should NOT skip CREATE DATABASE diffs even when the schema name is unknown")

	// Test case 2: Schema diff for BR-related database - should NOT skip
	brDiff := &model.SchemaDiff{
		SchemaID: 2, // BR-related database
		TableID:  20,
	}
	require.False(t, loaderForBR.skipLoadingDiff(brDiff), "should NOT skip diff for BR-related database")

	// Test case 3: Schema diff for user database - should skip
	userDiff := &model.SchemaDiff{
		SchemaID: 3, // user database
		TableID:  30,
	}
	require.True(t, loaderForBR.skipLoadingDiff(userDiff), "should skip diff for user database")

	// Test case 4: Schema diff with OldSchemaID for system database - still skipped because selection is based on SchemaID
	oldSystemDiff := &model.SchemaDiff{
		OldSchemaID: 1, // mysql system database
		SchemaID:    3, // user database
		TableID:     40,
	}
	require.True(t, loaderForBR.skipLoadingDiff(oldSystemDiff), "should skip diff when SchemaID is filtered out even if OldSchemaID is system database")

	// Test case 5: Schema diff with OldSchemaID for BR-related database - still skipped because SchemaID is filtered out
	oldBRDiff := &model.SchemaDiff{
		OldSchemaID: 2, // BR-related database
		SchemaID:    3, // user database
		TableID:     50,
	}
	require.True(t, loaderForBR.skipLoadingDiff(oldBRDiff), "should skip diff when SchemaID is filtered out even if OldSchemaID is BR-related database")

	// Test case 6: Schema diff with both SchemaID and OldSchemaID as user databases - should skip
	userToUserDiff := &model.SchemaDiff{
		OldSchemaID: 3, // user database
		SchemaID:    3, // user database
		TableID:     60,
	}
	require.True(t, loaderForBR.skipLoadingDiff(userToUserDiff), "should skip diff when both SchemaID and OldSchemaID are user databases")

	// Test case 7: Schema diff with non-existent SchemaID - should skip
	nonExistentDiff := &model.SchemaDiff{
		SchemaID: 999, // non-existent database
		TableID:  70,
	}
	require.True(t, loaderForBR.skipLoadingDiff(nonExistentDiff), "should skip diff for non-existent database")
}

func TestLoadForBR(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)

	// Create multiple databases: system database, BR-related database, and user database
	require.NoError(t, tidbkv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn tidbkv.Transaction) error {
		mu := meta.NewMutator(txn)
		// Create mysql (system database)
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr(mysql.SystemDB)}))
		require.NoError(t, mu.CreateTableOrView(1, &model.TableInfo{ID: 1, Name: ast.NewCIStr("t1"), State: model.StatePublic}))

		// Create a BR-related temporary database
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 2, Name: ast.NewCIStr("__TiDB_BR_Temporary_test")}))
		require.NoError(t, mu.CreateTableOrView(2, &model.TableInfo{ID: 2, Name: ast.NewCIStr("t2"), State: model.StatePublic}))

		// Create a regular user database
		require.NoError(t, mu.CreateDatabase(&model.DBInfo{ID: 3, Name: ast.NewCIStr("userdb")}))
		require.NoError(t, mu.CreateTableOrView(3, &model.TableInfo{ID: 3, Name: ast.NewCIStr("t3"), State: model.StatePublic}))

		schVer, err := mu.GenSchemaVersion()
		require.NoError(t, err)
		require.NoError(t, mu.SetSchemaDiff(&model.SchemaDiff{
			Version:  schVer,
			Type:     model.ActionCreateTable,
			SchemaID: 3,
			TableID:  3,
		}))
		return nil
	}))

	ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
	require.NoError(t, err)

	// Test with BR filter (only load system and BR-related databases)
	brFilter := newTestNameFilter(func(dbName ast.CIStr) bool {
		return metadef.IsSystemDB(dbName.L) || metadef.IsBRRelatedDB(dbName.O)
	})
	loaderForBR := newLoader(store, infoschema.NewCache(nil, 1), nil, brFilter)
	is, hitCache, oldSchemaVersion, changes, err := loaderForBR.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)

	// Verify only system databases and BR-related databases are loaded
	allSchemas := is.AllSchemas()
	schemaNames := make(map[string]bool)
	for _, schema := range allSchemas {
		schemaNames[schema.Name.L] = true
	}

	// Should include system database
	require.True(t, schemaNames[mysql.SystemDB], "system database should be loaded")
	// Should include BR-related database
	require.True(t, schemaNames["__tidb_br_temporary_test"], "BR-related database should be loaded")
	// Should NOT include user database
	require.False(t, schemaNames["userdb"], "user database should NOT be loaded for BR")
	// Should include memory schemas
	require.True(t, schemaNames["information_schema"], "information_schema should be loaded")
	require.True(t, schemaNames["metrics_schema"], "metrics_schema should be loaded")

	// Verify tables in system database
	tbls, err := is.SchemaTableInfos(ctx, ast.NewCIStr(mysql.SystemDB))
	require.NoError(t, err)
	require.Len(t, tbls, 1)
	require.Equal(t, "t1", tbls[0].Name.L)

	// Verify tables in BR-related database
	tbls, err = is.SchemaTableInfos(ctx, ast.NewCIStr("__TiDB_BR_Temporary_test"))
	require.NoError(t, err)
	require.Len(t, tbls, 1)
	require.Equal(t, "t2", tbls[0].Name.L)

	// Verify user database is not accessible
	tbls, err = is.SchemaTableInfos(ctx, ast.NewCIStr("userdb"))
	require.NoError(t, err)
	require.Len(t, tbls, 0)

	// Test with no filter (load all databases)
	loaderNormal := newLoader(store, infoschema.NewCache(nil, 1), nil, nil)
	isNormal, hitCache, oldSchemaVersion, changes, err := loaderNormal.LoadWithTS(ver.Ver, false)
	require.NoError(t, err)
	require.False(t, hitCache)
	require.Zero(t, oldSchemaVersion)
	require.Nil(t, changes)

	// Verify all databases are loaded when no filter is used
	allSchemasNormal := isNormal.AllSchemas()
	schemaNamesNormal := make(map[string]bool)
	for _, schema := range allSchemasNormal {
		schemaNamesNormal[schema.Name.L] = true
	}

	// Should include all databases
	require.True(t, schemaNamesNormal[mysql.SystemDB], "system database should be loaded")
	require.True(t, schemaNamesNormal["__tidb_br_temporary_test"], "BR-related database should be loaded")
	require.True(t, schemaNamesNormal["userdb"], "user database should be loaded")
	require.True(t, schemaNamesNormal["information_schema"], "information_schema should be loaded")
	require.True(t, schemaNamesNormal["metrics_schema"], "metrics_schema should be loaded")
}
