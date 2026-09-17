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

	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
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
	t.Run("system_db", func(t *testing.T) {
		systemDiff := &model.SchemaDiff{
			SchemaID: 1, // mysql system database
			TableID:  10,
		}
		require.False(t, loaderForBR.skipLoadingDiff(systemDiff), "should NOT skip diff for system database")
	})

	// Test case 1.1: CREATE DATABASE diff should always pass through the filter
	t.Run("create_database_action", func(t *testing.T) {
		createDBDiff := &model.SchemaDiff{
			Type:     model.ActionCreateSchema,
			SchemaID: 4,
		}
		require.False(t, loaderForBR.skipLoadingDiff(createDBDiff), "should NOT skip CREATE DATABASE diffs even when the schema name is unknown")
	})

	// Test case 2: Schema diff for BR-related database - should NOT skip
	t.Run("br_related_db", func(t *testing.T) {
		brDiff := &model.SchemaDiff{
			SchemaID: 2, // BR-related database
			TableID:  20,
		}
		require.False(t, loaderForBR.skipLoadingDiff(brDiff), "should NOT skip diff for BR-related database")
	})

	// Test case 3: Schema diff for user database - should skip
	t.Run("user_db", func(t *testing.T) {
		userDiff := &model.SchemaDiff{
			SchemaID: 3, // user database
			TableID:  30,
		}
		require.True(t, loaderForBR.skipLoadingDiff(userDiff), "should skip diff for user database")
	})

	// Test case 4: Schema diff with OldSchemaID for system database - still skipped because selection is based on SchemaID
	t.Run("old_system_to_user", func(t *testing.T) {
		oldSystemDiff := &model.SchemaDiff{
			OldSchemaID: 1, // mysql system database
			SchemaID:    3, // user database
			TableID:     40,
		}
		require.True(t, loaderForBR.skipLoadingDiff(oldSystemDiff), "should skip diff when SchemaID is filtered out even if OldSchemaID is system database")
	})

	// Test case 5: Schema diff with OldSchemaID for BR-related database - still skipped because SchemaID is filtered out
	t.Run("old_br_to_user", func(t *testing.T) {
		oldBRDiff := &model.SchemaDiff{
			OldSchemaID: 2, // BR-related database
			SchemaID:    3, // user database
			TableID:     50,
		}
		require.True(t, loaderForBR.skipLoadingDiff(oldBRDiff), "should skip diff when SchemaID is filtered out even if OldSchemaID is BR-related database")
	})

	// Test case 6: Schema diff with both SchemaID and OldSchemaID as user databases - should skip
	t.Run("user_to_user", func(t *testing.T) {
		userToUserDiff := &model.SchemaDiff{
			OldSchemaID: 3, // user database
			SchemaID:    3, // user database
			TableID:     60,
		}
		require.True(t, loaderForBR.skipLoadingDiff(userToUserDiff), "should skip diff when both SchemaID and OldSchemaID are user databases")
	})

	// Test case 7: Schema diff with non-existent SchemaID - should skip
	t.Run("non_existent_db", func(t *testing.T) {
		nonExistentDiff := &model.SchemaDiff{
			SchemaID: 999, // non-existent database
			TableID:  70,
		}
		require.True(t, loaderForBR.skipLoadingDiff(nonExistentDiff), "should skip diff for non-existent database")
	})
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
	t.Run("br_filter_only", func(t *testing.T) {
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
	})

	// Test with no filter (load all databases)
	t.Run("no_filter_all_dbs", func(t *testing.T) {
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
	})
}

func TestCrossKSTableSubscriptions(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	ctx := tidbkv.WithInternalSourceType(context.Background(), tidbkv.InternalTxnAdmin)
	update := func(diff *model.SchemaDiff, fn func(*meta.Mutator) error) uint64 {
		require.NoError(t, tidbkv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn tidbkv.Transaction) error {
			m := meta.NewMutator(txn)
			if err := fn(m); err != nil {
				return err
			}
			v, err := m.GenSchemaVersion()
			if err != nil {
				return err
			}
			diff.Version = v
			return m.SetSchemaDiff(diff)
		}))
		ver, err := store.CurrentVersion(tidbkv.GlobalTxnScope)
		require.NoError(t, err)
		return ver.Ver
	}
	oldTS := update(&model.SchemaDiff{Type: model.ActionCreateSchema, SchemaID: 1}, func(m *meta.Mutator) error {
		require.NoError(t, m.CreateDatabase(&model.DBInfo{ID: metadef.SystemDatabaseID, Name: ast.NewCIStr("mysql")}))
		require.NoError(t, m.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("test")}))
		require.NoError(t, m.CreateDatabase(&model.DBInfo{ID: 2, Name: ast.NewCIStr("other")}))
		for i, name := range []string{"a", "b", "unrelated"} {
			require.NoError(t, m.CreateTableOrView(1, &model.TableInfo{ID: int64(i + 1), Name: ast.NewCIStr(name), State: model.StatePublic}))
		}
		return nil
	})
	cache := infoschema.NewCache(store, 8)
	s := NewCrossKSSyncer(store, cache, 1e9, nil, isvalidator.New(1e9), "test-ks")
	s.schemaVerSyncer = schemaver.NewMemSyncer()
	latest := func() infoschema.InfoSchema {
		require.NoError(t, s.Reload())
		return cache.GetLatest()
	}
	incremental := func(ts uint64) infoschema.InfoSchema {
		is, hit, _, changes, err := s.LoadWithTS(ts, false)
		require.NoError(t, err)
		require.False(t, hit)
		require.NotNil(t, changes, "must apply diffs rather than fall back to full load")
		return is
	}
	names := []ast.Ident{{Schema: ast.NewCIStr("test"), Name: ast.NewCIStr("a")}}
	lookup := func(is infoschema.InfoSchema, db, name string) *model.TableInfo {
		tbl, err := is.TableByName(ctx, ast.NewCIStr(db), ast.NewCIStr(name))
		require.NoError(t, err)
		return tbl.Meta()
	}
	// SELECT does not register live tables or initialize the shared cache.
	snapshot, err := s.LoadSnapshotInfoSchema(ctx, names, oldTS)
	require.NoError(t, err)
	require.EqualValues(t, 1, lookup(snapshot, "test", "a").ID)
	require.Nil(t, cache.GetLatest())
	require.Empty(t, s.loader.tableSubscriptions)
	a, err := s.RegisterTables(ctx, 1, 1)
	require.NoError(t, err)
	require.Nil(t, cache.GetLatest())
	frozen := latest()
	b, err := s.RegisterTables(ctx, 1, 2)
	require.NoError(t, err)
	defer b()
	require.Equal(t, frozen.SchemaMetaVersion(), latest().SchemaMetaVersion())
	require.EqualValues(t, 2, lookup(latest(), "test", "b").ID)
	_, err = frozen.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("b"))
	require.Error(t, err)
	_, err = latest().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("unrelated"))
	require.Error(t, err)
	require.False(t, s.skipMDLCheck(map[int64]struct{}{1: {}}))
	require.False(t, s.loader.skipLoadingDiff(&model.SchemaDiff{TableID: 3, AffectedOpts: []*model.AffectedOption{{TableID: 2}}}))
	require.True(t, s.loader.skipLoadingDiff(&model.SchemaDiff{TableID: 3, AffectedOpts: []*model.AffectedOption{{TableID: 4}}}))

	// Unrelated DDL is skipped; a registered table's changes are applied incrementally.
	for _, id := range []int64{3, 1} {
		ts := update(&model.SchemaDiff{Type: model.ActionModifyTableComment, SchemaID: 1, TableID: id}, func(m *meta.Mutator) error {
			tbl, err := m.GetTable(1, id)
			require.NoError(t, err)
			tbl.Comment = "changed"
			return m.UpdateTable(1, tbl)
		})
		is := incremental(ts)
		if id == 1 {
			require.Equal(t, "changed", lookup(is, "test", "a").Comment)
		}
	}
	ts := update(&model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: 1, OldSchemaID: 1, TableID: 1}, func(m *meta.Mutator) error {
		return m.UpdateTable(1, &model.TableInfo{ID: 1, Name: ast.NewCIStr("renamed"), State: model.StatePublic})
	})
	require.EqualValues(t, 1, lookup(incremental(ts), "test", "renamed").ID)
	ts = update(&model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: 1, TableID: 4}, func(m *meta.Mutator) error {
		return m.CreateTableOrView(1, &model.TableInfo{ID: 4, Name: ast.NewCIStr("a"), State: model.StatePublic})
	})
	live := incremental(ts)
	_, err = live.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("a"))
	require.Error(t, err, "registration follows ID 1, not the new table named a")
	newSnapshot, err := s.LoadSnapshotInfoSchema(ctx, names, ts)
	require.NoError(t, err)
	require.EqualValues(t, 4, lookup(newSnapshot, "test", "a").ID)
	require.Same(t, live, cache.GetLatest())
	historical, err := s.LoadSnapshotInfoSchema(ctx, names, oldTS)
	require.NoError(t, err)
	require.EqualValues(t, 1, lookup(historical, "test", "a").ID)
	require.EqualValues(t, 1, lookup(frozen, "test", "a").ID)

	otherA, err := s.RegisterTables(ctx, 1, 1)
	require.NoError(t, err)
	a()
	require.EqualValues(t, 1, lookup(latest(), "test", "renamed").ID)
	ts = update(&model.SchemaDiff{Type: model.ActionTruncateTable, SchemaID: 1, OldTableID: 1, TableID: 5}, func(m *meta.Mutator) error {
		require.NoError(t, m.DropTableOrView(1, 1))
		return m.CreateTableOrView(1, &model.TableInfo{ID: 5, Name: ast.NewCIStr("renamed"), State: model.StatePublic})
	})
	_, err = incremental(ts).TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("renamed"))
	require.Error(t, err, "TRUNCATE must not subscribe to the replacement ID")
	otherA()
	latest()
	// A cross-database rename may need a full reload, but must retain the same ID.
	update(&model.SchemaDiff{Type: model.ActionRenameTable, SchemaID: 2, OldSchemaID: 1, TableID: 2}, func(m *meta.Mutator) error {
		require.NoError(t, m.DropTableOrView(1, 2))
		return m.CreateTableOrView(2, &model.TableInfo{ID: 2, Name: ast.NewCIStr("b"), State: model.StatePublic})
	})
	require.EqualValues(t, 2, lookup(latest(), "other", "b").ID)
	require.EqualValues(t, 2, s.loader.tableSubscriptions[2].schemaID)
	future, err := s.RegisterTables(ctx, 1, 6)
	require.NoError(t, err)
	defer future()
	latest()
	ts = update(&model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: 1, TableID: 6}, func(m *meta.Mutator) error {
		return m.CreateTableOrView(1, &model.TableInfo{ID: 6, Name: ast.NewCIStr("future"), State: model.StatePublic})
	})
	require.EqualValues(t, 6, lookup(incremental(ts), "test", "future").ID)
	ts = update(&model.SchemaDiff{Type: model.ActionDropTable, SchemaID: 1, TableID: 6}, func(m *meta.Mutator) error {
		return m.DropTableOrView(1, 6)
	})
	_, err = incremental(ts).TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("future"))
	require.Error(t, err)
	ts = update(&model.SchemaDiff{Type: model.ActionCreateTable, SchemaID: 1, TableID: 7}, func(m *meta.Mutator) error {
		return m.CreateTableOrView(1, &model.TableInfo{ID: 7, Name: ast.NewCIStr("future"), State: model.StatePublic})
	})
	_, err = incremental(ts).TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("future"))
	require.Error(t, err, "DROP and recreate must not follow the replacement ID")
}
