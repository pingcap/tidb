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

package infoschema_test

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestApplyDropMaterializedViewDiff(t *testing.T) {
	for _, useV2 := range []bool{false, true} {
		name := "infoschema-v1"
		if useV2 {
			name = "infoschema-v2"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("materialized view", func(t *testing.T) {
				testApplyDropMaterializedViewDiff(t, useV2)
			})
			t.Run("materialized view log", func(t *testing.T) {
				testApplyDropMaterializedViewLogDiff(t, useV2)
			})
		})
	}
}

func TestApplyMViewRefreshOutOfPlaceCutoverDiff(t *testing.T) {
	for _, useV2 := range []bool{false, true} {
		name := "infoschema-v1"
		if useV2 {
			name = "infoschema-v2"
		}
		t.Run(name, func(t *testing.T) {
			re := internal.CreateAutoIDRequirement(t)
			t.Cleanup(func() {
				require.NoError(t, re.Store().Close())
			})

			dbInfo, policy, baseTable, mlogTable, oldMView := setupMaterializedViewInfoSchema(t, re, true)
			shadowTable := oldMView.Clone()
			shadowTable.ID = oldMView.ID + 1000
			shadowTable.Name = ast.NewCIStr("__mv_shadow")
			shadowTable.MaterializedView = nil
			shadowTable.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: oldMView.ID}
			internal.AddTable(t, re.Store(), dbInfo.ID, shadowTable)

			oldMLog := internal.MockTableInfo(t, re.Store(), "$mlog$mv")
			oldMLog.DBID = dbInfo.ID
			child := internal.MockTableInfo(t, re.Store(), "child")
			child.DBID = dbInfo.ID
			child.MaterializedView = &model.MaterializedViewInfo{BaseTableIDs: []int64{oldMView.ID, baseTable.ID}}
			oldMView.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: oldMLog.ID, MViewIDs: []int64{child.ID}}
			oldMLog.MaterializedViewLog = &model.MaterializedViewLogInfo{BaseTableID: oldMView.ID, DependentMViewIDs: []int64{child.ID}}
			internal.AddTable(t, re.Store(), dbInfo.ID, oldMLog)
			internal.AddTable(t, re.Store(), dbInfo.ID, child)
			internal.UpdateTable(t, re.Store(), dbInfo, oldMView)
			oldVersion, err := re.Store().CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			newMLog := internal.MockTableInfo(t, re.Store(), "$mlog$mv")
			newMLog.DBID = dbInfo.ID
			newMLog.MaterializedViewLog = oldMLog.MaterializedViewLog.Clone()
			newMLog.MaterializedViewLog.BaseTableID = shadowTable.ID
			internal.DropTable(t, re.Store(), dbInfo, oldMLog.ID, oldMLog.Name.O)
			internal.AddTable(t, re.Store(), dbInfo.ID, newMLog)
			childAfterCutover := child.Clone()
			childAfterCutover.MaterializedView.BaseTableIDs = []int64{shadowTable.ID, baseTable.ID}
			internal.UpdateTable(t, re.Store(), dbInfo, childAfterCutover)

			baseAfterCutover := baseTable.Clone()
			baseAfterCutover.MaterializedViewBase.MViewIDs = []int64{shadowTable.ID}
			internal.UpdateTable(t, re.Store(), dbInfo, baseAfterCutover)
			mlogAfterCutover := mlogTable.Clone()
			mlogAfterCutover.MaterializedViewLog.DependentMViewIDs = []int64{shadowTable.ID}
			internal.UpdateTable(t, re.Store(), dbInfo, mlogAfterCutover)

			newMView := shadowTable.Clone()
			newMView.Name = oldMView.Name
			newMView.MaterializedViewShadow = nil
			newMView.MaterializedView = oldMView.MaterializedView.Clone()
			newMView.MaterializedViewBase = oldMView.MaterializedViewBase.Clone()
			newMView.MaterializedViewBase.MLogID = newMLog.ID
			internal.UpdateTable(t, re.Store(), dbInfo, newMView)
			internal.DropTable(t, re.Store(), dbInfo, oldMView.ID, oldMView.Name.O)

			is, oldInfoSchema := applyMaterializedViewCutoverDiff(t, re, useV2, dbInfo.ID, oldVersion.Ver, policy,
				[]*model.TableInfo{baseTable, mlogTable, oldMView, shadowTable, oldMLog, child}, &model.SchemaDiff{
					Type:       model.ActionMViewRefreshOutOfPlaceCutover,
					SchemaID:   dbInfo.ID,
					TableID:    shadowTable.ID,
					OldTableID: oldMView.ID,
					Version:    2,
					AffectedOpts: []*model.AffectedOption{{
						SchemaID: dbInfo.ID, OldSchemaID: dbInfo.ID,
						TableID: baseTable.ID, OldTableID: baseTable.ID,
					}, {
						SchemaID: dbInfo.ID, OldSchemaID: dbInfo.ID,
						TableID: mlogTable.ID, OldTableID: mlogTable.ID,
					}, {
						SchemaID: dbInfo.ID, OldSchemaID: dbInfo.ID,
						TableID: newMLog.ID, OldTableID: oldMLog.ID,
					}, {
						SchemaID: dbInfo.ID, OldSchemaID: dbInfo.ID,
						TableID: child.ID, OldTableID: child.ID,
					}},
				})

			_, exists := is.TableByID(context.Background(), oldMView.ID)
			require.False(t, exists)
			newTable, exists := is.TableByID(context.Background(), shadowTable.ID)
			require.True(t, exists)
			require.Equal(t, oldMView.Name, newTable.Meta().Name)
			require.NotNil(t, newTable.Meta().MaterializedView)
			require.Nil(t, newTable.Meta().MaterializedViewShadow)
			shadowByName, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("__mv_shadow"))
			require.Error(t, err)
			require.Nil(t, shadowByName)
			mvByName, err := is.TableByName(context.Background(), ast.NewCIStr("test"), oldMView.Name)
			require.NoError(t, err)
			require.Equal(t, shadowTable.ID, mvByName.Meta().ID)
			base, exists := is.TableByID(context.Background(), baseTable.ID)
			require.True(t, exists)
			require.Equal(t, []int64{shadowTable.ID}, base.Meta().MaterializedViewBase.MViewIDs)
			mlog, exists := is.TableByID(context.Background(), mlogTable.ID)
			require.True(t, exists)
			require.Equal(t, []int64{shadowTable.ID}, mlog.Meta().MaterializedViewLog.DependentMViewIDs)
			_, exists = is.TableByID(context.Background(), oldMLog.ID)
			require.False(t, exists)
			log, err := is.TableByName(context.Background(), dbInfo.Name, oldMLog.Name)
			require.NoError(t, err)
			require.Equal(t, newMLog.ID, log.Meta().ID)
			require.Equal(t, newMView.ID, log.Meta().MaterializedViewLog.BaseTableID)
			require.Equal(t, []int64{child.ID}, log.Meta().MaterializedViewLog.DependentMViewIDs)
			newChild, exists := is.TableByID(context.Background(), child.ID)
			require.True(t, exists)
			require.Equal(t, []int64{newMView.ID, baseTable.ID}, newChild.Meta().MaterializedView.BaseTableIDs)
			// Applying a diff must leave an existing snapshot's graph intact.
			oldChild, exists := oldInfoSchema.TableByID(context.Background(), child.ID)
			require.True(t, exists)
			require.Equal(t, []int64{oldMView.ID, baseTable.ID}, oldChild.Meta().MaterializedView.BaseTableIDs)
		})
	}
}

func applyMaterializedViewCutoverDiff(
	t *testing.T,
	re autoid.Requirement,
	useV2 bool,
	dbID int64,
	oldSnapshotTS uint64,
	policy *model.PolicyInfo,
	tables []*model.TableInfo,
	diff *model.SchemaDiff,
) (infoschema.InfoSchema, infoschema.InfoSchema) {
	schemaCacheSize := uint64(0)
	if useV2 {
		schemaCacheSize = 1
	}
	data := infoschema.NewData()
	dbInfo := &model.DBInfo{ID: dbID, Name: ast.NewCIStr("test"), State: model.StatePublic}
	dbInfo.Deprecated.Tables = tables
	builder := infoschema.NewBuilder(re, schemaCacheSize, nil, data, useV2)
	require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, []*model.PolicyInfo{policy}, nil, nil, 1))
	// V2 loads tables lazily, so pin the old schema to the pre-cutover snapshot.
	oldInfoSchema := builder.Build(oldSnapshotTS)

	txn, err := re.Store().Begin()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, txn.Rollback())
	})
	builder = infoschema.NewBuilder(re, schemaCacheSize, nil, data, useV2)
	require.NoError(t, builder.InitWithOldInfoSchema(oldInfoSchema))
	_, err = builder.ApplyDiff(meta.NewMutator(txn), diff)
	require.NoError(t, err)
	return builder.Build(math.MaxUint64), oldInfoSchema
}

func testApplyDropMaterializedViewDiff(t *testing.T, useV2 bool) {
	re := internal.CreateAutoIDRequirement(t)
	t.Cleanup(func() {
		require.NoError(t, re.Store().Close())
	})

	dbInfo, policy, baseTable, mlogTable, mviewTable := setupMaterializedViewInfoSchema(t, re, true)
	baseAfterDrop := baseTable.Clone()
	baseAfterDrop.MaterializedViewBase.MViewIDs = nil
	mlogAfterDrop := mlogTable.Clone()
	mlogAfterDrop.MaterializedViewLog.DependentMViewIDs = nil
	internal.UpdateTable(t, re.Store(), dbInfo, baseAfterDrop)
	internal.UpdateTable(t, re.Store(), dbInfo, mlogAfterDrop)
	internal.DropTable(t, re.Store(), dbInfo, mviewTable.ID, mviewTable.Name.O)

	is := applyMaterializedViewDropDiff(t, re, useV2, dbInfo.ID, policy, []*model.TableInfo{baseTable, mlogTable, mviewTable}, &model.SchemaDiff{
		Type:     model.ActionDropMaterializedView,
		SchemaID: dbInfo.ID,
		TableID:  mviewTable.ID,
		Version:  2,
		AffectedOpts: []*model.AffectedOption{
			materializedViewAffectedOption(dbInfo.ID, baseTable.ID),
			materializedViewAffectedOption(dbInfo.ID, mlogTable.ID),
		},
	})

	_, exists := is.TableByID(context.Background(), mviewTable.ID)
	require.False(t, exists)
	base, exists := is.TableByID(context.Background(), baseTable.ID)
	require.True(t, exists)
	require.Equal(t, mlogTable.ID, base.Meta().MaterializedViewBase.MLogID)
	require.Empty(t, base.Meta().MaterializedViewBase.MViewIDs)
	mlog, exists := is.TableByID(context.Background(), mlogTable.ID)
	require.True(t, exists)
	require.Empty(t, mlog.Meta().MaterializedViewLog.DependentMViewIDs)
	_, exists = is.PlacementBundleByPhysicalTableID(baseTable.ID)
	require.True(t, exists)
}

func testApplyDropMaterializedViewLogDiff(t *testing.T, useV2 bool) {
	re := internal.CreateAutoIDRequirement(t)
	t.Cleanup(func() {
		require.NoError(t, re.Store().Close())
	})

	dbInfo, policy, baseTable, mlogTable, _ := setupMaterializedViewInfoSchema(t, re, false)
	baseAfterDrop := baseTable.Clone()
	baseAfterDrop.MaterializedViewBase = nil
	internal.UpdateTable(t, re.Store(), dbInfo, baseAfterDrop)
	internal.DropTable(t, re.Store(), dbInfo, mlogTable.ID, mlogTable.Name.O)

	is := applyMaterializedViewDropDiff(t, re, useV2, dbInfo.ID, policy, []*model.TableInfo{baseTable, mlogTable}, &model.SchemaDiff{
		Type:     model.ActionDropMaterializedViewLog,
		SchemaID: dbInfo.ID,
		TableID:  mlogTable.ID,
		Version:  2,
		AffectedOpts: []*model.AffectedOption{
			materializedViewAffectedOption(dbInfo.ID, baseTable.ID),
		},
	})

	_, exists := is.TableByID(context.Background(), mlogTable.ID)
	require.False(t, exists)
	base, exists := is.TableByID(context.Background(), baseTable.ID)
	require.True(t, exists)
	require.Nil(t, base.Meta().MaterializedViewBase)
	_, exists = is.PlacementBundleByPhysicalTableID(baseTable.ID)
	require.True(t, exists)
}

func setupMaterializedViewInfoSchema(
	t *testing.T,
	re autoid.Requirement,
	withMaterializedView bool,
) (*model.DBInfo, *model.PolicyInfo, *model.TableInfo, *model.TableInfo, *model.TableInfo) {
	dbInfo := internal.MockDBInfo(t, re.Store(), "test")
	policy := internal.MockPolicyInfo(t, re.Store(), "p")
	policy.State = model.StatePublic
	policy.PlacementSettings = &model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1,r2"}
	baseTable := internal.MockTableInfo(t, re.Store(), "base")
	mlogTable := internal.MockTableInfo(t, re.Store(), "$mlog$base")
	baseTable.DBID = dbInfo.ID
	mlogTable.DBID = dbInfo.ID
	baseTable.PlacementPolicyRef = &model.PolicyRefInfo{ID: policy.ID, Name: policy.Name}
	baseTable.MaterializedViewBase = &model.MaterializedViewBaseInfo{MLogID: mlogTable.ID}
	mlogTable.MaterializedViewLog = &model.MaterializedViewLogInfo{BaseTableID: baseTable.ID}

	tables := []*model.TableInfo{baseTable, mlogTable}
	var mviewTable *model.TableInfo
	if withMaterializedView {
		mviewTable = internal.MockTableInfo(t, re.Store(), "mv")
		mviewTable.DBID = dbInfo.ID
		mviewTable.MaterializedView = &model.MaterializedViewInfo{BaseTableIDs: []int64{baseTable.ID}}
		baseTable.MaterializedViewBase.MViewIDs = []int64{mviewTable.ID}
		mlogTable.MaterializedViewLog.DependentMViewIDs = []int64{mviewTable.ID}
		tables = append(tables, mviewTable)
	}
	dbInfo.Deprecated.Tables = tables
	internal.AddDB(t, re.Store(), dbInfo)
	for _, tbl := range tables {
		internal.AddTable(t, re.Store(), dbInfo.ID, tbl)
	}

	return dbInfo, policy, baseTable, mlogTable, mviewTable
}

func applyMaterializedViewDropDiff(
	t *testing.T,
	re autoid.Requirement,
	useV2 bool,
	dbID int64,
	policy *model.PolicyInfo,
	tables []*model.TableInfo,
	diff *model.SchemaDiff,
) infoschema.InfoSchema {
	schemaCacheSize := uint64(0)
	if useV2 {
		schemaCacheSize = 1
	}
	data := infoschema.NewData()
	dbInfo := &model.DBInfo{ID: dbID, Name: ast.NewCIStr("test"), State: model.StatePublic}
	dbInfo.Deprecated.Tables = tables
	builder := infoschema.NewBuilder(re, schemaCacheSize, nil, data, useV2)
	require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, []*model.PolicyInfo{policy}, nil, nil, 1))
	oldInfoSchema := builder.Build(math.MaxUint64)

	txn, err := re.Store().Begin()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, txn.Rollback())
	})
	builder = infoschema.NewBuilder(re, schemaCacheSize, nil, data, useV2)
	require.NoError(t, builder.InitWithOldInfoSchema(oldInfoSchema))
	_, err = builder.ApplyDiff(meta.NewMutator(txn), diff)
	require.NoError(t, err)
	return builder.Build(math.MaxUint64)
}

func materializedViewAffectedOption(schemaID, tableID int64) *model.AffectedOption {
	return &model.AffectedOption{
		SchemaID:    schemaID,
		OldSchemaID: schemaID,
		TableID:     tableID,
		OldTableID:  tableID,
	}
}
