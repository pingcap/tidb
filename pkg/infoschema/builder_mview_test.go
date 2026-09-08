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
