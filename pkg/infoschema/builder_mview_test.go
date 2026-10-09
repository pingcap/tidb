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
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestApplyMViewRefreshOutOfPlaceCutoverDiff(t *testing.T) {
	for _, useV2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "infoschema-v1", true: "infoschema-v2"}[useV2], func(t *testing.T) {
			re := internal.CreateAutoIDRequirement(t)
			t.Cleanup(func() {
				require.NoError(t, re.Store().Close())
			})

			dbInfo := internal.MockDBInfo(t, re.Store(), "test")
			baseTable := internal.MockTableInfo(t, re.Store(), "base")
			baseTable.DBID = dbInfo.ID
			oldMView := internal.MockTableInfo(t, re.Store(), "mv")
			oldMView.DBID = dbInfo.ID
			oldMView.MaterializedView = &model.MaterializedViewInfo{BaseTableIDs: []int64{baseTable.ID}}
			baseTable.MaterializedViewBase = &model.MaterializedViewBaseInfo{MViewIDs: []int64{oldMView.ID}}
			shadowTable := internal.MockTableInfo(t, re.Store(), "__mv_shadow")
			shadowTable.DBID = dbInfo.ID
			shadowTable.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: oldMView.ID}
			dbInfo.Deprecated.Tables = []*model.TableInfo{baseTable, oldMView, shadowTable}
			internal.AddDB(t, re.Store(), dbInfo)
			internal.AddTable(t, re.Store(), dbInfo.ID, baseTable)
			internal.AddTable(t, re.Store(), dbInfo.ID, oldMView)
			internal.AddTable(t, re.Store(), dbInfo.ID, shadowTable)

			data := infoschema.NewData()
			builder := infoschema.NewBuilder(re, nil, data, useV2)
			require.NoError(t, builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, nil, nil, 1))
			oldInfoSchema := builder.Build(math.MaxUint64)

			newMView := shadowTable.Clone()
			newMView.Name = oldMView.Name
			newMView.MaterializedView = oldMView.MaterializedView
			newMView.MaterializedViewShadow = nil
			baseAfterCutover := baseTable.Clone()
			baseAfterCutover.MaterializedViewBase.MViewIDs = []int64{shadowTable.ID}
			internal.UpdateTable(t, re.Store(), dbInfo, baseAfterCutover)
			internal.UpdateTable(t, re.Store(), dbInfo, newMView)
			internal.DropTable(t, re.Store(), dbInfo, oldMView.ID, oldMView.Name.O)

			txn, err := re.Store().Begin()
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, txn.Rollback())
			})
			builder = infoschema.NewBuilder(re, nil, data, useV2)
			require.NoError(t, builder.InitWithOldInfoSchema(oldInfoSchema))
			_, err = builder.ApplyDiff(meta.NewMutator(txn), &model.SchemaDiff{
				Type:       model.ActionMViewRefreshOutOfPlaceCutover,
				SchemaID:   dbInfo.ID,
				TableID:    shadowTable.ID,
				OldTableID: oldMView.ID,
				Version:    2,
				AffectedOpts: []*model.AffectedOption{{
					SchemaID: dbInfo.ID, OldSchemaID: dbInfo.ID,
					TableID: baseTable.ID, OldTableID: baseTable.ID,
				}},
			})
			require.NoError(t, err)

			is := builder.Build(math.MaxUint64)
			_, exists := is.TableByID(context.Background(), oldMView.ID)
			require.False(t, exists)
			newTable, exists := is.TableByID(context.Background(), shadowTable.ID)
			require.True(t, exists)
			require.Equal(t, oldMView.Name, newTable.Meta().Name)
			require.NotNil(t, newTable.Meta().MaterializedView)
			require.Nil(t, newTable.Meta().MaterializedViewShadow)
			shadowByName, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("__mv_shadow"))
			require.Error(t, err)
			require.Nil(t, shadowByName)
			newTable, err = is.TableByName(context.Background(), dbInfo.Name, oldMView.Name)
			require.NoError(t, err)
			require.Equal(t, shadowTable.ID, newTable.Meta().ID)
			base, exists := is.TableByID(context.Background(), baseTable.ID)
			require.True(t, exists)
			require.Equal(t, []int64{shadowTable.ID}, base.Meta().MaterializedViewBase.MViewIDs)
		})
	}
}
