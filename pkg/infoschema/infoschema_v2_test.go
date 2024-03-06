// Copyright 2024 PingCAP, Inc.
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

package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestV2Basic(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()
	is := NewInfoSchemaV2(r, NewData())

	schemaName := model.NewCIStr("testDB")
	tableName := model.NewCIStr("test")

	dbInfo := internal.MockDBInfo(t, r.Store(), schemaName.O)
	is.Data.addDB(1, dbInfo)
	internal.AddDB(t, r.Store(), dbInfo)
	tblInfo := internal.MockTableInfo(t, r.Store(), tableName.O)
	is.Data.add(tableItem{schemaName.L, dbInfo.ID, tableName.L, tblInfo.ID, 2}, internal.MockTable(t, r.Store(), tblInfo))
	internal.AddTable(t, r.Store(), dbInfo, tblInfo)
	require.Equal(t, 1, len(is.AllSchemas()))
	require.Equal(t, 0, len(is.SchemaTables(is.AllSchemas()[0].Name)))
	ver, err := r.Store().CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is.schemaVersion = 2
	is.ts = ver.Ver
	require.Equal(t, 1, len(is.AllSchemas()))
	require.Equal(t, 1, len(is.SchemaTables(is.AllSchemas()[0].Name)))

	getDBInfo, ok := is.SchemaByName(schemaName)
	require.True(t, ok)
	require.Equal(t, dbInfo, getDBInfo)
	require.True(t, is.SchemaExists(schemaName))

	getTableInfo, err := is.TableByName(schemaName, tableName)
	require.NoError(t, err)
	require.NotNil(t, getTableInfo)
	require.True(t, is.TableExists(schemaName, tableName))

	getDBInfo, ok = is.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	require.Equal(t, dbInfo, getDBInfo)

	getTableInfo, ok = is.TableByID(tblInfo.ID)
	require.True(t, ok)
	require.NotNil(t, getTableInfo)

	require.Equal(t, int64(2), is.SchemaMetaVersion())
	// TODO: support FindTableByPartitionID.
}

func TestMisc(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()

	builder, err := NewBuilder(r, nil, NewData()).InitWithDBInfos(nil, nil, nil, 1)
	require.NoError(t, err)
	is := builder.Build()
	require.Len(t, is.AllResourceGroups(), 0)

	// test create resource group
	resourceGroupInfo := internal.MockResourceGroupInfo(t, r.Store(), "test")
	internal.AddResourceGroup(t, r.Store(), resourceGroupInfo)
	txn, err := r.Store().Begin()
	require.NoError(t, err)
	err = applyCreateOrAlterResourceGroup(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: resourceGroupInfo.ID})
	require.NoError(t, err)
	is = builder.Build()
	require.Len(t, is.AllResourceGroups(), 1)
	getResourceGroupInfo, ok := is.ResourceGroupByName(resourceGroupInfo.Name)
	require.True(t, ok)
	require.Equal(t, resourceGroupInfo, getResourceGroupInfo)
	require.NoError(t, txn.Rollback())

	// create another resource group
	resourceGroupInfo2 := internal.MockResourceGroupInfo(t, r.Store(), "test2")
	internal.AddResourceGroup(t, r.Store(), resourceGroupInfo2)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	err = applyCreateOrAlterResourceGroup(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: resourceGroupInfo2.ID})
	is = builder.Build()
	require.Len(t, is.AllResourceGroups(), 2)
	getResourceGroupInfo, ok = is.ResourceGroupByName(resourceGroupInfo2.Name)
	require.True(t, ok)
	require.Equal(t, resourceGroupInfo2, getResourceGroupInfo)
	require.NoError(t, txn.Rollback())

	// test alter resource group
	resourceGroupInfo.State = model.StatePublic
	internal.UpdateResourceGroup(t, r.Store(), resourceGroupInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	err = applyCreateOrAlterResourceGroup(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: resourceGroupInfo.ID})
	require.NoError(t, err)
	is = builder.Build()
	require.Len(t, is.AllResourceGroups(), 2)
	getResourceGroupInfo, ok = is.ResourceGroupByName(resourceGroupInfo.Name)
	require.True(t, ok)
	require.Equal(t, resourceGroupInfo, getResourceGroupInfo)
	require.NoError(t, txn.Rollback())

	// test drop resource group
	internal.DropResourceGroup(t, r.Store(), resourceGroupInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_ = applyDropResourceGroup(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: resourceGroupInfo.ID})
	is = builder.Build()
	require.Len(t, is.AllResourceGroups(), 1)
	getResourceGroupInfo, ok = is.ResourceGroupByName(resourceGroupInfo2.Name)
	require.True(t, ok)
	require.Equal(t, resourceGroupInfo2, getResourceGroupInfo)
	require.NoError(t, txn.Rollback())
}
