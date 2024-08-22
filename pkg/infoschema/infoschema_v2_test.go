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
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestV2Basic(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()
	is := NewInfoSchemaV2(r, nil, NewData())

	schemaName := model.NewCIStr("testDB")
	tableName := model.NewCIStr("test")

	dbInfo := internal.MockDBInfo(t, r.Store(), schemaName.O)
	is.Data.addDB(1, dbInfo)
	internal.AddDB(t, r.Store(), dbInfo)
	tblInfo := internal.MockTableInfo(t, r.Store(), tableName.O)
	tblInfo.DBID = dbInfo.ID
	is.Data.add(tableItem{schemaName, dbInfo.ID, tableName, tblInfo.ID, 2, false}, internal.MockTable(t, r.Store(), tblInfo))
	internal.AddTable(t, r.Store(), dbInfo, tblInfo)
	is.base().schemaMetaVersion = 1
	require.Equal(t, 1, len(is.AllSchemas()))
	ver, err := r.Store().CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	is.base().schemaMetaVersion = 2
	is.ts = ver.Ver
	require.Equal(t, 1, len(is.AllSchemas()))
	tblInfos, err := is.SchemaTableInfos(context.Background(), is.AllSchemas()[0].Name)
	require.NoError(t, err)
	require.Equal(t, 1, len(tblInfos))

	getDBInfo, ok := is.SchemaByName(schemaName)
	require.True(t, ok)
	require.Equal(t, dbInfo, getDBInfo)
	require.True(t, is.SchemaExists(schemaName))

	getTableInfo, err := is.TableByName(context.Background(), schemaName, tableName)
	require.NoError(t, err)
	require.NotNil(t, getTableInfo)
	require.True(t, is.TableExists(schemaName, tableName))

	gotTblInfo, err := is.TableInfoByName(schemaName, tableName)
	require.NoError(t, err)
	require.Same(t, gotTblInfo, getTableInfo.Meta())

	gotTblInfo, err = is.TableInfoByName(schemaName, model.NewCIStr("notexist"))
	require.Error(t, err)
	require.Nil(t, gotTblInfo)

	getDBInfo, ok = is.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	require.Equal(t, dbInfo, getDBInfo)

	getTableInfo, ok = is.TableByID(context.Background(), tblInfo.ID)
	require.True(t, ok)
	require.NotNil(t, getTableInfo)

	gotTblInfo, ok = is.TableInfoByID(tblInfo.ID)
	require.True(t, ok)
	require.Same(t, gotTblInfo, getTableInfo.Meta())

	// negative id should always be seen as not exists
	getTableInfo, ok = is.TableByID(context.Background(), -1)
	require.False(t, ok)
	require.Nil(t, getTableInfo)
	gotTblInfo, ok = is.TableInfoByID(-1)
	require.False(t, ok)
	require.Nil(t, gotTblInfo)
	getDBInfo, ok = is.SchemaByID(-1)
	require.False(t, ok)
	require.Nil(t, getDBInfo)

	gotTblInfo, ok = is.TableInfoByID(1234567)
	require.False(t, ok)
	require.Nil(t, gotTblInfo)

	tables, err := is.SchemaTableInfos(context.Background(), schemaName)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))
	require.Equal(t, tblInfo.ID, tables[0].ID)

	tblInfos, err1 := is.SchemaTableInfos(context.Background(), schemaName)
	require.NoError(t, err1)
	require.Equal(t, 1, len(tblInfos))
	require.Equal(t, tables[0], tblInfos[0])

	tables, err = is.SchemaTableInfos(context.Background(), model.NewCIStr("notexist"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tables))

	tblInfos, err = is.SchemaTableInfos(context.Background(), model.NewCIStr("notexist"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tblInfos))

	require.Equal(t, int64(2), is.SchemaMetaVersion())
	// TODO: support FindTableByPartitionID.
}

func TestMisc(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()

	builder := NewBuilder(r, nil, NewData(), variable.SchemaCacheSize.Load() > 0)
	err := builder.InitWithDBInfos(nil, nil, nil, 1)
	require.NoError(t, err)
	is := builder.Build(math.MaxUint64)
	require.Len(t, is.AllResourceGroups(), 0)

	// test create resource group
	resourceGroupInfo := internal.MockResourceGroupInfo(t, r.Store(), "test")
	internal.AddResourceGroup(t, r.Store(), resourceGroupInfo)
	txn, err := r.Store().Begin()
	require.NoError(t, err)
	err = applyCreateOrAlterResourceGroup(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: resourceGroupInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
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
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
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
	is = builder.Build(math.MaxUint64)
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
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllResourceGroups(), 1)
	getResourceGroupInfo, ok = is.ResourceGroupByName(resourceGroupInfo2.Name)
	require.True(t, ok)
	require.Equal(t, resourceGroupInfo2, getResourceGroupInfo)
	require.NoError(t, txn.Rollback())

	// test create policy
	policyInfo := internal.MockPolicyInfo(t, r.Store(), "test")
	internal.CreatePolicy(t, r.Store(), policyInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	err = applyCreatePolicy(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: policyInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllPlacementPolicies(), 1)
	getPolicyInfo, ok := is.PolicyByName(policyInfo.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo, getPolicyInfo)
	require.NoError(t, txn.Rollback())

	// create another policy
	policyInfo2 := internal.MockPolicyInfo(t, r.Store(), "test2")
	internal.CreatePolicy(t, r.Store(), policyInfo2)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	err = applyCreatePolicy(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: policyInfo2.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllPlacementPolicies(), 2)
	getPolicyInfo, ok = is.PolicyByName(policyInfo2.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo2, getPolicyInfo)
	require.NoError(t, txn.Rollback())

	// test alter policy
	policyInfo.State = model.StatePublic
	internal.UpdatePolicy(t, r.Store(), policyInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = applyAlterPolicy(builder, meta.NewMeta(txn), &model.SchemaDiff{SchemaID: policyInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllPlacementPolicies(), 2)
	getPolicyInfo, ok = is.PolicyByName(policyInfo.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo, getPolicyInfo)
	require.NoError(t, txn.Rollback())

	// test drop policy
	internal.DropPolicy(t, r.Store(), policyInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_ = applyDropPolicy(builder, policyInfo.ID)
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllPlacementPolicies(), 1)
	getPolicyInfo, ok = is.PolicyByName(policyInfo2.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo2, getPolicyInfo)
	require.NoError(t, txn.Rollback())
}

func TestBundles(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()

	schemaName := model.NewCIStr("testDB")
	tableName := model.NewCIStr("test")
	builder := NewBuilder(r, nil, NewData(), variable.SchemaCacheSize.Load() > 0)
	err := builder.InitWithDBInfos(nil, nil, nil, 1)
	require.NoError(t, err)
	is := builder.Build(math.MaxUint64)
	require.Equal(t, 2, len(is.AllSchemas()))

	// create database
	dbInfo := internal.MockDBInfo(t, r.Store(), schemaName.O)
	internal.AddDB(t, r.Store(), dbInfo)
	txn, err := r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionCreateSchema, Version: 1, SchemaID: dbInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Equal(t, 3, len(is.AllSchemas()))
	require.NoError(t, txn.Rollback())

	// create table
	tblInfo := internal.MockTableInfo(t, r.Store(), tableName.O)
	tblInfo.Partition = &model.PartitionInfo{Definitions: []model.PartitionDefinition{{ID: 1}, {ID: 2}}}
	internal.AddTable(t, r.Store(), dbInfo, tblInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionCreateTable, Version: 2, SchemaID: dbInfo.ID, TableID: tblInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	tblInfos, err := is.SchemaTableInfos(context.Background(), dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, 1, len(tblInfos))
	require.NoError(t, txn.Rollback())

	// test create policy
	policyInfo := internal.MockPolicyInfo(t, r.Store(), "test")
	internal.CreatePolicy(t, r.Store(), policyInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionCreatePlacementPolicy, Version: 3, SchemaID: policyInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Len(t, is.AllPlacementPolicies(), 1)
	getPolicyInfo, ok := is.PolicyByName(policyInfo.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo, getPolicyInfo)
	require.NoError(t, txn.Rollback())

	// markTableBundleShouldUpdate
	// test alter table placement
	policyRefInfo := internal.MockPolicyRefInfo(t, r.Store(), "test")
	tblInfo.PlacementPolicyRef = policyRefInfo
	internal.UpdateTable(t, r.Store(), dbInfo, tblInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionAlterTablePlacement, Version: 4, SchemaID: dbInfo.ID, TableID: tblInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	getTableInfo, err := is.TableByName(context.Background(), schemaName, tableName)
	require.NoError(t, err)
	require.Equal(t, policyRefInfo, getTableInfo.Meta().PlacementPolicyRef)
	require.NoError(t, txn.Rollback())

	// markBundlesReferPolicyShouldUpdate
	// test alter policy
	policyInfo.State = model.StatePublic
	internal.UpdatePolicy(t, r.Store(), policyInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionAlterPlacementPolicy, Version: 5, SchemaID: policyInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	getTableInfo, err = is.TableByName(context.Background(), schemaName, tableName)
	require.NoError(t, err)
	getPolicyInfo, ok = is.PolicyByName(getTableInfo.Meta().PlacementPolicyRef.Name)
	require.True(t, ok)
	require.Equal(t, policyInfo, getPolicyInfo)
}

func updateTableSpecialAttribute(t *testing.T, dbInfo *model.DBInfo, tblInfo *model.TableInfo, builder *Builder, r autoid.Requirement,
	actionType model.ActionType, ver int64, filter specialAttributeFilter, add bool) *model.TableInfo {
	internal.UpdateTable(t, r.Store(), dbInfo, tblInfo)
	txn, err := r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: actionType, Version: ver, SchemaID: dbInfo.ID, TableID: tblInfo.ID})
	require.NoError(t, err)
	is := builder.Build(math.MaxUint64)
	tblInfoRes := is.ListTablesWithSpecialAttribute(filter)
	if add {
		// add special attribute
		require.Equal(t, 1, len(tblInfoRes))
		require.Equal(t, 1, len(tblInfoRes[0].TableInfos))
		return tblInfoRes[0].TableInfos[0]
	}
	require.Equal(t, 0, len(tblInfoRes))
	return nil
}

func TestSpecialAttributeCorrectnessInSchemaChange(t *testing.T) {
	r := internal.CreateAutoIDRequirement(t)
	defer func() {
		r.Store().Close()
	}()

	schemaName := model.NewCIStr("testDB")
	tableName := model.NewCIStr("testTable")
	builder := NewBuilder(r, nil, NewData(), variable.SchemaCacheSize.Load() > 0)
	err := builder.InitWithDBInfos(nil, nil, nil, 1)
	require.NoError(t, err)
	is := builder.Build(math.MaxUint64)
	require.Equal(t, 2, len(is.AllSchemas()))

	// create database
	dbInfo := internal.MockDBInfo(t, r.Store(), schemaName.O)
	internal.AddDB(t, r.Store(), dbInfo)
	txn, err := r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionCreateSchema, Version: 1, SchemaID: dbInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	require.Equal(t, 3, len(is.AllSchemas()))
	require.NoError(t, txn.Rollback())

	// create table
	tblInfo := internal.MockTableInfo(t, r.Store(), tableName.O)
	internal.AddTable(t, r.Store(), dbInfo, tblInfo)
	txn, err = r.Store().Begin()
	require.NoError(t, err)
	_, err = builder.ApplyDiff(meta.NewMeta(txn), &model.SchemaDiff{Type: model.ActionCreateTable, Version: 2, SchemaID: dbInfo.ID, TableID: tblInfo.ID})
	require.NoError(t, err)
	is = builder.Build(math.MaxUint64)
	tblInfos, err := is.SchemaTableInfos(context.Background(), dbInfo.Name)
	require.NoError(t, err)
	require.Equal(t, 1, len(tblInfos))
	require.NoError(t, txn.Rollback())

	// tests partition info correctness in schema change
	tblInfo.Partition = &model.PartitionInfo{
		Expr: "aa+1",
		Columns: []model.CIStr{
			model.NewCIStr("aa"),
		},
		Definitions: []model.PartitionDefinition{
			{ID: 1, Name: model.NewCIStr("p1")},
			{ID: 2, Name: model.NewCIStr("p2")},
		},
		Enable:   true,
		DDLState: model.StatePublic,
	}
	// add partition
	tblInfo1 := updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionAddTablePartition, 3, PartitionAttribute, true)
	require.Equal(t, tblInfo.Partition, tblInfo1.Partition)
	// drop partition
	tblInfo.Partition.Definitions = tblInfo.Partition.Definitions[:1]
	tblInfo1 = updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionDropTablePartition, 4, PartitionAttribute, true)
	require.Equal(t, tblInfo.Partition, tblInfo1.Partition)

	// test placement policy correctness in schema change
	tblInfo.PlacementPolicyRef = &model.PolicyRefInfo{
		ID:   1,
		Name: model.NewCIStr("p3"),
	}
	tblInfo1 = updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionAlterTablePlacement, 5, PlacementPolicyAttribute, true)
	require.Equal(t, tblInfo.PlacementPolicyRef, tblInfo1.PlacementPolicyRef)
	tblInfo.PlacementPolicyRef = nil
	updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionAlterTablePlacement, 6, PlacementPolicyAttribute, false)

	// test tiflash replica correctness in schema change
	tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:          1,
		Available:      true,
		LocationLabels: []string{"zone"},
	}
	tblInfo1 = updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionSetTiFlashReplica, 7, TiFlashAttribute, true)
	require.Equal(t, tblInfo.TiFlashReplica, tblInfo1.TiFlashReplica)
	tblInfo.TiFlashReplica = nil
	updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionSetTiFlashReplica, 8, TiFlashAttribute, false)

	// test table lock correctness in schema change
	tblInfo.Lock = &model.TableLockInfo{
		Tp:    model.TableLockRead,
		State: model.TableLockStatePublic,
		TS:    1,
	}
	tblInfo1 = updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionLockTable, 9, TableLockAttribute, true)
	require.Equal(t, tblInfo.Lock, tblInfo1.Lock)
	tblInfo.Lock = nil
	updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionUnlockTable, 10, TableLockAttribute, false)

	// test foreign key correctness in schema change
	tblInfo.ForeignKeys = []*model.FKInfo{{
		ID:        1,
		Name:      model.NewCIStr("fk_1"),
		RefSchema: model.NewCIStr("t"),
		RefTable:  model.NewCIStr("t"),
		RefCols:   []model.CIStr{model.NewCIStr("a")},
		Cols:      []model.CIStr{model.NewCIStr("t_a")},
		State:     model.StateWriteOnly,
	}}
	tblInfo1 = updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionAddForeignKey, 11, ForeignKeysAttribute, true)
	require.Equal(t, tblInfo.ForeignKeys, tblInfo1.ForeignKeys)
	tblInfo.ForeignKeys = nil
	updateTableSpecialAttribute(t, dbInfo, tblInfo, builder, r, model.ActionDropForeignKey, 12, ForeignKeysAttribute, false)
}
