// Copyright 2015 PingCAP, Inc.
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

package meta_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestPlacementPolicy(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)

	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	// test the independent policy ID allocation.
	m := meta.NewMeta(txn)

	// test the meta storage of placemnt policy.
	policy := &model.PolicyInfo{
		ID:   1,
		Name: model.NewCIStr("aa"),
		PlacementSettings: &model.PlacementSettings{
			PrimaryRegion:      "my primary",
			Regions:            "my regions",
			Learners:           1,
			Followers:          2,
			Voters:             3,
			Schedule:           "even",
			Constraints:        "+disk=ssd",
			LearnerConstraints: "+zone=shanghai",
		},
	}
	err = m.CreatePolicy(policy)
	require.NoError(t, err)
	require.Equal(t, policy.ID, int64(1))

	err = m.CreatePolicy(policy)
	require.NotNil(t, err)
	require.True(t, meta.ErrPolicyExists.Equal(err))

	val, err := m.GetPolicy(1)
	require.NoError(t, err)
	require.Equal(t, policy, val)

	// mock updating the placement policy.
	policy.Name = model.NewCIStr("bb")
	policy.LearnerConstraints = "+zone=nanjing"
	err = m.UpdatePolicy(policy)
	require.NoError(t, err)

	val, err = m.GetPolicy(1)
	require.NoError(t, err)
	require.Equal(t, policy, val)

	ps, err := m.ListPolicies()
	require.NoError(t, err)
	require.Equal(t, []*model.PolicyInfo{policy}, ps)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// fetch the stored value after committing.
	txn, err = store.Begin()
	require.NoError(t, err)

	m = meta.NewMeta(txn)
	val, err = m.GetPolicy(1)
	require.NoError(t, err)
	require.Equal(t, policy, val)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestResourceGroup(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	// test the independent policy ID allocation.
	m := meta.NewMeta(txn)
	groups, err := m.ListResourceGroups()
	require.NoError(t, err)
	require.Equal(t, len(groups), 1)
	require.Equal(t, groups[0], meta.DefaultGroupMeta4Test())

	groupID := int64(2)
	checkResourceGroup := func(ru uint64) {
		rg, err := m.GetResourceGroup(groupID)
		require.NoError(t, err)
		require.Equal(t, rg.RURate, ru)
	}

	rg := &model.ResourceGroupInfo{
		ID:   groupID,
		Name: model.NewCIStr("aa"),
		ResourceGroupSettings: &model.ResourceGroupSettings{
			RURate: 100,
		},
	}
	require.NoError(t, m.AddResourceGroup(rg))
	checkResourceGroup(100)

	groups, err = m.ListResourceGroups()
	require.NoError(t, err)
	require.Equal(t, len(groups), 2)

	rg.RURate = 200
	require.NoError(t, m.UpdateResourceGroup(rg))
	checkResourceGroup(200)

	m.DropResourceGroup(groupID)
	_, err = m.GetResourceGroup(groupID)
	require.Error(t, err)
}

func TestMeta(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)

	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)

	n, err := m.GenGlobalID()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = m.GetGlobalID()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	var wg util.WaitGroupWrapper
	wg.Run(func() {
		ids, err := m.GenGlobalIDs(3)
		require.NoError(t, err)
		anyMatch(t, ids, []int64{2, 3, 4}, []int64{6, 7, 8})
	})

	wg.Run(func() {
		ids, err := m.GenGlobalIDs(4)
		require.NoError(t, err)
		anyMatch(t, ids, []int64{5, 6, 7, 8}, []int64{2, 3, 4, 5})
	})
	wg.Wait()

	n, err = m.GetSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	n, err = m.GenSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	n, err = m.GetSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	dbInfo := &model.DBInfo{
		ID:   1,
		Name: model.NewCIStr("a"),
	}
	err = m.CreateDatabase(dbInfo)
	require.NoError(t, err)

	err = m.CreateDatabase(dbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrDBExists.Equal(err))

	v, err := m.GetDatabase(1)
	require.NoError(t, err)
	require.Equal(t, dbInfo, v)

	dbInfo.Name = model.NewCIStr("aa")
	err = m.UpdateDatabase(dbInfo)
	require.NoError(t, err)

	v, err = m.GetDatabase(1)
	require.NoError(t, err)
	require.Equal(t, dbInfo, v)

	dbs, err := m.ListDatabases()
	require.NoError(t, err)
	require.Equal(t, []*model.DBInfo{dbInfo}, dbs)

	tbInfo := &model.TableInfo{
		ID:   1,
		Name: model.NewCIStr("t"),
		DBID: dbInfo.ID,
	}
	err = m.CreateTableOrView(1, dbInfo.Name.L, tbInfo)
	require.NoError(t, err)

	n, err = m.GetAutoIDAccessors(1, 1).RowID().Inc(10)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	n, err = m.GetAutoIDAccessors(1, 1).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	err = m.CreateTableOrView(1, dbInfo.Name.L, tbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrTableExists.Equal(err))

	tbInfo.Name = model.NewCIStr("tt")
	err = m.UpdateTable(1, tbInfo)
	require.NoError(t, err)

	table, err := m.GetTable(1, 1)
	require.NoError(t, err)
	require.Equal(t, tbInfo, table)
	tblExist, err := m.CheckTableExists(1, 1)
	require.NoError(t, err)
	require.Equal(t, true, tblExist)

	table, err = m.GetTable(1, 2)
	require.NoError(t, err)
	require.Nil(t, table)
	tblExist, err = m.CheckTableExists(1, 2)
	require.NoError(t, err)
	require.Equal(t, false, tblExist)

	tbInfo2 := &model.TableInfo{
		ID:   2,
		Name: model.NewCIStr("bb"),
		DBID: dbInfo.ID,
	}
	err = m.CreateTableOrView(1, dbInfo.Name.L, tbInfo2)
	require.NoError(t, err)

	tblName := &model.TableNameInfo{ID: tbInfo.ID, Name: tbInfo.Name}
	tblName2 := &model.TableNameInfo{ID: tbInfo2.ID, Name: tbInfo2.Name}
	tableNames, err := m.ListSimpleTables(1)
	require.NoError(t, err)
	require.Equal(t, []*model.TableNameInfo{tblName, tblName2}, tableNames)
	tables, err := m.ListTables(1)
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{tbInfo, tbInfo2}, tables)
	{
		idx := 0
		err := m.IterTables(1, func(info *model.TableInfo) error {
			require.Less(t, idx, 2)
			if idx == 0 {
				require.Equal(t, tbInfo, info)
				idx += 1
			} else {
				require.Equal(t, tbInfo2, info)
			}
			return nil
		})
		require.NoError(t, err)
	}
	// Generate an auto id.
	n, err = m.GetAutoIDAccessors(1, 2).RowID().Inc(10)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)
	// Make sure the auto id key-value entry is there.
	n, err = m.GetAutoIDAccessors(1, 2).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	err = m.DropTableOrView(1, dbInfo.Name.L, tbInfo2.ID, tbInfo2.Name.L)
	require.NoError(t, err)
	err = m.GetAutoIDAccessors(1, tbInfo2.ID).Del()
	require.NoError(t, err)
	// Make sure auto id key-value entry is gone.
	n, err = m.GetAutoIDAccessors(1, 2).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	tableNames, err = m.ListSimpleTables(1)
	require.NoError(t, err)
	require.Equal(t, []*model.TableNameInfo{tblName}, tableNames)
	tables, err = m.ListTables(1)
	require.NoError(t, err)
	require.Equal(t, []*model.TableInfo{tbInfo}, tables)
	{
		idx := 0
		err := m.IterTables(1, func(info *model.TableInfo) error {
			require.Less(t, idx, 1)
			require.Equal(t, tbInfo, info)
			idx += 1
			return nil
		})
		require.NoError(t, err)
	}
	// Test case for drop a table without delete auto id key-value entry.
	tid := int64(100)
	tbInfo100 := &model.TableInfo{
		ID:   tid,
		Name: model.NewCIStr("t_rename"),
	}
	// Create table.
	err = m.CreateTableOrView(1, dbInfo.Name.L, tbInfo100)
	require.NoError(t, err)
	// Update auto ID.
	currentDBID := int64(1)
	n, err = m.GetAutoIDAccessors(currentDBID, tid).RowID().Inc(10)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)
	// Test to update non-existing auto ID.
	// The table ID doesn't exist.
	// We can no longer test for non-existing ids.
	nonExistentID := int64(1234)
	_, err = m.GetAutoIDAccessors(currentDBID, nonExistentID).RowID().Inc(10)
	require.NoError(t, err)
	//require.True(t, meta.ErrTableNotExists.Equal(err))
	// Test to update non-existing auto ID.
	// The current database ID doesn't exist.
	// We can no longer test for non-existing ids.
	currentDBID = nonExistentID
	_, err = m.GetAutoIDAccessors(currentDBID, tid).RowID().Inc(10)
	require.NoError(t, err)
	//require.True(t, meta.ErrDBNotExists.Equal(err))
	// Test case for CreateTableAndSetAutoID.
	tbInfo3 := &model.TableInfo{
		ID:   3,
		Name: model.NewCIStr("tbl3"),
	}
	err = m.CreateTableAndSetAutoID(1, dbInfo.Name.L, tbInfo3, meta.AutoIDGroup{RowID: 123, IncrementID: 0})
	require.NoError(t, err)
	id, err := m.GetAutoIDAccessors(1, tbInfo3.ID).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(123), id)
	// Test case for GenAutoTableIDKeyValue.
	key, val := m.GenAutoTableIDKeyValue(1, tbInfo3.ID, 1234)
	require.Equal(t, []byte(strconv.FormatInt(1234, 10)), val)
	require.Equal(t, []byte{0x6d, 0x44, 0x42, 0x3a, 0x31, 0x0, 0x0, 0x0, 0x0, 0xfb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x0, 0x0, 0x0, 0xfc}, key)

	err = m.DropDatabase(1, dbInfo.Name.L)
	require.NoError(t, err)
	err = m.DropDatabase(currentDBID, dbInfo.Name.L)
	require.NoError(t, err)

	dbs, err = m.ListDatabases()
	require.NoError(t, err)
	require.Len(t, dbs, 0)

	bootstrapVer, err := m.GetBootstrapVersion()
	require.NoError(t, err)
	require.Equal(t, int64(0), bootstrapVer)

	err = m.FinishBootstrap(int64(1))
	require.NoError(t, err)

	bootstrapVer, err = m.GetBootstrapVersion()
	require.NoError(t, err)
	require.Equal(t, int64(1), bootstrapVer)

	// Test case for meta.FinishBootstrap with a version.
	err = m.FinishBootstrap(int64(10))
	require.NoError(t, err)
	bootstrapVer, err = m.GetBootstrapVersion()
	require.NoError(t, err)

	require.Equal(t, int64(10), bootstrapVer)

	// Test case for SchemaDiff.
	schemaDiff := &model.SchemaDiff{
		Version:    100,
		SchemaID:   1,
		Type:       model.ActionTruncateTable,
		TableID:    2,
		OldTableID: 3,
	}
	err = m.SetSchemaDiff(schemaDiff)
	require.NoError(t, err)
	readDiff, err := m.GetSchemaDiff(schemaDiff.Version)
	require.NoError(t, err)
	require.Equal(t, schemaDiff, readDiff)

	// Test for BDR role
	role, err := m.GetBDRRole()
	require.NoError(t, err)
	require.Len(t, role, 0)
	require.NoError(t, m.SetBDRRole(string(ast.BDRRolePrimary)))
	role, err = m.GetBDRRole()
	require.NoError(t, err)
	require.Equal(t, string(ast.BDRRolePrimary), role)
	require.NoError(t, m.ClearBDRRole())
	role, err = m.GetBDRRole()
	require.NoError(t, err)
	require.Len(t, role, 0)

	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Test for DDLJobHistoryKey.
	key = meta.DDLJobHistoryKey(m, 888)
	require.Equal(t, []byte{0x6d, 0x44, 0x44, 0x4c, 0x4a, 0x6f, 0x62, 0x48, 0x69, 0xff, 0x73, 0x74, 0x6f, 0x72, 0x79, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x78, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf7}, key)
}

func TestSnapshot(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	txn, _ := store.Begin()
	m := meta.NewMeta(txn)
	_, err = m.GenGlobalID()
	require.NoError(t, err)
	n, _ := m.GetGlobalID()
	require.Equal(t, int64(1), n)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	ver1, _ := store.CurrentVersion(kv.GlobalTxnScope)
	time.Sleep(time.Millisecond)
	txn, _ = store.Begin()
	m = meta.NewMeta(txn)
	_, err = m.GenGlobalID()
	require.NoError(t, err)
	n, _ = m.GetGlobalID()
	require.Equal(t, int64(2), n)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	snapshot := store.GetSnapshot(ver1)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	n, _ = snapMeta.GetGlobalID()
	require.Equal(t, int64(1), n)
	_, err = snapMeta.GenGlobalID()
	require.NotNil(t, err)
	require.Equal(t, "[structure:8220]write on snapshot", err.Error())
}

func TestElement(t *testing.T) {
	checkElement := func(key []byte, resErr error) {
		e := &meta.Element{ID: 123, TypeKey: key}
		eBytes := e.EncodeElement()
		resE, err := meta.DecodeElement(eBytes)
		if resErr == nil {
			require.NoError(t, err)
			require.Equal(t, resE, e)
		} else {
			require.EqualError(t, err, resErr.Error())
		}
	}
	key := []byte("_col")
	checkElement(key, errors.Errorf(`invalid encoded element key prefix "_col\x00"`))
	checkElement(meta.IndexElementKey, nil)
	checkElement(meta.ColumnElementKey, nil)
	key = []byte("inexistent")
	checkElement(key, errors.Errorf("invalid encoded element key prefix %q", key[:5]))

	_, err := meta.DecodeElement([]byte("_col"))
	require.EqualError(t, err, `invalid encoded element "_col" length 4`)
	_, err = meta.DecodeElement(meta.ColumnElementKey)
	require.EqualError(t, err, `invalid encoded element "_col_" length 5`)
}

func BenchmarkGenGlobalIDs(b *testing.B) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(b, err)
	defer func() {
		err := store.Close()
		require.NoError(b, err)
	}()

	txn, err := store.Begin()
	require.NoError(b, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(b, err)
	}()

	m := meta.NewMeta(txn)

	b.ResetTimer()
	var ids []int64
	for i := 0; i < b.N; i++ {
		ids, _ = m.GenGlobalIDs(10)
	}
	require.Len(b, ids, 10)
	require.Equal(b, int64(b.N)*10, ids[9])
}

func BenchmarkGenGlobalIDOneByOne(b *testing.B) {
	store, err := mockstore.NewMockStore()
	require.NoError(b, err)
	defer func() {
		err := store.Close()
		require.NoError(b, err)
	}()

	txn, err := store.Begin()
	require.NoError(b, err)
	defer func() {
		err := txn.Rollback()
		require.NoError(b, err)
	}()

	m := meta.NewMeta(txn)

	b.ResetTimer()
	var id int64
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			id, _ = m.GenGlobalID()
		}
	}
	require.Equal(b, int64(b.N)*10, id)
}

func anyMatch(t *testing.T, ids []int64, candidates ...[]int64) {
	comment := fmt.Sprintf("ids %v cannot match any of %v", ids, candidates)

	for _, candidate := range candidates {
		if match(ids, candidate) {
			return
		}
	}

	require.FailNow(t, comment)
}

func match(ids, candidate []int64) bool {
	if len(ids) != len(candidate) {
		return false
	}

	for i, v := range candidate {
		if ids[i] != v {
			return false
		}
	}

	return true
}

func TestDBKey(b *testing.T) {
	var dbID int64 = 10
	dbKey := meta.DBkey(dbID)
	require.True(b, meta.IsDBkey(dbKey))

	parseID, err := meta.ParseDBKey(dbKey)
	require.NoError(b, err)
	require.Equal(b, dbID, parseID)
}

func TestTableKey(b *testing.T) {
	var tableID int64 = 10
	tableKey := meta.TableKey(tableID)
	require.True(b, meta.IsTableKey(tableKey))

	parseID, err := meta.ParseTableKey(tableKey)
	require.NoError(b, err)
	require.Equal(b, tableID, parseID)
}

func TestAutoTableIDKey(b *testing.T) {
	var tableID int64 = 10
	tableKey := meta.AutoTableIDKey(tableID)
	require.True(b, meta.IsAutoTableIDKey(tableKey))

	id, err := meta.ParseAutoTableIDKey(tableKey)
	require.NoError(b, err)
	require.Equal(b, tableID, id)
}

func TestAutoRandomTableIDKey(b *testing.T) {
	var tableID int64 = 10
	key := meta.AutoRandomTableIDKey(tableID)
	require.True(b, meta.IsAutoRandomTableIDKey(key))

	id, err := meta.ParseAutoRandomTableIDKey(key)
	require.NoError(b, err)
	require.Equal(b, tableID, id)
}

func TestSequenceKey(b *testing.T) {
	var tableID int64 = 10
	key := meta.SequenceKey(tableID)
	require.True(b, meta.IsSequenceKey(key))

	id, err := meta.ParseSequenceKey(key)
	require.NoError(b, err)
	require.Equal(b, tableID, id)
}

func TestCreateMySQLDatabase(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)

	dbID, err := m.CreateMySQLDatabaseIfNotExists()
	require.NoError(t, err)
	require.Greater(t, dbID, int64(0))

	anotherDBID, err := m.CreateMySQLDatabaseIfNotExists()
	require.NoError(t, err)
	require.Equal(t, dbID, anotherDBID)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestName(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	txn, err := store.Begin()
	require.NoError(t, err)

	// TestTableNameKey
	m := meta.NewMeta(txn)
	key := m.TableNameKey("db", "tb")
	require.Equal(t, string(key), "Names:db\x00tb")

	// TestCheckTableNameExists
	err = m.CheckTableNameExists(m.TableNameKey("db", "tb"))
	require.True(t, meta.ErrTableNotExists.Equal(err))
	// TestCheckTableNameNotExists
	err = m.CheckTableNameNotExists(m.TableNameKey("db", "tb"))
	require.NoError(t, err)

	// TestCreateTable
	err = m.CreateTableName("db", "tb", 1)
	require.NoError(t, err)
	err = m.CheckTableNameExists(m.TableNameKey("db", "tb"))
	require.NoError(t, err)
	err = m.CheckTableNameNotExists(m.TableNameKey("db", "tb"))
	require.True(t, meta.ErrTableExists.Equal(err))
	err = m.CreateTableName("db", "t", 2)
	require.NoError(t, err)

	err = m.CreateTableName("db", "tb", 3)
	require.True(t, meta.ErrTableExists.Equal(err))

	err = m.CreateTableName("d", "btb", 3)
	require.NoError(t, err)
	err = m.CheckTableNameExists(m.TableNameKey("d", "btb"))
	require.NoError(t, err)

	// TestDropTableName
	err = m.DropTableName("db1", "b")
	require.True(t, meta.ErrTableNotExists.Equal(err))
	err = m.DropTableName("db", "tb")
	require.NoError(t, err)

	// TestDropDatabaseName
	err = m.DropDatabaseName("xx")
	require.NoError(t, err)
	err = m.DropDatabaseName("d")
	require.NoError(t, err)
	err = m.CheckTableNameNotExists(m.TableNameKey("d", "btb"))
	require.NoError(t, err)
	err = m.CheckTableNameExists(m.TableNameKey("db", "t"))
	require.NoError(t, err)

	// TestClearAllTableNames
	err = m.ClearAllTableNames()
	require.NoError(t, err)
	err = m.CheckTableNameNotExists(m.TableNameKey("db1", "t"))
	require.NoError(t, err)

	// TestDDLV2Initialized
	v, err := m.GetDDLV2Initialized()
	require.NoError(t, err)
	require.Equal(t, v, false)
	err = m.SetDDLV2Initialized(true)
	require.NoError(t, err)
	v, err = m.GetDDLV2Initialized()
	require.NoError(t, err)
	require.Equal(t, v, true)
	err = m.SetDDLV2Initialized(false)
	require.NoError(t, err)
	v, err = m.GetDDLV2Initialized()
	require.NoError(t, err)
	require.Equal(t, v, false)

	err = txn.Rollback()
	require.NoError(t, err)
}
