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
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	_ "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mock"
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
		Name: pmodel.NewCIStr("aa"),
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
	policy.Name = pmodel.NewCIStr("bb")
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
		Name: pmodel.NewCIStr("aa"),
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
		Name: pmodel.NewCIStr("a"),
	}
	err = m.CreateDatabase(dbInfo)
	require.NoError(t, err)

	err = m.CreateDatabase(dbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrDBExists.Equal(err))

	v, err := m.GetDatabase(1)
	require.NoError(t, err)
	require.Equal(t, dbInfo, v)

	dbInfo.Name = pmodel.NewCIStr("aa")
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
		Name: pmodel.NewCIStr("t"),
		DBID: dbInfo.ID,
	}
	err = m.CreateTableOrView(1, tbInfo)
	require.NoError(t, err)

	n, err = m.GetAutoIDAccessors(1, 1).RowID().Inc(10)
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	n, err = m.GetAutoIDAccessors(1, 1).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(10), n)

	err = m.CreateTableOrView(1, tbInfo)
	require.NotNil(t, err)
	require.True(t, meta.ErrTableExists.Equal(err))

	tbInfo.Name = pmodel.NewCIStr("tt")
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
		Name: pmodel.NewCIStr("bb"),
		DBID: dbInfo.ID,
	}
	err = m.CreateTableOrView(1, tbInfo2)
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

	err = m.DropTableOrView(1, tbInfo2.ID)
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
		Name: pmodel.NewCIStr("t_rename"),
	}
	// Create table.
	err = m.CreateTableOrView(1, tbInfo100)
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
		Name: pmodel.NewCIStr("tbl3"),
	}
	err = m.CreateTableAndSetAutoID(1, tbInfo3, meta.AutoIDGroup{RowID: 123, IncrementID: 0})
	require.NoError(t, err)
	id, err := m.GetAutoIDAccessors(1, tbInfo3.ID).RowID().Get()
	require.NoError(t, err)
	require.Equal(t, int64(123), id)
	// Test case for GenAutoTableIDKeyValue.
	key, val := m.GenAutoTableIDKeyValue(1, tbInfo3.ID, 1234)
	require.Equal(t, []byte(strconv.FormatInt(1234, 10)), val)
	require.Equal(t, []byte{0x6d, 0x44, 0x42, 0x3a, 0x31, 0x0, 0x0, 0x0, 0x0, 0xfb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x0, 0x0, 0x0, 0xfc}, key)

	err = m.DropDatabase(1)
	require.NoError(t, err)
	err = m.DropDatabase(currentDBID)
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

func TestIsTableInfoMustLoad(t *testing.T) {
	tableInfo := &model.TableInfo{
		TTLInfo: &model.TTLInfo{IntervalExprStr: "1", IntervalTimeUnit: int(ast.TimeUnitDay), JobInterval: "1h"},
	}
	b, err := json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1},
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		PlacementPolicyRef: &model.PolicyRefInfo{ID: 1},
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		Partition: &model.PartitionInfo{Expr: "a"},
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		Lock: &model.TableLockInfo{State: model.TableLockStatePreLock},
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		ForeignKeys: []*model.FKInfo{{ID: 1}},
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		TempTableType: model.TempTableGlobal,
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.True(t, meta.IsTableInfoMustLoad(b))

	tableInfo = &model.TableInfo{
		ID: 123,
	}
	b, err = json.Marshal(tableInfo)
	require.NoError(t, err)
	require.False(t, meta.IsTableInfoMustLoad(b))
}

func TestIsTableInfoMustLoadSubStringsOrder(t *testing.T) {
	// The order matter!
	// IsTableInfoMustLoad relies on the order of the json marshal result,
	// or the internal of the json marshal in other words.
	// This test cover the invariance, if Go std library changes, we can catch it.
	tableInfo := &model.TableInfo{}
	b, err := json.Marshal(tableInfo)
	require.NoError(t, err)
	expect := `{"id":0,"name":{"O":"","L":""},"charset":"","collate":"","cols":null,"index_info":null,"constraint_info":null,"fk_info":null,"state":0,"pk_is_handle":false,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":0,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":0,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":0,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}`
	require.Equal(t, string(b), expect)
}

func TestTableNameExtract(t *testing.T) {
	var tbl model.TableInfo
	tbl.Name = pmodel.NewCIStr(`a`)
	b, err := json.Marshal(tbl)
	require.NoError(t, err)

	nameLRegex := regexp.MustCompile(meta.NameExtractRegexp)
	nameLMatch := nameLRegex.FindStringSubmatch(string(b))
	require.Len(t, nameLMatch, 2)
	require.Equal(t, "a", nameLMatch[1])

	tbl.Name = pmodel.NewCIStr(`"a"`)
	b, err = json.Marshal(tbl)
	require.NoError(t, err)
	nameLMatch = nameLRegex.FindStringSubmatch(string(b))
	require.Len(t, nameLMatch, 2)
	require.Equal(t, `"a"`, meta.Unescape(nameLMatch[1]))

	tbl.Name = pmodel.NewCIStr(`""a"`)
	b, err = json.Marshal(tbl)
	require.NoError(t, err)
	nameLMatch = nameLRegex.FindStringSubmatch(string(b))
	require.Len(t, nameLMatch, 2)
	require.Equal(t, `""a"`, meta.Unescape(nameLMatch[1]))

	tbl.Name = pmodel.NewCIStr(`"\"a"`)
	b, err = json.Marshal(tbl)
	require.NoError(t, err)
	nameLMatch = nameLRegex.FindStringSubmatch(string(b))
	require.Len(t, nameLMatch, 2)
	require.Equal(t, `"\"a"`, meta.Unescape(nameLMatch[1]))

	tbl.Name = pmodel.NewCIStr(`"\"啊"`)
	b, err = json.Marshal(tbl)
	require.NoError(t, err)
	nameLMatch = nameLRegex.FindStringSubmatch(string(b))
	require.Len(t, nameLMatch, 2)
	require.Equal(t, `"\"啊"`, meta.Unescape(nameLMatch[1]))
}

func TestNameExtractFromJob(t *testing.T) {
	type extractTestCase struct {
		schemaName string
		tableName  string
	}

	var job model.Job
	// Inject some table_name and schema_name into other fields of json
	job.Error = dbterror.ClassDDL.Synthesize(terror.CodeUnknown, `test error, "table_name":"aaa", "schema_name":"bbb"`)
	job.Warning = dbterror.ClassDDL.Synthesize(terror.CodeUnknown, `test warning, "table_name":"ccc", "schema_name":"ddd"`)
	job.Query = `create table test.t1(id int) comment 'create table, table_name:"eee", schema_name:"fff"'`

	var testCases = []extractTestCase{
		// Normal string
		{"", "schema_name"},
		{"table_name", ""},
		// String with quota
		{`"quota_schema_name"`, `"quota_table_name"`},
		{`"single_quota`, `""triple_quota"`},
		{"\"schema_name\"", "\"table_name\""},
		// String with slash
		{"\\", "\\\\"},
		// Unicode
		{"中文1", "中文2"},
		{"😋", "😭"},
		// Other interpunction
		{"comma,1", "dot.3"},
		// Put it together
		{`"combine:\\\",你好\\`, `"schema_name:1️⃣","table_name:2️⃣"`},
	}

	for _, tc := range testCases {
		job.SchemaName = tc.schemaName
		job.TableName = tc.tableName

		b, err := job.Encode(true)
		require.NoError(t, err)

		schemaName, tableName, err := meta.ExtractSchemaAndTableNameFromJob(b)
		require.NoError(t, err)

		require.Equal(t, tc.schemaName, schemaName)
		require.Equal(t, tc.tableName, tableName)
	}
}

var benchCases = [][2]string{
	{"narrow", `CREATE TABLE t (c INT PRIMARY KEY);`},
	{"wide", `
CREATE TABLE t (
	c BIGINT PRIMARY KEY AUTO_RANDOM,
	c2 TINYINT,
	c3 BLOB,
	c4 VARCHAR(255) DEFAULT 'ohsdfihusdfihusdfiuh',
	c5 FLOAT,
	c6 BIGINT UNSIGNED,
	c7 DECIMAL(10, 2),
	c8 CHAR(10),
	c9 TEXT,
	c10 DATE,
	c11 TIME,
	c12 TIMESTAMP,
	c13 DATETIME,
	INDEX idx(c2),
	INDEX idx2(c4, c5),
	INDEX idx3(c6, c2),
    UNIQUE INDEX idx4(c12),
    INDEX idx5((c + c2))
);`},
}

func BenchmarkIsTableInfoMustLoad(b *testing.B) {
	for _, benchCase := range benchCases {
		b.Run(benchCase[0], func(b *testing.B) {
			benchIsTableInfoMustLoad(b, benchCase[1])
		})
	}
}

func getTableInfoJSON(b *testing.B, sql string) []byte {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(b, err)
	se := mock.NewContext()
	tblInfo, err := ddl.MockTableInfo(se, stmt.(*ast.CreateTableStmt), 1)
	require.NoError(b, err)
	data, err := json.Marshal(tblInfo)
	require.NoError(b, err)

	return data
}

func benchIsTableInfoMustLoad(b *testing.B, sql string) {
	data := getTableInfoJSON(b, sql)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got := meta.IsTableInfoMustLoad(data)
		intest.Assert(!got)
	}
}

func BenchmarkTableNameInfo(b *testing.B) {
	for _, benchCase := range benchCases {
		b.Run(benchCase[0]+"-json", func(b *testing.B) {
			benchJSONTableNameInfo(b, benchCase[1])
		})
		b.Run(benchCase[0]+"-fastjson", func(b *testing.B) {
			benchFastJSONTableNameInfo(b, benchCase[1])
		})
	}
}

func benchJSONTableNameInfo(b *testing.B, sql string) {
	data := getTableInfoJSON(b, sql)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbInfo := &model.TableNameInfo{}
		err := json.Unmarshal(data, tbInfo)
		intest.Assert(tbInfo.ID == 1)
		intest.Assert(tbInfo.Name.L == "t")
		intest.AssertNoError(err)
	}
}

func benchFastJSONTableNameInfo(b *testing.B, sql string) {
	data := getTableInfoJSON(b, sql)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbInfo, err := meta.FastUnmarshalTableNameInfo(data)
		intest.AssertNoError(err)
		intest.Assert(tbInfo.ID == 1)
		intest.Assert(tbInfo.Name.L == "t")
	}
}

func TestInfoSchemaV2SpecialAttributeCorrectnessAfterBootstrap(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// create database
	dbInfo := &model.DBInfo{
		ID:    10001,
		Name:  pmodel.NewCIStr("sc"),
		State: model.StatePublic,
	}

	// create table with special attributes
	tblInfo := &model.TableInfo{
		ID:    10002,
		Name:  pmodel.NewCIStr("cs"),
		State: model.StatePublic,
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 11, Name: pmodel.NewCIStr("p1")},
				{ID: 22, Name: pmodel.NewCIStr("p2")},
			},
			Enable: true,
		},
		ForeignKeys: []*model.FKInfo{{
			ID:       1,
			Name:     pmodel.NewCIStr("fk"),
			RefTable: pmodel.NewCIStr("t"),
			RefCols:  []pmodel.CIStr{pmodel.NewCIStr("a")},
			Cols:     []pmodel.CIStr{pmodel.NewCIStr("t_a")},
		}},
		TiFlashReplica: &model.TiFlashReplicaInfo{
			Count:          0,
			LocationLabels: []string{"a,b,c"},
			Available:      true,
		},
		Lock: &model.TableLockInfo{
			Tp:    pmodel.TableLockRead,
			State: model.TableLockStatePreLock,
			TS:    0,
		},
		PlacementPolicyRef: &model.PolicyRefInfo{
			ID:   1,
			Name: pmodel.NewCIStr("r1"),
		},
		TTLInfo: &model.TTLInfo{
			IntervalExprStr:  "1",
			IntervalTimeUnit: int(ast.TimeUnitDay),
			Enable:           true,
			JobInterval:      "1h",
		},
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		require.NoError(t, err)
		err = meta.NewMeta(txn).CreateTableOrView(dbInfo.ID, tblInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)

	// bootstrap
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	// verify partition info correctness
	tblInfoRes := dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.PartitionAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.Partition, tblInfoRes[0].TableInfos[0].Partition)
	// foreign key info
	tblInfoRes = dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.ForeignKeysAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.ForeignKeys, tblInfoRes[0].TableInfos[0].ForeignKeys)
	// tiflash replica info
	tblInfoRes = dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.TiFlashAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.TiFlashReplica, tblInfoRes[0].TableInfos[0].TiFlashReplica)
	// lock info
	tblInfoRes = dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.TableLockAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.Lock, tblInfoRes[0].TableInfos[0].Lock)
	// placement policy
	tblInfoRes = dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.PlacementPolicyAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.PlacementPolicyRef, tblInfoRes[0].TableInfos[0].PlacementPolicyRef)
	// ttl info
	tblInfoRes = dom.InfoSchema().ListTablesWithSpecialAttribute(infoschema.TTLAttribute)
	require.Equal(t, len(tblInfoRes[0].TableInfos), 1)
	require.Equal(t, tblInfo.TTLInfo, tblInfoRes[0].TableInfos[0].TTLInfo)
}

func TestInfoSchemaV2DataFieldsCorrectnessAfterBootstrap(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	// create database
	dbInfo := &model.DBInfo{
		ID:      10001,
		Name:    pmodel.NewCIStr("sc"),
		Charset: "utf8",
		Collate: "utf8_general_ci",
		State:   model.StatePublic,
	}

	// create table with partition info
	tblInfo := &model.TableInfo{
		ID:      10002,
		Name:    pmodel.NewCIStr("cs"),
		Charset: "latin1",
		Collate: "latin1_bin",
		State:   model.StatePublic,
		Partition: &model.PartitionInfo{
			Definitions: []model.PartitionDefinition{
				{ID: 1, Name: pmodel.NewCIStr("p1")},
			},
			Enable: true,
		},
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		err := meta.NewMeta(txn).CreateDatabase(dbInfo)
		require.NoError(t, err)
		err = meta.NewMeta(txn).CreateTableOrView(dbInfo.ID, tblInfo)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)

	// bootstrap
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	is := dom.InfoSchema()
	//byID, traverse byID and load from store
	tbl, ok := is.TableByID(context.Background(), 10002)
	require.True(t, ok)
	require.Equal(t, tbl.Meta().ID, tblInfo.ID)

	//byName, traverse byName and load from store,
	tbl, err = is.TableByName(context.Background(), pmodel.NewCIStr("sc"), pmodel.NewCIStr("cs"))
	require.NoError(t, err)
	require.Equal(t, tbl.Meta().ID, tblInfo.ID)

	//tableCache, table info exists in cache now, just use id to seek
	tbl, ok = is.TableByID(context.Background(), 10002)
	require.True(t, ok)
	require.Equal(t, tbl.Meta().ID, tblInfo.ID)

	//schemaMap, traverse schemaMap find dbInfo
	db, ok := is.SchemaByName(pmodel.NewCIStr("sc"))
	require.True(t, ok)
	require.Equal(t, db.ID, dbInfo.ID)

	//schemaID2Name, traverse schemaID2Name find dbInfo
	db, ok = is.SchemaByID(dbInfo.ID)
	require.True(t, ok)
	require.Equal(t, db.ID, dbInfo.ID)

	//pid2tid, traverse pid2tid find tblInfo, dbInfo and partition info
	tbl, ok = is.TableByID(context.Background(), 10002)
	require.True(t, ok)
	require.Equal(t, len(tbl.Meta().GetPartitionInfo().Definitions), 1)
	pid := tbl.Meta().GetPartitionInfo().Definitions[0].ID
	tbl, db, pDef := is.FindTableByPartitionID(pid)
	require.NotNil(t, tbl)
	require.NotNil(t, db)
	require.NotNil(t, pDef)
}

func TestInfoSchemaMiscFieldsCorrectnessAfterBootstrap(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	dbInfo := &model.DBInfo{
		ID:    10001,
		Name:  pmodel.NewCIStr("sc"),
		State: model.StatePublic,
	}
	policy := &model.PolicyInfo{
		ID:   2,
		Name: pmodel.NewCIStr("policy_1"),
		PlacementSettings: &model.PlacementSettings{
			PrimaryRegion: "r1",
			Regions:       "r1,r2",
		},
	}
	group := &model.ResourceGroupInfo{
		ID:   3,
		Name: pmodel.NewCIStr("groupName_1"),
	}
	tblInfo := &model.TableInfo{
		ID:    10002,
		Name:  pmodel.NewCIStr("cs"),
		State: model.StatePublic,
		ForeignKeys: []*model.FKInfo{{
			ID:        1,
			Name:      pmodel.NewCIStr("fk_1"),
			RefSchema: pmodel.NewCIStr("t1"),
			RefTable:  pmodel.NewCIStr("parent"),
			Version:   1,
		}},
		PlacementPolicyRef: &model.PolicyRefInfo{
			ID:   policy.ID,
			Name: policy.Name,
		},
	}
	tblInfo1 := &model.TableInfo{
		ID:            10003,
		Name:          pmodel.NewCIStr("cs"),
		State:         model.StatePublic,
		TempTableType: model.TempTableLocal,
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	err = kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := m.CreatePolicy(policy)
		require.NoError(t, err)
		err = m.AddResourceGroup(group)
		require.NoError(t, err)
		err = m.CreateDatabase(dbInfo)
		require.NoError(t, err)
		err = m.CreateTableOrView(dbInfo.ID, tblInfo)
		require.NoError(t, err)
		err = m.CreateTableOrView(dbInfo.ID, tblInfo1)
		require.NoError(t, err)
		return errors.Trace(err)
	})
	require.NoError(t, err)

	// bootstrap
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	is := dom.InfoSchema()
	tbl, ok := is.TableByID(context.Background(), 10002)
	require.True(t, ok)
	require.Equal(t, tbl.Meta().ID, tblInfo.ID)
	// placement policy
	policy1 := is.AllPlacementPolicies()
	require.Equal(t, len(policy1), 1)
	require.Equal(t, policy1[0].Name, policy.Name)
	// resource group
	group1 := is.AllResourceGroups()
	require.Equal(t, len(group1), 2)
	sort.Slice(group1, func(i, j int) bool {
		return group1[i].Name.L < group1[j].Name.L
	})
	require.Equal(t, group1[1].Name, group.Name)
	// referred foreign key
	referredFk := is.GetTableReferredForeignKeys(tblInfo.ForeignKeys[0].RefSchema.L, tblInfo.ForeignKeys[0].RefTable.L)
	require.Equal(t, len(referredFk), 1)
	require.Equal(t, referredFk[0].ChildFKName, tblInfo.ForeignKeys[0].Name)
	// temp table
	require.True(t, is.HasTemporaryTable())
}
