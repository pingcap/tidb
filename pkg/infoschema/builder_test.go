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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema/internal"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

type mockAlloc struct {
	autoid.Allocator
	tp autoid.AllocatorType
}

func (m *mockAlloc) GetType() autoid.AllocatorType {
	return m.tp
}

func TestGetKeptAllocators(t *testing.T) {
	checkAllocators := func(allocators autoid.Allocators, expected []autoid.AllocatorType) {
		require.Len(t, allocators.Allocs, len(expected))
		for i, tp := range expected {
			require.Equal(t, tp, allocators.Allocs[i].GetType())
		}
	}
	allocators := autoid.Allocators{Allocs: []autoid.Allocator{
		&mockAlloc{tp: autoid.RowIDAllocType},
		&mockAlloc{tp: autoid.AutoIncrementType},
		&mockAlloc{tp: autoid.AutoRandomType},
	}}
	cases := []struct {
		diff     *model.SchemaDiff
		expected []autoid.AllocatorType
	}{
		{
			diff:     &model.SchemaDiff{Type: model.ActionTruncateTable},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType, autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionRebaseAutoID},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionModifyTableAutoIDCache},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff:     &model.SchemaDiff{Type: model.ActionRebaseAutoRandomBase},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionAddColumn, model.ActionRebaseAutoID}},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionModifyTableAutoIDCache}},
			expected: []autoid.AllocatorType{autoid.AutoRandomType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionRebaseAutoRandomBase}},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType},
		},
		{
			diff: &model.SchemaDiff{Type: model.ActionMultiSchemaChange,
				SubActionTypes: []model.ActionType{model.ActionAddColumn}},
			expected: []autoid.AllocatorType{autoid.RowIDAllocType, autoid.AutoIncrementType, autoid.AutoRandomType},
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			res := getKeptAllocators(c.diff, allocators)
			checkAllocators(res, c.expected)
		})
	}
}

// TestTableName2IDCaseSensitive tests that TableName2ID uses the original
// case-sensitive table name (Name.O) instead of the lowercase name (Name.L).
// This is a regression test for https://github.com/pingcap/tidb/issues/64369
func TestTableName2IDCaseSensitive(t *testing.T) {
	re := internal.CreateAutoIDRequirement(t)
	defer func() {
		err := re.Store().Close()
		require.NoError(t, err)
	}()

	// Create a table with mixed case name
	mixedCaseTableName := "MyTable"
	tblInfo := internal.MockTableInfo(t, re.Store(), mixedCaseTableName)

	// Create a database
	dbInfo := internal.MockDBInfo(t, re.Store(), "testdb")
	dbInfo.Deprecated.Tables = []*model.TableInfo{tblInfo}
	tblInfo.DBID = dbInfo.ID

	// Set up TableName2ID with the original case name (as it would be extracted from JSON "O" field)
	// The key should be the original name "MyTable", not the lowercase "mytable"
	dbInfo.TableName2ID = map[string]int64{
		mixedCaseTableName: tblInfo.ID, // Using original case "MyTable"
	}

	internal.AddDB(t, re.Store(), dbInfo)
	internal.AddTable(t, re.Store(), dbInfo.ID, tblInfo)

	// Build infoschema
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	builder := NewBuilder(re, schemaCacheSize, nil, NewData(), schemaCacheSize > 0)
	err := builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, nil, nil, nil, 1)
	require.NoError(t, err)

	// After InitWithDBInfos, the table should be processed and removed from TableName2ID
	// Before the fix, delete(di.TableName2ID, t.Name.L) would try to delete "mytable"
	// but the key is "MyTable", so it would fail to delete.
	// After the fix, delete(di.TableName2ID, t.Name.O) correctly deletes "MyTable".
	require.Empty(t, dbInfo.TableName2ID, "TableName2ID should be empty after processing, but it still contains: %v", dbInfo.TableName2ID)
}

// TestTableName2IDCaseSensitiveMultipleTables tests that TableName2ID handles
// multiple tables with different case patterns correctly.
func TestTableName2IDCaseSensitiveMultipleTables(t *testing.T) {
	re := internal.CreateAutoIDRequirement(t)
	defer func() {
		err := re.Store().Close()
		require.NoError(t, err)
	}()

	// Create tables with different case patterns
	tableNames := []string{"lowercase", "UPPERCASE", "MixedCase", "CamelCase"}
	tables := make([]*model.TableInfo, 0, len(tableNames))
	tableName2ID := make(map[string]int64)

	for _, name := range tableNames {
		tblInfo := internal.MockTableInfo(t, re.Store(), name)
		tables = append(tables, tblInfo)
		tableName2ID[name] = tblInfo.ID // Use original case as key
	}

	// Create a database
	dbInfo := internal.MockDBInfo(t, re.Store(), "testdb")
	dbInfo.Deprecated.Tables = tables
	dbInfo.TableName2ID = tableName2ID

	for _, tbl := range tables {
		tbl.DBID = dbInfo.ID
	}

	internal.AddDB(t, re.Store(), dbInfo)
	for _, tbl := range tables {
		internal.AddTable(t, re.Store(), dbInfo.ID, tbl)
	}

	// Build infoschema
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	builder := NewBuilder(re, schemaCacheSize, nil, NewData(), schemaCacheSize > 0)
	err := builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, nil, nil, nil, 1)
	require.NoError(t, err)

	// All entries should be deleted from TableName2ID
	require.Empty(t, dbInfo.TableName2ID, "TableName2ID should be empty after processing, but it still contains: %v", dbInfo.TableName2ID)
}

// TestTableName2IDWithUnloadedTables tests that tables not in Deprecated.Tables
// remain in TableName2ID for lazy loading.
func TestTableName2IDWithUnloadedTables(t *testing.T) {
	re := internal.CreateAutoIDRequirement(t)
	defer func() {
		err := re.Store().Close()
		require.NoError(t, err)
	}()

	// Create a loaded table
	loadedTbl := internal.MockTableInfo(t, re.Store(), "LoadedTable")

	// Create a database with only one table loaded
	dbInfo := internal.MockDBInfo(t, re.Store(), "testdb")
	dbInfo.Deprecated.Tables = []*model.TableInfo{loadedTbl}
	loadedTbl.DBID = dbInfo.ID

	// But TableName2ID contains both loaded and unloaded tables
	// (simulating schema cache scenario)
	unloadedTableName := "UnloadedTable"
	unloadedTableID := int64(99999)
	dbInfo.TableName2ID = map[string]int64{
		"LoadedTable":     loadedTbl.ID,
		unloadedTableName: unloadedTableID,
	}

	internal.AddDB(t, re.Store(), dbInfo)
	internal.AddTable(t, re.Store(), dbInfo.ID, loadedTbl)

	// Build infoschema
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	builder := NewBuilder(re, schemaCacheSize, nil, NewData(), schemaCacheSize > 0)
	err := builder.InitWithDBInfos([]*model.DBInfo{dbInfo}, nil, nil, nil, 1)
	require.NoError(t, err)

	// LoadedTable should be removed from TableName2ID
	_, exists := dbInfo.TableName2ID["LoadedTable"]
	require.False(t, exists, "LoadedTable should be removed from TableName2ID")

	// UnloadedTable should remain in TableName2ID for lazy loading
	id, exists := dbInfo.TableName2ID[unloadedTableName]
	require.True(t, exists, "UnloadedTable should remain in TableName2ID")
	require.Equal(t, unloadedTableID, id)
}
