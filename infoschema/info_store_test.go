// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestInfoStoreLowerCaseTableNames(t *testing.T) {
	dbName := model.NewCIStr("DBName")
	lowerDBName := model.NewCIStr("dbname")
	tableName := model.NewCIStr("TableName")
	lowerTableName := model.NewCIStr("tablename")
	dbInfo := &model.DBInfo{Name: dbName}
	tableInfo := &model.TableInfo{Name: tableName}

	// case-sensitive

	is := NewInfoStore(0)
	is.PutSchema(dbInfo)
	got := is.SchemaByName(dbName)
	require.NotNil(t, got)
	got = is.SchemaByName(lowerDBName)
	require.Nil(t, got)

	err := is.PutTable(lowerDBName, tableInfo)
	require.True(t, ErrDatabaseNotExists.Equal(err))
	err = is.PutTable(dbName, tableInfo)
	require.NoError(t, err)
	got2, err := is.TableByName(dbName, tableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	got2, err = is.TableByName(lowerTableName, tableName)
	require.True(t, ErrDatabaseNotExists.Equal(err))
	require.Nil(t, got2)
	got2, err = is.TableByName(dbName, lowerTableName)
	require.True(t, ErrTableNotExists.Equal(err))
	require.Nil(t, got2)

	// compare-insensitive

	is = NewInfoStore(2)
	is.PutSchema(dbInfo)
	got = is.SchemaByName(dbName)
	require.NotNil(t, got)
	got = is.SchemaByName(lowerDBName)
	require.NotNil(t, got)
	require.Equal(t, dbName, got.Name)

	err = is.PutTable(lowerDBName, tableInfo)
	require.NoError(t, err)
	got2, err = is.TableByName(dbName, tableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	got2, err = is.TableByName(dbName, lowerTableName)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, tableName, got2.Name)
}

func TestInfoStoreDeleteTables(t *testing.T) {
	is := NewInfoStore(0)
	dbName1 := model.NewCIStr("DBName1")
	dbName2 := model.NewCIStr("DBName2")
	tableName1 := model.NewCIStr("TableName1")
	tableName2 := model.NewCIStr("TableName2")
	dbInfo1 := &model.DBInfo{Name: dbName1}
	dbInfo2 := &model.DBInfo{Name: dbName2}
	tableInfo1 := &model.TableInfo{Name: tableName1}
	tableInfo2 := &model.TableInfo{Name: tableName2}

	is.PutSchema(dbInfo1)
	err := is.PutTable(dbName1, tableInfo1)
	require.NoError(t, err)
	err = is.PutTable(dbName1, tableInfo2)
	require.NoError(t, err)

	// db2 not created
	ok := is.DeleteSchema(dbName2)
	require.False(t, ok)
	err = is.PutTable(dbName2, tableInfo1)
	require.True(t, ErrDatabaseNotExists.Equal(err))
	err = is.DeleteTable(dbName2, tableName1)
	require.True(t, ErrDatabaseNotExists.Equal(err))

	is.PutSchema(dbInfo2)
	err = is.PutTable(dbName2, tableInfo1)
	require.NoError(t, err)

	err = is.DeleteTable(dbName2, tableName2)
	require.True(t, ErrTableNotExists.Equal(err))
	err = is.DeleteTable(dbName2, tableName1)
	require.NoError(t, err)

	// delete db will remove its tables
	ok = is.DeleteSchema(dbName1)
	require.True(t, ok)
	_, err = is.TableByName(dbName1, tableName1)
	require.True(t, ErrDatabaseNotExists.Equal(err))
}
