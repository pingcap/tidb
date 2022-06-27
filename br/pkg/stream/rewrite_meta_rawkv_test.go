// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/stretchr/testify/require"
)

var increaseID int64 = 100

func mockGenGenGlobalID(ctx context.Context) (int64, error) {
	increaseID++
	return int64(increaseID), nil
}

func ProduceValue(tableName string, dbID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   dbID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(tableInfo)
}

func MockEmptySchemasReplace() *SchemasReplace {
	dbMap := make(map[OldID]*DBReplace)
	return NewSchemasReplace(
		dbMap,
		0,
		filter.All(),
		mockGenGenGlobalID,
		nil,
	)
}

func produceDBInfoValue(dbName string, dbID int64) ([]byte, error) {
	dbInfo := model.DBInfo{
		ID:   dbID,
		Name: model.NewCIStr(dbName),
	}
	return json.Marshal(&dbInfo)
}

func produceTableInfoValue(tableName string, tableID int64) ([]byte, error) {
	tableInfo := model.TableInfo{
		ID:   tableID,
		Name: model.NewCIStr(tableName),
	}

	return json.Marshal(&tableInfo)
}

func TestRewriteValueForDB(t *testing.T) {
	var (
		dbID   int64 = 1
		dbName       = "db1"
		DBInfo model.DBInfo
	)

	value, err := produceDBInfoValue(dbName, dbID)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace()
	newValue, needWrite, err := sr.rewriteDBInfo(value)
	require.Nil(t, err)
	require.True(t, needWrite)

	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbMap[dbID].NewDBID)

	newId := sr.DbMap[dbID].NewDBID
	newValue, needWrite, err = sr.rewriteDBInfo(value)
	require.Nil(t, err)
	require.True(t, needWrite)

	err = json.Unmarshal(newValue, &DBInfo)
	require.Nil(t, err)
	require.Equal(t, DBInfo.ID, sr.DbMap[dbID].NewDBID)
	require.Equal(t, newId, sr.DbMap[dbID].NewDBID)
}

func TestRewriteValueForTable(t *testing.T) {

	var (
		dbId      int64 = 40
		tableID   int64 = 100
		tableName       = "t1"
		tableInfo model.TableInfo
	)

	value, err := produceTableInfoValue(tableName, tableID)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace()
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)

	newID := sr.DbMap[dbId].TableMap[tableID].NewTableID
	newValue, needRewrite, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.Equal(t, newID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
}

func TestRewriteValueForPartitionTable(t *testing.T) {
	var (
		dbId      int64 = 40
		tableID   int64 = 100
		pt1ID     int64 = 101
		pt2ID     int64 = 102
		tableName       = "t1"
		pt1Name         = "pt1"
		pt2Name         = "pt2"
		tableInfo model.TableInfo
	)

	pt1 := model.PartitionDefinition{
		ID:   pt1ID,
		Name: model.NewCIStr(pt1Name),
	}
	pt2 := model.PartitionDefinition{
		ID:   pt2ID,
		Name: model.NewCIStr(pt2Name),
	}

	pi := model.PartitionInfo{
		Enable:      true,
		Definitions: make([]model.PartitionDefinition, 0),
	}
	pi.Definitions = append(pi.Definitions, pt1)
	pi.Definitions = append(pi.Definitions, pt2)

	tbl := model.TableInfo{
		ID:        tableID,
		Name:      model.NewCIStr(tableName),
		Partition: &pi,
	}
	value, err := json.Marshal(&tbl)
	require.Nil(t, err)

	sr := MockEmptySchemasReplace()
	newValue, needRewrite, err := sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(t, tableInfo.ID, sr.DbMap[dbId].TableMap[tableID].NewTableID)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[0].Name,
		tableInfo.Partition.Definitions[0].Name,
	)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(
		t,
		tbl.Partition.Definitions[1].Name,
		tableInfo.Partition.Definitions[1].Name,
	)

	newID1 := sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID]
	newID2 := sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID]
	newValue, needRewrite, err = sr.rewriteTableInfo(value, dbId)
	require.Nil(t, err)
	require.True(t, needRewrite)

	err = json.Unmarshal(newValue, &tableInfo)
	require.Nil(t, err)
	require.Equal(t, tableInfo.Name.String(), tableName)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[0].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt1ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[0].ID, newID1)
	require.Equal(
		t,
		tableInfo.Partition.Definitions[1].ID,
		sr.DbMap[dbId].TableMap[tableID].PartitionMap[pt2ID],
	)
	require.Equal(t, tableInfo.Partition.Definitions[1].ID, newID2)
}
