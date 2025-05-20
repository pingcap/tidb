// Copyright 2022-present PingCAP, Inc.
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

package stream

import (
	"github.com/pingcap/tidb/pkg/meta/model"
)

// TableLocationInfo stores the table name, db id, and parent table id if is a partition
type TableLocationInfo struct {
	DbID          int64
	TableName     string
	IsPartition   bool
	ParentTableID int64 // only meaningful when IsPartition is true
}

type LogBackupTableHistoryManager struct {
	// maps table/partition ID to [original, current] location info
	tableNameHistory map[int64][2]TableLocationInfo
	dbIdToName       map[int64]string
}

func NewTableHistoryManager() *LogBackupTableHistoryManager {
	return &LogBackupTableHistoryManager{
		tableNameHistory: make(map[int64][2]TableLocationInfo),
		dbIdToName:       make(map[int64]string),
	}
}

// AddTableHistory adds or updates history for a regular table
func (info *LogBackupTableHistoryManager) AddTableHistory(tableId int64, tableName string, dbID int64) {
	locationInfo := TableLocationInfo{
		DbID:          dbID,
		TableName:     tableName,
		IsPartition:   false,
		ParentTableID: 0,
	}
	info.addHistory(tableId, locationInfo)
}

// AddPartitionHistory adds or updates history for a partition
func (info *LogBackupTableHistoryManager) AddPartitionHistory(partitionID int64, tableName string,
	dbID int64, parentTableID int64) {
	locationInfo := TableLocationInfo{
		DbID:          dbID,
		TableName:     tableName,
		IsPartition:   true,
		ParentTableID: parentTableID,
	}
	info.addHistory(partitionID, locationInfo)
}

// addHistory is a helper method to maintain the history
func (info *LogBackupTableHistoryManager) addHistory(id int64, locationInfo TableLocationInfo) {
	existing, exists := info.tableNameHistory[id]
	if !exists {
		// first occurrence - store as both original and current
		info.tableNameHistory[id] = [2]TableLocationInfo{locationInfo, locationInfo}
	} else {
		info.tableNameHistory[id] = [2]TableLocationInfo{existing[0], locationInfo}
	}
}

func (info *LogBackupTableHistoryManager) RecordDBIdToName(dbId int64, dbName string) {
	info.dbIdToName[dbId] = dbName
}

// GetTableHistory returns information about all tables that have been renamed.
// Returns a map of table IDs to their original and current locations
func (info *LogBackupTableHistoryManager) GetTableHistory() map[int64][2]TableLocationInfo {
	return info.tableNameHistory
}

func (info *LogBackupTableHistoryManager) GetDBNameByID(dbId int64) (string, bool) {
	name, ok := info.dbIdToName[dbId]
	return name, ok
}

func (info *LogBackupTableHistoryManager) GetNewlyCreatedDBHistory() map[int64]string {
	return info.dbIdToName
}

// OnDatabaseInfo implements MetaInfoCollector.OnDatabaseInfo
func (info *LogBackupTableHistoryManager) OnDatabaseInfo(dbInfo *model.DBInfo) {
	info.RecordDBIdToName(dbInfo.ID, dbInfo.Name.O)
}

// OnTableInfo implements MetaInfoCollector.OnTableInfo
func (info *LogBackupTableHistoryManager) OnTableInfo(dbID int64, tableInfo *model.TableInfo) {
	info.AddTableHistory(tableInfo.ID, tableInfo.Name.O, dbID)

	// add history for all partitions if this is a partitioned table
	if tableInfo.Partition != nil && tableInfo.Partition.Definitions != nil {
		for _, partition := range tableInfo.Partition.Definitions {
			info.AddPartitionHistory(partition.ID, tableInfo.Name.O, dbID, tableInfo.ID)
		}
	}
}
