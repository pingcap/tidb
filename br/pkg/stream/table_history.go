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

// TableLocationInfo stores the table name, db id, and parent table id if is a partition
type TableLocationInfo struct {
	DbID          int64
	TableName     string
	IsPartition   bool
	ParentTableID int64  // only meaningful when IsPartition is true
	Timestamp     uint64 // timestamp when this location info was recorded
}

type LogBackupTableHistoryManager struct {
	// maps table/partition ID to [original, current] location info
	tableNameHistory map[int64][2]TableLocationInfo
	dbIdToName       map[int64]string
	// maps db ID to timestamp when it was last updated
	dbTimestamps map[int64]uint64
}

func NewTableHistoryManager() *LogBackupTableHistoryManager {
	return &LogBackupTableHistoryManager{
		tableNameHistory: make(map[int64][2]TableLocationInfo),
		dbIdToName:       make(map[int64]string),
		dbTimestamps:     make(map[int64]uint64),
	}
}

// AddTableHistory adds or updates history for a regular table
func (info *LogBackupTableHistoryManager) AddTableHistory(tableId int64, tableName string, dbID int64, ts uint64) {
	locationInfo := TableLocationInfo{
		DbID:          dbID,
		TableName:     tableName,
		IsPartition:   false,
		ParentTableID: 0,
		Timestamp:     ts,
	}
	info.addHistory(tableId, locationInfo)
}

// AddPartitionHistory adds or updates history for a partition
func (info *LogBackupTableHistoryManager) AddPartitionHistory(partitionID int64, tableName string,
	dbID int64, parentTableID int64, ts uint64) {
	locationInfo := TableLocationInfo{
		DbID:          dbID,
		TableName:     tableName,
		IsPartition:   true,
		ParentTableID: parentTableID,
		Timestamp:     ts,
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
		// only update if the new timestamp is newer than the current one
		if locationInfo.Timestamp >= existing[1].Timestamp {
			info.tableNameHistory[id] = [2]TableLocationInfo{existing[0], locationInfo}
		}
		// if timestamp is older, don't update (keep the newer entry)
	}
}

func (info *LogBackupTableHistoryManager) RecordDBIdToName(dbId int64, dbName string, ts uint64) {
	// only update if the new timestamp is newer than the existing one
	if existingTs, exists := info.dbTimestamps[dbId]; !exists || ts >= existingTs {
		info.dbIdToName[dbId] = dbName
		info.dbTimestamps[dbId] = ts
	}
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
func (info *LogBackupTableHistoryManager) OnDatabaseInfo(dbId int64, dbName string, ts uint64) {
	info.RecordDBIdToName(dbId, dbName, ts)
}

// OnTableInfo implements MetaInfoCollector.OnTableInfo
func (info *LogBackupTableHistoryManager) OnTableInfo(
	dbID, tableId int64, tableSimpleInfo *tableSimpleInfo, commitTs uint64) {
	info.AddTableHistory(tableId, tableSimpleInfo.Name, dbID, commitTs)

	// add history for all partitions if this is a partitioned table
	for _, partitionId := range tableSimpleInfo.PartitionIds {
		info.AddPartitionHistory(partitionId, tableSimpleInfo.Name, dbID, tableId, commitTs)
	}
}
