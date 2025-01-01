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

// DBIDAndTableName stores db id and the table name to locate the table
type DBIDAndTableName struct {
	DbID      int64
	TableName string
}

type LogBackupTableHistoryManager struct {
	// maps table ID to its original and current names
	// [0] is original location, [1] is current location
	tableNameHistory map[int64][2]DBIDAndTableName
	// record all the db id to name that were seen during log backup DDL history
	dbIdToName       map[int64]string
	needToBuildIdMap bool
}

func NewTableHistoryManager() *LogBackupTableHistoryManager {
	return &LogBackupTableHistoryManager{
		tableNameHistory: make(map[int64][2]DBIDAndTableName),
		dbIdToName:       make(map[int64]string),
	}
}

func (info *LogBackupTableHistoryManager) AddTableHistory(tableId int64, tableName string, dbID int64) {
	tableLocationInfo := DBIDAndTableName{
		DbID:      dbID,
		TableName: tableName,
	}
	names, exists := info.tableNameHistory[tableId]
	if !exists {
		// first occurrence - store as original name
		info.tableNameHistory[tableId] = [2]DBIDAndTableName{tableLocationInfo, tableLocationInfo}
	} else {
		// update current name while preserving original name
		info.tableNameHistory[tableId] = [2]DBIDAndTableName{names[0], tableLocationInfo}
	}
}

func (info *LogBackupTableHistoryManager) RecordDBIdToName(dbId int64, dbName string) {
	info.dbIdToName[dbId] = dbName
}

// GetTableHistory returns information about all tables that have been renamed.
// Returns a map of table IDs to their original and current locations
func (info *LogBackupTableHistoryManager) GetTableHistory() map[int64][2]DBIDAndTableName {
	return info.tableNameHistory
}

func (info *LogBackupTableHistoryManager) GetDBNameByID(dbId int64) (string, bool) {
	name, ok := info.dbIdToName[dbId]
	return name, ok
}

func (info *LogBackupTableHistoryManager) GetNewlyCreatedDBHistory() map[int64]string {
	return info.dbIdToName
}
