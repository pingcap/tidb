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

package utils

import (
	"github.com/pingcap/log"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

// PiTRIdTracker tracks all the DB and tables ids that need to restore in a PiTR
type PiTRIdTracker struct {
	DBIds map[int64]struct{}
	// TableIdToDBIds maps a table ID to a set of database IDs it belongs to
	// This handles the case where a table can be renamed across databases
	TableIdToDBIds map[int64]map[int64]struct{}
	PartitionIds   map[int64]struct{}
	// DBNameToTableNames maps a database name to a set of table names it contains
	DBNameToTableNames map[string]map[string]struct{}
}

func NewPiTRIdTracker() *PiTRIdTracker {
	return &PiTRIdTracker{
		DBIds:              make(map[int64]struct{}),
		TableIdToDBIds:     make(map[int64]map[int64]struct{}),
		PartitionIds:       make(map[int64]struct{}),
		DBNameToTableNames: make(map[string]map[string]struct{}),
	}
}

// TrackTableId adds a physical ID to the filter for the given database ID
func (t *PiTRIdTracker) TrackTableId(dbID, tableId int64) {
	log.Info("tracking table id", zap.Int64("dbID", dbID), zap.Int64("tableID", tableId))

	if t.DBIds == nil {
		t.DBIds = make(map[int64]struct{})
	}
	if t.TableIdToDBIds == nil {
		t.TableIdToDBIds = make(map[int64]map[int64]struct{})
	}

	t.DBIds[dbID] = struct{}{}

	// Initialize the map for this table ID if it doesn't exist
	if _, exists := t.TableIdToDBIds[tableId]; !exists {
		t.TableIdToDBIds[tableId] = make(map[int64]struct{})
	}

	// Add this database ID to the set of databases for this table
	t.TableIdToDBIds[tableId][dbID] = struct{}{}
}

func (t *PiTRIdTracker) TrackPartitionId(partitionID int64) {
	if t.PartitionIds == nil {
		t.PartitionIds = make(map[int64]struct{})
	}
	t.PartitionIds[partitionID] = struct{}{}
}

// AddDB adds the database id
func (t *PiTRIdTracker) AddDB(dbID int64) {
	log.Info("tracking db id", zap.Int64("dbID", dbID))
	if t.DBIds == nil {
		t.DBIds = make(map[int64]struct{})
	}

	t.DBIds[dbID] = struct{}{}
}

// ContainsDBAndTableId checks if the given database ID and table ID combination is tracked
func (t *PiTRIdTracker) ContainsDBAndTableId(dbID, tableID int64) bool {
	if t.TableIdToDBIds == nil {
		return false
	}

	dbIDs, exists := t.TableIdToDBIds[tableID]
	if !exists {
		return false
	}

	// Check if this specific database ID is in the set of databases for this table
	_, hasDB := dbIDs[dbID]
	return hasDB
}

// ContainsTableId checks if the given table ID is tracked
func (t *PiTRIdTracker) ContainsTableId(tableID int64) bool {
	if t.TableIdToDBIds == nil {
		return false
	}

	_, exists := t.TableIdToDBIds[tableID]
	return exists
}

// ContainsPartitionId checks if the given partition ID is tracked
func (t *PiTRIdTracker) ContainsPartitionId(partitionID int64) bool {
	if t.PartitionIds == nil {
		return false
	}

	_, exists := t.PartitionIds[partitionID]
	return exists
}

// ContainsDB checks if the given database ID exists in the filter
func (t *PiTRIdTracker) ContainsDB(dbID int64) bool {
	if t.DBIds == nil {
		return false
	}
	_, ok := t.DBIds[dbID]
	return ok
}

func MatchSchema(filter filter.Filter, schema string, withSys bool) bool {
	schema = StripTempDBPrefixIfNeeded(schema)
	if IsSysDB(schema) && !withSys {
		// early return if system tables are disabled
		return false
	}
	return filter.MatchSchema(schema)
}

func MatchTable(filter filter.Filter, schema, table string, withSys bool) bool {
	schema = StripTempDBPrefixIfNeeded(schema)
	if IsSysDB(schema) && !withSys {
		// early return if system tables are disabled
		return false
	}
	return filter.MatchTable(schema, table)
}

// TrackTableName adds a table name for the given database name
func (t *PiTRIdTracker) TrackTableName(dbName, tableName string) {
	log.Info("tracking table name", zap.String("dbName", dbName), zap.String("tableName", tableName))

	if t.DBNameToTableNames == nil {
		t.DBNameToTableNames = make(map[string]map[string]struct{})
	}

	if _, ok := t.DBNameToTableNames[dbName]; !ok {
		t.DBNameToTableNames[dbName] = make(map[string]struct{})
	}

	t.DBNameToTableNames[dbName][tableName] = struct{}{}
}
