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
	"fmt"
	"sort"
	"strings"

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
}

func NewPiTRIdTracker() *PiTRIdTracker {
	return &PiTRIdTracker{
		DBIds:          make(map[int64]struct{}),
		TableIdToDBIds: make(map[int64]map[int64]struct{}),
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

// AddDB adds the database id
func (t *PiTRIdTracker) AddDB(dbID int64) {
	log.Info("tracking db id", zap.Int64("dbID", dbID))
	if t.DBIds == nil {
		t.DBIds = make(map[int64]struct{})
	}

	t.DBIds[dbID] = struct{}{}
}

// RemoveTableId removes a table ID from the tracker
func (t *PiTRIdTracker) RemoveTableId(tableID int64) {
	log.Info("remove tracking table id", zap.Int64("tableID", tableID))
	if t.TableIdToDBIds == nil {
		return
	}
	delete(t.TableIdToDBIds, tableID)
}

// ContainsTableId checks if the given database ID and table ID combination exists in the filter
func (t *PiTRIdTracker) ContainsTableId(dbID, tableID int64) bool {
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

// ContainsDB checks if the given database ID exists in the filter
func (t *PiTRIdTracker) ContainsDB(dbID int64) bool {
	if t.DBIds == nil {
		return false
	}
	_, ok := t.DBIds[dbID]
	return ok
}

func MatchSchema(filter filter.Filter, schema string, withSys bool) bool {
	if name, ok := StripTempDBPrefixIfNeeded(schema); IsSysDB(name) && ok {
		// early return if system tables are disabled
		if !withSys {
			return false
		}
		schema = name
	}
	return filter.MatchSchema(schema)
}

func MatchTable(filter filter.Filter, schema, table string, withSys bool) bool {
	if name, ok := StripTempDBPrefixIfNeeded(schema); IsSysDB(name) && ok {
		// early return if system tables are disabled
		if !withSys {
			return false
		}
		schema = name
	}
	return filter.MatchTable(schema, table)
}
