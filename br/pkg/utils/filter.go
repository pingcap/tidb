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
	DBIds         map[int64]struct{}
	TableIdToDBId map[int64]int64
}

func NewPiTRIdTracker() *PiTRIdTracker {
	return &PiTRIdTracker{
		DBIds:         make(map[int64]struct{}),
		TableIdToDBId: make(map[int64]int64),
	}
}

// TrackTableId adds a physical ID to the filter for the given database ID
func (t *PiTRIdTracker) TrackTableId(dbID, tableId int64) {
	log.Info("tracking table id", zap.Int64("dbID", dbID), zap.Int64("tableID", tableId))

	if t.DBIds == nil {
		t.DBIds = make(map[int64]struct{})
	}
	if t.TableIdToDBId == nil {
		t.TableIdToDBId = make(map[int64]int64)
	}

	t.DBIds[dbID] = struct{}{}
	t.TableIdToDBId[tableId] = dbID
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
	if t.TableIdToDBId == nil {
		return
	}
	delete(t.TableIdToDBId, tableID)
}

// ContainsTableId checks if the given database ID and table ID combination exists in the filter
func (t *PiTRIdTracker) ContainsTableId(dbID, tableID int64) bool {
	if t.TableIdToDBId == nil {
		return false
	}

	storedDBID, exists := t.TableIdToDBId[tableID]
	return exists && storedDBID == dbID
}

// ContainsDB checks if the given database ID exists in the filter
func (t *PiTRIdTracker) ContainsDB(dbID int64) bool {
	if t.DBIds == nil {
		return false
	}
	_, ok := t.DBIds[dbID]
	return ok
}

// String returns a string representation of the PiTRIdTracker for debugging
func (t *PiTRIdTracker) String() string {
	if t == nil || t.DBIds == nil || t.TableIdToDBId == nil {
		return "PiTRIdTracker{nil}"
	}

	var result strings.Builder
	result.WriteString("PiTRIdTracker{\n")

	// Print database IDs
	result.WriteString("  DBIds: {")
	dbIDs := make([]int64, 0, len(t.DBIds))
	for dbID := range t.DBIds {
		dbIDs = append(dbIDs, dbID)
	}
	// Sort for consistent output
	sort.Slice(dbIDs, func(i, j int) bool { return dbIDs[i] < dbIDs[j] })
	for i, dbID := range dbIDs {
		if i > 0 {
			result.WriteString(", ")
		}
		result.WriteString(fmt.Sprintf("%d", dbID))
	}
	result.WriteString("}\n")

	// Print table ID to DB ID mappings
	result.WriteString("  TableIdToDBId: {\n")
	tableIDs := make([]int64, 0, len(t.TableIdToDBId))
	for tableID := range t.TableIdToDBId {
		tableIDs = append(tableIDs, tableID)
	}
	// Sort for consistent output
	sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })
	for _, tableID := range tableIDs {
		dbID := t.TableIdToDBId[tableID]
		result.WriteString(fmt.Sprintf("    Table[%d] -> DB[%d]\n", tableID, dbID))
	}
	result.WriteString("  }\n")
	result.WriteString("}")
	return result.String()
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
