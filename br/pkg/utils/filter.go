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

	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// PiTRIdTracker tracks all the DB and physical ids that need to restore in a PiTR
type PiTRIdTracker struct {
	DBIdToPhysicalId map[int64]map[int64]struct{}
	TableToCleanup   map[int64]map[int64]struct{}
}

func NewPiTRTableTracker() *PiTRIdTracker {
	return &PiTRIdTracker{
		DBIdToPhysicalId: make(map[int64]map[int64]struct{}),
		TableToCleanup:   make(map[int64]map[int64]struct{}),
	}
}

// AddPhysicalId adds a physical ID to the filter for the given database ID
func (t *PiTRIdTracker) AddPhysicalId(dbID, physicalId int64) {
	if t.DBIdToPhysicalId == nil {
		t.DBIdToPhysicalId = make(map[int64]map[int64]struct{})
	}

	if _, ok := t.DBIdToPhysicalId[dbID]; !ok {
		t.DBIdToPhysicalId[dbID] = make(map[int64]struct{})
	}

	t.DBIdToPhysicalId[dbID][physicalId] = struct{}{}
}

// AddTableToCleanup tracks all the table id need to be cleaned up after restore
func (t *PiTRIdTracker) AddTableToCleanup(dbID, tableId int64) {
	if t.TableToCleanup == nil {
		t.TableToCleanup = make(map[int64]map[int64]struct{})
	}

	if _, ok := t.TableToCleanup[dbID]; !ok {
		t.TableToCleanup[dbID] = make(map[int64]struct{})
	}

	t.TableToCleanup[dbID][tableId] = struct{}{}
}

// AddDB adds the database id
func (t *PiTRIdTracker) AddDB(dbID int64) {
	if t.DBIdToPhysicalId == nil {
		t.DBIdToPhysicalId = make(map[int64]map[int64]struct{})
	}

	if _, ok := t.DBIdToPhysicalId[dbID]; !ok {
		t.DBIdToPhysicalId[dbID] = make(map[int64]struct{})
	}
}

// Remove removes a table ID from the filter for the given database ID.
// Returns true if the table was found and removed, false otherwise.
func (t *PiTRIdTracker) Remove(dbID, physicalId int64) bool {
	if tables, ok := t.DBIdToPhysicalId[dbID]; ok {
		if _, exists := tables[physicalId]; exists {
			delete(tables, physicalId)
			return true
		}
	}
	return false
}

// ContainsPhysicalId checks if the given database ID and table ID combination exists in the filter
func (t *PiTRIdTracker) ContainsPhysicalId(dbID, tableID int64) bool {
	if tables, ok := t.DBIdToPhysicalId[dbID]; ok {
		_, exists := tables[tableID]
		return exists
	}
	return false
}

// ContainsDB checks if the given database ID exists in the filter
func (t *PiTRIdTracker) ContainsDB(dbID int64) bool {
	_, ok := t.DBIdToPhysicalId[dbID]
	return ok
}

// String returns a string representation of the PiTRIdTracker for debugging
func (t *PiTRIdTracker) String() string {
	if t == nil || t.DBIdToPhysicalId == nil {
		return "PiTRIdTracker{nil}"
	}

	var result strings.Builder
	result.WriteString("PiTRIdTracker{\n")
	for dbID, tables := range t.DBIdToPhysicalId {
		result.WriteString(fmt.Sprintf("  DB[%d]: {", dbID))
		tableIDs := make([]int64, 0, len(tables))
		for tableID := range tables {
			tableIDs = append(tableIDs, tableID)
		}
		// Sort for consistent output
		sort.Slice(tableIDs, func(i, j int) bool { return tableIDs[i] < tableIDs[j] })
		for i, tableID := range tableIDs {
			if i > 0 {
				result.WriteString(", ")
			}
			result.WriteString(fmt.Sprintf("%d", tableID))
		}
		result.WriteString("}\n")
	}
	result.WriteString("}")
	return result.String()
}

func MatchSchema(filter filter.Filter, schema string, withSys bool) bool {
	if name, ok := StripTempTableNamePrefixIfNeeded(schema); IsSysDB(name) && ok {
		// early return if system tables are disabled
		if !withSys {
			return false
		}
		schema = name
	}
	return filter.MatchSchema(schema)
}

func MatchTable(filter filter.Filter, schema, table string, withSys bool) bool {
	if name, ok := StripTempTableNamePrefixIfNeeded(schema); IsSysDB(name) && ok {
		// early return if system tables are disabled
		if !withSys {
			return false
		}
		schema = name
	}
	return filter.MatchTable(schema, table)
}
