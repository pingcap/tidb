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

// PiTRIdTracker tracks all the DB and tables ids that need to restore in a PiTR
type PiTRIdTracker struct {
	DBIdToTableId map[int64]map[int64]struct{}
}

func NewPiTRIdTracker() *PiTRIdTracker {
	return &PiTRIdTracker{
		DBIdToTableId: make(map[int64]map[int64]struct{}),
	}
}

// TrackTableId adds a physical ID to the filter for the given database ID
func (t *PiTRIdTracker) TrackTableId(dbID, physicalId int64) {
	if t.DBIdToTableId == nil {
		t.DBIdToTableId = make(map[int64]map[int64]struct{})
	}

	if _, ok := t.DBIdToTableId[dbID]; !ok {
		t.DBIdToTableId[dbID] = make(map[int64]struct{})
	}

	t.DBIdToTableId[dbID][physicalId] = struct{}{}
}

// AddDB adds the database id
func (t *PiTRIdTracker) AddDB(dbID int64) {
	if t.DBIdToTableId == nil {
		t.DBIdToTableId = make(map[int64]map[int64]struct{})
	}

	if _, ok := t.DBIdToTableId[dbID]; !ok {
		t.DBIdToTableId[dbID] = make(map[int64]struct{})
	}
}

// ContainsTableId checks if the given database ID and table ID combination exists in the filter
func (t *PiTRIdTracker) ContainsTableId(dbID, tableID int64) bool {
	if tables, ok := t.DBIdToTableId[dbID]; ok {
		_, exists := tables[tableID]
		return exists
	}
	return false
}

// ContainsDB checks if the given database ID exists in the filter
func (t *PiTRIdTracker) ContainsDB(dbID int64) bool {
	_, ok := t.DBIdToTableId[dbID]
	return ok
}

// String returns a string representation of the PiTRIdTracker for debugging
func (t *PiTRIdTracker) String() string {
	if t == nil || t.DBIdToTableId == nil {
		return "PiTRIdTracker{nil}"
	}

	var result strings.Builder
	result.WriteString("PiTRIdTracker{\n")
	result.WriteString("  DBToTables: {\n")
	for dbID, tables := range t.DBIdToTableId {
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
