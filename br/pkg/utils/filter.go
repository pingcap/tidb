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

// PiTRTableTracker tracks all the DB and table ids that need to restore in a PiTR
type PiTRTableTracker struct {
	DBIdToTable map[int64]map[int64]struct{}
}

func NewPiTRTableFilter() *PiTRTableTracker {
	return &PiTRTableTracker{
		DBIdToTable: make(map[int64]map[int64]struct{}),
	}
}

// AddTable adds a table ID to the filter for the given database ID
func (f *PiTRTableTracker) AddTable(dbID, tableID int64) {
	if f.DBIdToTable == nil {
		f.DBIdToTable = make(map[int64]map[int64]struct{})
	}

	if _, ok := f.DBIdToTable[dbID]; !ok {
		f.DBIdToTable[dbID] = make(map[int64]struct{})
	}

	f.DBIdToTable[dbID][tableID] = struct{}{}
}

// AddDB adds the database id
func (f *PiTRTableTracker) AddDB(dbID int64) {
	if f.DBIdToTable == nil {
		f.DBIdToTable = make(map[int64]map[int64]struct{})
	}

	if _, ok := f.DBIdToTable[dbID]; !ok {
		f.DBIdToTable[dbID] = make(map[int64]struct{})
	}
}

// Remove removes a table ID from the filter for the given database ID.
// Returns true if the table was found and removed, false otherwise.
func (f *PiTRTableTracker) Remove(dbID, tableID int64) bool {
	if tables, ok := f.DBIdToTable[dbID]; ok {
		if _, exists := tables[tableID]; exists {
			delete(tables, tableID)
			return true
		}
	}
	return false
}

// ContainsTable checks if the given database ID and table ID combination exists in the filter
func (f *PiTRTableTracker) ContainsTable(dbID, tableID int64) bool {
	if tables, ok := f.DBIdToTable[dbID]; ok {
		_, exists := tables[tableID]
		return exists
	}
	return false
}

// ContainsDB checks if the given database ID exists in the filter
func (f *PiTRTableTracker) ContainsDB(dbID int64) bool {
	_, ok := f.DBIdToTable[dbID]
	return ok
}

// String returns a string representation of the PiTRTableTracker for debugging
func (f *PiTRTableTracker) String() string {
	if f == nil || f.DBIdToTable == nil {
		return "PiTRTableTracker{nil}"
	}

	var result strings.Builder
	result.WriteString("PiTRTableTracker{\n")
	for dbID, tables := range f.DBIdToTable {
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

func MatchSchema(filter filter.Filter, schema string) bool {
	if name, ok := StripTempTableNamePrefixIfNeeded(schema); IsSysDB(name) && ok {
		schema = name
	}
	return filter.MatchSchema(schema)
}

func MatchTable(filter filter.Filter, schema, table string) bool {
	if name, ok := StripTempTableNamePrefixIfNeeded(schema); IsSysDB(name) && ok {
		schema = name
	}
	return filter.MatchTable(schema, table)
}
