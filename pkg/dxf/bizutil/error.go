// Copyright 2026 PingCAP, Inc.
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

// Package bizutil contains utilities shared by DXF business logic.
package bizutil

import "strings"

// IsDataError checks whether an error is caused by invalid or conflicting input data.
func IsDataError(taskErr error) bool {
	if taskErr == nil {
		return false
	}
	errMsg := taskErr.Error()
	// Keep these checks string-based to avoid depending on Lightning error definitions.
	// We can replace this when those error definitions are split out of the Lightning
	// package. DXF error serialization keeps only the outer error code, so nested data
	// errors must be identified by their canonical messages.
	//
	// import-into examples:
	// [Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk:
	// encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0
	// at offset 0: Value conversion failed for column 'event_id'. Expected type:
	// bigint, received value: ?. Reason: [types:1292]Truncated incorrect DOUBLE value: '?'.
	//
	// add-index examples:
	// [kv:1062]Duplicate entry '1' for key 't.idx'
	isImportDataErr := strings.Contains(errMsg, "ErrEncodeKV") &&
		(strings.Contains(errMsg, "Value conversion failed for column") ||
			(strings.Contains(errMsg, "Check constraint '") && strings.Contains(errMsg, "' is violated")) ||
			strings.Contains(errMsg, "Table has no partition for value"))
	isImportConflictErr := (strings.Contains(errMsg, "[executor:8167]") && strings.Contains(errMsg, "Duplicate key conflict found")) ||
		(strings.Contains(errMsg, "ErrFoundDataConflictRecords") && strings.Contains(errMsg, "found data conflict records")) ||
		(strings.Contains(errMsg, "ErrFoundIndexConflictRecords") && strings.Contains(errMsg, "found index conflict records"))
	isUKDupEntryErr := strings.Contains(errMsg, "[kv:1062]") && strings.Contains(errMsg, "Duplicate entry")
	return isImportDataErr || isImportConflictErr || isUKDupEntryErr
}
