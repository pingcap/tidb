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

package types

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// To reduce memory overhead for `tableInfo` in scenarios with a large number of tables,
// CDC optimizes the sharing of `tableInfo` for tables with the same table structure.
// CDC compares the column and index information in `tableInfo` to confirm whether they belong to the same `tableInfo`.
// Therefore, to prevent upstream TiDB from modifying relevant fields in `tableInfo` without CDC's corresponding modification,
// leading to incorrect sharing of tables with different structures and data inconsistencies,
// we added this test to TiDB to ensure that CDC can adjust accordingly when TiDB modifies relevant `tableInfo` fields.
// If this test fails due to changes in `tableInfo` fields, please contact the CDC team for confirmation.
//
// TestFieldTypeJSONContractGuard ensures FieldType and jsonFieldType remain in sync.
// FieldType is serialized via jsonFieldType, so any FieldType field addition/rename/type-change
// must update jsonFieldType accordingly to avoid JSON forward/backward compatibility risks.
func TestFieldTypeJSONContractGuard(t *testing.T) {
	t.Parallel()

	fieldType := reflect.TypeOf(FieldType{})
	jsonType := reflect.TypeOf(jsonFieldType{})

	require.Equal(t, fieldType.NumField(), jsonType.NumField(), "FieldType and jsonFieldType must have the same number of fields")

	jsonFields := make(map[string]reflect.StructField, jsonType.NumField())
	for i := 0; i < jsonType.NumField(); i++ {
		f := jsonType.Field(i)
		k := strings.ToLower(f.Name)
		_, exists := jsonFields[k]
		require.Falsef(t, exists, "duplicate field name in jsonFieldType (case-insensitive): %s", f.Name)
		jsonFields[k] = f
	}

	for i := 0; i < fieldType.NumField(); i++ {
		f := fieldType.Field(i)
		k := strings.ToLower(f.Name)
		jf, ok := jsonFields[k]
		require.Truef(t, ok, "missing field in jsonFieldType (case-insensitive): %s", f.Name)
		require.Truef(t, f.Type == jf.Type, "field type mismatch for %s: FieldType=%v jsonFieldType=%v", f.Name, f.Type, jf.Type)
	}
}
