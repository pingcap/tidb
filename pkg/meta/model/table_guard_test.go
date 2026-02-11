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

package model

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func exportedFieldNamesSorted(typ reflect.Type) []string {
	names := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath == "" {
			names = append(names, f.Name)
		}
	}
	sort.Strings(names)
	return names
}

// TestTiCDCSharedColumnSchemaContractGuard is a contract guard for TiCDC's shared column schema.
// TiCDC relies on the exported field set of these meta/model structs for schema hashing, equality,
// and decoding. Any change to the exported field set must fail this test, forcing a CDC compatibility
// review and (if needed) a corresponding update in TiCDC before updating the allowed list here.
func TestTiCDCSharedColumnSchemaContractGuard(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		typ           reflect.Type
		allowedFields []string
	}{
		{
			name: "ColumnInfo",
			typ:  reflect.TypeOf(ColumnInfo{}),
			allowedFields: []string{
				"ChangeStateInfo",
				"ChangingFieldType",
				"Comment",
				"DefaultIsExpr",
				"DefaultValue",
				"DefaultValueBit",
				"Dependences",
				"FieldType",
				"GeneratedExprString",
				"GeneratedStored",
				"Hidden",
				"ID",
				"Name",
				"Offset",
				"OriginDefaultValue",
				"OriginDefaultValueBit",
				"State",
				"Version",
			},
		},
		{
			name: "IndexInfo",
			typ:  reflect.TypeOf(IndexInfo{}),
			allowedFields: []string{
				"AffectColumn",
				"BackfillState",
				"Columns",
				"Comment",
				"ConditionExprString",
				"FullTextInfo",
				"Global",
				"ID",
				"InvertedInfo",
				"Invisible",
				"MVIndex",
				"Name",
				"Primary",
				"State",
				"Table",
				"Tp",
				"Unique",
				"VectorInfo",
			},
		},
		{
			name: "IndexColumn",
			typ:  reflect.TypeOf(IndexColumn{}),
			allowedFields: []string{
				"Length",
				"Name",
				"Offset",
				"UseChangingType",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := exportedFieldNamesSorted(tc.typ)
			require.Equal(t, tc.allowedFields, got)
		})
	}
}
