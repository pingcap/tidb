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

package expression

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// Original function for comparison
func findFieldNameOriginal(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if !name.NotExplicitUsable && (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx != -1 {
				if names[idx].Redundant || name.Redundant {
					if !name.Redundant {
						idx = i
					}
					continue
				}
				return -1, errNonUniq.GenWithStackByArgs(astCol.String(), "field list")
			}
			idx = i
		}
	}
	return idx, nil
}

func generateTestData(size int) (types.NameSlice, *ast.ColumnName) {
	names := make(types.NameSlice, size)
	for i := range size {
		names[i] = &types.FieldName{
			DBName:  ast.NewCIStr("db"),
			TblName: ast.NewCIStr("tbl"),
			ColName: ast.NewCIStr("col" + string(rune('A'+i))),
		}
	}
	astCol := &ast.ColumnName{
		Schema: ast.NewCIStr("db"),
		Table:  ast.NewCIStr("tbl"),
		Name:   ast.NewCIStr("colZ"),
	}
	return names, astCol
}

func TestFindFieldName(t *testing.T) {
	tests := []struct {
		name     string
		names    types.NameSlice
		astCol   *ast.ColumnName
		expected int
		err      error
	}{
		{
			name: "Simple match",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			expected: 0,
		},
		{
			name: "Match with empty schema and table",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr(""), Table: ast.NewCIStr(""), Name: ast.NewCIStr("col")},
			expected: 0,
		},
		{
			name: "Match with empty schema, non-empty table",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr(""), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			expected: 0,
		},
		{
			name: "Match with non-empty schema, empty table",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr(""), Name: ast.NewCIStr("col")},
			expected: 0,
		},
		{
			name: "No match",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col1")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col2")},
			expected: -1,
		},
		{
			name: "Match with redundant field",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			expected: 2,
		},
		{
			name: "Non-unique match",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol: &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			err:    errNonUniq.GenWithStackByArgs("db.tbl.col", "field list"),
		},
		{
			name: "Match with empty schema and table and redundant",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr(""), Table: ast.NewCIStr(""), Name: ast.NewCIStr("col")},
			expected: 1,
		},
		{
			name: "Non-unique match with a redundant",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol: &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			err:    errNonUniq.GenWithStackByArgs("db.tbl.col", "field list"),
		},
		{
			name: "Match with multiple redundant",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), Redundant: true},
			},
			astCol:   &ast.ColumnName{Schema: ast.NewCIStr("db"), Table: ast.NewCIStr("tbl"), Name: ast.NewCIStr("col")},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			// Test findFieldNameOriginal
			idxOriginal, errOriginal := findFieldNameOriginal(tt.names, tt.astCol)
			if tt.err != nil {
				require.Error(errOriginal)
				require.Equal(tt.err.Error(), errOriginal.Error())
			} else {
				require.NoError(errOriginal)
				require.Equal(tt.expected, idxOriginal)
			}

			// Test FindFieldName
			idxOptimized, errOptimized := FindFieldName(tt.names, tt.astCol)
			if tt.err != nil {
				require.Error(errOptimized)
				require.Equal(tt.err.Error(), errOptimized.Error())
			} else {
				require.NoError(errOptimized)
				require.Equal(tt.expected, idxOptimized)
			}

			// Compare results of both functions
			require.Equal(idxOriginal, idxOptimized)
			require.Equal(errOriginal != nil, errOptimized != nil)
			if errOriginal != nil && errOptimized != nil {
				require.Equal(errOriginal.Error(), errOptimized.Error())
			}
		})
	}
}

// go test -bench=^BenchmarkFindFieldName$ -run=^$ -tags intest github.com/pingcap/tidb/pkg/expression
func BenchmarkFindFieldName(b *testing.B) {
	sizes := []int{10, 100, 200, 1000, 10000}

	for _, size := range sizes {
		names, astCol := generateTestData(size)

		b.Run("Original-"+fmt.Sprint(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				findFieldNameOriginal(names, astCol)
			}
		})

		b.Run("Optimized-"+fmt.Sprint(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				FindFieldName(names, astCol)
			}
		})
	}
}
