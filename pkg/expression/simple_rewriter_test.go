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
		{
			name: "Interleaved names across tables",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t1"), ColName: ast.NewCIStr("a")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t1"), ColName: ast.NewCIStr("col")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t2"), ColName: ast.NewCIStr("a")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t2"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Table: ast.NewCIStr("t2"), Name: ast.NewCIStr("col")},
			expected: 3,
		},
		{
			name: "Interleaved names, unqualified is ambiguous",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t1"), ColName: ast.NewCIStr("col")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t1"), ColName: ast.NewCIStr("b")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("t2"), ColName: ast.NewCIStr("col")},
			},
			astCol: &ast.ColumnName{Name: ast.NewCIStr("col")},
			err:    errNonUniq.GenWithStackByArgs("col", "field list"),
		},
		{
			name: "Skip not explicitly usable",
			names: types.NameSlice{
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col"), NotExplicitUsable: true},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("b")},
				{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr("col")},
			},
			astCol:   &ast.ColumnName{Name: ast.NewCIStr("col")},
			expected: 2,
		},
		{
			name:     "Empty names",
			names:    types.NameSlice{},
			astCol:   &ast.ColumnName{Name: ast.NewCIStr("col")},
			expected: -1,
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

			// The FieldNameIndex lookup must agree with the full scan.
			nameIdx := NewFieldNameIndex(tt.names)
			idxIndexed, errIndexed := nameIdx.FindFieldName(tt.astCol)
			require.Equal(idxOriginal, idxIndexed)
			require.Equal(errOriginal != nil, errIndexed != nil)
			if errOriginal != nil {
				require.Equal(errOriginal.Error(), errIndexed.Error())
			}
		})
	}

	// A FieldNameIndex covers only the exact slice it was built from.
	names, _ := generateTestData(4)
	nameIdx := NewFieldNameIndex(names)
	require.True(t, nameIdx.Covers(names))
	require.False(t, nameIdx.Covers(names.Shallow()))
	require.False(t, nameIdx.Covers(names[:3]))
	require.False(t, nameIdx.Covers(nil))
	var nilIdx *FieldNameIndex
	require.False(t, nilIdx.Covers(names))
	require.False(t, NewFieldNameIndex(nil).Covers(nil))

	// Every name of a wide slice, with qualified and unqualified references, is
	// resolved the same way through the index as by the full scan.
	wide := make(types.NameSlice, 0, 600)
	for i := range 300 {
		for _, tbl := range []string{"t1", "t2"} {
			wide = append(wide, &types.FieldName{
				DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr(tbl), ColName: ast.NewCIStr(fmt.Sprintf("c%d", i%250)),
				Redundant: i%7 == 0 && tbl == "t2",
			})
		}
	}
	wideIdx := NewFieldNameIndex(wide)
	for _, name := range wide {
		for _, astCol := range []*ast.ColumnName{
			{Name: name.ColName},
			{Table: name.TblName, Name: name.ColName},
			{Schema: name.DBName, Table: name.TblName, Name: name.ColName},
		} {
			want, wantErr := findFieldNameOriginal(wide, astCol)
			got, gotErr := wideIdx.FindFieldName(astCol)
			require.Equal(t, want, got, astCol.String())
			require.Equal(t, wantErr != nil, gotErr != nil, astCol.String())
		}
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

	// Resolve every name of a wide NameSlice once, as SELECT * on a wide table
	// does: a full scan per name versus building a FieldNameIndex and probing it.
	for _, size := range []int{32, 1000, 4000} {
		names := make(types.NameSlice, size)
		astCols := make([]*ast.ColumnName, size)
		for i := range size {
			names[i] = &types.FieldName{DBName: ast.NewCIStr("db"), TblName: ast.NewCIStr("tbl"), ColName: ast.NewCIStr(fmt.Sprintf("c%d", i))}
			astCols[i] = &ast.ColumnName{Schema: names[i].DBName, Table: names[i].TblName, Name: names[i].ColName}
		}
		b.Run("ResolveAll-Scan-"+fmt.Sprint(size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, astCol := range astCols {
					FindFieldName(names, astCol)
				}
			}
		})
		b.Run("ResolveAll-Indexed-"+fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				nameIdx := NewFieldNameIndex(names)
				for _, astCol := range astCols {
					nameIdx.FindFieldName(astCol)
				}
			}
		})
	}
}
