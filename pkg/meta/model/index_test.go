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

package model

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func newColumnForTest(id int64, offset int) *ColumnInfo {
	return &ColumnInfo{
		ID:     id,
		Name:   ast.NewCIStr(fmt.Sprintf("c_%d", id)),
		Offset: offset,
	}
}

func newIndexForTest(id int64, cols ...*ColumnInfo) *IndexInfo {
	idxCols := make([]*IndexColumn, 0, len(cols))
	for _, c := range cols {
		idxCols = append(idxCols, &IndexColumn{Offset: c.Offset, Name: c.Name})
	}
	return &IndexInfo{
		ID:      id,
		Name:    ast.NewCIStr(fmt.Sprintf("i_%d", id)),
		Columns: idxCols,
	}
}

func TestIsIndexPrefixCovered(t *testing.T) {
	c0 := newColumnForTest(0, 0)
	c1 := newColumnForTest(1, 1)
	c2 := newColumnForTest(2, 2)
	c3 := newColumnForTest(3, 3)
	c4 := newColumnForTest(4, 4)

	i0 := newIndexForTest(0, c0, c1, c2)
	i1 := newIndexForTest(1, c4, c2)

	tbl := &TableInfo{
		ID:      1,
		Name:    ast.NewCIStr("t"),
		Columns: []*ColumnInfo{c0, c1, c2, c3, c4},
		Indices: []*IndexInfo{i0, i1},
	}
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_0")))
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_0"), ast.NewCIStr("c_1"), ast.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_1")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_1"), ast.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_0"), ast.NewCIStr("c_2")))

	require.Equal(t, true, IsIndexPrefixCovered(tbl, i1, ast.NewCIStr("c_4")))
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i1, ast.NewCIStr("c_4"), ast.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, ast.NewCIStr("c_2")))
}

func TestGlobalIndexV1SupportedForNextGen(t *testing.T) {
	if kerneltype.IsNextGen() {
		require.True(t, GetGlobalIndexV1Supported())
	}
}

func TestIndexColumnDescRoundTrip(t *testing.T) {
	// Ascending is the zero value and must be omitted from JSON for backward
	// compatibility with schemas written before descending indexes were supported.
	ascCol := IndexColumn{Name: ast.NewCIStr("a"), Offset: 0, Length: -1}
	ascJSON, err := json.Marshal(ascCol)
	require.NoError(t, err)
	require.NotContains(t, string(ascJSON), "desc")

	descCol := IndexColumn{Name: ast.NewCIStr("b"), Offset: 1, Length: -1, Desc: true}
	descJSON, err := json.Marshal(descCol)
	require.NoError(t, err)
	require.Contains(t, string(descJSON), `"desc":true`)

	// Unknown fields from a newer TiDB must decode cleanly on an older binary.
	var decoded IndexColumn
	require.NoError(t, json.Unmarshal(descJSON, &decoded))
	require.True(t, decoded.Desc)

	// Clone preserves Desc.
	require.True(t, descCol.Clone().Desc)
}

func TestIndexInfoHasDescColumn(t *testing.T) {
	c0 := newColumnForTest(0, 0)
	c1 := newColumnForTest(1, 1)

	idx := newIndexForTest(10, c0, c1)
	require.False(t, idx.HasDescColumn())

	idx.Columns[1].Desc = true
	require.True(t, idx.HasDescColumn())
}

func TestIndexInfoIsServable(t *testing.T) {
	idx := newIndexForTest(1, newColumnForTest(0, 0))

	// Version 0 (legacy) and 1 (introduces Desc) are both serviceable today.
	require.True(t, idx.IsServable())
	idx.Version = 1
	require.True(t, idx.IsServable())

	// A newer-than-known version must be rejected so old binaries refuse to
	// serve queries against indexes whose semantics they do not understand.
	idx.Version = 255
	require.False(t, idx.IsServable())
}

func TestIndexInfoUnservableErr(t *testing.T) {
	idx := newIndexForTest(7, newColumnForTest(0, 0))
	idx.Version = 99

	err := idx.UnservableErr("orders")
	require.Error(t, err)
	// The message must name the index, the table, and the version mismatch
	// so an operator can decide whether to upgrade or DROP INDEX.
	msg := err.Error()
	require.Contains(t, msg, idx.Name.O)
	require.Contains(t, msg, "orders")
	require.Contains(t, msg, "99")
	require.Contains(t, msg, "DROP INDEX")

	// Empty table name is allowed (callers without context just leave it off).
	require.NotPanics(t, func() { _ = idx.UnservableErr("") })
}
