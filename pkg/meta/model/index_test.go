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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
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

	safePartial := newIndexForTest(2, c0, c1)
	safePartial.ConditionExprString = "`c_1` is not null"
	require.True(t, IsIndexPrefixCoveredForForeignKey(tbl, safePartial, ast.NewCIStr("c_0"), ast.NewCIStr("c_1")))

	safePartialOnFirstFKCol := newIndexForTest(3, c0, c1)
	safePartialOnFirstFKCol.ConditionExprString = "`c_0` is not null"
	require.True(t, IsIndexPrefixCoveredForForeignKey(tbl, safePartialOnFirstFKCol, ast.NewCIStr("c_0"), ast.NewCIStr("c_1")))

	unsafePartialOnNonFKCol := newIndexForTest(4, c0, c1)
	unsafePartialOnNonFKCol.ConditionExprString = "`c_2` is not null"
	require.False(t, IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialOnNonFKCol, ast.NewCIStr("c_0"), ast.NewCIStr("c_1")))

	unsafePartialIsNull := newIndexForTest(5, c0)
	unsafePartialIsNull.ConditionExprString = "`c_0` is null"
	require.False(t, IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialIsNull, ast.NewCIStr("c_0")))

	unsafePartialBinaryCondition := newIndexForTest(6, c0)
	unsafePartialBinaryCondition.ConditionExprString = "`c_0` > 0"
	require.False(t, IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialBinaryCondition, ast.NewCIStr("c_0")))

	badCondition := newIndexForTest(7, c0)
	badCondition.ConditionExprString = "`c_0` is"
	require.False(t, IsIndexPrefixCoveredForForeignKey(tbl, badCondition, ast.NewCIStr("c_0")))

	require.Same(t, safePartial, FindIndexByColumnsForForeignKey(tbl, []*IndexInfo{unsafePartialOnNonFKCol, safePartial}, ast.NewCIStr("c_0"), ast.NewCIStr("c_1")))
}

func TestGlobalIndexV1SupportedForNextGen(t *testing.T) {
	if kerneltype.IsNextGen() {
		require.True(t, GetGlobalIndexV1Supported())
	}
}
