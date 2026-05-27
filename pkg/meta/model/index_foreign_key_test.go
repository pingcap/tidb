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

package model_test

import (
	"fmt"
	"testing"

	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func newColumnForFKTest(id int64, offset int) *metamodel.ColumnInfo {
	return &metamodel.ColumnInfo{
		ID:     id,
		Name:   pmodel.NewCIStr(fmt.Sprintf("c_%d", id)),
		Offset: offset,
	}
}

func newIndexForFKTest(id int64, cols ...*metamodel.ColumnInfo) *metamodel.IndexInfo {
	idxCols := make([]*metamodel.IndexColumn, 0, len(cols))
	for _, c := range cols {
		idxCols = append(idxCols, &metamodel.IndexColumn{Offset: c.Offset, Name: c.Name})
	}
	return &metamodel.IndexInfo{
		ID:      id,
		Name:    pmodel.NewCIStr(fmt.Sprintf("i_%d", id)),
		Columns: idxCols,
	}
}

func TestIsIndexPrefixCoveredForForeignKey(t *testing.T) {
	c0 := newColumnForFKTest(0, 0)
	c1 := newColumnForFKTest(1, 1)
	c2 := newColumnForFKTest(2, 2)

	tbl := &metamodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("t"),
		Columns: []*metamodel.ColumnInfo{c0, c1, c2},
	}

	safePartial := newIndexForFKTest(2, c0, c1)
	safePartial.ConditionExprString = "`c_1` is not null"
	require.True(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, safePartial, pmodel.NewCIStr("c_0"), pmodel.NewCIStr("c_1")))

	safePartialOnFirstFKCol := newIndexForFKTest(3, c0, c1)
	safePartialOnFirstFKCol.ConditionExprString = "`c_0` is not null"
	require.True(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, safePartialOnFirstFKCol, pmodel.NewCIStr("c_0"), pmodel.NewCIStr("c_1")))

	unsafePartialOnNonFKCol := newIndexForFKTest(4, c0, c1)
	unsafePartialOnNonFKCol.ConditionExprString = "`c_2` is not null"
	require.False(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialOnNonFKCol, pmodel.NewCIStr("c_0"), pmodel.NewCIStr("c_1")))

	unsafePartialIsNull := newIndexForFKTest(5, c0)
	unsafePartialIsNull.ConditionExprString = "`c_0` is null"
	require.False(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialIsNull, pmodel.NewCIStr("c_0")))

	unsafePartialBinaryCondition := newIndexForFKTest(6, c0)
	unsafePartialBinaryCondition.ConditionExprString = "`c_0` > 0"
	require.False(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, unsafePartialBinaryCondition, pmodel.NewCIStr("c_0")))

	badCondition := newIndexForFKTest(7, c0)
	badCondition.ConditionExprString = "`c_0` is"
	require.False(t, metamodel.IsIndexPrefixCoveredForForeignKey(tbl, badCondition, pmodel.NewCIStr("c_0")))

	require.Same(t, safePartial, metamodel.FindIndexByColumnsForForeignKey(tbl, []*metamodel.IndexInfo{unsafePartialOnNonFKCol, safePartial}, pmodel.NewCIStr("c_0"), pmodel.NewCIStr("c_1")))
}
