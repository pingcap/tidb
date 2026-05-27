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

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func newColumnForTest(id int64, offset int) *ColumnInfo {
	return &ColumnInfo{
		ID:     id,
		Name:   model.NewCIStr(fmt.Sprintf("c_%d", id)),
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
		Name:    model.NewCIStr(fmt.Sprintf("i_%d", id)),
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
		Name:    model.NewCIStr("t"),
		Columns: []*ColumnInfo{c0, c1, c2, c3, c4},
		Indices: []*IndexInfo{i0, i1},
	}
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_0")))
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_0"), model.NewCIStr("c_1"), model.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_1")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_1"), model.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_0"), model.NewCIStr("c_2")))

	require.Equal(t, true, IsIndexPrefixCovered(tbl, i1, model.NewCIStr("c_4")))
	require.Equal(t, true, IsIndexPrefixCovered(tbl, i1, model.NewCIStr("c_4"), model.NewCIStr("c_2")))
	require.Equal(t, false, IsIndexPrefixCovered(tbl, i0, model.NewCIStr("c_2")))
}
