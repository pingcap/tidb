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
	"time"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/duration"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func checkOffsets(t *testing.T, tbl *TableInfo, ids ...int) {
	require.Equal(t, len(ids), len(tbl.Columns))
	for i := 0; i < len(ids); i++ {
		expected := fmt.Sprintf("c_%d", ids[i])
		require.Equal(t, expected, tbl.Columns[i].Name.L)
		require.Equal(t, i, tbl.Columns[i].Offset)
	}
	for _, col := range tbl.Columns {
		for _, idx := range tbl.Indices {
			for _, idxCol := range idx.Columns {
				if col.Name.L != idxCol.Name.L {
					continue
				}
				// Columns with the same name should have a same offset.
				require.Equal(t, col.Offset, idxCol.Offset)
			}
		}
	}
}

func TestMoveColumnInfo(t *testing.T) {
	c0 := newColumnForTest(0, 0)
	c1 := newColumnForTest(1, 1)
	c2 := newColumnForTest(2, 2)
	c3 := newColumnForTest(3, 3)
	c4 := newColumnForTest(4, 4)

	i0 := newIndexForTest(0, c0, c1, c2, c3, c4)
	i1 := newIndexForTest(1, c4, c2)
	i2 := newIndexForTest(2, c0, c4)
	i3 := newIndexForTest(3, c1, c2, c3)
	i4 := newIndexForTest(4, c3, c2, c1)

	tbl := &TableInfo{
		ID:      1,
		Name:    model.NewCIStr("t"),
		Columns: []*ColumnInfo{c0, c1, c2, c3, c4},
		Indices: []*IndexInfo{i0, i1, i2, i3, i4},
	}

	// Original offsets: [0, 1, 2, 3, 4]
	tbl.MoveColumnInfo(4, 0)
	checkOffsets(t, tbl, 4, 0, 1, 2, 3)
	tbl.MoveColumnInfo(2, 3)
	checkOffsets(t, tbl, 4, 0, 2, 1, 3)
	tbl.MoveColumnInfo(3, 2)
	checkOffsets(t, tbl, 4, 0, 1, 2, 3)
	tbl.MoveColumnInfo(0, 4)
	checkOffsets(t, tbl, 0, 1, 2, 3, 4)
	tbl.MoveColumnInfo(2, 2)
	checkOffsets(t, tbl, 0, 1, 2, 3, 4)
	tbl.MoveColumnInfo(0, 0)
	checkOffsets(t, tbl, 0, 1, 2, 3, 4)
	tbl.MoveColumnInfo(1, 4)
	checkOffsets(t, tbl, 0, 2, 3, 4, 1)
	tbl.MoveColumnInfo(3, 0)
	checkOffsets(t, tbl, 4, 0, 2, 3, 1)
}

func TestModelBasic(t *testing.T) {
	column := &ColumnInfo{
		ID:           1,
		Name:         model.NewCIStr("c"),
		Offset:       0,
		DefaultValue: 0,
		FieldType:    *types.NewFieldType(0),
		Hidden:       true,
	}
	column.AddFlag(mysql.PriKeyFlag)

	index := &IndexInfo{
		Name:  model.NewCIStr("key"),
		Table: model.NewCIStr("t"),
		Columns: []*IndexColumn{
			{
				Name:   model.NewCIStr("c"),
				Offset: 0,
				Length: 10,
			}},
		Unique:  true,
		Primary: true,
	}

	fk := &FKInfo{
		RefCols: []model.CIStr{model.NewCIStr("a")},
		Cols:    []model.CIStr{model.NewCIStr("a")},
	}

	seq := &SequenceInfo{
		Increment: 1,
		MinValue:  1,
		MaxValue:  100,
	}

	table := &TableInfo{
		ID:          1,
		Name:        model.NewCIStr("t"),
		Charset:     "utf8",
		Collate:     "utf8_bin",
		Columns:     []*ColumnInfo{column},
		Indices:     []*IndexInfo{index},
		ForeignKeys: []*FKInfo{fk},
		PKIsHandle:  true,
	}

	table2 := &TableInfo{
		ID:       2,
		Name:     model.NewCIStr("s"),
		Sequence: seq,
	}

	dbInfo := &DBInfo{
		ID:      1,
		Name:    model.NewCIStr("test"),
		Charset: "utf8",
		Collate: "utf8_bin",
	}
	dbInfo.Deprecated.Tables = []*TableInfo{table}

	n := dbInfo.Clone()
	require.Equal(t, dbInfo, n)

	pkName := table.GetPkName()
	require.Equal(t, model.NewCIStr("c"), pkName)
	newColumn := table.GetPkColInfo()
	require.Equal(t, true, newColumn.Hidden)
	require.Equal(t, column, newColumn)
	inIdx := table.ColumnIsInIndex(column)
	require.Equal(t, true, inIdx)
	tp := model.IndexTypeBtree
	require.Equal(t, "BTREE", tp.String())
	tp = model.IndexTypeHash
	require.Equal(t, "HASH", tp.String())
	tp = 1e5
	require.Equal(t, "", tp.String())
	has := index.HasPrefixIndex()
	require.Equal(t, true, has)
	require.Equal(t, TSConvert2Time(table.UpdateTS), table.GetUpdateTime())
	require.True(t, table2.IsSequence())
	require.False(t, table2.IsBaseTable())

	// Corner cases
	column.ToggleFlag(mysql.PriKeyFlag)
	pkName = table.GetPkName()
	require.Equal(t, model.NewCIStr(""), pkName)
	newColumn = table.GetPkColInfo()
	require.Nil(t, newColumn)
	anCol := &ColumnInfo{
		Name: model.NewCIStr("d"),
	}
	exIdx := table.ColumnIsInIndex(anCol)
	require.Equal(t, false, exIdx)
	anIndex := &IndexInfo{
		Columns: []*IndexColumn{},
	}
	no := anIndex.HasPrefixIndex()
	require.Equal(t, false, no)

	extraPK := NewExtraHandleColInfo()
	require.Equal(t, mysql.NotNullFlag|mysql.PriKeyFlag, extraPK.GetFlag())
	require.Equal(t, charset.CharsetBin, extraPK.GetCharset())
	require.Equal(t, charset.CollationBin, extraPK.GetCollate())
}

func TestTTLInfoClone(t *testing.T) {
	ttlInfo := &TTLInfo{
		ColumnName:       model.NewCIStr("test"),
		IntervalExprStr:  "test_expr",
		IntervalTimeUnit: 5,
		Enable:           true,
	}

	clonedTTLInfo := ttlInfo.Clone()
	clonedTTLInfo.ColumnName = model.NewCIStr("test_2")
	clonedTTLInfo.IntervalExprStr = "test_expr_2"
	clonedTTLInfo.IntervalTimeUnit = 9
	clonedTTLInfo.Enable = false

	require.Equal(t, "test", ttlInfo.ColumnName.O)
	require.Equal(t, "test_expr", ttlInfo.IntervalExprStr)
	require.Equal(t, 5, ttlInfo.IntervalTimeUnit)
	require.Equal(t, true, ttlInfo.Enable)
}

func TestTTLJobInterval(t *testing.T) {
	ttlInfo := &TTLInfo{}

	interval, err := ttlInfo.GetJobInterval()
	require.NoError(t, err)
	require.Equal(t, time.Hour, interval)

	ttlInfo = &TTLInfo{JobInterval: "200h"}
	interval, err = ttlInfo.GetJobInterval()
	require.NoError(t, err)
	require.Equal(t, time.Hour*200, interval)
}

func TestClearReorgIntermediateInfo(t *testing.T) {
	ptInfo := &PartitionInfo{}
	ptInfo.DDLType = model.PartitionTypeHash
	ptInfo.DDLExpr = "Test DDL Expr"
	ptInfo.NewTableID = 1111

	ptInfo.ClearReorgIntermediateInfo()
	require.Equal(t, model.PartitionTypeNone, ptInfo.DDLType)
	require.Equal(t, "", ptInfo.DDLExpr)
	require.Equal(t, true, ptInfo.DDLColumns == nil)
	require.Equal(t, int64(0), ptInfo.NewTableID)
}

func TestTTLDefaultJobInterval(t *testing.T) {
	// test const `DefaultJobIntervalStr` and `DefaultJobInterval` are consistent.
	d, err := duration.ParseDuration(DefaultJobIntervalStr)
	require.NoError(t, err)
	require.Equal(t, DefaultJobInterval, d)
}
