// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestColumnToProto(t *testing.T) {
	// Make sure the flag is set in tipb.ColumnInfo
	collate.SetNewCollationEnabledForTest(false)
	tp := types.NewFieldType(mysql.TypeLong)
	tp.SetFlag(10)
	tp.SetCollate("utf8_bin")
	col := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc := util.ColumnToProto(col, false, false)
	expect := &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: 83, ColumnLen: 11, Decimal: 0, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false}
	require.Equal(t, expect, pc)

	cols := []*model.ColumnInfo{col, col}
	pcs := util.ColumnsToProto(cols, false, false, false)
	for _, v := range pcs {
		require.Equal(t, int32(10), v.GetFlag())
	}
	pcs = util.ColumnsToProto(cols, true, false, false)
	for _, v := range pcs {
		require.Equal(t, int32(10), v.GetFlag())
	}

	// Make sure the collation ID is successfully set.
	tp = types.NewFieldType(mysql.TypeVarchar)
	tp.SetFlag(10)
	tp.SetCollate("latin1_swedish_ci")
	col1 := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = util.ColumnToProto(col1, false, false)
	require.Equal(t, int32(8), pc.Collation)

	collate.SetNewCollationEnabledForTest(true)

	pc = util.ColumnToProto(col, false, false)
	expect = &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: -83, ColumnLen: 11, Decimal: 0, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false}
	require.Equal(t, expect, pc)
	pcs = util.ColumnsToProto(cols, true, false, false)
	for _, v := range pcs {
		require.Equal(t, int32(-83), v.Collation)
	}
	pc = util.ColumnToProto(col1, false, false)
	require.Equal(t, int32(-8), pc.Collation)

	tp = types.NewFieldType(mysql.TypeEnum)
	tp.SetFlag(10)
	tp.SetElems([]string{"a", "b"})
	col2 := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = util.ColumnToProto(col2, false, false)
	require.Len(t, pc.Elems, 2)

	tp = types.NewFieldTypeBuilder().
		SetType(mysql.TypeString).
		SetCharset("utf8mb4").
		SetCollate("utf8mb4_bin").
		SetFlen(100).
		SetFlag(10).
		SetArray(true).
		BuildP()
	col3 := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = util.ColumnToProto(col3, true, false)
	expect = &tipb.ColumnInfo{ColumnId: 0, Tp: 0xfe, Collation: 63, ColumnLen: 100, Decimal: 0, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false}
	require.Equal(t, expect, pc)
}

// TestGeneratedColumnFlagForTiFlash verifies that the GeneratedColumnFlag is set
// on virtual generated columns when building protobuf for TiFlash.
// This is a regression test for https://github.com/pingcap/tidb/issues/59831.
func TestGeneratedColumnFlagForTiFlash(t *testing.T) {
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(false)

	tp := types.NewFieldType(mysql.TypeVarchar)
	tp.SetCollate("utf8_bin")

	// A virtual generated column.
	virtualGenCol := &model.ColumnInfo{
		ID:                  1,
		FieldType:           *tp,
		GeneratedExprString: "lower(c1)",
		GeneratedStored:     false,
	}
	require.True(t, virtualGenCol.IsVirtualGenerated())

	// A normal column.
	normalCol := &model.ColumnInfo{
		ID:        2,
		FieldType: *tp,
	}

	// When isTiFlashStore=true, GeneratedColumnFlag should be set on virtual generated columns.
	pc := util.ColumnToProto(virtualGenCol, false, true)
	require.NotZero(t, pc.Flag&int32(mysql.GeneratedColumnFlag), "GeneratedColumnFlag should be set for virtual generated column when isTiFlashStore=true")

	// When isTiFlashStore=false, GeneratedColumnFlag should NOT be set.
	pc = util.ColumnToProto(virtualGenCol, false, false)
	require.Zero(t, pc.Flag&int32(mysql.GeneratedColumnFlag), "GeneratedColumnFlag should not be set when isTiFlashStore=false")

	// Normal column should never have GeneratedColumnFlag.
	pc = util.ColumnToProto(normalCol, false, true)
	require.Zero(t, pc.Flag&int32(mysql.GeneratedColumnFlag), "GeneratedColumnFlag should not be set for normal column")

	// Test via ColumnsToProto with isTiFlashStore=true.
	cols := []*model.ColumnInfo{virtualGenCol, normalCol}
	pcs := util.ColumnsToProto(cols, false, false, true)
	require.NotZero(t, pcs[0].Flag&int32(mysql.GeneratedColumnFlag), "GeneratedColumnFlag should be set for virtual generated column in ColumnsToProto")
	require.Zero(t, pcs[1].Flag&int32(mysql.GeneratedColumnFlag), "GeneratedColumnFlag should not be set for normal column in ColumnsToProto")

	// Test BuildPartitionTableScanFromInfos sets GeneratedColumnFlag correctly.
	// This is the specific regression from https://github.com/pingcap/tidb/issues/59831:
	// BuildPartitionTableScanFromInfos is only called for TiFlash partition table scans,
	// but PR #55463 incorrectly passed isTiFlashStore=false.
	tableInfo := &model.TableInfo{
		ID:      1,
		Columns: cols,
	}
	ptsExec := tables.BuildPartitionTableScanFromInfos(tableInfo, cols, false)
	require.NotZero(t, ptsExec.Columns[0].Flag&int32(mysql.GeneratedColumnFlag),
		"BuildPartitionTableScanFromInfos should set GeneratedColumnFlag for virtual generated columns (TiFlash only)")
	require.Zero(t, ptsExec.Columns[1].Flag&int32(mysql.GeneratedColumnFlag),
		"BuildPartitionTableScanFromInfos should not set GeneratedColumnFlag for normal columns")
}
