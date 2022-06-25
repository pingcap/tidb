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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/collate"
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
	pc := util.ColumnToProto(col)
	expect := &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: 83, ColumnLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	require.Equal(t, expect, pc)

	cols := []*model.ColumnInfo{col, col}
	pcs := util.ColumnsToProto(cols, false)
	for _, v := range pcs {
		require.Equal(t, int32(10), v.GetFlag())
	}
	pcs = util.ColumnsToProto(cols, true)
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
	pc = util.ColumnToProto(col1)
	require.Equal(t, int32(8), pc.Collation)

	collate.SetNewCollationEnabledForTest(true)

	pc = util.ColumnToProto(col)
	expect = &tipb.ColumnInfo{ColumnId: 0, Tp: 3, Collation: -83, ColumnLen: -1, Decimal: -1, Flag: 10, Elems: []string(nil), DefaultVal: []uint8(nil), PkHandle: false, XXX_unrecognized: []uint8(nil)}
	require.Equal(t, expect, pc)
	pcs = util.ColumnsToProto(cols, true)
	for _, v := range pcs {
		require.Equal(t, int32(-83), v.Collation)
	}
	pc = util.ColumnToProto(col1)
	require.Equal(t, int32(-8), pc.Collation)

	tp = types.NewFieldType(mysql.TypeEnum)
	tp.SetFlag(10)
	tp.SetElems([]string{"a", "b"})
	col2 := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc = util.ColumnToProto(col2)
	require.Len(t, pc.Elems, 2)
}
