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

package export

import (
	"database/sql"
	"testing"

	"github.com/pingcap/tidb/pkg/dumpformat/csvfile"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestFileName(t *testing.T) {
	require.Equal(t, "db.orders.00000120003.csv", fileName("db", "orders", 12, 3))
	// Lexicographic order equals (ordinal, file) order.
	require.Less(t, fileName("d", "t", 1, 9), fileName("d", "t", 2, 0))
	require.Less(t, fileName("d", "t", 1, 0), fileName("d", "t", 1, 1))
}

func TestFieldKinds(t *testing.T) {
	col := func(tp byte, binary bool) *model.ColumnInfo {
		ft := types.NewFieldType(tp)
		if binary {
			ft.AddFlag(mysql.BinaryFlag)
		}
		return &model.ColumnInfo{FieldType: *ft}
	}
	kinds := fieldKinds([]*model.ColumnInfo{
		col(mysql.TypeLonglong, false),
		col(mysql.TypeVarchar, false),
		col(mysql.TypeBlob, true),
		col(mysql.TypeBlob, false),
	})
	require.Equal(t, []csvfile.FieldKind{
		csvfile.KindNumber, csvfile.KindString, csvfile.KindBytes, csvfile.KindString,
	}, kinds)
}

func TestRowEncoder(t *testing.T) {
	vt := types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).SetCharset("utf8mb4").SetCollate("utf8mb4_bin").Build()
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLong),
		&vt,
	}
	colInfos := []*model.ColumnInfo{
		{Name: ast.NewCIStr("a"), FieldType: *tps[0]},
		{Name: ast.NewCIStr("b"), FieldType: *tps[1]},
	}
	c := chunk.NewChunkWithCapacity(tps, 2)
	c.AppendInt64(0, 123)
	c.AppendString(1, `a"b`) // raw value; csvfile does the escaping, the encoder does not
	c.AppendInt64(0, 456)
	c.AppendNull(1)

	enc := newRowEncoder("tbl", colInfos)

	got, err := enc.encode(c.GetRow(0))
	require.NoError(t, err)
	require.Equal(t, []sql.RawBytes{sql.RawBytes("123"), sql.RawBytes(`a"b`)}, got)

	got, err = enc.encode(c.GetRow(1))
	require.NoError(t, err)
	require.Equal(t, []sql.RawBytes{sql.RawBytes("456"), nil}, got) // NULL -> nil
}
