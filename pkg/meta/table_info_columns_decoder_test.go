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
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestSimpleColumnsTableInfoDecoder(t *testing.T) {
	tableInfo := &model.TableInfo{
		ID:      100,
		Name:    ast.NewCIStr("T_Mixed_Case"),
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
		State:   model.StatePublic,
	}
	columnTypes := []byte{
		mysql.TypeLonglong,
		mysql.TypeVarchar,
		mysql.TypeDatetime,
		mysql.TypeNewDecimal,
		mysql.TypeJSON,
		mysql.TypeBlob,
	}
	for i, tp := range columnTypes {
		fieldType := types.NewFieldType(tp)
		fieldType.SetFlen(64 + i)
		fieldType.SetDecimal(i % 3)
		fieldType.SetCharset("utf8mb4")
		fieldType.SetCollate("utf8mb4_bin")
		tableInfo.Columns = append(tableInfo.Columns, &model.ColumnInfo{
			ID:        int64(i + 1),
			Name:      ast.NewCIStr(fmt.Sprintf("C_%d", i)),
			Offset:    i,
			FieldType: *fieldType,
			State:     model.StatePublic,
			Comment:   fmt.Sprintf("column %d", i),
			Version:   2,
		})
	}
	tableInfo.Indices = []*model.IndexInfo{{
		ID:      1,
		Name:    ast.NewCIStr("idx_c0"),
		Columns: []*model.IndexColumn{{Name: tableInfo.Columns[0].Name, Offset: 0}},
		State:   model.StatePublic,
	}}

	data, err := json.Marshal(tableInfo)
	require.NoError(t, err)
	expected := &model.TableInfo{}
	require.NoError(t, decodeColumnsTableInfo(data, expected))
	actual := &model.TableInfo{}
	iter := &TableInfoIterator{}
	require.True(t, iter.tryDecodeSimpleColumnsTableInfo(data, actual))
	require.Equal(t, expected, actual)
	require.Empty(t, actual.Indices)

	complex := tableInfo.Clone()
	complex.Columns[0].DefaultValue = "non-null default"
	data, err = json.Marshal(complex)
	require.NoError(t, err)
	require.False(t, iter.tryDecodeSimpleColumnsTableInfo(data, actual))

	complex = tableInfo.Clone()
	complex.Columns[0].FieldType.SetType(mysql.TypeEnum)
	complex.Columns[0].FieldType.SetElems([]string{"a", "b"})
	data, err = json.Marshal(complex)
	require.NoError(t, err)
	require.False(t, iter.tryDecodeSimpleColumnsTableInfo(data, actual))

	complex = tableInfo.Clone()
	complex.View = &model.ViewInfo{SelectStmt: "select 1"}
	data, err = json.Marshal(complex)
	require.NoError(t, err)
	require.False(t, iter.tryDecodeSimpleColumnsTableInfo(data, actual))
}
