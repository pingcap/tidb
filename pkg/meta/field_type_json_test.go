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

package meta_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMySQLUserFieldTypeJSONOmitEmpty(t *testing.T) {
	tableInfo := getMockTableInfoForFieldTypeJSONTest(t, metadef.CreateUserTable)

	currentJSON, err := json.Marshal(tableInfo)
	require.NoError(t, err)

	var roundTrip model.TableInfo
	require.NoError(t, json.Unmarshal(currentJSON, &roundTrip))
	require.Len(t, roundTrip.Columns, len(tableInfo.Columns))
	require.Len(t, roundTrip.Indices, len(tableInfo.Indices))
	for i := range tableInfo.Columns {
		requireFieldTypeEqual(t, tableInfo.Columns[i], roundTrip.Columns[i])
	}

	currentColumnsJSON, err := json.Marshal(tableInfo.Columns)
	require.NoError(t, err)

	var roundTripColumns []*model.ColumnInfo
	require.NoError(t, json.Unmarshal(currentColumnsJSON, &roundTripColumns))
	require.Len(t, roundTripColumns, len(tableInfo.Columns))
	for i := range tableInfo.Columns {
		requireFieldTypeEqual(t, tableInfo.Columns[i], roundTripColumns[i])
	}

	t.Logf("mysql.user table info json bytes: current=%d", len(currentJSON))
	t.Logf("mysql.user columns json bytes: current=%d", len(currentColumnsJSON))
}

func BenchmarkMySQLUserFieldTypeJSON(b *testing.B) {
	tableInfo := getMockTableInfoForFieldTypeJSONTest(b, metadef.CreateUserTable)
	b.Run("marshal_table_info", func(b *testing.B) {
		benchmarkMarshalJSON(b, tableInfo)
	})
	b.Run("unmarshal_table_info", func(b *testing.B) {
		data, err := json.Marshal(tableInfo)
		require.NoError(b, err)
		benchmarkUnmarshalTableInfoJSON(b, data, len(tableInfo.Columns), len(tableInfo.Indices))
	})
}

func benchmarkMarshalJSON(b *testing.B, input any) {
	b.Helper()

	reference, err := json.Marshal(input)
	require.NoError(b, err)

	b.ReportMetric(float64(len(reference)), "json_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(input); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUnmarshalTableInfoJSON(b *testing.B, data []byte, columnCount, indexCount int) {
	b.Helper()

	var tableInfo model.TableInfo
	err := json.Unmarshal(data, &tableInfo)
	require.NoError(b, err)
	require.Len(b, tableInfo.Columns, columnCount)
	require.Len(b, tableInfo.Indices, indexCount)

	b.ReportMetric(float64(len(data)), "json_bytes")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var tableInfo model.TableInfo
		if err := json.Unmarshal(data, &tableInfo); err != nil {
			b.Fatal(err)
		}
	}
}

func getMockTableInfoForFieldTypeJSONTest(tb testing.TB, sql string) *model.TableInfo {
	tb.Helper()

	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(tb, err)
	se := mock.NewContext()
	tblInfo, err := ddl.MockTableInfo(se, stmt.(*ast.CreateTableStmt), 1)
	require.NoError(tb, err)
	return tblInfo
}

func requireFieldTypeEqual(t testing.TB, expected, actual *model.ColumnInfo) {
	t.Helper()

	require.Equal(t, expected.Name, actual.Name)
	require.True(t, expected.FieldType.Equals(&actual.FieldType), expected.Name.O)
}
