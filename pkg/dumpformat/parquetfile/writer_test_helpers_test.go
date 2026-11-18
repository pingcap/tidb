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

package parquetfile

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/stretchr/testify/require"
)

type wrappedColumnChunkWriter struct {
	file.ColumnChunkWriter
}

func readInt32Column(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []int32, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.Int32ColumnChunkReader)
	values := make([]int32, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}

func readInt64Column(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []int64, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.Int64ColumnChunkReader)
	values := make([]int64, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}

func readByteArrayColumn(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []string, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.ByteArrayColumnChunkReader)
	values := make([]parquet.ByteArray, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	actual := make([]string, 0, valuesRead)
	for _, value := range values[:valuesRead] {
		actual = append(actual, string(value))
	}
	require.Equal(t, expected, actual)
	require.Equal(t, expectedDef, defLevels)
}

func readBooleanColumn(t *testing.T, rowGroup *file.RowGroupReader, column, rows int, expected []bool, expectedDef []int16, expectedValues int) {
	t.Helper()
	chunkReader, err := rowGroup.Column(column)
	require.NoError(t, err)
	reader := chunkReader.(*file.BooleanColumnChunkReader)
	values := make([]bool, rows)
	defLevels := make([]int16, rows)
	total, valuesRead, err := reader.ReadBatch(int64(rows), values, defLevels, nil)
	require.NoError(t, err)
	require.EqualValues(t, rows, total)
	require.Equal(t, expectedValues, valuesRead)
	require.Equal(t, expected, values[:valuesRead])
	require.Equal(t, expectedDef, defLevels)
}
