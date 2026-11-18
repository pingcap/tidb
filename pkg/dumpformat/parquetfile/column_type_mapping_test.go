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
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestToColumnTypeMappings(t *testing.T) {
	t.Run("maps FLOAT to DOUBLE", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "FLOAT"})
		require.Equal(t, parquet.Types.Double, columnType.Physical)
	})

	t.Run("maps CHAR to BYTE_ARRAY with string logical type", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "CHAR"})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
		_, ok := columnType.Logical.(schema.StringLogicalType)
		require.True(t, ok)
	})

	t.Run("maps NUMERIC as fallback BYTE_ARRAY", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "NUMERIC", Precision: 10, Scale: 2})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
		require.Nil(t, columnType.Logical)
	})

	t.Run("maps UNSIGNED MEDIUMINT as fallback BYTE_ARRAY", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "UNSIGNED MEDIUMINT"})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
	})

	t.Run("maps DECIMAL precision 38 to FIXED_LEN_BYTE_ARRAY", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "DECIMAL", Precision: 38, Scale: 6})
		require.Equal(t, parquet.Types.FixedLenByteArray, columnType.Physical)
		require.Equal(t, 16, columnType.TypeLength)
		_, ok := columnType.Logical.(schema.DecimalLogicalType)
		require.True(t, ok)
		require.Equal(t, 38, columnType.Precision)
		require.Equal(t, 6, columnType.Scale)
	})

	t.Run("maps NUMERIC precision over 38 as fallback BYTE_ARRAY", func(t *testing.T) {
		columnType := toColumnType(&ColumnInfo{DatabaseTypeName: "NUMERIC", Precision: 39, Scale: 2})
		require.Equal(t, parquet.Types.ByteArray, columnType.Physical)
		require.Nil(t, columnType.Logical)
	})

	t.Run("maps MariaDB aliases and unknown types", func(t *testing.T) {
		realType := toColumnType(&ColumnInfo{DatabaseTypeName: "REAL"})
		require.Equal(t, parquet.Types.ByteArray, realType.Physical)

		boolType := toColumnType(&ColumnInfo{DatabaseTypeName: "BOOL"})
		require.Equal(t, parquet.Types.ByteArray, boolType.Physical)

		ncharType := toColumnType(&ColumnInfo{DatabaseTypeName: "NCHAR"})
		require.Equal(t, parquet.Types.ByteArray, ncharType.Physical)

		fallbackType := toColumnType(&ColumnInfo{DatabaseTypeName: "SOME_NEW_TYPE"})
		require.Equal(t, parquet.Types.ByteArray, fallbackType.Physical)
	})

	t.Run("maps DECIMAL precision boundaries", func(t *testing.T) {
		int32Type := toColumnType(&ColumnInfo{DatabaseTypeName: "DECIMAL", Precision: 9, Scale: 3})
		require.Equal(t, parquet.Types.Int32, int32Type.Physical)
		require.Equal(t, 9, int32Type.Precision)
		require.Equal(t, 3, int32Type.Scale)

		int64Type := toColumnType(&ColumnInfo{DatabaseTypeName: "DECIMAL", Precision: 18, Scale: 4})
		require.Equal(t, parquet.Types.Int64, int64Type.Physical)

		fixedType := toColumnType(&ColumnInfo{DatabaseTypeName: "DECIMAL", Precision: 19, Scale: 5})
		require.Equal(t, parquet.Types.FixedLenByteArray, fixedType.Physical)
		require.Equal(t, decimalFixedLengthBytesForPrecision(19), fixedType.TypeLength)

		stringType := toColumnType(&ColumnInfo{DatabaseTypeName: "DECIMAL", Precision: 0, Scale: 2})
		require.Equal(t, parquet.Types.ByteArray, stringType.Physical)
	})

	t.Run("maps additional SQL type families", func(t *testing.T) {
		enumType := toColumnType(&ColumnInfo{DatabaseTypeName: "ENUM"})
		require.Equal(t, parquet.Types.ByteArray, enumType.Physical)
		_, ok := enumType.Logical.(schema.StringLogicalType)
		require.True(t, ok)

		blobType := toColumnType(&ColumnInfo{DatabaseTypeName: "BLOB"})
		require.Equal(t, parquet.Types.ByteArray, blobType.Physical)

		timestampType := toColumnType(&ColumnInfo{DatabaseTypeName: "TIMESTAMP"})
		require.Equal(t, parquet.Types.Int64, timestampType.Physical)
		timestampLogicalType, ok := timestampType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMicros, timestampLogicalType.TimeUnit())

		timestampMillisType := toColumnType(&ColumnInfo{DatabaseTypeName: "TIMESTAMP", Scale: 3})
		timestampMillisLogicalType, ok := timestampMillisType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		// we always use TimeUnitMicros.
		require.Equal(t, schema.TimeUnitMicros, timestampMillisLogicalType.TimeUnit())

		datetimeMicrosType := toColumnType(&ColumnInfo{DatabaseTypeName: "DATETIME", Scale: 6})
		datetimeMicrosLogicalType, ok := datetimeMicrosType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		require.Equal(t, schema.TimeUnitMicros, datetimeMicrosLogicalType.TimeUnit())

		datetimeMillisType := toColumnType(&ColumnInfo{DatabaseTypeName: "DATETIME", Precision: 23, Scale: 3})
		datetimeMillisLogicalType, ok := datetimeMillisType.Logical.(schema.TimestampLogicalType)
		require.True(t, ok)
		// we always use TimeUnitMicros.
		require.Equal(t, schema.TimeUnitMicros, datetimeMillisLogicalType.TimeUnit())

		bigintType := toColumnType(&ColumnInfo{DatabaseTypeName: "BIGINT"})
		require.Equal(t, parquet.Types.Int64, bigintType.Physical)

		doubleType := toColumnType(&ColumnInfo{DatabaseTypeName: "DOUBLE"})
		require.Equal(t, parquet.Types.Double, doubleType.Physical)

		integerAlias := toColumnType(&ColumnInfo{DatabaseTypeName: "INTEGER"})
		require.Equal(t, parquet.Types.ByteArray, integerAlias.Physical)

		int8Alias := toColumnType(&ColumnInfo{DatabaseTypeName: "INT8"})
		require.Equal(t, parquet.Types.ByteArray, int8Alias.Physical)

		fixedAlias := toColumnType(&ColumnInfo{DatabaseTypeName: "FIXED"})
		require.Equal(t, parquet.Types.ByteArray, fixedAlias.Physical)
	})
}

func TestDecimalFixedLengthBytesForPrecisionEdgeCases(t *testing.T) {
	require.Equal(t, -1, decimalFixedLengthBytesForPrecision(0))
	require.Equal(t, 1, decimalFixedLengthBytesForPrecision(2))
}
