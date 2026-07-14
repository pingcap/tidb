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
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// toColumnType converts database type to parquet column type.
// ColumnInfo.DatabaseTypeName must come from database/sql
// ColumnType.DatabaseTypeName().
// With MySQL drivers, temporal types are reported as base names
// (e.g. TIMESTAMP/DATETIME) without precision suffixes such as "(6)".
//
// go-sql-driver/mysql DatabaseTypeName() does not emit the type aliases below.
// They are intentionally not specialized and fall back to BYTE_ARRAY so unknown
// or driver-specific names remain forward compatible with string-like export.
// NUMERIC, FIXED, UNSIGNED MEDIUMINT, VECTOR, NCHAR, NVARCHAR, CHARACTER,
// VARCHARACTER, SQL_TSI_YEAR, VAR_STRING, LONG, INTEGER, INT1, INT2, INT3, INT8,
// BOOL, BOOLEAN, REAL, DOUBLE PRECISION,
func toColumnType(columnInfo *ColumnInfo) columnType {
	dbTypeName := strings.ToUpper(strings.TrimSpace(columnInfo.DatabaseTypeName))
	switch dbTypeName {
	case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
		"DATE", "TIME", "SET", "JSON", "ENUM", "NULL", "GEOMETRY":
		// "NULL" in DatabaseTypeName() means the server reported the column
		// metadata type as MySQL protocol MYSQL_TYPE_NULL.
		// Typical case: SELECT NULL AS c -> DatabaseTypeName() for c is "NULL".
		return columnType{Physical: parquet.Types.ByteArray, Logical: schema.StringLogicalType{}, TypeLength: -1}
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT":
		return columnType{Physical: parquet.Types.ByteArray, TypeLength: -1}
	case "TIMESTAMP", "DATETIME":
		return columnType{Physical: parquet.Types.Int64, Logical: schema.NewTimestampLogicalType(false, schema.TimeUnitMicros), TypeLength: -1}
	case "YEAR", "TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "INT":
		// UNSIGNED MEDIUMINT is returned as MEDIUMINT in the driver, it's fine
		// to use int32 to store it.
		return columnType{Physical: parquet.Types.Int32, TypeLength: -1}
	case "BIGINT", "UNSIGNED INT":
		return columnType{Physical: parquet.Types.Int64, TypeLength: -1}
	case "DECIMAL":
		return decimalColumnType(columnInfo)
	case "UNSIGNED BIGINT":
		return columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			Logical:    schema.NewDecimalLogicalType(20, 0),
			TypeLength: 9,
			Precision:  20,
			Scale:      0,
		}
	case "FLOAT":
		// MySQL stores FLOAT as single-precision (4-byte) values, but MySQL evaluates
		// FLOAT expressions in double precision. Exporting as Parquet FLOAT (32-bit)
		// is not safe for compatibility because intermediate/query results may need
		// more precision than 32-bit can preserve.
		return columnType{Physical: parquet.Types.Double, TypeLength: -1}
	case "DOUBLE":
		return columnType{Physical: parquet.Types.Double, TypeLength: -1}
	default:
		return columnType{Physical: parquet.Types.ByteArray, TypeLength: -1}
	}
}

func decimalColumnType(columnInfo *ColumnInfo) columnType {
	precision, scale := int(columnInfo.Precision), int(columnInfo.Scale)
	// Keep an interoperability-first mapping here, not just raw Parquet limits:
	// - Parquet DECIMAL itself is not capped at 38 digits when stored as
	//   BYTE_ARRAY.
	// - However, many downstream engines cap DECIMAL at 38 digits, so map
	//   DECIMAL/NUMERIC with precision <= 38 to DECIMAL
	//   (INT32/INT64/FIXED_LEN_BYTE_ARRAY), and represent larger precision as
	//   UTF-8 strings in BYTE_ARRAY to avoid reader incompatibilities.
	if precision <= 0 || precision > 38 {
		return columnType{Physical: parquet.Types.ByteArray, Logical: schema.StringLogicalType{}, TypeLength: -1}
	}
	decimalLogicalType := schema.NewDecimalLogicalType(int32(precision), int32(scale))
	if precision <= 9 {
		return columnType{
			Physical:   parquet.Types.Int32,
			Logical:    decimalLogicalType,
			TypeLength: -1,
			Precision:  precision,
			Scale:      scale,
		}
	}
	if precision <= 18 {
		return columnType{
			Physical:   parquet.Types.Int64,
			Logical:    decimalLogicalType,
			TypeLength: -1,
			Precision:  precision,
			Scale:      scale,
		}
	}

	typeLength := decimalFixedLengthBytesForPrecision(precision)
	return columnType{
		Physical:   parquet.Types.FixedLenByteArray,
		Logical:    decimalLogicalType,
		TypeLength: typeLength,
		Precision:  precision,
		Scale:      scale,
	}
}

func decimalFixedLengthBytesForPrecision(precision int) int {
	if precision <= 0 {
		return -1
	}
	// Parquet DECIMAL for FIXED_LEN_BYTE_ARRAY requires:
	// precision <= floor(log10(2^(8*n - 1) - 1)).
	// Solving for n gives:
	// n >= (precision*log2(10) + 1) / 8.
	return int(math.Ceil((float64(precision)*math.Log2(10) + 1) / 8))
}
