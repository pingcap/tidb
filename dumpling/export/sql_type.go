// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import "database/sql"

// initColumnTypeSets classifies MySQL/TiDB DATA_TYPE names into the string, int,
// numeric and binary sets used by columnKinds. The type strings come from tidb's
// INFORMATION_SCHEMA.COLUMNS DATA_TYPE (table sample) or sql.DB's
// rows.DatabaseTypeName (select rows):
//   - https://github.com/pingcap/tidb/blob/619c4720059ea619081b01644ef3084b426d282f/executor/infoschema_reader.go#L654
//   - https://github.com/go-sql-driver/mysql/blob/v1.5.0/fields.go#L17-97
func initColumnTypeSets() {
	dataTypeStringArr := []string{
		"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER",
		"TIMESTAMP", "DATETIME", "DATE", "TIME", "YEAR", "SQL_TSI_YEAR",
		"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
		"ENUM", "SET", "JSON", "NULL", "VAR_STRING",
	}

	dataTypeIntArr := []string{
		"INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT",
		"INT", "INT1", "INT2", "INT3", "INT8",
		"UNSIGNED INT", "UNSIGNED BIGINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT", // introduced in https://github.com/go-sql-driver/mysql/pull/1238
	}

	dataTypeNumArr := append(dataTypeIntArr, []string{
		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
		"DECIMAL", "NUMERIC", "FIXED",
		"BOOL", "BOOLEAN",
	}...)

	dataTypeBinArr := []string{
		"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "LONG",
		"BINARY", "VARBINARY",
		"BIT", "GEOMETRY",
	}

	for _, s := range dataTypeStringArr {
		dataTypeString[s] = struct{}{}
	}
	for _, s := range dataTypeIntArr {
		dataTypeInt[s] = struct{}{}
	}
	for _, s := range dataTypeNumArr {
		dataTypeNum[s] = struct{}{}
	}
	for _, s := range dataTypeBinArr {
		dataTypeBin[s] = struct{}{}
	}
}

var dataTypeString, dataTypeInt, dataTypeNum, dataTypeBin = make(map[string]struct{}), make(map[string]struct{}), make(map[string]struct{}), make(map[string]struct{})

// MakeRowReceiver builds a RowReceiverArr sized for len(colTypes) columns. Every
// column decodes to raw bytes, so only the count matters; the column type feeds
// FieldKind classification later (see columnKinds).
func MakeRowReceiver(colTypes []string) *RowReceiverArr {
	return &RowReceiverArr{data: make([]sql.RawBytes, len(colTypes))}
}

// RowReceiverArr holds one row's column values, decoded as raw bytes by Scan.
type RowReceiverArr struct {
	bound bool
	data  []sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress, pointing Scan at each column.
func (r *RowReceiverArr) BindAddress(args []any) {
	if r.bound {
		return
	}
	r.bound = true
	for i := range r.data {
		args[i] = &r.data[i]
	}
}

// GetRawBytes returns the current row's raw column values in a fresh slice.
func (r RowReceiverArr) GetRawBytes() []sql.RawBytes {
	return r.appendRawBytes(make([]sql.RawBytes, 0, len(r.data)))
}

// appendRawBytes appends the current row's raw values to dst so callers can reuse
// one slice across rows.
func (r RowReceiverArr) appendRawBytes(dst []sql.RawBytes) []sql.RawBytes {
	return append(dst, r.data...)
}
