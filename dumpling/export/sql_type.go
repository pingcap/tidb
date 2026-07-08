// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
)

// There are two kinds of scenes to use this dataType
// The first is to be the receiver of table sample, which will use tidb's INFORMATION_SCHEMA.COLUMNS's DATA_TYPE column, which is from
// https://github.com/pingcap/tidb/blob/619c4720059ea619081b01644ef3084b426d282f/executor/infoschema_reader.go#L654
// https://github.com/pingcap/parser/blob/8e8ed7927bde11c4cf0967afc5e05ab5aeb14cc7/types/etc.go#L44-70
// The second is to be the receiver of select row type, which will use sql.DB's rows.DatabaseTypeName(), which is from
// https://github.com/go-sql-driver/mysql/blob/v1.5.0/fields.go#L17-97
func initColTypeRowReceiverMap() {
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

	dataTypeBinArr := []string{
		"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "LONG",
		"BINARY", "VARBINARY",
		"BIT", "GEOMETRY",
	}

	dataTypeNumArr := append(dataTypeIntArr, []string{
		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
		"DECIMAL", "NUMERIC", "FIXED",
		"BOOL", "BOOLEAN",
	}...)

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

// MakeRowReceiver constructs a RowReceiverArr for a row of len(colTypes) columns.
func MakeRowReceiver(colTypes []string) *RowReceiverArr {
	return &RowReceiverArr{data: make([]sql.RawBytes, len(colTypes))}
}

// RowReceiverArr scans one SQL row into a reusable []sql.RawBytes: BindAddress
// points the driver at each element, and AppendRawBytes/GetRawBytes read them
// back. The values alias the driver's buffer and are only valid until the next
// scan, so the caller must consume them before advancing the row iterator.
type RowReceiverArr struct {
	bound bool
	data  []sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress.
func (r *RowReceiverArr) BindAddress(args []any) {
	if r.bound {
		return
	}
	r.bound = true
	for i := range args {
		args[i] = &r.data[i]
	}
}

// GetRawBytes returns the current row's raw bytes as a fresh slice.
func (r *RowReceiverArr) GetRawBytes() []sql.RawBytes {
	return r.AppendRawBytes(make([]sql.RawBytes, 0, len(r.data)))
}

// AppendRawBytes appends the current row's raw bytes to dst and returns it, so
// the caller can reuse one slice across rows instead of allocating per row.
func (r *RowReceiverArr) AppendRawBytes(dst []sql.RawBytes) []sql.RawBytes {
	return append(dst, r.data...)
}
