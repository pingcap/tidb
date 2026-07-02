// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

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
		colTypeRowReceiverMap[s] = SQLTypeStringMaker
	}
	for _, s := range dataTypeIntArr {
		dataTypeInt[s] = struct{}{}
	}
	for _, s := range dataTypeNumArr {
		colTypeRowReceiverMap[s] = SQLTypeNumberMaker
	}
	for _, s := range dataTypeBinArr {
		dataTypeBin[s] = struct{}{}
		colTypeRowReceiverMap[s] = SQLTypeBytesMaker
	}
}

var dataTypeString, dataTypeInt, dataTypeBin = make(map[string]struct{}), make(map[string]struct{}), make(map[string]struct{})

// SQLTypeStringMaker returns a SQLTypeString
func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

// SQLTypeBytesMaker returns a SQLTypeBytes
func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

// SQLTypeNumberMaker returns a SQLTypeNumber
func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeNumber{}
}

// MakeRowReceiver constructs RowReceiverArr from column types
func MakeRowReceiver(colTypes []string) *RowReceiverArr {
	rowReceiverArr := make([]RowReceiverStringer, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return &RowReceiverArr{
		bound:     false,
		receivers: rowReceiverArr,
	}
}

// RowReceiverArr is the combined RowReceiver array
type RowReceiverArr struct {
	bound     bool
	receivers []RowReceiverStringer
}

// BindAddress implements RowReceiver.BindAddress
func (r *RowReceiverArr) BindAddress(args []any) {
	if r.bound {
		return
	}
	r.bound = true
	for i := range args {
		r.receivers[i].BindAddress(args[i : i+1])
	}
}

// GetRawBytes implements Stringer.GetRawBytes.
func (r RowReceiverArr) GetRawBytes() []sql.RawBytes {
	return r.AppendRawBytes(make([]sql.RawBytes, 0, len(r.receivers)))
}

// AppendRawBytes appends each receiver's raw bytes to dst and returns it, so the
// caller can reuse one slice across rows instead of allocating per row.
func (r RowReceiverArr) AppendRawBytes(dst []sql.RawBytes) []sql.RawBytes {
	for _, receiver := range r.receivers {
		dst = append(dst, receiver.GetRawBytes()[0])
	}
	return dst
}

// SQLTypeNumber implements RowReceiverStringer which represents numeric type columns in database
type SQLTypeNumber struct {
	SQLTypeString
}

// GetRawBytes implements Stringer.GetRawBytes.
func (s *SQLTypeNumber) GetRawBytes() []sql.RawBytes {
	return []sql.RawBytes{s.RawBytes}
}

// SQLTypeString implements RowReceiverStringer which represents string type columns in database
type SQLTypeString struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeString) BindAddress(arg []any) {
	arg[0] = &s.RawBytes
}

// GetRawBytes implements Stringer.GetRawBytes.
func (s *SQLTypeString) GetRawBytes() []sql.RawBytes {
	return []sql.RawBytes{s.RawBytes}
}

// SQLTypeBytes implements RowReceiverStringer which represents bytes type columns in database
type SQLTypeBytes struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeBytes) BindAddress(arg []any) {
	arg[0] = &s.RawBytes
}

// GetRawBytes implements Stringer.GetRawBytes.
func (s *SQLTypeBytes) GetRawBytes() []sql.RawBytes {
	return []sql.RawBytes{s.RawBytes}
}
