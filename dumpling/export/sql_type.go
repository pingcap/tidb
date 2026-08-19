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

// MakeRowReceiver builds a RowReceiverArr with one receiver per column. Every
// column decodes to raw bytes, so the receivers are identical; the column type
// only feeds FieldKind classification later (see columnKinds).
func MakeRowReceiver(colTypes []string) *RowReceiverArr {
	receivers := make([]RowReceiverStringer, len(colTypes))
	for i := range receivers {
		receivers[i] = &rawReceiver{}
	}
	return &RowReceiverArr{receivers: receivers}
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
	return r.appendRawBytes(make([]sql.RawBytes, 0, len(r.receivers)))
}

func (r RowReceiverArr) appendRawBytes(dst []sql.RawBytes) []sql.RawBytes {
	for _, receiver := range r.receivers {
		dst = append(dst, receiver.GetRawBytes()[0])
	}
	return dst
}

// rawReceiver decodes one column as raw bytes from a *sql.Rows scan.
type rawReceiver struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *rawReceiver) BindAddress(arg []any) {
	arg[0] = &s.RawBytes
}

// GetRawBytes implements Stringer.GetRawBytes.
func (s *rawReceiver) GetRawBytes() []sql.RawBytes {
	return []sql.RawBytes{s.RawBytes}
}
