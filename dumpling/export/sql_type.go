// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"fmt"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

var (
	nullValue         = "NULL"
	quotationMark     = []byte{'\''}
	twoQuotationMarks = []byte{'\'', '\''}
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

func escapeBackslashSQL(s []byte, bf *bytes.Buffer) {
	var (
		escape byte
		last   = 0
	)
	// reference: https://gist.github.com/siddontang/8875771
	for i := 0; i < len(s); i++ {
		escape = 0

		switch s[i] {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\n': /* Must be escaped for logs */
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"': /* Better safe than sorry */
			escape = '"'
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(escape)
			last = i + 1
		}
	}
	bf.Write(s[last:])
}

func escapeBackslashCSV(s []byte, bf *bytes.Buffer, opt *csvOption) {
	var (
		escape  byte
		last    int
		specCmt byte
	)
	if len(opt.delimiter) > 0 {
		specCmt = opt.delimiter[0] // if csv has a delimiter, we should use backslash to comment the delimiter in field value
	} else if len(opt.separator) > 0 {
		specCmt = opt.separator[0] // if csv's delimiter is "", we should escape the separator to avoid error
	}

	for i := 0; i < len(s); i++ {
		escape = 0

		switch s[i] {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\r':
			escape = 'r'
		case '\n': /* escaped for line terminators */
			escape = 'n'
		case '\\':
			escape = '\\'
		case specCmt:
			escape = specCmt
		}

		if escape != 0 {
			bf.Write(s[last:i])
			bf.WriteByte('\\')
			bf.WriteByte(escape)
			last = i + 1
		}
	}
	bf.Write(s[last:])
}

func escapeSQL(s []byte, bf *bytes.Buffer, escapeBackslash bool) { // revive:disable-line:flag-parameter
	if escapeBackslash {
		escapeBackslashSQL(s, bf)
	} else {
		bf.Write(bytes.ReplaceAll(s, quotationMark, twoQuotationMarks))
	}
}

func escapeCSV(s []byte, bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) { // revive:disable-line:flag-parameter
	switch {
	case escapeBackslash:
		escapeBackslashCSV(s, bf, opt)
	case len(opt.delimiter) > 0:
		bf.Write(bytes.ReplaceAll(s, opt.delimiter, append(opt.delimiter, opt.delimiter...)))
	default:
		bf.Write(s)
	}
}

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

// WriteToBuffer implements Stringer.WriteToBuffer
func (r *RowReceiverArr) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	bf.WriteByte('(')
	for i, receiver := range r.receivers {
		receiver.WriteToBuffer(bf, escapeBackslash)
		if i != len(r.receivers)-1 {
			bf.WriteByte(',')
		}
	}
	bf.WriteByte(')')
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (r *RowReceiverArr) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	for i, receiver := range r.receivers {
		receiver.WriteToBufferInCsv(bf, escapeBackslash, opt)
		if i != len(r.receivers)-1 {
			bf.Write(opt.separator)
		}
	}
}

// SQLTypeNumber implements RowReceiverStringer which represents numeric type columns in database
type SQLTypeNumber struct {
	SQLTypeString
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s SQLTypeNumber) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s SQLTypeNumber) WriteToBufferInCsv(bf *bytes.Buffer, _ bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

// SQLTypeString implements RowReceiverStringer which represents string type columns in database
type SQLTypeString struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeString) BindAddress(arg []any) {
	arg[0] = &s.RawBytes
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s *SQLTypeString) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	if s.RawBytes != nil {
		bf.Write(quotationMark)
		escapeSQL(s.RawBytes, bf, escapeBackslash)
		bf.Write(quotationMark)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s *SQLTypeString) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		escapeCSV(s.RawBytes, bf, escapeBackslash, opt)
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}

// SQLTypeBytes implements RowReceiverStringer which represents bytes type columns in database
type SQLTypeBytes struct {
	sql.RawBytes
}

// BindAddress implements RowReceiver.BindAddress
func (s *SQLTypeBytes) BindAddress(arg []any) {
	arg[0] = &s.RawBytes
}

// WriteToBuffer implements Stringer.WriteToBuffer
func (s *SQLTypeBytes) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		fmt.Fprintf(bf, "x'%x'", s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

// WriteToBufferInCsv implements Stringer.WriteToBufferInCsv
func (s *SQLTypeBytes) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, opt *csvOption) {
	if s.RawBytes != nil {
		bf.Write(opt.delimiter)
		switch opt.binaryFormat {
		case BinaryFormatHEX:
			fmt.Fprintf(bf, "%x", s.RawBytes)
		case BinaryFormatBase64:
			bf.WriteString(base64.StdEncoding.EncodeToString(s.RawBytes))
		default:
			escapeCSV(s.RawBytes, bf, escapeBackslash, opt)
		}
		bf.Write(opt.delimiter)
	} else {
		bf.WriteString(opt.nullValue)
	}
}
