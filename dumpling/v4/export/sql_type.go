package export

import (
	"bytes"
	"database/sql"
	"fmt"
)

var colTypeRowReceiverMap = map[string]func() RowReceiverStringer{}

var nullValue = "NULL"
var quotationMark byte = '\''
var doubleQuotationMark byte = '"'
var quotationMarkNotQuote = []byte{quotationMark}
var quotationMarkQuote = []byte{quotationMark, quotationMark}

func init() {
	for _, s := range dataTypeString {
		colTypeRowReceiverMap[s] = SQLTypeStringMaker
	}
	for _, s := range dataTypeNum {
		colTypeRowReceiverMap[s] = SQLTypeNumberMaker
	}
	for _, s := range dataTypeBin {
		colTypeRowReceiverMap[s] = SQLTypeBytesMaker
	}
}

var dataTypeString = []string{
	"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "CHARACTER", "VARCHARACTER",
	"TIMESTAMP", "DATETIME", "DATE", "TIME", "YEAR", "SQL_TSI_YEAR",
	"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
	"ENUM", "SET", "JSON",
}

var dataTypeNum = []string{
	"INTEGER", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT",
	"INT", "INT1", "INT2", "INT3", "INT8",
	"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
	"DECIMAL", "NUMERIC", "FIXED",
	"BOOL", "BOOLEAN",
}

var dataTypeBin = []string{
	"BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "LONG",
	"BINARY", "VARBINARY",
	"BIT",
}

func escape(s []byte, bf *bytes.Buffer, escapeBackslash bool) {
	if !escapeBackslash {
		bf.Write(bytes.ReplaceAll(s, quotationMarkNotQuote, quotationMarkQuote))
		return
	}
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
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
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
	if last == 0 {
		bf.Write(s)
	} else if last < len(s) {
		bf.Write(s[last:])
	}
}

func SQLTypeStringMaker() RowReceiverStringer {
	return &SQLTypeString{}
}

func SQLTypeBytesMaker() RowReceiverStringer {
	return &SQLTypeBytes{}
}

func SQLTypeNumberMaker() RowReceiverStringer {
	return &SQLTypeNumber{}
}

func MakeRowReceiver(colTypes []string) RowReceiverStringer {
	rowReceiverArr := make(RowReceiverArr, len(colTypes))
	for i, colTp := range colTypes {
		recMaker, ok := colTypeRowReceiverMap[colTp]
		if !ok {
			recMaker = SQLTypeStringMaker
		}
		rowReceiverArr[i] = recMaker()
	}
	return rowReceiverArr
}

type RowReceiverArr []RowReceiverStringer

func (r RowReceiverArr) BindAddress(args []interface{}) {
	for i := range args {
		r[i].BindAddress(args[i : i+1])
	}
}
func (r RowReceiverArr) ReportSize() uint64 {
	var sum uint64
	for _, receiver := range r {
		sum += receiver.ReportSize()
	}
	return sum
}

func (r RowReceiverArr) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	bf.WriteByte('(')
	for i, receiver := range r {
		receiver.WriteToBuffer(bf, escapeBackslash)
		if i != len(r)-1 {
			bf.WriteByte(',')
		}
	}
	bf.WriteByte(')')
}

func (r RowReceiverArr) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, csvNullValue string) {
	for i, receiver := range r {
		receiver.WriteToBufferInCsv(bf, escapeBackslash, csvNullValue)
		if i != len(r)-1 {
			bf.WriteByte(',')
		}
	}
}

type SQLTypeNumber struct {
	SQLTypeString
}

func (s SQLTypeNumber) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(nullValue)
	}
}

func (s SQLTypeNumber) WriteToBufferInCsv(bf *bytes.Buffer, _ bool, csvNullValue string) {
	if s.RawBytes != nil {
		bf.Write(s.RawBytes)
	} else {
		bf.WriteString(csvNullValue)
	}
}

type SQLTypeString struct {
	sql.RawBytes
}

func (s *SQLTypeString) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
}
func (s *SQLTypeString) ReportSize() uint64 {
	if s.RawBytes != nil {
		return uint64(len(s.RawBytes))
	}
	return uint64(len(nullValue))
}

func (s *SQLTypeString) WriteToBuffer(bf *bytes.Buffer, escapeBackslash bool) {
	if s.RawBytes != nil {
		bf.WriteByte(quotationMark)
		escape(s.RawBytes, bf, escapeBackslash)
		bf.WriteByte(quotationMark)
	} else {
		bf.WriteString(nullValue)
	}
}

func (s *SQLTypeString) WriteToBufferInCsv(bf *bytes.Buffer, escapeBackslash bool, csvNullValue string) {
	if s.RawBytes != nil {
		bf.WriteByte(doubleQuotationMark)
		escape(s.RawBytes, bf, escapeBackslash)
		bf.WriteByte(doubleQuotationMark)
	} else {
		bf.WriteString(csvNullValue)
	}
}

type SQLTypeBytes struct {
	sql.RawBytes
}

func (s *SQLTypeBytes) BindAddress(arg []interface{}) {
	arg[0] = &s.RawBytes
}
func (s *SQLTypeBytes) ReportSize() uint64 {
	return uint64(len(s.RawBytes))
}

func (s *SQLTypeBytes) WriteToBuffer(bf *bytes.Buffer, _ bool) {
	fmt.Fprintf(bf, "x'%x'", s.RawBytes)
}

func (s *SQLTypeBytes) WriteToBufferInCsv(bf *bytes.Buffer, _ bool, csvNullValue string) {
	if s.RawBytes != nil {
		bf.WriteByte(doubleQuotationMark)
		bf.Write(s.RawBytes)
		bf.WriteByte(doubleQuotationMark)
	} else {
		bf.WriteString(csvNullValue)
	}
}
