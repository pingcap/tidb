package export

import (
	"bytes"
	"database/sql"
)

// TableDataIR is table data intermediate representation.
type TableDataIR interface {
	DatabaseName() string
	TableName() string
	ChunkIndex() int
	ColumnCount() uint
	ColumnTypes() []string
	ColumnNames() []string
	SelectedField() string
	EscapeBackSlash() bool

	SpecialComments() StringIter
	Rows() SQLRowIter
}

// SQLRowIter is the iterator on a collection of sql.Row.
type SQLRowIter interface {
	Decode(RowReceiver) error
	Next()
	HasNext() bool
	HasNextSQLRowIter() bool
	NextSQLRowIter() SQLRowIter
	// release SQLRowIter
	Close() error
}

type RowReceiverStringer interface {
	RowReceiver
	Stringer
}

type Stringer interface {
	WriteToBuffer(*bytes.Buffer, bool)
	WriteToBufferInCsv(*bytes.Buffer, bool)
}

type RowReceiver interface {
	BindAddress([]interface{})
	ReportSize() uint64
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row RowReceiver) error {
	row.BindAddress(args)
	if err := rows.Scan(args...); err != nil {
		rows.Close()
		return withStack(err)
	}
	return nil
}

// StringIter is the iterator on a collection of strings.
type StringIter interface {
	Next() string
	HasNext() bool
}

type MetaIR interface {
	SpecialComments() StringIter
	TargetName() string
	MetaSQL() string
}

// Logger used for logging when export.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}
