// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/pingcap/errors"
)

// TableDataIR is table data intermediate representation.
type TableDataIR interface {
	Start(context.Context, *sql.Conn) error
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
	Close() error
}

// SQLRowIter is the iterator on a collection of sql.Row.
type SQLRowIter interface {
	Decode(RowReceiver) error
	Next()
	Error() error
	HasNext() bool
	// release SQLRowIter
	Close() error
}

type RowReceiverStringer interface {
	RowReceiver
	Stringer
}

type Stringer interface {
	WriteToBuffer(*bytes.Buffer, bool)
	WriteToBufferInCsv(*bytes.Buffer, bool, *csvOption)
}

type RowReceiver interface {
	BindAddress([]interface{})
}

func decodeFromRows(rows *sql.Rows, args []interface{}, row RowReceiver) error {
	row.BindAddress(args)
	if err := rows.Scan(args...); err != nil {
		rows.Close()
		return errors.Trace(err)
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
