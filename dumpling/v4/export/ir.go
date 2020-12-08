// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/pingcap/errors"
)

// TableDataIR is table data intermediate representation.
// A table may be split into multiple TableDataIRs.
type TableDataIR interface {
	Start(context.Context, *sql.Conn) error
	Rows() SQLRowIter
	Close() error
}

// TableMeta contains the meta information of a table.
type TableMeta interface {
	DatabaseName() string
	TableName() string
	ColumnCount() uint
	ColumnTypes() []string
	ColumnNames() []string
	SelectedField() string
	SpecialComments() StringIter
	ShowCreateTable() string
	ShowCreateView() string
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

// RowReceiverStringer is a combined interface of RowReceiver and Stringer
type RowReceiverStringer interface {
	RowReceiver
	Stringer
}

// Stringer is an interface which represents sql types that support writing to buffer in sql/csv type
type Stringer interface {
	WriteToBuffer(*bytes.Buffer, bool)
	WriteToBufferInCsv(*bytes.Buffer, bool, *csvOption)
}

// RowReceiver is an interface which represents sql types that support bind address for *sql.Rows
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

// MetaIR is the interface that wraps database/table/view's metadata
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
