// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
)

// TableDataIR is table data intermediate representation.
// A table may be split into multiple TableDataIRs.
type TableDataIR interface {
	Start(*tcontext.Context, *sql.Conn) error
	Rows() SQLRowIter
	Close() error
	RawRows() *sql.Rows
}

// TableMeta contains the meta information of a table.
type TableMeta interface {
	DatabaseName() string
	TableName() string
	ColumnCount() uint
	// ColumnTypes returns the selected output column types.
	ColumnTypes() []string
	// ColumnNames returns the selected output column names.
	ColumnNames() []string
	// SourceColumnTypes returns all source writable column types for schema-based logic such as split keys.
	SourceColumnTypes() []string
	// SourceColumnNames returns all source writable column names for schema-based logic such as split keys.
	SourceColumnNames() []string
	SelectedField() string
	SelectedLen() int
	SpecialComments() StringIter
	ShowCreateTable() string
	ShowCreateView() string
	AvgRowLength() uint64
	HasImplicitRowID() bool
	ColumnInfos() []*ColumnInfo
}

// ColumnInfo is an alias of parquet column metadata used by dumpling.
type ColumnInfo = parquetfile.ColumnInfo

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
	GetRawBytes() []sql.RawBytes
}

// RowReceiver is an interface which represents sql types that support bind address for *sql.Rows
type RowReceiver interface {
	BindAddress([]any)
}

func decodeFromRows(rows *sql.Rows, args []any, row RowReceiver) error {
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

func setTableMetaFromRows(serverType version.ServerType, rows *sql.Rows) (TableMeta, error) {
	tps, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Trace(err)
	}
	nms, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range nms {
		nms[i] = wrapBackTicks(nms[i])
	}
	return &tableMeta{
		colTypes:       tps,
		sourceColTypes: tps,
		selectedField:  strings.Join(nms, ","),
		specCmts:       getSpecialComments(serverType),
	}, nil
}
