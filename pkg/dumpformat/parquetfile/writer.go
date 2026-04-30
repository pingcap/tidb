// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetfile

import (
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/docker/go-units"
)

// defaultCompressionType is the default Parquet compression codec.
var defaultCompressionType = compress.Codecs.Zstd

const (
	defaultRowGroupMemoryLimitBytes = 120 * units.MiB
	definitionLevelMemoryBytes      = int64(2)
)

// ColumnInfo describes a SQL result column to be written into Parquet.
type ColumnInfo struct {
	Name string
	// DatabaseTypeName must be the canonical database/sql
	// ColumnType.DatabaseTypeName() value (for example: TIMESTAMP, DATETIME,
	// DECIMAL, VARCHAR).
	DatabaseTypeName string
	Nullable         bool
	Precision        int64
	Scale            int64
}

// columnType describes the physical and logical Parquet type for a SQL column.
type columnType struct {
	Physical   parquet.Type
	Logical    schema.LogicalType
	TypeLength int
	Precision  int
	Scale      int
}

type column struct {
	ColumnInfo
	columnType
	Repetition parquet.Repetition
	// allowsNullEncoding is intentionally broader than SQL nullability: besides
	// nullable columns, it also covers timestamp/datetime compatibility fallback
	// where invalid MySQL temporal values are encoded as NULL.
	allowsNullEncoding bool
	// timestampUnit caches TimestampLogicalType.TimeUnit() to avoid calling it
	// for every row.
	timestampUnit schema.TimeUnitType
}

func timestampUnitFromLogicalType(logicalType schema.LogicalType) schema.TimeUnitType {
	timestampLogicalType, ok := logicalType.(schema.TimestampLogicalType)
	if !ok {
		return schema.TimeUnitUnknown
	}
	return timestampLogicalType.TimeUnit()
}

type parsedColumnValue struct {
	value  any
	isNull bool
}

type countingWriter struct {
	writer       io.Writer
	writtenBytes int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.writer.Write(p)
	cw.writtenBytes += int64(n)
	return n, err
}

func (cw *countingWriter) Close() error {
	if closer, ok := cw.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// ParquetWriter writes SQL rows into a Parquet file using parquet/file.Writer.
type ParquetWriter struct {
	writer                   *file.Writer
	output                   *countingWriter
	columns                  []column
	buffers                  []columnBuffer
	rowGroupMemoryLimitBytes int64
	bufferedRows             int
	bufferedMemoryBytes      int64
	closed                   bool
}

type writerOptions struct {
	writerProperties         []parquet.WriterProperty
	rowGroupMemoryLimitBytes int64
}

// WriterOption configures parquet writer properties exposed by this package.
type WriterOption func(writerOptions) writerOptions

func defaultWriterOptions() writerOptions {
	return writerOptions{
		writerProperties: []parquet.WriterProperty{
			parquet.WithCompression(defaultCompressionType),
		},
		rowGroupMemoryLimitBytes: defaultRowGroupMemoryLimitBytes,
	}
}

// WithCompression sets parquet writer compression codec.
func WithCompression(codec compress.Compression) WriterOption {
	return func(options writerOptions) writerOptions {
		options.writerProperties = append(options.writerProperties, parquet.WithCompression(codec))
		return options
	}
}

// WithDataPageSize sets parquet writer data page size in bytes.
func WithDataPageSize(pageSize int64) WriterOption {
	return func(options writerOptions) writerOptions {
		options.writerProperties = append(options.writerProperties, parquet.WithDataPageSize(pageSize))
		return options
	}
}

// WithRowGroupMemoryLimit sets the row-group flush threshold by accounted
// in-memory bytes. Non-positive values are ignored and keep the default limit.
func WithRowGroupMemoryLimit(limitBytes int64) WriterOption {
	return func(options writerOptions) writerOptions {
		if limitBytes > 0 {
			options.rowGroupMemoryLimitBytes = limitBytes
		}
		return options
	}
}

// NewParquetWriter creates a Parquet writer for SQL result rows.
func NewParquetWriter(w io.Writer, columns []*ColumnInfo, options ...WriterOption) (*ParquetWriter, error) {
	if w == nil {
		return nil, fmt.Errorf("parquet output buffer is nil")
	}
	output := &countingWriter{writer: w}

	parquetSchema, parsedColumns, err := buildParquetSchemaFromColumns(columns)
	if err != nil {
		return nil, err
	}

	localOptions := newWriterOptions(options)
	props := parquet.NewWriterProperties(localOptions.writerProperties...)

	buffers, err := newColumnBuffers(parsedColumns, 0)
	if err != nil {
		return nil, err
	}

	return &ParquetWriter{
		writer:                   file.NewParquetWriter(output, parquetSchema, file.WithWriterProps(props)),
		output:                   output,
		columns:                  parsedColumns,
		buffers:                  buffers,
		rowGroupMemoryLimitBytes: localOptions.rowGroupMemoryLimitBytes,
	}, nil
}

func newWriterOptions(options []WriterOption) writerOptions {
	localOptions := defaultWriterOptions()
	for _, option := range options {
		if option != nil {
			localOptions = option(localOptions)
		}
	}
	return localOptions
}

// Write appends one row from SQL raw column bytes.
// Any write failure makes this writer unusable; callers should stop writing
// and close it.
func (pw *ParquetWriter) Write(src []sql.RawBytes) error {
	if pw.closed {
		return fmt.Errorf("parquet writer is closed")
	}
	if err := pw.parseAndAppendRow(src); err != nil {
		return err
	}
	if pw.rowGroupMemoryLimitBytes > 0 && pw.bufferedMemoryBytes >= pw.rowGroupMemoryLimitBytes {
		return pw.flushRows()
	}
	return nil
}

// Close flushes buffered rows and closes the Parquet writer.
func (pw *ParquetWriter) Close() error {
	if pw.closed {
		return nil
	}
	pw.closed = true
	flushErr := pw.flushRows()
	closeErr := pw.writer.Close()
	return errors.Join(flushErr, closeErr)
}

// EstimateFileSize returns an estimated final file size by summing bytes
// already flushed to the sink and bytes still buffered in memory.
func (pw *ParquetWriter) EstimateFileSize() uint64 {
	estimatedBytes := pw.totalWrittenBytes() + pw.bufferedMemoryBytes
	if estimatedBytes <= 0 {
		return 0
	}
	return uint64(estimatedBytes)
}

func (pw *ParquetWriter) totalWrittenBytes() int64 {
	if pw.output == nil {
		return 0
	}
	return pw.output.writtenBytes
}

func (pw *ParquetWriter) parseAndAppendRow(rawRow []sql.RawBytes) error {
	if len(rawRow) != len(pw.columns) {
		return fmt.Errorf("parquet row has %d values, expected %d", len(rawRow), len(pw.columns))
	}

	for i, rawValue := range rawRow {
		parsedValue, err := pw.parseColumnValue(i, rawValue)
		if err != nil {
			return fmt.Errorf("convert parquet column %s: %w", pw.columns[i].Name, err)
		}
		if err := pw.appendParsedColumnValue(i, parsedValue); err != nil {
			return fmt.Errorf("convert parquet column %s: %w", pw.columns[i].Name, err)
		}
	}

	pw.bufferedRows++
	return nil
}

func (pw *ParquetWriter) flushRows() error {
	if pw.bufferedRows == 0 {
		return nil
	}

	rowGroupWriter := pw.writer.AppendRowGroup()
	for i := range pw.columns {
		columnWriter, err := rowGroupWriter.NextColumn()
		if err != nil {
			return err
		}
		if err := writeColumnBatch(columnWriter, pw.columns[i], pw.buffers[i]); err != nil {
			_ = columnWriter.Close()
			return fmt.Errorf("write parquet column %s: %w", pw.columns[i].Name, err)
		}
		if err := columnWriter.Close(); err != nil {
			return fmt.Errorf("close parquet column %s: %w", pw.columns[i].Name, err)
		}
	}
	if err := rowGroupWriter.Close(); err != nil {
		return err
	}
	pw.bufferedRows = 0
	pw.bufferedMemoryBytes = 0
	for i := range pw.buffers {
		pw.buffers[i].reset()
	}
	return nil
}

func (pw *ParquetWriter) parseColumnValue(colIdx int, rawValue sql.RawBytes) (parsedColumnValue, error) {
	column := pw.columns[colIdx]
	if rawValue == nil {
		if !column.allowsNullEncoding {
			return parsedColumnValue{}, fmt.Errorf("required column receives NULL")
		}
		return parsedColumnValue{isNull: true}, nil
	}

	parsedValue, isNull, err := parseRawColumnValue(rawValue, column)
	if err != nil {
		return parsedColumnValue{}, err
	}
	return parsedColumnValue{value: parsedValue, isNull: isNull}, nil
}

func (pw *ParquetWriter) appendParsedColumnValue(colIdx int, parsedValue parsedColumnValue) error {
	column := pw.columns[colIdx]
	buffer := &pw.buffers[colIdx]

	if column.allowsNullEncoding {
		pw.bufferedMemoryBytes += definitionLevelMemoryBytes
		if parsedValue.isNull {
			buffer.defLevels = append(buffer.defLevels, 0)
			return nil
		}
		buffer.defLevels = append(buffer.defLevels, 1)
	}

	if parsedValue.isNull {
		return nil
	}
	if err := appendColumnValue(buffer, column, parsedValue.value); err != nil {
		return err
	}
	pw.bufferedMemoryBytes += accountColumnValueMemoryBytes(column, parsedValue.value)
	return nil
}
