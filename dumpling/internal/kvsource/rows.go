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

package kvsource

import (
	"context"
	"database/sql"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/dumpling/dumpservice"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
)

// NewRows opens the first range and prefetches its first row. Check Error before use.
func NewRows(
	ctx context.Context,
	scanner Scanner,
	table *model.TableInfo,
	columns []*model.ColumnInfo,
	ranges []dumpservice.Range,
) *Rows {
	iter := &Rows{
		ctx:     ctx,
		scanner: scanner,
		ranges:  ranges,
		table:   table,
		decoder: newRowDecoder(table, columns),
		encoder: textrow.NewResultEncoder(charset.CharsetUTF8MB4),
	}
	iter.readNext()
	return iter
}

// Rows scans table ranges and decodes their records using UTC and utf8mb4.
type Rows struct {
	ctx       context.Context
	scanner   Scanner
	ranges    []dumpservice.Range
	nextRange int
	scan      *dumpservice.Scan
	table     *model.TableInfo
	decoder   *rowDecoder
	encoder   *textrow.ResultEncoder
	key       []byte
	value     []byte
	err       error
	hasRow    bool
}

// HasNext reports whether a current row is available.
func (i *Rows) HasNext() bool { return i.err == nil && i.hasRow }

// Decode writes the current row as MySQL text into one *sql.RawBytes per column.
func (i *Rows) Decode(args []any) error {
	if !i.HasNext() {
		return errors.New("KV row iterator has no current row")
	}
	values, err := i.decoder.decode(i.table, i.key, i.value)
	if err != nil {
		return err
	}
	if values.Len() != len(args) {
		return errors.Errorf("KV row has %d values for %d columns", values.Len(), len(args))
	}
	for index, column := range i.decoder.columns {
		destination, ok := args[index].(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling kv row receiver %T", args[index])
		}
		if values.IsNull(index) {
			*destination = nil
			continue
		}
		// Format values with the same text serializer as the server so the KV
		// dump matches the SQL dump, for example MySQL's float formatting.
		columnInfo := textrow.ColumnInfo{
			Table:   i.table.Name.O,
			Charset: uint16(mysql.CharsetNameToID(column.GetCharset())),
			Flag:    uint16(column.GetFlag()),
			Decimal: uint8(column.GetDecimal()),
			Type:    column.GetType(),
		}
		data, err := textrow.FormatValueText(values, index, columnInfo, i.encoder)
		if err != nil {
			return errors.Annotatef(err, "format KV column %d", index)
		}
		*destination = append((*destination)[:0], data...)
		if len(data) == 0 {
			*destination = sql.RawBytes{}
		}
	}
	return nil
}

// Next advances to the next record, opening the next range when needed.
func (i *Rows) Next() {
	if i.HasNext() {
		i.readNext()
	}
}

// Error returns the first scan error.
func (i *Rows) Error() error { return i.err }

// Close releases the current scan, if any.
func (i *Rows) Close() error {
	if i.scan == nil {
		return nil
	}
	scan := i.scan
	i.scan = nil
	return scan.Close()
}

func (i *Rows) readNext() {
	for {
		if i.scan == nil {
			if i.nextRange == len(i.ranges) {
				i.hasRow = false
				return
			}
			rangeToScan := i.ranges[i.nextRange]
			i.nextRange++
			scan, err := i.scanner.Scan(
				i.ctx,
				rangeToScan.Start,
				rangeToScan.End,
			)
			if err != nil {
				i.err = err
				i.hasRow = false
				return
			}
			i.scan = scan
		}

		key, value, end, err := i.scan.ReadRow(i.key, i.value)
		if err != nil {
			i.err = err
			i.hasRow = false
			i.scan = nil
			return
		}
		if end {
			i.scan = nil
			continue
		}
		i.key = key
		i.value = value
		i.hasRow = true
		return
	}
}

type rowDecoder struct {
	columns []*model.ColumnInfo
	ctx     *mock.Context
	schema  *expression.Schema
	decoder *rowcodec.ChunkDecoder
	chunk   *chunk.Chunk
}

func newRowDecoder(table *model.TableInfo, columns []*model.ColumnInfo) *rowDecoder {
	schema := expression.NewSchema()
	fieldTypes := make([]*types.FieldType, 0, len(columns))
	for _, column := range columns {
		schema.Append(&expression.Column{ID: column.ID, RetType: &column.FieldType})
		fieldTypes = append(fieldTypes, &column.FieldType)
	}
	ctx := mock.NewContextDeprecated()
	ctx.ResetSessionAndStmtTimeZone(time.UTC)
	return &rowDecoder{
		columns: columns,
		ctx:     ctx,
		schema:  schema,
		decoder: executor.NewRowDecoder(ctx, schema, table),
		chunk:   chunk.New(fieldTypes, 1, 1),
	}
}

func (d *rowDecoder) decode(table *model.TableInfo, key, value []byte) (chunk.Row, error) {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return chunk.Row{}, errors.Annotatef(err, "decode KV row key %x", key)
	}
	d.chunk.Reset()
	if err := executor.DecodeRowValToChunk(d.ctx, d.schema, table, handle, value, d.chunk, d.decoder); err != nil {
		return chunk.Row{}, errors.Annotatef(err, "decode KV row value at key %x", key)
	}
	return d.chunk.GetRow(0), nil
}
