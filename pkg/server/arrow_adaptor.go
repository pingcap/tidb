// Copyright 2025 PingCAP, Inc.
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

//go:build flightsql

package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

var (
	// ErrUnsupportedType indicates that a MySQL type cannot be converted to Arrow type
	ErrUnsupportedType = errors.New("unsupported MySQL type for Arrow conversion")
)

// ResultSetRecordReader reads from a ResultSet and retuns
// a simple RecordReader for each chunk.
type ResultSetRecordReader struct {
	set sqlexec.RecordSet

	schema         *arrow.Schema
	tidbAllocator  chunk.Allocator
	arrowAllocator memory.Allocator
	builder        *array.RecordBuilder
	cur            *chunk.Chunk
	err            error
}

func NewResultSetRecordReader(set sqlexec.RecordSet, tidbAllocator chunk.Allocator, arrowAllocator memory.Allocator) (*ResultSetRecordReader, error) {
	schema, err := adaptSchema(set.Fields())
	if err != nil {
		return nil, err
	}
	if tidbAllocator == nil {
		tidbAllocator = chunk.NewAllocator()
	}
	if arrowAllocator == nil {
		arrowAllocator = memory.NewGoAllocator()
	}
	builder := array.NewRecordBuilder(arrowAllocator, schema)
	return &ResultSetRecordReader{
		set:            set,
		schema:         schema,
		tidbAllocator:  tidbAllocator,
		arrowAllocator: arrowAllocator,
		builder:        builder,
	}, nil
}

func (r *ResultSetRecordReader) Retain() {
	// NOP
}

func (r *ResultSetRecordReader) Release() {
	if r.cur != nil {
		r.cur.Reset()
		r.cur = nil
	}
	if r.builder != nil {
		r.builder.Release()
		r.builder = nil
	}
	r.err = r.set.Close()
}

func (r *ResultSetRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *ResultSetRecordReader) Next() bool {
	if r.cur == nil {
		r.cur = r.set.NewChunk(r.tidbAllocator)
	} else {
		// Reset the chunk for reuse
		r.cur.Reset()
	}

	r.err = r.set.Next(context.Background(), r.cur)

	return r.cur.NumRows() > 0
}

func (r *ResultSetRecordReader) Record() arrow.Record {
	// TODO: Optimize memory allocation
	// Current approach copies data from TiDB chunk to Arrow builders.
	// Potential optimizations:
	// 1. Reuse builders across multiple Record() calls
	// 2. Create Arrow arrays directly from chunk memory when possible
	// 3. Use zero-copy techniques for fixed-width types

	// Extract field types from the result set
	fieldTypes := make([]*types.FieldType, len(r.set.Fields()))
	for i, field := range r.set.Fields() {
		fieldTypes[i] = &field.Column.FieldType
	}

	// Use shared conversion logic
	convertChunkToRecord(r.cur, fieldTypes, r.builder)

	return r.builder.NewRecord()
}

func (r *ResultSetRecordReader) Err() error {
	return r.err
}

func adaptSchema(fields []*resolve.ResultField) (*arrow.Schema, error) {
	arrowFields := []arrow.Field{}

	for _, field := range fields {
		typ, err := adaptFieldType(&field.Column.FieldType)
		if err != nil {
			return nil, err
		}
		arrowField := arrow.Field{
			Name:     field.Column.Name.String(),
			Type:     typ,
			Nullable: true,
		}
		arrowFields = append(arrowFields, arrowField)
	}
	schema := arrow.NewSchema(arrowFields, nil)

	return schema, nil
}

// convertChunkToRecord converts a TiDB chunk to an Arrow record using the provided builder.
// This shared logic is used by both ResultSetRecordReader and prepared statement execution.
func convertChunkToRecord(chk *chunk.Chunk, fieldTypes []*types.FieldType, builder *array.RecordBuilder) {
	numCols := chk.NumCols()
	numRows := chk.NumRows()

	for colIdx := 0; colIdx < numCols; colIdx++ {
		col := chk.Column(colIdx)
		ft := fieldTypes[colIdx]

		dest := builder.Field(colIdx)
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			// Check for NULL values first
			if col.IsNull(rowIdx) {
				dest.AppendNull()
				continue
			}

			switch ft.GetType() {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
				// Always convert to Int64 for compatibility with FlightSQL driver
				if mysql.HasUnsignedFlag(ft.GetFlag()) {
					val := col.GetUint64(rowIdx)
					// Convert uint64 to int64 (may overflow for very large values)
					dest.(*array.Int64Builder).Append(int64(val))
				} else {
					val := col.GetInt64(rowIdx)
					dest.(*array.Int64Builder).Append(val)
				}

			case mysql.TypeFloat:
				val := col.GetFloat32(rowIdx)
				dest.(*array.Float32Builder).Append(val)

			case mysql.TypeDouble:
				val := col.GetFloat64(rowIdx)
				dest.(*array.Float64Builder).Append(val)

			case mysql.TypeNewDecimal:
				dec := col.GetDecimal(rowIdx)
				// Convert MyDecimal to float64 for now
				// TODO: Use Arrow Decimal128 for better precision
				f64, _ := dec.ToFloat64()
				dest.(*array.Float64Builder).Append(f64)

			case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
				t := col.GetTime(rowIdx)
				// Convert to Unix milliseconds (Date64 in Arrow)
				// Use time.UTC as the location
				goTime, err := t.GoTime(time.UTC)
				if err != nil {
					// Handle invalid time by using zero time
					goTime = time.Time{}.UTC()
				}
				unixMilli := goTime.UnixMilli()
				dest.(*array.Date64Builder).Append(arrow.Date64(unixMilli))

			case mysql.TypeDuration:
				dur := col.GetDuration(rowIdx, 0)
				// Store duration as int64 nanoseconds
				dest.(*array.Int64Builder).Append(int64(dur.Duration))

			case mysql.TypeJSON:
				j := col.GetJSON(rowIdx)
				// Convert JSON to string representation
				dest.(*array.StringBuilder).Append(j.String())

			case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
				val := col.GetString(rowIdx)
				dest.(*array.StringBuilder).Append(val)

			case mysql.TypeBit:
				val := col.GetInt64(rowIdx)
				dest.(*array.Int64Builder).Append(val)

			default:
				// For unsupported types, convert to string representation
				val := col.GetString(rowIdx)
				dest.(*array.StringBuilder).Append(val)
			}
		}
	}
}

func adaptFieldType(ft *types.FieldType) (arrow.DataType, error) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		// Always use Int64 for integer types, even if unsigned
		// FlightSQL driver has better compatibility with signed types
		// TODO: Consider using Uint64 when driver support improves
		return arrow.PrimitiveTypes.Int64, nil

	case mysql.TypeFloat:
		return arrow.PrimitiveTypes.Float32, nil

	case mysql.TypeDouble:
		return arrow.PrimitiveTypes.Float64, nil

	case mysql.TypeNewDecimal:
		// Using Float64 for now for simplicity
		// TODO: Use arrow.Decimal128Type for better precision
		return arrow.PrimitiveTypes.Float64, nil

	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		// Date64 represents milliseconds since Unix epoch
		return arrow.FixedWidthTypes.Date64, nil

	case mysql.TypeDuration:
		// Store duration as int64 nanoseconds
		return arrow.PrimitiveTypes.Int64, nil

	case mysql.TypeJSON:
		// JSON stored as string
		return arrow.BinaryTypes.String, nil

	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		return arrow.BinaryTypes.String, nil

	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		// Binary data stored as string (can handle binary safely)
		return arrow.BinaryTypes.String, nil

	case mysql.TypeBit:
		return arrow.PrimitiveTypes.Int64, nil

	case mysql.TypeEnum, mysql.TypeSet:
		// Store as string
		return arrow.BinaryTypes.String, nil

	default:
		return nil, fmt.Errorf("%w: MySQL type %v", ErrUnsupportedType, ft.GetType())
	}
}
