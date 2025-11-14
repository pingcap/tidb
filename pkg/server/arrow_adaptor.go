package server

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
	r.cur.Reset()
	r.cur = nil
	r.err = r.set.Close()
}

func (r *ResultSetRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *ResultSetRecordReader) Next() bool {
	if r.cur == nil {
		r.cur = r.set.NewChunk(r.tidbAllocator)
	}

	r.err = r.set.Next(context.Background(), r.cur)

	return r.cur.NumRows() > 0
}

func (r *ResultSetRecordReader) Record() arrow.Record {
	// Quick & dirty copy logic for MVP
	// Should be rewritten to avoid double allocation
	numCols := r.cur.NumCols()
	numRows := r.cur.NumRows()

	for colIdx := 0; colIdx < numCols; colIdx++ {
		col := r.cur.Column(colIdx)
		colInfo := r.set.Fields()[colIdx].Column
		ft := colInfo.FieldType

		dest := r.builder.Field(colIdx)
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			switch ft.GetType() {
			case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
				if mysql.HasUnsignedFlag(ft.GetFlag()) {
					val := col.GetUint64(rowIdx)
					dest.(*array.Uint64Builder).Append(val)
				}
				val := col.GetInt64(rowIdx)
				dest.(*array.Int64Builder).Append(val)

			case mysql.TypeFloat:
				val := col.GetFloat32(rowIdx)
				dest.(*array.Float32Builder).Append(val)

			case mysql.TypeDouble:
				val := col.GetFloat64(rowIdx)
				dest.(*array.Float64Builder).Append(val)

			case mysql.TypeVarString, mysql.TypeString:
				val := col.GetString(rowIdx)
				dest.(*array.StringBuilder).Append(val)
			}
		}
	}

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

func adaptFieldType(ft *types.FieldType) (arrow.DataType, error) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return arrow.PrimitiveTypes.Uint64, nil
		}
		return arrow.PrimitiveTypes.Int64, nil

	case mysql.TypeFloat:
		return arrow.PrimitiveTypes.Float32, nil

	case mysql.TypeDouble:
		return arrow.PrimitiveTypes.Float64, nil

	// case mysql.TypeNewDecimal:
	// 	return arrow.PrimitiveTypes.Float64, nil

	// case mysql.TypeDate, mysql.TypeDatetime:
	// 	return arrow.PrimitiveTypes.Date64, nil

	case mysql.TypeVarString, mysql.TypeString:
		return arrow.BinaryTypes.LargeString, nil
	}

	return nil, errors.ErrUnsupported
}
