package sqlexec

import (
	"context"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// SimpleRecordSet is a simple implementation of RecordSet. All values are known when creating SimpleRecordSet.
type SimpleRecordSet struct {
	ResultFields []*ast.ResultField
	Rows         [][]interface{}
	MaxChunkSize int
	idx          int
}

func (r *SimpleRecordSet) Fields() []*ast.ResultField {
	return r.ResultFields
}

func (r *SimpleRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for r.idx < len(r.Rows) {
		if req.IsFull() {
			return nil
		}
		for i, _ := range r.ResultFields {
			datum := types.NewDatum(r.Rows[r.idx][i])
			req.AppendDatum(i, &datum)
		}
		r.idx++
	}
	return nil
}

func (r *SimpleRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(r.ResultFields))
	for _, field := range r.ResultFields {
		fields = append(fields, &field.Column.FieldType)
	}
	if alloc != nil {
		return alloc.Alloc(fields, 0, r.MaxChunkSize)
	}
	return chunk.New(fields, r.MaxChunkSize, r.MaxChunkSize)
}

func (r *SimpleRecordSet) Close() error {
	r.idx = 0
	return nil
}
