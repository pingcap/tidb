// Copyright 2022 PingCAP, Inc.
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

// Fields implements the sqlexec.RecordSet interface.
func (r *SimpleRecordSet) Fields() []*ast.ResultField {
	return r.ResultFields
}

// Next implements the sqlexec.RecordSet interface.
func (r *SimpleRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for r.idx < len(r.Rows) {
		if req.IsFull() {
			return nil
		}
		for i := range r.ResultFields {
			datum := types.NewDatum(r.Rows[r.idx][i])
			req.AppendDatum(i, &datum)
		}
		r.idx++
	}
	return nil
}

// NewChunk implements the sqlexec.RecordSet interface.
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

// Close implements the sqlexec.RecordSet interface.
func (r *SimpleRecordSet) Close() error {
	r.idx = 0
	return nil
}
