// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// streamResult implements the SelectResult interface's NextChunk method.
type streamResult struct {
	resp       kv.Response
	rowLen     int
	fieldTypes []*types.FieldType
	ctx        context.Context

	chunk *tipb.Chunk
}

func (r *streamResult) Fetch(goctx.Context) {}

func (r *streamResult) Next(goctx.Context) (PartialResult, error) {
	data, err := r.resp.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ret tipbChunk
	if err := ret.Chunk.Unmarshal(data); err != nil {
		return nil, errors.Trace(err)
	}
	ret.rowLen = r.rowLen
	return &ret, nil
}

func (r *streamResult) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		if err := r.takeData(); err != nil {
			return errors.Trace(err)
		}
		if r.chunk == nil {
			// No more data.
			return nil
		}

		err := r.readRowsData(chk)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// takeData returns current chunk if it's not consumed, try to get the next chunk otherwise.
func (r *streamResult) takeData() error {
	if r.chunk != nil {
		return nil
	}

	data, err := r.resp.Next()
	if err != nil {
		return errors.Trace(err)
	}
	if len(data) == 0 {
		return nil
	}

	tmp := new(tipb.Chunk)
	err = tmp.Unmarshal(data)
	if err != nil {
		return errors.Trace(err)
	}
	r.chunk = tmp
	return nil
}

func (r *streamResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.chunk.RowsData
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	timeZone := r.ctx.GetSessionVars().GetTimeZone()
	for chk.NumRows() < maxChunkSize && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = codec.DecodeOneToChunk(rowsData, chk, i, r.fieldTypes[i], timeZone)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	r.chunk.RowsData = rowsData
	if len(rowsData) == 0 {
		r.chunk = nil
	}
	return nil
}

func (r *streamResult) NextRaw() ([]byte, error) {
	return r.resp.Next()
}

func (r *streamResult) Close() error {
	return nil
}

// tipbChunk implements PartialResult.
type tipbChunk struct {
	tipb.Chunk
	rowLen int
}

func (pr *tipbChunk) Next(goCtx goctx.Context) (data []types.Datum, err error) {
	return readRowFromChunk(&pr.Chunk, pr.rowLen)
}

func (pr *tipbChunk) Close() error {
	return nil
}
