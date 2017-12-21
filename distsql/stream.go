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

// streamResult implements the SelectResult interface.
type streamResult struct {
	resp       kv.Response
	rowLen     int
	fieldTypes []*types.FieldType
	ctx        context.Context

	// NOTE: chunk == nil means finish, while len(chunk.RowsData) == 0 doesn't.
	chunk *tipb.Chunk
}

func (r *streamResult) Fetch(goctx.Context) {}

func (r *streamResult) Next(goctx.Context) (PartialResult, error) {
	var ret tipbChunk
	ret.rowLen = r.rowLen
	finish, err := readChunkFromResponse(r.resp, &ret.Chunk)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if finish {
		return nil, nil
	}
	return &ret, nil
}

func (r *streamResult) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		err := r.takeData()
		if err != nil {
			return errors.Trace(err)
		}
		if r.chunk == nil {
			return nil
		}

		err = r.readRowsData(chk)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// readDefaultFromResponse read the data to chunk. Returns true means the resp is finished.
func readChunkFromResponse(resp kv.Response, chunk *tipb.Chunk) (bool, error) {
	data, err := resp.Next()
	if err != nil {
		return false, errors.Trace(err)
	}
	if data == nil {
		return true, nil
	}

	var stream tipb.StreamResponse
	err = stream.Unmarshal(data)
	if err != nil {
		return false, errors.Trace(err)
	}
	if stream.Error != nil {
		return false, errors.Errorf("stream response error: [%d]%s\n", stream.Error.Code, stream.Error.Msg)
	}

	switch stream.GetEncodeType() {
	case tipb.EncodeType_TypeDefault:
	case tipb.EncodeType_TypeArrow:
		return false, errors.New("not implement yet")
	}

	err = chunk.Unmarshal(stream.Data)
	if err != nil {
		return false, errors.Trace(err)
	}
	return false, nil
}

// takeData ensure there're data is in current chunk. if no more data, return false.
func (r *streamResult) takeData() error {
	if r.chunk != nil && len(r.chunk.RowsData) > 0 {
		return nil
	}

	tmp := new(tipb.Chunk)
	finish, err := readChunkFromResponse(r.resp, tmp)
	if err != nil {
		return errors.Trace(err)
	}
	if finish {
		r.chunk = nil
		return nil
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
		r.chunk = nil // Current chunk is finished.
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
	if len(pr.Chunk.RowsData) == 0 {
		return nil, nil // partial result finished.
	}
	return readRowFromChunk(&pr.Chunk, pr.rowLen)
}

func (pr *tipbChunk) Close() error {
	return nil
}
