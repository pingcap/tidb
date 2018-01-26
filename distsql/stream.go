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

	// NOTE: curr == nil means stream finish, while len(curr.RowsData) == 0 doesn't.
	curr      *tipb.Chunk
	scanCount int64
}

func (r *streamResult) ScanCount() int64 {
	return r.scanCount
}

func (r *streamResult) Fetch(goctx.Context) {}

func (r *streamResult) Next(goCtx goctx.Context) (PartialResult, error) {
	var ret streamPartialResult
	ret.rowLen = r.rowLen
	finished, err := r.readDataFromResponse(goCtx, r.resp, &ret.Chunk)
	if err != nil || finished {
		return nil, errors.Trace(err)
	}
	return &ret, nil
}

func (r *streamResult) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	for chk.NumRows() < maxChunkSize {
		err := r.readDataIfNecessary(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if r.curr == nil {
			return nil
		}

		err = r.flushToChunk(chk)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// readDataFromResponse read the data to result. Returns true means the resp is finished.
func (r *streamResult) readDataFromResponse(goCtx goctx.Context, resp kv.Response, result *tipb.Chunk) (bool, error) {
	data, err := resp.Next(goCtx)
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

	// TODO: Check stream.GetEncodeType() here if we support tipb.EncodeType_TypeArrow some day.

	err = result.Unmarshal(stream.Data)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(stream.OutputCounts) > 0 {
		r.scanCount += stream.OutputCounts[0]
	}
	return false, nil
}

// readDataIfNecessary ensures there are some data in current chunk. If no more data, r.curr == nil.
func (r *streamResult) readDataIfNecessary(goCtx goctx.Context) error {
	if r.curr != nil && len(r.curr.RowsData) > 0 {
		return nil
	}

	tmp := new(tipb.Chunk)
	finish, err := r.readDataFromResponse(goCtx, r.resp, tmp)
	if err != nil {
		return errors.Trace(err)
	}
	if finish {
		r.curr = nil
		return nil
	}
	r.curr = tmp
	return nil
}

func (r *streamResult) flushToChunk(chk *chunk.Chunk) (err error) {
	remainRowsData := r.curr.RowsData
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	timeZone := r.ctx.GetSessionVars().GetTimeZone()
	for chk.NumRows() < maxChunkSize && len(remainRowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			remainRowsData, err = codec.DecodeOneToChunk(remainRowsData, chk, i, r.fieldTypes[i], timeZone)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	if len(remainRowsData) == 0 {
		r.curr = nil // Current chunk is finished.
	} else {
		r.curr.RowsData = remainRowsData
	}
	return nil
}

func (r *streamResult) NextRaw(goCtx goctx.Context) ([]byte, error) {
	return r.resp.Next(goCtx)
}

func (r *streamResult) Close() error {
	return nil
}

// streamPartialResult implements PartialResult.
type streamPartialResult struct {
	tipb.Chunk
	rowLen int
}

func (pr *streamPartialResult) Next(goCtx goctx.Context) (data []types.Datum, err error) {
	if len(pr.Chunk.RowsData) == 0 {
		return nil, nil // partial result finished.
	}
	return readRowFromChunk(&pr.Chunk, pr.rowLen)
}

func (pr *streamPartialResult) Close() error {
	return nil
}
