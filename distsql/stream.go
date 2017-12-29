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

func (r *streamResult) Next(goctx.Context) (PartialResult, error) {
	var ret streamPartialResult
	ret.rowLen = r.rowLen
	finish, err := r.readDataFromResponse(r.resp, &ret.Chunk)
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
func (r *streamResult) readDataFromResponse(resp kv.Response, result *tipb.Chunk) (bool, error) {
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

// takeData ensures there're data is in current chunk. If no more data, return false.
func (r *streamResult) takeData() error {
	if r.curr != nil && len(r.curr.RowsData) > 0 {
		return nil
	}

	tmp := new(tipb.Chunk)
	finish, err := r.readDataFromResponse(r.resp, tmp)
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
	r.curr.RowsData = remainRowsData
	if len(remainRowsData) == 0 {
		r.curr = nil // Current chunk is finished.
	}
	return nil
}

func (r *streamResult) NextRaw() ([]byte, error) {
	return r.resp.Next()
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
