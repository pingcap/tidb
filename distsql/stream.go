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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

// streamResult implements the SelectResult interface.
type streamResult struct {
	resp       kv.Response
	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	// NOTE: curr == nil means stream finish, while len(curr.RowsData) == 0 doesn't.
	curr         *tipb.Chunk
	partialCount int64
	feedback     *statistics.QueryFeedback
}

func (r *streamResult) Fetch(context.Context) {}

func (r *streamResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	for chk.NumRows() < maxChunkSize {
		err := r.readDataIfNecessary(ctx)
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
func (r *streamResult) readDataFromResponse(ctx context.Context, resp kv.Response, result *tipb.Chunk) (bool, error) {
	resultSubset, err := resp.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if resultSubset == nil {
		return true, nil
	}

	var stream tipb.StreamResponse
	err = stream.Unmarshal(resultSubset.GetData())
	if err != nil {
		return false, errors.Trace(err)
	}
	if stream.Error != nil {
		return false, errors.Errorf("stream response error: [%d]%s\n", stream.Error.Code, stream.Error.Msg)
	}
	for _, warning := range stream.Warnings {
		r.ctx.GetSessionVars().StmtCtx.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
	}

	err = result.Unmarshal(stream.Data)
	if err != nil {
		return false, errors.Trace(err)
	}
	r.feedback.Update(resultSubset.GetStartKey(), stream.OutputCounts)
	r.partialCount++
	return false, nil
}

// readDataIfNecessary ensures there are some data in current chunk. If no more data, r.curr == nil.
func (r *streamResult) readDataIfNecessary(ctx context.Context) error {
	if r.curr != nil && len(r.curr.RowsData) > 0 {
		return nil
	}

	tmp := new(tipb.Chunk)
	finish, err := r.readDataFromResponse(ctx, r.resp, tmp)
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
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for chk.NumRows() < maxChunkSize && len(remainRowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			remainRowsData, err = decoder.DecodeOne(remainRowsData, i, r.fieldTypes[i])
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

func (r *streamResult) NextRaw(ctx context.Context) ([]byte, error) {
	r.partialCount++
	r.feedback.Invalidate()
	resultSubset, err := r.resp.Next(ctx)
	if resultSubset == nil || err != nil {
		return nil, errors.Trace(err)
	}
	return resultSubset.GetData(), errors.Trace(err)
}

func (r *streamResult) Close() error {
	if r.feedback.Actual() > 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	return nil
}
