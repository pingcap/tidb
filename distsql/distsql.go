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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/goroutine_pool"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	errInvalidResp = terror.ClassXEval.New(codeInvalidResp, "invalid response")
	selectResultGP = gp.New(time.Minute * 2)
)

var (
	_ SelectResult  = &selectResult{}
	_ PartialResult = &partialResult{}
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Next gets the next partial result.
	Next(goctx.Context) (PartialResult, error)
	// NextRaw gets the next raw result.
	NextRaw(goctx.Context) ([]byte, error)
	// NextChunk reads the data into chunk.
	NextChunk(goctx.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(goctx.Context)
	// ScanCount gets the total scan row count.
	ScanCount() int64
}

// PartialResult is the result from a single region server.
type PartialResult interface {
	// Next returns the next rowData of the sub result.
	// If no more row to return, rowData would be nil.
	Next(goctx.Context) (rowData []types.Datum, err error)
	// Close closes the partial result.
	Close() error
}

type selectResult struct {
	label     string
	aggregate bool
	resp      kv.Response

	results chan newResultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        context.Context

	selectResp *tipb.SelectResponse
	respChkIdx int

	scanCount int64
}

type newResultWithErr struct {
	result []byte
	err    error
}

func (r *selectResult) Fetch(ctx goctx.Context) {
	selectResultGP.Go(func() {
		r.fetch(ctx)
	})
}

func (r *selectResult) fetch(goCtx goctx.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		queryHistgram.WithLabelValues(r.label).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next(goCtx)
		if err != nil {
			r.results <- newResultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}

		select {
		case r.results <- newResultWithErr{result: resultSubset}:
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-goCtx.Done():
			return
		}
	}
}

// Next returns the next row.
func (r *selectResult) Next(goCtx goctx.Context) (PartialResult, error) {
	re := <-r.results
	if re.err != nil {
		return nil, errors.Trace(re.err)
	}
	if re.result == nil {
		return nil, nil
	}
	pr := &partialResult{}
	pr.rowLen = r.rowLen
	err := pr.unmarshal(re.result)
	if len(pr.resp.OutputCounts) > 0 {
		r.scanCount += pr.resp.OutputCounts[0]
	} else {
		r.scanCount = -1
	}
	return pr, errors.Trace(err)
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(goCtx goctx.Context) ([]byte, error) {
	re := <-r.results
	return re.result, errors.Trace(re.err)
}

// NextChunk reads data to the chunk.
func (r *selectResult) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.getSelectResp()
			if err != nil || r.selectResp == nil {
				return errors.Trace(err)
			}
		}
		err := r.readRowsData(chk)
		if err != nil {
			return errors.Trace(err)
		}
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) getSelectResp() error {
	r.respChkIdx = 0
	for {
		re := <-r.results
		if re.err != nil {
			return errors.Trace(re.err)
		}
		if re.result == nil {
			r.selectResp = nil
			return nil
		}
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result)
		if err != nil {
			return errors.Trace(err)
		}
		if len(r.selectResp.OutputCounts) > 0 {
			r.scanCount += r.selectResp.OutputCounts[0]
		} else {
			r.scanCount = -1
		}
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

func (r *selectResult) ScanCount() int64 {
	return r.scanCount
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
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
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	// Close this channel tell fetch goroutine to exit.
	close(r.closed)
	return r.resp.Close()
}

type partialResult struct {
	resp     *tipb.SelectResponse
	chunkIdx int
	rowLen   int
}

func (pr *partialResult) unmarshal(resultSubset []byte) error {
	pr.resp = new(tipb.SelectResponse)
	err := pr.resp.Unmarshal(resultSubset)
	if err != nil {
		return errors.Trace(err)
	}

	if pr.resp.Error != nil {
		return errInvalidResp.Gen("[%d %s]", pr.resp.Error.GetCode(), pr.resp.Error.GetMsg())
	}

	return nil
}

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (pr *partialResult) Next(goCtx goctx.Context) (data []types.Datum, err error) {
	nextChunk := pr.getChunk()
	if nextChunk == nil {
		return nil, nil
	}
	return readRowFromChunk(nextChunk, pr.rowLen)
}

func readRowFromChunk(chunk *tipb.Chunk, numCols int) (row []types.Datum, err error) {
	row = make([]types.Datum, numCols)
	for i := 0; i < numCols; i++ {
		var raw []byte
		raw, chunk.RowsData, err = codec.CutOne(chunk.RowsData)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row[i].SetRaw(raw)
	}
	return
}

func (pr *partialResult) getChunk() *tipb.Chunk {
	for {
		if pr.chunkIdx >= len(pr.resp.Chunks) {
			return nil
		}
		currentChunk := &pr.resp.Chunks[pr.chunkIdx]
		if len(currentChunk.RowsData) > 0 {
			return currentChunk
		}
		pr.chunkIdx++
	}
}

// Close closes the sub result.
func (pr *partialResult) Close() error {
	return nil
}

// SelectDAG sends a DAG request, returns SelectResult.
// In kvReq, KeyRanges is required, Concurrency/KeepOrder/Desc/IsolationLevel/Priority are optional.
func SelectDAG(goCtx goctx.Context, ctx context.Context, kvReq *kv.Request, fieldTypes []*types.FieldType) (SelectResult, error) {
	var err error
	defer func() {
		// Add metrics.
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	resp := ctx.GetClient().Send(goCtx, kvReq)
	if resp == nil {
		err = errors.New("client returns nil response")
		return nil, errors.Trace(err)
	}

	if kvReq.Streaming {
		return &streamResult{
			resp:       resp,
			rowLen:     len(fieldTypes),
			fieldTypes: fieldTypes,
			ctx:        ctx,
		}, nil
	}

	return &selectResult{
		label:      "dag",
		resp:       resp,
		results:    make(chan newResultWithErr, kvReq.Concurrency),
		closed:     make(chan struct{}),
		rowLen:     len(fieldTypes),
		fieldTypes: fieldTypes,
		ctx:        ctx,
	}, nil
}

// Analyze do a analyze request.
func Analyze(ctx goctx.Context, client kv.Client, kvReq *kv.Request) (SelectResult, error) {
	var err error
	defer func() {
		// Add metrics.
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		label:   "analyze",
		resp:    resp,
		results: make(chan newResultWithErr, kvReq.Concurrency),
		closed:  make(chan struct{}),
	}
	return result, nil
}

// XAPI error codes.
const (
	codeInvalidResp = 1
)

// FieldTypeFromPBColumn creates a types.FieldType from tipb.ColumnInfo.
func FieldTypeFromPBColumn(col *tipb.ColumnInfo) *types.FieldType {
	return &types.FieldType{
		Tp:      byte(col.GetTp()),
		Flag:    uint(col.Flag),
		Flen:    int(col.GetColumnLen()),
		Decimal: int(col.GetDecimal()),
		Elems:   col.Elems,
		Collate: mysql.Collations[uint8(col.GetCollation())],
	}
}

func columnToProto(c *model.ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  c.ID,
		Collation: collationToProto(c.FieldType.Collate),
		ColumnLen: int32(c.FieldType.Flen),
		Decimal:   int32(c.FieldType.Decimal),
		Flag:      int32(c.Flag),
		Elems:     c.Elems,
	}
	pc.Tp = int32(c.FieldType.Tp)
	return pc
}

// TODO: update it when more collate is supported.
func collationToProto(c string) int32 {
	v := mysql.CollationNames[c]
	if v == mysql.BinaryCollationID {
		return int32(mysql.BinaryCollationID)
	}
	// We only support binary and utf8_bin collation.
	// Setting other collations to utf8_bin for old data compatibility.
	// For the data created when we didn't enforce utf8_bin collation in create table.
	return int32(mysql.DefaultCollationID)
}

// ColumnsToProto converts a slice of model.ColumnInfo to a slice of tipb.ColumnInfo.
func ColumnsToProto(columns []*model.ColumnInfo, pkIsHandle bool) []*tipb.ColumnInfo {
	cols := make([]*tipb.ColumnInfo, 0, len(columns))
	for _, c := range columns {
		col := columnToProto(c)
		// TODO: Here `PkHandle`'s meaning is changed, we will change it to `IsHandle` when tikv's old select logic
		// is abandoned.
		if (pkIsHandle && mysql.HasPriKeyFlag(c.Flag)) || c.ID == model.ExtraHandleID {
			col.PkHandle = true
		} else {
			col.PkHandle = false
		}
		cols = append(cols, col)
	}
	return cols
}

// IndexToProto converts a model.IndexInfo to a tipb.IndexInfo.
func IndexToProto(t *model.TableInfo, idx *model.IndexInfo) *tipb.IndexInfo {
	pi := &tipb.IndexInfo{
		TableId: t.ID,
		IndexId: idx.ID,
		Unique:  idx.Unique,
	}
	cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns)+1)
	for _, c := range idx.Columns {
		cols = append(cols, columnToProto(t.Columns[c.Offset]))
	}
	if t.PKIsHandle {
		// Coprocessor needs to know PKHandle column info, so we need to append it.
		for _, col := range t.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				colPB := columnToProto(col)
				colPB.PkHandle = true
				cols = append(cols, colPB)
				break
			}
		}
	}
	pi.Columns = cols
	return pi
}
