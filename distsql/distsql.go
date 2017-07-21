// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	errInvalidResp = terror.ClassXEval.New(codeInvalidResp, "invalid response")
)

var (
	_ SelectResult  = &selectResult{}
	_ PartialResult = &partialResult{}
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Next gets the next partial result.
	Next() (PartialResult, error)
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(ctx goctx.Context)
}

// PartialResult is the result from a single region server.
type PartialResult interface {
	// Next returns the next rowData of the sub result.
	// If no more row to return, rowData would be nil.
	Next() (handle int64, rowData []byte, err error)
	// Close closes the partial result.
	Close() error
}

// SelectResult is used to get response rows from SelectRequest.
type selectResult struct {
	label     string
	aggregate bool
	resp      kv.Response

	results chan resultWithErr
	closed  chan struct{}
}

type resultWithErr struct {
	result PartialResult
	err    error
}

func (r *selectResult) Fetch(ctx goctx.Context) {
	go r.fetch(ctx)
}

func (r *selectResult) fetch(ctx goctx.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		queryHistgram.WithLabelValues(r.label).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next()
		if err != nil {
			r.results <- resultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}
		pr := &partialResult{}
		pr.unmarshal(resultSubset)

		select {
		case r.results <- resultWithErr{result: pr}:
		case <-r.closed:
			// if selectResult called Close() already, make fetch goroutine exit
			return
		case <-ctx.Done():
			return
		}
	}
}

// Next returns the next row.
func (r *selectResult) Next() (PartialResult, error) {
	re := <-r.results
	return re.result, errors.Trace(re.err)
}

// Close closes SelectResult.
func (r *selectResult) Close() error {
	// close this channel tell fetch goroutine to exit
	close(r.closed)
	return r.resp.Close()
}

// partialResult represents a subset of select result.
type partialResult struct {
	resp       *tipb.SelectResponse
	chunkIdx   int
	cursor     int
	dataOffset int64
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

var zeroLenData = make([]byte, 0)

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (pr *partialResult) Next() (handle int64, data []byte, err error) {
	chunk := pr.getChunk()
	if chunk == nil {
		return 0, nil, nil
	}
	rowMeta := chunk.RowsMeta[pr.cursor]
	data = chunk.RowsData[pr.dataOffset : pr.dataOffset+rowMeta.Length]
	if data == nil {
		// The caller checks if data is nil to determine finished.
		data = zeroLenData
	}
	pr.dataOffset += rowMeta.Length
	handle = rowMeta.Handle
	pr.cursor++
	return
}

func (pr *partialResult) getChunk() *tipb.Chunk {
	for {
		if pr.chunkIdx >= len(pr.resp.Chunks) {
			return nil
		}
		chunk := &pr.resp.Chunks[pr.chunkIdx]
		if pr.cursor < len(chunk.RowsMeta) {
			return chunk
		}
		pr.cursor = 0
		pr.dataOffset = 0
		pr.chunkIdx++
	}
}

// Close closes the sub result.
func (pr *partialResult) Close() error {
	return nil
}

// Select do a select request, returns SelectResult.
// concurrency: The max concurrency for underlying coprocessor request.
// keepOrder: If the result should returned in key order. For example if we need keep data in order by
//            scan index, we should set keepOrder to true.
func Select(client kv.Client, ctx goctx.Context, req *tipb.SelectRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool, isolationLevel kv.IsoLevel) (SelectResult, error) {
	var err error
	defer func() {
		// Add metrics
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	// Convert tipb.*Request to kv.Request.
	kvReq, err1 := composeRequest(req, keyRanges, concurrency, keepOrder, isolationLevel)
	if err1 != nil {
		err = errors.Trace(err1)
		return nil, err
	}

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		err = errors.New("client returns nil response")
		return nil, err
	}
	result := &selectResult{
		resp:    resp,
		results: make(chan resultWithErr, 5),
		closed:  make(chan struct{}),
	}
	// If Aggregates is not nil, we should set result fields latter.
	if len(req.Aggregates) == 0 && len(req.GroupBy) == 0 {
		if req.TableInfo != nil {
			result.label = "table"
		} else {
			result.label = "index"
		}
	} else {
		result.label = "aggregate"
	}
	return result, nil
}

// SelectDAG sends a DAG request, returns SelectResult.
// concurrency: The max concurrency for underlying coprocessor request.
// keepOrder: If the result should returned in key order. For example if we need keep data in order by
//            scan index, we should set keepOrder to true.
func SelectDAG(client kv.Client, ctx goctx.Context, dag *tipb.DAGRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool, desc bool, isolationLevel kv.IsoLevel) (SelectResult, error) {
	var err error
	defer func() {
		// Add metrics.
		if err != nil {
			queryCounter.WithLabelValues(queryFailed).Inc()
		} else {
			queryCounter.WithLabelValues(querySucc).Inc()
		}
	}()

	kvReq := &kv.Request{
		Tp:             kv.ReqTypeDAG,
		Concurrency:    concurrency,
		KeepOrder:      keepOrder,
		KeyRanges:      keyRanges,
		Desc:           desc,
		IsolationLevel: isolationLevel,
	}
	kvReq.Data, err = dag.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	resp := client.Send(ctx, kvReq)
	if resp == nil {
		err = errors.New("client returns nil response")
		return nil, errors.Trace(err)
	}
	result := &selectResult{
		label:   "dag",
		resp:    resp,
		results: make(chan resultWithErr, concurrency),
		closed:  make(chan struct{}),
	}
	return result, nil
}

// Convert tipb.Request to kv.Request.
func composeRequest(req *tipb.SelectRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool, isolationLevel kv.IsoLevel) (*kv.Request, error) {
	kvReq := &kv.Request{
		Concurrency:    concurrency,
		KeepOrder:      keepOrder,
		KeyRanges:      keyRanges,
		IsolationLevel: isolationLevel,
	}
	if req.IndexInfo != nil {
		kvReq.Tp = kv.ReqTypeIndex
	} else {
		kvReq.Tp = kv.ReqTypeSelect
	}
	if req.OrderBy != nil {
		kvReq.Desc = req.OrderBy[0].Desc
	}
	var err error
	kvReq.Data, err = req.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvReq, nil
}

// XAPI error codes.
const (
	codeInvalidResp = 1
	codeNilResp     = 2
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

func collationToProto(c string) int32 {
	v, ok := mysql.CollationNames[c]
	if ok {
		return int32(v)
	}
	return int32(mysql.DefaultCollationID)
}

// ColumnsToProto converts a slice of model.ColumnInfo to a slice of tipb.ColumnInfo.
func ColumnsToProto(columns []*model.ColumnInfo, pkIsHandle bool) []*tipb.ColumnInfo {
	cols := make([]*tipb.ColumnInfo, 0, len(columns))
	for _, c := range columns {
		col := columnToProto(c)
		if pkIsHandle && mysql.HasPriKeyFlag(c.Flag) {
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
