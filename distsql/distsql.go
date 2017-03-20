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
	"io"
	"io/ioutil"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/bytespool"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	errInvalidResp = terror.ClassXEval.New(codeInvalidResp, "invalid response")
	errNilResp     = terror.ClassXEval.New(codeNilResp, "client returns nil response")
)

var (
	_ SelectResult  = &selectResult{}
	_ PartialResult = &partialResult{}
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Next gets the next partial result.
	Next() (PartialResult, error)
	// SetFields sets the expected result type.
	SetFields(fields []*types.FieldType)
	// Close closes the iterator.
	Close() error
	// Fetch fetches partial results from client.
	// The caller should call SetFields() before call Fetch().
	Fetch(ctx goctx.Context)
	// IgnoreData sets ignore data attr to true.
	// For index double scan, we do not need row data when scanning index.
	IgnoreData()
}

// PartialResult is the result from a single region server.
type PartialResult interface {
	// Next returns the next row of the sub result.
	// If no more row to return, data would be nil.
	Next() (handle int64, data []types.Datum, err error)
	// Close closes the partial result.
	Close() error
}

// SelectResult is used to get response rows from SelectRequest.
type selectResult struct {
	index      bool
	aggregate  bool
	fields     []*types.FieldType
	resp       kv.Response
	ignoreData bool

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
		var label string
		if r.index {
			label = "index"
		} else {
			label = "table"
		}
		queryHistgram.WithLabelValues(label).Observe(duration.Seconds())
	}()
	for {
		reader, err := r.resp.Next()
		if err != nil {
			r.results <- resultWithErr{err: errors.Trace(err)}
			return
		}
		if reader == nil {
			return
		}
		pr := &partialResult{
			index:      r.index,
			fields:     r.fields,
			reader:     reader,
			aggregate:  r.aggregate,
			ignoreData: r.ignoreData,
			done:       make(chan error, 1),
		}
		go pr.fetch()

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

// SetFields sets select result field types.
func (r *selectResult) SetFields(fields []*types.FieldType) {
	r.fields = fields
}

func (r *selectResult) IgnoreData() {
	r.ignoreData = true
}

// Close closes SelectResult.
func (r *selectResult) Close() error {
	// close this channel tell fetch goroutine to exit
	close(r.closed)
	return r.resp.Close()
}

// partialResult represents a subset of select result.
type partialResult struct {
	index      bool
	aggregate  bool
	fields     []*types.FieldType
	reader     io.ReadCloser
	resp       *tipb.SelectResponse
	chunkIdx   int
	cursor     int
	dataOffset int64
	ignoreData bool

	done    chan error
	fetched bool
}

func (pr *partialResult) fetch() {
	defer close(pr.done)
	pr.resp = new(tipb.SelectResponse)
	var b []byte
	var err error
	if rc, ok := pr.reader.(*bytespool.ReadCloser); ok {
		b = rc.SharedBytes()
	} else {
		b, err = ioutil.ReadAll(pr.reader)
		if err != nil {
			pr.done <- errors.Trace(err)
			return
		}
	}

	err = pr.resp.Unmarshal(b)
	if err != nil {
		pr.done <- errors.Trace(err)
		return
	}

	if pr.resp.Error != nil {
		pr.done <- errInvalidResp.Gen("[%d %s]", pr.resp.Error.GetCode(), pr.resp.Error.GetMsg())
		return
	}

	pr.done <- nil
}

var dummyData = make([]types.Datum, 0)

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (pr *partialResult) Next() (handle int64, data []types.Datum, err error) {
	if !pr.fetched {
		err = <-pr.done
		pr.fetched = true
		if err != nil {
			return 0, nil, err
		}
	}
	if len(pr.resp.Chunks) > 0 {
		// For new resp rows structure.
		chunk := pr.getChunk()
		if chunk == nil {
			return 0, nil, nil
		}
		rowMeta := chunk.RowsMeta[pr.cursor]
		if !pr.ignoreData {
			rowData := chunk.RowsData[pr.dataOffset : pr.dataOffset+rowMeta.Length]
			data, err = tablecodec.DecodeValues(rowData, pr.fields, pr.index)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			pr.dataOffset += rowMeta.Length
		}
		if data == nil {
			data = dummyData
		}
		if !pr.aggregate {
			handle = rowMeta.Handle
		}
		pr.cursor++
		return
	}
	if pr.cursor >= len(pr.resp.Rows) {
		return 0, nil, nil
	}
	row := pr.resp.Rows[pr.cursor]
	if !pr.ignoreData {
		data, err = tablecodec.DecodeValues(row.Data, pr.fields, pr.index)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}
	if data == nil {
		// When no column is referenced, the data may be nil, like 'select count(*) from t'.
		// In this case, we need to create a zero length datum slice,
		// as caller will check if data is nil to finish iteration.
		// data = make([]types.Datum, 0)
		data = dummyData
	}
	if !pr.aggregate {
		handleBytes := row.GetHandle()
		_, datum, err := codec.DecodeOne(handleBytes)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		handle = datum.GetInt64()
	}
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
	return pr.reader.Close()
}

// Select do a select request, returns SelectResult.
// concurrency: The max concurrency for underlying coprocessor request.
// keepOrder: If the result should returned in key order. For example if we need keep data in order by
//            scan index, we should set keepOrder to true.
func Select(client kv.Client, ctx goctx.Context, req *tipb.SelectRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool) (SelectResult, error) {
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
	kvReq, err1 := composeRequest(req, keyRanges, concurrency, keepOrder)
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
			result.fields = ProtoColumnsToFieldTypes(req.TableInfo.Columns)
		} else {
			result.fields = ProtoColumnsToFieldTypes(req.IndexInfo.Columns)
			length := len(req.IndexInfo.Columns)
			if req.IndexInfo.Columns[length-1].GetPkHandle() {
				// Returned index row do not contains extra PKHandle column.
				result.fields = result.fields[:length-1]
			}
			result.index = true
		}
	} else {
		result.aggregate = true
	}
	return result, nil
}

// Convert tipb.Request to kv.Request.
func composeRequest(req *tipb.SelectRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool) (*kv.Request, error) {
	kvReq := &kv.Request{
		Concurrency: concurrency,
		KeepOrder:   keepOrder,
		KeyRanges:   keyRanges,
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

// SupportExpression checks if the expression is supported by the client.
func SupportExpression(client kv.Client, expr *tipb.Expr) bool {
	return false
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

// ProtoColumnsToFieldTypes converts tipb column info slice to FieldTyps slice.
func ProtoColumnsToFieldTypes(pColumns []*tipb.ColumnInfo) []*types.FieldType {
	fields := make([]*types.FieldType, len(pColumns))
	for i, v := range pColumns {
		field := new(types.FieldType)
		field.Tp = byte(v.GetTp())
		field.Collate = mysql.Collations[byte(v.GetCollation())]
		field.Decimal = int(v.GetDecimal())
		field.Flen = int(v.GetColumnLen())
		field.Flag = uint(v.GetFlag())
		field.Elems = v.GetElems()
		fields[i] = field
	}
	return fields
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
