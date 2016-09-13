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

package xapi

import (
	"io"
	"io/ioutil"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
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
	Fetch()
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

	results chan PartialResult
	done    chan error
}

func (r *selectResult) Fetch() {
	go r.fetch()
}

func (r *selectResult) fetch() {
	defer close(r.results)
	for {
		reader, err := r.resp.Next()
		if err != nil {
			r.done <- errors.Trace(err)
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
			done:       make(chan error),
			ch:         make(chan []selectResponseRow, 1),
		}
		go pr.fetch()
		r.results <- pr
	}
}

// Next returns the next row.
func (r *selectResult) Next() (pr PartialResult, err error) {
	var ok bool
	select {
	case pr, ok = <-r.results:
	case err = <-r.done:
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return
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
	return r.resp.Close()
}

// partialResult represents a subset of select result.
type partialResult struct {
	index      bool
	aggregate  bool
	fields     []*types.FieldType
	reader     io.ReadCloser
	ignoreData bool

	done    chan error
	fetched bool

	ch     chan []selectResponseRow
	rows   []selectResponseRow
	cursor int
}

type selectResponseRow struct {
	handle int64
	data   []types.Datum
}

const batchDecodeSize = 1024

func (pr *partialResult) fetch() {
	defer close(pr.ch)
	b, err := ioutil.ReadAll(pr.reader)
	pr.reader.Close()
	pr.reader = nil
	if err != nil {
		pr.done <- errors.Trace(err)
		return
	}

	resp := new(tipb.SelectResponse)
	if err = resp.Unmarshal(b); err != nil {
		pr.done <- errors.Trace(err)
		return
	}
	if resp.Error != nil {
		pr.done <- errInvalidResp.Gen("[%d %s]", resp.Error.GetCode(), resp.Error.GetMsg())
		return
	}
	pr.done <- nil

	// an optimize for allocation, ringBuffer for big rows.
	if size, ok := onlyFewRows(resp.Chunks); ok {
		var err error
		rows := make([]selectResponseRow, 0, size)
		for chunkIdx := 0; chunkIdx < len(resp.Chunks); chunkIdx++ {
			chunk := &resp.Chunks[chunkIdx]
			rows, err = pr.decodeChunk(chunk, rows)
			if err != nil {
				pr.done <- errors.Trace(err)
				return
			}
		}
		if len(rows) > 0 {
			pr.ch <- rows
		}
		return
	}

	// var buf ringBuffer
	// rows := buf.get()
	rows := make([]selectResponseRow, 0, batchDecodeSize)
	for chunkIdx := 0; chunkIdx < len(resp.Chunks); chunkIdx++ {
		chunk := &resp.Chunks[chunkIdx]
		rows, err := pr.decodeChunk(chunk, rows)
		if err != nil {
			pr.done <- errors.Trace(err)
			return
		}
		if len(rows) >= batchDecodeSize {
			// when it collect enough rows, send to channel in a batch
			pr.ch <- rows
			rows = make([]selectResponseRow, 0, batchDecodeSize)
			// rows = buf.get()
		}
	}
	// don't forget the last batch that may small than batchDecodeSize
	if len(rows) > 0 {
		pr.ch <- rows
	}
}

// onlyFewRows returns (count, true) if there are few chunks rows, otherwist (_, false)
func onlyFewRows(chunks []tipb.Chunk) (int, bool) {
	totalRows := 0
	for chunkIdx := 0; chunkIdx < len(chunks); chunkIdx++ {
		chunk := &chunks[chunkIdx]
		if totalRows >= batchDecodeSize {
			return 0, false
		}
		totalRows += len(chunk.RowsMeta)
	}
	return totalRows, true
}

type ringBuffer struct {
	data [2][batchDecodeSize + 64]selectResponseRow
	cur  int
}

func (r *ringBuffer) get() []selectResponseRow {
	r.cur = (r.cur + 1) % 2
	return r.data[r.cur][:]
}

var dummyData = make([]types.Datum, 0)

func (pr *partialResult) decodeChunk(chunk *tipb.Chunk, rows []selectResponseRow) ([]selectResponseRow, error) {
	dataOffset := 0
	for _, rowMeta := range chunk.RowsMeta {
		var data []types.Datum
		var err error
		if !pr.ignoreData {
			rowData := chunk.RowsData[dataOffset : dataOffset+int(rowMeta.Length)]
			data, err = tablecodec.DecodeValues(rowData, pr.fields, pr.index)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dataOffset += int(rowMeta.Length)
		}
		if data == nil {
			data = dummyData
		}

		rows = append(rows, selectResponseRow{
			handle: rowMeta.Handle,
			data:   data,
		})
	}
	return rows, nil
}

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

	if pr.cursor >= len(pr.rows) {
		rows, ok := <-pr.ch
		if !ok {
			return 0, nil, nil
		}
		pr.rows = rows
		pr.cursor = 0
	}

	row := pr.rows[pr.cursor]
	handle, data = row.handle, row.data
	pr.cursor++
	return
}

// Close closes the sub result.
func (pr *partialResult) Close() error {
	return nil
}

// Select do a select request, returns SelectResult.
// conncurrency: The max concurrency for underlying coprocessor request.
// keepOrder: If the result should returned in key order. For example if we need keep data in order by
//            scan index, we should set keepOrder to true.
func Select(client kv.Client, req *tipb.SelectRequest, keyRanges []kv.KeyRange, concurrency int, keepOrder bool) (SelectResult, error) {
	// Convert tipb.*Request to kv.Request.
	kvReq, err := composeRequest(req, keyRanges, concurrency, keepOrder)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := client.Send(kvReq)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &selectResult{
		resp:    resp,
		results: make(chan PartialResult, 5),
		done:    make(chan error, 1),
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
