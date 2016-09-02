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
	"github.com/pingcap/tidb/util/codec"
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
	index     bool
	aggregate bool
	fields    []*types.FieldType
	resp      kv.Response

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
			index:     r.index,
			fields:    r.fields,
			reader:    reader,
			aggregate: r.aggregate,
			done:      make(chan error),
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

// Close closes SelectResult.
func (r *selectResult) Close() error {
	return r.resp.Close()
}

// partialResult represents a subset of select result.
type partialResult struct {
	index     bool
	aggregate bool
	fields    []*types.FieldType
	reader    io.ReadCloser
	resp      *tipb.SelectResponse
	cursor    int

	done    chan error
	fetched bool
}

func (pr *partialResult) fetch() {
	pr.resp = new(tipb.SelectResponse)
	b, err := ioutil.ReadAll(pr.reader)
	pr.reader.Close()
	if err != nil {
		pr.done <- errors.Trace(err)
		return
	}
	err = pr.resp.Unmarshal(b)
	if err != nil {
		pr.done <- errors.Trace(err)
		return
	}
	if pr.resp.Error != nil {
		pr.done <- errInvalidResp.Gen("[%d %s]", pr.resp.Error.GetCode(), pr.resp.Error.GetMsg())
	}
	pr.done <- nil
}

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (pr *partialResult) Next() (handle int64, data []types.Datum, err error) {
	if !pr.fetched {
		select {
		case err = <-pr.done:
		}
		pr.fetched = true
		if err != nil {
			return 0, nil, err
		}
	}
	if pr.cursor >= len(pr.resp.Rows) {
		return 0, nil, nil
	}
	row := pr.resp.Rows[pr.cursor]
	data, err = tablecodec.DecodeValues(row.Data, pr.fields, pr.index)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if data == nil {
		// When no column is referenced, the data may be nil, like 'select count(*) from t'.
		// In this case, we need to create a zero length datum slice,
		// as caller will check if data is nil to finish iteration.
		data = make([]types.Datum, 0)
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

// Close closes the sub result.
func (pr *partialResult) Close() error {
	return nil
}

// Select do a select request, returns SelectResult.
// conncurrency: The max concurrency for underlying coprocessor request.
// keepOrder: If the result should returned in key order. For example if we need keep data in order by
//            scan index, we should set keepOrder to true.
func Select(client kv.Client, req *tipb.SelectRequest, concurrency int, keepOrder bool) (SelectResult, error) {
	// Convert tipb.*Request to kv.Request.
	kvReq, err := composeRequest(req, concurrency, keepOrder)
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
func composeRequest(req *tipb.SelectRequest, concurrency int, keepOrder bool) (*kv.Request, error) {
	kvReq := &kv.Request{
		Concurrency: concurrency,
		KeepOrder:   keepOrder,
	}
	if req.IndexInfo != nil {
		kvReq.Tp = kv.ReqTypeIndex
		tid := req.IndexInfo.GetTableId()
		idxID := req.IndexInfo.GetIndexId()
		kvReq.KeyRanges = EncodeIndexRanges(tid, idxID, req.Ranges)
	} else {
		kvReq.Tp = kv.ReqTypeSelect
		tid := req.GetTableInfo().GetTableId()
		kvReq.KeyRanges = EncodeTableRanges(tid, req.Ranges)
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

// EncodeTableRanges encodes table ranges into kv.KeyRanges.
func EncodeTableRanges(tid int64, rans []*tipb.KeyRange) []kv.KeyRange {
	keyRanges := make([]kv.KeyRange, 0, len(rans))
	buf := make([]byte, 0, tablecodec.RecordRowKeyLen*len(rans)*2)
	for _, r := range rans {
		var start, end kv.Key
		buf, start = tablecodec.EncodeRowKey(buf, tid, r.Low)
		buf, end = tablecodec.EncodeRowKey(buf, tid, r.High)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	return keyRanges
}

// EncodeIndexRanges encodes index ranges into kv.KeyRanges.
func EncodeIndexRanges(tid, idxID int64, rans []*tipb.KeyRange) []kv.KeyRange {
	keyRanges := make([]kv.KeyRange, 0, len(rans))
	for _, r := range rans {
		// Convert range to kv.KeyRange
		start := tablecodec.EncodeIndexSeekKey(tid, idxID, r.Low)
		end := tablecodec.EncodeIndexSeekKey(tid, idxID, r.High)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	return keyRanges
}
