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

	"github.com/golang/protobuf/proto"
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

// SelectResult is used to get response rows from SelectRequest.
type SelectResult struct {
	index     bool
	aggregate bool
	fields    []*types.FieldType
	resp      kv.Response
}

// Next returns the next row.
func (r *SelectResult) Next() (subResult *SubResult, err error) {
	var reader io.ReadCloser
	reader, err = r.resp.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if reader == nil {
		return nil, nil
	}
	subResult = &SubResult{
		index:     r.index,
		fields:    r.fields,
		reader:    reader,
		aggregate: r.aggregate,
	}
	return
}

// SetFields sets select result field types.
func (r *SelectResult) SetFields(fields []*types.FieldType) {
	r.fields = fields
}

// Close closes SelectResult.
func (r *SelectResult) Close() error {
	return r.resp.Close()
}

// SubResult represents a subset of select result.
type SubResult struct {
	index     bool
	aggregate bool
	fields    []*types.FieldType
	reader    io.ReadCloser
	resp      *tipb.SelectResponse
	cursor    int
}

// Next returns the next row of the sub result.
// If no more row to return, data would be nil.
func (r *SubResult) Next() (handle int64, data []types.Datum, err error) {
	if r.resp == nil {
		r.resp = new(tipb.SelectResponse)
		var b []byte
		b, err = ioutil.ReadAll(r.reader)
		r.reader.Close()
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		err = proto.Unmarshal(b, r.resp)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if r.resp.Error != nil {
			return 0, nil, errInvalidResp.Gen("[%d %s]", r.resp.Error.GetCode(), r.resp.Error.GetMsg())
		}
	}
	if r.cursor >= len(r.resp.Rows) {
		return 0, nil, nil
	}
	row := r.resp.Rows[r.cursor]
	data, err = tablecodec.DecodeValues(row.Data, r.fields, r.index)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if data == nil {
		// When no column is referenced, the data may be nil, like 'select count(*) from t'.
		// In this case, we need to create a zero length datum slice,
		// as caller will check if data is nil to finish iteration.
		data = make([]types.Datum, 0)
	}
	if !r.aggregate {
		handleBytes := row.GetHandle()
		datums, err := codec.Decode(handleBytes)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		handle = datums[0].GetInt64()
	}
	r.cursor++
	return
}

// Close closes the sub result.
func (r *SubResult) Close() error {
	return nil
}

// Select do a select request, returns SelectResult.
func Select(client kv.Client, req *tipb.SelectRequest, concurrency int) (*SelectResult, error) {
	// Convert tipb.*Request to kv.Request
	kvReq, err := composeRequest(req, concurrency)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := client.Send(kvReq)
	if resp == nil {
		return nil, errors.New("client returns nil response")
	}
	result := &SelectResult{resp: resp}
	// If Aggregates is not nil, we should set result fields latter.
	if len(req.Aggregates) == 0 && len(req.GroupBy) == 0 {
		if req.TableInfo != nil {
			result.fields = ProtoColumnsToFieldTypes(req.TableInfo.Columns)
		} else {
			result.fields = ProtoColumnsToFieldTypes(req.IndexInfo.Columns)
			result.index = true
		}
	} else {
		result.aggregate = true
	}
	return result, nil
}

// Convert tipb.Request to kv.Request.
func composeRequest(req *tipb.SelectRequest, concurrency int) (*kv.Request, error) {
	kvReq := &kv.Request{
		Concurrency: concurrency,
		KeepOrder:   true,
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
		kvReq.Desc = *req.OrderBy[0].Desc
	}
	var err error
	kvReq.Data, err = proto.Marshal(req)
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
		ColumnId:  proto.Int64(c.ID),
		Collation: proto.Int32(collationToProto(c.FieldType.Collate)),
		ColumnLen: proto.Int32(int32(c.FieldType.Flen)),
		Decimal:   proto.Int32(int32(c.FieldType.Decimal)),
		Flag:      proto.Int32(int32(c.Flag)),
		Elems:     c.Elems,
	}
	t := int32(c.FieldType.Tp)
	pc.Tp = &t
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
			col.PkHandle = proto.Bool(true)
		} else {
			col.PkHandle = proto.Bool(false)
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
		TableId: proto.Int64(t.ID),
		IndexId: proto.Int64(idx.ID),
		Unique:  proto.Bool(idx.Unique),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns))
	for _, c := range idx.Columns {
		cols = append(cols, columnToProto(t.Columns[c.Offset]))
	}
	pi.Columns = cols
	return pi
}

// EncodeTableRanges encodes table ranges into kv.KeyRanges.
func EncodeTableRanges(tid int64, rans []*tipb.KeyRange) []kv.KeyRange {
	keyRanges := make([]kv.KeyRange, 0, len(rans))
	for _, r := range rans {
		start := tablecodec.EncodeRowKey(tid, r.Low)
		end := tablecodec.EncodeRowKey(tid, r.High)
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
