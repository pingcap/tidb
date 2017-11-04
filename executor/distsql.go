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

package executor

import (
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}

	_ DataReader = &TableReaderExecutor{}
	_ DataReader = &IndexReaderExecutor{}
	_ DataReader = &IndexLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	handles []int64
	rows    []Row
	cursor  int

	done   bool
	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder map[int64]int
}

func (task *lookupTableTask) getRow() (Row, error) {
	if !task.done {
		err := <-task.doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
		task.done = true
	}

	if task.cursor < len(task.rows) {
		row := task.rows[task.cursor]
		task.cursor++
		return row, nil
	}

	return nil, nil
}

// rowsSorter sorts the rows by its index order.
type rowsSorter struct {
	order     map[int64]int
	rows      []Row
	handleIdx int
}

func (s *rowsSorter) Less(i, j int) bool {
	x := s.order[s.rows[i][s.handleIdx].GetInt64()]
	y := s.order[s.rows[j][s.handleIdx].GetInt64()]
	return x < y
}

func (s *rowsSorter) Len() int {
	return len(s.rows)
}

func (s *rowsSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func tableRangesToKVRanges(tid int64, tableRanges []types.IntColumnRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, tableRange.LowVal)

		var endKey kv.Key
		if tableRange.HighVal != math.MaxInt64 {
			endKey = tablecodec.EncodeRowKeyWithHandle(tid, tableRange.HighVal+1)
		} else {
			endKey = tablecodec.EncodeRowKeyWithHandle(tid, tableRange.HighVal).Next()
		}
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

/*
 * Convert sorted handle to kv ranges.
 * For continuous handles, we should merge them to a single key range.
 */
func tableHandlesToKVRanges(tid int64, handles []int64) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		h := handles[i]
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			i++
			continue
		}
		j := i + 1
		endHandle := h + 1
		for ; j < len(handles); j++ {
			if handles[j] == endHandle {
				endHandle = handles[j] + 1
				continue
			}
			break
		}
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, h)
		endKey := tablecodec.EncodeRowKeyWithHandle(tid, endHandle)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

// indexValuesToKVRanges will convert the index datums to kv ranges.
func indexValuesToKVRanges(tid, idxID int64, values [][]types.Datum) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(values))
	for _, vals := range values {
		// TODO: We don't process the case that equal key has different types.
		valKey, err := codec.EncodeKey(nil, vals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		valKeyNext := []byte(kv.Key(valKey).PrefixNext())
		rangeBeginKey := tablecodec.EncodeIndexSeekKey(tid, idxID, valKey)
		rangeEndKey := tablecodec.EncodeIndexSeekKey(tid, idxID, valKeyNext)
		krs = append(krs, kv.KeyRange{StartKey: rangeBeginKey, EndKey: rangeEndKey})
	}
	return krs, nil
}

func indexRangesToKVRanges(sc *variable.StatementContext, tid, idxID int64, ranges []*types.IndexRange, fieldTypes []*types.FieldType) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		err := convertIndexRangeTypes(sc, ran, fieldTypes)
		if err != nil {
			return nil, errors.Trace(err)
		}

		low, err := codec.EncodeKey(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		high, err := codec.EncodeKey(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PrefixNext())
		}
		startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs, nil
}

func convertIndexRangeTypes(sc *variable.StatementContext, ran *types.IndexRange, fieldTypes []*types.FieldType) error {
	for i := range ran.LowVal {
		if ran.LowVal[i].Kind() == types.KindMinNotNull || ran.LowVal[i].Kind() == types.KindMaxValue {
			continue
		}
		converted, err := ran.LowVal[i].ConvertTo(sc, fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(sc, &ran.LowVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[i] = converted
		if cmp == 0 {
			continue
		}
		if cmp < 0 && !ran.LowExclude {
			// For int column a, a >= 1.1 is converted to a > 1.
			ran.LowExclude = true
		} else if cmp > 0 && ran.LowExclude {
			// For int column a, a > 1.9 is converted to a >= 2.
			ran.LowExclude = false
		}
		// The converted value has changed, the other column values doesn't matter.
		// For equal condition, converted value changed means there will be no match.
		// For non equal condition, this column would be the last one to build the range.
		// Break here to prevent the rest columns modify LowExclude again.
		break
	}
	for i := range ran.HighVal {
		if ran.HighVal[i].Kind() == types.KindMaxValue || ran.LowVal[i].Kind() == types.KindNull {
			continue
		}
		converted, err := ran.HighVal[i].ConvertTo(sc, fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(sc, &ran.HighVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.HighVal[i] = converted
		if cmp == 0 {
			continue
		}
		// For int column a, a < 1.1 is converted to a <= 1.
		if cmp < 0 && ran.HighExclude {
			ran.HighExclude = false
		}
		// For int column a, a <= 1.9 is converted to a < 2.
		if cmp > 0 && !ran.HighExclude {
			ran.HighExclude = true
		}
		break
	}
	return nil
}

func extractHandlesFromNewIndexResult(idxResult distsql.SelectResult) (handles []int64, finish bool, err error) {
	subResult, e0 := idxResult.Next()
	if e0 != nil {
		err = errors.Trace(e0)
		return
	}
	if subResult == nil {
		finish = true
		return
	}
	handles, err = extractHandlesFromNewIndexSubResult(subResult)
	if err != nil {
		err = errors.Trace(err)
	}
	return
}

func extractHandlesFromNewIndexSubResult(subResult distsql.PartialResult) ([]int64, error) {
	defer terror.Call(subResult.Close)
	var (
		handles     []int64
		handleDatum types.Datum
	)
	handleType := types.NewFieldType(mysql.TypeLonglong)
	for {
		data, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		handleDatum, err = tablecodec.DecodeColumnValue(data[0].GetRaw(), handleType, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, handleDatum.GetInt64())
	}
	return handles, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	var err error
	for _, obj := range objs {
		if obj != nil {
			err1 := obj.Close()
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	return errors.Trace(err)
}

func decodeRawValues(values []types.Datum, schema *expression.Schema, loc *time.Location) error {
	var err error
	for i := 0; i < schema.Len(); i++ {
		if values[i].Kind() == types.KindRaw {
			values[i], err = tablecodec.DecodeColumnValue(values[i].GetRaw(), schema.Columns[i].RetType, loc)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// timeZoneOffset returns the local time zone offset in seconds.
func timeZoneOffset(ctx context.Context) int64 {
	loc := ctx.GetSessionVars().GetTimeZone()
	_, offset := time.Now().In(loc).Zone()
	return int64(offset)
}

// Flags are used by tipb.SelectRequest.Flags to handle execution mode, like how to handle truncate error.
const (
	// FlagIgnoreTruncate indicates if truncate error should be ignored.
	// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
	FlagIgnoreTruncate uint64 = 1
	// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
	FlagTruncateAsWarning uint64 = 1 << 1
)

// statementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func statementContextToFlags(sc *variable.StatementContext) uint64 {
	var flags uint64
	if sc.IgnoreTruncate {
		flags |= FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= FlagTruncateAsWarning
	}
	return flags
}

func setPBColumnsDefaultValue(ctx context.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return errors.Trace(err)
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(d, ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// DataReader can send requests which ranges are constructed by datums.
type DataReader interface {
	Executor

	doRequestForDatums(goCtx goctx.Context, datums [][]types.Datum) error
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	if col != nil && col.ID == model.ExtraHandleID {
		return true
	}
	return false
}

type requestBuilder struct {
	kv.Request
	err error
}

func (builder *requestBuilder) Build() (*kv.Request, error) {
	return &builder.Request, errors.Trace(builder.err)
}

func (builder *requestBuilder) SetTableRanges(tid int64, tableRanges []types.IntColumnRange) *requestBuilder {
	builder.Request.KeyRanges = tableRangesToKVRanges(tid, tableRanges)
	return builder
}

func (builder *requestBuilder) SetIndexRanges(sc *variable.StatementContext, tid, idxID int64, ranges []*types.IndexRange, fieldTypes []*types.FieldType) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexRangesToKVRanges(sc, tid, idxID, ranges, fieldTypes)
	return builder
}

func (builder *requestBuilder) SetTableHandles(tid int64, handles []int64) *requestBuilder {
	builder.Request.KeyRanges = tableHandlesToKVRanges(tid, handles)
	return builder
}

func (builder *requestBuilder) SetIndexValues(tid, idxID int64, values [][]types.Datum) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexValuesToKVRanges(tid, idxID, values)
	return builder
}

func (builder *requestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeDAG
	builder.Request.StartTs = dag.StartTs
	builder.Request.Data, builder.err = dag.Marshal()
	return builder
}

func (builder *requestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeAnalyze
	builder.Request.StartTs = ana.StartTs
	builder.Request.Data, builder.err = ana.Marshal()
	builder.Request.NotFillCache = true
	return builder
}

func (builder *requestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *requestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

func (builder *requestBuilder) SetDesc(desc bool) *requestBuilder {
	builder.Request.Desc = desc
	return builder
}

func (builder *requestBuilder) SetKeepOrder(order bool) *requestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

func getIsolationLevel(sv *variable.SessionVars) kv.IsoLevel {
	if sv.Systems[variable.TxnIsolation] == ast.ReadCommitted {
		return kv.RC
	}
	return kv.SI
}

func (builder *requestBuilder) SetFromSessionVars(sv *variable.SessionVars) *requestBuilder {
	builder.Request.Concurrency = sv.DistSQLScanConcurrency
	builder.Request.IsolationLevel = getIsolationLevel(sv)
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	return builder
}

func (builder *requestBuilder) SetPriority(priority int) *requestBuilder {
	builder.Request.Priority = priority
	return builder
}
