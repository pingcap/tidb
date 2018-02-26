package distsql

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

type RequestBuilder struct {
	kv.Request
	err error
}

func (builder *RequestBuilder) Build() (*kv.Request, error) {
	return &builder.Request, errors.Trace(builder.err)
}

func (builder *RequestBuilder) SetTableRanges(tid int64, tableRanges []*ranger.NewRange) *RequestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges = TableRangesToKVRanges(tid, tableRanges)
	return builder
}

func (builder *RequestBuilder) SetIndexRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.NewRange) *RequestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = IndexRangesToKVRanges(sc, tid, idxID, ranges)
	return builder
}

func (builder *RequestBuilder) SetTableHandles(tid int64, handles []int64) *RequestBuilder {
	builder.Request.KeyRanges = TableHandlesToKVRanges(tid, handles)
	return builder
}

func (builder *RequestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *RequestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeDAG
	builder.Request.StartTs = dag.StartTs
	builder.Request.Data, builder.err = dag.Marshal()
	return builder
}

func (builder *RequestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *RequestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeAnalyze
	builder.Request.StartTs = ana.StartTs
	builder.Request.Data, builder.err = ana.Marshal()
	builder.Request.NotFillCache = true
	return builder
}

func (builder *RequestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *RequestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

func (builder *RequestBuilder) SetDesc(desc bool) *RequestBuilder {
	builder.Request.Desc = desc
	return builder
}

func (builder *RequestBuilder) SetKeepOrder(order bool) *RequestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

func getIsolationLevel(sv *variable.SessionVars) kv.IsoLevel {
	isoLevel, _ := sv.GetSystemVar(variable.TxnIsolation)
	if isoLevel == ast.ReadCommitted {
		return kv.RC
	}
	return kv.SI
}

func (builder *RequestBuilder) SetFromSessionVars(sv *variable.SessionVars) *RequestBuilder {
	builder.Request.Concurrency = sv.DistSQLScanConcurrency
	builder.Request.IsolationLevel = getIsolationLevel(sv)
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	return builder
}

func (builder *RequestBuilder) SetPriority(priority int) *RequestBuilder {
	builder.Request.Priority = priority
	return builder
}

func TableRangesToKVRanges(tid int64, ranges []*ranger.NewRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		var low, high []byte
		low = codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
		high = codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PrefixNext())
		}
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

// TableHandlesToKVRanges converts sorted handle to kv ranges.
// For continuous handles, we should merge them to a single key range.
func TableHandlesToKVRanges(tid int64, handles []int64) []kv.KeyRange {
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

func IndexRangesToKVRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.NewRange) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		low, err := codec.EncodeKey(sc, nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		high, err := codec.EncodeKey(sc, nil, ran.HighVal...)
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
