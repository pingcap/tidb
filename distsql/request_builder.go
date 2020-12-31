// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// RequestBuilder is used to build a "kv.Request".
// It is called before we issue a kv request by "Select".
type RequestBuilder struct {
	kv.Request
	// txnScope indicates the value of txn_scope
	txnScope string
	bundles  map[string]*placement.Bundle
	err      error
}

// Build builds a "kv.Request".
func (builder *RequestBuilder) Build() (*kv.Request, error) {
	err := builder.verifyTxnScope()
	if err != nil {
		builder.err = err
	}
	return &builder.Request, builder.err
}

// SetMemTracker sets a memTracker for this request.
func (builder *RequestBuilder) SetMemTracker(tracker *memory.Tracker) *RequestBuilder {
	builder.Request.MemTracker = tracker
	return builder
}

// SetTableRanges sets "KeyRanges" for "kv.Request" by converting "tableRanges"
// to "KeyRanges" firstly.
// Note this function should be deleted or at least not exported, but currently
// br refers it, so have to keep it.
func (builder *RequestBuilder) SetTableRanges(tid int64, tableRanges []*ranger.Range, fb *statistics.QueryFeedback) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges = TableRangesToKVRanges(tid, tableRanges, fb)
	}
	return builder
}

// SetIndexRanges sets "KeyRanges" for "kv.Request" by converting index range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = IndexRangesToKVRanges(sc, tid, idxID, ranges, nil)
	}
	return builder
}

// SetIndexRangesForTables sets "KeyRanges" for "kv.Request" by converting multiple indexes range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRangesForTables(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = IndexRangesToKVRangesForTables(sc, tids, idxID, ranges, nil)
	}
	return builder
}

// SetHandleRanges sets "KeyRanges" for "kv.Request" by converting table handle range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetHandleRanges(sc *stmtctx.StatementContext, tid int64, isCommonHandle bool, ranges []*ranger.Range, fb *statistics.QueryFeedback) *RequestBuilder {
	return builder.SetHandleRangesForTables(sc, []int64{tid}, isCommonHandle, ranges, fb)
}

// SetHandleRangesForTables sets "KeyRanges" for "kv.Request" by converting table handle range
// "ranges" to "KeyRanges" firstly for multiple tables.
func (builder *RequestBuilder) SetHandleRangesForTables(sc *stmtctx.StatementContext, tid []int64, isCommonHandle bool, ranges []*ranger.Range, fb *statistics.QueryFeedback) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = TableHandleRangesToKVRanges(sc, tid, isCommonHandle, ranges, fb)
	}
	return builder
}

// SetTableHandles sets "KeyRanges" for "kv.Request" by converting table handles
// "handles" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetTableHandles(tid int64, handles []kv.Handle) *RequestBuilder {
	builder.Request.KeyRanges = TableHandlesToKVRanges(tid, handles)
	return builder
}

// SetPartitionsAndHandles sets "KeyRanges" for "kv.Request" by converting ParitionHandles to KeyRanges.
// handles in slice must be kv.PartitionHandle.
func (builder *RequestBuilder) SetPartitionsAndHandles(handles []kv.Handle) *RequestBuilder {
	builder.Request.KeyRanges = PartitionHandlesToKVRanges(handles)
	return builder
}

const estimatedRegionRowCount = 100000

// SetDAGRequest sets the request type to "ReqTypeDAG" and construct request data.
func (builder *RequestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = kv.ReqTypeDAG
		builder.Request.Cacheable = true
		builder.Request.Data, builder.err = dag.Marshal()
	}
	// When the DAG is just simple scan and small limit, set concurrency to 1 would be sufficient.
	if len(dag.Executors) == 2 && dag.Executors[1].GetLimit() != nil {
		limit := dag.Executors[1].GetLimit()
		if limit != nil && limit.Limit < estimatedRegionRowCount {
			builder.Request.Concurrency = 1
		}
	}
	return builder
}

// SetAnalyzeRequest sets the request type to "ReqTypeAnalyze" and construct request data.
func (builder *RequestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = kv.ReqTypeAnalyze
		builder.Request.Data, builder.err = ana.Marshal()
		builder.Request.NotFillCache = true
		builder.Request.IsolationLevel = kv.RC
		builder.Request.Priority = kv.PriorityLow
	}

	return builder
}

// SetChecksumRequest sets the request type to "ReqTypeChecksum" and construct request data.
func (builder *RequestBuilder) SetChecksumRequest(checksum *tipb.ChecksumRequest) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = kv.ReqTypeChecksum
		builder.Request.Data, builder.err = checksum.Marshal()
		builder.Request.NotFillCache = true
	}

	return builder
}

// SetKeyRanges sets "KeyRanges" for "kv.Request".
func (builder *RequestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *RequestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

// SetStartTS sets "StartTS" for "kv.Request".
func (builder *RequestBuilder) SetStartTS(startTS uint64) *RequestBuilder {
	builder.Request.StartTs = startTS
	return builder
}

// SetDesc sets "Desc" for "kv.Request".
func (builder *RequestBuilder) SetDesc(desc bool) *RequestBuilder {
	builder.Request.Desc = desc
	return builder
}

// SetKeepOrder sets "KeepOrder" for "kv.Request".
func (builder *RequestBuilder) SetKeepOrder(order bool) *RequestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

// SetStoreType sets "StoreType" for "kv.Request".
func (builder *RequestBuilder) SetStoreType(storeType kv.StoreType) *RequestBuilder {
	builder.Request.StoreType = storeType
	return builder
}

// SetAllowBatchCop sets `BatchCop` property.
func (builder *RequestBuilder) SetAllowBatchCop(batchCop bool) *RequestBuilder {
	builder.Request.BatchCop = batchCop
	return builder
}

func (builder *RequestBuilder) getIsolationLevel() kv.IsoLevel {
	switch builder.Tp {
	case kv.ReqTypeAnalyze:
		return kv.RC
	}
	return kv.SI
}

func (builder *RequestBuilder) getKVPriority(sv *variable.SessionVars) int {
	switch sv.StmtCtx.Priority {
	case mysql.NoPriority, mysql.DelayedPriority:
		return kv.PriorityNormal
	case mysql.LowPriority:
		return kv.PriorityLow
	case mysql.HighPriority:
		return kv.PriorityHigh
	}
	return kv.PriorityNormal
}

// SetFromSessionVars sets the following fields for "kv.Request" from session variables:
// "Concurrency", "IsolationLevel", "NotFillCache", "ReplicaRead", "SchemaVar".
func (builder *RequestBuilder) SetFromSessionVars(sv *variable.SessionVars) *RequestBuilder {
	if builder.Request.Concurrency == 0 {
		// Concurrency may be set to 1 by SetDAGRequest
		builder.Request.Concurrency = sv.DistSQLScanConcurrency()
	}
	builder.Request.IsolationLevel = builder.getIsolationLevel()
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	builder.Request.TaskID = sv.StmtCtx.TaskID
	builder.Request.Priority = builder.getKVPriority(sv)
	builder.Request.ReplicaRead = sv.GetReplicaRead()
	if sv.SnapshotInfoschema != nil {
		builder.Request.SchemaVar = infoschema.GetInfoSchemaBySessionVars(sv).SchemaMetaVersion()
	} else {
		builder.Request.SchemaVar = sv.TxnCtx.SchemaVersion
	}
	builder.txnScope = sv.TxnCtx.TxnScope
	return builder
}

// SetStreaming sets "Streaming" flag for "kv.Request".
func (builder *RequestBuilder) SetStreaming(streaming bool) *RequestBuilder {
	builder.Request.Streaming = streaming
	return builder
}

// SetConcurrency sets "Concurrency" for "kv.Request".
func (builder *RequestBuilder) SetConcurrency(concurrency int) *RequestBuilder {
	builder.Request.Concurrency = concurrency
	return builder
}

// SetTiDBServerID sets "TiDBServerID" for "kv.Request"
//   ServerID is a unique id of TiDB instance among the cluster.
//   See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md
func (builder *RequestBuilder) SetTiDBServerID(serverID uint64) *RequestBuilder {
	builder.Request.TiDBServerID = serverID
	return builder
}

// SetFromInfoSchema sets the following fields from infoSchema:
// "bundles"
func (builder *RequestBuilder) SetFromInfoSchema(is infoschema.InfoSchema) *RequestBuilder {
	if is == nil {
		return builder
	}
	builder.bundles = is.RuleBundles()
	return builder
}

func (builder *RequestBuilder) verifyTxnScope() error {
	if builder.txnScope == "" {
		builder.txnScope = oracle.GlobalTxnScope
	}
	if builder.txnScope == oracle.GlobalTxnScope || len(builder.bundles) < 1 {
		return nil
	}
	visitTableID := make(map[int64]struct{})
	for _, keyRange := range builder.Request.KeyRanges {
		tableID := tablecodec.DecodeTableID(keyRange.StartKey)
		if tableID > 0 {
			visitTableID[tableID] = struct{}{}
		} else {
			return errors.New("requestBuilder can't decode tableID from keyRange")
		}
	}

	for tableID := range visitTableID {
		valid := VerifyTxnScope(builder.txnScope, tableID, builder.bundles)
		if !valid {
			return fmt.Errorf("table %v can not be read by %v txn_scope", tableID, builder.txnScope)
		}
	}
	return nil
}

// TableHandleRangesToKVRanges convert table handle ranges to "KeyRanges" for multiple tables.
func TableHandleRangesToKVRanges(sc *stmtctx.StatementContext, tid []int64, isCommonHandle bool, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	if !isCommonHandle {
		return tablesRangesToKVRanges(tid, ranges, fb), nil
	}
	return CommonHandleRangesToKVRanges(sc, tid, ranges)
}

// TableRangesToKVRanges converts table ranges to "KeyRange".
// Note this function should not be exported, but currently
// br refers to it, so have to keep it.
func TableRangesToKVRanges(tid int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []kv.KeyRange {
	return tablesRangesToKVRanges([]int64{tid}, ranges, fb)
}

// tablesRangesToKVRanges converts table ranges to "KeyRange".
func tablesRangesToKVRanges(tids []int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []kv.KeyRange {
	if fb == nil || fb.Hist == nil {
		return tableRangesToKVRangesWithoutSplit(tids, ranges)
	}
	krs := make([]kv.KeyRange, 0, len(ranges))
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
		high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we store the range here.
		r := &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}}
		feedbackRanges = append(feedbackRanges, r)

		if !ran.HighExclude {
			high = kv.Key(high).PrefixNext()
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	fb.StoreRanges(feedbackRanges)
	return krs
}

func tableRangesToKVRangesWithoutSplit(tids []int64, ranges []*ranger.Range) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(ranges)*len(tids))
	for _, ran := range ranges {
		low, high := encodeHandleKey(ran)
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return krs
}

func encodeHandleKey(ran *ranger.Range) ([]byte, []byte) {
	low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
	high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}
	return low, high
}

// TableHandlesToKVRanges converts sorted handle to kv ranges.
// For continuous handles, we should merge them to a single key range.
func TableHandlesToKVRanges(tid int64, handles []kv.Handle) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		if commonHandle, ok := handles[i].(*kv.CommonHandle); ok {
			ran := kv.KeyRange{
				StartKey: tablecodec.EncodeRowKey(tid, commonHandle.Encoded()),
				EndKey:   tablecodec.EncodeRowKey(tid, kv.Key(commonHandle.Encoded()).Next()),
			}
			krs = append(krs, ran)
			i++
			continue
		}
		j := i + 1
		for ; j < len(handles) && handles[j-1].IntValue() != math.MaxInt64; j++ {
			if handles[j].IntValue() != handles[j-1].IntValue()+1 {
				break
			}
		}
		low := codec.EncodeInt(nil, handles[i].IntValue())
		high := codec.EncodeInt(nil, handles[j-1].IntValue())
		high = kv.Key(high).PrefixNext()
		startKey := tablecodec.EncodeRowKey(tid, low)
		endKey := tablecodec.EncodeRowKey(tid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

// PartitionHandlesToKVRanges convert ParitionHandles to kv ranges.
// Handle in slices must be kv.PartitionHandle
func PartitionHandlesToKVRanges(handles []kv.Handle) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		ph := handles[i].(kv.PartitionHandle)
		h := ph.Handle
		pid := ph.PartitionID
		if commonHandle, ok := h.(*kv.CommonHandle); ok {
			ran := kv.KeyRange{
				StartKey: tablecodec.EncodeRowKey(pid, commonHandle.Encoded()),
				EndKey:   tablecodec.EncodeRowKey(pid, append(commonHandle.Encoded(), 0)),
			}
			krs = append(krs, ran)
			i++
			continue
		}
		j := i + 1
		for ; j < len(handles) && handles[j-1].IntValue() != math.MaxInt64; j++ {
			if handles[j].IntValue() != handles[j-1].IntValue()+1 {
				break
			}
			if handles[j].(kv.PartitionHandle).PartitionID != pid {
				break
			}
		}
		low := codec.EncodeInt(nil, handles[i].IntValue())
		high := codec.EncodeInt(nil, handles[j-1].IntValue())
		high = kv.Key(high).PrefixNext()
		startKey := tablecodec.EncodeRowKey(pid, low)
		endKey := tablecodec.EncodeRowKey(pid, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

// IndexRangesToKVRanges converts index ranges to "KeyRange".
func IndexRangesToKVRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	return IndexRangesToKVRangesForTables(sc, []int64{tid}, idxID, ranges, fb)
}

// IndexRangesToKVRangesForTables converts indexes ranges to "KeyRange".
func IndexRangesToKVRangesForTables(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	if fb == nil || fb.Hist == nil {
		return indexRangesToKVWithoutSplit(sc, tids, idxID, ranges)
	}
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		feedbackRanges = append(feedbackRanges, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true})
	}
	feedbackRanges, ok := fb.Hist.SplitRange(sc, feedbackRanges, true)
	if !ok {
		fb.Invalidate()
	}
	krs := make([]kv.KeyRange, 0, len(feedbackRanges))
	for _, ran := range feedbackRanges {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we store the high value here.
		ran.HighVal[0].SetBytes(high)
		if !ran.HighExclude {
			high = kv.Key(high).PrefixNext()
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	fb.StoreRanges(feedbackRanges)
	return krs, nil
}

// CommonHandleRangesToKVRanges converts common handle ranges to "KeyRange".
func CommonHandleRangesToKVRanges(sc *stmtctx.StatementContext, tids []int64, ranges []*ranger.Range) ([]kv.KeyRange, error) {
	rans := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		rans = append(rans, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true})
	}
	krs := make([]kv.KeyRange, 0, len(rans))
	for _, ran := range rans {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return krs, nil
}

// VerifyTxnScope verify whether the txnScope and visited physical table break the leader rule's dcLocation.
func VerifyTxnScope(txnScope string, physicalTableID int64, bundles map[string]*placement.Bundle) bool {
	if txnScope == "" || txnScope == oracle.GlobalTxnScope {
		return true
	}
	bundle, ok := bundles[placement.GroupID(physicalTableID)]
	if !ok {
		return true
	}
	leaderDC, ok := placement.GetLeaderDCByBundle(bundle, placement.DCLabelKey)
	if !ok {
		return true
	}
	if leaderDC != txnScope {
		return false
	}
	return true
}

func indexRangesToKVWithoutSplit(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return krs, nil
}

func encodeIndexKey(sc *stmtctx.StatementContext, ran *ranger.Range) ([]byte, []byte, error) {
	low, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	high, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return nil, nil, err
	}

	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}

	var hasNull bool
	for _, highVal := range ran.HighVal {
		if highVal.IsNull() {
			hasNull = true
			break
		}
	}

	if hasNull {
		// Append 0 to make unique-key range [null, null] to be a scan rather than point-get.
		high = kv.Key(high).Next()
	}
	return low, high, nil
}
