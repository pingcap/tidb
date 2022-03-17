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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
)

// RequestBuilder is used to build a "kv.Request".
// It is called before we issue a kv request by "Select".
type RequestBuilder struct {
	kv.Request
	is  infoschema.InfoSchema
	err error
}

// Build builds a "kv.Request".
func (builder *RequestBuilder) Build() (*kv.Request, error) {
	if builder.ReadReplicaScope == "" {
		builder.ReadReplicaScope = kv.GlobalReplicaScope
	}
	if builder.ReplicaRead.IsClosestRead() && builder.ReadReplicaScope != kv.GlobalReplicaScope {
		builder.MatchStoreLabels = []*metapb.StoreLabel{
			{
				Key:   placement.DCLabelKey,
				Value: builder.ReadReplicaScope,
			},
		}
	}
	failpoint.Inject("assertRequestBuilderReplicaOption", func(val failpoint.Value) {
		assertScope := val.(string)
		if builder.ReplicaRead.IsClosestRead() && assertScope != builder.ReadReplicaScope {
			panic("request builder get staleness option fail")
		}
	})
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

// SetPartitionIDAndRanges sets `PartitionIDAndRanges` property.
func (builder *RequestBuilder) SetPartitionIDAndRanges(PartitionIDAndRanges []kv.PartitionIDAndRanges) *RequestBuilder {
	builder.PartitionIDAndRanges = PartitionIDAndRanges
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
// "Concurrency", "IsolationLevel", "NotFillCache", "TaskID", "Priority", "ReplicaRead", "ResourceGroupTagger".
func (builder *RequestBuilder) SetFromSessionVars(sv *variable.SessionVars) *RequestBuilder {
	if builder.Request.Concurrency == 0 {
		// Concurrency may be set to 1 by SetDAGRequest
		builder.Request.Concurrency = sv.DistSQLScanConcurrency()
	}
	replicaReadType := sv.GetReplicaRead()
	if sv.StmtCtx.WeakConsistency {
		builder.Request.IsolationLevel = kv.RC
	} else if sv.StmtCtx.RCCheckTS {
		builder.Request.IsolationLevel = kv.RCCheckTS
		replicaReadType = kv.ReplicaReadLeader
	} else {
		builder.Request.IsolationLevel = builder.getIsolationLevel()
	}
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	builder.Request.TaskID = sv.StmtCtx.TaskID
	builder.Request.Priority = builder.getKVPriority(sv)
	builder.Request.ReplicaRead = replicaReadType
	builder.SetResourceGroupTagger(sv.StmtCtx)
	return builder
}

// SetStreaming sets "Streaming" flag for "kv.Request".
func (builder *RequestBuilder) SetStreaming(streaming bool) *RequestBuilder {
	builder.Request.Streaming = streaming
	return builder
}

// SetPaging sets "Paging" flag for "kv.Request".
func (builder *RequestBuilder) SetPaging(paging bool) *RequestBuilder {
	builder.Request.Paging = paging
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
func (builder *RequestBuilder) SetFromInfoSchema(pis interface{}) *RequestBuilder {
	is, ok := pis.(infoschema.InfoSchema)
	if !ok {
		return builder
	}
	builder.is = is
	builder.Request.SchemaVar = is.SchemaMetaVersion()
	return builder
}

// SetResourceGroupTagger sets the request resource group tagger.
func (builder *RequestBuilder) SetResourceGroupTagger(sc *stmtctx.StatementContext) *RequestBuilder {
	if topsqlstate.TopSQLEnabled() {
		builder.Request.ResourceGroupTagger = sc.GetResourceGroupTagger()
	}
	return builder
}

func (builder *RequestBuilder) verifyTxnScope() error {
	// Stale Read uses the calculated TSO for the read,
	// so there is no need to check the TxnScope here.
	if builder.IsStaleness {
		return nil
	}
	if builder.ReadReplicaScope == "" {
		builder.ReadReplicaScope = kv.GlobalReplicaScope
	}
	if builder.ReadReplicaScope == kv.GlobalReplicaScope || builder.is == nil {
		return nil
	}
	visitPhysicalTableID := make(map[int64]struct{})
	for _, keyRange := range builder.Request.KeyRanges {
		tableID := tablecodec.DecodeTableID(keyRange.StartKey)
		if tableID > 0 {
			visitPhysicalTableID[tableID] = struct{}{}
		} else {
			return errors.New("requestBuilder can't decode tableID from keyRange")
		}
	}

	for phyTableID := range visitPhysicalTableID {
		valid := VerifyTxnScope(builder.ReadReplicaScope, phyTableID, builder.is)
		if !valid {
			var tblName string
			var partName string
			tblInfo, _, partInfo := builder.is.FindTableByPartitionID(phyTableID)
			if tblInfo != nil && partInfo != nil {
				tblName = tblInfo.Meta().Name.String()
				partName = partInfo.Name.String()
			} else {
				tblInfo, _ = builder.is.TableByID(phyTableID)
				tblName = tblInfo.Meta().Name.String()
			}
			err := fmt.Errorf("table %v can not be read by %v txn_scope", tblName, builder.ReadReplicaScope)
			if len(partName) > 0 {
				err = fmt.Errorf("table %v's partition %v can not be read by %v txn_scope",
					tblName, partName, builder.ReadReplicaScope)
			}
			return err
		}
	}
	return nil
}

// SetReadReplicaScope sets request readReplicaScope
func (builder *RequestBuilder) SetReadReplicaScope(scope string) *RequestBuilder {
	builder.ReadReplicaScope = scope
	return builder
}

// SetIsStaleness sets request IsStaleness
func (builder *RequestBuilder) SetIsStaleness(is bool) *RequestBuilder {
	builder.IsStaleness = is
	return builder
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
			HighVal: []types.Datum{types.NewBytesDatum(high)}, Collators: collate.GetBinaryCollatorSlice(1)}
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

// SplitRangesAcrossInt64Boundary split the ranges into two groups:
// 1. signedRanges is less or equal than MaxInt64
// 2. unsignedRanges is greater than MaxInt64
//
// We do this because every key of tikv is encoded as an int64. As a result, MaxUInt64 is small than zero when
// interpreted as an int64 variable.
//
// This function does the following:
// 1. split ranges into two groups as described above.
// 2. if there's a range that straddles the int64 boundary, split it into two ranges, which results in one smaller and
//    one greater than MaxInt64.
//
// if `KeepOrder` is false, we merge the two groups of ranges into one group, to save an rpc call later
// if `desc` is false, return signed ranges first, vice versa.
func SplitRangesAcrossInt64Boundary(ranges []*ranger.Range, keepOrder bool, desc bool, isCommonHandle bool) ([]*ranger.Range, []*ranger.Range) {
	if isCommonHandle || len(ranges) == 0 || ranges[0].LowVal[0].Kind() == types.KindInt64 {
		return ranges, nil
	}
	idx := sort.Search(len(ranges), func(i int) bool { return ranges[i].HighVal[0].GetUint64() > math.MaxInt64 })
	if idx == len(ranges) {
		return ranges, nil
	}
	if ranges[idx].LowVal[0].GetUint64() > math.MaxInt64 {
		signedRanges := ranges[0:idx]
		unsignedRanges := ranges[idx:]
		if !keepOrder {
			return append(unsignedRanges, signedRanges...), nil
		}
		if desc {
			return unsignedRanges, signedRanges
		}
		return signedRanges, unsignedRanges
	}
	// need to split the range that straddles the int64 boundary
	signedRanges := make([]*ranger.Range, 0, idx+1)
	unsignedRanges := make([]*ranger.Range, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	if !(ranges[idx].LowVal[0].GetUint64() == math.MaxInt64 && ranges[idx].LowExclude) {
		signedRanges = append(signedRanges, &ranger.Range{
			LowVal:     ranges[idx].LowVal,
			LowExclude: ranges[idx].LowExclude,
			HighVal:    []types.Datum{types.NewUintDatum(math.MaxInt64)},
			Collators:  ranges[idx].Collators,
		})
	}
	if !(ranges[idx].HighVal[0].GetUint64() == math.MaxInt64+1 && ranges[idx].HighExclude) {
		unsignedRanges = append(unsignedRanges, &ranger.Range{
			LowVal:      []types.Datum{types.NewUintDatum(math.MaxInt64 + 1)},
			HighVal:     ranges[idx].HighVal,
			HighExclude: ranges[idx].HighExclude,
			Collators:   ranges[idx].Collators,
		})
	}
	if idx < len(ranges) {
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	if !keepOrder {
		return append(unsignedRanges, signedRanges...), nil
	}
	if desc {
		return unsignedRanges, signedRanges
	}
	return signedRanges, unsignedRanges
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
	return IndexRangesToKVRangesWithInterruptSignal(sc, tid, idxID, ranges, fb, nil, nil)
}

// IndexRangesToKVRangesWithInterruptSignal converts index ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func IndexRangesToKVRangesWithInterruptSignal(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	return indexRangesToKVRangesForTablesWithInterruptSignal(sc, []int64{tid}, idxID, ranges, fb, memTracker, interruptSignal)
}

// IndexRangesToKVRangesForTables converts indexes ranges to "KeyRange".
func IndexRangesToKVRangesForTables(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	return indexRangesToKVRangesForTablesWithInterruptSignal(sc, tids, idxID, ranges, fb, nil, nil)
}

// IndexRangesToKVRangesForTablesWithInterruptSignal converts indexes ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func indexRangesToKVRangesForTablesWithInterruptSignal(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	if fb == nil || fb.Hist == nil {
		return indexRangesToKVWithoutSplit(sc, tids, idxID, ranges, memTracker, interruptSignal)
	}
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		feedbackRanges = append(feedbackRanges, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true, Collators: collate.GetBinaryCollatorSlice(1)})
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
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true, Collators: collate.GetBinaryCollatorSlice(1)})
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
func VerifyTxnScope(txnScope string, physicalTableID int64, is infoschema.InfoSchema) bool {
	if txnScope == "" || txnScope == kv.GlobalTxnScope {
		return true
	}
	bundle, ok := is.BundleByName(placement.GroupID(physicalTableID))
	if !ok {
		return true
	}
	leaderDC, ok := bundle.GetLeaderDC(placement.DCLabelKey)
	if !ok {
		return true
	}
	if leaderDC != txnScope {
		return false
	}
	return true
}

func indexRangesToKVWithoutSplit(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	const CheckSignalStep = 8
	var estimatedMemUsage int64
	// encodeIndexKey and EncodeIndexSeekKey is time-consuming, thus we need to
	// check the interrupt signal periodically.
	for i, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			estimatedMemUsage += int64(cap(low) + cap(high))
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			if i == 0 {
				estimatedMemUsage += int64(cap(startKey)) + int64(cap(endKey))
			}
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
		if i%CheckSignalStep == 0 {
			if i == 0 && memTracker != nil {
				estimatedMemUsage *= int64(len(ranges))
				memTracker.Consume(estimatedMemUsage)
			}
			if interruptSignal != nil && interruptSignal.Load().(bool) {
				return nil, nil
			}
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

	// NOTE: this is a hard-code operation to avoid wrong results when accessing unique index with NULL;
	// Please see https://github.com/pingcap/tidb/issues/29650 for more details
	if hasNull {
		// Append 0 to make unique-key range [null, null] to be a scan rather than point-get.
		high = kv.Key(high).Next()
	}
	return low, high, nil
}
