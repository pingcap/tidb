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
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// RequestBuilder is used to build a "kv.Request".
// It is called before we issue a kv request by "Select".
type RequestBuilder struct {
	kv.Request
	is  infoschema.MetaOnlyInfoSchema
	err error

	// When SetDAGRequest is called, builder will also this field.
	dag *tipb.DAGRequest
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
	if builder.Request.KeyRanges == nil {
		builder.Request.KeyRanges = kv.NewNonPartitionedKeyRanges(nil)
	}

	if dag := builder.dag; dag != nil {
		if execCnt := len(dag.Executors); execCnt == 1 {
			// select * from t order by id
			if builder.Request.KeepOrder && builder.Request.Concurrency == vardef.DefDistSQLScanConcurrency {
				// When the DAG is just simple scan and keep order, set concurrency to 2.
				// If a lot data are returned to client, mysql protocol is the bottleneck so concurrency 2 is enough.
				// If very few data are returned to client, the speed is not optimal but good enough.
				//
				// If a user set @@tidb_distsql_scan_concurrency, he must be doing it by intention.
				// so only rewrite concurrency when @@tidb_distsql_scan_concurrency is default value.
				switch dag.Executors[0].Tp {
				case tipb.ExecType_TypeTableScan, tipb.ExecType_TypeIndexScan, tipb.ExecType_TypePartitionTableScan:
					oldConcurrency := builder.Request.Concurrency
					builder.Request.Concurrency = 2
					failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
						if val.(bool) {
							// When the concurrency is too small, test case tests/realtikvtest/sessiontest.TestCoprocessorOOMAction can't trigger OOM condition
							builder.Request.Concurrency = oldConcurrency
						}
					})
				}
			}
		}
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
func (builder *RequestBuilder) SetTableRanges(tid int64, tableRanges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges = kv.NewNonPartitionedKeyRanges(TableRangesToKVRanges(tid, tableRanges))
	}
	return builder
}

// SetIndexRanges sets "KeyRanges" for "kv.Request" by converting index range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRanges(dctx *distsqlctx.DistSQLContext, tid, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = IndexRangesToKVRanges(dctx, tid, idxID, ranges)
	}
	return builder
}

// SetIndexRangesForTables sets "KeyRanges" for "kv.Request" by converting multiple indexes range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetIndexRangesForTables(dctx *distsqlctx.DistSQLContext, tids []int64, idxID int64, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = IndexRangesToKVRangesForTables(dctx, tids, idxID, ranges)
	}
	return builder
}

// SetHandleRanges sets "KeyRanges" for "kv.Request" by converting table handle range
// "ranges" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetHandleRanges(dctx *distsqlctx.DistSQLContext, tid int64, isCommonHandle bool, ranges []*ranger.Range) *RequestBuilder {
	builder = builder.SetHandleRangesForTables(dctx, []int64{tid}, isCommonHandle, ranges)
	builder.err = builder.Request.KeyRanges.SetToNonPartitioned()
	return builder
}

// SetHandleRangesForTables sets "KeyRanges" for "kv.Request" by converting table handle range
// "ranges" to "KeyRanges" firstly for multiple tables.
func (builder *RequestBuilder) SetHandleRangesForTables(dctx *distsqlctx.DistSQLContext, tid []int64, isCommonHandle bool, ranges []*ranger.Range) *RequestBuilder {
	if builder.err == nil {
		builder.Request.KeyRanges, builder.err = TableHandleRangesToKVRanges(dctx, tid, isCommonHandle, ranges)
	}
	return builder
}

// SetTableHandles sets "KeyRanges" for "kv.Request" by converting table handles
// "handles" to "KeyRanges" firstly.
func (builder *RequestBuilder) SetTableHandles(tid int64, handles []kv.Handle) *RequestBuilder {
	keyRanges, hints := TableHandlesToKVRanges(tid, handles)
	builder.Request.KeyRanges = kv.NewNonParitionedKeyRangesWithHint(keyRanges, hints)
	return builder
}

// SetPartitionsAndHandles sets "KeyRanges" for "kv.Request" by converting ParitionHandles to KeyRanges.
// handles in slice must be kv.PartitionHandle.
func (builder *RequestBuilder) SetPartitionsAndHandles(handles []kv.Handle) *RequestBuilder {
	keyRanges, hints := PartitionHandlesToKVRanges(handles)
	builder.Request.KeyRanges = kv.NewNonParitionedKeyRangesWithHint(keyRanges, hints)
	return builder
}

const estimatedRegionRowCount = 100000

// SetDAGRequest sets the request type to "ReqTypeDAG" and construct request data.
func (builder *RequestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = kv.ReqTypeDAG
		builder.Request.Cacheable = true
		builder.Request.Data, builder.err = dag.Marshal()
		builder.dag = dag
		if execCnt := len(dag.Executors); execCnt != 0 && dag.Executors[execCnt-1].GetLimit() != nil {
			limit := dag.Executors[execCnt-1].GetLimit()
			builder.Request.LimitSize = limit.GetLimit()
			// When the DAG is just simple scan and small limit, set concurrency to 1 would be sufficient.
			if execCnt == 2 {
				if limit.Limit < estimatedRegionRowCount {
					if kr := builder.Request.KeyRanges; kr != nil {
						builder.Request.Concurrency = kr.PartitionNum()
					} else {
						builder.Request.Concurrency = 1
					}
				}
			}
		}
	}
	return builder
}

// SetAnalyzeRequest sets the request type to "ReqTypeAnalyze" and construct request data.
func (builder *RequestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq, isoLevel kv.IsoLevel) *RequestBuilder {
	if builder.err == nil {
		builder.Request.Tp = kv.ReqTypeAnalyze
		builder.Request.Data, builder.err = ana.Marshal()
		builder.Request.NotFillCache = true
		builder.Request.IsolationLevel = isoLevel
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
	builder.Request.KeyRanges = kv.NewNonPartitionedKeyRanges(keyRanges)
	return builder
}

// SetKeyRangesWithHints sets "KeyRanges" for "kv.Request" with row count hints.
func (builder *RequestBuilder) SetKeyRangesWithHints(keyRanges []kv.KeyRange, hints []int) *RequestBuilder {
	builder.Request.KeyRanges = kv.NewNonParitionedKeyRangesWithHint(keyRanges, hints)
	return builder
}

// SetWrappedKeyRanges sets "KeyRanges" for "kv.Request".
func (builder *RequestBuilder) SetWrappedKeyRanges(keyRanges *kv.KeyRanges) *RequestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

// SetPartitionKeyRanges sets the "KeyRanges" for "kv.Request" on partitioned table cases.
func (builder *RequestBuilder) SetPartitionKeyRanges(keyRanges [][]kv.KeyRange) *RequestBuilder {
	builder.Request.KeyRanges = kv.NewPartitionedKeyRanges(keyRanges)
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
func (builder *RequestBuilder) SetPartitionIDAndRanges(partitionIDAndRanges []kv.PartitionIDAndRanges) *RequestBuilder {
	builder.PartitionIDAndRanges = partitionIDAndRanges
	return builder
}

func (builder *RequestBuilder) getIsolationLevel() kv.IsoLevel {
	if builder.Tp == kv.ReqTypeAnalyze {
		return kv.RC
	}
	return kv.SI
}

func (*RequestBuilder) getKVPriority(dctx *distsqlctx.DistSQLContext) int {
	switch dctx.Priority {
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
// "Concurrency", "IsolationLevel", "NotFillCache", "TaskID", "Priority", "ReplicaRead",
// "ResourceGroupTagger", "ResourceGroupName"
func (builder *RequestBuilder) SetFromSessionVars(dctx *distsqlctx.DistSQLContext) *RequestBuilder {
	distsqlConcurrency := dctx.DistSQLConcurrency
	if builder.Request.Concurrency == 0 {
		// Concurrency unset.
		builder.Request.Concurrency = distsqlConcurrency
	} else if builder.Request.Concurrency > distsqlConcurrency {
		// Concurrency is set in SetDAGRequest, check the upper limit.
		builder.Request.Concurrency = distsqlConcurrency
	}
	replicaReadType := dctx.ReplicaReadType
	if dctx.WeakConsistency {
		builder.Request.IsolationLevel = kv.RC
	} else if dctx.RCCheckTS {
		builder.Request.IsolationLevel = kv.RCCheckTS
		replicaReadType = kv.ReplicaReadLeader
	} else {
		builder.Request.IsolationLevel = builder.getIsolationLevel()
	}
	builder.Request.NotFillCache = dctx.NotFillCache
	builder.Request.TaskID = dctx.TaskID
	builder.Request.Priority = builder.getKVPriority(dctx)
	builder.Request.ReplicaRead = replicaReadType
	builder.SetResourceGroupTagger(dctx.ResourceGroupTagger)
	{
		builder.SetPaging(dctx.EnablePaging)
		builder.Request.Paging.MinPagingSize = uint64(dctx.MinPagingSize)
		builder.Request.Paging.MaxPagingSize = uint64(dctx.MaxPagingSize)
	}
	builder.RequestSource.RequestSourceInternal = dctx.InRestrictedSQL
	builder.RequestSource.RequestSourceType = dctx.RequestSourceType
	builder.RequestSource.ExplicitRequestSourceType = dctx.ExplicitRequestSourceType
	builder.StoreBatchSize = dctx.StoreBatchSize
	builder.Request.ResourceGroupName = dctx.ResourceGroupName
	builder.Request.StoreBusyThreshold = dctx.LoadBasedReplicaReadThreshold
	builder.Request.RunawayChecker = dctx.RunawayChecker
	builder.Request.TiKVClientReadTimeout = dctx.TiKVClientReadTimeout
	builder.Request.MaxExecutionTime = dctx.MaxExecutionTime
	return builder
}

// SetPaging sets "Paging" flag for "kv.Request".
func (builder *RequestBuilder) SetPaging(paging bool) *RequestBuilder {
	builder.Request.Paging.Enable = paging
	return builder
}

// SetConcurrency sets "Concurrency" for "kv.Request".
func (builder *RequestBuilder) SetConcurrency(concurrency int) *RequestBuilder {
	builder.Request.Concurrency = concurrency
	return builder
}

// SetTiDBServerID sets "TiDBServerID" for "kv.Request"
//
//	ServerID is a unique id of TiDB instance among the cluster.
//	See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md
func (builder *RequestBuilder) SetTiDBServerID(serverID uint64) *RequestBuilder {
	builder.Request.TiDBServerID = serverID
	return builder
}

// SetFromInfoSchema sets the following fields from infoSchema:
// "bundles"
func (builder *RequestBuilder) SetFromInfoSchema(is infoschema.MetaOnlyInfoSchema) *RequestBuilder {
	builder.is = is
	builder.Request.SchemaVar = is.SchemaMetaVersion()
	return builder
}

// SetResourceGroupTagger sets the request resource group tagger.
func (builder *RequestBuilder) SetResourceGroupTagger(tagger *kv.ResourceGroupTagBuilder) *RequestBuilder {
	builder.Request.ResourceGroupTagger = tagger
	return builder
}

// SetResourceGroupName sets the request resource group name.
func (builder *RequestBuilder) SetResourceGroupName(name string) *RequestBuilder {
	builder.Request.ResourceGroupName = name
	return builder
}

// SetExplicitRequestSourceType sets the explicit request source type.
func (builder *RequestBuilder) SetExplicitRequestSourceType(sourceType string) *RequestBuilder {
	builder.RequestSource.ExplicitRequestSourceType = sourceType
	return builder
}

func (builder *RequestBuilder) verifyTxnScope() error {
	txnScope := builder.TxnScope
	if txnScope == "" || txnScope == kv.GlobalReplicaScope || builder.is == nil {
		return nil
	}
	visitPhysicalTableID := make(map[int64]struct{})
	tids, err := tablecodec.VerifyTableIDForRanges(builder.Request.KeyRanges)
	if err != nil {
		return err
	}
	for _, tid := range tids {
		visitPhysicalTableID[tid] = struct{}{}
	}

	for phyTableID := range visitPhysicalTableID {
		valid := VerifyTxnScope(txnScope, phyTableID, builder.is)
		if !valid {
			var tblName string
			var partName string
			tblInfo, _, partInfo := builder.is.FindTableInfoByPartitionID(phyTableID)
			if tblInfo != nil && partInfo != nil {
				tblName = tblInfo.Name.String()
				partName = partInfo.Name.String()
			} else {
				tblInfo, _ = builder.is.TableInfoByID(phyTableID)
				tblName = tblInfo.Name.String()
			}
			err := fmt.Errorf("table %v can not be read by %v txn_scope", tblName, txnScope)
			if len(partName) > 0 {
				err = fmt.Errorf("table %v's partition %v can not be read by %v txn_scope",
					tblName, partName, txnScope)
			}
			return err
		}
	}
	return nil
}

// SetTxnScope sets request TxnScope
func (builder *RequestBuilder) SetTxnScope(scope string) *RequestBuilder {
	builder.TxnScope = scope
	return builder
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

// SetClosestReplicaReadAdjuster sets request CoprRequestAdjuster
func (builder *RequestBuilder) SetClosestReplicaReadAdjuster(chkFn kv.CoprRequestAdjuster) *RequestBuilder {
	builder.ClosestReplicaReadAdjuster = chkFn
	return builder
}

// SetConnIDAndConnAlias sets connection id for the builder.
func (builder *RequestBuilder) SetConnIDAndConnAlias(connID uint64, connAlias string) *RequestBuilder {
	builder.ConnID = connID
	builder.ConnAlias = connAlias
	return builder
}

// TableHandleRangesToKVRanges convert table handle ranges to "KeyRanges" for multiple tables.
func TableHandleRangesToKVRanges(dctx *distsqlctx.DistSQLContext, tid []int64, isCommonHandle bool, ranges []*ranger.Range) (*kv.KeyRanges, error) {
	if !isCommonHandle {
		return tablesRangesToKVRanges(tid, ranges), nil
	}
	return CommonHandleRangesToKVRanges(dctx, tid, ranges)
}

// TableRangesToKVRanges converts table ranges to "KeyRange".
// Note this function should not be exported, but currently
// br refers to it, so have to keep it.
func TableRangesToKVRanges(tid int64, ranges []*ranger.Range) []kv.KeyRange {
	if len(ranges) == 0 {
		return []kv.KeyRange{}
	}
	return tablesRangesToKVRanges([]int64{tid}, ranges).FirstPartitionRange()
}

// tablesRangesToKVRanges converts table ranges to "KeyRange".
func tablesRangesToKVRanges(tids []int64, ranges []*ranger.Range) *kv.KeyRanges {
	return tableRangesToKVRangesWithoutSplit(tids, ranges)
}

func tableRangesToKVRangesWithoutSplit(tids []int64, ranges []*ranger.Range) *kv.KeyRanges {
	krs := make([][]kv.KeyRange, len(tids))
	for i := range krs {
		krs[i] = make([]kv.KeyRange, 0, len(ranges))
	}
	for _, ran := range ranges {
		low, high := encodeHandleKey(ran)
		for i, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs[i] = append(krs[i], kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return kv.NewPartitionedKeyRanges(krs)
}

func encodeHandleKey(ran *ranger.Range) (low, high []byte) {
	low = codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
	high = codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
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
// We do this because every key of tikv is encoded as an int64. As a result, MaxUInt64 is smaller than zero when
// interpreted as an int64 variable.
//
// This function does the following:
//  1. split ranges into two groups as described above.
//  2. if there's a range that straddles the int64 boundary, split it into two ranges, which results in one smaller and
//     one greater than MaxInt64.
//
// if `KeepOrder` is false, we merge the two groups of ranges into one group, to save a rpc call later
// if `desc` is false, return signed ranges first, vice versa.
func SplitRangesAcrossInt64Boundary(ranges []*ranger.Range, keepOrder bool, desc bool, isCommonHandle bool) (signedRanges, unsignedRanges []*ranger.Range) {
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
	signedRanges = make([]*ranger.Range, 0, idx+1)
	unsignedRanges = make([]*ranger.Range, 0, len(ranges)-idx)
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
func TableHandlesToKVRanges(tid int64, handles []kv.Handle) ([]kv.KeyRange, []int) {
	krs := make([]kv.KeyRange, 0, len(handles))
	hints := make([]int, 0, len(handles))
	i := 0
	for i < len(handles) {
		var isCommonHandle bool
		var commonHandle *kv.CommonHandle
		if partitionHandle, ok := handles[i].(kv.PartitionHandle); ok {
			tid = partitionHandle.PartitionID
			commonHandle, isCommonHandle = partitionHandle.Handle.(*kv.CommonHandle)
		} else {
			commonHandle, isCommonHandle = handles[i].(*kv.CommonHandle)
		}
		if isCommonHandle {
			ran := kv.KeyRange{
				StartKey: tablecodec.EncodeRowKey(tid, commonHandle.Encoded()),
				EndKey:   tablecodec.EncodeRowKey(tid, kv.Key(commonHandle.Encoded()).Next()),
			}
			krs = append(krs, ran)
			hints = append(hints, 1)
			i++
			continue
		}
		j := i + 1
		for ; j < len(handles) && handles[j-1].IntValue() != math.MaxInt64; j++ {
			if p, ok := handles[j].(kv.PartitionHandle); ok && p.PartitionID != tid {
				break
			}
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
		hints = append(hints, j-i)
		i = j
	}
	return krs, hints
}

// PartitionHandlesToKVRanges convert ParitionHandles to kv ranges.
// Handle in slices must be kv.PartitionHandle
func PartitionHandlesToKVRanges(handles []kv.Handle) ([]kv.KeyRange, []int) {
	krs := make([]kv.KeyRange, 0, len(handles))
	hints := make([]int, 0, len(handles))
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
			hints = append(hints, 1)
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
		hints = append(hints, j-i)
		i = j
	}
	return krs, hints
}

// IndexRangesToKVRanges converts index ranges to "KeyRange".
func IndexRangesToKVRanges(dctx *distsqlctx.DistSQLContext, tid, idxID int64, ranges []*ranger.Range) (*kv.KeyRanges, error) {
	return IndexRangesToKVRangesWithInterruptSignal(dctx, tid, idxID, ranges, nil, nil)
}

// IndexRangesToKVRangesWithInterruptSignal converts index ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func IndexRangesToKVRangesWithInterruptSignal(dctx *distsqlctx.DistSQLContext, tid, idxID int64, ranges []*ranger.Range, memTracker *memory.Tracker, interruptSignal *atomic.Value) (*kv.KeyRanges, error) {
	keyRanges, err := indexRangesToKVRangesForTablesWithInterruptSignal(dctx, []int64{tid}, idxID, ranges, memTracker, interruptSignal)
	if err != nil {
		return nil, err
	}
	err = keyRanges.SetToNonPartitioned()
	return keyRanges, err
}

// IndexRangesToKVRangesForTables converts indexes ranges to "KeyRange".
func IndexRangesToKVRangesForTables(dctx *distsqlctx.DistSQLContext, tids []int64, idxID int64, ranges []*ranger.Range) (*kv.KeyRanges, error) {
	return indexRangesToKVRangesForTablesWithInterruptSignal(dctx, tids, idxID, ranges, nil, nil)
}

// IndexRangesToKVRangesForTablesWithInterruptSignal converts indexes ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func indexRangesToKVRangesForTablesWithInterruptSignal(dctx *distsqlctx.DistSQLContext, tids []int64, idxID int64, ranges []*ranger.Range, memTracker *memory.Tracker, interruptSignal *atomic.Value) (*kv.KeyRanges, error) {
	return indexRangesToKVWithoutSplit(dctx, tids, idxID, ranges, memTracker, interruptSignal)
}

// CommonHandleRangesToKVRanges converts common handle ranges to "KeyRange".
func CommonHandleRangesToKVRanges(dctx *distsqlctx.DistSQLContext, tids []int64, ranges []*ranger.Range) (*kv.KeyRanges, error) {
	rans := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := EncodeIndexKey(dctx, ran)
		if err != nil {
			return nil, err
		}
		rans = append(rans, &ranger.Range{
			LowVal:      []types.Datum{types.NewBytesDatum(low)},
			HighVal:     []types.Datum{types.NewBytesDatum(high)},
			LowExclude:  false,
			HighExclude: true,
			Collators:   collate.GetBinaryCollatorSlice(1),
		})
	}
	krs := make([][]kv.KeyRange, len(tids))
	for i := range krs {
		krs[i] = make([]kv.KeyRange, 0, len(ranges))
	}
	for _, ran := range rans {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		for i, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs[i] = append(krs[i], kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return kv.NewPartitionedKeyRanges(krs), nil
}

// VerifyTxnScope verify whether the txnScope and visited physical table break the leader rule's dcLocation.
func VerifyTxnScope(txnScope string, physicalTableID int64, is infoschema.MetaOnlyInfoSchema) bool {
	if txnScope == "" || txnScope == kv.GlobalTxnScope {
		return true
	}
	bundle, ok := is.PlacementBundleByPhysicalTableID(physicalTableID)
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

func indexRangesToKVWithoutSplit(dctx *distsqlctx.DistSQLContext, tids []int64, idxID int64, ranges []*ranger.Range, memTracker *memory.Tracker, interruptSignal *atomic.Value) (*kv.KeyRanges, error) {
	krs := make([][]kv.KeyRange, len(tids))
	for i := range krs {
		krs[i] = make([]kv.KeyRange, 0, len(ranges))
	}

	if memTracker != nil {
		memTracker.Consume(int64(unsafe.Sizeof(kv.KeyRange{})) * int64(len(ranges)))
	}
	const checkSignalStep = 8
	var estimatedMemUsage int64
	// encodeIndexKey and EncodeIndexSeekKey is time-consuming, thus we need to
	// check the interrupt signal periodically.
	for i, ran := range ranges {
		low, high, err := EncodeIndexKey(dctx, ran)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			estimatedMemUsage += int64(cap(low) + cap(high))
		}
		for j, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			if i == 0 {
				estimatedMemUsage += int64(cap(startKey)) + int64(cap(endKey))
			}
			krs[j] = append(krs[j], kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
		if i%checkSignalStep == 0 {
			if i == 0 && memTracker != nil {
				estimatedMemUsage *= int64(len(ranges))
				memTracker.Consume(estimatedMemUsage)
			}
			if interruptSignal != nil && interruptSignal.Load().(bool) {
				return kv.NewPartitionedKeyRanges(nil), nil
			}
			if memTracker != nil {
				memTracker.HandleKillSignal()
			}
		}
	}
	return kv.NewPartitionedKeyRanges(krs), nil
}

// EncodeIndexKey gets encoded keys containing low and high
func EncodeIndexKey(dctx *distsqlctx.DistSQLContext, ran *ranger.Range) (low, high []byte, err error) {
	tz := time.UTC
	errCtx := errctx.StrictNoWarningContext
	if dctx != nil {
		tz = dctx.Location
		errCtx = dctx.ErrCtx
	}

	low, err = codec.EncodeKey(tz, nil, ran.LowVal...)
	err = errCtx.HandleError(err)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	high, err = codec.EncodeKey(tz, nil, ran.HighVal...)
	err = errCtx.HandleError(err)
	if err != nil {
		return nil, nil, err
	}

	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}
	return low, high, nil
}

// BuildTableRanges returns the key ranges encompassing the entire table,
// and its partitions if exists.
func BuildTableRanges(tbl *model.TableInfo) ([]kv.KeyRange, error) {
	pis := tbl.GetPartitionInfo()
	if pis == nil {
		// Short path, no partition.
		return appendRanges(tbl, tbl.ID)
	}

	ranges := make([]kv.KeyRange, 0, len(pis.Definitions)*(len(tbl.Indices)+1)+1)
	// Handle global index ranges
	for _, idx := range tbl.Indices {
		if idx.State != model.StatePublic || !idx.Global {
			continue
		}
		idxRanges, err := IndexRangesToKVRanges(nil, tbl.ID, idx.ID, ranger.FullRange())
		if err != nil {
			return nil, err
		}
		ranges = idxRanges.AppendSelfTo(ranges)
	}

	for _, def := range pis.Definitions {
		rgs, err := appendRanges(tbl, def.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ranges = append(ranges, rgs...)
	}
	return ranges, nil
}

func appendRanges(tbl *model.TableInfo, tblID int64) ([]kv.KeyRange, error) {
	var ranges []*ranger.Range
	if tbl.IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}

	retRanges := make([]kv.KeyRange, 0, 1+len(tbl.Indices))
	kvRanges, err := TableHandleRangesToKVRanges(nil, []int64{tblID}, tbl.IsCommonHandle, ranges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	retRanges = kvRanges.AppendSelfTo(retRanges)

	for _, index := range tbl.Indices {
		if index.State != model.StatePublic || index.Global {
			continue
		}
		ranges = ranger.FullRange()
		idxRanges, err := IndexRangesToKVRanges(nil, tblID, index.ID, ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
		retRanges = idxRanges.AppendSelfTo(retRanges)
	}
	return retRanges, nil
}
