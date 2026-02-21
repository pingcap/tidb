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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

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
