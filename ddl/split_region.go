// Copyright 2019 PingCAP, Inc.
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

package ddl

import (
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func splitPartitionTableRegion(store kv.SplitableStore, pi *model.PartitionInfo, scatter bool) {
	// Max partition count is 4096, should we sample and just choose some of the partition to split?
	regionIDs := make([]uint64, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		regionIDs = append(regionIDs, splitRecordRegion(store, def.ID, scatter))
	}
	if scatter {
		waitScatterRegionFinish(store, regionIDs...)
	}
}

func splitTableRegion(store kv.SplitableStore, tbInfo *model.TableInfo, scatter bool) {
	if tbInfo.ShardRowIDBits > 0 && tbInfo.PreSplitRegions > 0 {
		splitPreSplitedTable(store, tbInfo, scatter)
	} else {
		regionID := splitRecordRegion(store, tbInfo.ID, scatter)
		if scatter {
			waitScatterRegionFinish(store, regionID)
		}
	}
}

func splitPreSplitedTable(store kv.SplitableStore, tbInfo *model.TableInfo, scatter bool) {
	// Example:
	// ShardRowIDBits = 5
	// PreSplitRegions = 3
	//
	// then will pre-split 2^(3-1) = 4 regions.
	//
	// in this code:
	// max   = 1 << (tblInfo.ShardRowIDBits - 1) = 1 << (5-1) = 16
	// step := int64(1 << (tblInfo.ShardRowIDBits - tblInfo.PreSplitRegions)) = 1 << (5-3) = 4;
	//
	// then split regionID is below:
	// 4  << 59 = 2305843009213693952
	// 8  << 59 = 4611686018427387904
	// 12 << 59 = 6917529027641081856
	//
	// The 4 pre-split regions range is below:
	// 0                   ~ 2305843009213693952
	// 2305843009213693952 ~ 4611686018427387904
	// 4611686018427387904 ~ 6917529027641081856
	// 6917529027641081856 ~ 9223372036854775807 ( (1 << 63) - 1 )
	//
	// And the max _tidb_rowid is 9223372036854775807, it won't be negative number.

	// Split table region.
	regionIDs := make([]uint64, 0, 1<<(tbInfo.PreSplitRegions-1)+len(tbInfo.Indices))
	step := int64(1 << (tbInfo.ShardRowIDBits - tbInfo.PreSplitRegions))
	// The highest bit is the symbol bit,and alloc _tidb_rowid will always be positive number.
	// So we only need to split the region for the positive number.
	max := int64(1 << (tbInfo.ShardRowIDBits - 1))
	for p := int64(step); p < max; p += step {
		recordID := p << (64 - tbInfo.ShardRowIDBits)
		recordPrefix := tablecodec.GenTableRecordPrefix(tbInfo.ID)
		key := tablecodec.EncodeRecordKey(recordPrefix, recordID)
		regionID, err := store.SplitRegion(key, scatter)
		if err != nil {
			logutil.Logger(context.Background()).Warn("[ddl] pre split table region failed", zap.Int64("recordID", recordID),
				zap.Error(err))
		} else {
			regionIDs = append(regionIDs, regionID)
		}
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter)...)
	if scatter {
		waitScatterRegionFinish(store, regionIDs...)
	}
}

func splitRecordRegion(store kv.SplitableStore, tableID int64, scatter bool) uint64 {
	tableStartKey := tablecodec.GenTablePrefix(tableID)
	regionID, err := store.SplitRegion(tableStartKey, scatter)
	if err != nil {
		// It will be automatically split by TiKV later.
		logutil.Logger(context.Background()).Warn("[ddl] split table region failed", zap.Error(err))
	}
	return regionID
}

func splitIndexRegion(store kv.SplitableStore, tblInfo *model.TableInfo, scatter bool) []uint64 {
	regionIDs := make([]uint64, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexPrefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idx.ID)
		regionID, err := store.SplitRegion(indexPrefix, scatter)
		if err != nil {
			logutil.Logger(context.Background()).Warn("[ddl] pre split table index region failed",
				zap.Stringer("table", tblInfo.Name),
				zap.Stringer("index", idx.Name),
				zap.Error(err))
		}
		regionIDs = append(regionIDs, regionID)
	}
	return regionIDs
}

func waitScatterRegionFinish(store kv.SplitableStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("[ddl] wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
}
