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

func splitPartitionTableRegion(store kv.SplittableStore, pi *model.PartitionInfo, scatter bool) {
	// Max partition count is 4096, should we sample and just choose some of the partition to split?
	regionIDs := make([]uint64, 0, len(pi.Definitions))
	for _, def := range pi.Definitions {
		regionIDs = append(regionIDs, splitRecordRegion(store, def.ID, scatter))
	}
	if scatter {
		waitScatterRegionFinish(store, regionIDs...)
	}
}

func splitTableRegion(store kv.SplittableStore, tbInfo *model.TableInfo, scatter bool) {
	if tbInfo.ShardRowIDBits > 0 && tbInfo.PreSplitRegions > 0 {
		splitPreSplitedTable(store, tbInfo, scatter)
	} else {
		regionID := splitRecordRegion(store, tbInfo.ID, scatter)
		if scatter {
			waitScatterRegionFinish(store, regionID)
		}
	}
}

func splitPreSplitedTable(store kv.SplittableStore, tbInfo *model.TableInfo, scatter bool) {
	// Example:
	// ShardRowIDBits = 4
	// PreSplitRegions = 2
	//
	// then will pre-split 2^2 = 4 regions.
	//
	// in this code:
	// max   = 1 << tblInfo.ShardRowIDBits = 16
	// step := int64(1 << (tblInfo.ShardRowIDBits - tblInfo.PreSplitRegions)) = 1 << (4-2) = 4;
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
	step := int64(1 << (tbInfo.ShardRowIDBits - tbInfo.PreSplitRegions))
	max := int64(1 << tbInfo.ShardRowIDBits)
	splitTableKeys := make([][]byte, 0, 1<<(tbInfo.PreSplitRegions))
	for p := step; p < max; p += step {
		recordID := p << (64 - tbInfo.ShardRowIDBits - 1)
		recordPrefix := tablecodec.GenTableRecordPrefix(tbInfo.ID)
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
		splitTableKeys = append(splitTableKeys, key)
	}
	var err error
	regionIDs, err := store.SplitRegions(context.Background(), splitTableKeys, scatter)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] pre split some table regions failed",
			zap.Stringer("table", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter)...)
	if scatter {
		waitScatterRegionFinish(store, regionIDs...)
	}
}

func splitRecordRegion(store kv.SplittableStore, tableID int64, scatter bool) uint64 {
	tableStartKey := tablecodec.GenTablePrefix(tableID)
	regionIDs, err := store.SplitRegions(context.Background(), [][]byte{tableStartKey}, scatter)
	if err != nil {
		// It will be automatically split by TiKV later.
		logutil.BgLogger().Warn("[ddl] split table region failed", zap.Error(err))
	}
	if len(regionIDs) == 1 {
		return regionIDs[0]
	}
	return 0
}

func splitIndexRegion(store kv.SplittableStore, tblInfo *model.TableInfo, scatter bool) []uint64 {
	splitKeys := make([][]byte, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexPrefix := tablecodec.EncodeTableIndexPrefix(tblInfo.ID, idx.ID)
		splitKeys = append(splitKeys, indexPrefix)
	}
	regionIDs, err := store.SplitRegions(context.Background(), splitKeys, scatter)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] pre split some table index regions failed",
			zap.Stringer("table", tblInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	return regionIDs
}

func waitScatterRegionFinish(store kv.SplittableStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("[ddl] wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
}
