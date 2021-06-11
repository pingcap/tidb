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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

func splitPartitionTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, pi *model.PartitionInfo, scatter bool) {
	// Max partition count is 4096, should we sample and just choose some of the partition to split?
	regionIDs := make([]uint64, 0, len(pi.Definitions))
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
		for _, def := range pi.Definitions {
			regionIDs = append(regionIDs, preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, def.ID, scatter)...)
		}
	} else {
		for _, def := range pi.Definitions {
			regionIDs = append(regionIDs, splitRecordRegion(ctxWithTimeout, store, def.ID, scatter))
		}
	}
	if scatter {
		waitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

func splitTableRegion(ctx sessionctx.Context, store kv.SplittableStore, tbInfo *model.TableInfo, scatter bool) {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	var regionIDs []uint64
	if shardingBits(tbInfo) > 0 && tbInfo.PreSplitRegions > 0 {
		regionIDs = preSplitPhysicalTableByShardRowID(ctxWithTimeout, store, tbInfo, tbInfo.ID, scatter)
	} else {
		regionIDs = append(regionIDs, splitRecordRegion(ctxWithTimeout, store, tbInfo.ID, scatter))
	}
	if scatter {
		waitScatterRegionFinish(ctxWithTimeout, store, regionIDs...)
	}
}

func preSplitPhysicalTableByShardRowID(ctx context.Context, store kv.SplittableStore, tbInfo *model.TableInfo, physicalID int64, scatter bool) []uint64 {
	// Example:
	// sharding_bits = 4
	// PreSplitRegions = 2
	//
	// then will pre-split 2^2 = 4 regions.
	//
	// in this code:
	// max   = 1 << sharding_bits = 16
	// step := int64(1 << (sharding_bits - tblInfo.PreSplitRegions)) = 1 << (4-2) = 4;
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
	shardingBits := shardingBits(tbInfo)
	step := int64(1 << (shardingBits - tbInfo.PreSplitRegions))
	max := int64(1 << shardingBits)
	splitTableKeys := make([][]byte, 0, 1<<(tbInfo.PreSplitRegions))
	splitTableKeys = append(splitTableKeys, tablecodec.GenTablePrefix(physicalID))
	for p := step; p < max; p += step {
		recordID := p << (64 - shardingBits - 1)
		recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
		splitTableKeys = append(splitTableKeys, key)
	}
	var err error
	regionIDs, err := store.SplitRegions(ctx, splitTableKeys, scatter, &tbInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] pre split some table regions failed",
			zap.Stringer("table", tbInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	regionIDs = append(regionIDs, splitIndexRegion(store, tbInfo, scatter)...)
	return regionIDs
}

func splitRecordRegion(ctx context.Context, store kv.SplittableStore, tableID int64, scatter bool) uint64 {
	tableStartKey := tablecodec.GenTablePrefix(tableID)
	regionIDs, err := store.SplitRegions(ctx, [][]byte{tableStartKey}, scatter, &tableID)
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
	regionIDs, err := store.SplitRegions(context.Background(), splitKeys, scatter, &tblInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] pre split some table index regions failed",
			zap.Stringer("table", tblInfo.Name), zap.Int("successful region count", len(regionIDs)), zap.Error(err))
	}
	return regionIDs
}

func waitScatterRegionFinish(ctx context.Context, store kv.SplittableStore, regionIDs ...uint64) {
	for _, regionID := range regionIDs {
		err := store.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("[ddl] wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
			// We don't break for PDError because it may caused by ScatterRegion request failed.
			if _, ok := errors.Cause(err).(*tikverr.PDError); !ok {
				break
			}
		}
	}
}
