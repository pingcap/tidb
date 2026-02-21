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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/set"
	pd "github.com/tikv/pd/client/http"
)

type tikvRegionPeersRetriever struct {
	dummyCloser
	extractor *plannercore.TikvRegionPeersExtractor
	retrieved bool
}

func (e *tikvRegionPeersRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	e.retrieved = true
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	pdCli, err := tikvHelper.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}

	var regionsInfo, regionsInfoByStoreID []pd.RegionInfo
	regionMap := make(map[int64]*pd.RegionInfo)
	storeMap := make(map[int64]struct{})

	if len(e.extractor.StoreIDs) == 0 && len(e.extractor.RegionIDs) == 0 {
		regionsInfo, err := pdCli.GetRegions(ctx)
		if err != nil {
			return nil, err
		}
		return e.packTiKVRegionPeersRows(regionsInfo.Regions, storeMap)
	}

	for _, storeID := range e.extractor.StoreIDs {
		// if a region_id located in 1, 4, 7 store we will get all of them when request any store_id,
		// storeMap is used to filter peers on unexpected stores.
		storeMap[int64(storeID)] = struct{}{}
		storeRegionsInfo, err := pdCli.GetRegionsByStoreID(ctx, storeID)
		if err != nil {
			return nil, err
		}
		for i, regionInfo := range storeRegionsInfo.Regions {
			// regionMap is used to remove dup regions and record the region in regionsInfoByStoreID.
			if _, ok := regionMap[regionInfo.ID]; !ok {
				regionsInfoByStoreID = append(regionsInfoByStoreID, regionInfo)
				regionMap[regionInfo.ID] = &storeRegionsInfo.Regions[i]
			}
		}
	}

	if len(e.extractor.RegionIDs) == 0 {
		return e.packTiKVRegionPeersRows(regionsInfoByStoreID, storeMap)
	}

	for _, regionID := range e.extractor.RegionIDs {
		regionInfoByStoreID, ok := regionMap[int64(regionID)]
		if !ok {
			// if there is storeIDs, target region_id is fetched by storeIDs,
			// otherwise we need to fetch it from PD.
			if len(e.extractor.StoreIDs) == 0 {
				regionInfo, err := pdCli.GetRegionByID(ctx, regionID)
				if err != nil {
					return nil, err
				}
				regionsInfo = append(regionsInfo, *regionInfo)
			}
		} else {
			regionsInfo = append(regionsInfo, *regionInfoByStoreID)
		}
	}

	return e.packTiKVRegionPeersRows(regionsInfo, storeMap)
}

func (e *tikvRegionPeersRetriever) isUnexpectedStoreID(storeID int64, storeMap map[int64]struct{}) bool {
	if len(e.extractor.StoreIDs) == 0 {
		return false
	}
	if _, ok := storeMap[storeID]; ok {
		return false
	}
	return true
}

func (e *tikvRegionPeersRetriever) packTiKVRegionPeersRows(
	regionsInfo []pd.RegionInfo, storeMap map[int64]struct{}) ([][]types.Datum, error) {
	//nolint: prealloc
	var rows [][]types.Datum
	for _, region := range regionsInfo {
		records := make([][]types.Datum, 0, len(region.Peers))
		pendingPeerIDSet := set.NewInt64Set()
		for _, peer := range region.PendingPeers {
			pendingPeerIDSet.Insert(peer.ID)
		}
		downPeerMap := make(map[int64]int64, len(region.DownPeers))
		for _, peerStat := range region.DownPeers {
			downPeerMap[peerStat.Peer.ID] = peerStat.DownSec
		}
		for _, peer := range region.Peers {
			// isUnexpectedStoreID return true if we should filter this peer.
			if e.isUnexpectedStoreID(peer.StoreID, storeMap) {
				continue
			}

			row := make([]types.Datum, len(infoschema.GetTableTiKVRegionPeersCols()))
			row[0].SetInt64(region.ID)
			row[1].SetInt64(peer.ID)
			row[2].SetInt64(peer.StoreID)
			if peer.IsLearner {
				row[3].SetInt64(1)
			} else {
				row[3].SetInt64(0)
			}
			if peer.ID == region.Leader.ID {
				row[4].SetInt64(1)
			} else {
				row[4].SetInt64(0)
			}
			if downSec, ok := downPeerMap[peer.ID]; ok {
				row[5].SetString(downPeer, mysql.DefaultCollationName)
				row[6].SetInt64(downSec)
			} else if pendingPeerIDSet.Exist(peer.ID) {
				row[5].SetString(pendingPeer, mysql.DefaultCollationName)
			} else {
				row[5].SetString(normalPeer, mysql.DefaultCollationName)
			}
			records = append(records, row)
		}
		rows = append(rows, records...)
	}
	return rows, nil
}
