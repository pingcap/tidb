// Copyright 2024 PingCAP, Inc.
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

package data

import (
	"sort"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.uber.org/zap"
)

type RecoverRegionInfo struct {
	RegionId      uint64
	RegionVersion uint64
	StartKey      []byte
	EndKey        []byte
	TombStone     bool
}

func SortRecoverRegions(regions map[uint64][]*RecoverRegion) []*RecoverRegionInfo {
	// last log term -> last index -> commit index
	cmps := []func(a, b *RecoverRegion) int{
		func(a, b *RecoverRegion) int {
			return int(a.GetLastLogTerm() - b.GetLastLogTerm())
		},
		func(a, b *RecoverRegion) int {
			return int(a.GetLastIndex() - b.GetLastIndex())
		},
		func(a, b *RecoverRegion) int {
			return int(a.GetCommitIndex() - b.GetCommitIndex())
		},
	}

	// Sort region peer by last log term -> last index -> commit index, and collect all regions' version.
	var regionInfos = make([]*RecoverRegionInfo, 0, len(regions))
	for regionId, peers := range regions {
		sort.Slice(peers, func(i, j int) bool {
			for _, cmp := range cmps {
				if v := cmp(peers[i], peers[j]); v != 0 {
					return v > 0
				}
			}
			return false
		})
		v := peers[0].Version
		sk := PrefixStartKey(peers[0].StartKey)
		ek := PrefixEndKey(peers[0].EndKey)
		regionInfos = append(regionInfos, &RecoverRegionInfo{
			RegionId:      regionId,
			RegionVersion: v,
			StartKey:      sk,
			EndKey:        ek,
			TombStone:     peers[0].Tombstone,
		})
	}

	sort.Slice(regionInfos, func(i, j int) bool { return regionInfos[i].RegionVersion > regionInfos[j].RegionVersion })
	return regionInfos
}

func CheckConsistencyAndValidPeer(regionInfos []*RecoverRegionInfo) (map[uint64]struct{}, error) {
	// split and merge in progressing during the backup, there may some overlap region, we have to handle it
	// Resolve version conflicts.
	var treeMap = treemap.NewWith(keyCmpInterface)
	for _, p := range regionInfos {
		var fk, fv any
		fk, _ = treeMap.Ceiling(p.StartKey)
		// keyspace overlap sk within ceiling - fk
		if fk != nil && (keyEq(fk.([]byte), p.StartKey) || keyCmp(fk.([]byte), p.EndKey) < 0) {
			continue
		}

		// keyspace overlap sk within floor - fk.end_key
		fk, fv = treeMap.Floor(p.StartKey)
		if fk != nil && keyCmp(fv.(*RecoverRegionInfo).EndKey, p.StartKey) > 0 {
			continue
		}
		treeMap.Put(p.StartKey, p)
	}

	// After resolved, all validPeer regions shouldn't be tombstone.
	// do some sanity check
	var validPeers = make(map[uint64]struct{}, 0)
	var iter = treeMap.Iterator()
	var prevEndKey = PrefixStartKey([]byte{})
	var prevRegion uint64 = 0
	for iter.Next() {
		v := iter.Value().(*RecoverRegionInfo)
		if v.TombStone {
			log.Error("validPeer shouldn't be tombstone", zap.Uint64("region id", v.RegionId))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidPeer,
				"Peer shouldn't be tombstone")
		}
		if !keyEq(prevEndKey, iter.Key().([]byte)) {
			log.Error("regions are not adjacent", zap.Uint64("pre region", prevRegion), zap.Uint64("cur region", v.RegionId))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return nil, errors.Annotatef(berrors.ErrInvalidRange,
				"invalid region range")
		}
		prevEndKey = v.EndKey
		prevRegion = v.RegionId
		validPeers[v.RegionId] = struct{}{}
	}
	return validPeers, nil
}

// in cloud, since iops and bandwidth limitation, write operator in raft is slow, so raft state (logterm, lastlog, commitlog...) are the same among the peers
// LeaderCandidates select all peers can be select as a leader during the restore
func LeaderCandidates(peers []*RecoverRegion) ([]*RecoverRegion, error) {
	if peers == nil {
		return nil, errors.Annotatef(berrors.ErrRestoreRegionWithoutPeer,
			"invalid region range")
	}
	candidates := make([]*RecoverRegion, 0, len(peers))
	// by default, the peers[0] to be assign as a leader, since peers already sorted by leader selection rule
	leader := peers[0]
	candidates = append(candidates, leader)
	for _, peer := range peers[1:] {
		// qualificated candidate is leader.logterm = candidate.logterm && leader.lastindex = candidate.lastindex && && leader.commitindex = candidate.commitindex
		if peer.LastLogTerm == leader.LastLogTerm && peer.LastIndex == leader.LastIndex && peer.CommitIndex == leader.CommitIndex {
			log.Debug("leader candidate", zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId), zap.Uint64("peer id", peer.PeerId))
			candidates = append(candidates, peer)
		}
	}
	return candidates, nil
}

// for region A, has candidate leader x, y, z
// peer x on store 1 with storeBalanceScore 3
// peer y on store 3 with storeBalanceScore 2
// peer z on store 4 with storeBalanceScore 1
// result: peer z will be select as leader on store 4
func SelectRegionLeader(storeBalanceScore map[uint64]int, peers []*RecoverRegion) *RecoverRegion {
	// by default, the peers[0] to be assign as a leader
	leader := peers[0]
	minLeaderStore := storeBalanceScore[leader.StoreId]
	for _, peer := range peers[1:] {
		log.Debug("leader candidate", zap.Int("score", storeBalanceScore[peer.StoreId]), zap.Int("min-score", minLeaderStore), zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId), zap.Uint64("peer id", peer.PeerId))
		if storeBalanceScore[peer.StoreId] < minLeaderStore {
			minLeaderStore = storeBalanceScore[peer.StoreId]
			leader = peer
		}
	}
	return leader
}
