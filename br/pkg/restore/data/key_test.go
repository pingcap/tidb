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

package data_test

import (
	"testing"

	recover_data "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/tidb/br/pkg/restore/data"
	"github.com/stretchr/testify/require"
)

func newPeerMeta(
	regionId uint64,
	peerId uint64,
	storeId uint64,
	startKey []byte,
	endKey []byte,
	lastLogTerm uint64,
	lastIndex uint64,
	commitIndex uint64,
	version uint64,
	tombstone bool,
) *data.RecoverRegion {
	return &data.RecoverRegion{
		RegionMeta: &recover_data.RegionMeta{
			RegionId:    regionId,
			PeerId:      peerId,
			StartKey:    startKey,
			EndKey:      endKey,
			LastLogTerm: lastLogTerm,
			LastIndex:   lastIndex,
			CommitIndex: commitIndex,
			Version:     version,
			Tombstone:   tombstone,
		},
		StoreId: storeId,
	}
}

func newRecoverRegionInfo(r *data.RecoverRegion) *data.RecoverRegionInfo {
	return &data.RecoverRegionInfo{
		RegionVersion: r.Version,
		RegionId:      r.RegionId,
		StartKey:      data.PrefixStartKey(r.StartKey),
		EndKey:        data.PrefixEndKey(r.EndKey),
		TombStone:     r.Tombstone,
	}
}

func TestSortRecoverRegions(t *testing.T) {
	selectedPeer1 := newPeerMeta(9, 11, 2, []byte("aa"), nil, 2, 0, 0, 0, false)
	selectedPeer2 := newPeerMeta(19, 22, 3, []byte("bbb"), nil, 2, 1, 0, 1, false)
	selectedPeer3 := newPeerMeta(29, 30, 1, []byte("c"), nil, 2, 1, 1, 2, false)
	regions := map[uint64][]*data.RecoverRegion{
		9: {
			// peer 11 should be selected because of log term
			newPeerMeta(9, 10, 1, []byte("a"), nil, 1, 1, 1, 1, false),
			selectedPeer1,
			newPeerMeta(9, 12, 3, []byte("aaa"), nil, 0, 0, 0, 0, false),
		},
		19: {
			// peer 22 should be selected because of log index
			newPeerMeta(19, 20, 1, []byte("b"), nil, 1, 1, 1, 1, false),
			newPeerMeta(19, 21, 2, []byte("bb"), nil, 2, 0, 0, 0, false),
			selectedPeer2,
		},
		29: {
			// peer 30 should be selected because of log index
			selectedPeer3,
			newPeerMeta(29, 31, 2, []byte("cc"), nil, 2, 0, 0, 0, false),
			newPeerMeta(29, 32, 3, []byte("ccc"), nil, 2, 1, 0, 0, false),
		},
	}
	regionsInfos := data.SortRecoverRegions(regions)
	expectRegionInfos := []*data.RecoverRegionInfo{
		newRecoverRegionInfo(selectedPeer3),
		newRecoverRegionInfo(selectedPeer2),
		newRecoverRegionInfo(selectedPeer1),
	}
	require.Equal(t, expectRegionInfos, regionsInfos)
}

func TestCheckConsistencyAndValidPeer(t *testing.T) {
	//key space is continuous
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 0, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 1, 2, false)

	validRegionInfos := []*data.RecoverRegionInfo{
		newRecoverRegionInfo(validPeer1),
		newRecoverRegionInfo(validPeer2),
		newRecoverRegionInfo(validPeer3),
	}

	validPeer, err := data.CheckConsistencyAndValidPeer(validRegionInfos)
	require.NoError(t, err)
	require.Equal(t, 3, len(validPeer))
	var regions = make(map[uint64]struct{}, 3)
	regions[9] = struct{}{}
	regions[19] = struct{}{}
	regions[29] = struct{}{}

	require.Equal(t, regions, validPeer)

	//key space is not continuous
	invalidPeer1 := newPeerMeta(9, 11, 2, []byte("aa"), []byte("cc"), 2, 0, 0, 0, false)
	invalidPeer2 := newPeerMeta(19, 22, 3, []byte("dd"), []byte("cc"), 2, 1, 0, 1, false)
	invalidPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte("dd"), 2, 1, 1, 2, false)

	invalidRegionInfos := []*data.RecoverRegionInfo{
		newRecoverRegionInfo(invalidPeer1),
		newRecoverRegionInfo(invalidPeer2),
		newRecoverRegionInfo(invalidPeer3),
	}

	_, err = data.CheckConsistencyAndValidPeer(invalidRegionInfos)
	require.Error(t, err)
	require.Regexp(t, ".*invalid restore range.*", err.Error())
}

func TestLeaderCandidates(t *testing.T) {
	//key space is continuous
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*data.RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}

	candidates, err := data.LeaderCandidates(peers)
	require.NoError(t, err)
	require.Equal(t, 3, len(candidates))
}

func TestSelectRegionLeader(t *testing.T) {
	validPeer1 := newPeerMeta(9, 11, 2, []byte(""), []byte("bb"), 2, 1, 0, 0, false)
	validPeer2 := newPeerMeta(19, 22, 3, []byte("bb"), []byte("cc"), 2, 1, 0, 1, false)
	validPeer3 := newPeerMeta(29, 30, 1, []byte("cc"), []byte(""), 2, 1, 0, 2, false)

	peers := []*data.RecoverRegion{
		validPeer1,
		validPeer2,
		validPeer3,
	}
	// init store banlance score all is 0
	storeBalanceScore := make(map[uint64]int, len(peers))
	leader := data.SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer1, leader)

	// change store banlance store
	storeBalanceScore[2] = 3
	storeBalanceScore[3] = 2
	storeBalanceScore[1] = 1
	leader = data.SelectRegionLeader(storeBalanceScore, peers)
	require.Equal(t, validPeer3, leader)

	// one peer
	peer := []*data.RecoverRegion{
		validPeer3,
	}
	// init store banlance score all is 0
	storeScore := make(map[uint64]int, len(peer))
	leader = data.SelectRegionLeader(storeScore, peer)
	require.Equal(t, validPeer3, leader)
}
