// Copyright 2022 PingCAP, Inc.
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

// Package pdtypes contains type defines under PD.
//
// Mainly copied from PD repo to avoid direct dependency.
package pdtypes

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// StoresInfo records stores' info.
type StoresInfo struct {
	Count  int          `json:"count"`
	Stores []*StoreInfo `json:"stores"`
}

// StoreInfo contains information about a store.
type StoreInfo struct {
	Store  *MetaStore   `json:"store"`
	Status *StoreStatus `json:"status"`
}

// MetaStore contains meta information about a store.
type MetaStore struct {
	*metapb.Store
	StateName string `json:"state_name"`
}

// StoreStatus contains status about a store.
type StoreStatus struct {
	Capacity           ByteSize   `json:"capacity"`
	Available          ByteSize   `json:"available"`
	UsedSize           ByteSize   `json:"used_size"`
	LeaderCount        int        `json:"leader_count"`
	LeaderWeight       float64    `json:"leader_weight"`
	LeaderScore        float64    `json:"leader_score"`
	LeaderSize         int64      `json:"leader_size"`
	RegionCount        int        `json:"region_count"`
	RegionWeight       float64    `json:"region_weight"`
	RegionScore        float64    `json:"region_score"`
	RegionSize         int64      `json:"region_size"`
	SlowScore          uint64     `json:"slow_score"`
	SendingSnapCount   uint32     `json:"sending_snap_count,omitempty"`
	ReceivingSnapCount uint32     `json:"receiving_snap_count,omitempty"`
	IsBusy             bool       `json:"is_busy,omitempty"`
	StartTS            *time.Time `json:"start_ts,omitempty"`
	LastHeartbeatTS    *time.Time `json:"last_heartbeat_ts,omitempty"`
	Uptime             *Duration  `json:"uptime,omitempty"`
}

// RegionsInfo contains some regions with the detailed region info.
type RegionsInfo struct {
	Count   int          `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// RegionInfo records detail region info for api usage.
type RegionInfo struct {
	ID          uint64              `json:"id"`
	StartKey    string              `json:"start_key"`
	EndKey      string              `json:"end_key"`
	RegionEpoch *metapb.RegionEpoch `json:"epoch,omitempty"`
	Peers       []MetaPeer          `json:"peers,omitempty"`

	Leader          MetaPeer      `json:"leader,omitempty"`
	DownPeers       []PDPeerStats `json:"down_peers,omitempty"`
	PendingPeers    []MetaPeer    `json:"pending_peers,omitempty"`
	WrittenBytes    uint64        `json:"written_bytes"`
	ReadBytes       uint64        `json:"read_bytes"`
	WrittenKeys     uint64        `json:"written_keys"`
	ReadKeys        uint64        `json:"read_keys"`
	ApproximateSize int64         `json:"approximate_size"`
	ApproximateKeys int64         `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// MetaPeer is api compatible with *metapb.Peer.
type MetaPeer struct {
	*metapb.Peer
	// RoleName is `Role.String()`.
	// Since Role is serialized as int by json by default,
	// introducing it will make the output of pd-ctl easier to identify Role.
	RoleName string `json:"role_name"`
	// IsLearner is `Role == "Learner"`.
	// Since IsLearner was changed to Role in kvproto in 5.0, this field was introduced to ensure api compatibility.
	IsLearner bool `json:"is_learner,omitempty"`
}

// PDPeerStats is api compatible with *pdpb.PeerStats.
type PDPeerStats struct {
	*pdpb.PeerStats
	Peer MetaPeer `json:"peer"`
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID uint64 `json:"state_id"`
}
