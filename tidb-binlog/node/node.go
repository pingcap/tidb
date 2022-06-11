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

package node

import (
	"fmt"

	"github.com/pingcap/tidb/util"
)

var (
	// DefaultRootPath is the root path of the keys stored in etcd, the `v1` is the tidb-binlog's version.
	DefaultRootPath = "/tidb-binlog/v1"

	// PumpNode is the name of pump.
	PumpNode = "pump"

	// DrainerNode is the name of drainer.
	DrainerNode = "drainer"

	// NodePrefix is the map (node => it's prefix in storage)
	NodePrefix = map[string]string{
		PumpNode:    "pumps",
		DrainerNode: "drainers",
	}
)

const (
	// Online means the node can receive request.
	Online = "online"

	// Pausing means the node is pausing.
	Pausing = "pausing"

	// Paused means the node is already paused.
	Paused = "paused"

	// Closing means the node is closing, and the state will be Offline when closed.
	Closing = "closing"

	// Offline means the node is offline, and will not provide service.
	Offline = "offline"
)

// Label is key/value pairs that are attached to objects
type Label struct {
	Labels map[string]string `json:"labels"`
}

// Status describes the status information of a tidb-binlog node in etcd.
type Status struct {
	// the id of node.
	NodeID string `json:"nodeId"`

	// the host of pump or node.
	Addr string `json:"host"`

	// the state of pump.
	State string `json:"state"`

	// the node is alive or not.
	IsAlive bool `json:"isAlive"`

	// the score of node, it is report by node, calculated by node's qps, disk usage and binlog's data size.
	// if Score is less than 0, means this node is useless. Now only used for pump.
	Score int64 `json:"score"`

	// the label of this node. Now only used for pump.
	// pump client will only send to a pump which label is matched.
	Label *Label `json:"label"`

	// for pump: max commit ts in pump
	// for drainer: drainer has consume all binlog less than or equal MaxCommitTS
	MaxCommitTS int64 `json:"maxCommitTS"`

	// UpdateTS is the last update ts of node's status.
	UpdateTS int64 `json:"updateTS"`
}

func (s *Status) String() string {
	updateTime := util.TSOToRoughTime(s.UpdateTS)
	return fmt.Sprintf("{NodeID: %s, Addr: %s, State: %s, MaxCommitTS: %d, UpdateTime: %v}", s.NodeID, s.Addr, s.State, s.MaxCommitTS, updateTime)
}
