// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverinfo

import (
	"encoding/json"
	"maps"
	"os"
	"path"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

const (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
	// KeyOpDefaultRetryCnt is the default retry count for etcd store.
	KeyOpDefaultRetryCnt = 5
	// KeyOpDefaultTimeout is the default time out for etcd store.
	KeyOpDefaultTimeout = 1 * time.Second
	// TopologyInformationPath means etcd path for storing topology info.
	TopologyInformationPath = "/topology/tidb"
	// TopologySessionTTL is ttl for topology, ant it's the ETCD session's TTL in seconds.
	TopologySessionTTL = 45
	// TopologyTimeToRefresh means time to refresh etcd.
	TopologyTimeToRefresh = 30 * time.Second
	// minTSReportInterval is interval of infoSyncerKeeper reporting min startTS.
	minTSReportInterval = 30 * time.Second
)

// VersionInfo is the server version and git_hash.
type VersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// StaticInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
// DO NOT edit it after tidb-server started.
type StaticInfo struct {
	VersionInfo
	ID             string `json:"ddl_id"`
	IP             string `json:"ip"`
	Port           uint   `json:"listening_port"`
	StatusPort     uint   `json:"status_port"`
	Lease          string `json:"lease"`
	StartTimestamp int64  `json:"start_timestamp"`
	// Keyspace is the keyspace name of this TiDB instance.
	// it's always empty in classic kernel.
	Keyspace string `json:"keyspace,omitempty"`
	// AssumedKeyspace is the keyspace that this server info syncer assumes to be.
	// it's empty when the server info syncer represents the keyspace of this TiDB
	// instance.
	// it's only used in cross keyspace scenario, such as on a user keyspace TiDB
	// instance, it will access the SYSTEM keyspace, it will assume to be in the
	// SYSTEM keyspace to join the online schema change process, to make sure the
	// info schema is correctly synced.
	AssumedKeyspace string `json:"assumed_keyspace,omitempty"`
	// ServerID is a function, to always retrieve latest serverID from `Domain`,
	// which will be changed on occasions such as connection to PD is restored after broken.
	ServerIDGetter func() uint64 `json:"-"`

	// JSONServerID is `serverID` for json marshal/unmarshal ONLY.
	JSONServerID uint64 `json:"server_id"`
}

// IsAssumed checks if the StaticInfo is assumed to be in a keyspace other than its own.
func (i *StaticInfo) IsAssumed() bool {
	return i.AssumedKeyspace != ""
}

// DynamicInfo represents the dynamic information of the server.
// Please note that it may change when TiDB is running.
// To update the dynamic server information, use `InfoSyncer.cloneDynamicServerInfo` to obtain a copy of the dynamic server info.
// After making modifications, use `InfoSyncer.setDynamicServerInfo` to update the dynamic server information.
type DynamicInfo struct {
	Labels map[string]string `json:"labels"`
}

// Clone the DynamicInfo.
func (d *DynamicInfo) Clone() *DynamicInfo {
	return &DynamicInfo{
		Labels: maps.Clone(d.Labels),
	}
}

// TopologyInfo is the topology info
type TopologyInfo struct {
	VersionInfo
	IP             string            `json:"ip"`
	StatusPort     uint              `json:"status_port"`
	DeployPath     string            `json:"deploy_path"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
}

// ServerInfo represents the server's basic information.
// It consists of two sections: static and dynamic.
// The static information is generated during the startup of the TiDB server and should never be modified while the TiDB server is running.
// The dynamic information can be updated while the TiDB server is running and should be synchronized with PD's etcd.
type ServerInfo struct {
	StaticInfo
	DynamicInfo
}

// Clone the ServerInfo.
func (info *ServerInfo) Clone() *ServerInfo {
	return &ServerInfo{
		StaticInfo:  info.StaticInfo,
		DynamicInfo: *info.DynamicInfo.Clone(),
	}
}

// Marshal `ServerInfo` into bytes.
func (info *ServerInfo) Marshal() ([]byte, error) {
	info.JSONServerID = info.ServerIDGetter()
	infoBuf, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return infoBuf, nil
}

// Unmarshal `ServerInfo` from bytes.
func (info *ServerInfo) Unmarshal(v []byte) error {
	if err := json.Unmarshal(v, info); err != nil {
		return err
	}
	info.ServerIDGetter = func() uint64 {
		return info.JSONServerID
	}
	return nil
}

// ToTopologyInfo converts ServerInfo to TopologyInfo.
func (info *ServerInfo) ToTopologyInfo() TopologyInfo {
	s, err := os.Executable()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	return TopologyInfo{
		VersionInfo: VersionInfo{
			Version: mysql.TiDBReleaseVersion,
			GitHash: info.VersionInfo.GitHash,
		},
		IP:             info.IP,
		StatusPort:     info.StatusPort,
		DeployPath:     dir,
		StartTimestamp: info.StartTimestamp,
		Labels:         info.Labels,
	}
}
