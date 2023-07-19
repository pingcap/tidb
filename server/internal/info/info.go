// Copyright 2023 PingCAP, Inc.
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

package info

import "github.com/pingcap/tidb/domain/infosync"

// ClusterServerInfo is used to report cluster servers info when do http request.
type ClusterServerInfo struct {
	ServersNum                   int                             `json:"servers_num,omitempty"`
	OwnerID                      string                          `json:"owner_id"`
	IsAllServerVersionConsistent bool                            `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []infosync.ServerVersionInfo    `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*infosync.ServerInfo `json:"all_servers_info,omitempty"`
}

type TableFlashReplicaInfo struct {
	// Modifying the field name needs to negotiate with TiFlash colleague.
	ID             int64    `json:"id"`
	ReplicaCount   uint64   `json:"replica_count"`
	LocationLabels []string `json:"location_labels"`
	Available      bool     `json:"available"`
	HighPriority   bool     `json:"high_priority"`
}

// ServerInfo is used to report the servers info when do http request.
type ServerInfo struct {
	IsOwner  bool `json:"is_owner"`
	MaxProcs int  `json:"max_procs"`
	GOGC     int  `json:"gogc"`
	*infosync.ServerInfo
}

// SchemaTableStorage is used to report the storage info of a table.
type SchemaTableStorage struct {
	TableSchema   string `json:"table_schema"`
	TableName     string `json:"table_name"`
	TableRows     int64  `json:"table_rows"`
	AvgRowLength  int64  `json:"avg_row_length"`
	DataLength    int64  `json:"data_length"`
	MaxDataLength int64  `json:"max_data_length"`
	IndexLength   int64  `json:"index_length"`
	DataFree      int64  `json:"data_free"`
}
