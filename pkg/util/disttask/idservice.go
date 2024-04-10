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

package disttaskutil

import (
	"context"
	"fmt"
	"net"

	"github.com/pingcap/tidb/pkg/domain/infosync"
)

// GenerateExecID used to generate IP:port as exec_id value
// This function is used by distributed task execution to generate serverID string to
// correlated one subtask to on TiDB node to be executed.
func GenerateExecID(info *infosync.ServerInfo) string {
	return net.JoinHostPort(info.IP, fmt.Sprintf("%d", info.Port))
}

// MatchServerInfo will check if the schedulerID matched in all serverInfos.
func MatchServerInfo(serverInfos []*infosync.ServerInfo, schedulerID string) bool {
	return FindServerInfo(serverInfos, schedulerID) >= 0
}

// FindServerInfo will find the schedulerID in all serverInfos.
func FindServerInfo(serverInfos []*infosync.ServerInfo, schedulerID string) int {
	for i, serverInfo := range serverInfos {
		serverID := GenerateExecID(serverInfo)
		if serverID == schedulerID {
			return i
		}
	}
	return -1
}

// GenerateSubtaskExecID generates the subTask execID.
func GenerateSubtaskExecID(ctx context.Context, id string) string {
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil || len(serverInfos) == 0 {
		return ""
	}
	if serverNode, ok := serverInfos[id]; ok {
		return GenerateExecID(serverNode)
	}
	return ""
}

// GenerateSubtaskExecID4Test generates the subTask execID, only used in unit tests.
func GenerateSubtaskExecID4Test(id string) string {
	serverInfos := infosync.MockGlobalServerInfoManagerEntry.GetAllServerInfo()
	if len(serverInfos) == 0 {
		return ""
	}
	if serverNode, ok := serverInfos[id]; ok {
		return GenerateExecID(serverNode)
	}
	return ""
}
