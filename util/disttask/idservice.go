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

	"github.com/pingcap/tidb/domain/infosync"
)

// GenerateExecID used to generate IP:port as exec_id value
// This function is used by distributed task execution to generate serverID string to
// correlated one subtask to on TiDB node to be executed.
func GenerateExecID(ip string, port uint) string {
	portstring := fmt.Sprintf("%d", port)
	return net.JoinHostPort(ip, portstring)
}

func GenerateExecID4Test(ip string, port uint, id string) string {
	portstring := fmt.Sprintf("%d", port)
	return fmt.Sprintf("%s:%s", net.JoinHostPort(ip, portstring), id)
}

func MatchServerInfo(serverInfos map[string]*infosync.ServerInfo, schedulerID string) bool {
	for _, serverInfo := range serverInfos {
		serverID := GenerateExecID(serverInfo.IP, serverInfo.Port)
		if serverID == schedulerID {
			return true
		}
	}
	return false
}

func MatchServerInfo4Test(serverInfos map[string]*infosync.ServerInfo, schedulerID string, id string) bool {
	for _, serverInfo := range serverInfos {
		serverID := GenerateExecID4Test(serverInfo.IP, serverInfo.Port, id)
		if serverID == schedulerID {
			return true
		}
	}
	return false
}

func GenerateSubtaskExecID(ctx context.Context, ID string) string {
	serverInfos, err := infosync.GetAllServerInfo(ctx)
	if err != nil || len(serverInfos) == 0 {
		return ""
	}
	if serverNode, ok := serverInfos[ID]; ok {
		return GenerateExecID(serverNode.IP, serverNode.Port)
	}
	return ""
}

func GenerateSubtaskExecID4Test(ID string) string {
	serverInfos := infosync.MockGlobalServerInfoManagerEntry.GetAllServerInfo()
	if len(serverInfos) == 0 {
		return ""
	}
	if serverNode, ok := serverInfos[ID]; ok {
		return GenerateExecID4Test(serverNode.IP, serverNode.Port, ID)
	}
	return ""
}
