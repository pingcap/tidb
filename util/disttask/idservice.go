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

package disttaskutiltest

import (
	"fmt"
	"net"
)

// GenerateExecID used to generate IP:port as exec_id value
// This function is used by distributed task execution to generate serverID string to
// correlated one subtask to on TiDB node to be executed.
func GenerateExecID(ip string, port uint, id string) string {
	portstring := fmt.Sprintf("%d", port)
	return net.JoinHostPort(ip, portstring) + id
}
