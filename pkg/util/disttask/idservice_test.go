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
	"testing"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/stretchr/testify/require"
)

// This testCase show GenerateExecID only generate string by input parametas
func TestGenServerID(t *testing.T) {
	var str string
	serverIO := GenerateExecID(&infosync.ServerInfo{IP: "", Port: 0})
	require.Equal(t, serverIO, ":0")
	serverIO = GenerateExecID(&infosync.ServerInfo{IP: "10.124.122.25", Port: 3456})
	require.Equal(t, serverIO, "10.124.122.25:3456")
	serverIO = GenerateExecID(&infosync.ServerInfo{IP: "10.124", Port: 3456})
	require.Equal(t, serverIO, "10.124:3456")
	serverIO = GenerateExecID(&infosync.ServerInfo{IP: str, Port: 65537})
	require.Equal(t, serverIO, ":65537")
	// IPv6 testcase
	serverIO = GenerateExecID(&infosync.ServerInfo{IP: "ABCD:EF01:2345:6789:ABCD:EF01:2345:6789", Port: 65537})
	require.Equal(t, serverIO, "[ABCD:EF01:2345:6789:ABCD:EF01:2345:6789]:65537")
}
