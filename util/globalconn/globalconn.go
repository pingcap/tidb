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

package globalconn

// GlobalConnID is the global connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
// Used when GlobalKill feature is enable.
// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md
// 32 bits version:
//
//	 31    21 20               1    0
//	+--------+------------------+------+
//	|serverID|   local connID   |markup|
//	| (11b)  |       (20b)      |  =0  |
//	+--------+------------------+------+
//
// 64 bits version:
//
//	 63 62                 41 40                                   1   0
//	+--+---------------------+--------------------------------------+------+
//	|  |      serverId       |             local connId             |markup|
//	|=0|       (22b)         |                 (40b)                |  =1  |
//	+--+---------------------+--------------------------------------+------+
const (
	// MaxServerID32 is maximum serverID for 32bits global connection ID.
	MaxServerID32 = 1<<11 - 1
	// LocalConnIDBits32 is the number of bits of localConnID for 32bits global connection ID.
	LocalConnIDBits32 = 20
	// MaxLocalConnID32 is maximum localConnID for 32bits global connection ID.
	MaxLocalConnID32 = 1<<LocalConnIDBits32 - 1

	// MaxServerID64 is maximum serverID for 64bits global connection ID.
	MaxServerID64 = 1<<22 - 1
	// LocalConnIDBits64 is the number of bits of localConnID for 64bits global connection ID.
	LocalConnIDBits64 = 40
	// MaxLocalConnID64 is maximum localConnID for 64bits global connection ID.
	MaxLocalConnID64 = 1<<LocalConnIDBits64 - 1
)
