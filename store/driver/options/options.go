// Copyright 2021 PingCAP, Inc.
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

package options

import (
	"github.com/pingcap/tidb/kv"
	storekv "github.com/tikv/client-go/v2/kv"
)

// GetTiKVReplicaReadType maps kv.ReplicaReadType to tikv/kv.ReplicaReadType.
func GetTiKVReplicaReadType(t kv.ReplicaReadType) storekv.ReplicaReadType {
	switch t {
	case kv.ReplicaReadLeader:
		return storekv.ReplicaReadLeader
	case kv.ReplicaReadFollower:
		return storekv.ReplicaReadFollower
	case kv.ReplicaReadMixed:
		return storekv.ReplicaReadMixed
	case kv.ReplicaReadClosest:
		return storekv.ReplicaReadMixed
	}
	return 0
}
