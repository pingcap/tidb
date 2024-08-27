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

package tiflash

// ReplicaRead is the policy to select TiFlash nodes.
type ReplicaRead int

const (
	// AllReplicas  means using all the available nodes to do analytic computing, regardless of local zone or other zones.
	AllReplicas ReplicaRead = iota
	// ClosestAdaptive means using the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will involve the tiflash nodes from other zones.
	ClosestAdaptive
	// ClosestReplicas means using only the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will report an error, and show an error message. Because of the feature of TiFlash remote read, a small number of regions  in other zones is acceptable, but performance will be affected. The threshold is fixed, 3 regions per tiflash node.
	ClosestReplicas
)

const (
	// AllReplicaStr is the string value of AllReplicas.
	AllReplicaStr = "all_replicas"
	// ClosestAdaptiveStr is the string value of ClosestAdaptive.
	ClosestAdaptiveStr = "closest_adaptive"
	// ClosestReplicasStr is the string value of ClosestReplicas.
	ClosestReplicasStr = "closest_replicas"
)

// IsAllReplicas return whether the policy is AllReplicas.
func (policy ReplicaRead) IsAllReplicas() bool {
	return policy == AllReplicas
}

// IsClosestReplicas return whether the policy is ClosestReplicas.
func (policy ReplicaRead) IsClosestReplicas() bool {
	return policy == ClosestReplicas
}

// GetTiFlashReplicaRead return corresponding policy string in integer.
func GetTiFlashReplicaRead(policy ReplicaRead) string {
	switch policy {
	case AllReplicas:
		return AllReplicaStr
	case ClosestAdaptive:
		return ClosestAdaptiveStr
	case ClosestReplicas:
		return ClosestReplicasStr
	default:
		return AllReplicaStr
	}
}

// GetTiFlashReplicaReadByStr return corresponding policy in string.
func GetTiFlashReplicaReadByStr(str string) ReplicaRead {
	switch str {
	case AllReplicaStr:
		return AllReplicas
	case ClosestAdaptiveStr:
		return ClosestAdaptive
	case ClosestReplicasStr:
		return ClosestReplicas
	default:
		return AllReplicas
	}
}

const (
	// MaxRemoteReadCountPerNodeForClosestReplicas is the max remote read count per node for "closest_replicas".
	MaxRemoteReadCountPerNodeForClosestReplicas = 3
)
