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
	// ClosetAdaptive means using the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will involve the tiflash nodes from other zones.
	ClosetAdaptive
	// ClosetReplicas means using only the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will report an error, and show an error message. Because of the feature of TiFlash remote read, a small number of regions  in other zones is acceptable, but performance will be affected. The threshold is fixed, 3 regions per tiflash node.
	ClosetReplicas
)

const (
	// AllReplicaStr is the string value of AllReplicas.
	AllReplicaStr = "all_replicas"
	// ClosetAdaptiveStr is the string value of ClosetAdaptive.
	ClosetAdaptiveStr = "closet_adaptive"
	// ClosetReplicasStr is the string value of ClosetReplicas.
	ClosetReplicasStr = "closet_replicas"
)

// IsPolicyAllReplicas return whether the policy is AllReplicas.
func (policy ReplicaRead) IsPolicyAllReplicas() bool {
	return policy == AllReplicas
}

// IsPolicyClosetReplicas return whether the policy is ClosetReplicas.
func (policy ReplicaRead) IsPolicyClosetReplicas() bool {
	return policy == ClosetReplicas
}

// GetTiflashReplicaRead return corresponding policy string in integer.
func GetTiflashReplicaRead(policy ReplicaRead) string {
	switch policy {
	case AllReplicas:
		return AllReplicaStr
	case ClosetAdaptive:
		return ClosetAdaptiveStr
	case ClosetReplicas:
		return ClosetReplicasStr
	default:
		return AllReplicaStr
	}
}

// GetTiflashReplicaReadByStr return corresponding policy in string.
func GetTiflashReplicaReadByStr(str string) ReplicaRead {
	switch str {
	case AllReplicaStr:
		return AllReplicas
	case ClosetAdaptiveStr:
		return ClosetAdaptive
	case ClosetReplicasStr:
		return ClosetReplicas
	default:
		return AllReplicas
	}
}

const (
	// MaxRemoteReadCountPerNodeForClosetReplicas is the max remote read count per node for only local zone.
	MaxRemoteReadCountPerNodeForClosetReplicas = 3
)
