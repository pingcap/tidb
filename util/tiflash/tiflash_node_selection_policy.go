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

// NodeSelectionPolicy is the policy to select TiFlash nodes.
type NodeSelectionPolicy int

const (
	// AllNodes  means using all the available nodes to do analytic computing, regardless of local zone or other zones.
	AllNodes NodeSelectionPolicy = iota
	// PriorityLocalZoneNodes means using the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will involve the tiflash nodes from other zones.
	PriorityLocalZoneNodes
	// OnlyLocalZoneNodes means using only the nodes in the same zone as the entry TiDB. If not all the tiflash data can be accessed, the query will report an error, and show an error message. Because of the feature of TiFlash remote read, a small number of regions  in other zones is acceptable, but performance will be affected. The threshold is fixed, 3 regions per tiflash node.
	OnlyLocalZoneNodes
)

const (
	// AllNodesStr is the string value of AllNodes.
	AllNodesStr = "all_nodes"
	// PriorityLocalZoneNodesStr is the string value of PriorityLocalZoneNodes.
	PriorityLocalZoneNodesStr = "priority_local_zone_nodes"
	// OnlyLocalZoneNodesStr is the string value of OnlyLocalZoneNodes.
	OnlyLocalZoneNodesStr = "only_local_zone_nodes"
)

// IsPolicyAllNodes return whether the policy is AllNodes.
func (policy NodeSelectionPolicy) IsPolicyAllNodes() bool {
	return policy == AllNodes
}

// IsPolicyOnlyLocalZoneNodes return whether the policy is OnlyLocalZoneNodes.
func (policy NodeSelectionPolicy) IsPolicyOnlyLocalZoneNodes() bool {
	return policy == OnlyLocalZoneNodes
}

// GetNodeSelectionPolicy return corresponding policy string in integer.
func GetNodeSelectionPolicy(policy NodeSelectionPolicy) string {
	switch policy {
	case AllNodes:
		return AllNodesStr
	case PriorityLocalZoneNodes:
		return PriorityLocalZoneNodesStr
	case OnlyLocalZoneNodes:
		return OnlyLocalZoneNodesStr
	default:
		return AllNodesStr
	}
}

// GetNodeSelectionPolicyByStr return corresponding policy in string.
func GetNodeSelectionPolicyByStr(str string) NodeSelectionPolicy {
	switch str {
	case AllNodesStr:
		return AllNodes
	case PriorityLocalZoneNodesStr:
		return PriorityLocalZoneNodes
	case OnlyLocalZoneNodesStr:
		return OnlyLocalZoneNodes
	default:
		return AllNodes
	}
}

const (
	// MaxRemoteReadCountPerNodeForOnlyLocalZone is the max remote read count per node for only local zone.
	MaxRemoteReadCountPerNodeForOnlyLocalZone = 3
)
