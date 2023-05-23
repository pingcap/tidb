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

type NodeSelectionPolicy int

const (
	AllNodes NodeSelectionPolicy = iota
	PriorityLocalZoneNodes
	OnlyLocalZoneNodes
)

const (
	AllNodesStr               = "all_nodes"
	PriorityLocalZoneNodesStr = "priority_local_zone_nodes"
	OnlyLocalZoneNodesStr     = "only_local_zone_nodes"
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
