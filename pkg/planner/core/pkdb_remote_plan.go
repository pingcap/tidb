// Copyright 2024 PingCAP, Inc.
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

package core

// PhyPlanType represents the physical plan location type.
// It determines whether a plan should be executed locally, remotely, or in a distributed manner.
type PhyPlanType int

const (
	// PhyPlanUninitialized indicates the plan type has not been determined yet.
	PhyPlanUninitialized PhyPlanType = iota
	// PhyPlanLocal indicates the plan can be executed entirely on the local TiDB node.
	// This includes plans that only access local data or don't require data access.
	PhyPlanLocal
	// PhyPlanRemote indicates the plan should be forwarded to a specific remote TiDB node.
	// This is typically used when all data accessed by the plan resides on a single remote node.
	PhyPlanRemote
	// PhyPlanDistributed indicates the plan requires distributed execution across multiple nodes.
	// This is used for plans that access data from multiple TiKV/TiFlash regions.
	PhyPlanDistributed
	// PhyPlanUncertain indicates the plan type cannot be determined at planning time.
	// This may happen when the data distribution is unknown or dynamic.
	PhyPlanUncertain
)

// String returns the string representation of the plan type.
func (t PhyPlanType) String() string {
	switch t {
	case PhyPlanUninitialized:
		return "UNINITIALIZED"
	case PhyPlanLocal:
		return "LOCAL"
	case PhyPlanRemote:
		return "REMOTE"
	case PhyPlanDistributed:
		return "DISTRIBUTED"
	case PhyPlanUncertain:
		return "UNCERTAIN"
	default:
		return "UNKNOWN"
	}
}

// ShouldForward returns true if the plan type indicates the query should be forwarded
// to a remote node for execution. Only PhyPlanRemote plans should be forwarded.
func (t PhyPlanType) ShouldForward() bool {
	return t == PhyPlanRemote
}

// PlanLocationInfo contains information about where a plan's data is located.
// This is used to determine the plan type and target node.
type PlanLocationInfo struct {
	// PlanType is the determined plan type
	PlanType PhyPlanType
	// TargetStore is the store address if PlanType is PhyPlanRemote
	TargetStore string
	// TableID is the table being accessed (if applicable)
	TableID int64
	// PartitionID is the partition being accessed (if applicable, 0 for non-partitioned)
	PartitionID int64
	// Reason explains why this plan type was determined
	Reason string
}
