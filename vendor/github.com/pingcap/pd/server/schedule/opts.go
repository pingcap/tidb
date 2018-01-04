// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"time"
)

// Options for schedulers.
type Options interface {
	GetLeaderScheduleLimit() uint64
	GetRegionScheduleLimit() uint64
	GetReplicaScheduleLimit() uint64

	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxStoreDownTime() time.Duration

	GetMaxReplicas() int
	GetLocationLabels() []string

	GetHotRegionLowThreshold() int
	GetTolerantSizeRatio() float64
}

// NamespaceOptions for namespace cluster
type NamespaceOptions interface {
	GetLeaderScheduleLimit(name string) uint64
	GetRegionScheduleLimit(name string) uint64
	GetReplicaScheduleLimit(name string) uint64
	GetMaxReplicas(name string) int
}
