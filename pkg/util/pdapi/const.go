// Copyright 2019 PingCAP, Inc.
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

package pdapi

import (
	"fmt"
	"time"
)

// The following constants are the APIs of PD server.
const (
	HotRead                = "/pd/api/v1/hotspot/regions/read"
	HotHistory             = "/pd/api/v1/hotspot/regions/history"
	Regions                = "/pd/api/v1/regions"
	StoreRegions           = "/pd/api/v1/regions/store"
	EmptyRegions           = "/pd/api/v1/regions/check/empty-region"
	AccelerateSchedule     = "/pd/api/v1/regions/accelerate-schedule"
	RegionByID             = "/pd/api/v1/region/id"
	store                  = "/pd/api/v1/store"
	Stores                 = "/pd/api/v1/stores"
	Status                 = "/pd/api/v1/status"
	RegionStats            = "/pd/api/v1/stats/region"
	Version                = "/pd/api/v1/version"
	Config                 = "/pd/api/v1/config"
	ClusterVersion         = "/pd/api/v1/config/cluster-version"
	ScheduleConfig         = "/pd/api/v1/config/schedule"
	ReplicateConfig        = "/pd/api/v1/config/replicate"
	PlacementRule          = "/pd/api/v1/config/rule"
	PlacementRules         = "/pd/api/v1/config/rules"
	PlacementRulesGroup    = "/pd/api/v1/config/rules/group"
	RegionLabelRule        = "/pd/api/v1/config/region-label/rule"
	Schedulers             = "/pd/api/v1/schedulers"
	scatterRangeScheduler  = "/pd/api/v1/schedulers/scatter-range-"
	ResetTS                = "/pd/api/v1/admin/reset-ts"
	BaseAllocID            = "/pd/api/v1/admin/base-alloc-id"
	SnapshotRecoveringMark = "/pd/api/v1/admin/cluster/markers/snapshot-recovering"
	MinResolvedTS          = "/pd/api/v1/min-resolved-ts"
	PProfProfile           = "/pd/api/v1/debug/pprof/profile"
	PProfHeap              = "/pd/api/v1/debug/pprof/heap"
	PProfMutex             = "/pd/api/v1/debug/pprof/mutex"
	PProfAllocs            = "/pd/api/v1/debug/pprof/allocs"
	PProfBlock             = "/pd/api/v1/debug/pprof/block"
	PProfGoroutine         = "/pd/api/v1/debug/pprof/goroutine"
)

// ConfigWithTTLSeconds returns the config API with the TTL seconds parameter.
func ConfigWithTTLSeconds(ttlSeconds float64) string {
	return fmt.Sprintf("%s?ttlSecond=%.0f", Config, ttlSeconds)
}

// StoreByID returns the store API with store ID parameter.
func StoreByID(id uint64) string {
	return fmt.Sprintf("%s/%d", store, id)
}

// StoreLabelByID returns the store label API with store ID parameter.
func StoreLabelByID(id uint64) string {
	return fmt.Sprintf("%s/%d/label", store, id)
}

// RegionStatsByStartEndKey returns the region stats API with start key and end key parameters.
func RegionStatsByStartEndKey(startKey, endKey string) string {
	return fmt.Sprintf("%s?start_key=%s&end_key=%s", RegionStats, startKey, endKey)
}

// SchedulerByName returns the scheduler API with the given scheduler name.
func SchedulerByName(name string) string {
	return fmt.Sprintf("%s/%s", Schedulers, name)
}

// ScatterRangeSchedulerWithName returns the scatter range scheduler API with name parameter.
func ScatterRangeSchedulerWithName(name string) string {
	return fmt.Sprintf("%s%s", scatterRangeScheduler, name)
}

// PProfProfileAPIWithInterval returns the pprof profile API with interval parameter.
func PProfProfileAPIWithInterval(interval time.Duration) string {
	return fmt.Sprintf("%s?seconds=%d", PProfProfile, interval/time.Second)
}

// PProfGoroutineWithDebugLevel returns the pprof goroutine API with debug level parameter.
func PProfGoroutineWithDebugLevel(level int) string {
	return fmt.Sprintf("%s?debug=%d", PProfGoroutine, level)
}
