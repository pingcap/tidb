// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vardef

import (
	"strings"
	"sync/atomic"
	"time"
)

var (
	// schemaLease is lease of info schema, we use this to check whether info schema
	// is valid in SchemaChecker. we also use half of it as info schema reload interval.
	// Default info schema lease 45s which is init at main, we set it to 1 second
	// here for tests. you can change it with a proper time, but you must know that
	// too little may cause badly performance degradation.
	schemaLease = int64(1 * time.Second)
	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)
	// planReplayerGCLease is the time for plan replayer gc.
	planReplayerGCLease = int64(10 * time.Minute)
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// GetSchemaLease returns the schema lease time.
func GetSchemaLease() time.Duration {
	return time.Duration(atomic.LoadInt64(&schemaLease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// GetStatsLease returns the stats lease time.
func GetStatsLease() time.Duration {
	return time.Duration(atomic.LoadInt64(&statsLease))
}

// SetPlanReplayerGCLease changes the default plan repalyer gc lease time.
func SetPlanReplayerGCLease(lease time.Duration) {
	atomic.StoreInt64(&planReplayerGCLease, int64(lease))
}

// GetPlanReplayerGCLease returns the plan replayer gc lease time.
func GetPlanReplayerGCLease() time.Duration {
	return time.Duration(atomic.LoadInt64(&planReplayerGCLease))
}

// IsReadOnlyVarInNextGen checks if the variable is read-only in the nextgen.
func IsReadOnlyVarInNextGen(name string) bool {
	name = strings.ToLower(name)
	switch name {
	case TiDBEnableMDL, TiDBDDLReorgMaxWriteSpeed, TiDBDDLDiskQuota,
		TiDBEnableDistTask, TiDBDDLEnableFastReorg:
		return true
	}
	return false
}
