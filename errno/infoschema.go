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

package errno

import (
	"sync"
	"time"
)

// The error summary is protected by a mutex for simplicity.
// It is not expected to be hot unless there are concurrent workloads
// that are generating high error/warning counts, in which case
// the system probably has other issues already.

// ErrorSummary summarizes errors and warnings
type ErrorSummary struct {
	ErrorCount   int
	WarningCount int
	FirstSeen    time.Time
	LastSeen     time.Time
}

// instanceStatistics provide statistics for a tidb-server instance.
type instanceStatistics struct {
	sync.Mutex
	global map[uint16]*ErrorSummary
	users  map[string]map[uint16]*ErrorSummary
	hosts  map[string]map[uint16]*ErrorSummary
}

var stats instanceStatistics

func init() {
	FlushStats()
}

// FlushStats resets errors and warnings across global/users/hosts
func FlushStats() {
	stats.Lock()
	defer stats.Unlock()
	stats.global = make(map[uint16]*ErrorSummary)
	stats.users = make(map[string]map[uint16]*ErrorSummary)
	stats.hosts = make(map[string]map[uint16]*ErrorSummary)
}

func copyMap(oldMap map[uint16]*ErrorSummary) map[uint16]*ErrorSummary {
	newMap := make(map[uint16]*ErrorSummary, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = &ErrorSummary{
			ErrorCount:   v.ErrorCount,
			WarningCount: v.WarningCount,
			FirstSeen:    v.FirstSeen,
			LastSeen:     v.LastSeen,
		}
	}
	return newMap
}

// GlobalStats summarizes errors and warnings across all users/hosts
func GlobalStats() map[uint16]*ErrorSummary {
	stats.Lock()
	defer stats.Unlock()
	return copyMap(stats.global)
}

// UserStats summarizes per-user
func UserStats() map[string]map[uint16]*ErrorSummary {
	stats.Lock()
	defer stats.Unlock()
	newMap := make(map[string]map[uint16]*ErrorSummary, len(stats.users))
	for k, v := range stats.users {
		newMap[k] = copyMap(v)
	}
	return newMap
}

// HostStats summarizes per remote-host
func HostStats() map[string]map[uint16]*ErrorSummary {
	stats.Lock()
	defer stats.Unlock()
	newMap := make(map[string]map[uint16]*ErrorSummary, len(stats.hosts))
	for k, v := range stats.hosts {
		newMap[k] = copyMap(v)
	}
	return newMap
}

func initCounters(errCode uint16, user, host string) {
	seen := time.Now()
	stats.Lock()
	defer stats.Unlock()

	if _, ok := stats.global[errCode]; !ok {
		stats.global[errCode] = &ErrorSummary{FirstSeen: seen}
	}
	if _, ok := stats.users[user]; !ok {
		stats.users[user] = make(map[uint16]*ErrorSummary)
	}
	if _, ok := stats.users[user][errCode]; !ok {
		stats.users[user][errCode] = &ErrorSummary{FirstSeen: seen}
	}
	if _, ok := stats.hosts[host]; !ok {
		stats.hosts[host] = make(map[uint16]*ErrorSummary)
	}
	if _, ok := stats.hosts[host][errCode]; !ok {
		stats.hosts[host][errCode] = &ErrorSummary{FirstSeen: seen}
	}
}

// IncrementError increments the global/user/host statistics for an errCode
func IncrementError(errCode uint16, user, host string) {
	seen := time.Now()
	initCounters(errCode, user, host)

	stats.Lock()
	defer stats.Unlock()

	// Increment counter + update last seen
	stats.global[errCode].ErrorCount++
	stats.global[errCode].LastSeen = seen
	// Increment counter + update last seen
	stats.users[user][errCode].ErrorCount++
	stats.users[user][errCode].LastSeen = seen
	// Increment counter + update last seen
	stats.hosts[host][errCode].ErrorCount++
	stats.hosts[host][errCode].LastSeen = seen
}

// IncrementWarning increments the global/user/host statistics for an errCode
func IncrementWarning(errCode uint16, user, host string) {
	seen := time.Now()
	initCounters(errCode, user, host)

	stats.Lock()
	defer stats.Unlock()

	// Increment counter + update last seen
	stats.global[errCode].WarningCount++
	stats.global[errCode].LastSeen = seen
	// Increment counter + update last seen
	stats.users[user][errCode].WarningCount++
	stats.users[user][errCode].LastSeen = seen
	// Increment counter + update last seen
	stats.hosts[host][errCode].WarningCount++
	stats.hosts[host][errCode].LastSeen = seen
}
