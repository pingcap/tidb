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
	sync.Mutex
	ErrorCount   int
	WarningCount int
	FirstSeen    time.Time
	LastSeen     time.Time
}

// GlobalStats summarizes errors and warnings globally
type GlobalStats struct {
	sync.Mutex
	Errors map[uint16]*ErrorSummary
}

// UserStats summarizes errors and warnings per user
type UserStats struct {
	sync.Mutex
	Errors map[string]map[uint16]*ErrorSummary
}

// HostStats summarizes errors and warnings per host
type HostStats struct {
	sync.Mutex
	Errors map[string]map[uint16]*ErrorSummary
}

var global GlobalStats
var users UserStats
var hosts HostStats

func init() {
	global.Errors = make(map[uint16]*ErrorSummary)
	users.Errors = make(map[string]map[uint16]*ErrorSummary)
	hosts.Errors = make(map[string]map[uint16]*ErrorSummary)
}

// FlushStats resets errors and warnings across global/users/hosts
func FlushStats() {
	global.Lock()
	defer global.Unlock()
	users.Lock()
	defer users.Unlock()
	hosts.Lock()
	defer hosts.Unlock()

	global.Errors = make(map[uint16]*ErrorSummary)
	users.Errors = make(map[string]map[uint16]*ErrorSummary)
	hosts.Errors = make(map[string]map[uint16]*ErrorSummary)
}

// GetGlobalStats summarizes errors and warnings across all users/hosts
// It should be read while held under a lock.
func GetGlobalStats() *GlobalStats {
	return &global
}

// GetUserStats summarizes per-user
// It should be read while held under a lock.
func GetUserStats() *UserStats {
	return &users
}

// GetHostStats summarizes per remote-host
// It should be read while held under a lock.
func GetHostStats() *HostStats {
	return &hosts
}

func initCounters(errCode uint16, user, host string) {
	seen := time.Now()
	global.Lock()
	if _, ok := global.Errors[errCode]; !ok {
		global.Errors[errCode] = &ErrorSummary{FirstSeen: seen}
	}
	global.Unlock()
	users.Lock()
	if _, ok := users.Errors[user]; !ok {
		users.Errors[user] = make(map[uint16]*ErrorSummary)
	}
	if _, ok := users.Errors[user][errCode]; !ok {
		users.Errors[user][errCode] = &ErrorSummary{FirstSeen: seen}
	}
	users.Unlock()
	hosts.Lock()
	if _, ok := hosts.Errors[host]; !ok {
		hosts.Errors[host] = make(map[uint16]*ErrorSummary)
	}
	if _, ok := hosts.Errors[host][errCode]; !ok {
		hosts.Errors[host][errCode] = &ErrorSummary{FirstSeen: seen}
	}
	hosts.Unlock()
}

// IncrementError increments the global/user/host statistics for an errCode
func IncrementError(errCode uint16, user, host string) {
	seen := time.Now()
	initCounters(errCode, user, host)
	// Increment counter + update last seen
	global.Errors[errCode].Lock()
	global.Errors[errCode].ErrorCount++
	global.Errors[errCode].LastSeen = seen
	global.Errors[errCode].Unlock()
	// Increment counter + update last seen
	users.Errors[user][errCode].Lock()
	users.Errors[user][errCode].ErrorCount++
	users.Errors[user][errCode].LastSeen = seen
	users.Errors[user][errCode].Unlock()
	// Increment counter + update last seen
	hosts.Errors[host][errCode].Lock()
	hosts.Errors[host][errCode].ErrorCount++
	hosts.Errors[host][errCode].LastSeen = seen
	hosts.Errors[host][errCode].Unlock()
}

// IncrementWarning increments the global/user/host statistics for an errCode
func IncrementWarning(errCode uint16, user, host string) {
	seen := time.Now()
	initCounters(errCode, user, host)
	// Increment counter + update last seen
	global.Errors[errCode].Lock()
	global.Errors[errCode].WarningCount++
	global.Errors[errCode].LastSeen = seen
	global.Errors[errCode].Unlock()
	// Increment counter + update last seen
	users.Errors[user][errCode].Lock()
	users.Errors[user][errCode].WarningCount++
	users.Errors[user][errCode].LastSeen = seen
	users.Errors[user][errCode].Unlock()
	// Increment counter + update last seen
	hosts.Errors[host][errCode].Lock()
	hosts.Errors[host][errCode].WarningCount++
	hosts.Errors[host][errCode].LastSeen = seen
	hosts.Errors[host][errCode].Unlock()
}
