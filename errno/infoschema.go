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

type errorSummary struct {
	sync.Mutex
	ErrorCount   int
	WarningCount int
	FirstSeen    time.Time
	LastSeen     time.Time
}

type globalStats struct {
	sync.Mutex
	errors map[uint16]*errorSummary
}

type userStats struct {
	sync.Mutex
	errors map[string]map[uint16]*errorSummary
}

type hostStats struct {
	sync.Mutex
	errors map[string]map[uint16]*errorSummary
}

var global globalStats
var users userStats
var hosts hostStats

func init() {
	global.errors = make(map[uint16]*errorSummary)
	users.errors = make(map[string]map[uint16]*errorSummary)
	hosts.errors = make(map[string]map[uint16]*errorSummary)
}

// FlushStats resets errors and warnings across global/users/hosts
func FlushStats() {
	global.Lock()
	defer global.Unlock()
	users.Lock()
	defer users.Unlock()
	hosts.Lock()
	defer hosts.Unlock()

	global.errors = make(map[uint16]*errorSummary)
	users.errors = make(map[string]map[uint16]*errorSummary)
	hosts.errors = make(map[string]map[uint16]*errorSummary)
}

// GlobalStats summarizes errors and warnings across all users/hosts
func GlobalStats() map[uint16]*errorSummary {
	global.Lock()
	defer global.Unlock()
	return global.errors
}

// UserStats summarizes per-user
func UserStats() map[string]map[uint16]*errorSummary {
	users.Lock()
	defer users.Unlock()
	return users.errors
}

// HostStats summarizes per remote-host
func HostStats() map[string]map[uint16]*errorSummary {
	hosts.Lock()
	defer hosts.Unlock()
	return hosts.errors
}

func initCounters(errCode uint16, user, host string) {
	global.Lock()
	if _, ok := global.errors[errCode]; !ok {
		global.errors[errCode] = &errorSummary{FirstSeen: time.Now()}
	}
	global.Unlock()
	users.Lock()
	if _, ok := users.errors[user]; !ok {
		users.errors[user] = make(map[uint16]*errorSummary)
	}
	if _, ok := users.errors[user][errCode]; !ok {
		users.errors[user][errCode] = &errorSummary{FirstSeen: time.Now()}
	}
	users.Unlock()
	hosts.Lock()
	if _, ok := hosts.errors[host]; !ok {
		hosts.errors[host] = make(map[uint16]*errorSummary)
	}
	if _, ok := hosts.errors[host][errCode]; !ok {
		hosts.errors[host][errCode] = &errorSummary{FirstSeen: time.Now()}
	}
	hosts.Unlock()
}

// IncrementError increments the global/user/host statistics for an errCode
func IncrementError(errCode uint16, user, host string) {
	initCounters(errCode, user, host)
	// Increment counter + update last seen
	global.errors[errCode].Lock()
	global.errors[errCode].ErrorCount++
	global.errors[errCode].LastSeen = time.Now()
	global.errors[errCode].Unlock()
	// Increment counter + update last seen
	users.errors[user][errCode].Lock()
	users.errors[user][errCode].ErrorCount++
	users.errors[user][errCode].LastSeen = time.Now()
	users.errors[user][errCode].Unlock()
	// Increment counter + update last seen
	hosts.errors[host][errCode].Lock()
	hosts.errors[host][errCode].ErrorCount++
	hosts.errors[host][errCode].LastSeen = time.Now()
	hosts.errors[host][errCode].Unlock()
}

// IncrementWarning increments the global/user/host statistics for an errCode
func IncrementWarning(errCode uint16, user, host string) {
	initCounters(errCode, user, host)
	// Increment counter + update last seen
	global.errors[errCode].Lock()
	global.errors[errCode].WarningCount++
	global.errors[errCode].LastSeen = time.Now()
	global.errors[errCode].Unlock()
	// Increment counter + update last seen
	users.errors[user][errCode].Lock()
	users.errors[user][errCode].WarningCount++
	users.errors[user][errCode].LastSeen = time.Now()
	users.errors[user][errCode].Unlock()
	// Increment counter + update last seen
	hosts.errors[host][errCode].Lock()
	hosts.errors[host][errCode].WarningCount++
	hosts.errors[host][errCode].LastSeen = time.Now()
	hosts.errors[host][errCode].Unlock()
}
