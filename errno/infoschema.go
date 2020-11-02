// Copyright 2020 PingCAP, Inc.
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

// A set of counters for errors and warnings displayed in infoschema
// TODO: provide a method to reset statistics.

type errorSummary struct {
	sync.RWMutex
	ErrorCount   int
	WarningCount int
	FirstSeen    time.Time
	LastSeen     time.Time
}

type globalStats struct {
	sync.RWMutex
	errors map[uint16]*errorSummary
}

type userStats struct {
	sync.RWMutex
	errors map[string]map[uint16]*errorSummary
}

type hostStats struct {
	sync.RWMutex
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

// GlobalStats summarizes errors and warnings across all users/hosts
func GlobalStats() map[uint16]*errorSummary {
	return global.errors
}

// UserStats summarizes per-user
func UserStats() map[string]map[uint16]*errorSummary {
	return users.errors
}

// HostStats summarizes per remote-host
func HostStats() map[string]map[uint16]*errorSummary {
	return hosts.errors
}

func initCounters(errCode uint16, user, host string) {
	if _, ok := global.errors[errCode]; !ok {
		global.errors[errCode] = &errorSummary{FirstSeen: time.Now()}
	}
	if _, ok := users.errors[user]; !ok {
		users.errors[user] = make(map[uint16]*errorSummary)
	}
	if _, ok := hosts.errors[host]; !ok {
		hosts.errors[host] = make(map[uint16]*errorSummary)
	}
	if _, ok := users.errors[user][errCode]; !ok {
		users.errors[user][errCode] = &errorSummary{FirstSeen: time.Now()}
	}
	if _, ok := hosts.errors[host][errCode]; !ok {
		hosts.errors[host][errCode] = &errorSummary{FirstSeen: time.Now()}
	}
}

// IncrementError increments the global/user/host statistics for an errCode
func IncrementError(errCode uint16, user, host string) {
	initCounters(errCode, user, host)
	global.errors[errCode].ErrorCount++
	global.errors[errCode].LastSeen = time.Now()
	users.errors[user][errCode].ErrorCount++
	users.errors[user][errCode].LastSeen = time.Now()
	hosts.errors[host][errCode].ErrorCount++
	hosts.errors[host][errCode].LastSeen = time.Now()
}

// IncrementWarning increments the global/user/host statistics for an errCode
func IncrementWarning(errCode uint16, user, host string) {
	initCounters(errCode, user, host)
	global.errors[errCode].WarningCount++
	global.errors[errCode].LastSeen = time.Now()
	users.errors[user][errCode].WarningCount++
	users.errors[user][errCode].LastSeen = time.Now()
	hosts.errors[host][errCode].WarningCount++
	hosts.errors[host][errCode].LastSeen = time.Now()
}
