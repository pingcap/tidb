// Copyright 2022 PingCAP, Inc.
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

//go:build !deadlock
// +build !deadlock

package syncutil

import "sync"

// Mutex is a mutual exclusion lock. The zero value for a Mutex is an unlocked mutex.
//
// Mutex must not be copied after first use.
type Mutex struct {
	sync.Mutex
}

// RWMutex is a reader/writer mutual exclusion lock.
// The lock can be held by an arbitrary number of readers or a single writer.
// The zero value for a RWMutex is an unlocked mutex.
//
// RWMutex must not be copied after first use.
type RWMutex struct {
	sync.RWMutex
}
