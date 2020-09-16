// Copyright 2018 PingCAP, Inc.
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

package set

import (
	"sync"
)

// SyncSet is a synchronized set
type SyncSet struct {
	data map[interface{}]struct{}
	lock *sync.RWMutex
}

// NewSyncSet builds a synchronized set.
func NewSyncSet(vs ...interface{}) SyncSet {
	set := SyncSet{
		data: map[interface{}]struct{}{},
		lock: &sync.RWMutex{},
	}
	for _, v := range vs {
		set.data[v] = struct{}{}
	}
	return set
}

// Exist checks whether `val` exists in `s`.
func (s SyncSet) Exist(val interface{}) bool {
	s.lock.RLock()
	_, ok := s.data[val]
	s.lock.RUnlock()
	return ok
}

// InsertIfNotExist inserts `val` into `s` if `val` does not exists in `s`.
// It returns true if `val` already exists.
func (s SyncSet) InsertIfNotExist(val interface{}) bool {
	s.lock.Lock()
	_, ok := s.data[val]
	if ok {
		s.lock.Unlock()
		return true
	}
	s.data[val] = struct{}{}
	s.lock.Unlock()
	return false
}

// Count returns the number in Set s.
func (s SyncSet) Count() int {
	s.lock.RLock()
	count := len(s.data)
	s.lock.RUnlock()
	return count
}
