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

package util

import "sync"

// TSSet is a set of timestamps.
type TSSet struct {
	sync.RWMutex
	m map[uint64]struct{}
}

// NewTSSet creates a set to store timestamps.
func NewTSSet(capacity int) *TSSet {
	return &TSSet{
		m: make(map[uint64]struct{}, capacity),
	}
}

// Put puts timestamps into the map.
func (s *TSSet) Put(tss ...uint64) {
	s.Lock()
	defer s.Unlock()
	for _, ts := range tss {
		s.m[ts] = struct{}{}
	}
}

// GetAll returns all timestamps in the set.
func (s *TSSet) GetAll() []uint64 {
	s.RLock()
	defer s.RUnlock()
	if len(s.m) == 0 {
		return nil
	}
	ret := make([]uint64, 0, len(s.m))
	for ts := range s.m {
		ret = append(ret, ts)
	}
	return ret
}
