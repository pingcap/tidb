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

	"github.com/uber-go/atomic"
)

// SyncSet is a synchronized set
type SyncSet struct {
	*sync.Map
	count *atomic.Int64
}

// NewSyncSet builds a synchronized set.
func NewSyncSet(vs ...interface{}) SyncSet {
	set := SyncSet{
		Map:   new(sync.Map),
		count: atomic.NewInt64(0),
	}
	for _, v := range vs {
		set.Store(v, struct{}{})
	}
	set.count.Add(int64(len(vs)))
	return set
}

// Exist checks whether `val` exists in `s`.
func (s SyncSet) Exist(val interface{}) bool {
	_, ok := s.Load(val)
	return ok
}

// Insert inserts `val` into `s`.
func (s SyncSet) Insert(val interface{}) {
	s.Store(val, struct{}{})
	s.count.Inc()
}

// Count returns the number in Set s.
func (s SyncSet) Count() int {
	return int(s.count.Load())
}
