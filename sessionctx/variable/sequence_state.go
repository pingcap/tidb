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

package variable

import (
	"sync"
)

// SequenceState cache all sequence's latest value accessed by lastval() builtins. It's a session scoped
// variable, and all public methods of SequenceState are currently-safe.
type SequenceState struct {
	mu sync.Mutex
	// latestValueMap caches the last value obtained by nextval() for each sequence id.
	latestValueMap map[int64]int64
}

// NewSequenceState creates a SequenceState.
func NewSequenceState() *SequenceState {
	return &SequenceState{mu: sync.Mutex{}, latestValueMap: make(map[int64]int64)}
}

// UpdateState will update the last value of specified sequenceID in a session.
func (ss *SequenceState) UpdateState(sequenceID, value int64) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.latestValueMap[sequenceID] = value
}

// GetLastValue will return last value of the specified sequenceID, bool(true) indicates
// the sequenceID is not in the cache map and NULL will be returned.
func (ss *SequenceState) GetLastValue(sequenceID int64) (int64, bool, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if len(ss.latestValueMap) > 0 {
		if last, ok := ss.latestValueMap[sequenceID]; ok {
			return last, false, nil
		}
	}
	return 0, true, nil
}
