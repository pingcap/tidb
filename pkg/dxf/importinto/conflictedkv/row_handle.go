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

package conflictedkv

import (
	"maps"
	"sync/atomic"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"go.uber.org/zap"
)

const (
	// we use this size as a hint to the map size for the conflict rows which is
	// from index KV conflict and ingested to downstream.
	// as conflict row might generate more than one conflict KV, and its data KV
	// might not be ingested to downstream, so we choose a small value for its init
	// size.
	initMapSizeForConflictedRows = 128
	rowKeyMapEntryShallowSize    = int64(unsafe.Sizeof("") + unsafe.Sizeof(true))
)

// KeyFilter is used to filter data row keys.
type KeyFilter struct {
	globalSet *BoundedKeySet
	localSet  *BoundedKeySet
}

// NewKeyFilter creates a new KeyFilter.
// exported for test.
func NewKeyFilter(globalSet, localSet *BoundedKeySet) *KeyFilter {
	return &KeyFilter{globalSet: globalSet, localSet: localSet}
}

// it's used to check whether the row key is already handled when processing
// another index kv group.
func (f *KeyFilter) isHandledGlobally(rowKey tidbkv.Key) bool {
	if f == nil {
		return false
	}
	return f.globalSet.Contains(rowKey)
}

func (f *KeyFilter) isHandledLocally(keyStr string) bool {
	if f == nil {
		return false
	}
	return f.localSet.containsStrKey(keyStr)
}

func (f *KeyFilter) addLocal(keyStr string) {
	if f == nil {
		return
	}
	f.localSet.addStr(keyStr)
}

// BoundedKeySet is a set of data row keys with a size limit.
// this set is not goroutine safe.
type BoundedKeySet struct {
	logger *zap.Logger
	// we use a shared size, as we collect conflicted rows concurrently
	sharedSize *atomic.Int64
	sizeLimit  int64
	rowKeys    map[string]bool
}

// NewBoundedKeySet creates a new BoundedKeySet.
func NewBoundedKeySet(logger *zap.Logger, sharedSize *atomic.Int64, limit int64) *BoundedKeySet {
	size := initMapSizeForConflictedRows
	if sharedSize.Load() >= limit {
		size = 0
	}
	return &BoundedKeySet{
		logger:     logger,
		sharedSize: sharedSize,
		sizeLimit:  limit,
		rowKeys:    make(map[string]bool, size),
	}
}

// Add adds a data row key to the set.
// for partitioned table, row handle itself is not enough to make sure uniqueness,
// need to used together with the physical table ID, so we use row key directly.
func (s *BoundedKeySet) Add(rowKey tidbkv.Key) {
	if s.BoundExceeded() {
		return
	}
	s.addStr(string(rowKey))
}

func (s *BoundedKeySet) addStr(keyStr string) {
	if s.BoundExceeded() {
		return
	}

	delta := int64(len(keyStr)) + rowKeyMapEntryShallowSize
	newSize := s.sharedSize.Add(delta)
	s.rowKeys[keyStr] = true

	// log when exceeding the limit for the first time
	if newSize >= s.sizeLimit && newSize-delta < s.sizeLimit {
		s.logger.Info("too many conflict rows from index, skip checking",
			zap.String("totalRowKeySize", units.BytesSize(float64(s.sharedSize.Load()))),
			zap.String("sizeLimit", units.BytesSize(float64(s.sizeLimit))),
			zap.Int("localRowKeyCount", len(s.rowKeys)))
		// Note: we still keep the row keys in memory, to make them merged into the
		// global set, so we can avoid handling other KV groups of the same row as
		// much as possible.
		return
	}
}

// Contains checks whether the data row key is in the set.
func (s *BoundedKeySet) Contains(rowKey tidbkv.Key) bool {
	// during handling the first kv group, this map is empty, we can avoid calling
	// string(rowKey)
	if len(s.rowKeys) == 0 {
		return false
	}
	return s.rowKeys[string(rowKey)]
}

// Contains checks whether the data row key is in the set.
func (s *BoundedKeySet) containsStrKey(keyStr string) bool {
	if len(s.rowKeys) == 0 {
		return false
	}
	return s.rowKeys[keyStr]
}

// Merge merges another BoundedKeySet into this one.
func (s *BoundedKeySet) Merge(other *BoundedKeySet) {
	if other == nil {
		return
	}
	maps.Copy(s.rowKeys, other.rowKeys)
}

// BoundExceeded checks whether the size limit is exceeded.
func (s *BoundedKeySet) BoundExceeded() bool {
	limit := s.sizeLimit
	failpoint.InjectCall("mockKeySetSizeLimit", &limit)
	return s.sharedSize.Load() >= limit
}
