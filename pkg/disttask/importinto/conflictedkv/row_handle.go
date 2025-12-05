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
	handleMapEntryShallowSize    = int64(unsafe.Sizeof("") + unsafe.Sizeof(true))
)

// HandleFilter is used to filter row handles.
type HandleFilter struct {
	set *BoundedHandleSet
}

// NewHandleFilter creates a new HandleFilter.
// exported for test.
func NewHandleFilter(set *BoundedHandleSet) *HandleFilter {
	return &HandleFilter{set: set}
}

func (f *HandleFilter) needSkip(handle tidbkv.Handle) bool {
	if f == nil {
		return false
	}
	return f.set.Contains(handle)
}

// BoundedHandleSet is a set of row handles with a size limit.
type BoundedHandleSet struct {
	logger *zap.Logger
	// we use a shared size, as we collect conflicted rows concurrently
	sharedSize *atomic.Int64
	sizeLimit  int64
	handles    map[string]bool
}

// NewBoundedHandleSet creates a new BoundedHandleSet.
func NewBoundedHandleSet(logger *zap.Logger, sharedSize *atomic.Int64, limit int64) *BoundedHandleSet {
	size := initMapSizeForConflictedRows
	if sharedSize.Load() >= limit {
		size = 0
	}
	return &BoundedHandleSet{
		logger:     logger,
		sharedSize: sharedSize,
		sizeLimit:  limit,
		handles:    make(map[string]bool, size),
	}
}

// Add adds a handle to the set.
func (s *BoundedHandleSet) Add(handle tidbkv.Handle) {
	if s.BoundExceeded() {
		return
	}

	hdlStr := handle.String()
	delta := int64(len(hdlStr)) + handleMapEntryShallowSize
	newSize := s.sharedSize.Add(delta)
	s.handles[hdlStr] = true

	// log when exceeding the limit for the first time
	if newSize >= s.sizeLimit && newSize-delta < s.sizeLimit {
		s.logger.Info("too many conflict rows from index, skip checking",
			zap.String("totalHandleSize", units.BytesSize(float64(s.sharedSize.Load()))),
			zap.String("sizeLimit", units.BytesSize(float64(s.sizeLimit))),
			zap.Int("localHandleCount", len(s.handles)))
		// Note: we still keep the handles in memory, to make it merged into the
		// global handle set, so we can avoid handling other KV groups of same
		// handle as much as possible.
		return
	}
}

// Contains checks whether the handle is in the set.
func (s *BoundedHandleSet) Contains(handle tidbkv.Handle) bool {
	// during handling the first kv group, this map is empty, we can avoid calling
	// handle.String()
	if len(s.handles) == 0 {
		return false
	}
	return s.handles[handle.String()]
}

// Merge merges another BoundedHandleSet into this one.
func (s *BoundedHandleSet) Merge(other *BoundedHandleSet) {
	if other == nil {
		return
	}
	maps.Copy(s.handles, other.handles)
}

// BoundExceeded checks whether the size limit is exceeded.
func (s *BoundedHandleSet) BoundExceeded() bool {
	limit := s.sizeLimit
	failpoint.InjectCall("mockHandleSetSizeLimit", &limit)
	return s.sharedSize.Load() >= limit
}
