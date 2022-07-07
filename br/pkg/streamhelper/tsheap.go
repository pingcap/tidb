// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/oracle"
)

// CheckpointsCache is the heap-like cache for checkpoints.
//
// "Checkpoint" is the "Resolved TS" of some range.
// A resolved ts is a "watermark" for the system, which:
//   - implies there won't be any transactions (in some range) commit with `commit_ts` smaller than this TS.
//   - is monotonic increasing.
// A "checkpoint" is a "safe" Resolved TS, which:
//   - is a TS *less than* the real resolved ts of now.
//   - is based on range (it only promises there won't be new committed txns in the range).
//   - the checkpoint of union of ranges is the minimal checkpoint of all ranges.
// As an example:
// +----------------------------------+
// ^-----------^ (Checkpoint = 42)
//         ^---------------^ (Checkpoint = 76)
// ^-----------------------^ (Checkpoint = min(42, 76) = 42)
//
// For calculating the global checkpoint, we can make a heap-like structure:
// Checkpoint    Ranges
// 42         -> {[0, 8], [16, 100]}
// 1002       -> {[8, 16]}
// 1082       -> {[100, inf]}
// For now, the checkpoint of range [8, 16] and [100, inf] won't affect the global checkpoint
// directly, so we can try to advance only the ranges of {[0, 8], [16, 100]} (which's checkpoint is steal).
// Once them get advance, the global checkpoint would be advanced then,
// and we don't need to update all ranges (because some new ranges don't need to be advanced so quickly.)
type CheckpointsCache interface {
	fmt.Stringer
	// InsertRange inserts a range with specified TS to the cache.
	InsertRange(ts uint64, rng kv.KeyRange)
	// InsertRanges inserts a set of ranges that sharing checkpoint to the cache.
	InsertRanges(rst RangesSharesTS)
	// CheckpointTS returns the now global (union of all ranges) checkpoint of the cache.
	CheckpointTS() uint64
	// PopRangesWithGapGT pops the ranges which's checkpoint is
	PopRangesWithGapGT(d time.Duration) []*RangesSharesTS
	// Check whether the ranges in the cache is integrate.
	ConsistencyCheck() error
	// Clear the cache.
	Clear()
}

// NoOPCheckpointCache is used when cache disabled.
type NoOPCheckpointCache struct{}

func (NoOPCheckpointCache) InsertRange(ts uint64, rng kv.KeyRange) {}

func (NoOPCheckpointCache) InsertRanges(rst RangesSharesTS) {}

func (NoOPCheckpointCache) Clear() {}

func (NoOPCheckpointCache) String() string {
	return "NoOPCheckpointCache"
}

func (NoOPCheckpointCache) CheckpointTS() uint64 {
	panic("invalid state: NoOPCheckpointCache should never be used in advancing!")
}

func (NoOPCheckpointCache) PopRangesWithGapGT(d time.Duration) []*RangesSharesTS {
	panic("invalid state: NoOPCheckpointCache should never be used in advancing!")
}

func (NoOPCheckpointCache) ConsistencyCheck() error {
	return errors.Annotatef(berrors.ErrUnsupportedOperation, "invalid state: NoOPCheckpointCache should never be used in advancing!")
}

// RangesSharesTS is a set of ranges shares the same timestamp.
type RangesSharesTS struct {
	TS     uint64
	Ranges []kv.KeyRange
}

func (rst *RangesSharesTS) String() string {
	// Make a more friendly string.
	return fmt.Sprintf("@%sR%d", oracle.GetTimeFromTS(rst.TS).Format("0405"), len(rst.Ranges))
}

func (rst *RangesSharesTS) Less(other btree.Item) bool {
	return rst.TS < other.(*RangesSharesTS).TS
}

// Checkpoints is a heap that collects all checkpoints of
// regions, it supports query the latest checkpoint fast.
// This structure is thread safe.
type Checkpoints struct {
	tree *btree.BTree

	mu sync.Mutex
}

func NewCheckpoints() *Checkpoints {
	return &Checkpoints{
		tree: btree.New(32),
	}
}

// String formats the slowest 5 ranges sharing TS to string.
func (h *Checkpoints) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()

	b := new(strings.Builder)
	count := 0
	total := h.tree.Len()
	h.tree.Ascend(func(i btree.Item) bool {
		rst := i.(*RangesSharesTS)
		b.WriteString(rst.String())
		b.WriteString(";")
		count++
		return count < 5
	})
	if total-count > 0 {
		fmt.Fprintf(b, "O%d", total-count)
	}
	return b.String()
}

// InsertRanges insert a RangesSharesTS directly to the tree.
func (h *Checkpoints) InsertRanges(r RangesSharesTS) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if items := h.tree.Get(&r); items != nil {
		i := items.(*RangesSharesTS)
		i.Ranges = append(i.Ranges, r.Ranges...)
	} else {
		h.tree.ReplaceOrInsert(&r)
	}
}

// InsertRegion inserts the region and its TS into the region tree.
func (h *Checkpoints) InsertRange(ts uint64, rng kv.KeyRange) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.tree.Get(&RangesSharesTS{TS: ts})
	if r == nil {
		r = &RangesSharesTS{TS: ts}
		h.tree.ReplaceOrInsert(r)
	}
	rr := r.(*RangesSharesTS)
	rr.Ranges = append(rr.Ranges, rng)
}

// Clear removes all records in the checkpoint cache.
func (h *Checkpoints) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tree.Clear(false)
}

// PopRangesWithGapGT pops ranges with gap greater than the specified duration.
// NOTE: maybe make something like `DrainIterator` for better composing?
func (h *Checkpoints) PopRangesWithGapGT(d time.Duration) []*RangesSharesTS {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := []*RangesSharesTS{}
	for {
		item, ok := h.tree.Min().(*RangesSharesTS)
		if !ok {
			return result
		}
		if time.Since(oracle.GetTimeFromTS(item.TS)) >= d {
			result = append(result, item)
			h.tree.DeleteMin()
		} else {
			return result
		}
	}
}

// CheckpointTS returns the cached checkpoint TS by the current state of the cache.
func (h *Checkpoints) CheckpointTS() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	item, ok := h.tree.Min().(*RangesSharesTS)
	if !ok {
		return 0
	}
	return item.TS
}

// ConsistencyCheck checks whether the tree contains the full range of key space.
// TODO: add argument to it and check a sub range.
func (h *Checkpoints) ConsistencyCheck() error {
	h.mu.Lock()
	ranges := make([]kv.KeyRange, 0, 1024)
	h.tree.Ascend(func(i btree.Item) bool {
		ranges = append(ranges, i.(*RangesSharesTS).Ranges...)
		return true
	})
	h.mu.Unlock()

	r := CollapseRanges(len(ranges), func(i int) kv.KeyRange { return ranges[i] })
	if len(r) != 1 || len(r[0].StartKey) != 0 || len(r[0].EndKey) != 0 {
		return errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
			"the region tree cannot cover the key space, collapsed: %s", logutil.StringifyKeys(r))
	}
	return nil
}
