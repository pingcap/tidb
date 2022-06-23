// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/oracle"
)

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

// Checkpoints is a heap that collectes all checkpoints of
// regions, it supports query the latest checkpoint fastly.
// This structure is thread safe.
type Checkpoints struct {
	tree *btree.BTree

	mu sync.Mutex
}

func NewCheckpoints() Checkpoints {
	return Checkpoints{
		tree: btree.New(32),
	}
}

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

func (h *Checkpoints) insertDirect(r RangesSharesTS) {
	h.mu.Lock()
	defer h.mu.Unlock()
	old := h.tree.ReplaceOrInsert(&r)
	if old != nil {
		item := h.tree.Get(old).(*RangesSharesTS)
		item.Ranges = append(item.Ranges, old.(*RangesSharesTS).Ranges...)
	}
}

func (h *Checkpoints) InsertRegion(ts uint64, region RegionWithLeader) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.tree.Get(&RangesSharesTS{TS: ts})
	if r == nil {
		r = &RangesSharesTS{TS: ts}
		h.tree.ReplaceOrInsert(r)
	}
	rr := r.(*RangesSharesTS)
	rr.Ranges = append(rr.Ranges, kv.KeyRange{
		StartKey: region.Region.StartKey,
		EndKey:   region.Region.EndKey,
	})
}

func (h *Checkpoints) PopMinTsRegions() *RangesSharesTS {
	h.mu.Lock()
	defer h.mu.Unlock()
	item, ok := h.tree.DeleteMin().(*RangesSharesTS)
	if !ok {
		return nil
	}
	return item
}

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

func (h *Checkpoints) CheckpointTS() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	item, ok := h.tree.Min().(*RangesSharesTS)
	if !ok {
		return 0
	}
	return item.TS
}
