// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"bytes"

	"github.com/google/btree"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

// Range represents a backup response.
type Range struct {
	StartKey []byte
	EndKey   []byte
	Files    []*backuppb.File
	Size     uint64
}

// BytesAndKeys returns total bytes and keys in a range.
func (rg *Range) BytesAndKeys() (bytes, keys uint64) {
	for _, f := range rg.Files {
		bytes += f.TotalBytes
		keys += f.TotalKvs
	}
	return
}

// Intersect returns intersect range in the tree.
func (rg *Range) Intersect(
	start, end []byte,
) (subStart, subEnd []byte, isIntersect bool) {
	// empty mean the max end key
	if len(rg.EndKey) != 0 && bytes.Compare(start, rg.EndKey) >= 0 {
		isIntersect = false
		return
	}
	if len(end) != 0 && bytes.Compare(end, rg.StartKey) <= 0 {
		isIntersect = false
		return
	}
	isIntersect = true
	if bytes.Compare(start, rg.StartKey) >= 0 {
		subStart = start
	} else {
		subStart = rg.StartKey
	}
	switch {
	case len(end) == 0:
		subEnd = rg.EndKey
	case len(rg.EndKey) == 0:
		subEnd = end
	case bytes.Compare(end, rg.EndKey) < 0:
		subEnd = end
	default:
		subEnd = rg.EndKey
	}
	return
}

// Contains check if the range contains the given key, [start, end).
func (rg *Range) Contains(key []byte) bool {
	start, end := rg.StartKey, rg.EndKey
	return bytes.Compare(key, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

// Less impls btree.Item.
func (rg *Range) Less(than btree.Item) bool {
	// rg.StartKey < than.StartKey
	ta := than.(*Range)
	return bytes.Compare(rg.StartKey, ta.StartKey) < 0
}

// NeedsMerge checks whether two ranges needs to be merged.
func NeedsMerge(left, right *Range, splitSizeBytes, splitKeyCount uint64) bool {
	leftBytes, leftKeys := left.BytesAndKeys()
	rightBytes, rightKeys := right.BytesAndKeys()
	if rightBytes == 0 {
		return true
	}
	if leftBytes+rightBytes > splitSizeBytes {
		return false
	}
	if leftKeys+rightKeys > splitKeyCount {
		return false
	}
	tableID1, indexID1, isRecord1, err1 := tablecodec.DecodeKeyHead(kv.Key(left.StartKey))
	tableID2, indexID2, isRecord2, err2 := tablecodec.DecodeKeyHead(kv.Key(right.StartKey))

	// Failed to decode the file key head... can this happen?
	if err1 != nil || err2 != nil {
		log.Warn("Failed to parse the key head for merging files, skipping",
			logutil.Key("left-start-key", left.StartKey),
			logutil.Key("right-start-key", right.StartKey),
			logutil.AShortError("left-err", err1),
			logutil.AShortError("right-err", err2),
		)
		return false
	}
	// Merge if they are both record keys
	if isRecord1 && isRecord2 {
		// Do not merge ranges in different tables.
		return tableID1 == tableID2
	}
	// If they are all index keys...
	if !isRecord1 && !isRecord2 {
		// Do not merge ranges in different indexes even if they are in the same
		// table, as rewrite rule only supports rewriting one pattern.
		// Merge left and right if they are in the same index.
		return tableID1 == tableID2 && indexID1 == indexID2
	}
	return false
}

var _ btree.Item = &Range{}

// RangeTree is sorted tree for Ranges.
// All the ranges it stored do not overlap.
type RangeTree struct {
	*btree.BTree
}

// NewRangeTree returns an empty range tree.
func NewRangeTree() RangeTree {
	return RangeTree{
		BTree: btree.New(32),
	}
}

// Find is a helper function to find an item that contains the range start
// key.
func (rangeTree *RangeTree) Find(rg *Range) *Range {
	var ret *Range
	rangeTree.DescendLessOrEqual(rg, func(i btree.Item) bool {
		ret = i.(*Range)
		return false
	})

	if ret == nil || !ret.Contains(rg.StartKey) {
		return nil
	}

	return ret
}

// getOverlaps gets the ranges which are overlapped with the specified range range.
func (rangeTree *RangeTree) getOverlaps(rg *Range) []*Range {
	// note that find() gets the last item that is less or equal than the range.
	// in the case: |_______a_______|_____b_____|___c___|
	// new range is     |______d______|
	// find() will return Range of range_a
	// and both startKey of range_a and range_b are less than endKey of range_d,
	// thus they are regarded as overlapped ranges.
	found := rangeTree.Find(rg)
	if found == nil {
		found = rg
	}

	var overlaps []*Range
	rangeTree.AscendGreaterOrEqual(found, func(i btree.Item) bool {
		over := i.(*Range)
		if len(rg.EndKey) > 0 && bytes.Compare(rg.EndKey, over.StartKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

// Update inserts range into tree and delete overlapping ranges.
func (rangeTree *RangeTree) Update(rg Range) {
	overlaps := rangeTree.getOverlaps(&rg)
	// Range has backuped, overwrite overlapping range.
	for _, item := range overlaps {
		log.Info("delete overlapping range",
			logutil.Key("startKey", item.StartKey),
			logutil.Key("endKey", item.EndKey))
		rangeTree.Delete(item)
	}
	rangeTree.ReplaceOrInsert(&rg)
}

// Put forms a range and inserts it into tree.
func (rangeTree *RangeTree) Put(
	startKey, endKey []byte, files []*backuppb.File,
) {
	rg := Range{
		StartKey: startKey,
		EndKey:   endKey,
		Files:    files,
	}
	rangeTree.Update(rg)
}

// InsertRange inserts ranges into the range tree.
// It returns a non-nil range if there are soe overlapped ranges.
func (rangeTree *RangeTree) InsertRange(rg Range) *Range {
	out := rangeTree.ReplaceOrInsert(&rg)
	if out == nil {
		return nil
	}
	return out.(*Range)
}

// MergedRanges output the sortedRanges having merged according to given `splitSizeBytes` and `splitKeyCount`.
func (rangeTree *RangeTree) MergedRanges(splitSizeBytes, splitKeyCount uint64) []Range {
	var mergeTargetIndex int = -1
	sortedRanges := make([]Range, 0, rangeTree.Len())
	rangeTree.Ascend(func(item btree.Item) bool {
		rg := item.(*Range)
		if mergeTargetIndex < 0 || !NeedsMerge(&sortedRanges[mergeTargetIndex], rg, splitSizeBytes, splitKeyCount) {
			// unintialized or the sortedRanges[mergeTargetIndex] does not need to merged
			mergeTargetIndex += 1
			sortedRanges = append(sortedRanges, *rg)
		} else {
			// need to merge from rg to sortedRages[mergeTargetIndex]
			sortedRanges[mergeTargetIndex].EndKey = rg.EndKey
			sortedRanges[mergeTargetIndex].Size += rg.Size
			sortedRanges[mergeTargetIndex].Files = append(sortedRanges[mergeTargetIndex].Files, rg.Files...)
		}

		return true
	})
	return sortedRanges
}

// GetSortedRanges collects and returns sorted ranges.
func (rangeTree *RangeTree) GetSortedRanges() []Range {
	sortedRanges := make([]Range, 0, rangeTree.Len())
	rangeTree.Ascend(func(rg btree.Item) bool {
		if rg == nil {
			return false
		}
		sortedRanges = append(sortedRanges, *rg.(*Range))
		return true
	})
	return sortedRanges
}

// GetIncompleteRange returns missing range covered by startKey and endKey.
func (rangeTree *RangeTree) GetIncompleteRange(
	startKey, endKey []byte,
) []Range {
	if len(startKey) != 0 && bytes.Equal(startKey, endKey) {
		return []Range{}
	}
	incomplete := make([]Range, 0, 64)
	requestRange := Range{StartKey: startKey, EndKey: endKey}
	lastEndKey := startKey
	pviot := &Range{StartKey: startKey}
	if first := rangeTree.Find(pviot); first != nil {
		pviot.StartKey = first.StartKey
	}
	pviotNotFound := true
	rangeTree.AscendGreaterOrEqual(pviot, func(i btree.Item) bool {
		pviotNotFound = false
		rg := i.(*Range)
		if bytes.Compare(lastEndKey, rg.StartKey) < 0 {
			start, end, isIntersect :=
				requestRange.Intersect(lastEndKey, rg.StartKey)
			if isIntersect {
				// There is a gap between the last item and the current item.
				incomplete =
					append(incomplete, Range{StartKey: start, EndKey: end})
			}
		}
		lastEndKey = rg.EndKey
		return len(endKey) == 0 || bytes.Compare(rg.EndKey, endKey) < 0
	})

	// Check whether we need append the last range
	if pviotNotFound ||
		(!bytes.Equal(lastEndKey, endKey) && len(lastEndKey) != 0 &&
			(len(endKey) == 0 || bytes.Compare(lastEndKey, endKey) < 0)) {
		start, end, isIntersect := requestRange.Intersect(lastEndKey, endKey)
		if isIntersect {
			incomplete =
				append(incomplete, Range{StartKey: start, EndKey: end})
		}
	}
	return incomplete
}

type ProgressRange struct {
	Res        RangeTree
	Incomplete []Range
	Origin     Range
	GroupKey   string
}
