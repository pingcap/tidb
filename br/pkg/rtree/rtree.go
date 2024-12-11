// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"bytes"

	"github.com/google/btree"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pkg/errors"
)

// Range represents a backup response.
type Range struct {
	StartKey []byte
	EndKey   []byte
	Files    []*backuppb.File
}

func (rg *Range) ToKeyRange() *kvrpcpb.KeyRange {
	return &kvrpcpb.KeyRange{
		StartKey: rg.StartKey,
		EndKey:   rg.EndKey,
	}
}

// BytesAndKeys returns total bytes and keys in a range.
func (rg *Range) BytesAndKeys() (bytes, keys uint64) {
	keys, bytes = metautil.CalculateKvStatsOnFiles(rg.Files)
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

// RangeStats represents a restore merge result.
type RangeStats struct {
	Range
	Size  uint64
	Count uint64
}

// Less impls btree.Item.
func (rg *RangeStats) Less(ta *RangeStats) bool {
	// rg.StartKey < than.StartKey
	return bytes.Compare(rg.StartKey, ta.StartKey) < 0
}

type RangeStatsTree struct {
	*btree.BTreeG[*RangeStats]
}

func NewRangeStatsTree() RangeStatsTree {
	return RangeStatsTree{
		BTreeG: btree.NewG[*RangeStats](32, (*RangeStats).Less),
	}
}

// InsertRange inserts ranges into the range tree.
// It returns a non-nil range if there are soe overlapped ranges.
func (rangeTree *RangeStatsTree) InsertRange(rg *Range, rangeSize, rangeCount uint64) *RangeStats {
	out, _ := rangeTree.ReplaceOrInsert(&RangeStats{
		Range: *rg,
		Size:  rangeSize,
		Count: rangeCount,
	})
	return out
}

// MergedRanges output the sortedRanges having merged according to given `splitSizeBytes` and `splitKeyCount`.
func (rangeTree *RangeStatsTree) MergedRanges(splitSizeBytes, splitKeyCount uint64) []RangeStats {
	var mergeTargetIndex int = -1
	sortedRanges := make([]RangeStats, 0, rangeTree.Len())
	rangeTree.Ascend(func(rg *RangeStats) bool {
		if mergeTargetIndex < 0 || !NeedsMerge(&sortedRanges[mergeTargetIndex], rg, splitSizeBytes, splitKeyCount) {
			// unintialized or the sortedRanges[mergeTargetIndex] does not need to merged
			mergeTargetIndex += 1
			sortedRanges = append(sortedRanges, *rg)
		} else {
			// need to merge from rg to sortedRages[mergeTargetIndex]
			sortedRanges[mergeTargetIndex].EndKey = rg.EndKey
			sortedRanges[mergeTargetIndex].Size += rg.Size
			sortedRanges[mergeTargetIndex].Count += rg.Count
			sortedRanges[mergeTargetIndex].Files = append(sortedRanges[mergeTargetIndex].Files, rg.Files...)
		}

		return true
	})
	return sortedRanges
}

// NeedsMerge checks whether two ranges needs to be merged.
func NeedsMerge(left, right *RangeStats, splitSizeBytes, splitKeyCount uint64) bool {
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
	tableID1, indexID1, isRecord1, err1 := tablecodec.DecodeKeyHead(left.StartKey)
	tableID2, indexID2, isRecord2, err2 := tablecodec.DecodeKeyHead(right.StartKey)

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

	PhysicalID int64
}

// NewRangeTree returns an empty range tree.
func NewRangeTree() RangeTree {
	return RangeTree{
		BTree: btree.New(32),
	}
}

func NewRangeTreeWithPhysicalID(physicalID int64) RangeTree {
	return RangeTree{
		BTree: btree.New(32),

		PhysicalID: physicalID,
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

// isOverlapContainsRange currently only checks whether the only one overlap
// entirely contains the range to avoid partition the merged backup SST file.
//
//	the merged backup SST file:   |___table A___|___table B____|__table C___|
//	the only one overlap:                       |___overlap____|
//	the new range:                              |__new range___|
//
// skip replace to avoid table A and table C having the same SST, but tbale B
// does not have it.
func isOverlapContainsRange(overlaps []*Range, rg *Range) bool {
	if len(overlaps) != 1 {
		return false
	}
	return bytes.Compare(rg.StartKey, overlaps[0].StartKey) >= 0 &&
		bytes.Compare(rg.EndKey, overlaps[0].EndKey) <= 0
}

// removeOverlappedTableMetas remove table meta in files since the data is given up.
func (rangeTree *RangeTree) removeOverlappedTableMetas(overlaps []*Range, rg *Range, force bool) bool {
	if len(overlaps) == 0 {
		return false
	}
	if !force && isOverlapContainsRange(overlaps, rg) {
		for _, file := range rg.Files {
			for i := range file.TableMetas {
				if file.TableMetas[i] != nil && file.TableMetas[i].PhysicalId == rangeTree.PhysicalID {
					file.TableMetas[i] = nil
					break
				}
			}
		}
		return true
	}
	// files in the first overlap having other tables range, so remove the last one table meta
	for _, file := range overlaps[0].Files {
		for i := len(file.TableMetas) - 1; i >= 0; i-- {
			if file.TableMetas[i] != nil && file.TableMetas[i].PhysicalId == rangeTree.PhysicalID {
				file.TableMetas[i] = nil
				break
			}
		}
	}
	// files in the last overlap having other tables range, so remove the first one table meta
	for _, file := range overlaps[len(overlaps)-1].Files {
		for i := range file.TableMetas {
			if file.TableMetas[i] != nil && file.TableMetas[i].PhysicalId == rangeTree.PhysicalID {
				file.TableMetas[i] = nil
				break
			}
		}
	}
	return false
}

// Update inserts range into tree and delete overlapping ranges.
func (rangeTree *RangeTree) Update(rg Range) bool {
	return rangeTree.UpdateForce(rg, false)
}

func (rangeTree *RangeTree) UpdateForce(rg Range, force bool) bool {
	overlaps := rangeTree.getOverlaps(&rg)
	if rangeTree.removeOverlappedTableMetas(overlaps, &rg, force) {
		return false
	}
	// Range has backuped, overwrite overlapping range.
	for _, item := range overlaps {
		log.Info("delete overlapping range",
			logutil.Key("startKey", item.StartKey),
			logutil.Key("endKey", item.EndKey))
		rangeTree.Delete(item)
	}
	rangeTree.ReplaceOrInsert(&rg)
	return true
}

// Put forms a range and inserts it into tree.
func (rangeTree *RangeTree) Put(
	startKey, endKey []byte, files []*backuppb.File,
) bool {
	rg := Range{
		StartKey: startKey,
		EndKey:   endKey,
		Files:    files,
	}
	return rangeTree.Update(rg)
}

// Put forms a range and inserts it into tree.
func (rangeTree *RangeTree) PutForce(
	startKey, endKey []byte, files []*backuppb.File, force bool,
) bool {
	rg := Range{
		StartKey: startKey,
		EndKey:   endKey,
		Files:    files,
	}
	return rangeTree.UpdateForce(rg, force)
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

// Less impls btree.Item.
func (pr *ProgressRange) Less(than *ProgressRange) bool {
	// pr.StartKey <= than.StartKey
	return bytes.Compare(pr.Origin.StartKey, than.Origin.StartKey) < 0
}

// ProgressRangeTree is a sorted tree for ProgressRanges.
// All the progress ranges it sorted do not overlap.
type ProgressRangeTree struct {
	*btree.BTreeG[*ProgressRange]
}

// NewProgressRangeTree returns an empty range tree.
func NewProgressRangeTree() ProgressRangeTree {
	return ProgressRangeTree{
		BTreeG: btree.NewG[*ProgressRange](32, (*ProgressRange).Less),
	}
}

// find is a helper function to find an item that contains the range.
func (rangeTree *ProgressRangeTree) find(pr *ProgressRange) *ProgressRange {
	var ret *ProgressRange
	rangeTree.DescendLessOrEqual(pr, func(item *ProgressRange) bool {
		ret = item
		return false
	})

	if ret == nil || !ret.Origin.Contains(pr.Origin.StartKey) {
		return nil
	}

	return ret
}

// Insert inserts a ProgressRange into the tree, it will return an error if there is a overlapping range.
func (rangeTree *ProgressRangeTree) Insert(pr *ProgressRange) error {
	overlap := rangeTree.find(pr)
	if overlap != nil {
		return errors.Errorf("failed to insert the progress range into range tree, "+
			"because there is a overlapping range. The insert item start key: %s; "+
			"The overlapped item start key: %s, end key: %s.",
			redact.Key(pr.Origin.StartKey), redact.Key(overlap.Origin.StartKey), redact.Key(overlap.Origin.EndKey))
	}
	rangeTree.ReplaceOrInsert(pr)
	return nil
}

// containsRange check if the range contains the region's key range.
func containsRange(start, end, startKey, endKey []byte) bool {
	return bytes.Compare(startKey, start) >= 0 &&
		(len(end) == 0 || (len(endKey) > 0 && bytes.Compare(endKey, end) <= 0))
}

// FindContained finds if there is a progress range containing the key range [startKey, endKey).
func (rangeTree *ProgressRangeTree) FindContained(startKey, endKey []byte) ([]*ProgressRange, error) {
	ret := make([]*ProgressRange, 0)
	startPr := &ProgressRange{
		Origin: Range{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}
	pivot := rangeTree.find(startPr)
	if pivot == nil || bytes.Compare(pivot.Origin.EndKey, startKey) <= 0 {
		return nil, errors.Errorf("The given start key is not contained in any progress range. "+
			"The given start key is %s.", startKey)
	}
	rangeTree.AscendGreaterOrEqual(pivot, func(item *ProgressRange) bool {
		if bytes.Compare(item.Origin.StartKey, endKey) >= 0 {
			return false
		}
		ret = append(ret, item)
		return true
	})

	if len(ret) == 0 {
		return nil, errors.Errorf("Cannot find progress range that contains the start key: %s", redact.Key(startKey))
	}

	if !containsRange(ret[0].Origin.StartKey, ret[len(ret)-1].Origin.EndKey, startKey, endKey) {
		return nil, errors.Errorf("The given region is not contained in the found progress range. "+
			"The region start key is %s; The progress range start key is %s, end key is %s.",
			startKey, redact.Key(ret[0].Origin.StartKey), redact.Key(ret[len(ret)-1].Origin.EndKey))
	}

	return ret, nil
}

type GroupRange struct {
	Ranges []Range
}

func (rangeTree *ProgressRangeTree) GetIncompleteRanges() []*backuppb.SubRanges {
	// about 64 MB memory if there are 1 million ranges
	incompleteGroupRanges := make([]*backuppb.SubRanges, 0)
	rightContinuous := false
	rangeTree.Ascend(func(item *ProgressRange) bool {
		incomplete := item.Res.GetIncompleteRange(item.Origin.StartKey, item.Origin.EndKey)
		if len(incomplete) == 0 {
			rightContinuous = false
			return true
		}
		// now the length of incomplete must be larger than zero
		if rightContinuous && bytes.Equal(incomplete[0].StartKey, item.Origin.StartKey) {
			incompleteGroupRanges[len(incompleteGroupRanges)-1].SubRanges = append(incompleteGroupRanges[len(incompleteGroupRanges)-1].SubRanges, incomplete[0].ToKeyRange())
		} else {
			incompleteGroupRanges = append(incompleteGroupRanges, &backuppb.SubRanges{SubRanges: []*kvrpcpb.KeyRange{incomplete[0].ToKeyRange()}})
		}
		for i := 1; i < len(incomplete); i += 1 {
			incompleteGroupRanges = append(incompleteGroupRanges, &backuppb.SubRanges{SubRanges: []*kvrpcpb.KeyRange{incomplete[i].ToKeyRange()}})
		}
		rightContinuous = bytes.Equal(incomplete[len(incomplete)-1].EndKey, item.Origin.EndKey)
		return true
	})
	return incompleteGroupRanges
}
