// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"bytes"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// KeyRange represents a origin key range.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// Contains check if the range contains the given key, [start, end).
func (rg *KeyRange) Contains(key []byte) bool {
	start, end := rg.StartKey, rg.EndKey
	return bytes.Compare(key, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

// ContainsRange check if the range contains the region's key range.
func (rg *KeyRange) ContainsRange(startKey, endKey []byte) bool {
	start, end := rg.StartKey, rg.EndKey
	return bytes.Compare(startKey, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(endKey, end) <= 0)
}

// Intersect returns intersect range in the tree.
func (rg *KeyRange) Intersect(
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

// Range represents a backup response.
type Range struct {
	KeyRange
	Files []*backuppb.File
}

// BytesAndKeys returns total bytes and keys in a range.
func (rg *Range) BytesAndKeys() (bytes, keys uint64) {
	for _, f := range rg.Files {
		bytes += f.TotalBytes
		keys += f.TotalKvs
	}
	return
}

// Less impls btree.Item.
func (rg *Range) Less(ta *Range) bool {
	// rg.StartKey < than.StartKey
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
		BTreeG: btree.NewG(32, (*RangeStats).Less),
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

	parseInnerKey := func(key []byte) (int64, int64, bool, error) {
		// Trim the keyspace prefix.
		_, innerKey, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
		if err != nil {
			// Not a V2 (keyspaced) key.
			return tablecodec.DecodeKeyHead(key)
		}
		return tablecodec.DecodeKeyHead(innerKey)
	}

	tableID1, indexID1, isRecord1, err1 := parseInnerKey(left.StartKey)
	tableID2, indexID2, isRecord2, err2 := parseInnerKey(right.StartKey)

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

// RangeTree is sorted tree for Ranges.
// All the ranges it stored do not overlap.
type RangeTree struct {
	*btree.BTreeG[*Range]

	PhysicalID int64
}

// NewRangeTree returns an empty range tree.
func NewRangeTree() RangeTree {
	return RangeTree{
		BTreeG: btree.NewG(32, (*Range).Less),
	}
}

// NewRangeTreeWithFreeListG returns an empty range tree with the specified free list
func NewRangeTreeWithFreeListG(physicalID int64, f *btree.FreeListG[*Range]) RangeTree {
	return RangeTree{
		BTreeG:     btree.NewWithFreeListG(32, (*Range).Less, f),
		PhysicalID: physicalID,
	}
}

// Find is a helper function to find an item that contains the range start
// key.
func (rangeTree *RangeTree) Find(rg *Range) *Range {
	var ret *Range
	rangeTree.DescendLessOrEqual(rg, func(i *Range) bool {
		ret = i
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
	rangeTree.AscendGreaterOrEqual(found, func(over *Range) bool {
		if len(rg.EndKey) > 0 && bytes.Compare(rg.EndKey, over.StartKey) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})
	return overlaps
}

// Update inserts range into tree and delete overlapping ranges.
func (rangeTree *RangeTree) Update(rg Range) bool {
	return rangeTree.updateForce(&rg, true)
}

func (rangeTree *RangeTree) updateForce(rg *Range, force bool) bool {
	overlaps := rangeTree.getOverlaps(rg)
	if !force && len(overlaps) > 0 {
		return false
	}
	// Range has backuped, overwrite overlapping range.
	for _, item := range overlaps {
		log.Info("delete overlapping range",
			logutil.Key("startKey", item.StartKey),
			logutil.Key("endKey", item.EndKey))
		rangeTree.Delete(item)
	}
	rangeTree.ReplaceOrInsert(rg)
	return true
}

// Put forms a range and inserts it into tree.
func (rangeTree *RangeTree) Put(
	startKey, endKey []byte, files []*backuppb.File,
) {
	rg := Range{
		KeyRange: KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		},
		Files: files,
	}
	rangeTree.updateForce(&rg, true)
}

// PutForce forms a range and inserts it into tree without force.
func (rangeTree *RangeTree) PutForce(
	startKey, endKey []byte, files []*backuppb.File, force bool,
) bool {
	rg := Range{
		KeyRange: KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		},
		Files: files,
	}

	return rangeTree.updateForce(&rg, force)
}

// InsertRange inserts ranges into the range tree.
// It returns a non-nil range if there are soe overlapped ranges.
func (rangeTree *RangeTree) InsertRange(rg Range) *Range {
	out, _ := rangeTree.ReplaceOrInsert(&rg)
	if out == nil {
		return nil
	}
	return out
}

// GetIncompleteRange returns missing range covered by startKey and endKey.
func (rangeTree *RangeTree) GetIncompleteRange(
	startKey, endKey []byte,
) []*kvrpcpb.KeyRange {
	if len(startKey) != 0 && bytes.Equal(startKey, endKey) {
		return []*kvrpcpb.KeyRange{}
	}
	// Don't use a large buffer, because it will cause memory issue.
	// And the number of missing ranges is usually small.
	incomplete := make([]*kvrpcpb.KeyRange, 0, 1)
	requestRange := KeyRange{StartKey: startKey, EndKey: endKey}
	lastEndKey := startKey
	pviot := &Range{KeyRange: KeyRange{StartKey: startKey}}
	if first := rangeTree.Find(pviot); first != nil {
		pviot.StartKey = first.StartKey
	}
	pviotNotFound := true
	rangeTree.AscendGreaterOrEqual(pviot, func(rg *Range) bool {
		pviotNotFound = false
		if bytes.Compare(lastEndKey, rg.StartKey) < 0 {
			start, end, isIntersect :=
				requestRange.Intersect(lastEndKey, rg.StartKey)
			if isIntersect {
				// There is a gap between the last item and the current item.
				incomplete =
					append(incomplete, &kvrpcpb.KeyRange{StartKey: start, EndKey: end})
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
				append(incomplete, &kvrpcpb.KeyRange{StartKey: start, EndKey: end})
		}
	}
	return incomplete
}

type ProgressRange struct {
	Res    RangeTree
	Origin KeyRange
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

	checksumMap      map[int64]*metautil.ChecksumStats
	skipChecksum     bool
	metaWriter       *metautil.MetaWriter
	completeCallBack func()
}

// NewProgressRangeTree returns an empty range tree.
func NewProgressRangeTree(metaWriter *metautil.MetaWriter, skipChecksum bool) ProgressRangeTree {
	return ProgressRangeTree{
		BTreeG: btree.NewG(32, (*ProgressRange).Less),

		checksumMap:      make(map[int64]*metautil.ChecksumStats),
		skipChecksum:     skipChecksum,
		metaWriter:       metaWriter,
		completeCallBack: func() {},
	}
}

// SetCallBack set the complete call back to update the progress.
func (rangeTree *ProgressRangeTree) SetCallBack(callback func()) {
	rangeTree.completeCallBack = callback
}

// GetChecksumMap get the checksum map from physical ID to checksum.
func (rangeTree *ProgressRangeTree) GetChecksumMap() map[int64]*metautil.ChecksumStats {
	return rangeTree.checksumMap
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

// FindContained finds if there is a progress range containing the key range [startKey, endKey).
func (rangeTree *ProgressRangeTree) FindContained(startKey, endKey []byte) (*ProgressRange, error) {
	startPr := &ProgressRange{
		Origin: KeyRange{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}
	ret := rangeTree.find(startPr)

	if ret == nil {
		log.Warn("Cannot find progress range that contains the start key, maybe the duplicated response",
			zap.String("key", redact.Key(startKey)))
		return nil, nil
	}

	if !ret.Origin.ContainsRange(startKey, endKey) {
		return nil, errors.Errorf("The given region is not contained in the found progress range. "+
			"The region start key is %s; The progress range start key is %s, end key is %s.",
			startKey, redact.Key(ret.Origin.StartKey), redact.Key(ret.Origin.EndKey))
	}

	return ret, nil
}

type DeletedRange struct {
	rg       *ProgressRange
	checksum metautil.ChecksumStats
}

func (rangeTree *ProgressRangeTree) GetIncompleteRanges() ([]*kvrpcpb.KeyRange, error) {
	var (
		// about 64 MB memory if there are 1 million ranges
		incompleteRanges = make([]*kvrpcpb.KeyRange, 0, rangeTree.Len())
		deletedRanges    = make([]DeletedRange, 0)
		rangeAscendErr   error
	)
	rangeTree.Ascend(func(item *ProgressRange) bool {
		// NOTE: maybe there is a late response whose range overlaps with an existing item, which
		// may cause the complete range tree to become incomplete. Therefore, `item.Complete` is
		// only for statistic.
		incomplete := item.Res.GetIncompleteRange(item.Origin.StartKey, item.Origin.EndKey)
		if len(incomplete) == 0 {
			var checksum metautil.ChecksumStats
			checksum, rangeAscendErr = rangeTree.collectRangeFiles(item)
			if rangeAscendErr != nil {
				return false
			}
			deletedRanges = append(deletedRanges, DeletedRange{
				rg:       item,
				checksum: checksum,
			})
			rangeTree.completeCallBack()
			return true
		}
		incompleteRanges = append(incompleteRanges, incomplete...)
		return true
	})
	if rangeAscendErr != nil {
		return nil, errors.Trace(rangeAscendErr)
	}
	for _, deletedRange := range deletedRanges {
		rangeTree.Delete(deletedRange.rg)
		if !rangeTree.skipChecksum {
			rangeTree.UpdateChecksum(
				deletedRange.rg.Res.PhysicalID,
				deletedRange.checksum.Crc64Xor,
				deletedRange.checksum.TotalKvs,
				deletedRange.checksum.TotalBytes,
			)
		}
	}
	return incompleteRanges, nil
}

func (rangeTree *ProgressRangeTree) collectRangeFiles(item *ProgressRange) (metautil.ChecksumStats, error) {
	var checksum metautil.ChecksumStats
	if rangeTree.metaWriter == nil {
		return checksum, nil
	}
	var rangeAscendErr error
	item.Res.Ascend(func(r *Range) bool {
		crc, kvs, bytes := utils.SummaryFiles(r.Files)
		if err := rangeTree.metaWriter.Send(r.Files, metautil.AppendDataFile); err != nil {
			rangeAscendErr = err
			return false
		}
		checksum.Crc64Xor ^= crc
		checksum.TotalKvs += kvs
		checksum.TotalBytes += bytes
		return true
	})
	return checksum, rangeAscendErr
}

func (rangeTree *ProgressRangeTree) UpdateChecksum(physicalID int64, crc, kvs, bytes uint64) {
	ckm, ok := rangeTree.checksumMap[physicalID]
	if !ok {
		ckm = &metautil.ChecksumStats{}
		rangeTree.checksumMap[physicalID] = ckm
	}
	ckm.Crc64Xor ^= crc
	ckm.TotalKvs += kvs
	ckm.TotalBytes += bytes
}
