// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/utils"
)

// btreeSplitHelper is the original SplitHelper implementation: a btree of
// fragments in which every merge deletes and re-inserts each overlapped
// fragment. SplitHelper switches a table to it once an inverted span (start key
// after end key) is merged: such a span breaks the partition of the key space
// that SplitHelper's treap relies on, and this keeps the previous behavior.
type btreeSplitHelper struct {
	inner *btree.BTree
}

// newBTreeSplitHelper creates a set of spans covering the full key space with zero value.
func newBTreeSplitHelper() *btreeSplitHelper {
	t := btree.New(16)
	t.ReplaceOrInsert(Valued{Value: Value{Size: 0, Number: 0}, Key: Span{StartKey: []byte(""), EndKey: []byte("")}})
	return &btreeSplitHelper{inner: t}
}

func (f *btreeSplitHelper) Merge(val Valued) {
	if len(val.Key.StartKey) == 0 || len(val.Key.EndKey) == 0 {
		return
	}
	overlaps := make([]Valued, 0, 8)
	f.overlapped(val.Key, &overlaps)
	f.mergeWithOverlap(val, overlaps)
}

// traverse the items in ascend order
func (f *btreeSplitHelper) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item btree.Item) bool {
		return m(item.(Valued))
	})
}

func (f *btreeSplitHelper) mergeWithOverlap(val Valued, overlapped []Valued) {
	// There isn't any range overlaps with the input range, perhaps the input range is empty.
	// do nothing for this case.
	if len(overlapped) == 0 {
		return
	}

	for _, r := range overlapped {
		f.inner.Delete(r)
	}
	// Assert All overlapped ranges are deleted.

	// the new valued item's Value is equally dividedd into `len(overlapped)` shares
	appendValue := Value{
		Size:   val.Value.Size / uint64(len(overlapped)),
		Number: val.Value.Number / int64(len(overlapped)),
	}
	var (
		rightTrail *Valued
		leftTrail  *Valued
		// overlapped ranges   +-------------+----------+
		// new valued item             +-------------+
		//                     a       b     c       d  e
		// the part [a,b] is `standalone` because it is not overlapped with the new valued item
		// the part [a,b] and [b,c] are `split` because they are from range [a,c]
		emitToCollected = func(rng Valued, standalone bool, split bool) {
			merged := rng.Value
			if split {
				merged.Size /= 2
				merged.Number /= 2
			}
			if !standalone {
				merged = join(appendValue, merged)
			}
			rng.Value = merged
			f.inner.ReplaceOrInsert(rng)
		}
	)

	leftmost := overlapped[0]
	if bytes.Compare(leftmost.Key.StartKey, val.Key.StartKey) < 0 {
		leftTrail = &Valued{
			Key:   Span{StartKey: leftmost.Key.StartKey, EndKey: val.Key.StartKey},
			Value: leftmost.Value,
		}
		overlapped[0].Key.StartKey = val.Key.StartKey
	}

	rightmost := overlapped[len(overlapped)-1]
	if utils.CompareBytesExt(rightmost.Key.EndKey, true, val.Key.EndKey, true) > 0 {
		rightTrail = &Valued{
			Key:   Span{StartKey: val.Key.EndKey, EndKey: rightmost.Key.EndKey},
			Value: rightmost.Value,
		}
		overlapped[len(overlapped)-1].Key.EndKey = val.Key.EndKey
		if len(overlapped) == 1 && leftTrail != nil {
			//                      (split)   (split)     (split)
			// overlapped ranges   +-----------------------------+
			// new valued item             +-------------+
			//                     a       b             c       d
			// now the overlapped range should be divided into 3 equal parts
			// so modify the value to the 2/3x to be compatible with function `emitToCollected`
			val := Value{
				Size:   rightTrail.Value.Size * 2 / 3,
				Number: rightTrail.Value.Number * 2 / 3,
			}
			leftTrail.Value = val
			overlapped[0].Value = val
			rightTrail.Value = val
		}
	}

	if leftTrail != nil {
		emitToCollected(*leftTrail, true, true)
	}

	for i, rng := range overlapped {
		split := (i == 0 && leftTrail != nil) || (i == len(overlapped)-1 && rightTrail != nil)
		emitToCollected(rng, false, split)
	}

	if rightTrail != nil {
		emitToCollected(*rightTrail, true, true)
	}
}

// overlapped inserts the overlapped ranges of the span into the `result` slice.
func (f *btreeSplitHelper) overlapped(k Span, result *[]Valued) {
	var first Span
	f.inner.DescendLessOrEqual(Valued{Key: k}, func(item btree.Item) bool {
		first = item.(Valued).Key
		return false
	})

	f.inner.AscendGreaterOrEqual(Valued{Key: first}, func(item btree.Item) bool {
		r := item.(Valued)
		if !checkOverlaps(r.Key, k) {
			return false
		}
		*result = append(*result, r)
		return true
	})
}
