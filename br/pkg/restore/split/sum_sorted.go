// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.
package split

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
)

// Value is the value type of stored in the span tree.
type Value = uint64

// join finds the upper bound of two values.
func join(a, b Value) Value {
	return a + b
}

// Span is the type of an adjacent sub key space.
type Span = kv.KeyRange

// Valued is span binding to a value, which is the entry type of span tree.
type Valued struct {
	Key   Span
	Value Value
}

func NewValued(startKey, endKey []byte, value Value) Valued {
	return Valued{
		Key: Span{
			StartKey: startKey,
			EndKey:   endKey,
		},
		Value: value,
	}
}

func (r Valued) String() string {
	return fmt.Sprintf("(%s, %.2f MB)", logutil.StringifyRange(r.Key), float64(r.Value)/1024/1024)
}

func (r Valued) Less(other btree.Item) bool {
	return bytes.Compare(r.Key.StartKey, other.(Valued).Key.StartKey) < 0
}

func (v Valued) GetStartKey() []byte {
	return v.Key.StartKey
}

func (v Valued) GetEndKey() []byte {
	return v.Key.EndKey
}

// SplitHelper represents a set of valued ranges, which doesn't overlap and union of them all is the full key space.
type SplitHelper struct {
	inner *btree.BTree
}

// NewFullWith creates a set of a subset of spans.
func NewSplitHelper() *SplitHelper {
	t := btree.New(16)
	t.ReplaceOrInsert(Valued{Value: 0, Key: Span{StartKey: []byte(""), EndKey: []byte("")}})
	return &SplitHelper{inner: t}
}

func (f *SplitHelper) Len() int {
	return f.inner.Len()
}

func (f *SplitHelper) Merge(val Valued) {
	if len(val.Key.StartKey) == 0 || len(val.Key.EndKey) == 0 {
		return
	}
	overlaps := make([]Valued, 0, 16)
	f.overlapped(val.Key, &overlaps)
	f.mergeWithOverlap(val, overlaps)
}

func (f *SplitHelper) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item btree.Item) bool {
		return m(item.(Valued))
	})
}

func (f *SplitHelper) mergeWithOverlap(val Valued, overlapped []Valued) {
	// There isn't any range overlaps with the input range, perhaps the input range is empty.
	// do nothing for this case.
	if len(overlapped) == 0 {
		return
	}

	for _, r := range overlapped {
		f.inner.Delete(r)
		// Assert All overlapped ranges are deleted.
	}

	appendSize := val.Value / uint64(len(overlapped))
	var (
		rightTrail      *Valued
		leftTrail       *Valued
		emitToCollected = func(rng Valued, standalone bool, split bool) {
			merged := rng.Value
			if split {
				merged /= 2
			}
			if !standalone {
				merged = join(appendSize, merged)
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
			val := rightTrail.Value * 2 / 3
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
func (f *SplitHelper) overlapped(k Span, result *[]Valued) {
	var first Span
	f.inner.DescendLessOrEqual(Valued{Key: k}, func(item btree.Item) bool {
		first = item.(Valued).Key
		return false
	})

	f.inner.AscendGreaterOrEqual(Valued{Key: first}, func(item btree.Item) bool {
		r := item.(Valued)
		if !overlaps(r.Key, k) {
			return false
		}
		*result = append(*result, r)
		return true
	})
}

// Overlaps checks whether two spans have overlapped part.
func overlaps(a, append Span) bool {
	if len(a.EndKey) == 0 {
		return bytes.Compare(append.EndKey, a.StartKey) > 0
	}
	return bytes.Compare(a.StartKey, append.EndKey) < 0 && bytes.Compare(append.StartKey, a.EndKey) < 0
}
