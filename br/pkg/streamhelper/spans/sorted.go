// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
)

// Value is the value type of stored in the span tree.
type Value = uint64

// join finds the upper bound of two values.
func join(a, b Value) Value {
	if a > b {
		return a
	}
	return b
}

// Span is the type of an adjacent sub key space.
type Span = kv.KeyRange

// Valued is span binding to a value, which is the entry type of span tree.
type Valued struct {
	Key   Span
	Value Value
}

func (r Valued) String() string {
	return fmt.Sprintf("(%s, %d)", logutil.StringifyRange(r.Key), r.Value)
}

func (r Valued) Less(other btree.Item) bool {
	return bytes.Compare(r.Key.StartKey, other.(Valued).Key.StartKey) < 0
}

// ValuedFull represents a set of valued ranges, which doesn't overlap and union of them all is the full key space.
type ValuedFull struct {
	inner *btree.BTree
}

// NewFullWith creates a set of a subset of spans.
func NewFullWith(initSpans []Span, init Value) *ValuedFull {
	t := btree.New(16)
	for _, r := range Collapse(len(initSpans), func(i int) Span { return initSpans[i] }) {
		t.ReplaceOrInsert(Valued{Value: init, Key: r})
	}
	return &ValuedFull{inner: t}
}

// Merge merges a new interval into the span set. The value of overlapped
// part with other spans would be "merged" by the `join` function.
// An example:
/*
|___________________________________________________________________________|
^-----------------^-----------------^-----------------^---------------------^
|      c = 42     |      c = 43     |     c = 45      |      c = 41         |
                       ^--------------------------^
                 merge(|          c = 44          |)
Would Give:
|___________________________________________________________________________|
^-----------------^----^------------^-------------^---^---------------------^
|      c = 42     | 43 |   c = 44   |     c = 45      |      c = 41         |
                                    |-------------|
                                    Unchanged, because 44 < 45.
*/
func (f *ValuedFull) Merge(val Valued) {
	overlaps := make([]Valued, 0, 16)
	f.overlapped(val.Key, &overlaps)
	f.mergeWithOverlap(val, overlaps, nil)
}

// Traverse traverses all ranges by order.
func (f *ValuedFull) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item btree.Item) bool {
		return m(item.(Valued))
	})
}

func (f *ValuedFull) mergeWithOverlap(val Valued, overlapped []Valued, newItems *[]Valued) {
	// There isn't any range overlaps with the input range, perhaps the input range is empty.
	// do nothing for this case.
	if len(overlapped) == 0 {
		return
	}

	for _, r := range overlapped {
		f.inner.Delete(r)
		// Assert All overlapped ranges are deleted.
	}

	var (
		initialized    = false
		collected      Valued
		rightTrail     *Valued
		flushCollected = func() {
			if initialized {
				f.inner.ReplaceOrInsert(collected)
				if newItems != nil {
					*newItems = append(*newItems, collected)
				}
			}
		}
		emitToCollected = func(rng Valued, standalone bool) {
			merged := rng.Value
			if !standalone {
				merged = join(val.Value, rng.Value)
			}
			if !initialized {
				collected = rng
				collected.Value = merged
				initialized = true
				return
			}
			if merged == collected.Value && utils.CompareBytesExt(collected.Key.EndKey, true, rng.Key.StartKey, false) == 0 {
				collected.Key.EndKey = rng.Key.EndKey
			} else {
				flushCollected()
				collected = Valued{
					Key:   rng.Key,
					Value: merged,
				}
			}
		}
	)

	leftmost := overlapped[0]
	if bytes.Compare(leftmost.Key.StartKey, val.Key.StartKey) < 0 {
		emitToCollected(Valued{
			Key:   Span{StartKey: leftmost.Key.StartKey, EndKey: val.Key.StartKey},
			Value: leftmost.Value,
		}, true)
		overlapped[0].Key.StartKey = val.Key.StartKey
	}

	rightmost := overlapped[len(overlapped)-1]
	if utils.CompareBytesExt(rightmost.Key.EndKey, true, val.Key.EndKey, true) > 0 {
		rightTrail = &Valued{
			Key:   Span{StartKey: val.Key.EndKey, EndKey: rightmost.Key.EndKey},
			Value: rightmost.Value,
		}
		overlapped[len(overlapped)-1].Key.EndKey = val.Key.EndKey
	}

	for _, rng := range overlapped {
		emitToCollected(rng, false)
	}

	if rightTrail != nil {
		emitToCollected(*rightTrail, true)
	}

	flushCollected()
}

// overlapped inserts the overlapped ranges of the span into the `result` slice.
func (f *ValuedFull) overlapped(k Span, result *[]Valued) {
	var (
		first    Span
		hasFirst bool
	)
	// Firstly, let's find whether there is a overlapped region with less start key.
	f.inner.DescendLessOrEqual(Valued{Key: k}, func(item btree.Item) bool {
		first = item.(Valued).Key
		hasFirst = true
		return false
	})
	if !hasFirst || !Overlaps(first, k) {
		first = k
	}

	f.inner.AscendGreaterOrEqual(Valued{Key: first}, func(item btree.Item) bool {
		r := item.(Valued)
		if !Overlaps(r.Key, k) {
			return false
		}
		*result = append(*result, r)
		return true
	})
}
