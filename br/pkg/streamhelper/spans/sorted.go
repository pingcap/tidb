// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"

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

// valuedItem stores the first 16 key bytes in the node. The record remains
// shared with exact, so advancing a checkpoint does not rewrite the tree.
type valuedItem struct {
	hi, lo uint64
	record *Valued
}

func makeValuedItem(record *Valued) valuedItem {
	key := record.Key.StartKey
	if len(key) >= 16 {
		return valuedItem{binary.BigEndian.Uint64(key[:8]), binary.BigEndian.Uint64(key[8:16]), record}
	}
	var padded [16]byte
	copy(padded[:], key)
	return valuedItem{binary.BigEndian.Uint64(padded[:8]), binary.BigEndian.Uint64(padded[8:]), record}
}

func lessValuedItem(a, b valuedItem) bool {
	if a.hi != b.hi {
		return a.hi < b.hi
	}
	if a.lo != b.lo {
		return a.lo < b.lo
	}
	ak, bk := a.record.Key.StartKey, b.record.Key.StartKey
	if len(ak) >= 16 && len(bk) >= 16 {
		ak, bk = ak[16:], bk[16:]
	}
	return bytes.Compare(ak, bk) < 0
}

// Do not retain a large temporary array after a wide range update.
const maxOverlapScratch = 1024

// ValuedFull represents a set of valued ranges, which doesn't overlap and union of them all is the full key space.
// Retained keys must remain immutable; operations require external synchronization.
type ValuedFull struct {
	inner *btree.BTreeG[valuedItem]
	// exact and inner share records so same-boundary updates need no tree write.
	// Fingerprints are only hints: both boundaries must match before updating.
	exact map[uint64]*Valued
	seed  maphash.Seed
	// Only fallback merges use these buffers; none are retained per region.
	lookup         Valued
	overlapScratch []Valued
}

// NewFullWith creates a set of a subset of spans.
func NewFullWith(initSpans []Span, init Value) *ValuedFull {
	t := btree.NewG[valuedItem](64, lessValuedItem)
	f := &ValuedFull{inner: t, exact: make(map[uint64]*Valued, len(initSpans)), seed: maphash.MakeSeed()}
	for _, r := range Collapse(len(initSpans), func(i int) Span { return initSpans[i] }) {
		f.insert(Valued{Value: init, Key: r})
	}
	return f
}

func (f *ValuedFull) insert(val Valued) {
	f.inner.ReplaceOrInsert(makeValuedItem(&val))
	f.exact[maphash.Bytes(f.seed, val.Key.StartKey)] = &val
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
	if old := f.exact[maphash.Bytes(f.seed, val.Key.StartKey)]; old != nil &&
		bytes.Equal(old.Key.StartKey, val.Key.StartKey) && bytes.Equal(old.Key.EndKey, val.Key.EndKey) {
		old.Value = join(old.Value, val.Value)
		return
	}
	overlaps := f.overlapScratch[:0]
	defer func() {
		clear(overlaps)
		if cap(overlaps) <= maxOverlapScratch {
			f.overlapScratch = overlaps[:0]
		} else {
			f.overlapScratch = nil
		}
	}()
	f.overlapped(val.Key, &overlaps)
	// Old notifications must not rebuild ranges when no checkpoint advances.
	for _, old := range overlaps {
		if old.Value < val.Value {
			f.mergeWithOverlap(val, overlaps)
			return
		}
	}
}

// Traverse traverses all ranges by order.
func (f *ValuedFull) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item valuedItem) bool { return m(*item.record) })
}

// TraverseRef visits records without copying their key ranges. The callback
// must not mutate the records or tree. Borrowed records must not outlive the
// caller's external lock, which must be held throughout the traversal.
func (f *ValuedFull) TraverseRef(m func(*Valued) bool) {
	f.inner.Ascend(func(item valuedItem) bool { return m(item.record) })
}

func (f *ValuedFull) mergeWithOverlap(val Valued, overlapped []Valued) {
	for _, r := range overlapped {
		f.lookup.Key.StartKey = r.Key.StartKey
		f.inner.Delete(makeValuedItem(&f.lookup))
		delete(f.exact, maphash.Bytes(f.seed, r.Key.StartKey))
		// Assert All overlapped ranges are deleted.
	}
	f.lookup.Key.StartKey = nil

	var (
		initialized    = false
		collected      Valued
		rightTrail     *Valued
		flushCollected = func() {
			if initialized {
				f.insert(collected)
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
	f.lookup.Key.StartKey = k.StartKey
	f.inner.DescendLessOrEqual(makeValuedItem(&f.lookup), func(item valuedItem) bool {
		first = item.record.Key
		hasFirst = true
		return false
	})
	if !hasFirst || !Overlaps(first, k) {
		first = k
	}

	f.lookup.Key.StartKey = first.StartKey
	f.inner.AscendGreaterOrEqual(makeValuedItem(&f.lookup), func(item valuedItem) bool {
		r := item.record
		if !Overlaps(r.Key, k) {
			return false
		}
		*result = append(*result, *r)
		return true
	})
	f.lookup.Key.StartKey = nil
}
