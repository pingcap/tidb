package spans

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/kv"
)

type Value = uint64
type Span = kv.KeyRange
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

	merge func(Value, Value) Value
}

func NewFullWith(init Value, merger func(Value, Value) Value) *ValuedFull {
	t := btree.New(16)
	t.ReplaceOrInsert(Valued{Value: init, Key: Span{StartKey: []byte{}, EndKey: []byte{}}})
	return &ValuedFull{inner: t, merge: merger}
}

func (f *ValuedFull) Merge(val Valued) {
	overlaps := make([]Valued, 0, 16)
	f.getOverlap(val.Key, &overlaps)
	f.mergeWithOverlap(val, overlaps, nil)
}

func (f *ValuedFull) Traverse(m func(Valued) bool) {
	f.inner.Ascend(func(item btree.Item) bool {
		return m(item.(Valued))
	})
}

func (f *ValuedFull) mergeWithOverlap(val Valued, overlapped []Valued, newItems *[]Valued) {
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
				merged = f.merge(val.Value, rng.Value)
			}
			if !initialized {
				collected = rng
				collected.Value = merged
				initialized = true
				return
			}
			if merged == collected.Value {
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
	if !bytes.Equal(leftmost.Key.StartKey, val.Key.StartKey) {
		emitToCollected(Valued{
			Key:   Span{StartKey: leftmost.Key.StartKey, EndKey: val.Key.StartKey},
			Value: leftmost.Value,
		}, true)
		overlapped[0].Key.StartKey = val.Key.StartKey
	}

	rightmost := overlapped[len(overlapped)-1]
	if !bytes.Equal(rightmost.Key.EndKey, val.Key.EndKey) {
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

func (f *ValuedFull) getOverlap(k Span, result *[]Valued) {
	var first Span
	f.inner.DescendLessOrEqual(Valued{Key: k}, func(item btree.Item) bool {
		first = item.(Valued).Key
		return false
	})

	f.inner.AscendGreaterOrEqual(Valued{Key: first}, func(item btree.Item) bool {
		r := item.(Valued)
		if !Overlaps(r.Key, k) {
			return false
		}
		*result = append(*result, r)
		return true
	})
}
