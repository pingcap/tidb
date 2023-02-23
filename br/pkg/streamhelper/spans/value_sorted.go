// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans

import "github.com/google/btree"

type sortedByValueThenStartKey Valued

func (s sortedByValueThenStartKey) Less(o btree.Item) bool {
	other := o.(sortedByValueThenStartKey)
	if s.Value != other.Value {
		return s.Value < other.Value
	}
	return Valued(s).Less(Valued(other))
}

// ValueSortedFull is almost the same as `Valued`, however it added an
// extra index hence enabled query range by theirs value.
type ValueSortedFull struct {
	*ValuedFull
	valueIdx *btree.BTree
}

// Sorted takes the ownership of a raw `ValuedFull` and then wrap it with `ValueSorted`.
func Sorted(f *ValuedFull) *ValueSortedFull {
	vf := &ValueSortedFull{
		ValuedFull: f,
		valueIdx:   btree.New(16),
	}
	f.Traverse(func(v Valued) bool {
		vf.valueIdx.ReplaceOrInsert(sortedByValueThenStartKey(v))
		return true
	})
	return vf
}

func (v *ValueSortedFull) Merge(newItem Valued) {
	v.MergeAll([]Valued{newItem})
}

func (v *ValueSortedFull) MergeAll(newItems []Valued) {
	var overlapped []Valued
	var inserted []Valued

	for _, item := range newItems {
		overlapped = overlapped[:0]
		inserted = inserted[:0]

		v.overlapped(item.Key, &overlapped)
		v.mergeWithOverlap(item, overlapped, &inserted)

		for _, o := range overlapped {
			v.valueIdx.Delete(sortedByValueThenStartKey(o))
		}
		for _, i := range inserted {
			v.valueIdx.ReplaceOrInsert(sortedByValueThenStartKey(i))
		}
	}
}

func (v *ValueSortedFull) TraverseValuesLessThan(n Value, action func(Valued) bool) {
	v.valueIdx.AscendLessThan(sortedByValueThenStartKey{Value: n}, func(item btree.Item) bool {
		return action(Valued(item.(sortedByValueThenStartKey)))
	})
}

func (v *ValueSortedFull) MinValue() Value {
	return v.valueIdx.Min().(sortedByValueThenStartKey).Value
}
