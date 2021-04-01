// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
)

// KeyRanges is like []kv.KeyRange, but may has extra elements at head/tail.
// It's for avoiding alloc big slice during build copTask.
type KeyRanges struct {
	first *kv.KeyRange
	mid   []kv.KeyRange
	last  *kv.KeyRange
}

// NewKeyRanges constructs a KeyRanges instance.
func NewKeyRanges(ranges []kv.KeyRange) *KeyRanges {
	return &KeyRanges{mid: ranges}
}

func (r *KeyRanges) String() string {
	var s string
	r.Do(func(ran *kv.KeyRange) {
		s += fmt.Sprintf("[%q, %q]", ran.StartKey, ran.EndKey)
	})
	return s
}

// Len returns the count of ranges.
func (r *KeyRanges) Len() int {
	var l int
	if r.first != nil {
		l++
	}
	l += len(r.mid)
	if r.last != nil {
		l++
	}
	return l
}

// At returns the range at the ith position.
func (r *KeyRanges) At(i int) kv.KeyRange {
	if r.first != nil {
		if i == 0 {
			return *r.first
		}
		i--
	}
	if i < len(r.mid) {
		return r.mid[i]
	}
	return *r.last
}

// Slice returns the sub ranges [from, to).
func (r *KeyRanges) Slice(from, to int) *KeyRanges {
	var ran KeyRanges
	if r.first != nil {
		if from == 0 && to > 0 {
			ran.first = r.first
		}
		if from > 0 {
			from--
		}
		if to > 0 {
			to--
		}
	}
	if to <= len(r.mid) {
		ran.mid = r.mid[from:to]
	} else {
		if from <= len(r.mid) {
			ran.mid = r.mid[from:]
		}
		if from < to {
			ran.last = r.last
		}
	}
	return &ran
}

// Do applies a functions to all ranges.
func (r *KeyRanges) Do(f func(ran *kv.KeyRange)) {
	if r.first != nil {
		f(r.first)
	}
	for _, ran := range r.mid {
		f(&ran)
	}
	if r.last != nil {
		f(r.last)
	}
}

// Split ranges into (left, right) by key.
func (r *KeyRanges) Split(key []byte) (*KeyRanges, *KeyRanges) {
	n := sort.Search(r.Len(), func(i int) bool {
		cur := r.At(i)
		return len(cur.EndKey) == 0 || bytes.Compare(cur.EndKey, key) > 0
	})
	// If a range p contains the key, it will split to 2 parts.
	if n < r.Len() {
		p := r.At(n)
		if bytes.Compare(key, p.StartKey) > 0 {
			left := r.Slice(0, n)
			left.last = &kv.KeyRange{StartKey: p.StartKey, EndKey: key}
			right := r.Slice(n+1, r.Len())
			right.first = &kv.KeyRange{StartKey: key, EndKey: p.EndKey}
			return left, right
		}
	}
	return r.Slice(0, n), r.Slice(n, r.Len())
}

// ToPBRanges converts ranges to wire type.
func (r *KeyRanges) ToPBRanges() []*coprocessor.KeyRange {
	ranges := make([]*coprocessor.KeyRange, 0, r.Len())
	r.Do(func(ran *kv.KeyRange) {
		ranges = append(ranges, &coprocessor.KeyRange{
			Start: ran.StartKey,
			End:   ran.EndKey,
		})
	})
	return ranges
}

// SplitRegionRanges get the split ranges from pd region.
func SplitRegionRanges(bo *Backoffer, cache *RegionCache, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	ranges := NewKeyRanges(keyRanges)

	var ret []kv.KeyRange
	appendRange := func(regionWithRangeInfo *KeyLocation, ranges *KeyRanges) {
		for i := 0; i < ranges.Len(); i++ {
			ret = append(ret, ranges.At(i))
		}
	}

	err := SplitKeyRanges(bo, cache, ranges, appendRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

// SplitKeyRanges splits KeyRanges by the regions info from cache.
func SplitKeyRanges(bo *Backoffer, cache *RegionCache, ranges *KeyRanges, fn func(regionWithRangeInfo *KeyLocation, ranges *KeyRanges)) error {
	for ranges.Len() > 0 {
		loc, err := cache.LocateKey(bo, ranges.At(0).StartKey)
		if err != nil {
			return errors.Trace(err)
		}

		// Iterate to the first range that is not complete in the region.
		var i int
		for ; i < ranges.Len(); i++ {
			r := ranges.At(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.Len() {
			fn(loc, ranges)
			break
		}

		r := ranges.At(i)
		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.Slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			fn(loc, taskRanges)

			ranges = ranges.Slice(i+1, ranges.Len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.Slice(0, i)
			fn(loc, taskRanges)
			ranges = ranges.Slice(i, ranges.Len())
		}
	}

	return nil
}
