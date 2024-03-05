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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/kv"
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

// RefAt returns the reference at the ith position without copy.
func (r *KeyRanges) RefAt(i int) *kv.KeyRange {
	if r.first != nil {
		if i == 0 {
			return r.first
		}
		i--
	}
	if i < len(r.mid) {
		return &r.mid[i]
	}
	return r.last
}

// At returns the range at the ith position.
func (r *KeyRanges) At(i int) kv.KeyRange {
	return *r.RefAt(i)
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
	for i := range r.mid {
		f(&r.mid[i])
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

// ToRanges converts ranges to []kv.KeyRange.
func (r *KeyRanges) ToRanges() []kv.KeyRange {
	ranges := make([]kv.KeyRange, 0, r.Len())
	r.Do(func(ran *kv.KeyRange) {
		ranges = append(ranges, *ran)
	})
	return ranges
}

// ToPBRanges converts ranges to wire type.
func (r *KeyRanges) ToPBRanges() []*coprocessor.KeyRange {
	ranges := make([]*coprocessor.KeyRange, 0, r.Len())
	r.Do(func(ran *kv.KeyRange) {
		// kv.KeyRange and coprocessor.KeyRange are the same,
		// so use unsafe.Pointer to avoid allocation here.
		ranges = append(ranges, (*coprocessor.KeyRange)(unsafe.Pointer(ran)))
	})
	return ranges
}
