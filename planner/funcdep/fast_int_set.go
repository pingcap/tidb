// Copyright 2022 PingCAP, Inc.
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

package funcdep

import (
	"bytes"
	"fmt"
	"math/bits"

	"golang.org/x/tools/container/intsets"
)

const smallCutOff = 64

// FastIntSet is wrapper of sparse with an optimization that number [0 ~ 64) can be cached for quick access.
// From the benchmark in fd_graph_test.go, we choose to use sparse to accelerate int set. And when the set
// size is quite small we can just skip the block allocation in the sparse chain list.
type FastIntSet struct {
	// an uint64 used like quick-small bitmap of 0~63.
	small uint64
	// when some is bigger than 64, then all that previous inserted will be dumped into sparse.
	large *intsets.Sparse
}

// NewFastIntSet is used to make the instance of FastIntSet with initial values.
func NewFastIntSet(values ...int) FastIntSet {
	var res FastIntSet
	for _, v := range values {
		res.Insert(v)
	}
	return res
}

// Len return the size of the int set.
func (s FastIntSet) Len() int {
	if s.large == nil {
		return bits.OnesCount64(s.small)
	}
	return s.large.Len()
}

// Only1Zero is a usage function for convenience judgement.
func (s FastIntSet) Only1Zero() bool {
	return s.Len() == 1 && s.Has(0)
}

// Insert is used to insert a value into int-set.
func (s *FastIntSet) Insert(i int) {
	isSmall := i >= 0 && i < smallCutOff
	if isSmall {
		s.small |= 1 << uint64(i)
	}
	if !isSmall && s.large == nil {
		// first encounter a larger/smaller number, dump all that in `small` into `large`.
		s.large = s.toLarge()
	}
	if s.large != nil {
		s.large.Insert(i)
	}
}

func (s FastIntSet) toLarge() *intsets.Sparse {
	if s.large != nil {
		return s.large
	}
	large := new(intsets.Sparse)
	for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
		large.Insert(i)
	}
	return large
}

// Next returns the next existing number in the Set. If there's no larger one than the given start val, return (MaxInt, false).
func (s FastIntSet) Next(startVal int) (int, bool) {
	if startVal < smallCutOff {
		if startVal < 0 {
			startVal = 0
		}
		// x=0, gap=64, which means there is no `1` after right shift.
		if gap := bits.TrailingZeros64(s.small >> uint64(startVal)); gap < 64 {
			return gap + startVal, true
		}
	}
	if s.large != nil {
		res := s.large.LowerBound(startVal)
		return res, res != intsets.MaxInt
	}
	return intsets.MaxInt, false
}

// Remove is used to remove a value from the set. Nothing done if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	if i >= 0 && i < smallCutOff {
		s.small &^= 1 << uint64(i)
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

// Clear is used to clear a fastIntSet and reuse it as an empty one.
func (s *FastIntSet) Clear() {
	s.small = 0
	if s.large != nil {
		s.large.Clear()
	}
}

// Has is used ot judge whether a value is in the set.
func (s FastIntSet) Has(i int) bool {
	if i >= 0 && i < smallCutOff {
		return (s.small & (1 << uint64(i))) != 0
	}
	if s.large != nil {
		return s.large.Has(i)
	}
	return false
}

// IsEmpty is used to judge whether the int-set is empty.
func (s FastIntSet) IsEmpty() bool {
	return s.small == 0 && (s.large == nil || s.large.IsEmpty())
}

// SortedArray is used to return the in array of the set.
func (s FastIntSet) SortedArray() []int {
	if s.IsEmpty() {
		return nil
	}
	if s.large != nil {
		return s.large.AppendTo([]int(nil))
	}
	res := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		res = append(res, i)
	})
	return res
}

// ForEach call a function for each value in the int-set. (Ascend)
func (s FastIntSet) ForEach(f func(i int)) {
	if s.large != nil {
		for x := s.large.Min(); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
		return
	}
	for v := s.small; v != 0; {
		// from the left to right.
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
}

// Copy returns a copy of s.
func (s FastIntSet) Copy() FastIntSet {
	c := FastIntSet{}
	c.small = s.small
	if s.large != nil {
		c.large = new(intsets.Sparse)
		c.large.Copy(s.large)
	}
	return c
}

// CopyFrom clear the receiver to be a copy of the param.
func (s *FastIntSet) CopyFrom(target FastIntSet) {
	s.small = target.small
	if target.large != nil {
		if s.large == nil {
			s.large = new(intsets.Sparse)
		}
		s.large.Copy(target.large)
	} else {
		if s.large != nil {
			s.large.Clear()
		}
	}
}

// Equals returns whether two int-set are identical.
func (s FastIntSet) Equals(rhs FastIntSet) bool {
	if s.large == nil && rhs.large == nil {
		return s.small == rhs.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.Equals(rhs.large)
	}
	// how come to this? eg: a set operates like: {insert:1, insert:65, remove:65}, resulting a large int-set with only small numbers.
	// so we need calculate the exact numbers.
	var excess bool
	s1 := s.small
	s2 := rhs.small
	if s.large != nil {
		s1, excess = s.largeToSmall()
	} else {
		s2, excess = rhs.largeToSmall()
	}
	return !excess && s1 == s2
}

func (s FastIntSet) largeToSmall() (small uint64, otherValues bool) {
	if s.large == nil {
		panic("set contains no large")
	}
	return s.small, s.large.Min() < 0 || s.large.Max() >= smallCutOff
}

// *************************************************************************
// *                            Logic Operators                            *
// *************************************************************************

// Difference is used to return the s without elements in rhs.
func (s FastIntSet) Difference(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// DifferenceWith removes any elements in rhs from source.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	s.small &^= rhs.small
	if s.large == nil {
		return
	}
	s.large.DifferenceWith(rhs.toLarge())
}

// Union is used to return a union of s and rhs as new set.
func (s FastIntSet) Union(rhs FastIntSet) FastIntSet {
	cps := s.Copy()
	cps.UnionWith(rhs)
	return cps
}

// UnionWith is used to copy all the elements of rhs to source.
func (s *FastIntSet) UnionWith(rhs FastIntSet) {
	s.small |= rhs.small
	if s.large == nil && rhs.large == nil {
		return
	}
	if s.large == nil {
		s.large = s.toLarge()
	}
	if rhs.large == nil {
		for i, ok := rhs.Next(0); ok; i, ok = rhs.Next(i + 1) {
			s.large.Insert(i)
		}
	} else {
		s.large.UnionWith(rhs.large)
	}
}

// Intersection is used to return the intersection of s and rhs.
func (s FastIntSet) Intersection(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from source.
func (s *FastIntSet) IntersectionWith(rhs FastIntSet) {
	s.small &= rhs.small
	if rhs.large == nil {
		s.large = nil
	}
	if s.large == nil {
		return
	}
	s.large.IntersectionWith(rhs.toLarge())
}

// Intersects is used to judge whether two set has something in common.
func (s FastIntSet) Intersects(rhs FastIntSet) bool {
	if (s.small & rhs.small) != 0 {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.toLarge())
}

// SubsetOf is used to judge whether rhs contains source set.
func (s FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.large == nil {
		return (s.small & rhs.small) == s.small
	}
	if s.large != nil && rhs.large != nil {
		return s.large.SubsetOf(rhs.large)
	}
	// s is large and rhs is small.
	if _, excess := s.largeToSmall(); excess {
		// couldn't map s to small.
		return false
	}
	// how come to this? eg: a set operates like: {insert:1, insert:65, remove:65}, resulting a large
	// int-set with only small numbers.
	return (s.small & rhs.small) == s.small
}

// Shift generates a new set which contains elements i+delta for elements i in
// the original set.
func (s *FastIntSet) Shift(delta int) FastIntSet {
	if s.large == nil {
		// Fast path.
		if delta > 0 {
			if bits.LeadingZeros64(s.small)-(64-smallCutOff) >= delta {
				return FastIntSet{small: s.small << uint32(delta)}
			}
		} else {
			if bits.TrailingZeros64(s.small) >= -delta {
				return FastIntSet{small: s.small >> uint32(-delta)}
			}
		}
	}
	// Do the slow thing.
	var result FastIntSet
	s.ForEach(func(i int) {
		result.Insert(i + delta)
	})
	return result
}

// AddRange adds the interval [from, to] to the Set.
func (s *FastIntSet) AddRange(from, to int) {
	if to < from {
		panic("invalid range when adding range to FastIntSet")
	}

	withinSmallBounds := from >= 0 && to < smallCutOff
	if withinSmallBounds && s.large == nil {
		nValues := to - from + 1
		s.small |= (1<<uint64(nValues) - 1) << uint64(from)
	} else {
		for i := from; i <= to; i++ {
			s.Insert(i)
		}
	}
}

func (s FastIntSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else if start+1 == end {
			fmt.Fprintf(&buf, "%d,%d", start, end)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}
	rangeStart, rangeEnd := -1, -1
	s.ForEach(func(i int) {
		if i < 0 {
			appendRange(i, i)
			return
		}
		if rangeStart != -1 && rangeEnd == i-1 {
			rangeEnd = i
		} else {
			if rangeStart != -1 {
				appendRange(rangeStart, rangeEnd)
			}
			rangeStart, rangeEnd = i, i
		}
	})
	if rangeStart != -1 {
		appendRange(rangeStart, rangeEnd)
	}
	buf.WriteByte(')')
	return buf.String()
}
