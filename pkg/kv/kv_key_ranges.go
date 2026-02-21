// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"slices"

	"github.com/pingcap/errors"
)
// KeyRanges wrap the ranges for partitioned table cases.
// We might send ranges from different in the one request.
type KeyRanges struct {
	ranges        [][]KeyRange
	rowCountHints [][]int

	isPartitioned bool
}

// NewPartitionedKeyRanges constructs a new RequestRange for partitioned table.
func NewPartitionedKeyRanges(ranges [][]KeyRange) *KeyRanges {
	return NewPartitionedKeyRangesWithHints(ranges, nil)
}

// NewNonPartitionedKeyRanges constructs a new RequestRange for a non-partitioned table.
func NewNonPartitionedKeyRanges(ranges []KeyRange) *KeyRanges {
	return NewNonParitionedKeyRangesWithHint(ranges, nil)
}

// NewPartitionedKeyRangesWithHints constructs a new RequestRange for partitioned table with row count hint.
func NewPartitionedKeyRangesWithHints(ranges [][]KeyRange, hints [][]int) *KeyRanges {
	return &KeyRanges{
		ranges:        ranges,
		rowCountHints: hints,
		isPartitioned: true,
	}
}

// NewNonParitionedKeyRangesWithHint constructs a new RequestRange for a non partitioned table with rou count hint.
func NewNonParitionedKeyRangesWithHint(ranges []KeyRange, hints []int) *KeyRanges {
	rr := &KeyRanges{
		ranges:        [][]KeyRange{ranges},
		isPartitioned: false,
	}
	if hints != nil {
		rr.rowCountHints = [][]int{hints}
	}
	return rr
}

// FirstPartitionRange returns the the result of first range.
// We may use some func to generate ranges for both partitioned table and non partitioned table.
// This method provides a way to fallback to non-partitioned ranges.
func (rr *KeyRanges) FirstPartitionRange() []KeyRange {
	if len(rr.ranges) == 0 {
		return []KeyRange{}
	}
	return rr.ranges[0]
}

// SetToNonPartitioned set the status to non-partitioned.
func (rr *KeyRanges) SetToNonPartitioned() error {
	if len(rr.ranges) > 1 {
		return errors.Errorf("you want to change the partitioned ranges to non-partitioned ranges")
	}
	rr.isPartitioned = false
	return nil
}

// AppendSelfTo appends itself to another slice.
func (rr *KeyRanges) AppendSelfTo(ranges []KeyRange) []KeyRange {
	for _, r := range rr.ranges {
		ranges = append(ranges, r...)
	}
	return ranges
}

// SortByFunc sorts each partition's ranges.
// Since the ranges are sorted in most cases, we check it first.
func (rr *KeyRanges) SortByFunc(sortFunc func(i, j KeyRange) int) {
	if !slices.IsSortedFunc(rr.ranges, func(i, j []KeyRange) int {
		// A simple short-circuit since the empty range actually won't make anything wrong.
		if len(i) == 0 || len(j) == 0 {
			return -1
		}
		return sortFunc(i[0], j[0])
	}) {
		slices.SortFunc(rr.ranges, func(i, j []KeyRange) int {
			if len(i) == 0 {
				return -1
			}
			if len(j) == 0 {
				return 1
			}
			return sortFunc(i[0], j[0])
		})
	}
	for i := range rr.ranges {
		if !slices.IsSortedFunc(rr.ranges[i], sortFunc) {
			slices.SortFunc(rr.ranges[i], sortFunc)
		}
	}
}

// ForEachPartitionWithErr runs the func for each partition with an error check.
func (rr *KeyRanges) ForEachPartitionWithErr(theFunc func([]KeyRange, []int) error) (err error) {
	for i := range rr.ranges {
		var hints []int
		if len(rr.rowCountHints) > i {
			hints = rr.rowCountHints[i]
		}
		err = theFunc(rr.ranges[i], hints)
		if err != nil {
			return err
		}
	}
	return nil
}

// ForEachPartition runs the func for each partition without error check.
func (rr *KeyRanges) ForEachPartition(theFunc func([]KeyRange)) {
	for i := range rr.ranges {
		theFunc(rr.ranges[i])
	}
}

// PartitionNum returns how many partition is involved in the ranges.
func (rr *KeyRanges) PartitionNum() int {
	return len(rr.ranges)
}

// IsFullySorted checks whether the ranges are sorted inside partition and each partition is also sorated.
func (rr *KeyRanges) IsFullySorted() bool {
	sortedByPartition := slices.IsSortedFunc(rr.ranges, func(i, j []KeyRange) int {
		// A simple short-circuit since the empty range actually won't make anything wrong.
		if len(i) == 0 || len(j) == 0 {
			return -1
		}
		return bytes.Compare(i[0].StartKey, j[0].StartKey)
	})
	if !sortedByPartition {
		return false
	}
	for _, ranges := range rr.ranges {
		if !slices.IsSortedFunc(ranges, func(i, j KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		}) {
			return false
		}
	}
	return true
}

// TotalRangeNum returns how many ranges there are.
func (rr *KeyRanges) TotalRangeNum() int {
	ret := 0
	for _, r := range rr.ranges {
		ret += len(r)
	}
	return ret
}

