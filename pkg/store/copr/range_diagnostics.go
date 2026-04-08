// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/kv"
)

func rangeIssuesForKeyRanges(ranges *KeyRanges) rangeIssueStats {
	var stats rangeIssueStats
	if ranges == nil || ranges.Len() == 0 {
		return stats
	}

	validateRange := func(r kv.KeyRange) {
		if len(r.EndKey) > 0 && bytes.Compare(r.StartKey, r.EndKey) > 0 {
			stats.add(rangeIssueInvalidBound)
		}
	}

	prev := ranges.At(0)
	validateRange(prev)
	for i := 1; i < ranges.Len(); i++ {
		curr := ranges.At(i)
		validateRange(curr)
		switch {
		case len(prev.EndKey) == 0:
			stats.add(rangeIssueInfiniteTail)
		case bytes.Compare(prev.EndKey, curr.StartKey) > 0:
			stats.add(classifyRangePair(prev, curr))
		}
		prev = curr
	}
	return stats
}

func minStartAndMaxEndKeyOfKeyRanges(ranges *KeyRanges) (minStart, maxEnd []byte) {
	if ranges == nil || ranges.Len() == 0 {
		return nil, nil
	}
	minStart = ranges.At(0).StartKey
	maxEnd = ranges.At(0).EndKey
	for i := 1; i < ranges.Len(); i++ {
		r := ranges.At(i)
		if bytes.Compare(r.StartKey, minStart) < 0 {
			minStart = r.StartKey
		}
		if compareRangeEnd(r.EndKey, maxEnd) > 0 {
			maxEnd = r.EndKey
		}
	}
	return minStart, maxEnd
}

func firstOutOfBoundKeyRangeInLocation(ranges *KeyRanges, locStart, locEnd []byte) (idx int, r kv.KeyRange, reason string) {
	if ranges == nil {
		return -1, kv.KeyRange{}, ""
	}
	rangeCount := ranges.Len()
	for i := range rangeCount {
		r = ranges.At(i)
		if bytes.Compare(r.StartKey, locStart) < 0 {
			return i, r, "start_before_location_start"
		}
		if len(locEnd) > 0 && bytes.Compare(r.StartKey, locEnd) >= 0 {
			return i, r, "start_after_or_eq_location_end"
		}
		if len(r.EndKey) == 0 {
			if len(locEnd) != 0 {
				return i, r, "end_infinite_but_location_finite"
			}
		} else if len(locEnd) != 0 && bytes.Compare(r.EndKey, locEnd) > 0 {
			return i, r, "end_after_location_end"
		}
		if len(r.EndKey) > 0 && bytes.Compare(r.StartKey, r.EndKey) > 0 {
			return i, r, "invalid_start_greater_than_end"
		}
	}
	return -1, kv.KeyRange{}, ""
}
