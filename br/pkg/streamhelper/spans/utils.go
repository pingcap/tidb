// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/tidb/br/pkg/utils"
)

// Overlaps checks whether two spans have overlapped part.
func Overlaps(a, b Span) bool {
	if len(b.EndKey) == 0 {
		return len(a.EndKey) == 0 || bytes.Compare(a.EndKey, b.StartKey) > 0
	}
	if len(a.EndKey) == 0 {
		return len(b.EndKey) == 0 || bytes.Compare(b.EndKey, a.StartKey) > 0
	}
	return bytes.Compare(a.StartKey, b.EndKey) < 0 && bytes.Compare(b.StartKey, a.EndKey) < 0
}

func Debug(full *ValueSortedFull) {
	var result []Valued
	full.Traverse(func(v Valued) bool {
		result = append(result, v)
		return true
	})
	var idx []Valued
	full.TraverseValuesLessThan(math.MaxUint64, func(v Valued) bool {
		idx = append(idx, v)
		return true
	})
	fmt.Printf("%s\n\tidx = %s\n", result, idx)
}

// Collapse collapse ranges overlapping or adjacent.
// Example:
// Collapse({[1, 4], [2, 8], [3, 9]}) == {[1, 9]}
// Collapse({[1, 3], [4, 7], [2, 3]}) == {[1, 3], [4, 7]}
func Collapse(length int, getRange func(int) Span) []Span {
	frs := make([]Span, 0, length)
	for i := 0; i < length; i++ {
		frs = append(frs, getRange(i))
	}

	sort.Slice(frs, func(i, j int) bool {
		start := bytes.Compare(frs[i].StartKey, frs[j].StartKey)
		if start != 0 {
			return start < 0
		}
		return utils.CompareBytesExt(frs[i].EndKey, true, frs[j].EndKey, true) < 0
	})

	result := make([]Span, 0, len(frs))
	i := 0
	for i < len(frs) {
		item := frs[i]
		for {
			i++
			if i >= len(frs) || (len(item.EndKey) != 0 && bytes.Compare(frs[i].StartKey, item.EndKey) > 0) {
				break
			}
			if len(item.EndKey) != 0 && bytes.Compare(item.EndKey, frs[i].EndKey) < 0 || len(frs[i].EndKey) == 0 {
				item.EndKey = frs[i].EndKey
			}
		}
		result = append(result, item)
	}
	return result
}

// Full returns a full span crossing the key space.
func Full() []Span {
	return []Span{{}}
}

func (x Valued) Equals(y Valued) bool {
	return x.Value == y.Value && bytes.Equal(x.Key.StartKey, y.Key.StartKey) && bytes.Equal(x.Key.EndKey, y.Key.EndKey)
}

func ValuedSetEquals(xs, ys []Valued) bool {
	if len(xs) == 0 || len(ys) == 0 {
		return len(ys) == len(xs)
	}

	sort.Slice(xs, func(i, j int) bool {
		start := bytes.Compare(xs[i].Key.StartKey, xs[j].Key.StartKey)
		if start != 0 {
			return start < 0
		}
		return utils.CompareBytesExt(xs[i].Key.EndKey, true, xs[j].Key.EndKey, true) < 0
	})
	sort.Slice(ys, func(i, j int) bool {
		start := bytes.Compare(ys[i].Key.StartKey, ys[j].Key.StartKey)
		if start != 0 {
			return start < 0
		}
		return utils.CompareBytesExt(ys[i].Key.EndKey, true, ys[j].Key.EndKey, true) < 0
	})

	xi := 0
	yi := 0

	for {
		if xi >= len(xs) || yi >= len(ys) {
			return (xi >= len(xs)) == (yi >= len(ys))
		}
		x := xs[xi]
		y := ys[yi]

		if !bytes.Equal(x.Key.StartKey, y.Key.StartKey) {
			return false
		}

		for {
			if xi >= len(xs) || yi >= len(ys) {
				return (xi >= len(xs)) == (yi >= len(ys))
			}
			x := xs[xi]
			y := ys[yi]

			if x.Value != y.Value {
				return false
			}

			c := utils.CompareBytesExt(x.Key.EndKey, true, y.Key.EndKey, true)
			if c == 0 {
				xi++
				yi++
				break
			}
			if c < 0 {
				xi++
				// If not adjacent key, return false directly.
				if xi < len(xs) && utils.CompareBytesExt(x.Key.EndKey, true, xs[xi].Key.StartKey, false) != 0 {
					return false
				}
			}
			if c > 0 {
				yi++
				if yi < len(ys) && utils.CompareBytesExt(y.Key.EndKey, true, ys[yi].Key.StartKey, false) != 0 {
					return false
				}
			}
		}
	}
}
