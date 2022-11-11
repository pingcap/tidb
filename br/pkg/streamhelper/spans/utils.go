package spans

import (
	"bytes"
	"fmt"
	"math"
	"sort"
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
		return bytes.Compare(frs[i].StartKey, frs[j].StartKey) < 0
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
