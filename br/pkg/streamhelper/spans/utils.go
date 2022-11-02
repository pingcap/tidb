package spans

import (
	"bytes"
	"fmt"
	"math"
)

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
