// Copyright 2017 PingCAP, Inc.
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

package ranger

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

// MutableRanges represents a range may change after it is created.
// It's mainly designed for plan-cache, since some ranges in a cached plan have to be rebuild when reusing.
type MutableRanges interface {
	// Range returns the underlying range values.
	Range() Ranges
	// Rebuild rebuilds the underlying ranges again.
	Rebuild(sctx planctx.PlanContext) error
	// CloneForPlanCache clones the MutableRanges for plan cache.
	CloneForPlanCache() MutableRanges
}

// Ranges implements the MutableRanges interface for range array.
type Ranges []*Range

// Range returns the range array.
func (rs Ranges) Range() Ranges {
	return rs
}

// Rebuild rebuilds this range.
func (Ranges) Rebuild(planctx.PlanContext) error {
	return nil
}

// CloneForPlanCache clones the MutableRanges for plan cache.
func (rs Ranges) CloneForPlanCache() MutableRanges {
	if rs == nil {
		return nil
	}
	cloned := make([]*Range, 0, len(rs))
	for _, r := range rs {
		cloned = append(cloned, r.Clone())
	}
	return Ranges(cloned)
}

// MemUsage gets the memory usage of ranges.
func (rs Ranges) MemUsage() (sum int64) {
	for _, ran := range rs {
		sum += ran.MemUsage()
	}
	return
}

// Range represents a range generated in physical plan building phase.
type Range struct {
	LowVal      []types.Datum // Low value is exclusive.
	HighVal     []types.Datum // High value is exclusive.
	Collators   []collate.Collator
	LowExclude  bool
	HighExclude bool
}

// Width returns the width of this range.
func (ran *Range) Width() int {
	return len(ran.LowVal)
}

// Clone clones a Range.
func (ran *Range) Clone() *Range {
	if ran == nil {
		return nil
	}
	newRange := &Range{
		LowVal:      make([]types.Datum, 0, len(ran.LowVal)),
		HighVal:     make([]types.Datum, 0, len(ran.HighVal)),
		LowExclude:  ran.LowExclude,
		HighExclude: ran.HighExclude,
	}
	for i, length := 0, len(ran.LowVal); i < length; i++ {
		newRange.LowVal = append(newRange.LowVal, ran.LowVal[i])
	}
	for i, length := 0, len(ran.HighVal); i < length; i++ {
		newRange.HighVal = append(newRange.HighVal, ran.HighVal[i])
	}
	newRange.Collators = append(newRange.Collators, ran.Collators...)
	return newRange
}

// IsPoint returns if the range is a point.
func (ran *Range) IsPoint(sctx *rangerctx.RangerContext) bool {
	return ran.isPoint(sctx.TypeCtx, sctx.RegardNULLAsPoint)
}

func (ran *Range) isPoint(tc types.Context, regardNullAsPoint bool) bool {
	if len(ran.LowVal) != len(ran.HighVal) {
		return false
	}
	for i := range ran.LowVal {
		a := ran.LowVal[i]
		b := ran.HighVal[i]
		if a.Kind() == types.KindMinNotNull || b.Kind() == types.KindMaxValue {
			return false
		}
		cmp, err := a.Compare(tc, &b, ran.Collators[i])
		if err != nil {
			return false
		}
		if cmp != 0 {
			return false
		}

		if a.IsNull() && b.IsNull() { // [NULL, NULL]
			if !regardNullAsPoint {
				return false
			}
		}
	}
	return !ran.LowExclude && !ran.HighExclude
}

// IsOnlyNull checks if the range has [NULL, NULL] or [NULL NULL, NULL NULL] range.
func (ran *Range) IsOnlyNull() bool {
	for i := range ran.LowVal {
		a := ran.LowVal[i]
		b := ran.HighVal[i]
		if !(a.IsNull() && b.IsNull()) {
			return false
		}
	}
	return true
}

// IsPointNonNullable returns if the range is a point without NULL.
func (ran *Range) IsPointNonNullable(tc types.Context) bool {
	return ran.isPoint(tc, false)
}

// IsPointNullable returns if the range is a point.
// TODO: unify the parameter type with IsPointNullable and IsPoint
func (ran *Range) IsPointNullable(tc types.Context) bool {
	return ran.isPoint(tc, true)
}

// IsFullRange check if the range is full scan range
func (ran *Range) IsFullRange(unsignedIntHandle bool) bool {
	if unsignedIntHandle {
		if len(ran.LowVal) != 1 || len(ran.HighVal) != 1 {
			return false
		}
		lowValRawString := formatDatum(ran.LowVal[0], true)
		highValRawString := formatDatum(ran.HighVal[0], false)
		return lowValRawString == "0" && highValRawString == "+inf"
	}
	if len(ran.LowVal) != len(ran.HighVal) {
		return false
	}
	for i := range ran.LowVal {
		lowValRawString := formatDatum(ran.LowVal[i], true)
		highValRawString := formatDatum(ran.HighVal[i], false)
		if ("-inf" != lowValRawString && "NULL" != lowValRawString) ||
			("+inf" != highValRawString && "NULL" != highValRawString) ||
			("NULL" == lowValRawString && "NULL" == highValRawString) {
			return false
		}
	}
	return true
}

// HasFullRange checks if any range in the slice is a full range.
func HasFullRange(ranges []*Range, unsignedIntHandle bool) bool {
	for _, ran := range ranges {
		if ran.IsFullRange(unsignedIntHandle) {
			return true
		}
	}
	return false
}

func dealWithRedact(input string, redact string) string {
	if input == "-inf" || input == "+inf" {
		return input
	}
	if redact == errors.RedactLogDisable {
		return input
	} else if redact == errors.RedactLogEnable {
		return "?"
	}
	return fmt.Sprintf("‹%s›", input)
}

// String implements the Stringer interface.
// don't use it in the product.
func (ran *Range) String() string {
	return ran.string(errors.RedactLogDisable)
}

// Redact is to print the range with redacting sensitive data.
func (ran *Range) Redact(redact string) string {
	return ran.string(redact)
}

// String implements the Stringer interface.
func (ran *Range) string(redact string) string {
	lowStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.LowVal {
		lowStrs = append(lowStrs, dealWithRedact(formatDatum(d, true), redact))
	}
	highStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.HighVal {
		highStrs = append(highStrs, dealWithRedact(formatDatum(d, false), redact))
	}
	l, r := "[", "]"
	if ran.LowExclude {
		l = "("
	}
	if ran.HighExclude {
		r = ")"
	}
	return l + strings.Join(lowStrs, " ") + "," + strings.Join(highStrs, " ") + r
}

// Encode encodes the range to its encoded value.
func (ran *Range) Encode(ec errctx.Context, loc *time.Location, lowBuffer, highBuffer []byte) ([]byte, []byte, error) {
	var err error
	lowBuffer, err = codec.EncodeKey(loc, lowBuffer[:0], ran.LowVal...)
	err = ec.HandleError(err)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		lowBuffer = kv.Key(lowBuffer).PrefixNext()
	}
	highBuffer, err = codec.EncodeKey(loc, highBuffer[:0], ran.HighVal...)
	err = ec.HandleError(err)
	if err != nil {
		return nil, nil, err
	}
	if !ran.HighExclude {
		highBuffer = kv.Key(highBuffer).PrefixNext()
	}
	return lowBuffer, highBuffer, nil
}

// PrefixEqualLen tells you how long the prefix of the range is a point.
// e.g. If this range is (1 2 3, 1 2 +inf), then the return value is 2.
func (ran *Range) PrefixEqualLen(tc types.Context) (int, error) {
	// Here, len(ran.LowVal) always equal to len(ran.HighVal)
	for i := range len(ran.LowVal) {
		cmp, err := ran.LowVal[i].Compare(tc, &ran.HighVal[i], ran.Collators[i])
		if err != nil {
			return 0, errors.Trace(err)
		}
		if cmp != 0 {
			return i, nil
		}
	}
	return len(ran.LowVal), nil
}

// EmptyRangeSize is the size of empty range.
const EmptyRangeSize = int64(unsafe.Sizeof(Range{}))

// MemUsage gets the memory usage of range.
func (ran *Range) MemUsage() (sum int64) {
	// 16 is the size of Collator interface.
	sum = EmptyRangeSize + int64(len(ran.Collators))*16
	for _, val := range ran.LowVal {
		sum += val.MemUsage()
	}
	for _, val := range ran.HighVal {
		sum += val.MemUsage()
	}
	// We ignore size of collator currently.
	return sum
}

func formatDatum(d types.Datum, isLeftSide bool) string {
	switch d.Kind() {
	case types.KindNull:
		return "NULL"
	case types.KindMinNotNull:
		return "-inf"
	case types.KindMaxValue:
		return "+inf"
	case types.KindInt64:
		switch d.GetInt64() {
		case math.MinInt64:
			if isLeftSide {
				return "-inf"
			}
		case math.MaxInt64:
			if !isLeftSide {
				return "+inf"
			}
		}
	case types.KindUint64:
		if d.GetUint64() == math.MaxUint64 && !isLeftSide {
			return "+inf"
		}
	case types.KindBytes:
		return fmt.Sprintf("%q", d.GetValue())
	case types.KindString:
		return fmt.Sprintf("%q", d.GetValue())
	case types.KindMysqlEnum, types.KindMysqlSet,
		types.KindMysqlJSON, types.KindBinaryLiteral, types.KindMysqlBit:
		return fmt.Sprintf("\"%v\"", d.GetValue())
	}
	return fmt.Sprintf("%v", d.GetValue())
}

// compareLexicographically compares two bounds from two ranges and returns 0, 1, -1
// for equal, greater than or less than respectively. It gets the two bounds,
// collations and if each bound is open (open1, open2) or closed. In addition,
// it also gets if each bound is lower or upper (low1, low2).
// Lower bounds logically can be extended with -infinity and upper bounds can be extended with +infinity.
func compareLexicographically(tc types.Context, bound1, bound2 []types.Datum, collators []collate.Collator,
	open1, open2, low1, low2 bool) (int, error) {
	n1 := len(bound1)
	n2 := len(bound2)
	n := min(n1, n2)

	for i := range n {
		cmp, err := bound1[i].Compare(tc, &bound2[i], collators[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}

	// Handle interval types
	if n1 == n2 {
		switch {
		case !open1 && !open2:
			return 0, nil
		case open1 == open2:
			if low1 == low2 {
				return 0, nil
			} else if low1 {
				return 1, nil
			} else {
				return -1, nil
			}
		case open1:
			if low1 {
				return 1, nil
			}
			return -1, nil
		case open2:
			if low2 {
				return -1, nil
			}
			return 1, nil
		}
	}

	// Unequal length ranges. We use -infinity for lower bounds and +infinity for upper bounds.
	if n1 < n2 {
		if low1 {
			// -infinity is less than anything
			return -1, nil
		}
		// +infinity is higher than anything
		return 1, nil
	}
	// n1 > n2
	if low2 {
		// anything is larger than -infinity.
		return 1, nil
	}
	// anything is less than +infinity
	return -1, nil
}

// Check if a list of Datum is a prefix of another list of Datum. This is useful for checking if
// lower/upper bound of a range is a subset of another.
func prefix(tc types.Context, superValue []types.Datum, supValue []types.Datum, length int, collators []collate.Collator) bool {
	for i := range length {
		cmp, err := superValue[i].Compare(tc, &supValue[i], collators[i])
		if (err != nil) || (cmp != 0) {
			return false
		}
	}
	return true
}

// Subset checks if a list of ranges(rs) is a subset of another list of ranges(superRanges).
// This is true if every range in the first list is a subset of any
// range in the second list. Also, we check if all elements of superRanges are covered.
func (rs Ranges) Subset(tc types.Context, superRanges Ranges) bool {
	var subset bool
	superRangesCovered := make([]bool, len(superRanges))
	if len(rs) == 0 {
		return len(superRanges) == 0
	} else if len(superRanges) == 0 {
		// unrestricted superRanges and restricted rs
		return true
	}

	for _, subRange := range rs {
		subset = false
		for i, superRange := range superRanges {
			if subRange.Subset(tc, superRange) {
				subset = true
				superRangesCovered[i] = true
				break
			}
		}
		if !subset {
			return false
		}
	}
	for i := range superRangesCovered {
		if !superRangesCovered[i] {
			return false
		}
	}

	return true
}

func checkCollators(ran1 *Range, ran2 *Range, length int) bool {
	// Make sure both ran and superRange have the same collations.
	// The current code path for this function always will have same collation
	// for ran and superRange. It is added here for future
	// use of the function.
	for i := range length {
		if ran1.Collators[i] != ran2.Collators[i] {
			return false
		}
	}
	return true
}

// Subset for Range type, check if range(ran)  is a subset of another range(otherRange).
// This is done by:
//   - Both ran and otherRange have the same collators. This is not needed for the current code path.
//     But, it is used here for future use of the function.
//   - Checking if the lower/upper bound of otherRange covers the corresponding lower/upper bound of ran.
//     Thus include checking open/closed inetrvals.
func (ran *Range) Subset(tc types.Context, otherRange *Range) bool {
	if len(ran.LowVal) < len(otherRange.LowVal) {
		return false
	}

	if !checkCollators(ran, otherRange, len(otherRange.LowVal)) {
		return false
	}

	// Either otherRange is closed or both ranges have the same open/close setting.
	lowExcludeOK := !otherRange.LowExclude || ran.LowExclude == otherRange.LowExclude
	highExcludeOK := !otherRange.HighExclude || ran.HighExclude == otherRange.HighExclude
	if !lowExcludeOK || !highExcludeOK {
		return false
	}

	return prefix(tc, otherRange.LowVal, ran.LowVal, len(otherRange.LowVal), ran.Collators) &&
		prefix(tc, otherRange.HighVal, ran.HighVal, len(otherRange.LowVal), ran.Collators)
}

// IntersectRange computes intersection between two ranges. err is set of something went wrong
// during comparison.
func (ran *Range) IntersectRange(tc types.Context, otherRange *Range) (*Range, error) {
	intersectLength := max(len(ran.LowVal), len(otherRange.LowVal))
	result := &Range{
		LowVal:    make([]types.Datum, 0, intersectLength),
		HighVal:   make([]types.Datum, 0, intersectLength),
		Collators: make([]collate.Collator, 0, intersectLength),
	}

	if len(ran.LowVal) > len(otherRange.LowVal) {
		result.Collators = ran.Collators
	} else {
		result.Collators = otherRange.Collators
	}

	lowVsHigh, err := compareLexicographically(tc, ran.LowVal, otherRange.HighVal, ran.Collators,
		ran.LowExclude, otherRange.HighExclude, true, false)
	if err != nil {
		return &Range{}, err
	}
	if lowVsHigh == 1 {
		return nil, nil
	}

	lowVsHigh, err = compareLexicographically(tc, otherRange.LowVal, ran.HighVal, ran.Collators,
		otherRange.LowExclude, ran.HighExclude, true, false)
	if err != nil {
		return &Range{}, err
	}
	if lowVsHigh == 1 {
		return nil, nil
	}

	lowVsLow, err := compareLexicographically(tc, ran.LowVal, otherRange.LowVal,
		ran.Collators, ran.LowExclude, otherRange.LowExclude, true, true)
	if err != nil {
		return &Range{}, err
	}
	if lowVsLow == -1 {
		result.LowVal = otherRange.LowVal
		result.LowExclude = otherRange.LowExclude
	} else {
		result.LowVal = ran.LowVal
		result.LowExclude = ran.LowExclude
	}

	highVsHigh, err := compareLexicographically(tc, ran.HighVal, otherRange.HighVal,
		ran.Collators, ran.HighExclude, otherRange.HighExclude, false, false)
	if err != nil {
		return &Range{}, err
	}
	if highVsHigh == 1 {
		result.HighVal = otherRange.HighVal
		result.HighExclude = otherRange.HighExclude
	} else {
		result.HighVal = ran.HighVal
		result.HighExclude = ran.HighExclude
	}
	return result, nil
}

// IntersectRanges computes pairwise intersection between each element in rs and otherRangeList.
func (rs Ranges) IntersectRanges(tc types.Context, otherRanges Ranges) Ranges {
	result := Ranges{}
	for _, rsRange := range rs {
		for _, otherRange := range otherRanges {
			subsetLength := min(len(rsRange.LowVal), len(otherRange.LowVal))
			if !checkCollators(rsRange, otherRange, subsetLength) {
				return nil
			}
			oneIntersection, err := rsRange.IntersectRange(tc, otherRange)
			if err != nil {
				return nil
			}
			if oneIntersection != nil {
				result = append(result, oneIntersection)
			}
		}
	}
	return result
}
