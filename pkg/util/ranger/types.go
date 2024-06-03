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
	Rebuild() error
}

// Ranges implements the MutableRanges interface for range array.
type Ranges []*Range

// Range returns the range array.
func (rs Ranges) Range() Ranges {
	return rs
}

// Rebuild rebuilds this range.
func (Ranges) Rebuild() error {
	return nil
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

// String implements the Stringer interface.
func (ran *Range) String() string {
	lowStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.LowVal {
		lowStrs = append(lowStrs, formatDatum(d, true))
	}
	highStrs := make([]string, 0, len(ran.LowVal))
	for _, d := range ran.HighVal {
		highStrs = append(highStrs, formatDatum(d, false))
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
	for i := 0; i < len(ran.LowVal); i++ {
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
