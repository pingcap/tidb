// Copyright 2019 PingCAP, Inc.
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

package mathutil

import (
	"math"

	"golang.org/x/exp/constraints"
)

// Architecture and/or implementation specific integer limits and bit widths.
const (
	MaxInt  = 1<<(IntBits-1) - 1
	MinInt  = -MaxInt - 1
	MaxUint = 1<<IntBits - 1
	IntBits = 1 << (^uint(0)>>32&1 + ^uint(0)>>16&1 + ^uint(0)>>8&1 + 3)
)

// Abs implement the abs function according to http://cavaliercoder.com/blog/optimized-abs-for-int64-in-go.html
func Abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

// uintSizeTable is used as a table to do comparison to get uint length is faster than doing loop on division with 10
var uintSizeTable = [21]uint64{
	0, // redundant 0 here, so to make function StrLenOfUint64Fast to count from 1 and return i directly
	9, 99, 999, 9999, 99999,
	999999, 9999999, 99999999, 999999999, 9999999999,
	99999999999, 999999999999, 9999999999999, 99999999999999, 999999999999999,
	9999999999999999, 99999999999999999, 999999999999999999, 9999999999999999999,
	math.MaxUint64,
} // math.MaxUint64 is 18446744073709551615 and it has 20 digits

// StrLenOfUint64Fast efficiently calculate the string character lengths of an uint64 as input
func StrLenOfUint64Fast(x uint64) int {
	for i := 1; ; i++ {
		if x <= uintSizeTable[i] {
			return i
		}
	}
}

// StrLenOfInt64Fast efficiently calculate the string character lengths of an int64 as input
func StrLenOfInt64Fast(x int64) int {
	size := 0
	if x < 0 {
		size = 1 // add "-" sign on the length count
	}
	return size + StrLenOfUint64Fast(uint64(Abs(x)))
}

// IsFinite reports whether f is neither NaN nor an infinity.
func IsFinite(f float64) bool {
	return !math.IsNaN(f - f)
}

// Max returns the largest one from its arguments.
func Max[v constraints.Ordered](x v, xs ...v) v {
	max := x
	for _, n := range xs {
		if n > max {
			max = n
		}
	}
	return max
}

// Min returns the smallest one from its arguments.
func Min[v constraints.Ordered](x v, xs ...v) v {
	min := x
	for _, n := range xs {
		if n < min {
			min = n
		}
	}
	return min
}

// Clamp restrict a value to a certain interval.
func Clamp[v constraints.Integer | constraints.Float](n, min, max v) v {
	if n >= max {
		return max
	} else if n <= min {
		return min
	}
	return n
}
