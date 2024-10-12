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

import "math"

// Architecture and/or implementation specific integer limits and bit widths.
const (
	MaxInt  = 1<<(IntBits-1) - 1
	MinInt  = -MaxInt - 1
	MaxUint = 1<<IntBits - 1
	IntBits = 32 << (^uint(0) >> 63) // Detects whether 32 or 64 bit
)

// Abs computes the absolute value of an int64.
func Abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// uintSizeTable is used to determine the length of a uint64.
var uintSizeTable = [21]uint64{
	0, // redundant 0 here, so to make function StrLenOfUint64Fast to count from 1 and return i directly
	9, 99, 999, 9999, 99999,
	999999, 9999999, 99999999, 999999999, 9999999999,
	99999999999, 999999999999, 9999999999999, 99999999999999, 999999999999999,
	9999999999999999, 99999999999999999, 999999999999999999, 9999999999999999999,
	math.MaxUint64,
}

// StrLenOfUint64Fast efficiently calculates the length of the string representation of a uint64.
func StrLenOfUint64Fast(x uint64) int {
	for i := 1; ; i++ {
		if x <= uintSizeTable[i] {
			return i
		}
	}
}

// StrLenOfInt64Fast efficiently calculates the length of the string representation of an int64.
func StrLenOfInt64Fast(x int64) int {
	size := 0
	if x < 0 {
		size = 1 // for the negative sign
	}
	return size + StrLenOfUint64Fast(uint64(Abs(x)))
}

// IsFinite checks whether a float64 is neither NaN nor infinity.
func IsFinite(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

// Max returns the largest element from its arguments.
func Max[T constraints.Ordered](x T, xs ...T) T {
	maxv := x
	for _, n := range xs {
		if n > maxv {
			maxv = n
		}
	}
	return maxv
}

// Min returns the smallest element from its arguments.
func Min[T constraints.Ordered](x T, xs ...T) T {
	minv := x
	for _, n := range xs {
		if n < minv {
			minv = n
		}
	}
	return minv
}

// Clamp restricts a value to a certain range [minv, maxv].
func Clamp[T constraints.Ordered](n, minv, maxv T) T {
	if n > maxv {
		return maxv
	}
	if n < minv {
		return minv
	}
	return n
}

// NextPowerOfTwo returns the smallest power of two greater than or equal to `i`.
func NextPowerOfTwo(i int64) int64 {
	if i <= 0 {
		return 1
	}
	return 1 << (64 - bits.LeadingZeros64(uint64(i-1)))
}

// Divide2Batches divides 'total' into 'batches', returning the size of each batch.
// Σ(batchSizes) = 'total'. If 'total' < 'batches', it returns 'total' batches with size 1.
// 'total' is allowed to be 0.
func Divide2Batches(total, batches int) []int {
	result := make([]int, 0, batches)
	quotient := total / batches
	remainder := total % batches
	for i := 0; i < batches && total > 0; i++ {
		size := quotient
		if remainder > 0 {
			size++
			remainder--
		}
		result = append(result, size)
		total -= size
	}
	return result
}
