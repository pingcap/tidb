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
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"math"
)

// RoundFloat rounds float val to the nearest integer value with float64 format, like GNU rint function.
// RoundFloat uses default rounding mode, see http://www.gnu.org/software/libc/manual/html_node/Rounding.html
// so we will choose the even number if the result is midway between two representable value.
// e.g, 1.5 -> 2, 2.5 -> 2.
func RoundFloat(val float64) float64 {
	v, frac := math.Modf(val)
	if val >= 0.0 {
		if frac > 0.5 || (frac == 0.5 && uint64(v)%2 != 0) {
			v += 1.0
		}
	} else {
		if frac < -0.5 || (frac == -0.5 && uint64(v)%2 != 0) {
			v -= 1.0
		}
	}

	return v
}

// CompareInt64 returns an integer comparing the int64 x to y.
func CompareInt64(x, y int64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareUint64 returns an integer comparing the uint64 x to y.
func CompareUint64(x, y uint64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareFloat64 returns an integer comparing the float64 x to y.
func CompareFloat64(x, y float64) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

// CompareInteger  returns an integer comparing the int64 x to the uint64 y.
func CompareInteger(x int64, y uint64) int {
	if x < 0 {
		return -1
	}
	return CompareUint64(uint64(x), y)
}

// CompareString returns an integer comparing the string x to y.
func CompareString(x, y string) int {
	if x < y {
		return -1
	} else if x == y {
		return 0
	}

	return 1
}

func getMaxFloat(flen int, decimal int) float64 {
	intPartLen := flen - decimal
	f := math.Pow10(intPartLen)
	f -= math.Pow10(-decimal)
	return f
}

func truncateFloat(f float64, decimal int) float64 {
	pow := math.Pow10(decimal)
	t := (f - math.Floor(f)) * pow

	round := RoundFloat(t)

	f = math.Floor(f) + round/pow
	return f
}

// TruncateFloat tries to truncate f.
// If the result exceeds the max/min float that flen/decimal allowed, returns the max/min float allowed.
func TruncateFloat(f float64, flen int, decimal int) (float64, error) {
	if math.IsNaN(f) {
		// nan returns 0
		return 0, nil
	}

	maxF := getMaxFloat(flen, decimal)

	if !math.IsInf(f, 0) {
		f = truncateFloat(f, decimal)
	}

	if f > maxF {
		f = maxF
	} else if f < -maxF {
		f = -maxF
	}

	return f, nil
}
