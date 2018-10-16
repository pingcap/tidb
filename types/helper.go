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
	"strings"
	"unicode"

	"github.com/pkg/errors"
)

// RoundFloat rounds float val to the nearest integer value with float64 format, like MySQL Round function.
// RoundFloat uses default rounding mode, see https://dev.mysql.com/doc/refman/5.7/en/precision-math-rounding.html
// so rounding use "round half away from zero".
// e.g, 1.5 -> 2, -1.5 -> -2.
func RoundFloat(f float64) float64 {
	if math.Abs(f) < 0.5 {
		return 0
	}

	return math.Trunc(f + math.Copysign(0.5, f))
}

// Round rounds the argument f to dec decimal places.
// dec defaults to 0 if not specified. dec can be negative
// to cause dec digits left of the decimal point of the
// value f to become zero.
func Round(f float64, dec int) float64 {
	shift := math.Pow10(dec)
	tmp := f * shift
	if math.IsInf(tmp, 0) {
		return f
	}
	return RoundFloat(tmp) / shift
}

// Truncate truncates the argument f to dec decimal places.
// dec defaults to 0 if not specified. dec can be negative
// to cause dec digits left of the decimal point of the
// value f to become zero.
func Truncate(f float64, dec int) float64 {
	shift := math.Pow10(dec)
	tmp := f * shift
	if math.IsInf(tmp, 0) {
		return f
	}
	return math.Trunc(tmp) / shift
}

func getMaxFloat(flen int, decimal int) float64 {
	intPartLen := flen - decimal
	f := math.Pow10(intPartLen)
	f -= math.Pow10(-decimal)
	return f
}

// TruncateFloat tries to truncate f.
// If the result exceeds the max/min float that flen/decimal allowed, returns the max/min float allowed.
func TruncateFloat(f float64, flen int, decimal int) (float64, error) {
	if math.IsNaN(f) {
		// nan returns 0
		return 0, ErrOverflow.GenWithStackByArgs("DOUBLE", "")
	}

	maxF := getMaxFloat(flen, decimal)

	if !math.IsInf(f, 0) {
		f = Round(f, decimal)
	}

	var err error
	if f > maxF {
		f = maxF
		err = ErrOverflow.GenWithStackByArgs("DOUBLE", "")
	} else if f < -maxF {
		f = -maxF
		err = ErrOverflow.GenWithStackByArgs("DOUBLE", "")
	}

	return f, errors.Trace(err)
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func myMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func myMaxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

func myMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func myMinInt8(a, b int8) int8 {
	if a < b {
		return a
	}
	return b
}

const (
	maxUint    = uint64(math.MaxUint64)
	uintCutOff = maxUint/uint64(10) + 1
	intCutOff  = uint64(math.MaxInt64) + 1
)

// strToInt converts a string to an integer in best effort.
func strToInt(str string) (int64, error) {
	str = strings.TrimSpace(str)
	if len(str) == 0 {
		return 0, ErrTruncated
	}
	negative := false
	i := 0
	if str[i] == '-' {
		negative = true
		i++
	} else if str[i] == '+' {
		i++
	}

	var (
		err    error
		hasNum = false
	)
	r := uint64(0)
	for ; i < len(str); i++ {
		if !unicode.IsDigit(rune(str[i])) {
			err = ErrTruncated
			break
		}
		hasNum = true
		if r >= uintCutOff {
			r = 0
			err = errors.Trace(ErrBadNumber)
			break
		}
		r = r * uint64(10)

		r1 := r + uint64(str[i]-'0')
		if r1 < r || r1 > maxUint {
			r = 0
			err = errors.Trace(ErrBadNumber)
			break
		}
		r = r1
	}
	if !hasNum {
		err = ErrTruncated
	}

	if !negative && r >= intCutOff {
		return math.MaxInt64, errors.Trace(ErrBadNumber)
	}

	if negative && r > intCutOff {
		return math.MinInt64, errors.Trace(ErrBadNumber)
	}

	if negative {
		r = -r
	}
	return int64(r), err
}

// CalculateMergeFloat64 merge variance and partialCount > 0 and mergeCount > 0.
// NOTE: mergeCount and mergeSum do not include partialCount and partialSum yet.
//
// Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
// "Algorithms for computing the sample variance: analysis and recommendations"
// The American Statistician, 37 (1983) pp. 242--247.
//
// variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)
//
// where:
// - variance is sum[(x-avg)^2] (this is actually n times the variance) and is updated at every step.
// - n is the count of elements in chunk1
// - m is the count of elements in chunk2
// - t1 = sum of elements in chunk1,
// - t2 = sum of elements in chunk2.
//
// This algorithm was proven to be numerically stable by J.L. Barlow in
// "Error analysis of a pairwise summation algorithm to compute sample variance"
// Numer. Math, 58 (1991) pp. 583--590
func CalculateMergeFloat64(partialCount int64, mergeCount int64, partialSum float64, mergeSum float64, partialVariance float64, mergeVariance float64) (float64, float64, error) {
	doublePartialCount, doubleMergeCount := float64(partialCount), float64(mergeCount)
	t := (doublePartialCount/doubleMergeCount)*mergeSum - partialSum
	return mergeVariance + partialVariance + (doubleMergeCount/doublePartialCount/(doubleMergeCount+doublePartialCount))*t*t, partialSum + mergeSum, nil
}

// CalculateMergeDecimal merge variance and partialCount > 0 and mergeCount > 0.
// NOTE: mergeCount and mergeSum do not include partialCount and partialSum yet.
func CalculateMergeDecimal(partialCount int64, mergeCount int64, partialSum *MyDecimal, mergeSum *MyDecimal, partialVariance *MyDecimal, mergeVariance *MyDecimal) (variance *MyDecimal, sum *MyDecimal, err error) {
	err = DecimalAdd(partialSum, mergeSum, sum)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	decimalPartialCount, decimalMergeCount := NewDecFromInt(partialCount), NewDecFromInt(mergeCount)
	divCount := new(MyDecimal)
	err = DecimalDiv(decimalPartialCount, decimalMergeCount, divCount, DivFracIncr)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	addCount := new(MyDecimal)
	err = DecimalAdd(decimalPartialCount, decimalMergeCount, addCount)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	doubleDivCount := new(MyDecimal)
	err = DecimalMul(divCount, divCount, doubleDivCount)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	mgDivPt := new(MyDecimal)
	err = DecimalDiv(decimalMergeCount, decimalPartialCount, mgDivPt, DivFracIncr)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	t2 := new(MyDecimal)
	err = DecimalDiv(mgDivPt, addCount, t2, DivFracIncr)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	t2MulDouble := new(MyDecimal)
	err = DecimalMul(t2, doubleDivCount, t2MulDouble)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	addVariance := new(MyDecimal)
	err = DecimalAdd(partialVariance, mergeVariance, addVariance)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	err = DecimalAdd(addVariance, t2MulDouble, variance)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	return
}
