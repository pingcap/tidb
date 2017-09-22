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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"math"

	"github.com/pingcap/tidb/util/types"
)

// calcFraction is used to calculate the fraction of the interval [lower, upper] that lies within the [lower, value]
// using the continuous-value assumption.
func calcFraction(lower, upper, value *types.Datum) float64 {
	lowerScalar, upperScalar, valueScalar := convertToScalar(lower, upper, value)
	if upperScalar <= lowerScalar {
		return 0.5
	}
	if valueScalar <= lowerScalar {
		return 0
	}
	if valueScalar >= upperScalar {
		return 1
	}
	frac := (valueScalar - lowerScalar) / (upperScalar - lowerScalar)
	if math.IsNaN(frac) || math.IsInf(frac, 0) || frac < 0 || frac > 1 {
		return 0.5
	}
	return frac
}

// convertToScalar will convert the datum to scalar values.
func convertToScalar(lower, upper, value *types.Datum) (float64, float64, float64) {
	switch value.Kind() {
	case types.KindFloat32:
		return float64(lower.GetFloat32()), float64(upper.GetFloat32()), float64(value.GetFloat32())
	case types.KindFloat64:
		return lower.GetFloat64(), upper.GetFloat64(), value.GetFloat64()
	case types.KindInt64:
		return float64(lower.GetInt64()), float64(upper.GetInt64()), float64(value.GetInt64())
	case types.KindUint64:
		return float64(lower.GetUint64()), float64(upper.GetUint64()), float64(value.GetUint64())
	case types.KindMysqlDecimal:
		return convertDecimalToScalar(lower, upper, value)
	case types.KindMysqlDuration:
		return float64(lower.GetMysqlDuration().Duration), float64(upper.GetMysqlDuration().Duration), float64(value.GetMysqlDuration().Duration)
	case types.KindMysqlTime:
		lowerTime := lower.GetMysqlTime()
		upperTime := upper.GetMysqlTime()
		valueTime := value.GetMysqlTime()
		return 0, float64(upperTime.Sub(&lowerTime).Duration), float64(valueTime.Sub(&lowerTime).Duration)
	case types.KindString, types.KindBytes:
		return convertBytesToScalar(lower.GetBytes(), upper.GetBytes(), value.GetBytes())
	default:
		// do not know how to convert
		return 0, 0, 0
	}
}

// Decimal types are simply converted to their equivalent float64 values.
func convertDecimalToScalar(lower, upper, value *types.Datum) (float64, float64, float64) {
	lowerScalar, err := lower.GetMysqlDecimal().ToFloat64()
	if err != nil {
		return 0, 0, 0
	}
	upperScalar, err := upper.GetMysqlDecimal().ToFloat64()
	if err != nil {
		return 0, 0, 0
	}
	valueScalar, err := value.GetMysqlDecimal().ToFloat64()
	if err != nil {
		return 0, 0, 0
	}
	return lowerScalar, upperScalar, valueScalar
}

// Bytes type is viewed as a base-256 value.
func convertBytesToScalar(lower, upper, value []byte) (float64, float64, float64) {
	minLen := len(lower)
	if len(upper) < minLen {
		minLen = len(upper)
	}
	if len(value) < minLen {
		minLen = len(value)
	}
	// remove their common prefix
	common := 0
	for common < minLen {
		if lower[common] == upper[common] && lower[common] == value[common] {
			common++
		} else {
			break
		}
	}
	return convertOneBytesToScalar(lower[common:]), convertOneBytesToScalar(upper[common:]), convertOneBytesToScalar(value[common:])
}

func convertOneBytesToScalar(value []byte) float64 {
	base, num := float64(math.MaxUint8+1), 0.0
	// Since the base is 256, we only consider at most 10 bytes.
	maxLen := 10
	denom := math.Pow(base, float64(maxLen)/2.0)
	for i, b := range value {
		if i >= maxLen {
			return num
		}
		num += float64(b) * denom
		denom /= base
	}
	return num
}
