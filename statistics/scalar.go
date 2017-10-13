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
	"encoding/binary"
	"math"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// calcFraction is used to calculate the fraction of the interval [lower, upper] that lies within the [lower, value]
// using the continuous-value assumption.
func calcFraction(lower, upper, value float64) float64 {
	if upper <= lower {
		return 0.5
	}
	if value <= lower {
		return 0
	}
	if value >= upper {
		return 1
	}
	frac := (value - lower) / (upper - lower)
	if math.IsNaN(frac) || math.IsInf(frac, 0) || frac < 0 || frac > 1 {
		return 0.5
	}
	return frac
}

// preCalculateDatumScalar converts the lower and upper to scalar. When the datum type is KindString or KindBytes, we also
// calculate their common prefix length, because when a value falls between lower and upper, the common prefix
// of lower and upper equals to the common prefix of the lower, upper and the value.
func preCalculateDatumScalar(lower, upper *types.Datum) (float64, float64, int) {
	common := 0
	if lower.Kind() == types.KindString || lower.Kind() == types.KindBytes {
		common = commonPrefixLength(lower.GetBytes(), upper.GetBytes())
	}
	return convertDatumToScalar(lower, common), convertDatumToScalar(upper, common), common
}

func convertDatumToScalar(value *types.Datum, commonPfxLen int) float64 {
	switch value.Kind() {
	case types.KindFloat32:
		return float64(value.GetFloat32())
	case types.KindFloat64:
		return value.GetFloat64()
	case types.KindInt64:
		return float64(value.GetInt64())
	case types.KindUint64:
		return float64(value.GetUint64())
	case types.KindMysqlDecimal:
		scalar, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return 0
		}
		return scalar
	case types.KindMysqlDuration:
		return float64(value.GetMysqlDuration().Duration)
	case types.KindMysqlTime:
		valueTime := value.GetMysqlTime()
		var minTime types.Time
		switch valueTime.Type {
		case mysql.TypeDate:
			minTime = types.Time{
				Time: types.MinDatetime,
				Type: mysql.TypeDate,
				Fsp:  types.DefaultFsp,
			}
		case mysql.TypeDatetime:
			minTime = types.Time{
				Time: types.MinDatetime,
				Type: mysql.TypeDatetime,
				Fsp:  types.DefaultFsp,
			}
		case mysql.TypeTimestamp:
			minTime = types.Time{
				Time: types.MinTimestamp,
				Type: mysql.TypeTimestamp,
				Fsp:  types.DefaultFsp,
			}
		}
		return float64(valueTime.Sub(&minTime).Duration)
	case types.KindString, types.KindBytes:
		bytes := value.GetBytes()
		if len(bytes) <= commonPfxLen {
			return 0
		}
		return convertBytesToScalar(bytes[commonPfxLen:])
	default:
		// do not know how to convert
		return 0
	}
}

func commonPrefixLength(lower, upper []byte) int {
	minLen := len(lower)
	if minLen > len(upper) {
		minLen = len(upper)
	}
	for i := 0; i < minLen; i++ {
		if lower[i] != upper[i] {
			return i
		}
	}
	return minLen
}

func convertBytesToScalar(value []byte) float64 {
	// Bytes type is viewed as a base-256 value, so we only consider at most 8 bytes.
	var buf [8]byte
	copy(buf[:], value)
	return float64(binary.BigEndian.Uint64(buf[:]))
}
