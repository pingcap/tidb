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
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/types"
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

func convertDatumToScalar(value *types.Datum, commonPfxLen int) float64 {
	switch value.Kind() {
	case types.KindMysqlDecimal:
		scalar, err := value.GetMysqlDecimal().ToFloat64()
		if err != nil {
			return 0
		}
		return scalar
	case types.KindMysqlTime:
		valueTime := value.GetMysqlTime()
		var minTime types.Time
		switch valueTime.Type() {
		case mysql.TypeDate:
			minTime = types.NewTime(types.MinDatetime, mysql.TypeDate, types.DefaultFsp)
		case mysql.TypeDatetime:
			minTime = types.NewTime(types.MinDatetime, mysql.TypeDatetime, types.DefaultFsp)
		case mysql.TypeTimestamp:
			minTime = types.MinTimestamp
		}
		sc := &stmtctx.StatementContext{TimeZone: types.BoundTimezone}
		return float64(valueTime.Sub(sc, &minTime).Duration)
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

// PreCalculateScalar converts the lower and upper to scalar. When the datum type is KindString or KindBytes, we also
// calculate their common prefix length, because when a value falls between lower and upper, the common prefix
// of lower and upper equals to the common prefix of the lower, upper and the value. For some simple types like `Int64`,
// we do not convert it because we can directly infer the scalar value.
func (hg *Histogram) PreCalculateScalar() {
	len := hg.Len()
	if len == 0 {
		return
	}
	switch hg.GetLower(0).Kind() {
	case types.KindMysqlDecimal, types.KindMysqlTime:
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			hg.scalars[i] = scalar{
				lower: convertDatumToScalar(hg.GetLower(i), 0),
				upper: convertDatumToScalar(hg.GetUpper(i), 0),
			}
		}
	case types.KindBytes, types.KindString:
		hg.scalars = make([]scalar, len)
		for i := 0; i < len; i++ {
			lower, upper := hg.GetLower(i), hg.GetUpper(i)
			common := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
			hg.scalars[i] = scalar{
				commonPfxLen: common,
				lower:        convertDatumToScalar(lower, common),
				upper:        convertDatumToScalar(upper, common),
			}
		}
	}
}

func (hg *Histogram) calcFraction(index int, value *types.Datum) float64 {
	lower, upper := hg.Bounds.GetRow(2*index), hg.Bounds.GetRow(2*index+1)
	switch value.Kind() {
	case types.KindFloat32:
		return calcFraction(float64(lower.GetFloat32(0)), float64(upper.GetFloat32(0)), float64(value.GetFloat32()))
	case types.KindFloat64:
		return calcFraction(lower.GetFloat64(0), upper.GetFloat64(0), value.GetFloat64())
	case types.KindInt64:
		return calcFraction(float64(lower.GetInt64(0)), float64(upper.GetInt64(0)), float64(value.GetInt64()))
	case types.KindUint64:
		return calcFraction(float64(lower.GetUint64(0)), float64(upper.GetUint64(0)), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		return calcFraction(float64(lower.GetDuration(0, 0).Duration), float64(upper.GetDuration(0, 0).Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		return calcFraction(hg.scalars[index].lower, hg.scalars[index].upper, convertDatumToScalar(value, hg.scalars[index].commonPfxLen))
	}
	return 0.5
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

func calcFraction4Datums(lower, upper, value *types.Datum) float64 {
	switch value.Kind() {
	case types.KindFloat32:
		return calcFraction(float64(lower.GetFloat32()), float64(upper.GetFloat32()), float64(value.GetFloat32()))
	case types.KindFloat64:
		return calcFraction(lower.GetFloat64(), upper.GetFloat64(), value.GetFloat64())
	case types.KindInt64:
		return calcFraction(float64(lower.GetInt64()), float64(upper.GetInt64()), float64(value.GetInt64()))
	case types.KindUint64:
		return calcFraction(float64(lower.GetUint64()), float64(upper.GetUint64()), float64(value.GetUint64()))
	case types.KindMysqlDuration:
		return calcFraction(float64(lower.GetMysqlDuration().Duration), float64(upper.GetMysqlDuration().Duration), float64(value.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal, types.KindMysqlTime:
		return calcFraction(convertDatumToScalar(lower, 0), convertDatumToScalar(upper, 0), convertDatumToScalar(value, 0))
	case types.KindBytes, types.KindString:
		commonPfxLen := commonPrefixLength(lower.GetBytes(), upper.GetBytes())
		return calcFraction(convertDatumToScalar(lower, commonPfxLen), convertDatumToScalar(upper, commonPfxLen), convertDatumToScalar(value, commonPfxLen))
	}
	return 0.5
}

const maxNumStep = 10

func enumRangeValues(low, high types.Datum, lowExclude, highExclude bool) []types.Datum {
	if low.Kind() != high.Kind() {
		return nil
	}
	exclude := 0
	if lowExclude {
		exclude++
	}
	if highExclude {
		exclude++
	}
	switch low.Kind() {
	case types.KindInt64:
		// Overflow check.
		lowVal, highVal := low.GetInt64(), high.GetInt64()
		if lowVal <= 0 && highVal >= 0 {
			if lowVal < -maxNumStep || highVal > maxNumStep {
				return nil
			}
		}
		remaining := highVal - lowVal
		if remaining >= maxNumStep+1 {
			return nil
		}
		remaining = remaining + 1 - int64(exclude)
		if remaining >= maxNumStep {
			return nil
		}
		values := make([]types.Datum, 0, remaining)
		startValue := lowVal
		if lowExclude {
			startValue++
		}
		for i := int64(0); i < remaining; i++ {
			values = append(values, types.NewIntDatum(startValue+i))
		}
		return values
	case types.KindUint64:
		remaining := high.GetUint64() - low.GetUint64()
		if remaining >= maxNumStep+1 {
			return nil
		}
		remaining = remaining + 1 - uint64(exclude)
		if remaining >= maxNumStep {
			return nil
		}
		values := make([]types.Datum, 0, remaining)
		startValue := low.GetUint64()
		if lowExclude {
			startValue++
		}
		for i := uint64(0); i < remaining; i++ {
			values = append(values, types.NewUintDatum(startValue+i))
		}
		return values
	case types.KindMysqlDuration:
		lowDur, highDur := low.GetMysqlDuration(), high.GetMysqlDuration()
		fsp := mathutil.MaxInt8(lowDur.Fsp, highDur.Fsp)
		stepSize := int64(math.Pow10(int(types.MaxFsp-fsp))) * int64(time.Microsecond)
		lowDur.Duration = lowDur.Duration.Round(time.Duration(stepSize))
		remaining := int64(highDur.Duration-lowDur.Duration)/stepSize + 1 - int64(exclude)
		if remaining >= maxNumStep {
			return nil
		}
		startValue := int64(lowDur.Duration)
		if lowExclude {
			startValue += stepSize
		}
		values := make([]types.Datum, 0, remaining)
		for i := int64(0); i < remaining; i++ {
			values = append(values, types.NewDurationDatum(types.Duration{Duration: time.Duration(startValue + i*stepSize), Fsp: fsp}))
		}
		return values
	case types.KindMysqlTime:
		lowTime, highTime := low.GetMysqlTime(), high.GetMysqlTime()
		if lowTime.Type() != highTime.Type() {
			return nil
		}
		fsp := mathutil.MaxInt8(lowTime.Fsp(), highTime.Fsp())
		var stepSize int64
		sc := &stmtctx.StatementContext{TimeZone: time.UTC}
		if lowTime.Type() == mysql.TypeDate {
			stepSize = 24 * int64(time.Hour)
			lowTime.SetCoreTime(types.FromDate(lowTime.Year(), lowTime.Month(), lowTime.Day(), 0, 0, 0, 0))
		} else {
			var err error
			lowTime, err = lowTime.RoundFrac(sc, fsp)
			if err != nil {
				return nil
			}
			stepSize = int64(math.Pow10(int(types.MaxFsp-fsp))) * int64(time.Microsecond)
		}
		remaining := int64(highTime.Sub(sc, &lowTime).Duration)/stepSize + 1 - int64(exclude)
		if remaining >= maxNumStep {
			return nil
		}
		startValue := lowTime
		var err error
		if lowExclude {
			startValue, err = lowTime.Add(sc, types.Duration{Duration: time.Duration(stepSize), Fsp: fsp})
			if err != nil {
				return nil
			}
		}
		values := make([]types.Datum, 0, remaining)
		for i := int64(0); i < remaining; i++ {
			value, err := startValue.Add(sc, types.Duration{Duration: time.Duration(i * stepSize), Fsp: fsp})
			if err != nil {
				return nil
			}
			values = append(values, types.NewTimeDatum(value))
		}
		return values
	}
	return nil
}
