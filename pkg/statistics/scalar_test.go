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

package statistics

import (
	"math"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const eps = 1e-9

func getDecimal(value float64) *types.MyDecimal {
	dec := &types.MyDecimal{}
	err := dec.FromFloat64(value)
	if err != nil {
		panic(err)
	}
	return dec
}

func getDuration(value string) types.Duration {
	dur, _, _ := types.ParseDuration(types.DefaultStmtNoWarningContext, value, 0)
	return dur
}

func getTime(year, month, day int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(year, month, day, 0, 0, 0, 0), timeType, types.DefaultFsp)
	return ret
}

func getTimeStamp(hour, minute, sec int, timeType byte) types.Time {
	ret := types.NewTime(types.FromDate(2017, 1, 1, hour, minute, sec, 0), timeType, 0)
	return ret
}

func getBinaryLiteral(value string) types.BinaryLiteral {
	b, _ := types.ParseBitStr(value)
	return b
}

func getUnsignedFieldType() *types.FieldType {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.AddFlag(mysql.UnsignedFlag)
	return tp
}

func TestCalcFraction(t *testing.T) {
	tests := []struct {
		lower    types.Datum
		upper    types.Datum
		value    types.Datum
		fraction float64
		tp       *types.FieldType
	}{
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			value:    types.NewIntDatum(1),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			value:    types.NewIntDatum(4),
			fraction: 1.0,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			value:    types.NewIntDatum(-1),
			fraction: 0.0,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewUintDatum(0),
			upper:    types.NewUintDatum(4),
			value:    types.NewUintDatum(1),
			fraction: 0.25,
			tp:       getUnsignedFieldType(),
		},
		{
			lower:    types.NewFloat64Datum(0),
			upper:    types.NewFloat64Datum(4),
			value:    types.NewFloat64Datum(1),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeDouble),
		},
		{
			lower:    types.NewFloat32Datum(0),
			upper:    types.NewFloat32Datum(4),
			value:    types.NewFloat32Datum(1),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeFloat),
		},
		{
			lower:    types.NewDecimalDatum(getDecimal(0)),
			upper:    types.NewDecimalDatum(getDecimal(4)),
			value:    types.NewDecimalDatum(getDecimal(1)),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeNewDecimal),
		},
		{
			lower:    types.NewMysqlBitDatum(getBinaryLiteral("0b0")),
			upper:    types.NewMysqlBitDatum(getBinaryLiteral("0b100")),
			value:    types.NewMysqlBitDatum(getBinaryLiteral("0b1")),
			fraction: 0.5,
			tp:       types.NewFieldType(mysql.TypeBit),
		},
		{
			lower:    types.NewDurationDatum(getDuration("0:00:00")),
			upper:    types.NewDurationDatum(getDuration("4:00:00")),
			value:    types.NewDurationDatum(getDuration("1:00:00")),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeDuration),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeTimestamp)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeTimestamp)),
			value:    types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeTimestamp)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			lower:    types.NewTimeDatum(types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp)),
			upper:    types.NewTimeDatum(types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 3, 0), mysql.TypeTimestamp, types.DefaultFsp)),
			value:    types.NewTimeDatum(types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 2, 0), mysql.TypeTimestamp, types.DefaultFsp)),
			fraction: 0.5,
			tp:       types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			lower:    types.NewTimeDatum(types.MaxTimestamp),
			upper:    types.NewTimeDatum(types.NewTime(types.FromDate(9999, 12, 31, 23, 59, 59, 0), mysql.TypeTimestamp, types.DefaultFsp)),
			value:    types.NewTimeDatum(types.NewTime(types.FromDate(9999, 12, 31, 23, 59, 59, 0), mysql.TypeTimestamp, types.DefaultFsp)),
			fraction: 1.0,
			tp:       types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDatetime)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeDatetime)),
			value:    types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeDatetime)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(mysql.TypeDatetime),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeDate)),
			value:    types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeDate)),
			fraction: 0.34444444444444444,
			tp:       types.NewFieldType(mysql.TypeDate),
		},
		{
			lower:    types.NewStringDatum("aasad"),
			upper:    types.NewStringDatum("addad"),
			value:    types.NewStringDatum("abfsd"),
			fraction: 0.32280253984063745,
			tp:       types.NewFieldType(mysql.TypeString),
		},
		{
			lower:    types.NewBytesDatum([]byte("aasad")),
			upper:    types.NewBytesDatum([]byte("asdff")),
			value:    types.NewBytesDatum([]byte("abfsd")),
			fraction: 0.0529216802217269,
			tp:       types.NewFieldType(mysql.TypeBlob),
		},
	}
	for _, test := range tests {
		hg := NewHistogram(0, 0, 0, 0, test.tp, 1, 0)
		hg.AppendBucket(&test.lower, &test.upper, 0, 0)
		hg.PreCalculateScalar()
		fraction := hg.calcFraction(0, &test.value)
		require.InDelta(t, test.fraction, fraction, eps)
	}
}

// TestConvertMysqlTimeToScalarTimestampBounds verifies that zero/before-min and
// above-max TIMESTAMP values are clamped to finite, ordered scalars and that
// the zero-TIMESTAMP path does not emit tolerated GoTime error logs.
func TestConvertMysqlTimeToScalarTimestampBounds(t *testing.T) {
	core, recorded := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(
		zap.New(core),
		&log.ZapProperties{Core: core, Level: zap.NewAtomicLevelAt(zap.InfoLevel)},
	)
	defer restore()

	datum := types.NewTimeDatum(types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp))
	scalar := convertDatumToScalar(&datum, 0)

	require.Empty(t, recorded.FilterMessage("encountered error").All())
	require.Equal(t, float64(-1), scalar)

	minTimestamp := types.NewTimeDatum(types.MinTimestamp)
	require.Equal(t, float64(0), convertDatumToScalar(&minTimestamp, 0))

	maxTimestamp := types.NewTimeDatum(types.MaxTimestamp)
	maxTimestampScalar := convertDatumToScalar(&maxTimestamp, 0)
	afterMaxTimestamp := types.NewTimeDatum(types.NewTime(types.FromDate(9999, 12, 31, 23, 59, 59, 0), mysql.TypeTimestamp, types.DefaultFsp))
	afterMaxTimestampScalar := convertDatumToScalar(&afterMaxTimestamp, 0)
	require.Greater(t, afterMaxTimestampScalar, maxTimestampScalar)
}

// TestDateTimeHistogramScalars checks calendar ordering and interpolation across
// invalid dates, unbounded ranges, and supported fractional precisions.
func TestDateTimeHistogramScalars(t *testing.T) {
	for _, tp := range []byte{mysql.TypeDate, mysql.TypeDatetime} {
		name := "date"
		if tp == mysql.TypeDatetime {
			name = "datetime"
		}
		t.Run(name, func(t *testing.T) {
			datum := func(year, month, day int) types.Datum {
				return types.NewTimeDatum(getTime(year, month, day, tp))
			}
			t.Run("calendar ordering", func(t *testing.T) {
				leapDay, march := datum(0, 2, 29), datum(0, 3, 1)
				require.Less(t, convertDatumToScalar(&leapDay, 0), convertDatumToScalar(&march, 0))
				previous := datum(0, 1, 1)
				for year := 1; year <= 9999; year++ {
					current := datum(year, 1, 1)
					require.Less(t, convertDatumToScalar(&previous, 0), convertDatumToScalar(&current, 0), "%v", current.GetMysqlTime())
					previous = current
				}
			})
			t.Run("crossing bucket", func(t *testing.T) {
				lower, upper, value := datum(2045, 7, 5), datum(2049, 5, 20), datum(2046, 6, 1)
				hg := NewHistogram(0, 100, 0, 0, types.NewFieldType(tp), 1, 0)
				hg.AppendBucket(&lower, &upper, 100, 0)
				hg.PreCalculateScalar()
				// Use calendar distances, independent of the scalar representation.
				start := time.Date(2045, 7, 5, 0, 0, 0, 0, time.UTC).Unix()
				end := time.Date(2049, 5, 20, 0, 0, 0, 0, time.UTC).Unix()
				point := time.Date(2046, 6, 1, 0, 0, 0, 0, time.UTC).Unix()
				expected := float64(point-start) / float64(end-start)
				require.InDelta(t, expected, hg.calcFraction(0, &value), eps)
				require.InDelta(t, expected, calcFraction4Datums(&lower, &upper, &value), eps)
			})
			t.Run("out of range", func(t *testing.T) {
				lower, upper := datum(2020, 1, 1), datum(2026, 12, 31)
				hg := NewHistogram(0, 2557, 0, 0, types.NewFieldType(tp), 1, 0)
				hg.AppendBucket(&lower, &upper, 2557, 1)
				left, reference := datum(2027, 6, 1), datum(2046, 12, 9)
				ctx := mock.NewContext()
				expected := hg.OutOfRangeRowCount(ctx, &left, &reference, 4018, 1461, 2557)
				require.Greater(t, expected.Est, 1.0)
				for _, right := range []types.Datum{datum(2046, 12, 10), datum(9999, 12, 31)} {
					require.NotPanics(t, func() {
						actual := hg.OutOfRangeRowCount(ctx, &left, &right, 4018, 1461, 2557)
						require.Equal(t, expected, actual)
					})
				}
			})
			t.Run("zero and invalid bounds", func(t *testing.T) {
				bounds := []types.Datum{
					datum(0, 0, 0), datum(1, 1, 1), datum(2017, 0, 0),
					datum(2017, 1, 0), datum(2017, 1, 1), datum(2017, 2, 28),
					datum(2017, 2, 29), datum(2017, 2, 30), datum(2017, 2, 31),
					datum(2017, 3, 0), datum(2017, 3, 1), datum(9999, 12, 31),
				}
				for i := 1; i < len(bounds); i++ {
					require.LessOrEqual(t, convertDatumToScalar(&bounds[i-1], 0), convertDatumToScalar(&bounds[i], 0), "%v <= %v", bounds[i-1].GetMysqlTime(), bounds[i].GetMysqlTime())
				}
			})
			t.Run("null lower bound", func(t *testing.T) {
				lower, upper := datum(2020, 1, 11), datum(2020, 1, 21)
				hg := NewHistogram(0, 100, 0, 0, types.NewFieldType(tp), 1, 0)
				hg.AppendBucket(&lower, &upper, 100, 1)
				var left types.Datum
				right := datum(2020, 1, 6)
				shape := hg.OutOfRangeShape(&left, &right, 100)
				require.False(t, shape.Impossible)
				// The predicate covers half the width of the left triangle.
				require.InDelta(t, 0.125, shape.TotalPercent, eps)
				require.InDelta(t, 0.25, shape.MaxTotalPercent, eps)
				minNotNull := types.MinNotNullDatum()
				require.Equal(t, hg.OutOfRangeShape(&minNotNull, &right, 100), shape)
				ctx := mock.NewContext()
				ctx.GetSessionVars().RiskRangeSkewRatio = 0
				estimate := hg.OutOfRangeRowCount(ctx, &left, &right, 200, 100, 100)
				require.InDelta(t, 6.25, estimate.Est, eps)
			})
		})
	}
	t.Run("fractional precision", func(t *testing.T) {
		for _, year := range []int{0, 1, 2026, 2046, 9999} {
			for fsp := 0; fsp <= types.MaxFsp; fsp++ {
				step := int(math.Pow10(types.MaxFsp - fsp))
				datum := func(offset int) types.Datum {
					return types.NewTimeDatum(types.NewTime(types.FromDate(year, 12, 31, 12, 0, offset/1e6, offset%1e6), mysql.TypeDatetime, fsp))
				}
				lower, upper, value := datum(0), datum(10*step), datum(3*step)
				hg := NewHistogram(0, 11, 0, 0, types.NewFieldType(mysql.TypeDatetime), 1, 0)
				hg.AppendBucket(&lower, &upper, 11, 1)
				hg.PreCalculateScalar()
				require.InDelta(t, 0.3, hg.calcFraction(0, &value), eps, "year=%d fsp=%d", year, fsp)
				require.InDelta(t, 0.3, calcFraction4Datums(&lower, &upper, &value), eps, "year=%d fsp=%d", year, fsp)
				// Out-of-range distances must also be taken before float rounding.
				left, right := datum(10*step), datum(15*step)
				shape := hg.OutOfRangeShape(&left, &right, 11)
				require.InDelta(t, 0.375, shape.TotalPercent, eps, "year=%d fsp=%d", year, fsp)
			}
		}
	})
}

// TestOutOfRangeRowCountZeroTimestampLowerBound verifies the end-to-end
// out-of-range estimate when a histogram bucket has a zero TIMESTAMP (clamped
// to scalar -1) as its lower bound.
func TestOutOfRangeRowCountZeroTimestampLowerBound(t *testing.T) {
	datum := types.NewTimeDatum(types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp))

	hg := NewHistogram(0, 0, 0, 0, types.NewFieldType(mysql.TypeTimestamp), 1, 0)
	upper := types.NewTimeDatum(types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 3, 0), mysql.TypeTimestamp, types.DefaultFsp))
	hg.AppendBucket(&datum, &upper, 2, 1)

	ctx := mock.NewContext()
	ctx.GetSessionVars().RiskRangeSkewRatio = 0.5
	right := types.NewTimeDatum(types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 4, 0), mysql.TypeTimestamp, types.DefaultFsp))
	estimate := hg.OutOfRangeRowCount(ctx, &upper, &right, 100, 100, 2)
	require.InDelta(t, 45.937499984687506, estimate.Est, eps)
	require.InDelta(t, 18.374999993875, estimate.MinEst, eps)
	require.InDelta(t, 73.4999999755, estimate.MaxEst, eps)
}

func TestEnumRangeValues(t *testing.T) {
	tests := []struct {
		low         types.Datum
		high        types.Datum
		lowExclude  bool
		highExclude bool
		res         string
	}{
		{
			low:         types.NewIntDatum(0),
			high:        types.NewIntDatum(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewIntDatum(math.MinInt64),
			high:        types.NewIntDatum(math.MaxInt64),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
		{
			low:         types.NewUintDatum(0),
			high:        types.NewUintDatum(5),
			lowExclude:  false,
			highExclude: true,
			res:         "(0, 1, 2, 3, 4)",
		},
		{
			low:         types.NewDurationDatum(getDuration("0:00:00")),
			high:        types.NewDurationDatum(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewDurationDatum(getDuration("0:00:00")),
			high:        types.NewDurationDatum(getDuration("0:00:05")),
			lowExclude:  false,
			highExclude: true,
			res:         "(00:00:00, 00:00:01, 00:00:02, 00:00:03, 00:00:04)",
		},
		{
			low:         types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			high:        types.NewTimeDatum(getTime(2017, 1, 5, mysql.TypeDate)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01, 2017-01-02, 2017-01-03, 2017-01-04)",
		},
		{
			low:         types.NewTimeDatum(getTimeStamp(0, 0, 0, mysql.TypeTimestamp)),
			high:        types.NewTimeDatum(getTimeStamp(0, 0, 5, mysql.TypeTimestamp)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01 00:00:00, 2017-01-01 00:00:01, 2017-01-01 00:00:02, 2017-01-01 00:00:03, 2017-01-01 00:00:04)",
		},
		{
			low:         types.NewTimeDatum(getTimeStamp(0, 0, 0, mysql.TypeDatetime)),
			high:        types.NewTimeDatum(getTimeStamp(0, 0, 5, mysql.TypeDatetime)),
			lowExclude:  false,
			highExclude: true,
			res:         "(2017-01-01 00:00:00, 2017-01-01 00:00:01, 2017-01-01 00:00:02, 2017-01-01 00:00:03, 2017-01-01 00:00:04)",
		},
		// fix issue 11610
		{
			low:         types.NewIntDatum(math.MinInt64),
			high:        types.NewIntDatum(0),
			lowExclude:  false,
			highExclude: false,
			res:         "",
		},
		{
			low:         types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			high:        types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			lowExclude:  true,
			highExclude: true,
			res:         "",
		},
	}
	for _, test := range tests {
		vals := EnumRangeValues(test.low, test.high, test.lowExclude, test.highExclude)
		str, err := types.DatumsToString(vals, true)
		require.NoError(t, err)
		require.Equal(t, test.res, str)
	}
}
