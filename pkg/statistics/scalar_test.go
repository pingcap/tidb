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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
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

func TestTimestampScalar(t *testing.T) {
	timestamp := func(year, month, day, hour, minute, second, microsecond int) types.Datum {
		return types.NewTimeDatum(types.NewTime(types.FromDate(year, month, day, hour, minute, second, microsecond), mysql.TypeTimestamp, types.MaxFsp))
	}
	nearMax := func(microsecond int) types.Datum { return timestamp(2038, 1, 19, 3, 14, 7, microsecond) }
	t.Run("exact microseconds and boundaries", func(t *testing.T) {
		cases := []struct {
			value types.Datum
			want  float64
		}{
			{timestamp(0, 0, 0, 0, 0, 0, 0), 999999},
			{timestamp(1969, 12, 31, 23, 59, 59, 999999), 999999},
			{timestamp(1970, 1, 1, 0, 0, 0, 999999), 999999},
			{types.NewTimeDatum(types.MinTimestamp), 1000000},
			{timestamp(2000, 1, 1, 0, 0, 0, 123456), 946684800123456},
			{types.NewTimeDatum(types.MaxTimestamp), 2147483647999999},
			{timestamp(2038, 1, 19, 3, 14, 8, 0), 2147483648000000},
			{timestamp(9999, 12, 31, 23, 59, 59, 999999), 2147483648000000},
		}
		var previous float64
		for _, tc := range cases {
			got := convertDatumToScalar(&tc.value, 0)
			require.Equal(t, tc.want, got, tc.value.GetMysqlTime().String())
			require.GreaterOrEqual(t, got, previous)
			previous = got
		}
		for us := 999980; us <= 999999; us++ {
			value := nearMax(us)
			require.Equal(t, float64(2147483647000000+int64(us)), convertDatumToScalar(&value, 0))
		}
	})
	t.Run("invalid calendar order", func(t *testing.T) {
		for _, tc := range []struct{ before, invalid, after types.Datum }{
			{timestamp(2016, 12, 31, 23, 59, 59, 999999), timestamp(2017, 0, 15, 12, 0, 0, 0), timestamp(2017, 1, 1, 0, 0, 0, 0)},
			{timestamp(2017, 2, 28, 23, 59, 59, 999999), timestamp(2017, 2, 30, 0, 0, 0, 0), timestamp(2017, 3, 1, 0, 0, 0, 0)},
			{timestamp(2017, 2, 28, 23, 59, 59, 999999), timestamp(2017, 3, 0, 12, 0, 0, 0), timestamp(2017, 3, 1, 0, 0, 0, 0)},
			{timestamp(2016, 2, 29, 23, 59, 59, 999999), timestamp(2016, 2, 30, 12, 0, 0, 0), timestamp(2016, 3, 1, 0, 0, 0, 0)},
		} {
			// Ranger keeps the converted datum despite ErrWrongValue so that
			// impossible DATE/DATETIME bounds remain comparable (#28424).
			invalidDate := tc.invalid.GetMysqlTime()
			invalidDate.SetType(mysql.TypeDatetime)
			require.NoError(t, invalidDate.Check(UTCWithAllowInvalidDateCtx))
			source := types.NewTimeDatum(invalidDate)
			converted, err := source.ConvertTo(UTCWithAllowInvalidDateCtx, types.NewFieldType(mysql.TypeTimestamp))
			require.Error(t, err)
			require.Equal(t, mysql.TypeTimestamp, converted.GetMysqlTime().Type())
			tc.invalid = converted
			before := convertDatumToScalar(&tc.before, 0)
			invalid := convertDatumToScalar(&tc.invalid, 0)
			after := convertDatumToScalar(&tc.after, 0)
			require.Equal(t, before, invalid)
			require.Equal(t, invalid+1, after)
			hg := NewHistogram(0, 100, 0, 0, types.NewFieldType(mysql.TypeTimestamp), 1, 0)
			hg.AppendBucket(&tc.before, &tc.after, 100, 0)
			hg.PreCalculateScalar()
			require.Zero(t, hg.calcFraction(0, &tc.invalid))
			require.False(t, hg.OutOfRangeShape(&tc.before, &tc.invalid, 100).Impossible)
			require.False(t, hg.OutOfRangeShape(&tc.invalid, &tc.after, 100).Impossible)
		}
	})
	t.Run("interpolation", func(t *testing.T) {
		lower, upper, value := nearMax(999990), nearMax(999999), nearMax(999991)
		hg := NewHistogram(0, 90, 0, 0, types.NewFieldType(mysql.TypeTimestamp), 1, 0)
		hg.AppendBucket(&lower, &upper, 90, 0)
		hg.PreCalculateScalar()
		require.Equal(t, 1.0/9, hg.calcFraction(0, &value))
		require.Equal(t, 1.0/9, calcFraction4Datums(&lower, &upper, &value))
		require.Equal(t, 10.0, hg.LessRowCount(nil, value))
	})
	t.Run("out of range", func(t *testing.T) {
		lower, upper, value := nearMax(999991), nearMax(999995), nearMax(999997)
		hg := NewHistogram(0, 100, 0, 0, types.NewFieldType(mysql.TypeTimestamp), 1, 0)
		hg.AppendBucket(&lower, &upper, 100, 0)
		hg.PreCalculateScalar()
		shape := hg.OutOfRangeShape(&upper, &value, 100)
		require.False(t, shape.Impossible)
		require.Equal(t, 0.375, shape.TotalPercent)
		require.Equal(t, 0.75, shape.MaxTotalPercent)
	})
	t.Run("zero and above maximum bounds", func(t *testing.T) {
		for _, bounds := range [][3]types.Datum{
			{timestamp(0, 0, 0, 0, 0, 0, 0), timestamp(1970, 1, 1, 0, 0, 1, 1), types.NewTimeDatum(types.MinTimestamp)},
			{nearMax(999998), timestamp(2038, 1, 19, 3, 14, 8, 0), types.NewTimeDatum(types.MaxTimestamp)},
		} {
			lower, upper, value := bounds[0], bounds[1], bounds[2]
			hg := NewHistogram(0, 100, 0, 0, types.NewFieldType(mysql.TypeTimestamp), 1, 0)
			hg.AppendBucket(&lower, &upper, 100, 0)
			hg.PreCalculateScalar()
			require.Equal(t, 0.5, hg.calcFraction(0, &value))
			require.Equal(t, 50.0, hg.LessRowCount(nil, value))
			shape := hg.OutOfRangeShape(&lower, &upper, 100)
			require.False(t, shape.Impossible)
			require.Zero(t, shape.TotalPercent)
		}
	})
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
