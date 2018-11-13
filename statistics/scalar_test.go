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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

const eps = 1e-9

func getDecimal(value float64) *types.MyDecimal {
	dec := &types.MyDecimal{}
	dec.FromFloat64(value)
	return dec
}

func getDuration(value string) types.Duration {
	dur, _ := types.ParseDuration(nil, value, 0)
	return dur
}

func getTime(year, month, day int, timeType byte) types.Time {
	ret := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: timeType,
		Fsp:  types.DefaultFsp}
	return ret
}

func getBinaryLiteral(value string) types.BinaryLiteral {
	b, _ := types.ParseBitStr(value)
	return b
}

func getUnsignedFieldType() *types.FieldType {
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag |= mysql.UnsignedFlag
	return tp
}

func (s *testStatisticsSuite) TestCalcFraction(c *C) {
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
			fraction: 1,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			value:    types.NewIntDatum(-1),
			fraction: 0,
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
		c.Assert(math.Abs(fraction-test.fraction) < eps, IsTrue)
	}
}

func (s *testStatisticsSuite) TestCalcRangeFraction(c *C) {
	tests := []struct {
		lower    types.Datum
		upper    types.Datum
		lInner   types.Datum
		uInner   types.Datum
		fraction float64
		tp       *types.FieldType
	}{
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			lInner:   types.NewIntDatum(1),
			uInner:   types.NewIntDatum(3),
			fraction: 0.6,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewIntDatum(0),
			upper:    types.NewIntDatum(4),
			lInner:   types.NewIntDatum(0),
			uInner:   types.NewIntDatum(4),
			fraction: 1,
			tp:       types.NewFieldType(mysql.TypeLonglong),
		},
		{
			lower:    types.NewUintDatum(0),
			upper:    types.NewUintDatum(4),
			lInner:   types.NewUintDatum(1),
			uInner:   types.NewUintDatum(1),
			fraction: 0.20,
			tp:       getUnsignedFieldType(),
		},
		{
			lower:    types.NewFloat64Datum(0),
			upper:    types.NewFloat64Datum(4),
			lInner:   types.NewFloat64Datum(1.5),
			uInner:   types.NewFloat64Datum(2.5),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeDouble),
		},
		{
			lower:    types.NewFloat32Datum(0),
			upper:    types.NewFloat32Datum(4),
			lInner:   types.NewFloat32Datum(1),
			uInner:   types.NewFloat32Datum(2),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeFloat),
		},
		{
			lower:    types.NewFloat32Datum(4),
			upper:    types.NewFloat32Datum(4),
			lInner:   types.NewFloat32Datum(4),
			uInner:   types.NewFloat32Datum(4),
			fraction: 1,
			tp:       types.NewFieldType(mysql.TypeFloat),
		},
		{
			lower:    types.NewDecimalDatum(getDecimal(0)),
			upper:    types.NewDecimalDatum(getDecimal(4)),
			lInner:   types.NewDecimalDatum(getDecimal(1)),
			uInner:   types.NewDecimalDatum(getDecimal(1.5)),
			fraction: 0.125,
			tp:       types.NewFieldType(mysql.TypeNewDecimal),
		},
		{
			lower:    types.NewMysqlBitDatum(getBinaryLiteral("0b0")),
			upper:    types.NewMysqlBitDatum(getBinaryLiteral("0b100")),
			lInner:   types.NewMysqlBitDatum(getBinaryLiteral("0b00")),
			uInner:   types.NewMysqlBitDatum(getBinaryLiteral("0b10")),
			fraction: 0.5,
			tp:       types.NewFieldType(mysql.TypeBit),
		},
		{
			lower:    types.NewDurationDatum(getDuration("0:00:00")),
			upper:    types.NewDurationDatum(getDuration("4:00:00")),
			lInner:   types.NewDurationDatum(getDuration("1:00:00")),
			uInner:   types.NewDurationDatum(getDuration("2:00:00")),
			fraction: 0.25,
			tp:       types.NewFieldType(mysql.TypeDuration),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeTimestamp)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeTimestamp)),
			lInner:   types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeTimestamp)),
			uInner:   types.NewTimeDatum(getTime(2017, 3, 1, mysql.TypeTimestamp)),
			fraction: 0.3111111111111111,
			tp:       types.NewFieldType(mysql.TypeTimestamp),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDatetime)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeDatetime)),
			lInner:   types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeDatetime)),
			uInner:   types.NewTimeDatum(getTime(2017, 3, 1, mysql.TypeDatetime)),
			fraction: 0.3111111111111111,
			tp:       types.NewFieldType(mysql.TypeDatetime),
		},
		{
			lower:    types.NewTimeDatum(getTime(2017, 1, 1, mysql.TypeDate)),
			upper:    types.NewTimeDatum(getTime(2017, 4, 1, mysql.TypeDate)),
			lInner:   types.NewTimeDatum(getTime(2017, 2, 1, mysql.TypeDate)),
			uInner:   types.NewTimeDatum(getTime(2017, 3, 1, mysql.TypeDate)),
			fraction: 0.3111111111111111,
			tp:       types.NewFieldType(mysql.TypeDate),
		},
		{
			lower:    types.NewStringDatum("aasad"),
			upper:    types.NewStringDatum("addad"),
			lInner:   types.NewStringDatum("aafsd"),
			uInner:   types.NewStringDatum("abfsd"),
			fraction: 0.3399734395750332,
			tp:       types.NewFieldType(mysql.TypeString),
		},
		{
			lower:    types.NewBytesDatum([]byte("aasad")),
			upper:    types.NewBytesDatum([]byte("asdff")),
			lInner:   types.NewBytesDatum([]byte("aafsd")),
			uInner:   types.NewBytesDatum([]byte("abfsd")),
			fraction: 0.05573675368834722,
			tp:       types.NewFieldType(mysql.TypeBlob),
		},
	}
	for i, test := range tests {
		hg := NewHistogram(0, 0, 0, 0, test.tp, 1, 0)
		hg.AppendBucket(&test.lower, &test.upper, 0, 0)
		hg.PreCalculateScalar()
		chk := chunk.New([]*types.FieldType{test.tp}, 2, 2)
		chk.AppendDatum(0, &test.lInner)
		chk.AppendDatum(0, &test.uInner)
		lInner := chk.GetRow(0)
		uInner := chk.GetRow(1)
		fraction := hg.calcRangeFraction(0, lInner, uInner)
		c.Assert(math.Abs(fraction-test.fraction) < eps, IsTrue, Commentf("Failed at #%v, need: %v, got: %v", i, test.fraction, fraction))
	}
}
