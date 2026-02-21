// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func (d *Datum) convertToMysqlDuration(typeCtx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	var ret Datum
	switch d.k {
	case KindMysqlTime:
		dur, err := d.GetMysqlTime().ConvertToDuration()
		if err != nil {
			ret.SetMysqlDuration(dur)
			return ret, errors.Trace(err)
		}
		dur, err = dur.RoundFrac(fsp, typeCtx.Location())
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlDuration:
		dur, err := d.GetMysqlDuration().RoundFrac(fsp, typeCtx.Location())
		ret.SetMysqlDuration(dur)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindInt64, KindUint64, KindFloat32, KindFloat64, KindMysqlDecimal:
		// TODO: We need a ParseDurationFromNum to avoid the cost of converting a num to string.
		timeStr, err := d.ToString()
		if err != nil {
			return ret, errors.Trace(err)
		}
		timeNum, err := d.ToInt64(typeCtx)
		if err != nil {
			return ret, errors.Trace(err)
		}
		// For huge numbers(>'0001-00-00 00-00-00') try full DATETIME in ParseDuration.
		if timeNum > MaxDuration && timeNum < 10000000000 {
			// mysql return max in no strict sql mode.
			ret.SetMysqlDuration(Duration{Duration: MaxTime, Fsp: 0})
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		if timeNum < -MaxDuration {
			return ret, ErrWrongValue.GenWithStackByArgs(TimeStr, timeStr)
		}
		t, _, err := ParseDuration(typeCtx, timeStr, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindString, KindBytes:
		t, _, err := ParseDuration(typeCtx, d.GetString(), fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		s, err := j.Unquote()
		if err != nil {
			return ret, errors.Trace(err)
		}
		t, _, err := ParseDuration(typeCtx, s, fsp)
		ret.SetMysqlDuration(t)
		if err != nil {
			return ret, errors.Trace(err)
		}
	default:
		return invalidConv(d, tp)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlDecimal(ctx Context, target *FieldType) (Datum, error) {
	var ret Datum
	ret.SetLength(target.GetFlen())
	ret.SetFrac(target.GetDecimal())
	var dec = &MyDecimal{}
	var err error
	switch d.k {
	case KindInt64:
		dec.FromInt(d.GetInt64())
	case KindUint64:
		dec.FromUint(d.GetUint64())
	case KindFloat32, KindFloat64:
		err = dec.FromFloat64(d.GetFloat64())
	case KindString, KindBytes:
		err = dec.FromString(d.GetBytes())
	case KindMysqlDecimal:
		*dec = *d.GetMysqlDecimal()
	case KindMysqlTime:
		dec = d.GetMysqlTime().ToNumber()
	case KindMysqlDuration:
		dec = d.GetMysqlDuration().ToNumber()
	case KindMysqlEnum:
		err = dec.FromFloat64(d.GetMysqlEnum().ToNumber())
	case KindMysqlSet:
		err = dec.FromFloat64(d.GetMysqlSet().ToNumber())
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		err = err1
		dec.FromUint(val)
	case KindMysqlJSON:
		f, err1 := ConvertJSONToDecimal(ctx, d.GetMysqlJSON())
		if err1 != nil {
			return ret, errors.Trace(err1)
		}
		dec = f
	default:
		return invalidConv(d, target.GetType())
	}
	dec1, err1 := ProduceDecWithSpecifiedTp(ctx, dec, target)
	// If there is a error, dec1 may be nil.
	if dec1 != nil {
		dec = dec1
	}
	if err == nil && err1 != nil {
		err = err1
	}
	if dec.negative && mysql.HasUnsignedFlag(target.GetFlag()) {
		*dec = zeroMyDecimal
		if err == nil {
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", target.GetFlen(), target.GetDecimal()))
		}
	}
	ret.SetMysqlDecimal(dec)
	return ret, err
}

// ProduceDecWithSpecifiedTp produces a new decimal according to `flen` and `decimal`.
func ProduceDecWithSpecifiedTp(ctx Context, dec *MyDecimal, tp *FieldType) (_ *MyDecimal, err error) {
	flen, decimal := tp.GetFlen(), tp.GetDecimal()
	if flen != UnspecifiedLength && decimal != UnspecifiedLength {
		if flen < decimal {
			return nil, ErrMBiggerThanD.GenWithStackByArgs("")
		}

		var old *MyDecimal
		if int(dec.digitsFrac) > decimal {
			old = new(MyDecimal)
			*old = *dec
		}
		if int(dec.digitsFrac) != decimal {
			// Error doesn't matter because the following code will check the new decimal
			// and set error if any.
			_ = dec.Round(dec, decimal, ModeHalfUp)
		}

		_, digitsInt := dec.removeLeadingZeros()
		// After rounding decimal, the new decimal may have a longer integer length which may be longer than expected.
		// So the check of integer length must be after rounding.
		// E.g. "99.9999", flen 5, decimal 3, Round("99.9999", 3, ModelHalfUp) -> "100.000".
		if flen-decimal < digitsInt {
			// Integer length is longer, choose the max or min decimal.
			dec = NewMaxOrMinDec(dec.IsNegative(), flen, decimal)
			// select cast(111 as decimal(1)) causes a warning in MySQL.
			err = ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%d, %d)", flen, decimal))
		} else if old != nil && dec.Compare(old) != 0 {
			ctx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs("DECIMAL", old))
		}
	}

	unsigned := mysql.HasUnsignedFlag(tp.GetFlag())
	if unsigned && dec.IsNegative() {
		dec = dec.FromUint(0)
	}
	return dec, err
}

// ConvertToMysqlYear converts a datum to MySQLYear.
func (d *Datum) ConvertToMysqlYear(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret    Datum
		y      int64
		err    error
		adjust bool
	)
	switch d.k {
	case KindString, KindBytes:
		s := d.GetString()
		trimS := strings.TrimSpace(s)
		y, err = StrToInt(ctx, trimS, false)
		if err != nil {
			ret.SetInt64(0)
			return ret, errors.Trace(err)
		}
		// condition:
		// parsed to 0, not a string of length 4, the first valid char is a 0 digit
		if len(s) != 4 && y == 0 && strings.HasPrefix(trimS, "0") {
			adjust = true
		}
	case KindMysqlTime:
		y = int64(d.GetMysqlTime().Year())
	case KindMysqlDuration:
		y, err = d.GetMysqlDuration().ConvertToYear(ctx)
	case KindMysqlJSON:
		y, err = ConvertJSONToInt64(ctx, d.GetMysqlJSON(), false)
		if err != nil {
			ret.SetInt64(0)
			return ret, errors.Trace(err)
		}
	default:
		ret, err = d.convertToInt(ctx, NewFieldType(mysql.TypeLonglong))
		if err != nil {
			_, err = invalidConv(d, target.GetType())
			ret.SetInt64(0)
			return ret, err
		}
		y = ret.GetInt64()
	}

	// Duration has been adjusted in `Duration.ConvertToYear()`
	if d.k != KindMysqlDuration {
		y, err = AdjustYear(y, adjust)
	}
	ret.SetInt64(y)
	return ret, errors.Trace(err)
}

func (d *Datum) convertToMysqlBit(ctx Context, target *FieldType) (Datum, error) {
	var ret Datum
	var uintValue uint64
	var err error
	switch d.k {
	case KindString, KindBytes:
		uintValue, err = BinaryLiteral(d.b).ToInt(ctx)
	case KindInt64:
		// if input kind is int64 (signed), when trans to bit, we need to treat it as unsigned
		d.k = KindUint64
		fallthrough
	default:
		uintDatum, err1 := d.convertToUint(ctx, target)
		uintValue, err = uintDatum.GetUint64(), err1
	}
	// Avoid byte size panic, never goto this branch.
	if target.GetFlen() <= 0 || target.GetFlen() >= 128 {
		return Datum{}, errors.Trace(ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.GetFlen()))
	}
	if target.GetFlen() < 64 && uintValue >= 1<<(uint64(target.GetFlen())) {
		uintValue = (1 << (uint64(target.GetFlen()))) - 1
		err = ErrDataTooLong.GenWithStack("Data Too Long, field len %d", target.GetFlen())
	}
	byteSize := (target.GetFlen() + 7) >> 3
	ret.SetMysqlBit(NewBinaryLiteralFromUint(uintValue, byteSize))
	return ret, errors.Trace(err)
}

func (d *Datum) convertToMysqlEnum(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		e   Enum
		err error
	)
	switch d.k {
	case KindString, KindBytes, KindBinaryLiteral:
		e, err = ParseEnum(target.GetElems(), d.GetString(), target.GetCollate())
	case KindMysqlEnum:
		if d.i == 0 {
			// MySQL enum zero value has an empty string name(Enum{Name: '', Value: 0}). It is
			// different from the normal enum string value(Enum{Name: '', Value: n}, n > 0).
			e = Enum{}
		} else {
			e, err = ParseEnum(target.GetElems(), d.GetMysqlEnum().Name, target.GetCollate())
		}
	case KindMysqlSet:
		e, err = ParseEnum(target.GetElems(), d.GetMysqlSet().Name, target.GetCollate())
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(ctx, target)
		if err == nil {
			e, err = ParseEnumValue(target.GetElems(), uintDatum.GetUint64())
		} else {
			err = errors.Wrap(ErrTruncated, "convert to MySQL enum failed: "+err.Error())
		}
	}
	ret.SetMysqlEnum(e, target.GetCollate())
	return ret, err
}

func (d *Datum) convertToMysqlSet(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		s   Set
		err error
	)
	switch d.k {
	case KindString, KindBytes, KindBinaryLiteral:
		s, err = ParseSet(target.GetElems(), d.GetString(), target.GetCollate())
	case KindMysqlEnum:
		s, err = ParseSet(target.GetElems(), d.GetMysqlEnum().Name, target.GetCollate())
	case KindMysqlSet:
		s, err = ParseSet(target.GetElems(), d.GetMysqlSet().Name, target.GetCollate())
	case KindVectorFloat32:
		return invalidConv(d, mysql.TypeSet)
	default:
		var uintDatum Datum
		uintDatum, err = d.convertToUint(ctx, target)
		if err == nil {
			s, err = ParseSetValue(target.GetElems(), uintDatum.GetUint64())
		}
	}
	if err != nil {
		err = errors.Wrap(ErrTruncated, "convert to MySQL set failed: "+err.Error())
	}
	ret.SetMysqlSet(s, target.GetCollate())
	return ret, err
}

func (d *Datum) convertToMysqlJSON(_ *FieldType) (ret Datum, err error) {
	switch d.k {
	case KindString, KindBytes:
		var j BinaryJSON
		if j, err = ParseBinaryJSONFromString(d.GetString()); err == nil {
			ret.SetMysqlJSON(j)
		}
	case KindMysqlSet, KindMysqlEnum:
		var j BinaryJSON
		var s string
		if s, err = d.ToString(); err == nil {
			if j, err = ParseBinaryJSONFromString(s); err == nil {
				ret.SetMysqlJSON(j)
			}
		}
	case KindInt64:
		i64 := d.GetInt64()
		ret.SetMysqlJSON(CreateBinaryJSON(i64))
	case KindUint64:
		u64 := d.GetUint64()
		ret.SetMysqlJSON(CreateBinaryJSON(u64))
	case KindFloat32, KindFloat64:
		f64 := d.GetFloat64()
		ret.SetMysqlJSON(CreateBinaryJSON(f64))
	case KindMysqlDecimal:
		var f64 float64
		if f64, err = d.GetMysqlDecimal().ToFloat64(); err == nil {
			ret.SetMysqlJSON(CreateBinaryJSON(f64))
		}
	case KindMysqlJSON:
		ret = *d
	case KindMysqlTime:
		tm := d.GetMysqlTime()
		ret.SetMysqlJSON(CreateBinaryJSON(tm))
	case KindMysqlDuration:
		dur := d.GetMysqlDuration()
		ret.SetMysqlJSON(CreateBinaryJSON(dur))
	case KindBinaryLiteral:
		err = ErrInvalidJSONCharset.GenWithStackByArgs(charset.CharsetBin)
	default:
		var s string
		if s, err = d.ToString(); err == nil {
			// TODO: fix precision of MysqlTime. For example,
			// On MySQL 5.7 CAST(NOW() AS JSON) -> "2011-11-11 11:11:11.111111",
			// But now we can only return "2011-11-11 11:11:11".
			ret.SetMysqlJSON(CreateBinaryJSON(s))
		}
	}
	return ret, errors.Trace(err)
}

func (d *Datum) convertToVectorFloat32(_ Context, target *FieldType) (ret Datum, err error) {
	switch d.k {
	case KindVectorFloat32:
		v := d.GetVectorFloat32()
		if err = v.CheckDimsFitColumn(target.GetFlen()); err != nil {
			return ret, errors.Trace(err)
		}
		ret = *d
	case KindString, KindBytes:
		var v VectorFloat32
		if v, err = ParseVectorFloat32(d.GetString()); err != nil {
			return ret, errors.Trace(err)
		}
		if err = v.CheckDimsFitColumn(target.GetFlen()); err != nil {
			return ret, errors.Trace(err)
		}
		ret.SetVectorFloat32(v)
	default:
		return invalidConv(d, mysql.TypeTiDBVectorFloat32)
	}
	return ret, errors.Trace(err)
}
