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
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ConvertTo converts a datum to the target field type.
// change this method need sync modification to type2Kind in rowcodec/types.go
func (d *Datum) ConvertTo(ctx Context, target *FieldType) (Datum, error) {
	if d.k == KindNull {
		return Datum{}, nil
	}
	switch target.GetType() { // TODO: implement mysql types convert when "CAST() AS" syntax are supported.
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		unsigned := mysql.HasUnsignedFlag(target.GetFlag())
		if unsigned {
			return d.convertToUint(ctx, target)
		}
		return d.convertToInt(ctx, target)
	case mysql.TypeFloat, mysql.TypeDouble:
		return d.convertToFloat(ctx, target)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		return d.convertToString(ctx, target)
	case mysql.TypeTimestamp:
		return d.convertToMysqlTimestamp(ctx, target)
	case mysql.TypeDatetime, mysql.TypeDate:
		return d.convertToMysqlTime(ctx, target)
	case mysql.TypeDuration:
		return d.convertToMysqlDuration(ctx, target)
	case mysql.TypeNewDecimal:
		return d.convertToMysqlDecimal(ctx, target)
	case mysql.TypeYear:
		return d.ConvertToMysqlYear(ctx, target)
	case mysql.TypeEnum:
		return d.convertToMysqlEnum(ctx, target)
	case mysql.TypeBit:
		return d.convertToMysqlBit(ctx, target)
	case mysql.TypeSet:
		return d.convertToMysqlSet(ctx, target)
	case mysql.TypeJSON:
		return d.convertToMysqlJSON(target)
	case mysql.TypeTiDBVectorFloat32:
		return d.convertToVectorFloat32(ctx, target)
	case mysql.TypeNull:
		return Datum{}, nil
	default:
		panic("should never happen")
	}
}

func (d *Datum) convertToFloat(ctx Context, target *FieldType) (Datum, error) {
	var (
		f   float64
		ret Datum
		err error
	)
	switch d.k {
	case KindNull:
		return ret, nil
	case KindInt64:
		f = float64(d.GetInt64())
	case KindUint64:
		f = float64(d.GetUint64())
	case KindFloat32, KindFloat64:
		f = d.GetFloat64()
	case KindString, KindBytes:
		f, err = StrToFloat(ctx, d.GetString(), false)
	case KindMysqlTime:
		f, err = d.GetMysqlTime().ToNumber().ToFloat64()
	case KindMysqlDuration:
		f, err = d.GetMysqlDuration().ToNumber().ToFloat64()
	case KindMysqlDecimal:
		f, err = d.GetMysqlDecimal().ToFloat64()
	case KindMysqlSet:
		f = d.GetMysqlSet().ToNumber()
	case KindMysqlEnum:
		f = d.GetMysqlEnum().ToNumber()
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := d.GetBinaryLiteral().ToInt(ctx)
		f, err = float64(val), err1
	case KindMysqlJSON:
		f, err = ConvertJSONToFloat(ctx, d.GetMysqlJSON())
	default:
		return invalidConv(d, target.GetType())
	}
	f, err1 := ProduceFloatWithSpecifiedTp(f, target)
	if err == nil && err1 != nil {
		err = err1
	}
	if target.GetType() == mysql.TypeFloat {
		ret.SetFloat32(float32(f))
	} else {
		ret.SetFloat64(f)
	}
	return ret, errors.Trace(err)
}

// ProduceFloatWithSpecifiedTp produces a new float64 according to `flen` and `decimal`.
func ProduceFloatWithSpecifiedTp(f float64, target *FieldType) (_ float64, err error) {
	if math.IsNaN(f) {
		return 0, overflow(f, target.GetType())
	}
	if math.IsInf(f, 0) {
		return f, overflow(f, target.GetType())
	}
	// For float and following double type, we will only truncate it for float(M, D) format.
	// If no D is set, we will handle it like origin float whether M is set or not.
	if target.GetFlen() != UnspecifiedLength && target.GetDecimal() != UnspecifiedLength {
		f, err = TruncateFloat(f, target.GetFlen(), target.GetDecimal())
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) && f < 0 {
		return 0, overflow(f, target.GetType())
	}

	if err != nil {
		// We must return the error got from TruncateFloat after checking whether the target is unsigned to make sure
		// the returned float is not negative when the target type is unsigned.
		return f, errors.Trace(err)
	}

	if target.GetType() == mysql.TypeFloat && (f > math.MaxFloat32 || f < -math.MaxFloat32) {
		if f > 0 {
			return math.MaxFloat32, overflow(f, target.GetType())
		}
		return -math.MaxFloat32, overflow(f, target.GetType())
	}
	return f, errors.Trace(err)
}

func (d *Datum) convertToString(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		s   string
		err error
	)
	switch d.k {
	case KindInt64:
		s = strconv.FormatInt(d.GetInt64(), 10)
	case KindUint64:
		s = strconv.FormatUint(d.GetUint64(), 10)
	case KindFloat32:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 32)
	case KindFloat64:
		s = strconv.FormatFloat(d.GetFloat64(), 'f', -1, 64)
	case KindString, KindBytes:
		fromBinary := d.Collation() == charset.CollationBin
		toBinary := target.GetCharset() == charset.CharsetBin
		if fromBinary && toBinary {
			s = d.GetString()
		} else if fromBinary {
			s, err = d.GetBinaryStringDecoded(ctx.Flags(), target.GetCharset())
		} else if toBinary {
			s = d.GetBinaryStringEncoded()
		} else {
			s, err = d.GetStringWithCheck(ctx.Flags(), target.GetCharset())
		}
	case KindMysqlTime:
		s = d.GetMysqlTime().String()
	case KindMysqlDuration:
		s = d.GetMysqlDuration().String()
	case KindMysqlDecimal:
		s = d.GetMysqlDecimal().String()
	case KindMysqlEnum:
		s = d.GetMysqlEnum().String()
	case KindMysqlSet:
		s = d.GetMysqlSet().String()
	case KindBinaryLiteral:
		s, err = d.GetBinaryStringDecoded(ctx.Flags(), target.GetCharset())
	case KindMysqlBit:
		// https://github.com/pingcap/tidb/issues/31124.
		// Consider converting to uint first.
		val, err := d.GetBinaryLiteral().ToInt(ctx)
		// The length of BIT is limited to 64, so this function will never fail / truncated.
		intest.AssertNoError(err)
		if err != nil {
			s = d.GetBinaryLiteral().ToString()
		} else {
			s = strconv.FormatUint(val, 10)
		}
	case KindMysqlJSON:
		s = d.GetMysqlJSON().String()
	case KindVectorFloat32:
		s = d.GetVectorFloat32().String()
	default:
		return invalidConv(d, target.GetType())
	}
	if err == nil {
		s, err = ProduceStrWithSpecifiedTp(s, target, ctx, true)
	}
	ret.SetString(s, target.GetCollate())
	if target.GetCharset() == charset.CharsetBin {
		ret.k = KindBytes
	}
	return ret, errors.Trace(err)
}

// ProduceStrWithSpecifiedTp produces a new string according to `flen` and `chs`. Param `padZero` indicates
// whether we should pad `\0` for `binary(flen)` type.
func ProduceStrWithSpecifiedTp(s string, tp *FieldType, ctx Context, padZero bool) (_ string, err error) {
	flen, chs := tp.GetFlen(), tp.GetCharset()
	if flen >= 0 {
		// overflowed stores the part of the string that is out of the length constraint, it is later checked to see if the
		// overflowed part is all whitespaces
		var overflowed string
		var characterLen int
		var needCalculateLen bool

		// For  mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob(defined in tidb)
		// and tinytext, text, mediumtext, longtext(not explicitly defined in tidb, corresponding to blob(s) in tidb) flen is the store length limit regardless of charset.
		if chs != charset.CharsetBin {
			switch tp.GetType() {
			case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
				characterLen = len(s)
				// We need to truncate the value to a proper length that contains complete word.
				if characterLen > flen {
					var r rune
					var size int
					var tempStr string
					var truncateLen int
					// Find the truncate position.
					for truncateLen = flen; truncateLen > 0; truncateLen-- {
						tempStr = truncateStr(s, truncateLen)
						r, size = utf8.DecodeLastRuneInString(tempStr)
						if r == utf8.RuneError && size == 0 {
							// Empty string
							continue
						} else if r == utf8.RuneError && size == 1 {
							// Invalid string
							continue
						}
						// Get the truncate position
						break
					}
					overflowed = s[truncateLen:]
					s = truncateStr(s, truncateLen)
				}
			default:
				if len(s) > flen {
					characterLen = utf8.RuneCountInString(s)
					if characterLen > flen {
						// 1. If len(s) is 0 and flen is 0, truncateLen will be 0, don't truncate s.
						//    CREATE TABLE t (a char(0));
						//    INSERT INTO t VALUES (``);
						// 2. If len(s) is 10 and flen is 0, truncateLen will be 0 too, but we still need to truncate s.
						//    SELECT 1, CAST(1234 AS CHAR(0));
						// So truncateLen is not a suitable variable to determine to do truncate or not.
						var runeCount int
						var truncateLen int
						for i := range s {
							if runeCount == flen {
								truncateLen = i
								break
							}
							runeCount++
						}
						overflowed = s[truncateLen:]
						s = truncateStr(s, truncateLen)
					} else {
						needCalculateLen = true
					}
				}
			}
		} else if len(s) > flen {
			characterLen = len(s)
			overflowed = s[flen:]
			s = truncateStr(s, flen)
		}

		if len(overflowed) != 0 {
			trimmed := strings.TrimRight(overflowed, " \t\n\r")
			if len(trimmed) == 0 && !IsBinaryStr(tp) && IsTypeChar(tp.GetType()) {
				if tp.GetType() == mysql.TypeVarchar {
					if needCalculateLen {
						characterLen = utf8.RuneCountInString(s)
					}
					ctx.AppendWarning(ErrTruncated.FastGen("Data truncated, field len %d, data len %d", flen, characterLen))
				}
			} else {
				if needCalculateLen {
					characterLen = utf8.RuneCountInString(s)
				}
				err = ErrDataTooLong.FastGen("Data Too Long, field len %d, data len %d", flen, characterLen)
			}
		}

		if tp.GetType() == mysql.TypeString && IsBinaryStr(tp) && len(s) < flen && padZero {
			padding := make([]byte, flen-len(s))
			s = string(append([]byte(s), padding...))
		}
	}
	return s, errors.Trace(ctx.HandleTruncate(err))
}

func (d *Datum) convertToInt(ctx Context, target *FieldType) (Datum, error) {
	i64, err := d.toSignedInteger(ctx, target.GetType())
	return NewIntDatum(i64), errors.Trace(err)
}

func (d *Datum) convertToUint(ctx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	upperBound := IntegerUnsignedUpperBound(tp)
	var (
		val uint64
		err error
		ret Datum
	)
	switch d.k {
	case KindInt64:
		val, err = ConvertIntToUint(ctx.Flags(), d.GetInt64(), upperBound, tp)
	case KindUint64:
		val, err = ConvertUintToUint(d.GetUint64(), upperBound, tp)
	case KindFloat32, KindFloat64:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetFloat64(), upperBound, tp)
	case KindString, KindBytes:
		var err1 error
		val, err1 = StrToUint(ctx, d.GetString(), false)
		val, err = ConvertUintToUint(val, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlTime:
		dec := d.GetMysqlTime().ToNumber()
		err = dec.Round(dec, 0, ModeHalfUp)
		ival, err1 := dec.ToInt()
		if err == nil {
			err = err1
		}
		val, err1 = ConvertIntToUint(ctx.Flags(), ival, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlDuration:
		dec := d.GetMysqlDuration().ToNumber()
		err = dec.Round(dec, 0, ModeHalfUp)
		var err1 error
		val, err1 = ConvertDecimalToUint(dec, upperBound, tp)
		if err == nil {
			err = err1
		}
	case KindMysqlDecimal:
		val, err = ConvertDecimalToUint(d.GetMysqlDecimal(), upperBound, tp)
	case KindMysqlEnum:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetMysqlEnum().ToNumber(), upperBound, tp)
	case KindMysqlSet:
		val, err = ConvertFloatToUint(ctx.Flags(), d.GetMysqlSet().ToNumber(), upperBound, tp)
	case KindBinaryLiteral, KindMysqlBit:
		val, err = d.GetBinaryLiteral().ToInt(ctx)
		if err == nil {
			val, err = ConvertUintToUint(val, upperBound, tp)
		}
	case KindMysqlJSON:
		var i64 int64
		i64, err = ConvertJSONToInt(ctx, d.GetMysqlJSON(), true, tp)
		val = uint64(i64)
	default:
		return invalidConv(d, target.GetType())
	}
	ret.SetUint64(val)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlTimestamp(ctx Context, target *FieldType) (Datum, error) {
	var (
		ret Datum
		t   Time
		err error
	)
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	switch d.k {
	case KindMysqlTime:
		t, err = d.GetMysqlTime().Convert(ctx, target.GetType())
		if err != nil {
			// t might be an invalid Timestamp, but should still be comparable, since same representation (KindMysqlTime)
			ret.SetMysqlTime(t)
			return ret, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimestampStr, t.String()))
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(ctx, mysql.TypeTimestamp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindString, KindBytes:
		t, err = ParseTime(ctx, d.GetString(), mysql.TypeTimestamp, fsp)
	case KindInt64:
		t, err = ParseTimeFromNum(ctx, d.GetInt64(), mysql.TypeTimestamp, fsp)
	case KindMysqlDecimal:
		t, err = ParseTimeFromFloatString(ctx, d.GetMysqlDecimal().String(), mysql.TypeTimestamp, fsp)
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(ctx, s, mysql.TypeTimestamp, fsp)
	default:
		return invalidConv(d, mysql.TypeTimestamp)
	}
	t.SetType(mysql.TypeTimestamp)
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

func (d *Datum) convertToMysqlTime(ctx Context, target *FieldType) (Datum, error) {
	tp := target.GetType()
	fsp := DefaultFsp
	if target.GetDecimal() != UnspecifiedLength {
		fsp = target.GetDecimal()
	}
	var (
		ret Datum
		t   Time
		err error
	)
	switch d.k {
	case KindMysqlTime:
		t, err = d.GetMysqlTime().Convert(ctx, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDuration:
		t, err = d.GetMysqlDuration().ConvertToTime(ctx, tp)
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, errors.Trace(err)
		}
		t, err = t.RoundFrac(ctx, fsp)
	case KindMysqlDecimal:
		t, err = ParseTimeFromFloatString(ctx, d.GetMysqlDecimal().String(), tp, fsp)
	case KindString, KindBytes:
		t, err = ParseTime(ctx, d.GetString(), tp, fsp)
	case KindInt64:
		t, err = ParseTimeFromNum(ctx, d.GetInt64(), tp, fsp)
	case KindUint64:
		intOverflow64 := d.GetInt64() < 0
		if intOverflow64 {
			uNum := strconv.FormatUint(d.GetUint64(), 10)
			t, err = ZeroDate, ErrWrongValue.GenWithStackByArgs(TimeStr, uNum)
		} else {
			t, err = ParseTimeFromNum(ctx, d.GetInt64(), tp, fsp)
		}
	case KindMysqlJSON:
		j := d.GetMysqlJSON()
		var s string
		s, err = j.Unquote()
		if err != nil {
			ret.SetMysqlTime(t)
			return ret, err
		}
		t, err = ParseTime(ctx, s, tp, fsp)
	default:
		return invalidConv(d, tp)
	}
	if tp == mysql.TypeDate {
		// Truncate hh:mm:ss part if the type is Date.
		t.SetCoreTime(FromDate(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0))
	}
	ret.SetMysqlTime(t)
	if err != nil {
		return ret, errors.Trace(err)
	}
	return ret, nil
}

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
