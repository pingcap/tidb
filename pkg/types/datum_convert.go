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

