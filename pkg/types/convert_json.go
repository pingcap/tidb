// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package types

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/hack"
)

// ConvertJSONToInt64 casts JSON into int64.
func ConvertJSONToInt64(ctx Context, j BinaryJSON, unsigned bool) (int64, error) {
	return ConvertJSONToInt(ctx, j, unsigned, mysql.TypeLonglong)
}

// ConvertJSONToInt casts JSON into int by type.
func ConvertJSONToInt(ctx Context, j BinaryJSON, unsigned bool, tp byte) (int64, error) {
	switch j.TypeCode {
	case JSONTypeCodeObject, JSONTypeCodeArray, JSONTypeCodeOpaque, JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp, JSONTypeCodeDuration:
		return 0, ctx.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", j.String()))
	case JSONTypeCodeLiteral:
		switch j.Value[0] {
		case JSONLiteralFalse:
			return 0, nil
		case JSONLiteralNil:
			return 0, ctx.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("INTEGER", j.String()))
		default:
			return 1, nil
		}
	case JSONTypeCodeInt64:
		i := j.GetInt64()
		if unsigned {
			uBound := IntegerUnsignedUpperBound(tp)
			u, err := ConvertIntToUint(ctx.Flags(), i, uBound, tp)
			return int64(u), err
		}

		lBound := IntegerSignedLowerBound(tp)
		uBound := IntegerSignedUpperBound(tp)
		return ConvertIntToInt(i, lBound, uBound, tp)
	case JSONTypeCodeUint64:
		u := j.GetUint64()
		if unsigned {
			uBound := IntegerUnsignedUpperBound(tp)
			u, err := ConvertUintToUint(u, uBound, tp)
			return int64(u), err
		}

		uBound := IntegerSignedUpperBound(tp)
		return ConvertUintToInt(u, uBound, tp)
	case JSONTypeCodeFloat64:
		f := j.GetFloat64()
		if !unsigned {
			lBound := IntegerSignedLowerBound(tp)
			uBound := IntegerSignedUpperBound(tp)
			u, e := ConvertFloatToInt(f, lBound, uBound, tp)
			return u, e
		}
		bound := IntegerUnsignedUpperBound(tp)
		u, err := ConvertFloatToUint(ctx.Flags(), f, bound, tp)
		return int64(u), err
	case JSONTypeCodeString:
		str := string(hack.String(j.GetString()))
		// The behavior of casting json string as an integer is consistent with casting a string as an integer.
		// See the `builtinCastStringAsIntSig` in `expression` pkg. The only difference is that this function
		// doesn't append any warning. This behavior is compatible with MySQL.
		isNegative := len(str) > 1 && str[0] == '-'
		if !isNegative {
			r, err := StrToUint(ctx, str, false)
			return int64(r), err
		}

		return StrToInt(ctx, str, false)
	}
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToFloat casts JSON into float64.
func ConvertJSONToFloat(ctx Context, j BinaryJSON) (float64, error) {
	switch j.TypeCode {
	case JSONTypeCodeObject, JSONTypeCodeArray, JSONTypeCodeOpaque, JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp, JSONTypeCodeDuration:
		return 0, ctx.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("FLOAT", j.String()))
	case JSONTypeCodeLiteral:
		switch j.Value[0] {
		case JSONLiteralFalse:
			return 0, nil
		case JSONLiteralNil:
			return 0, ctx.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("FLOAT", j.String()))
		default:
			return 1, nil
		}
	case JSONTypeCodeInt64:
		return float64(j.GetInt64()), nil
	case JSONTypeCodeUint64:
		return float64(j.GetUint64()), nil
	case JSONTypeCodeFloat64:
		return j.GetFloat64(), nil
	case JSONTypeCodeString:
		str := string(hack.String(j.GetString()))
		return StrToFloat(ctx, str, false)
	}
	return 0, errors.New("Unknown type code in JSON")
}

// ConvertJSONToDecimal casts JSON into decimal.
func ConvertJSONToDecimal(ctx Context, j BinaryJSON) (*MyDecimal, error) {
	var err error = nil
	res := new(MyDecimal)
	switch j.TypeCode {
	case JSONTypeCodeObject, JSONTypeCodeArray, JSONTypeCodeOpaque, JSONTypeCodeDate, JSONTypeCodeDatetime, JSONTypeCodeTimestamp, JSONTypeCodeDuration:
		err = ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", j.String())
	case JSONTypeCodeLiteral:
		switch j.Value[0] {
		case JSONLiteralFalse:
			res = res.FromInt(0)
		case JSONLiteralNil:
			err = ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", j.String())
		default:
			res = res.FromInt(1)
		}
	case JSONTypeCodeInt64:
		res = res.FromInt(j.GetInt64())
	case JSONTypeCodeUint64:
		res = res.FromUint(j.GetUint64())
	case JSONTypeCodeFloat64:
		err = res.FromFloat64(j.GetFloat64())
	case JSONTypeCodeString:
		err = res.FromString(j.GetString())
	}
	err = ctx.HandleTruncate(err)
	if err != nil {
		return res, errors.Trace(err)
	}
	return res, errors.Trace(err)
}

// getValidFloatPrefix gets prefix of string which can be successfully parsed as float.
func getValidFloatPrefix(ctx Context, s string, isFuncCast bool) (valid string, err error) {
	if isFuncCast && s == "" {
		return "0", nil
	}

	var (
		sawDot   bool
		sawDigit bool
		validLen int
		eIdx     = -1
	)
	for i := range len(s) {
		c := s[i]
		if c == '+' || c == '-' {
			if i != 0 && i != eIdx+1 { // "1e+1" is valid.
				break
			}
		} else if c == '.' {
			if sawDot || eIdx > 0 { // "1.1." or "1e1.1"
				break
			}
			sawDot = true
			if sawDigit { // "123." is valid.
				validLen = i + 1
			}
		} else if c == 'e' || c == 'E' {
			if !sawDigit { // "+.e"
				break
			}
			if eIdx != -1 { // "1e5e"
				break
			}
			eIdx = i
			if i+1 == len(s) {
				// ParseFloat doesn't accept 'e' as last char, MySQL does.
				return s[:i], nil
			}
		} else if c == '\u0000' {
			s = s[:validLen]
			break
		} else if c < '0' || c > '9' {
			break
		} else {
			sawDigit = true
			validLen = i + 1
		}
	}
	valid = s[:validLen]
	if valid == "" {
		valid = "0"
	}
	if validLen == 0 || validLen != len(s) {
		err = errors.Trace(ctx.HandleTruncate(ErrTruncatedWrongVal.GenWithStackByArgs("DOUBLE", s)))
	}
	return valid, err
}

// ToString converts an interface to a string.
func ToString(value any) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case Time:
		return v.String(), nil
	case Duration:
		return v.String(), nil
	case *MyDecimal:
		return v.String(), nil
	case BinaryLiteral:
		return v.ToString(), nil
	case Enum:
		return v.String(), nil
	case Set:
		return v.String(), nil
	case BinaryJSON:
		return v.String(), nil
	default:
		return "", errors.Errorf("cannot convert %v(type %T) to string", value, value)
	}
}
