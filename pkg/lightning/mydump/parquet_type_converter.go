// Copyright 2025 PingCAP, Inc.
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

package mydump

import (
	"slices"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) (bool, error)

var zeroMyDecimal = types.MyDecimal{}

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
)

func isUnsignedParquetType(ct schema.ConvertedType) bool {
	switch ct {
	case schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Uint64:
		return true
	default:
		return false
	}
}

func isTargetType(target *model.ColumnInfo, tps ...byte) bool {
	if target == nil {
		return false
	}
	return slices.Contains(tps, target.GetType())
}

func getDecimalCheckFunc(
	decimalMeta schema.DecimalMetadata,
	target *model.ColumnInfo,
) func(*types.Datum) bool {
	if target == nil || target.GetType() != mysql.TypeNewDecimal || target.GetFlen() <= 0 || target.GetDecimal() < 0 {
		return nil
	}
	targetFlen := target.GetFlen()
	targetDecimal := target.GetDecimal()
	unsigned := mysql.HasUnsignedFlag(target.GetFlag())
	if !decimalMeta.IsSet {
		return nil
	}
	if int(decimalMeta.Scale) != targetDecimal {
		return nil
	}
	if int(decimalMeta.Precision) > targetFlen {
		return nil
	}
	return func(d *types.Datum) bool {
		if d.Kind() != types.KindMysqlDecimal {
			return false
		}
		dec := d.GetMysqlDecimal()
		if unsigned && dec.IsNegative() && !dec.IsZero() {
			return false
		}
		precision, frac := dec.PrecisionAndFrac()
		if targetFlen > 0 && precision > targetFlen {
			return false
		}
		if targetDecimal >= 0 && frac != targetDecimal {
			return false
		}
		return true
	}
}

func stringCanSkipCast(target *model.ColumnInfo) bool {
	if target == nil {
		return false
	}
	switch target.GetType() {
	case mysql.TypeVarchar, mysql.TypeVarString:
		return true
	case mysql.TypeString:
		return !types.IsBinaryStr(&target.FieldType)
	default:
		return false
	}
}

func stringCheckFunc(b []byte, targetFlen int, enc charset.Encoding) bool {
	if !enc.IsValid(b) {
		return false
	}
	if enc.Tp() == charset.EncodingTpBin {
		return targetFlen == types.UnspecifiedLength || len(b) <= targetFlen
	}
	return targetFlen == types.UnspecifiedLength || utf8.RuneCount(b) <= targetFlen
}

func resolveTemporalTarget(target *model.ColumnInfo) (tp byte, fsp int, canSkipCast bool) {
	if target == nil {
		return mysql.TypeTimestamp, types.MaxFsp, false
	}
	switch target.GetType() {
	case mysql.TypeDate:
		return mysql.TypeDate, 0, true
	case mysql.TypeDatetime:
		if target.GetDecimal() >= 0 {
			return mysql.TypeDatetime, min(target.GetDecimal(), types.MaxFsp), true
		}
		return mysql.TypeDatetime, types.MaxFsp, true
	default:
		return mysql.TypeTimestamp, types.MaxFsp, false
	}
}

var fspRoundUnit = [7]time.Duration{
	time.Second,
	100 * time.Millisecond,
	10 * time.Millisecond,
	time.Millisecond,
	100 * time.Microsecond,
	10 * time.Microsecond,
	time.Microsecond,
}

// setTemporalDatum constructs a MySQL temporal datum from a Go time value.
// It returns true if the resulting time is within MySQL's valid range [0000, 9999],
// which callers use to decide skip-cast eligibility.
func setTemporalDatum(t time.Time, d *types.Datum, loc *time.Location, isAdjustedToUTC bool, tp byte, fsp int) bool {
	if isAdjustedToUTC {
		t = t.In(loc)
	}
	if tp == mysql.TypeDate {
		t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	} else if fsp < types.MaxFsp {
		t = t.Round(fspRoundUnit[fsp])
	}
	d.SetMysqlTime(types.NewTime(types.FromGoTime(t), tp, fsp))
	return t.Year() >= 0 && t.Year() <= 9999
}

func initializeMyDecimal(d *types.Datum) *types.MyDecimal {
	// reuse existing decimal
	if d.Kind() == types.KindMysqlDecimal {
		dec := d.GetMysqlDecimal()
		*dec = zeroMyDecimal
		return dec
	}

	dec := new(types.MyDecimal)
	d.SetMysqlDecimal(dec)
	return dec
}

func setDatumFromDecimalByte(d *types.Datum, val []byte, scale int) error {
	// Typically it shouldn't happen.
	if len(val) == 0 {
		return errors.New("invalid parquet decimal byte array")
	}

	// Truncate leading zeros in two's complement representation.
	negative := (val[0] & 0x80) != 0
	start := 0
	for ; start < len(val); start++ {
		if negative && val[start] != 0xff || !negative && val[start] != 0x00 {
			break
		}
	}
	// Keep at least one byte.
	start = max(start-1, 0)
	val = val[start:]

	// If the length or scale is too large, fallback to string parsing.
	if len(val) >= maximumDecimalBytes || scale > 81 {
		s := getStringFromParquetByte(val, scale)
		d.SetBytesAsString(s, "utf8mb4_bin", 0)
		return nil
	}

	dec := initializeMyDecimal(d)
	return dec.FromParquetArray(val, scale)
}

func getStringFromParquetByte(rawBytes []byte, scale int) []byte {
	base := uint64(1_000_000_000)
	baseDigits := 9

	negative := (rawBytes[0] & 0x80) != 0
	if negative {
		for i := range rawBytes {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i]++
			if rawBytes[i] != 0 {
				break
			}
		}
	}

	var (
		s          = make([]byte, 0, 64)
		n          int
		nDigits    int
		startIndex = 0
		endIndex   = len(rawBytes)
	)

	for startIndex < endIndex && rawBytes[startIndex] == 0 {
		startIndex++
	}

	// Convert base-256 bytes to base-10 string representation.
	for startIndex < endIndex {
		var rem uint64
		for i := startIndex; i < endIndex; i++ {
			v := (rem << 8) | uint64(rawBytes[i])
			q := v / base
			rem = v % base
			rawBytes[i] = byte(q)
			if q == 0 && i == startIndex {
				startIndex++
			}
		}

		for range baseDigits {
			s = append(s, byte(48+rem%10))
			n++
			nDigits++
			rem /= 10
			if nDigits == scale {
				s = append(s, '.')
				n++
			}
			if startIndex == endIndex && rem == 0 {
				break
			}
		}
	}

	for nDigits < scale+1 {
		s = append(s, '0')
		n++
		nDigits++
		if nDigits == scale {
			s = append(s, '.')
			n++
		}
	}

	if negative {
		s = append(s, '-')
	}

	// Reverse the string.
	for i := range len(s) / 2 {
		j := len(s) - 1 - i
		s[i], s[j] = s[j], s[i]
	}

	return s
}

func setParquetDecimalFromInt64(
	unscaled int64,
	dec *types.MyDecimal,
	decimalMeta schema.DecimalMetadata,
) error {
	dec.FromInt(unscaled)

	scale := int(decimalMeta.Scale)
	if err := dec.Shift(-scale); err != nil {
		return err
	}

	return dec.Round(dec, scale, types.ModeTruncate)
}

func getBoolSetter(target *model.ColumnInfo) setter[bool] {
	skipCast := target != nil && mysql.IsIntegerType(target.GetType())
	if skipCast && mysql.HasUnsignedFlag(target.GetFlag()) {
		return func(val bool, d *types.Datum) (bool, error) {
			if val {
				d.SetUint64(1)
			} else {
				d.SetUint64(0)
			}
			return true, nil
		}
	}
	return func(val bool, d *types.Datum) (bool, error) {
		if val {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
		return skipCast, nil
	}
}

func getInt32Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int32] {
	temporalType, temporalFSP, temporalSkipCast := resolveTemporalTarget(target)
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		checkFunc := getDecimalCheckFunc(converted.decimalMeta, target)
		if checkFunc == nil {
			return func(val int32, d *types.Datum) (bool, error) {
				dec := initializeMyDecimal(d)
				return false, setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
			}
		}
		return func(val int32, d *types.Datum) (bool, error) {
			dec := initializeMyDecimal(d)
			err := setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
			if err != nil {
				return false, err
			}
			return checkFunc(d), nil
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) (bool, error) {
			// DATE is days since epoch, timezone-agnostic — no loc conversion.
			// Ref: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#date
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			// Only skip cast when target is DATE; DATE→DATETIME needs cast.
			return temporalType == mysql.TypeDate, nil
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) (bool, error) {
			t := time.UnixMilli(int64(val)).In(time.UTC)
			inRange := setTemporalDatum(t, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
			return temporalSkipCast && inRange, nil
		}
	default:
		fromUnsigned := isUnsignedParquetType(converted.converted)
		if target != nil && mysql.IsIntegerType(target.GetType()) {
			toUnsigned := mysql.HasUnsignedFlag(target.GetFlag())
			if fromUnsigned && toUnsigned {
				upperBound := types.IntegerUnsignedUpperBound(target.GetType())
				return func(val int32, d *types.Datum) (bool, error) {
					v := uint64(uint32(val))
					d.SetUint64(v)
					return v <= upperBound, nil
				}
			} else if !fromUnsigned && !toUnsigned {
				lowerBound := types.IntegerSignedLowerBound(target.GetType())
				upperBound := types.IntegerSignedUpperBound(target.GetType())
				return func(val int32, d *types.Datum) (bool, error) {
					v := int64(val)
					d.SetInt64(v)
					return v >= lowerBound && v <= upperBound, nil
				}
			}
		}
		if fromUnsigned {
			return func(val int32, d *types.Datum) (bool, error) {
				d.SetUint64(uint64(uint32(val)))
				return false, nil
			}
		}
		return func(val int32, d *types.Datum) (bool, error) {
			d.SetInt64(int64(val))
			return false, nil
		}
	}
}

func getInt64Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[int64] {
	temporalType, temporalFSP, temporalSkipCast := resolveTemporalTarget(target)
	switch converted.converted {
	case schema.ConvertedTypes.TimeMicros, schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) (bool, error) {
			t := time.UnixMicro(val).In(time.UTC)
			inRange := setTemporalDatum(t, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
			return temporalSkipCast && inRange, nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) (bool, error) {
			t := time.UnixMilli(val).In(time.UTC)
			inRange := setTemporalDatum(t, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
			return temporalSkipCast && inRange, nil
		}
	case schema.ConvertedTypes.Decimal:
		checkFunc := getDecimalCheckFunc(converted.decimalMeta, target)
		if checkFunc == nil {
			return func(val int64, d *types.Datum) (bool, error) {
				dec := initializeMyDecimal(d)
				return false, setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
			}
		}
		return func(val int64, d *types.Datum) (bool, error) {
			dec := initializeMyDecimal(d)
			err := setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
			if err != nil {
				return false, err
			}
			return checkFunc(d), nil
		}
	default:
		fromUnsigned := isUnsignedParquetType(converted.converted)
		if target != nil && mysql.IsIntegerType(target.GetType()) {
			toUnsigned := mysql.HasUnsignedFlag(target.GetFlag())
			if fromUnsigned && toUnsigned {
				upperBound := types.IntegerUnsignedUpperBound(target.GetType())
				return func(val int64, d *types.Datum) (bool, error) {
					d.SetUint64(uint64(val))
					return uint64(val) <= upperBound, nil
				}
			} else if !fromUnsigned && !toUnsigned {
				lowerBound := types.IntegerSignedLowerBound(target.GetType())
				upperBound := types.IntegerSignedUpperBound(target.GetType())
				return func(val int64, d *types.Datum) (bool, error) {
					d.SetInt64(val)
					return val >= lowerBound && val <= upperBound, nil
				}
			}
		}
		if fromUnsigned {
			return func(val int64, d *types.Datum) (bool, error) {
				d.SetUint64(uint64(val))
				return false, nil
			}
		}
		return func(val int64, d *types.Datum) (bool, error) {
			d.SetInt64(val)
			return false, nil
		}
	}
}

func getInt96Setter(converted *columnType, loc *time.Location, target *model.ColumnInfo) setter[parquet.Int96] {
	temporalType, temporalFSP, temporalSkipCast := resolveTemporalTarget(target)
	return func(val parquet.Int96, d *types.Datum) (bool, error) {
		// Ref: https://github.com/apache/parquet-format/blob/d20f9dd3d8e7663c44586f7dfb9b7a0534808c87/src/main/thrift/parquet.thrift#L1105-L1112
		// INT96 is deprecated. Layout: [8B nanoseconds-of-day | 4B Julian day].
		// Julian day is uint32(days since noon on January 1, 4713 BC), so dates
		// before 1970-01-01 may be truncated.
		t := val.ToTime().In(time.UTC)
		inRange := setTemporalDatum(t, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
		return temporalSkipCast && inRange, nil
	}
}

func getFloat32Setter(target *model.ColumnInfo) setter[float32] {
	skipCast := isTargetType(target, mysql.TypeFloat) &&
		target.GetDecimal() == types.UnspecifiedLength &&
		!mysql.HasUnsignedFlag(target.GetFlag())
	return func(val float32, d *types.Datum) (bool, error) {
		d.SetFloat32(val)
		return skipCast, nil
	}
}

func getFloat64Setter(target *model.ColumnInfo) setter[float64] {
	skipCast := isTargetType(target, mysql.TypeDouble) &&
		target.GetDecimal() == types.UnspecifiedLength &&
		!mysql.HasUnsignedFlag(target.GetFlag())
	return func(val float64, d *types.Datum) (bool, error) {
		d.SetFloat64(val)
		return skipCast, nil
	}
}

func getBytesSetter[T parquet.ByteArray | parquet.FixedLenByteArray](
	converted *columnType, target *model.ColumnInfo,
) setter[T] {
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		checkFunc := getDecimalCheckFunc(converted.decimalMeta, target)
		if checkFunc == nil {
			return func(val T, d *types.Datum) (bool, error) {
				return false, setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
			}
		}
		return func(val T, d *types.Datum) (bool, error) {
			err := setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
			if err != nil {
				return false, err
			}
			return checkFunc(d), nil
		}
	case schema.ConvertedTypes.UTF8:
		if stringCanSkipCast(target) {
			targetFlen := target.GetFlen()
			enc := charset.FindEncoding(target.GetCharset())
			collation := target.GetCollate()
			return func(val T, d *types.Datum) (bool, error) {
				skip := stringCheckFunc(val, targetFlen, enc)
				if skip {
					d.SetBytesAsString(val, collation, 0)
				} else {
					d.SetBytesAsString(val, "utf8mb4_bin", 0)
				}
				return skip, nil
			}
		}
		fallthrough
	default:
		return func(val T, d *types.Datum) (bool, error) {
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return false, nil
		}
	}
}
