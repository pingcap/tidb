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

package parquetfile

import (
	"encoding/binary"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) error

func unsupportedParquetValueSetter[T parquet.ColumnTypes](logicalType schema.LogicalType) setter[T] {
	return func(T, *types.Datum) error {
		return errors.Errorf("unsupported parquet logical type %s", logicalType.String())
	}
}

var zeroMyDecimal = types.MyDecimal{}

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
)

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
	scale int32,
) error {
	dec.FromInt(unscaled)

	if err := dec.Shift(-int(scale)); err != nil {
		return err
	}

	return dec.Round(dec, int(scale), types.ModeTruncate)
}

//nolint:all_revive
func getBoolDataSetter(val bool, d *types.Datum) error {
	if val {
		d.SetUint64(1)
	} else {
		d.SetUint64(0)
	}
	return nil
}

func getInt32Setter(parquetColumnType *parquetColumnType, loc *time.Location) setter[int32] {
	// For parquet TIME/TIMESTAMP epoch values:
	// - IsAdjustedToUTC=true: interpret as UTC instant, then render in parser location.
	// - IsAdjustedToUTC=false: keep as local-semantics wall clock ("as-if UTC"), no loc conversion.
	// The LogicalType's IsAdjustedToUTC flag controls whether the decoded instant
	// is rendered in the parser location or kept as a wall-clock value.
	switch logicalType := parquetColumnType.logicalType.(type) {
	case schema.DecimalLogicalType:
		return func(val int32, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(int64(val), dec, logicalType.Scale())
		}
	case schema.DateLogicalType:
		return func(val int32, d *types.Datum) error {
			if parquetColumnType.sparkRebaseMicros.timeZoneID != "" {
				val = int32(rebaseJulianToGregorianDays(int(val)))
			}
			t := arrow.Date32(val).ToTime()
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.TimeLogicalType:
		return func(val int32, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			return setTimestampDatum(t, d, loc, logicalType.IsAdjustedToUTC())
		}
	case schema.IntLogicalType:
		return func(val int32, d *types.Datum) error {
			if logicalType.IsSigned() {
				d.SetInt64(int64(val))
			} else {
				d.SetUint64(uint64(uint32(val)))
			}
			return nil
		}
	case schema.NoLogicalType, schema.UnknownLogicalType:
		return func(val int32, d *types.Datum) error {
			d.SetInt64(int64(val))
			return nil
		}
	}

	return unsupportedParquetValueSetter[int32](parquetColumnType.logicalType)
}

func getInt64Setter(parquetColumnType *parquetColumnType, loc *time.Location) setter[int64] {
	switch logicalType := parquetColumnType.logicalType.(type) {
	case schema.IntLogicalType:
		return func(val int64, d *types.Datum) error {
			if logicalType.IsSigned() {
				d.SetInt64(val)
			} else {
				d.SetUint64(uint64(val))
			}
			return nil
		}
	case schema.NoLogicalType, schema.UnknownLogicalType:
		return func(val int64, d *types.Datum) error {
			d.SetInt64(val)
			return nil
		}
	case schema.TimeLogicalType:
		return func(val int64, d *types.Datum) error {
			var t time.Time
			switch logicalType.TimeUnit() {
			case schema.TimeUnitNanos:
				t = time.Unix(0, val).In(time.UTC)
			case schema.TimeUnitMicros:
				t = time.UnixMicro(val).In(time.UTC)
			default:
				return errors.Errorf("unsupported parquet time unit %d", logicalType.TimeUnit())
			}
			return setTimestampDatum(t, d, loc, logicalType.IsAdjustedToUTC())
		}
	case schema.TimestampLogicalType:
		return func(val int64, d *types.Datum) error {
			if parquetColumnType.sparkRebaseMicros.timeZoneID != "" && logicalType.TimeUnit() != schema.TimeUnitNanos {
				rebaseMicros := val
				if logicalType.TimeUnit() == schema.TimeUnitMillis {
					rebaseMicros *= 1000
				}
				rebased, err := parquetColumnType.sparkRebaseMicros.rebase(rebaseMicros)
				if err != nil {
					return err
				}
				if logicalType.TimeUnit() == schema.TimeUnitMillis {
					val = rebased / 1000
				} else {
					val = rebased
				}
			}
			var unit arrow.TimeUnit
			switch logicalType.TimeUnit() {
			case schema.TimeUnitMillis:
				unit = arrow.Millisecond
			case schema.TimeUnitMicros:
				unit = arrow.Microsecond
			case schema.TimeUnitNanos:
				unit = arrow.Nanosecond
			default:
				return errors.Errorf("unsupported parquet timestamp time unit %d", logicalType.TimeUnit())
			}
			t := arrow.Timestamp(val).ToTime(unit)
			return setTimestampDatum(t, d, loc, logicalType.IsAdjustedToUTC())
		}
	case schema.DecimalLogicalType:
		return func(val int64, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(val, dec, logicalType.Scale())
		}
	}

	return unsupportedParquetValueSetter[int64](parquetColumnType.logicalType)
}

// newInt96 is a utility function to create a parquet.Int96 for test,
// where microseconds is the number of microseconds since Unix epoch.
func newInt96(microseconds int64) parquet.Int96 {
	// INT96 stores the time-of-day as a non-negative field, so pre-epoch
	// timestamps must be split with floor division instead of Go's truncation.
	dayOffset := floorDivInt64(microseconds, microsPerDay)
	microsOfDay := floorModInt64(microseconds, microsPerDay)
	day := uint32(dayOffset + julianDayOfUnixEpoch)
	nanoOfDay := uint64(microsOfDay * int64(time.Microsecond))
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], nanoOfDay)
	binary.LittleEndian.PutUint32(b[8:], day)
	return parquet.Int96(b)
}

func setTimestampDatum(t time.Time, d *types.Datum, loc *time.Location, adjustedToUTC bool) error {
	if adjustedToUTC {
		t = t.In(loc)
	}
	mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
	d.SetMysqlTime(mysqlTime)
	return nil
}

func decodeInt96ToTime(val parquet.Int96, rebaseMicros sparkRebaseMicrosLookup) (time.Time, error) {
	// FYI: https://github.com/apache/spark/blob/d66a4e82eceb89a274edeb22c2fb4384bed5078b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L171-L178
	// INT96 timestamp layout
	// --------------------------
	// |   64 bit   |   32 bit   |
	// ---------------------------
	// |  nano sec  |  julian day  |
	// ---------------------------
	// NOTE:
	// INT96 is a deprecated type in parquet format to store timestamp, which consists of
	// two parts: the first 8 bytes is the nanoseconds within the day, and the last 4 bytes
	// is the Julian Day (days since noon on January 1, 4713 BC). And it will be converted it to UTC by
	// subtracting the Julian day of the Unix epoch (1970-01-01 00:00:00).
	var t time.Time
	if rebaseMicros.timeZoneID == "" {
		// Preserve INT96 nanoseconds until FromGoTime so TIMESTAMP(6) keeps
		// TiDB's existing nearest-microsecond rounding instead of truncating.
		t = int96ToGoTime(val)
	} else {
		micros := int96ToUnixMicros(val)
		rebased, err := rebaseMicros.rebase(micros)
		if err != nil {
			return time.Time{}, err
		}
		t = arrow.Timestamp(rebased).ToTime(arrow.Microsecond)
	}
	return t, nil
}

func setInt96Data(val parquet.Int96, d *types.Datum, loc *time.Location, rebaseMicros sparkRebaseMicrosLookup) error {
	t, err := decodeInt96ToTime(val, rebaseMicros)
	if err != nil {
		return err
	}
	// INT96 has no standard LogicalType; preserve its existing UTC-normalized semantics.
	return setTimestampDatum(t, d, loc, true)
}

func int96ToGoTime(val parquet.Int96) time.Time {
	nanosOfDay := int64(binary.LittleEndian.Uint64(val[:8]))
	julianDay := int64(binary.LittleEndian.Uint32(val[8:]))
	seconds := (julianDay-julianDayOfUnixEpoch)*int64(24*time.Hour/time.Second) + nanosOfDay/int64(time.Second)
	nanoseconds := nanosOfDay % int64(time.Second)
	return time.Unix(seconds, nanoseconds).UTC()
}

func int96ToUnixMicros(val parquet.Int96) int64 {
	nanosOfDay := int64(binary.LittleEndian.Uint64(val[:8]))
	julianDay := int64(binary.LittleEndian.Uint32(val[8:]))
	return (julianDay-julianDayOfUnixEpoch)*microsPerDay + nanosOfDay/int64(time.Microsecond)
}

func getInt96Setter(parquetColumnType *parquetColumnType, loc *time.Location) setter[parquet.Int96] {
	return func(val parquet.Int96, d *types.Datum) error {
		return setInt96Data(val, d, loc, parquetColumnType.sparkRebaseMicros)
	}
}

func setFloat32Data(val float32, d *types.Datum) error {
	d.SetFloat32(val)
	return nil
}

func setFloat64Data(val float64, d *types.Datum) error {
	d.SetFloat64(val)
	return nil
}

// getDecimalByteSetter returns a setter for byte array DECIMAL columns.
// Dictionary decoded values alias the shared dictionary buffer, and the
// conversion consumes its input in place, so copy it into our own buffer first.
func getDecimalByteSetter[T parquet.ByteArray | parquet.FixedLenByteArray](scale int) setter[T] {
	var buf []byte
	return func(val T, d *types.Datum) error {
		buf = append(buf[:0], val...)
		return setDatumFromDecimalByte(d, buf, scale)
	}
}

func getByteArraySetter(parquetColumnType *parquetColumnType) setter[parquet.ByteArray] {
	switch logicalType := parquetColumnType.logicalType.(type) {
	case schema.NoLogicalType, schema.UnknownLogicalType, schema.BSONLogicalType, schema.JSONLogicalType, schema.StringLogicalType, schema.EnumLogicalType:
		return func(val parquet.ByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.DecimalLogicalType:
		return getDecimalByteSetter[parquet.ByteArray](int(logicalType.Scale()))
	}

	return unsupportedParquetValueSetter[parquet.ByteArray](parquetColumnType.logicalType)
}

func getFixedLenByteArraySetter(parquetColumnType *parquetColumnType) setter[parquet.FixedLenByteArray] {
	switch logicalType := parquetColumnType.logicalType.(type) {
	case schema.NoLogicalType, schema.UnknownLogicalType, schema.BSONLogicalType, schema.JSONLogicalType, schema.StringLogicalType, schema.EnumLogicalType:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.DecimalLogicalType:
		return getDecimalByteSetter[parquet.FixedLenByteArray](int(logicalType.Scale()))
	}

	return unsupportedParquetValueSetter[parquet.FixedLenByteArray](parquetColumnType.logicalType)
}
