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
	"encoding/binary"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) error

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
	decimalMeta schema.DecimalMetadata,
) error {
	dec.FromInt(unscaled)

	scale := int(decimalMeta.Scale)
	if err := dec.Shift(-scale); err != nil {
		return err
	}

	return dec.Round(dec, scale, types.ModeTruncate)
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

func getInt32Setter(converted *convertedType, loc *time.Location) setter[int32] {
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) error {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.Int32, schema.ConvertedTypes.Uint32,
		schema.ConvertedTypes.Int16, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Int8, schema.ConvertedTypes.Uint8,
		schema.ConvertedTypes.None:
		return func(val int32, d *types.Datum) error {
			d.SetInt64(int64(val))
			return nil
		}
	}

	return nil
}

func getInt64Setter(converted *convertedType, loc *time.Location) setter[int64] {
	switch converted.converted {
	case schema.ConvertedTypes.Uint64,
		schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Int32,
		schema.ConvertedTypes.Uint16, schema.ConvertedTypes.Int16,
		schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Int8:
		return func(val int64, d *types.Datum) error {
			d.SetUint64(uint64(val))
			return nil
		}
	case schema.ConvertedTypes.None, schema.ConvertedTypes.Int64:
		return func(val int64, d *types.Datum) error {
			d.SetInt64(val)
			return nil
		}
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val int64, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
		}
	}

	return nil
}

// newInt96 is a utility function to create a parquet.Int96 for test,
// where microseconds is the number of microseconds since Unix epoch.
func newInt96(microseconds int64) parquet.Int96 {
	day := uint32(microseconds/(86400*1e6) + 2440588)
	nanoOfDay := uint64(microseconds % (86400 * 1e6) * 1e3)
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], nanoOfDay)
	binary.LittleEndian.PutUint32(b[8:], day)
	return parquet.Int96(b)
}

func setInt96Data(val parquet.Int96, d *types.Datum, loc *time.Location, adjustToUTC bool) {
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
	//   julian day - 2440588 (Julian Day of the Unix epoch 1970-01-01 00:00:00)
	// As julian day is decoded as uint32, so if user store a date before 1970-01-01, the converted time will be wrong
	// and possibly to be truncated.
	t := val.ToTime().In(time.UTC)
	if adjustToUTC {
		t = t.In(loc)
	}
	mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
	d.SetMysqlTime(mysqlTime)
}

func getInt96Setter(converted *convertedType, loc *time.Location) setter[parquet.Int96] {
	return func(val parquet.Int96, d *types.Datum) error {
		setInt96Data(val, d, loc, converted.IsAdjustedToUTC)
		return nil
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

func getByteArraySetter(converted *convertedType) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.ByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.ByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}

func getFixedLenByteArraySetter(converted *convertedType) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}
