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
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum)

func binaryToDecimalStr(rawBytes []byte, scale int) string {
	negative := rawBytes[0] > 127
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

	intValue := big.NewInt(0)
	intValue = intValue.SetBytes(rawBytes)
	val := fmt.Sprintf("%0*d", scale, intValue)
	dotIndex := len(val) - scale
	var res strings.Builder
	if negative {
		res.WriteByte('-')
	}
	if dotIndex == 0 {
		res.WriteByte('0')
	} else {
		res.WriteString(val[:dotIndex])
	}
	if scale > 0 {
		res.WriteByte('.')
		res.WriteString(val[dotIndex:])
	}
	return res.String()
}

//nolint:all_revive
func getBoolDataSetter(val bool, d *types.Datum) {
	if val {
		d.SetUint64(1)
	} else {
		d.SetUint64(0)
	}
}

func setDecimalFromIntImpl(val int64, d *types.Datum, converted *convertedType) {
	decimal := converted.decimalMeta
	if !decimal.IsSet || decimal.Scale == 0 {
		d.SetInt64(val)
	}

	minLen := decimal.Scale + 1
	if val < 0 {
		minLen++
	}
	v := fmt.Sprintf("%0*d", minLen, val)
	dotIndex := len(v) - int(decimal.Scale)
	d.SetString(v[:dotIndex]+"."+v[dotIndex:], "utf8mb4_bin")
}

func getInt32Setter(converted *convertedType, loc *time.Location) setter[int32] {
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) {
			// TODO(joechenrh): don't convert to string here
			setDecimalFromIntImpl(int64(val), d, converted)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.Int32, schema.ConvertedTypes.Uint32,
		schema.ConvertedTypes.Int16, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Int8, schema.ConvertedTypes.Uint8,
		schema.ConvertedTypes.None:
		return func(val int32, d *types.Datum) {
			d.SetInt64(int64(val))
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
		return func(val int64, d *types.Datum) {
			d.SetUint64(uint64(val))
		}
	case schema.ConvertedTypes.None, schema.ConvertedTypes.Int64:
		return func(val int64, d *types.Datum) {
			d.SetInt64(val)
		}
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.Decimal:
		return func(val int64, d *types.Datum) {
			// TODO(joechenrh): don't convert to string here
			setDecimalFromIntImpl(val, d, converted)
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
	return func(val parquet.Int96, d *types.Datum) {
		setInt96Data(val, d, loc, converted.IsAdjustedToUTC)
	}
}

func setFloat32Data(val float32, d *types.Datum) {
	d.SetFloat32(val)
}

func setFloat64Data(val float64, d *types.Datum) {
	d.SetFloat64(val)
}

func getByteArraySetter(converted *convertedType) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.ByteArray, d *types.Datum) {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.ByteArray, d *types.Datum) {
			str := binaryToDecimalStr(val, int(converted.decimalMeta.Scale))
			d.SetString(str, "utf8mb4_bin")
		}
	}

	return nil
}

func getFixedLenByteArraySetter(converted *convertedType) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.FixedLenByteArray, d *types.Datum) {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.FixedLenByteArray, d *types.Datum) {
			str := binaryToDecimalStr(val, int(converted.decimalMeta.Scale))
			d.SetString(str, "utf8mb4_bin")
		}
	}

	return nil
}
