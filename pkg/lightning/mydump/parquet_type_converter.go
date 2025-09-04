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

func getBoolData(val bool, d *types.Datum) {
	if val {
		d.SetUint64(1)
	} else {
		d.SetUint64(0)
	}
}

func getDecimalFromIntImpl(val int64, d *types.Datum, converted *convertedType) {
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

func getInt32Getter(converted *convertedType, loc *time.Location) setter[int32] {
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) {
			getDecimalFromIntImpl(int64(val), d, converted)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(loc)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(loc)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.Int32, schema.ConvertedTypes.None:
		return func(val int32, d *types.Datum) {
			d.SetInt64(int64(val))
		}
	}

	return nil
}

func getInt64Getter(converted *convertedType, loc *time.Location) setter[int64] {
	switch converted.converted {
	case schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Uint64:
		return func(val int64, d *types.Datum) {
			d.SetUint64(uint64(val))
		}
	case schema.ConvertedTypes.None, schema.ConvertedTypes.Int64:
		return func(val int64, d *types.Datum) {
			d.SetInt64(int64(val))
		}
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(loc)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(loc)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(loc)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)
			d.SetMysqlTime(mysqlTime)
		}
	case schema.ConvertedTypes.Decimal:
		return func(val int64, d *types.Datum) {
			getDecimalFromIntImpl(val, d, converted)
		}
	}

	return nil
}

func getInt96Data(val parquet.Int96, d *types.Datum, loc *time.Location) {
	// FYI: https://github.com/apache/spark/blob/d66a4e82eceb89a274edeb22c2fb4384bed5078b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L171-L178
	// INT96 timestamp layout
	// --------------------------
	// |   64 bit   |   32 bit   |
	// ---------------------------
	// |  nano sec  |  julian day  |
	// ---------------------------
	// NOTE: parquet date can be less than 1970-01-01 that is not supported by TiDB,
	// where dt is a negative number but still legal in the context of Go.
	// But it will cause errors or potential data inconsistency when importing.
	t := val.ToTime().In(loc)
	mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 0)
	d.SetMysqlTime(mysqlTime)
}

func getInt96Getter(_ *convertedType, loc *time.Location) setter[parquet.Int96] {
	return func(val parquet.Int96, d *types.Datum) {
		getInt96Data(val, d, loc)
	}
}

func getFloat32Data(val float32, d *types.Datum) {
	d.SetFloat32(val)
}

func getFloat64Data(val float64, d *types.Datum) {
	d.SetFloat64(val)
}

func getByteArrayGetter(converted *convertedType) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.ByteArray, d *types.Datum) {
			d.SetString(string(val), "utf8mb4_bin")
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.ByteArray, d *types.Datum) {
			str := binaryToDecimalStr(val, int(converted.decimalMeta.Scale))
			d.SetString(str, "utf8mb4_bin")
		}
	}

	return nil
}

func getFixedLenByteArrayGetter(converted *convertedType) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.FixedLenByteArray, d *types.Datum) {
			d.SetString(string(val), "utf8mb4_bin")
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.FixedLenByteArray, d *types.Datum) {
			str := binaryToDecimalStr(val, int(converted.decimalMeta.Scale))
			d.SetString(str, "utf8mb4_bin")
		}
	}

	return nil
}
