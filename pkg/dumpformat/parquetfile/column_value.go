// Copyright 2026 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

const (
	byteArraySliceHeaderBytes         = int64(unsafe.Sizeof(parquet.ByteArray(nil)))
	fixedLenByteArraySliceHeaderBytes = int64(unsafe.Sizeof(parquet.FixedLenByteArray(nil)))
)

func parseRawColumnValue(rawValue sql.RawBytes, column column) (any, bool, error) {
	switch column.Physical {
	case parquet.Types.Boolean:
		s := string(rawValue)
		v, err := strconv.ParseBool(s)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.Int32:
		s := string(rawValue)
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			scaled, err := parseDecimalToScaledInteger(s, column.columnType.Scale)
			if err != nil {
				return nil, false, err
			}
			if !scaled.IsInt64() {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT32", s)
			}
			value := scaled.Int64()
			if value < math.MinInt32 || value > math.MaxInt32 {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT32", s)
			}
			return int32(value), false, nil
		}
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, false, err
		}
		return int32(v), false, nil
	case parquet.Types.Int64:
		s := string(rawValue)
		if _, ok := column.Logical.(schema.TimestampLogicalType); ok {
			// For mysql text protocol, temporal values are emitted as
			// "YYYY-MM-DD HH:MM:SS[.fraction]"; time.Parse(time.DateTime, s)
			// accepts optional fractional digits.
			// Since there is no timezone info in temporal values, Go parses them
			// as UTC. This is counterintuitive but matches our isAdjustedToUTC=false
			// setting in toColumnType (local-semantics "as-if UTC" encoding).
			t, err := time.Parse(time.DateTime, s)
			if err != nil {
				if column.allowsNullEncoding {
					return nil, true, nil
				}
				return nil, false, err
			}
			return unixTimestampByUnit(t, column.timestampUnit), false, nil
		}
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			scaled, err := parseDecimalToScaledInteger(s, column.columnType.Scale)
			if err != nil {
				return nil, false, err
			}
			if !scaled.IsInt64() {
				return nil, false, fmt.Errorf("decimal value %q does not fit in INT64", s)
			}
			return scaled.Int64(), false, nil
		}
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.Float:
		s := string(rawValue)
		// Kept for completeness when handling external/custom parquet schema:
		// current toColumnType maps SQL FLOAT to parquet.Types.Double.
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return nil, false, err
		}
		return float32(v), false, nil
	case parquet.Types.Double:
		s := string(rawValue)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	case parquet.Types.ByteArray:
		cloned := make([]byte, len(rawValue))
		copy(cloned, rawValue)
		return parquet.ByteArray(cloned), false, nil
	case parquet.Types.FixedLenByteArray:
		if _, ok := column.Logical.(schema.DecimalLogicalType); ok {
			s := string(rawValue)
			scaled, err := parseDecimalToScaledInteger(s, column.columnType.Scale)
			if err != nil {
				return nil, false, err
			}
			encoded, err := toFixedLenTwoComplement(scaled, column.TypeLength)
			if err != nil {
				return nil, false, err
			}
			return parquet.FixedLenByteArray(encoded), false, nil
		}
		if column.TypeLength <= 0 {
			return nil, false, fmt.Errorf("invalid fixed-size byte width %d", column.TypeLength)
		}
		cloned := make([]byte, len(rawValue))
		copy(cloned, rawValue)
		if len(cloned) != column.TypeLength {
			return nil, false, fmt.Errorf("fixed-len byte array width mismatch: got %d, expected %d", len(cloned), column.TypeLength)
		}
		return parquet.FixedLenByteArray(cloned), false, nil
	default:
		return nil, false, fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
}

func unixTimestampByUnit(t time.Time, unit schema.TimeUnitType) int64 {
	switch unit {
	case schema.TimeUnitMillis:
		return t.UnixMilli()
	case schema.TimeUnitMicros:
		return t.UnixMicro()
	case schema.TimeUnitNanos:
		return t.UnixNano()
	default:
		return t.UnixMicro()
	}
}

func appendColumnValue(buffer *columnBuffer, column column, value any) error {
	switch column.Physical {
	case parquet.Types.Boolean:
		buffer.boolValues = append(buffer.boolValues, value.(bool))
	case parquet.Types.Int32:
		buffer.int32Values = append(buffer.int32Values, value.(int32))
	case parquet.Types.Int64:
		buffer.int64Values = append(buffer.int64Values, value.(int64))
	case parquet.Types.Float:
		buffer.float32Values = append(buffer.float32Values, value.(float32))
	case parquet.Types.Double:
		buffer.float64Values = append(buffer.float64Values, value.(float64))
	case parquet.Types.ByteArray:
		buffer.byteArrayValues = append(buffer.byteArrayValues, value.(parquet.ByteArray))
	case parquet.Types.FixedLenByteArray:
		buffer.fixedLenByteArrayValues = append(buffer.fixedLenByteArrayValues, value.(parquet.FixedLenByteArray))
	default:
		return fmt.Errorf("unsupported parquet physical type %s", column.Physical)
	}
	return nil
}

func accountColumnValueMemoryBytes(column column, value any) int64 {
	switch column.Physical {
	case parquet.Types.Boolean:
		return 1
	case parquet.Types.Int32, parquet.Types.Float:
		return 4
	case parquet.Types.Int64, parquet.Types.Double:
		return 8
	case parquet.Types.ByteArray:
		return byteArraySliceHeaderBytes + int64(len(value.(parquet.ByteArray)))
	case parquet.Types.FixedLenByteArray:
		return fixedLenByteArraySliceHeaderBytes + int64(len(value.(parquet.FixedLenByteArray)))
	default:
		return 0
	}
}

func writeColumnBatch(columnWriter file.ColumnChunkWriter, column column, buffer columnBuffer) error {
	var defLevels []int16
	if column.allowsNullEncoding {
		defLevels = buffer.defLevels
	}

	var err error
	switch writer := columnWriter.(type) {
	case *file.BooleanColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.boolValues, defLevels, nil)
	case *file.Int32ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.int32Values, defLevels, nil)
	case *file.Int64ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.int64Values, defLevels, nil)
	case *file.Float32ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.float32Values, defLevels, nil)
	case *file.Float64ColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.float64Values, defLevels, nil)
	case *file.ByteArrayColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.byteArrayValues, defLevels, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		_, err = writer.WriteBatch(buffer.fixedLenByteArrayValues, defLevels, nil)
	default:
		return fmt.Errorf("unsupported column chunk writer %T", columnWriter)
	}
	return err
}

// parseDecimalToScaledInteger converts decimal text into an integer scaled by
// 10^scale. This writer layer intentionally only performs conversion/serialization
// and does not enforce original SQL type/domain validity; callers are expected to
// pass already validated values. Extra fractional digits are truncated toward zero.
func parseDecimalToScaledInteger(s string, scale int) (*big.Int, error) {
	if scale < 0 {
		return nil, fmt.Errorf("invalid decimal scale %d", scale)
	}
	rat, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid decimal value %q", s)
	}
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat.Mul(rat, new(big.Rat).SetInt(multiplier))
	return new(big.Int).Quo(rat.Num(), rat.Denom()), nil
}

func toFixedLenTwoComplement(value *big.Int, byteWidth int) ([]byte, error) {
	if byteWidth <= 0 {
		return nil, fmt.Errorf("invalid fixed-size byte width %d", byteWidth)
	}

	bitWidth := uint(8*byteWidth - 1)
	maxValue := new(big.Int).Lsh(big.NewInt(1), bitWidth)
	maxValue.Sub(maxValue, big.NewInt(1))
	minValue := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), bitWidth))
	if value.Cmp(minValue) < 0 || value.Cmp(maxValue) > 0 {
		return nil, fmt.Errorf("decimal value %s does not fit in %d bytes", value.String(), byteWidth)
	}

	encoded := make([]byte, byteWidth)
	if value.Sign() >= 0 {
		rawBytes := value.Bytes()
		copy(encoded[byteWidth-len(rawBytes):], rawBytes)
		return encoded, nil
	}

	modulus := new(big.Int).Lsh(big.NewInt(1), uint(8*byteWidth))
	twoComplement := new(big.Int).Add(value, modulus)
	rawBytes := twoComplement.Bytes()
	copy(encoded[byteWidth-len(rawBytes):], rawBytes)
	return encoded, nil
}
