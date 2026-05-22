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
	"math/big"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"
)

func TestParseDecimalToScaledInteger(t *testing.T) {
	scaled, err := parseDecimalToScaledInteger("12.349", 2)
	require.NoError(t, err)
	require.Equal(t, "1234", scaled.String())

	scaled, err = parseDecimalToScaledInteger("-12.349", 2)
	require.NoError(t, err)
	require.Equal(t, "-1234", scaled.String())

	_, err = parseDecimalToScaledInteger("not-decimal", 2)
	require.ErrorContains(t, err, "invalid decimal value")

	_, err = parseDecimalToScaledInteger("1", -1)
	require.ErrorContains(t, err, "invalid decimal scale")
}

func TestToFixedLenTwoComplementHandlesBoundariesAndOverflow(t *testing.T) {
	encoded, err := toFixedLenTwoComplement(big.NewInt(255), 2)
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0xff}, encoded)

	encoded, err = toFixedLenTwoComplement(big.NewInt(-1), 2)
	require.NoError(t, err)
	require.Equal(t, []byte{0xff, 0xff}, encoded)

	_, err = toFixedLenTwoComplement(big.NewInt(128), 1)
	require.ErrorContains(t, err, "does not fit in 1 bytes")

	_, err = toFixedLenTwoComplement(big.NewInt(-129), 1)
	require.ErrorContains(t, err, "does not fit in 1 bytes")

	_, err = toFixedLenTwoComplement(big.NewInt(0), 0)
	require.ErrorContains(t, err, "invalid fixed-size byte width")
}

func TestParseRawColumnValueCoversSuccessAndErrorBranches(t *testing.T) {
	value, isNull, err := parseRawColumnValue(sql.RawBytes("true"), column{
		columnType: columnType{Physical: parquet.Types.Boolean},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, true, value.(bool))

	_, _, err = parseRawColumnValue(sql.RawBytes("bad-bool"), column{
		columnType: columnType{Physical: parquet.Types.Boolean},
	})
	require.Error(t, err)

	value, isNull, err = parseRawColumnValue(sql.RawBytes("12.34"), column{
		columnType: columnType{
			Physical: parquet.Types.Int32,
			Logical:  schema.NewDecimalLogicalType(9, 2),
			Scale:    2,
		},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int32(1234), value.(int32))

	_, _, err = parseRawColumnValue(sql.RawBytes("21474836.48"), column{
		columnType: columnType{
			Physical: parquet.Types.Int32,
			Logical:  schema.NewDecimalLogicalType(9, 2),
			Scale:    2,
		},
	})
	require.ErrorContains(t, err, "does not fit in INT32")

	value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05"), column{
		allowsNullEncoding: true,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
		},
		timestampUnit: schema.TimeUnitMicros,
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixMicro(), value.(int64))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.123"), column{
		allowsNullEncoding: true,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMillis),
		},
		timestampUnit: schema.TimeUnitMillis,
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC).UnixMilli(), value.(int64))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.123456"), column{
		allowsNullEncoding: true,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
		},
		timestampUnit: schema.TimeUnitMicros,
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC).UnixMicro(), value.(int64))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("2024-01-02 03:04:05.1"), column{
		allowsNullEncoding: true,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
		},
		timestampUnit: schema.TimeUnitMicros,
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, time.Date(2024, 1, 2, 3, 4, 5, 100000000, time.UTC).UnixMicro(), value.(int64))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("0000-00-00 00:00:00"), column{
		allowsNullEncoding: true,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
		},
		timestampUnit: schema.TimeUnitMicros,
	})
	require.NoError(t, err)
	require.True(t, isNull)
	require.Nil(t, value)

	_, _, err = parseRawColumnValue(sql.RawBytes("0000-00-00 00:00:00"), column{
		allowsNullEncoding: false,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
		},
		timestampUnit: schema.TimeUnitMicros,
	})
	require.Error(t, err)

	_, _, err = parseRawColumnValue(sql.RawBytes("9223372036854775808"), column{
		timestampUnit: schema.TimeUnitUnknown,
		columnType: columnType{
			Physical: parquet.Types.Int64,
			Logical:  schema.NewDecimalLogicalType(19, 0),
		},
	})
	require.ErrorContains(t, err, "does not fit in INT64")

	rawBytes := sql.RawBytes("abcd")
	value, isNull, err = parseRawColumnValue(rawBytes, column{
		columnType: columnType{Physical: parquet.Types.ByteArray},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	rawBytes[0] = 'z'
	require.Equal(t, "abcd", string(value.(parquet.ByteArray)))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("-1.23"), column{
		columnType: columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			Logical:    schema.NewDecimalLogicalType(10, 2),
			TypeLength: 4,
			Scale:      2,
		},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, []byte{0xff, 0xff, 0xff, 0x85}, []byte(value.(parquet.FixedLenByteArray)))

	rawFixedBytes := sql.RawBytes("wxyz")
	value, isNull, err = parseRawColumnValue(rawFixedBytes, column{
		columnType: columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			TypeLength: 4,
		},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	rawFixedBytes[0] = 'q'
	require.Equal(t, "wxyz", string(value.(parquet.FixedLenByteArray)))

	_, _, err = parseRawColumnValue(sql.RawBytes("abc"), column{
		columnType: columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			TypeLength: 4,
		},
	})
	require.ErrorContains(t, err, "width mismatch")

	_, _, err = parseRawColumnValue(sql.RawBytes("abcd"), column{
		columnType: columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			TypeLength: 0,
		},
	})
	require.ErrorContains(t, err, "invalid fixed-size byte width")

	_, _, err = parseRawColumnValue(sql.RawBytes("v"), column{
		columnType: columnType{Physical: parquet.Types.Int96},
	})
	require.ErrorContains(t, err, "unsupported parquet physical type")
}

func TestParseRawColumnValueNumericPrimitiveBranches(t *testing.T) {
	value, isNull, err := parseRawColumnValue(sql.RawBytes("123"), column{
		columnType: columnType{Physical: parquet.Types.Int32},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int32(123), value.(int32))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("456"), column{
		timestampUnit: schema.TimeUnitUnknown,
		columnType:    columnType{Physical: parquet.Types.Int64},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(456), value.(int64))

	value, isNull, err = parseRawColumnValue(sql.RawBytes("1.5"), column{
		columnType: columnType{Physical: parquet.Types.Float},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float32(1.5), value.(float32))

	_, _, err = parseRawColumnValue(sql.RawBytes("bad-float"), column{
		columnType: columnType{Physical: parquet.Types.Float},
	})
	require.Error(t, err)

	value, isNull, err = parseRawColumnValue(sql.RawBytes("2.5"), column{
		columnType: columnType{Physical: parquet.Types.Double},
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(2.5), value.(float64))

	_, _, err = parseRawColumnValue(sql.RawBytes("bad-double"), column{
		columnType: columnType{Physical: parquet.Types.Double},
	})
	require.Error(t, err)

	_, _, err = parseRawColumnValue(sql.RawBytes("1.28"), column{
		columnType: columnType{
			Physical:   parquet.Types.FixedLenByteArray,
			Logical:    schema.NewDecimalLogicalType(3, 2),
			TypeLength: 1,
			Scale:      2,
		},
	})
	require.ErrorContains(t, err, "does not fit in 1 bytes")
}

func TestAppendColumnValueAppendsSupportedPhysicalTypes(t *testing.T) {
	buffer := &columnBuffer{}

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.Boolean},
	}, true))
	require.Equal(t, []bool{true}, buffer.boolValues)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.Int32},
	}, int32(7)))
	require.Equal(t, []int32{7}, buffer.int32Values)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.Int64},
	}, int64(8)))
	require.Equal(t, []int64{8}, buffer.int64Values)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.Float},
	}, float32(1.25)))
	require.Equal(t, []float32{1.25}, buffer.float32Values)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.Double},
	}, float64(2.25)))
	require.Equal(t, []float64{2.25}, buffer.float64Values)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.ByteArray},
	}, parquet.ByteArray([]byte("a"))))
	require.Equal(t, []parquet.ByteArray{parquet.ByteArray([]byte("a"))}, buffer.byteArrayValues)

	require.NoError(t, appendColumnValue(buffer, column{
		columnType: columnType{Physical: parquet.Types.FixedLenByteArray},
	}, parquet.FixedLenByteArray([]byte("bc"))))
	require.Equal(t, []parquet.FixedLenByteArray{parquet.FixedLenByteArray([]byte("bc"))}, buffer.fixedLenByteArrayValues)
}
