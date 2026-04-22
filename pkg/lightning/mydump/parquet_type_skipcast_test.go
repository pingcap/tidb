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

package mydump

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func newParquetTargetColumnInfo(tp byte, flag uint, flen int, decimal int, charset string, collate string) *model.ColumnInfo {
	col := &model.ColumnInfo{}
	col.SetType(tp)
	col.SetFlag(flag)
	col.SetFlen(flen)
	col.SetDecimal(decimal)
	col.SetCharset(charset)
	col.SetCollate(collate)
	return col
}

// TestSetTemporalDatumRounding tests FSP rounding and DATE zeroing in setTemporalDatum.
func TestSetTemporalDatumRounding(t *testing.T) {
	t.Run("DATE zeroes time portion", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDate, 0)
		got := d.GetMysqlTime()
		require.Equal(t, mysql.TypeDate, got.Type())
		require.Equal(t, "2025-01-02", got.String())
	})

	t.Run("DATETIME(0) rounds sub-second up", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 999999000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 0)
		got := d.GetMysqlTime()
		// 0.999999s rounds up to next second (same as types.RoundFrac)
		require.Equal(t, "2025-01-02 03:04:06", got.String())
	})

	t.Run("DATETIME(0) rounds sub-second down", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 499000000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 0)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05", got.String())
	})

	t.Run("DATETIME(3) rounds sub-millisecond up", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123900000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 3)
		got := d.GetMysqlTime()
		// 123.9ms rounds up to 124ms (same as types.RoundFrac)
		require.Equal(t, "2025-01-02 03:04:05.124", got.String())
	})

	t.Run("DATETIME(6) keeps full microseconds", func(t *testing.T) {
		var d types.Datum
		tm := time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)
		setTemporalDatum(tm, &d, nil, false, mysql.TypeDatetime, 6)
		got := d.GetMysqlTime()
		require.Equal(t, "2025-01-02 03:04:05.123456", got.String())
	})
}

// TestTemporalSetterUsesTargetType verifies that temporal setters respect
// the target column type (e.g. DATE) and also checks datum kind.
func TestTemporalSetterUsesTargetType(t *testing.T) {
	converted := &columnType{converted: schema.ConvertedTypes.TimeMicros, IsAdjustedToUTC: true}
	target := newParquetTargetColumnInfo(mysql.TypeDate, 0, 0, 0, "", "")

	setter := getInt64Setter(converted, time.UTC, target)
	var datum types.Datum
	canSkip, err := setter(0, &datum)
	require.NoError(t, err)
	require.True(t, canSkip)
	require.Equal(t, types.KindMysqlTime, datum.Kind())
	require.Equal(t, mysql.TypeDate, datum.GetMysqlTime().Type())
}

// TestTemporalSkipCastRangeValidation verifies that out-of-range temporal values
// fall back to CastColumnValue (canSkipCast=false).
func TestTemporalSkipCastRangeValidation(t *testing.T) {
	t.Run("year > 9999 falls back to cast", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
		target := newParquetTargetColumnInfo(mysql.TypeDatetime, 0, 19, 6, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		// year 12000: 316224000000000000 micros after epoch
		var d types.Datum
		canSkip, err := setter(316224000000000000, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
	})

	t.Run("DATETIME(0) rounding into year 10000 falls back to cast", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
		target := newParquetTargetColumnInfo(mysql.TypeDatetime, 0, 19, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		// 9999-12-31 23:59:59.999999 in micros since epoch
		// After rounding to fsp=0, becomes 10000-01-01 00:00:00 which is out of range.
		us := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()
		var d types.Datum
		canSkip, err := setter(us, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
	})

	t.Run("valid datetime allows skip-cast", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
		target := newParquetTargetColumnInfo(mysql.TypeDatetime, 0, 19, 6, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		us := time.Date(2025, 6, 15, 12, 30, 0, 0, time.UTC).UnixMicro()
		var d types.Datum
		canSkip, err := setter(us, &d)
		require.NoError(t, err)
		require.True(t, canSkip)
	})
}

// TestSetterSkipCastDecisions is the main table-driven test for all setter
// skip-cast decisions across types: bool, float, double, temporal, string,
// decimal, and integer signedness/range.
func TestSetterSkipCastDecisions(t *testing.T) {
	cases := []struct {
		name          string
		getSetter     func(target *model.ColumnInfo) func() (bool, error)
		target        *model.ColumnInfo
		expectCanSkip bool
	}{
		// Bool
		{"bool to signed int",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getBoolSetter(target)
				return func() (bool, error) { return s(true, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", ""),
			true},
		{"bool to unsigned int",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getBoolSetter(target)
				return func() (bool, error) { return s(true, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeLong, mysql.UnsignedFlag, 10, 0, "", ""),
			true},

		// Float / Double — no precision constraint
		{"float to float (no precision)",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat32Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeFloat, 0, types.UnspecifiedLength, types.UnspecifiedLength, "", ""),
			true},
		{"double to double (no precision)",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat64Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeDouble, 0, types.UnspecifiedLength, types.UnspecifiedLength, "", ""),
			true},
		// Float / Double — with precision constraint, must cast
		{"float to float(5,2) not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat32Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeFloat, 0, 5, 2, "", ""),
			false},
		{"double to double unsigned not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				s := getFloat64Setter(target)
				return func() (bool, error) { return s(1.0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeDouble, mysql.UnsignedFlag, types.UnspecifiedLength, types.UnspecifiedLength, "", ""),
			false},

		// Temporal
		{"int32 date to DATE",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Date}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeDate, 0, 10, 0, "", ""),
			true},
		{"temporal to TIMESTAMP not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", ""),
			false},

		// Integer — non-integer target
		{"int to YEAR not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.None}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(0, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeYear, 0, 4, 0, "", ""),
			false},

		// String
		{"utf8 to VARCHAR utf8mb4",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			true},
		{"utf8 to CHAR utf8mb4",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeString, 0, 255, 0, "utf8mb4", "utf8mb4_bin"),
			true},
		{"bytes to BINARY(M) not eligible",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.None}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeString, mysql.BinaryFlag, 10, 0, "binary", "binary"),
			false},
		{"utf8 to VARBINARY",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.UTF8}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte("hi"), &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeVarchar, mysql.BinaryFlag, 255, 0, "binary", "binary"),
			true},

		// Decimal via byte array
		{"decimal byte array fits",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{
					converted:   schema.ConvertedTypes.Decimal,
					decimalMeta: schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 2},
				}
				s := getBytesSetter[parquet.ByteArray](converted, target)
				return func() (bool, error) { return s([]byte{0x00, 0x7B}, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "binary", "binary"),
			true},
		{"decimal fixed-len scale mismatch",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{
					converted:   schema.ConvertedTypes.Decimal,
					decimalMeta: schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 3},
				}
				s := getBytesSetter[parquet.FixedLenByteArray](converted, target)
				return func() (bool, error) { return s([]byte{0x00, 0x7B}, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "", ""),
			false},

		// Integer — same-sign range checks
		{"int32 signed to signed bigint fits",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int32}
				s := getInt32Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(42, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeLonglong, 0, 20, 0, "", ""),
			true},
		{"int64 signed to smallint signed fits",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int64}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(42, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", ""),
			true},
		{"int64 signed to smallint signed overflow",
			func(target *model.ColumnInfo) func() (bool, error) {
				converted := &columnType{converted: schema.ConvertedTypes.Int64}
				s := getInt64Setter(converted, time.UTC, target)
				return func() (bool, error) { return s(40000, &types.Datum{}) }
			},
			newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", ""),
			false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := tc.getSetter(tc.target)
			canSkip, err := fn()
			require.NoError(t, err)
			require.Equal(t, tc.expectCanSkip, canSkip)
		})
	}
}

// TestIntSetterSignedness tests datum kind (KindInt64 vs KindUint64) and
// cross-sign skip-cast behavior for integer default branches.
func TestIntSetterSignedness(t *testing.T) {
	t.Run("INT64 Uint8 to signed target: cross-sign, no range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, 0, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(200, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(200), d.GetUint64())
	})

	t.Run("INT64 Uint8 to unsigned target: same-sign, range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Uint8}
		target := newParquetTargetColumnInfo(mysql.TypeShort, mysql.UnsignedFlag, 6, 0, "", "")
		setter := getInt64Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(200, &d)
		require.NoError(t, err)
		require.True(t, canSkip)
		require.Equal(t, types.KindUint64, d.Kind())
		require.Equal(t, uint64(200), d.GetUint64())
	})

	t.Run("INT32 signed to unsigned target: cross-sign, no range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, mysql.UnsignedFlag, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(42, &d)
		require.NoError(t, err)
		require.False(t, canSkip)
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(42), d.GetInt64())
	})

	t.Run("INT32 signed to signed target: same-sign, range check", func(t *testing.T) {
		converted := &columnType{converted: schema.ConvertedTypes.Int32}
		target := newParquetTargetColumnInfo(mysql.TypeLong, 0, 10, 0, "", "")
		setter := getInt32Setter(converted, time.UTC, target)
		var d types.Datum
		canSkip, err := setter(-42, &d)
		require.NoError(t, err)
		require.True(t, canSkip)
		require.Equal(t, types.KindInt64, d.Kind())
		require.Equal(t, int64(-42), d.GetInt64())
	})
}

// TestStringCheckFunc tests the string post-check logic (encoding validation, length).
func TestStringCheckFunc(t *testing.T) {
	utf8Enc := charset.FindEncoding(charset.CharsetUTF8MB4)

	t.Run("valid utf8 within length", func(t *testing.T) {
		require.True(t, stringCheckFunc([]byte("hello"), 10, utf8Enc))
	})

	t.Run("valid utf8 exceeds char length", func(t *testing.T) {
		require.False(t, stringCheckFunc([]byte("hello"), 3, utf8Enc))
	})

	t.Run("multi-byte within char length", func(t *testing.T) {
		require.True(t, stringCheckFunc([]byte("你好"), 5, utf8Enc)) // 2 chars, 6 bytes
	})

	t.Run("invalid utf8 fails", func(t *testing.T) {
		require.False(t, stringCheckFunc([]byte{0xff, 0xfe}, 100, utf8Enc))
	})

	t.Run("varbinary accepts any bytes", func(t *testing.T) {
		binEnc := charset.FindEncoding("binary")
		require.True(t, stringCheckFunc([]byte{0xff, 0xfe, 0x00}, 100, binEnc))
	})

	t.Run("varbinary exceeds byte length", func(t *testing.T) {
		binEnc := charset.FindEncoding("binary")
		require.False(t, stringCheckFunc([]byte{0xff, 0xfe, 0x00}, 2, binEnc))
	})

	t.Run("varbinary multi-byte utf8 exceeds byte flen", func(t *testing.T) {
		// 'é' = [0xC3, 0xA9]: 2 bytes, 1 rune. With flen=1 (byte count for
		// binary), must return false even though RuneCount(b)=1 <= 1.
		binEnc := charset.FindEncoding("binary")
		require.False(t, stringCheckFunc([]byte{0xC3, 0xA9}, 1, binEnc))
	})

	t.Run("negative flen means unlimited", func(t *testing.T) {
		require.True(t, stringCheckFunc([]byte("any length string"), -1, utf8Enc))
	})
}

// TestDecimalCheckFunc tests the decimal post-check logic via getDecimalCheckFunc.
func TestDecimalCheckFunc(t *testing.T) {
	t.Run("fits exactly", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 5, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123.45")))
		d := types.NewDecimalDatum(dec)
		require.True(t, check(&d))
	})

	t.Run("precision overflow", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 5, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 5, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("123456.78")))
		d := types.NewDecimalDatum(dec)
		require.False(t, check(&d))
	})

	t.Run("negative into unsigned", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, mysql.UnsignedFlag, 10, 2, "", ""),
		)
		dec := new(types.MyDecimal)
		require.NoError(t, dec.FromString([]byte("-1.00")))
		d := types.NewDecimalDatum(dec)
		require.False(t, check(&d))
	})

	t.Run("frac mismatch returns nil", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 1},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 10, 2, "", ""),
		)
		require.Nil(t, check)
	})

	t.Run("null datum returns false", func(t *testing.T) {
		check := getDecimalCheckFunc(
			schema.DecimalMetadata{IsSet: true, Precision: 10, Scale: 2},
			newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 10, 2, "", ""),
		)
		d := types.Datum{}
		require.True(t, d.IsNull())
		require.False(t, check(&d))
	})
}
