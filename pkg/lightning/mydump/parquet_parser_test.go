// Copyright 2023 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newParquetParserForTest(
	ctx context.Context,
	t *testing.T,
	dir string,
	fileName string,
	meta ParquetFileMeta,
) *ParquetParser {
	t.Helper()

	store, err := objstore.NewLocalStorage(dir)
	require.NoError(t, err)

	r, err := store.Open(ctx, fileName, nil)
	require.NoError(t, err)

	parser, err := NewParquetParser(ctx, store, r, fileName, meta)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, parser.Close())
	})
	return parser
}

func newInt96FromUnixNanos(nanoseconds int64) parquet.Int96 {
	nanosPerDay := int64(24 * time.Hour)
	dayOffset := floorDivInt64(nanoseconds, nanosPerDay)
	nanosOfDay := floorModInt64(nanoseconds, nanosPerDay)
	day := uint32(dayOffset + julianDayOfUnixEpoch)
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], uint64(nanosOfDay))
	binary.LittleEndian.PutUint32(b[8:], day)
	return parquet.Int96(b)
}

func TestParquetParser(t *testing.T) {
	pc := []ParquetColumn{
		{
			Name:      "sS",
			Type:      parquet.Types.ByteArray,
			Converted: schema.ConvertedTypes.UTF8,
			Gen: func(numRows int) (any, []int16) {
				defLevel := make([]int16, numRows)
				data := make([]parquet.ByteArray, numRows)
				for i := range numRows {
					s := strconv.Itoa(i)
					data[i] = parquet.ByteArray(s)
					defLevel[i] = 1
				}
				return data, defLevel
			},
		},
		{
			Name:      "a_A",
			Type:      parquet.Types.Int64,
			Converted: schema.ConvertedTypes.Int64,
			Gen: func(numRows int) (any, []int16) {
				defLevel := make([]int16, numRows)
				data := make([]int64, numRows)
				for i := range numRows {
					data[i] = int64(i)
					defLevel[i] = 1
				}
				return data, defLevel
			},
		},
	}

	dir := t.TempDir()
	name := "test123.parquet"
	WriteParquetFile(dir, name, pc, 100)

	reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{})

	require.Equal(t, []string{"ss", "a_a"}, reader.Columns())

	verifyRow := func(i int) {
		require.Equal(t, int64(i+1), reader.lastRow.RowID)
		require.Len(t, reader.lastRow.Row, 2)
		require.Equal(t, types.NewCollationStringDatum(strconv.Itoa(i), "utf8mb4_bin"), reader.lastRow.Row[0])
		require.Equal(t, types.NewIntDatum(int64(i)), reader.lastRow.Row[1])
	}

	// test read some rows
	for i := range 10 {
		require.NoError(t, reader.ReadRow())
		verifyRow(i)
	}

	require.NoError(t, reader.SetPos(15, 15))
	require.NoError(t, reader.ReadRow())
	verifyRow(15)

	require.NoError(t, reader.SetPos(80, 80))
	for i := 80; i < 100; i++ {
		require.NoError(t, reader.ReadRow())
		verifyRow(i)
	}

	require.ErrorIs(t, reader.ReadRow(), io.EOF)

	t.Run("unsupported_writer_option_is_rejected_before_create", func(t *testing.T) {
		dir := t.TempDir()
		fileName := "unsupported-option.parquet"
		err := WriteParquetFile(dir, fileName, pc, 1, struct{}{})
		require.ErrorContains(t, err, "unsupported parquet writer option type")

		_, statErr := os.Stat(filepath.Join(dir, fileName))
		require.True(t, os.IsNotExist(statErr), "unexpected file state: %v", statErr)
	})
}

func TestParquetParserMultipleRowGroup(t *testing.T) {
	pc := []ParquetColumn{
		{
			Name:      "v",
			Type:      parquet.Types.Int64,
			Converted: schema.ConvertedTypes.Int64,
			Gen: func(numRows int) (any, []int16) {
				defLevel := make([]int16, numRows)
				data := make([]int64, numRows)
				for i := range numRows {
					defLevel[i] = 1
					data[i] = int64(i)
				}
				return data, defLevel
			},
		},
	}

	dir := t.TempDir()
	fileName := "multi-row-group.parquet"
	err := WriteParquetFile(
		dir,
		fileName,
		pc,
		50,
		parquet.WithMaxRowGroupLength(9),
	)
	require.NoError(t, err)

	parser := newParquetParserForTest(context.Background(), t, dir, fileName, ParquetFileMeta{})

	for i := range 50 {
		require.NoError(t, parser.ReadRow())
		require.Equal(t, int64(i), parser.LastRow().Row[0].GetInt64())
		last := parser.LastRow()
		parser.RecycleRow(last)
	}
	require.ErrorIs(t, parser.ReadRow(), io.EOF)
}

func TestParquetVariousTypes(t *testing.T) {
	t.Run("timestamp_and_decimal", func(t *testing.T) {
		pc := []ParquetColumn{
			{
				Name:      "date",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Date,
				Gen: func(_ int) (any, []int16) {
					return []int32{18564}, []int16{1} // 2020-10-29
				},
			},
			{
				Name:      "timemillis",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.TimeMillis,
				Gen: func(_ int) (any, []int16) {
					return []int32{62775123}, []int16{1} // 1970-01-01 17:26:15.123Z
				},
			},
			{
				Name:      "timemicros",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.TimeMicros,
				Gen: func(_ int) (any, []int16) {
					return []int64{62775123456}, []int16{1} // 1970-01-01 17:26:15.123456Z
				},
			},
			{
				Name:      "timestampmillis",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.TimestampMillis,
				Gen: func(_ int) (any, []int16) {
					return []int64{1603963672356}, []int16{1} // 2020-10-29T09:27:52.356Z
				},
			},
			{
				Name:      "timestampmicros",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.TimestampMicros,
				Gen: func(_ int) (any, []int16) {
					return []int64{1603963672356956}, []int16{1} // 2020-10-29T09:27:52.356956Z
				},
			},
			{
				Name:      "timestampmicros2",
				Type:      parquet.Types.Int96,
				Converted: schema.ConvertedTypes.None,
				Gen: func(_ int) (any, []int16) {
					// also 2020-10-29T09:27:52.356956Z, but stored in Int96
					return []parquet.Int96{newInt96(1603963672356956)}, []int16{1}
				},
			},
			{
				Name:      "decimal1",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 9,
				Scale:     2,
				Gen: func(_ int) (any, []int16) {
					return []int32{-12345678}, []int16{1} // -123456.78,
				},
			},
			{
				Name:      "decimal2",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 4,
				Scale:     4,
				Gen: func(_ int) (any, []int16) {
					return []int32{456}, []int16{1} // 0.0456
				},
			},
			{
				Name:      "decimal3",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 18,
				Scale:     2,
				Gen: func(_ int) (any, []int16) {
					return []int64{123456789012345678}, []int16{1} // 1234567890123456.78,
				},
			},
			{
				Name:      "decimal6",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 4,
				Scale:     4,
				Gen: func(_ int) (any, []int16) {
					return []int32{-1}, []int16{1} // -0.0001
				},
			},
		}

		dir := t.TempDir()
		name := "test123.parquet"
		WriteParquetFile(dir, name, pc, 1)

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: time.UTC})

		require.Len(t, reader.colNames, 10)
		require.NoError(t, reader.ReadRow())

		// TODO(joechenrh): for now we don't find a simple way to convert int directly
		// to decimal datum, so here we just check the string values.
		// Remember to also update the expected values below if the implementation changes.
		expectedStringValues := []string{
			"2020-10-29",
			"1970-01-01 17:26:15.123Z",
			"1970-01-01 17:26:15.123456Z",
			"2020-10-29 09:27:52.356Z",
			"2020-10-29 09:27:52.356956Z",
			"2020-10-29 09:27:52.356956Z",
			"-123456.78", "0.0456", "1234567890123456.78", "-0.0001",
		}
		expectedTypes := []byte{
			mysql.TypeDate, mysql.TypeTimestamp, mysql.TypeTimestamp,
			mysql.TypeTimestamp, mysql.TypeTimestamp, mysql.TypeTimestamp,
			mysql.TypeNewDecimal, mysql.TypeNewDecimal, mysql.TypeNewDecimal, mysql.TypeNewDecimal,
		}

		row := reader.lastRow.Row
		require.Len(t, expectedStringValues, len(row))

		for i, s := range expectedStringValues {
			if expectedTypes[i] == mysql.TypeNewDecimal {
				require.Equal(t, s, row[i].GetMysqlDecimal().String())
				continue
			}
			tp := types.NewFieldType(expectedTypes[i])
			if expectedTypes[i] == mysql.TypeTimestamp {
				tp.SetDecimal(6)
			}
			expectedDatum, err := table.CastColumnValueWithStrictMode(types.NewStringDatum(s), tp)
			require.NoError(t, err)
			require.Equal(t, expectedDatum, row[i])
		}
	})

	t.Run("logical_timestamp_adjustment", func(t *testing.T) {
		asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)

		const timestampMillis = int64(1603963672356)
		const timestampMicros = int64(1603963672356956)
		pc := []ParquetColumn{
			{
				Name:    "timestamp_millis_instant",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimestampLogicalType(true, schema.TimeUnitMillis),
				Gen: func(_ int) (any, []int16) {
					return []int64{timestampMillis}, []int16{1}
				},
			},
			{
				Name:    "timestamp_micros_instant",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimestampLogicalType(true, schema.TimeUnitMicros),
				Gen: func(_ int) (any, []int16) {
					return []int64{timestampMicros}, []int16{1}
				},
			},
			{
				Name:    "timestamp_millis_local",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimestampLogicalType(false, schema.TimeUnitMillis),
				Gen: func(_ int) (any, []int16) {
					return []int64{timestampMillis}, []int16{1}
				},
			},
			{
				Name:    "timestamp_micros_local",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimestampLogicalType(false, schema.TimeUnitMicros),
				Gen: func(_ int) (any, []int16) {
					return []int64{timestampMicros}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "logical-timestamps.parquet"
		require.NoError(t, WriteParquetFile(dir, name, pc, 1))

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: asiaShanghai})
		require.Equal(t, schema.ConvertedTypes.TimestampMillis, reader.colTypes[0].converted)
		require.Equal(t, schema.ConvertedTypes.TimestampMicros, reader.colTypes[1].converted)
		require.Equal(t, schema.ConvertedTypes.TimestampMillis, reader.colTypes[2].converted)
		require.Equal(t, schema.ConvertedTypes.TimestampMicros, reader.colTypes[3].converted)
		require.True(t, reader.colTypes[0].IsAdjustedToUTC)
		require.True(t, reader.colTypes[1].IsAdjustedToUTC)
		require.False(t, reader.colTypes[2].IsAdjustedToUTC)
		require.False(t, reader.colTypes[3].IsAdjustedToUTC)

		require.NoError(t, reader.ReadRow())
		row := reader.lastRow.Row
		require.Len(t, row, 4)
		expected := []time.Time{
			time.Date(2020, 10, 29, 17, 27, 52, 356000000, time.UTC),
			time.Date(2020, 10, 29, 17, 27, 52, 356956000, time.UTC),
			time.Date(2020, 10, 29, 9, 27, 52, 356000000, time.UTC),
			time.Date(2020, 10, 29, 9, 27, 52, 356956000, time.UTC),
		}
		for i, want := range expected {
			got, err := row[i].GetMysqlTime().GoTime(time.UTC)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	})

	t.Run("logical_time_local_semantics", func(t *testing.T) {
		asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)

		const timeMillis = int32(123456)
		const timeMicros = int64(123456789)
		pc := []ParquetColumn{
			{
				Name:    "time_millis_local",
				Type:    parquet.Types.Int32,
				Logical: schema.NewTimeLogicalType(false, schema.TimeUnitMillis),
				Gen: func(_ int) (any, []int16) {
					return []int32{timeMillis}, []int16{1}
				},
			},
			{
				Name:    "time_micros_local",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimeLogicalType(false, schema.TimeUnitMicros),
				Gen: func(_ int) (any, []int16) {
					return []int64{timeMicros}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "logical-time-local.parquet"
		require.NoError(t, WriteParquetFile(dir, name, pc, 1))

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: asiaShanghai})
		require.Equal(t, schema.ConvertedTypes.TimeMillis, reader.colTypes[0].converted)
		require.Equal(t, schema.ConvertedTypes.TimeMicros, reader.colTypes[1].converted)
		require.False(t, reader.colTypes[0].IsAdjustedToUTC)
		require.False(t, reader.colTypes[1].IsAdjustedToUTC)

		require.NoError(t, reader.ReadRow())
		row := reader.lastRow.Row
		require.Len(t, row, 2)
		expected := []time.Time{
			time.Date(1970, 1, 1, 0, 2, 3, 456000000, time.UTC),
			time.Date(1970, 1, 1, 0, 2, 3, 456789000, time.UTC),
		}
		for i, want := range expected {
			got, err := row[i].GetMysqlTime().GoTime(time.UTC)
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
	})

	t.Run("unsupported_logical_timestamp_nanos", func(t *testing.T) {
		pc := []ParquetColumn{
			{
				Name:    "timestamp_nanos",
				Type:    parquet.Types.Int64,
				Logical: schema.NewTimestampLogicalType(false, schema.TimeUnitNanos),
				Gen: func(_ int) (any, []int16) {
					return []int64{1603963672356956000}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "logical-timestamp-nanos.parquet"
		require.NoError(t, WriteParquetFile(dir, name, pc, 1))

		store, err := objstore.NewLocalStorage(dir)
		require.NoError(t, err)
		r, err := store.Open(context.Background(), name, nil)
		require.NoError(t, err)
		parser, err := NewParquetParser(context.Background(), store, r, name, ParquetFileMeta{Loc: time.UTC})
		require.ErrorContains(t, err, "unsupported timestamp time unit")
		require.Nil(t, parser)
	})

	t.Run("int96_rounds_sub_microsecond_precision", func(t *testing.T) {
		input := time.Date(2020, 10, 29, 9, 27, 52, 999999500, time.UTC)
		pc := []ParquetColumn{
			{
				Name:      "timestampint96",
				Type:      parquet.Types.Int96,
				Converted: schema.ConvertedTypes.None,
				Gen: func(_ int) (any, []int16) {
					return []parquet.Int96{newInt96FromUnixNanos(input.UnixNano())}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "int96-rounds-sub-microsecond.parquet"
		require.NoError(t, WriteParquetFile(dir, name, pc, 1))

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: time.UTC})
		require.Equal(t, "", reader.colTypes[0].sparkRebaseMicros.timeZoneID)
		require.NoError(t, reader.ReadRow())

		gotTime, err := reader.lastRow.Row[0].GetMysqlTime().GoTime(time.UTC)
		require.NoError(t, err)
		require.Equal(t, time.Date(2020, 10, 29, 9, 27, 53, 0, time.UTC), gotTime)
	})

	t.Run("spark_legacy_datetime_rebase", func(t *testing.T) {
		legacyMeta := metadata.NewKeyValueMetadata()
		require.NoError(t, legacyMeta.Append("org.apache.spark.version", "2.4.8-amzn-1"))
		require.NoError(t, legacyMeta.Append("org.apache.spark.timeZone", "UTC"))

		modernMeta := metadata.NewKeyValueMetadata()
		require.NoError(t, modernMeta.Append("org.apache.spark.version", "3.5.0-amzn-1"))
		require.NoError(t, modernMeta.Append("org.apache.spark.timeZone", "UTC"))

		spark30Meta := metadata.NewKeyValueMetadata()
		require.NoError(t, spark30Meta.Append("org.apache.spark.version", "3.0.3"))
		require.NoError(t, spark30Meta.Append("org.apache.spark.timeZone", "UTC"))

		testCases := []struct {
			name                      string
			meta                      metadata.KeyValueMetadata
			date                      string
			expected                  []time.Time
			expectedRebaseTimeZoneIDs []string
		}{
			{
				name: "legacy",
				meta: legacyMeta,
				date: "1582-10-04",
				expected: []time.Time{
					time.Date(1582, 10, 4, 12, 34, 56, 789000000, time.UTC),
					time.Date(1582, 10, 4, 12, 34, 56, 789123000, time.UTC),
					time.Date(1582, 10, 4, 12, 34, 56, 789123000, time.UTC),
				},
				expectedRebaseTimeZoneIDs: []string{"UTC", "UTC", "UTC", "UTC"},
			},
			{
				name: "spark_3_0_int96_legacy_datetime_proleptic",
				meta: spark30Meta,
				date: "1582-10-14",
				expected: []time.Time{
					time.Date(1582, 10, 14, 12, 34, 56, 789000000, time.UTC),
					time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC),
					time.Date(1582, 10, 4, 12, 34, 56, 789123000, time.UTC),
				},
				expectedRebaseTimeZoneIDs: []string{"", "", "", "UTC"},
			},
			{
				name: "modern",
				meta: modernMeta,
				date: "1582-10-14",
				expected: []time.Time{
					time.Date(1582, 10, 14, 12, 34, 56, 789000000, time.UTC),
					time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC),
					time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC),
				},
				expectedRebaseTimeZoneIDs: []string{"", "", "", ""},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				pc := []ParquetColumn{
					{
						Name:      "date",
						Type:      parquet.Types.Int32,
						Converted: schema.ConvertedTypes.Date,
						Gen: func(_ int) (any, []int16) {
							return []int32{int32(time.Date(1582, 10, 14, 0, 0, 0, 0, time.UTC).Unix() / 86400)}, []int16{1}
						},
					},
					{
						Name:      "timestampmillis",
						Type:      parquet.Types.Int64,
						Converted: schema.ConvertedTypes.TimestampMillis,
						Gen: func(_ int) (any, []int16) {
							return []int64{time.Date(1582, 10, 14, 12, 34, 56, 789000000, time.UTC).UnixMilli()}, []int16{1}
						},
					},
					{
						Name:      "timestampmicros",
						Type:      parquet.Types.Int64,
						Converted: schema.ConvertedTypes.TimestampMicros,
						Gen: func(_ int) (any, []int16) {
							return []int64{time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC).UnixMicro()}, []int16{1}
						},
					},
					{
						Name:      "timestampint96",
						Type:      parquet.Types.Int96,
						Converted: schema.ConvertedTypes.None,
						Gen: func(_ int) (any, []int16) {
							return []parquet.Int96{newInt96(time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC).UnixMicro())}, []int16{1}
						},
					},
				}

				dir := t.TempDir()
				name := "legacy-timestamp.parquet"
				require.NoError(t,
					WriteParquetFile(
						dir, name, pc, 1,
						parquet.WithCreatedBy("parquet-mr version 1.10.1"),
						file.WithWriteMetadata(tc.meta),
					),
				)

				reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: time.UTC})
				require.NotNil(t, reader.fileMeta.KeyValueMetadata().FindValue("org.apache.spark.version"))
				if tc.name == "legacy" {
					version := sparkVersionFromMetadata(reader.fileMeta)
					require.NotNil(t, version)
					require.Equal(t, "spark", version.App)
					require.True(t, version.LessThan(sparkDatetimeRebaseCutoff))
				}
				for i, expectedID := range tc.expectedRebaseTimeZoneIDs {
					require.Equal(t, expectedID, reader.colTypes[i].sparkRebaseMicros.timeZoneID)
				}
				require.NoError(t, reader.ReadRow())
				row := reader.lastRow.Row
				require.Len(t, row, 4)

				require.Equal(t, tc.date, row[0].GetMysqlTime().String())

				for i, expected := range tc.expected {
					gotTime, err := row[i+1].GetMysqlTime().GoTime(time.UTC)
					require.NoError(t, err)
					require.Equal(t, expected, gotTime)
				}
			})
		}
	})

	t.Run("spark_legacy_date_switches", func(t *testing.T) {
		testCases := []struct {
			name     string
			days     int
			expected int
		}{
			{
				// Spark's first positive-year legacy DATE switch: hybrid 0100-03-01
				// rebases from -682945 to -682944.
				name:     "first_positive_year_switch",
				days:     -682945,
				expected: -682944,
			},
			{
				name:     "zero_delta_range",
				days:     -646420,
				expected: -646420,
			},
			{
				name:     "cutover_day",
				days:     int(time.Date(1582, 10, 15, 0, 0, 0, 0, time.UTC).Unix() / 86400),
				expected: int(time.Date(1582, 10, 15, 0, 0, 0, 0, time.UTC).Unix() / 86400),
			},
			{
				name:     "post_cutover_date",
				days:     int(time.Date(1700, 3, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400),
				expected: int(time.Date(1700, 3, 1, 0, 0, 0, 0, time.UTC).Unix() / 86400),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				require.Equal(t, tc.expected, rebaseJulianToGregorianDays(tc.days))
			})
		}
	})

	t.Run("legacy_timestamp_rebase_utc", func(t *testing.T) {
		loadedUTC, err := time.LoadLocation(sparkRebaseDefaultTimeZoneID)
		require.NoError(t, err)

		legacyMicros := time.Date(1582, 10, 14, 12, 34, 56, 789123000, time.UTC).UnixMicro()
		expected := time.Date(1582, 10, 4, 12, 34, 56, 789123000, time.UTC).UnixMicro()

		for _, tc := range []struct {
			name       string
			timeZoneID string
		}{
			{name: "utc", timeZoneID: sparkRebaseDefaultTimeZoneID},
			{name: "loaded_utc", timeZoneID: loadedUTC.String()},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rebased, err := rebaseSparkJulianToGregorianMicros(tc.timeZoneID, legacyMicros)
				require.NoError(t, err)
				require.Equal(t, expected, rebased)
			})
		}

		modernMicros := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
		rebased, err := rebaseSparkJulianToGregorianMicros(sparkRebaseDefaultTimeZoneID, modernMicros)
		require.NoError(t, err)
		require.Equal(t, modernMicros, rebased)
	})

	t.Run("legacy_timestamp_rebase_non_utc", func(t *testing.T) {
		testCases := []struct {
			timeZoneID string
			micros     int64
			expected   int64
		}{
			{
				timeZoneID: "Africa/Abidjan",
				micros:     time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro(),
				expected:   time.Date(1400, 3, 1, 0, 16, 8, 0, time.UTC).UnixMicro(),
			},
			{
				timeZoneID: "SystemV/AST4",
				micros:     time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro(),
				expected:   time.Date(1400, 3, 2, 0, 0, 0, 0, time.UTC).UnixMicro(),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.timeZoneID, func(t *testing.T) {
				rebased, err := rebaseSparkJulianToGregorianMicros(tc.timeZoneID, tc.micros)
				require.NoError(t, err)
				require.Equal(t, tc.expected, rebased)
			})
		}
	})

	t.Run("spark_legacy_timestamp_rebase_uses_spark_zone_tables", func(t *testing.T) {
		testCases := []struct {
			timeZoneID string
			expected   time.Time
		}{
			{
				timeZoneID: "Africa/Abidjan",
				expected:   time.Date(1400, 3, 1, 0, 16, 8, 0, time.UTC),
			},
			{
				timeZoneID: "SystemV/AST4",
				expected:   time.Date(1400, 3, 2, 0, 0, 0, 0, time.UTC),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.timeZoneID, func(t *testing.T) {
				legacyMeta := metadata.NewKeyValueMetadata()
				require.NoError(t, legacyMeta.Append("org.apache.spark.version", "2.4.8"))
				require.NoError(t, legacyMeta.Append("org.apache.spark.timeZone", tc.timeZoneID))

				legacyMicros := time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro()
				pc := []ParquetColumn{
					{
						Name:      "timestampmillis",
						Type:      parquet.Types.Int64,
						Converted: schema.ConvertedTypes.TimestampMillis,
						Gen: func(_ int) (any, []int16) {
							return []int64{legacyMicros / 1000}, []int16{1}
						},
					},
					{
						Name:      "timestampmicros",
						Type:      parquet.Types.Int64,
						Converted: schema.ConvertedTypes.TimestampMicros,
						Gen: func(_ int) (any, []int16) {
							return []int64{legacyMicros}, []int16{1}
						},
					},
					{
						Name:      "timestampint96",
						Type:      parquet.Types.Int96,
						Converted: schema.ConvertedTypes.None,
						Gen: func(_ int) (any, []int16) {
							return []parquet.Int96{newInt96(legacyMicros)}, []int16{1}
						},
					},
				}

				dir := t.TempDir()
				name := "legacy-timestamp-zone-table.parquet"
				require.NoError(t,
					WriteParquetFile(
						dir, name, pc, 1,
						parquet.WithCreatedBy("parquet-mr version 1.10.1"),
						file.WithWriteMetadata(legacyMeta),
					),
				)

				reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: time.UTC})
				for _, colType := range reader.colTypes {
					require.Equal(t, tc.timeZoneID, colType.sparkRebaseMicros.timeZoneID)
				}
				require.NoError(t, reader.ReadRow())
				row := reader.lastRow.Row
				require.Len(t, row, 3)

				for _, datum := range row {
					gotTime, err := datum.GetMysqlTime().GoTime(time.UTC)
					require.NoError(t, err)
					require.Equal(t, tc.expected, gotTime)
				}
			})
		}
	})

	t.Run("spark_legacy_timestamp_default_zone_exists_in_table", func(t *testing.T) {
		_, ok := sparkJulianGregorianRebaseMicrosIndex(sparkRebaseDefaultTimeZoneID)
		require.True(t, ok)
	})

	t.Run("spark_legacy_timestamp_rebase_uses_utc_when_zone_table_is_missing", func(t *testing.T) {
		legacyMeta := metadata.NewKeyValueMetadata()
		require.NoError(t, legacyMeta.Append("org.apache.spark.version", "2.4.8"))
		require.NoError(t, legacyMeta.Append("org.apache.spark.timeZone", "Unknown/Zone"))

		legacyMicros := time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro()
		pc := []ParquetColumn{
			{
				Name:      "timestampmicros",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.TimestampMicros,
				Gen: func(_ int) (any, []int16) {
					return []int64{legacyMicros}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "legacy-timestamp-unknown-zone.parquet"
		require.NoError(t,
			WriteParquetFile(
				dir, name, pc, 1,
				parquet.WithCreatedBy("parquet-mr version 1.10.1"),
				file.WithWriteMetadata(legacyMeta),
			),
		)

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{Loc: time.UTC})
		require.Equal(t, sparkRebaseDefaultTimeZoneID, reader.colTypes[0].sparkRebaseMicros.timeZoneID)
		require.NoError(t, reader.ReadRow())

		gotTime, err := reader.lastRow.Row[0].GetMysqlTime().GoTime(time.UTC)
		require.NoError(t, err)
		require.Equal(t, time.Date(1400, 3, 1, 0, 0, 0, 0, time.UTC), gotTime)
	})

	t.Run("spark_legacy_timestamp_rebase_uses_effective_parser_location_when_footer_zone_missing", func(t *testing.T) {
		asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)
		origLocal := time.Local
		time.Local = asiaShanghai
		t.Cleanup(func() {
			time.Local = origLocal
		})

		legacyMeta := metadata.NewKeyValueMetadata()
		require.NoError(t, legacyMeta.Append("org.apache.spark.version", "2.4.8"))

		pc := []ParquetColumn{
			{
				Name:      "timestampmicros",
				Type:      parquet.Types.Int64,
				Converted: schema.ConvertedTypes.TimestampMicros,
				Gen: func(_ int) (any, []int16) {
					return []int64{time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro()}, []int16{1}
				},
			},
		}

		dir := t.TempDir()
		name := "legacy-timestamp-system-zone.parquet"
		require.NoError(t,
			WriteParquetFile(
				dir, name, pc, 1,
				parquet.WithCreatedBy("parquet-mr version 1.10.1"),
				file.WithWriteMetadata(legacyMeta),
			),
		)

		reader := newParquetParserForTest(context.Background(), t, dir, name, ParquetFileMeta{})
		require.Equal(t, "Asia/Shanghai", reader.loc.String())
		require.Equal(t, "Asia/Shanghai", reader.colTypes[0].sparkRebaseMicros.timeZoneID)
	})

	t.Run("spark_legacy_timestamp_before_table_range_uses_hybrid_calendar_fallback", func(t *testing.T) {
		index, ok := sparkJulianGregorianRebaseMicrosIndex(sparkRebaseDefaultTimeZoneID)
		require.True(t, ok)
		switches, _ := sparkJulianGregorianRebaseMicrosSlices(index)
		require.NotEmpty(t, switches)

		testCases := []struct {
			name     string
			micros   int64
			expected int64
		}{
			{
				name:     "common_era_boundary",
				micros:   switches[0] - 1,
				expected: time.Date(0, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro(),
			},
			{
				name:     "julian_only_leap_day",
				micros:   -65317968000000001,
				expected: time.Date(-100, 3, 1, 23, 59, 59, 999999000, time.UTC).UnixMicro(),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rebased, err := rebaseSparkJulianToGregorianMicros(sparkRebaseDefaultTimeZoneID, tc.micros)
				require.NoError(t, err)
				require.Equal(t, tc.expected, rebased)
			})
		}
	})

	t.Run("decimal_with_nulls", func(t *testing.T) {
		pc := []ParquetColumn{
			{
				Name:      "decimal1",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 5,
				Scale:     3,
				Gen: func(_ int) (any, []int16) {
					return []int32{0, 1000, int32(-1000), 999, int32(-999), 1, int32(-1)}, []int16{1, 1, 1, 1, 1, 1, 1}
				},
			},
			{
				Name:      "decimal2",
				Type:      parquet.Types.Int32,
				Converted: schema.ConvertedTypes.Decimal,
				Precision: 5,
				Scale:     3,
				Gen: func(_ int) (any, []int16) {
					return []int32{0, int32(-1000), int32(-999), int32(-1), 0, 0, 0}, []int16{1, 0, 1, 0, 1, 0, 1}
				},
			},
		}

		expectedValues := []string{
			"0.000", "1.000", "-1.000", "0.999",
			"-0.999", "0.001", "-0.001",
		}

		dir := t.TempDir()
		fileName := "test.02.parquet"
		WriteParquetFile(dir, fileName, pc, 7)

		reader := newParquetParserForTest(context.Background(), t, dir, fileName, ParquetFileMeta{})

		for i, expectValue := range expectedValues {
			assert.NoError(t, reader.ReadRow())
			require.Len(t, reader.lastRow.Row, 2)

			s, err := reader.lastRow.Row[0].ToString()
			require.NoError(t, err)
			assert.Equal(t, expectValue, s)
			if i%2 == 1 {
				require.True(t, reader.lastRow.Row[1].IsNull())
			} else {
				s, err = reader.lastRow.Row[1].ToString()
				require.NoError(t, err)
				assert.Equal(t, expectValue, s)
			}
		}
	})

	t.Run("boolean", func(t *testing.T) {
		pc := []ParquetColumn{
			{
				Name:      "bool_val",
				Type:      parquet.Types.Boolean,
				Converted: schema.ConvertedTypes.None,
				Gen: func(_ int) (any, []int16) {
					return []bool{false, true}, []int16{1, 1}
				},
			},
		}

		dir := t.TempDir()
		fileName := "test.bool.parquet"
		WriteParquetFile(dir, fileName, pc, 2)

		reader := newParquetParserForTest(context.Background(), t, dir, fileName, ParquetFileMeta{})

		// because we always reuse the datums in reader.lastRow.Row, so we can't directly
		// compare will `DeepEqual` here
		assert.NoError(t, reader.ReadRow())
		assert.Equal(t, types.KindUint64, reader.lastRow.Row[0].Kind())
		assert.Equal(t, uint64(0), reader.lastRow.Row[0].GetValue())
		assert.NoError(t, reader.ReadRow())
		assert.Equal(t, types.KindUint64, reader.lastRow.Row[0].Kind())
		assert.Equal(t, uint64(1), reader.lastRow.Row[0].GetValue())
	})
}

// benchmarkRebasedSparkMicros keeps the rebased value observable so the
// compiler cannot discard the calculation while optimizing the benchmark.
var benchmarkRebasedSparkMicros int64

func BenchmarkRebaseSparkJulianToGregorianMicros(b *testing.B) {
	index, ok := sparkJulianGregorianRebaseMicrosIndex(sparkRebaseDefaultTimeZoneID)
	require.True(b, ok)
	switches, _ := sparkJulianGregorianRebaseMicrosSlices(index)
	require.NotEmpty(b, switches)
	lookup, err := newSparkRebaseMicrosLookup(sparkRebaseDefaultTimeZoneID)
	require.NoError(b, err)

	tableMicros := time.Date(1400, 3, 10, 0, 0, 0, 0, time.UTC).UnixMicro()
	require.GreaterOrEqual(b, tableMicros, switches[0])
	require.Less(b, tableMicros, legacyTimestampRebaseCutoffMicros)

	for _, tc := range []struct {
		name   string
		micros int64
	}{
		{name: "Table", micros: tableMicros},
		{name: "BeforeSwitch", micros: switches[0] - 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var rebased int64
			for b.Loop() {
				var err error
				rebased, err = lookup.rebase(tc.micros)
				if err != nil {
					b.Fatal(err)
				}
			}
			benchmarkRebasedSparkMicros = rebased
		})
	}
}

func TestParquetAurora(t *testing.T) {
	fileName := "test.parquet"
	parser := newParquetParserForTest(context.TODO(), t, "examples", fileName, ParquetFileMeta{})

	require.Equal(t, []string{"id", "val1", "val2", "d1", "d2", "d3", "d4", "d5", "d6"}, parser.Columns())

	expectedRes := [][]any{
		{int64(1), int64(1), "0", int64(123), "1.23", "0.00000001", "1234567890", "123", "1.23000000"},
		{
			int64(2), int64(123456), "0", int64(123456), "9999.99", "0.12345678", "99999999999999999999",
			"999999999999999999999999999999999999", "99999999999999999999.99999999",
		},
		{
			int64(3), int64(123456), "0", int64(-123456), "-9999.99", "-0.12340000", "-99999999999999999999",
			"-999999999999999999999999999999999999", "-99999999999999999999.99999999",
		},
		{
			int64(4), int64(1), "0", int64(123), "1.23", "0.00000001", "1234567890", "123", "1.23000000",
		},
		{
			int64(5), int64(123456), "0", int64(123456), "9999.99", "0.12345678", "12345678901234567890",
			"123456789012345678901234567890123456", "99999999999999999999.99999999",
		},
		{
			int64(6), int64(123456), "0", int64(-123456), "-9999.99", "-0.12340000",
			"-12345678901234567890", "-123456789012345678901234567890123456",
			"-99999999999999999999.99999999",
		},
	}

	for i := range expectedRes {
		err := parser.ReadRow()
		assert.NoError(t, err)
		expectedValues := expectedRes[i]
		row := parser.LastRow().Row
		assert.Len(t, expectedValues, len(row))
		for j := range row {
			switch v := expectedValues[j].(type) {
			case int64:
				s, err := row[j].ToString()
				require.NoError(t, err)
				require.Equal(t, strconv.Itoa(int(v)), s)
			case string:
				s, err := row[j].ToString()
				require.NoError(t, err)
				require.Equal(t, v, s)
			default:
				t.Fatal("unexpected value: ", expectedValues[j])
			}
		}
	}

	require.ErrorIs(t, parser.ReadRow(), io.EOF)
}

func TestHiveParquetParser(t *testing.T) {
	name := "000000_0.parquet"
	dir := "./parquet/"
	reader := newParquetParserForTest(context.TODO(), t, dir, name, ParquetFileMeta{Loc: time.UTC})
	// UTC+0:00
	results := []time.Time{
		time.Date(2022, 9, 10, 9, 9, 0, 0, time.UTC),
		time.Date(1997, 8, 11, 2, 1, 10, 0, time.UTC),
		time.Date(1995, 12, 31, 23, 0, 1, 0, time.UTC),
		time.Date(2020, 2, 29, 23, 0, 0, 0, time.UTC),
		time.Date(2038, 1, 19, 0, 0, 0, 0, time.UTC),
	}

	for i := range 5 {
		err := reader.ReadRow()
		require.NoError(t, err)
		lastRow := reader.LastRow()
		require.Equal(t, 2, len(lastRow.Row))
		require.Equal(t, types.KindMysqlTime, lastRow.Row[1].Kind())
		ts, err := lastRow.Row[1].GetMysqlTime().GoTime(time.UTC)
		require.NoError(t, err)
		require.Equal(t, results[i], ts)
	}
}

func TestNsecOutSideRange(t *testing.T) {
	a := time.Date(2022, 9, 10, 9, 9, 0, 0, time.Now().Local().Location())
	b := time.Unix(a.Unix(), 1000000000)
	// For nano sec out of 999999999, time will automatically execute a
	// carry operation. i.e. 1000000000 nsec => 1 sec
	require.Equal(t, a.Add(1*time.Second), b)
}

var randChars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ")

func randString() parquet.ByteArray {
	lens := rand.Intn(64)
	s := make([]byte, lens)
	for i := range lens {
		s[i] = randChars[rand.Intn(len(randChars))]
	}
	return s
}

func TestBasicReadFile(t *testing.T) {
	rowCnt := 111
	generated := make([]parquet.ByteArray, 0, rowCnt)

	pc := []ParquetColumn{
		{
			Name:      "s",
			Type:      parquet.Types.ByteArray,
			Converted: schema.ConvertedTypes.UTF8,
			Gen: func(rows int) (any, []int16) {
				data := make([]parquet.ByteArray, rows)
				defLevels := make([]int16, rows)
				for i := range rows {
					s := randString()
					data[i] = s
					defLevels[i] = 1
					generated = append(generated, s)
				}
				return data, defLevels
			},
		},
	}

	dir := t.TempDir()
	fileName := "test123.parquet"
	// Genearte small file with multiple pages.
	// The number of rows in each page is not multiple of batch size.
	WriteParquetFile(dir, fileName, pc, rowCnt,
		parquet.WithDataPageSize(512),
		parquet.WithBatchSize(20),
		parquet.WithCompressionFor("s", compress.Codecs.Uncompressed),
	)

	origBatchSize := readBatchSize
	readBatchSize = 32
	defer func() {
		readBatchSize = origBatchSize
	}()

	reader := newParquetParserForTest(context.TODO(), t, dir, fileName, ParquetFileMeta{})
	for i := range rowCnt {
		require.NoError(t, reader.ReadRow())
		require.Equal(t, string(generated[i]), reader.lastRow.Row[0].GetString())
		require.NotNil(t, reader.rowGroup)
		require.Len(t, reader.rowGroup.readers, len(reader.lastRow.Row))
	}
}

func TestParquetParserWrapper(t *testing.T) {
	const (
		rowCnt   = 256
		groupCnt = 2
	)

	dir := t.TempDir()
	fileName := "fixed-len-byte-array.parquet"

	pc := []ParquetColumn{
		{
			Name:      "fixed",
			Type:      parquet.Types.FixedLenByteArray,
			Converted: schema.ConvertedTypes.None,
			TypeLen:   8,
			Gen: func(numRows int) (any, []int16) {
				vals := make([]parquet.FixedLenByteArray, numRows)
				defLevels := make([]int16, numRows)
				for i := range numRows {
					defLevels[i] = 1
					vals[i] = parquet.FixedLenByteArray(fmt.Sprintf("k%07d", i))
				}
				return vals, defLevels
			},
		},
		{
			Name:      "val",
			Type:      parquet.Types.Int64,
			Converted: schema.ConvertedTypes.Int64,
			Gen: func(numRows int) (any, []int16) {
				vals := make([]int64, numRows)
				defLevels := make([]int16, numRows)
				for i := range numRows {
					defLevels[i] = 1
					vals[i] = int64(i)
				}
				return vals, defLevels
			},
		},
	}
	err := WriteParquetFile(
		dir,
		fileName,
		pc,
		rowCnt,
		parquet.WithDataPageSize(256),
		parquet.WithBatchSize(32),
		parquet.WithMaxRowGroupLength(int64(rowCnt/groupCnt)),
	)
	require.NoError(t, err)

	readRows := func(threshold int) ([]string, []int64) {
		origThreshold := rowGroupInMemoryThreshold
		rowGroupInMemoryThreshold = threshold
		defer func() {
			rowGroupInMemoryThreshold = origThreshold
		}()

		parser := newParquetParserForTest(context.Background(), t, dir, fileName, ParquetFileMeta{})

		gotFixed := make([]string, 0, rowCnt)
		gotInt := make([]int64, 0, rowCnt)
		for range rowCnt {
			require.NoError(t, parser.ReadRow())
			last := parser.LastRow()
			gotFixed = append(gotFixed, last.Row[0].GetString())
			gotInt = append(gotInt, last.Row[1].GetInt64())
			parser.RecycleRow(last)
		}
		require.ErrorIs(t, parser.ReadRow(), io.EOF)
		return gotFixed, gotInt
	}

	gotFixedInMemory, gotIntInMemory := readRows(1 << 30)
	gotFixedOnDemand, gotIntOnDemand := readRows(1)
	require.Equal(t, gotFixedOnDemand, gotFixedInMemory)
	require.Equal(t, gotIntOnDemand, gotIntInMemory)

	for i := range rowCnt {
		require.Equal(t, fmt.Sprintf("k%07d", i), gotFixedInMemory[i])
		require.Equal(t, int64(i), gotIntInMemory[i])
	}
}

// getStringFromParquetByteOld is the previous implementation used to convert
// parquet byte to string. It's only used to generate expected results in tests.
func getStringFromParquetByteOld(rawBytes []byte, scale int) string {
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

func TestBinaryToDecimalStr(t *testing.T) {
	type testCase struct {
		rawBytes []byte
		scale    int
	}

	// Small basics.
	tcs := []testCase{
		{[]byte{0x01}, 3},
		{[]byte{0x05}, 0},
		{[]byte{0xff}, 0},
		{[]byte{0xff}, 3},
		{[]byte{0xff, 0xff}, 0},
		{[]byte{0x01}, 5},
		{[]byte{0x0a}, 1},
	}

	// Longer scales and longer inputs.
	pattern := []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
	makeFull := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = pattern[i%len(pattern)]
		}
		return b
	}
	zeroN := func(n int) []byte { return make([]byte, n) }
	oneN := func(n int) []byte { return append(make([]byte, n-1), 0x01) }
	negOneN := func(n int) []byte { return bytes.Repeat([]byte{0xff}, n) }
	negMinN := func(n int) []byte {
		b := make([]byte, n)
		b[0] = 0x80
		return b
	}

	// Bound total digits to avoid exceeding mysql.MaxDecimalWidth.
	appendCases := func(n int, scales []int) {
		for _, sc := range scales {
			tcs = append(tcs,
				testCase{zeroN(n), sc},
				testCase{oneN(n), sc},
				testCase{negOneN(n), sc},
				testCase{makeFull(n), sc},
			)
		}
	}

	// 16 bytes (~39 digits max) safe up to scale 32.
	appendCases(16, []int{8, 12, 16, 32})

	// Extra carry/edge patterns (kept to small scales).
	tcs = append(tcs,
		testCase{negMinN(16), 0},
		testCase{negMinN(16), 8},
	)
	for i, tc := range tcs {
		rawCopy := append([]byte(nil), tc.rawBytes...)
		rawCopy2 := append([]byte(nil), tc.rawBytes...)
		expected := getStringFromParquetByteOld(tc.rawBytes, tc.scale)

		// test string conversion
		strNew := getStringFromParquetByte(rawCopy, tc.scale)
		require.Equal(t, expected, string(strNew))

		// test MyDecimal conversion
		var dec types.MyDecimal
		err := dec.FromParquetArray(rawCopy2, tc.scale)
		require.NoError(t, err)
		require.Equal(t, expected, dec.String(),
			"raw=%x scale=%d idx=%d", tc.rawBytes, tc.scale, i)
	}
}

func TestParquetDecimalFromInt64(t *testing.T) {
	type testCase struct {
		value    int64
		scale    int32
		expected string
	}

	tcs := []testCase{
		{0, 0, "0"},
		{0, 3, "0.000"},
		{0, 6, "0.000000"},
		{123, 0, "123"},
		{123, 3, "0.123"},
		{-7, 0, "-7"},
		{-7, 2, "-0.07"},
		{1, 3, "0.001"},
		{10, 1, "1.0"},
		{-1, 4, "-0.0001"},
	}

	for _, tc := range tcs {
		// No decimal meta or scale=0: stored as int64.
		var dec types.MyDecimal
		err := setParquetDecimalFromInt64(tc.value, &dec,
			schema.DecimalMetadata{IsSet: true, Scale: tc.scale})
		require.NoError(t, err)
		require.Equal(t, tc.expected, dec.String())
	}

	// Test overflow
	var dec types.MyDecimal
	raw := bytes.Repeat([]byte{0x7f}, 40)
	require.ErrorIs(t, types.ErrOverflow, dec.FromParquetArray(raw, 0))
}

func TestTrackingAllocatorReallocatePreservesPrefix(t *testing.T) {
	allocator := &trackingAllocator{}
	buf := allocator.Allocate(8)
	require.Equal(t, uintptr(0), addressOf(buf)%allocatorAlignment)
	require.Equal(t, int64(8+allocatorAlignment), allocator.currentAllocation.Load())
	for i := range len(buf) {
		buf[i] = byte(i + 1)
	}

	grown := allocator.Reallocate(16, buf)
	require.Equal(t, 16, len(grown))
	require.Equal(t, uintptr(0), addressOf(grown)%allocatorAlignment)
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, grown[:8])
	require.Equal(t, int64(16+allocatorAlignment), allocator.currentAllocation.Load())
	require.Equal(t, int64(8+16+2*allocatorAlignment), allocator.peakAllocation.Load())

	shrunk := allocator.Reallocate(4, grown)
	require.Equal(t, 4, len(shrunk))
	require.Equal(t, []byte{1, 2, 3, 4}, shrunk)
	require.Equal(t, int64(16+allocatorAlignment), allocator.currentAllocation.Load())

	allocator.Free(shrunk)
	require.Equal(t, int64(0), allocator.currentAllocation.Load())
}

func TestTrackingAllocatorReallocateZeroLengthAndGrow(t *testing.T) {
	allocator := &trackingAllocator{}
	buf := allocator.Allocate(8)
	require.Equal(t, int64(8+allocatorAlignment), allocator.currentAllocation.Load())

	zero := allocator.Reallocate(0, buf)
	require.Len(t, zero, 0)
	// Reallocating to zero length doesn't free the backing allocation.
	require.Equal(t, int64(8+allocatorAlignment), allocator.currentAllocation.Load())

	grown := allocator.Reallocate(16, zero)
	require.Len(t, grown, 16)
	require.Equal(t, uintptr(0), addressOf(grown)%allocatorAlignment)
	require.Equal(t, int64(16+allocatorAlignment), allocator.currentAllocation.Load())
	require.Equal(t, int64(8+16+2*allocatorAlignment), allocator.peakAllocation.Load())

	allocator.Free(grown)
	require.Equal(t, int64(0), allocator.currentAllocation.Load())
}
