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
	"context"
<<<<<<< HEAD
	"io"
=======
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
	"path/filepath"
	"strconv"
	"testing"
	"time"

<<<<<<< HEAD
	"github.com/pingcap/tidb/br/pkg/storage"
=======
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	writer2 "github.com/xitongsys/parquet-go/writer"
)

<<<<<<< HEAD
=======
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

>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
func TestParquetParser(t *testing.T) {
	type Test struct {
		S string `parquet:"name=sS, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		A int32  `parquet:"name=a_A, type=INT32"`
	}

	dir := t.TempDir()
	// prepare data
	name := "test123.parquet"
	testPath := filepath.Join(dir, name)
	pf, err := local.NewLocalFileWriter(testPath)
	require.NoError(t, err)
	test := &Test{}
	writer, err := writer2.NewParquetWriter(pf, test, 2)
	require.NoError(t, err)

	for i := range 100 {
		test.A = int32(i)
		test.S = strconv.Itoa(i)
		require.NoError(t, writer.Write(test))
	}

	require.NoError(t, writer.WriteStop())
	require.NoError(t, pf.Close())

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	require.NoError(t, err)
	defer reader.Close()

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

	// test set pos to pos < curpos + batchReadRowSize
	require.NoError(t, reader.SetPos(15, 15))
	require.NoError(t, reader.ReadRow())
	verifyRow(15)

	// test set pos to pos > curpos + batchReadRowSize
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

func TestParquetVariousTypes(t *testing.T) {
	type Test struct {
		Date            int32 `parquet:"name=date, type=INT32, convertedtype=DATE"`
		TimeMillis      int32 `parquet:"name=timemillis, type=INT32, isadjustedtoutc=true, convertedtype=TIME_MILLIS"`
		TimeMicros      int64 `parquet:"name=timemicros, type=INT64, isadjustedtoutc=true, convertedtype=TIME_MICROS"`
		TimestampMillis int64 `parquet:"name=timestampmillis, type=INT64, isadjustedtoutc=true, convertedtype=TIMESTAMP_MILLIS"`
		TimestampMicros int64 `parquet:"name=timestampmicros, type=INT64, isadjustedtoutc=true, convertedtype=TIMESTAMP_MICROS"`

		Decimal1 int32 `parquet:"name=decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
		Decimal2 int32 `parquet:"name=decimal2, type=INT32, convertedtype=DECIMAL, scale=4, precision=4"`
		Decimal3 int64 `parquet:"name=decimal3, type=INT64, convertedtype=DECIMAL, scale=2, precision=18"`
		Decimal6 int32 `parquet:"name=decimal6, type=INT32, convertedtype=DECIMAL, scale=4, precision=4"`
	}

	dir := t.TempDir()
	// prepare data
	name := "test123.parquet"
	testPath := filepath.Join(dir, name)
	pf, err := local.NewLocalFileWriter(testPath)
	require.NoError(t, err)
	test := &Test{}
	writer, err := writer2.NewParquetWriter(pf, test, 2)
	require.NoError(t, err)

	v := &Test{
		Date:            18564,              // 2020-10-29
		TimeMillis:      62775123,           // 17:26:15.123
		TimeMicros:      62775123456,        // 17:26:15.123
		TimestampMillis: 1603963672356,      // 2020-10-29T09:27:52.356Z
		TimestampMicros: 1603963672356956,   // 2020-10-29T09:27:52.356956Z
		Decimal1:        -12345678,          // -123456.78
		Decimal2:        456,                // 0.0456
		Decimal3:        123456789012345678, // 1234567890123456.78
		Decimal6:        -1,                 // -0.0001
	}
	require.NoError(t, writer.Write(v))
	require.NoError(t, writer.WriteStop())
	require.NoError(t, pf.Close())

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	require.NoError(t, err)
	defer reader.Close()

	require.Len(t, reader.columns, 9)

	require.NoError(t, reader.ReadRow())
	rowValue := []string{
		"2020-10-29", "17:26:15.123Z", "17:26:15.123456Z", "2020-10-29 09:27:52.356Z", "2020-10-29 09:27:52.356956Z",
		"-123456.78", "0.0456", "1234567890123456.78", "-0.0001",
	}
	row := reader.lastRow.Row
	require.Len(t, rowValue, len(row))
	for i := range row {
		assert.Equal(t, types.KindString, row[i].Kind())
		assert.Equal(t, row[i].GetString(), rowValue[i])
	}

	type TestDecimal struct {
		Decimal1   int32  `parquet:"name=decimal1, type=INT32, convertedtype=DECIMAL, scale=3, precision=5"`
		DecimalRef *int32 `parquet:"name=decimal2, type=INT32, convertedtype=DECIMAL, scale=3, precision=5"`
	}

	cases := [][]any{
		{int32(0), "0.000"},
		{int32(1000), "1.000"},
		{int32(-1000), "-1.000"},
		{int32(999), "0.999"},
		{int32(-999), "-0.999"},
		{int32(1), "0.001"},
		{int32(-1), "-0.001"},
	}

	fileName := "test.02.parquet"
	testPath = filepath.Join(dir, fileName)
	pf, err = local.NewLocalFileWriter(testPath)
	td := &TestDecimal{}
	require.NoError(t, err)
	writer, err = writer2.NewParquetWriter(pf, td, 2)
	require.NoError(t, err)
	for i, testCase := range cases {
		val, ok := testCase[0].(int32)
		require.True(t, ok)
		td.Decimal1 = val
		if i%2 == 0 {
			td.DecimalRef = &val
		} else {
			td.DecimalRef = nil
		}
		assert.NoError(t, writer.Write(td))
	}
	require.NoError(t, writer.WriteStop())
	require.NoError(t, pf.Close())

	r, err = store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	reader, err = NewParquetParser(context.TODO(), store, r, fileName)
	require.NoError(t, err)
	defer reader.Close()

	for i, testCase := range cases {
		assert.NoError(t, reader.ReadRow())
		strDatum, ok := testCase[1].(string)
		require.True(t, ok)
		vals := []types.Datum{types.NewCollationStringDatum(strDatum, "")}
		if i%2 == 0 {
			vals = append(vals, vals[0])
		} else {
			vals = append(vals, types.Datum{})
		}
<<<<<<< HEAD
=======
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

>>>>>>> 73b01d05c64 (import: honor Spark legacy Parquet datetime metadata (#67908))
		// because we always reuse the datums in reader.lastRow.Row, so we can't directly
		// compare will `DeepEqual` here
		assert.Len(t, reader.lastRow.Row, len(vals))
		for i, val := range vals {
			assert.Equal(t, val.Kind(), reader.lastRow.Row[i].Kind())
			assert.Equal(t, val.GetValue(), reader.lastRow.Row[i].GetValue())
		}
	}

	type TestBool struct {
		BoolVal bool `parquet:"name=bool_val, type=BOOLEAN"`
	}

	fileName = "test.bool.parquet"
	testPath = filepath.Join(dir, fileName)
	pf, err = local.NewLocalFileWriter(testPath)
	require.NoError(t, err)
	writer, err = writer2.NewParquetWriter(pf, new(TestBool), 2)
	require.NoError(t, err)
	require.NoError(t, writer.Write(&TestBool{false}))
	require.NoError(t, writer.Write(&TestBool{true}))
	require.NoError(t, writer.WriteStop())
	require.NoError(t, pf.Close())

	r, err = store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	reader, err = NewParquetParser(context.TODO(), store, r, fileName)
	require.NoError(t, err)
	defer reader.Close()

	// because we always reuse the datums in reader.lastRow.Row, so we can't directly
	// compare will `DeepEqual` here
	assert.NoError(t, reader.ReadRow())
	assert.Equal(t, types.KindUint64, reader.lastRow.Row[0].Kind())
	assert.Equal(t, uint64(0), reader.lastRow.Row[0].GetValue())
	assert.NoError(t, reader.ReadRow())
	assert.Equal(t, types.KindUint64, reader.lastRow.Row[0].Kind())
	assert.Equal(t, uint64(1), reader.lastRow.Row[0].GetValue())
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
	store, err := storage.NewLocalStorage("examples")
	require.NoError(t, err)

	fileName := "test.parquet"
	r, err := store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	parser, err := NewParquetParser(context.TODO(), store, r, fileName)
	require.NoError(t, err)

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
		err = parser.ReadRow()
		assert.NoError(t, err)
		expectedValues := expectedRes[i]
		row := parser.LastRow().Row
		assert.Len(t, expectedValues, len(row))
		for j := range row {
			switch v := expectedValues[j].(type) {
			case int64:
				assert.Equal(t, row[j].GetInt64(), v)
			case string:
				assert.Equal(t, row[j].GetString(), v)
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
	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	require.NoError(t, err)
	defer reader.Close()
	// UTC+0:00
	results := []time.Time{
		time.Date(2022, 9, 10, 9, 9, 0, 0, time.UTC),
		time.Date(1997, 8, 11, 2, 1, 10, 0, time.UTC),
		time.Date(1995, 12, 31, 23, 0, 1, 0, time.UTC),
		time.Date(2020, 2, 29, 23, 0, 0, 0, time.UTC),
		time.Date(2038, 1, 19, 0, 0, 0, 0, time.UTC),
	}

	for i := range 5 {
		err = reader.ReadRow()
		require.NoError(t, err)
		lastRow := reader.LastRow()
		require.Equal(t, 2, len(lastRow.Row))
		require.Equal(t, types.KindString, lastRow.Row[1].Kind())
		ts, err := time.Parse(utcTimeLayout, lastRow.Row[1].GetString())
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
