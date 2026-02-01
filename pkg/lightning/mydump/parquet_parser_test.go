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
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.Background(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.Background(), store, r, name, ParquetFileMeta{})
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
	for i := 0; i < 10; i++ {
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
}

func TestParquetVariousTypes(t *testing.T) {
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
	// prepare data
	name := "test123.parquet"
	WriteParquetFile(dir, name, pc, 1)

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, name, ParquetFileMeta{Loc: time.UTC})
	require.NoError(t, err)
	defer reader.Close()

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

	// Convert the expected string values to datum
	// to check the parquet type converter output.
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

	pc = []ParquetColumn{
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

	fileName := "test.02.parquet"
	WriteParquetFile(dir, fileName, pc, 7)

	r, err = store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	reader, err = NewParquetParser(context.TODO(), store, r, fileName, ParquetFileMeta{})
	require.NoError(t, err)
	defer reader.Close()

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

	pc = []ParquetColumn{
		{
			Name:      "bool_val",
			Type:      parquet.Types.Boolean,
			Converted: schema.ConvertedTypes.None,
			Gen: func(_ int) (any, []int16) {
				return []bool{false, true}, []int16{1, 1}
			},
		},
	}

	fileName = "test.bool.parquet"
	WriteParquetFile(dir, fileName, pc, 2)

	r, err = store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	reader, err = NewParquetParser(context.TODO(), store, r, fileName, ParquetFileMeta{})
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

func TestParquetAurora(t *testing.T) {
	store, err := storage.NewLocalStorage("examples")
	require.NoError(t, err)

	fileName := "test.parquet"
	r, err := store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	parser, err := NewParquetParser(context.TODO(), store, r, fileName, ParquetFileMeta{})
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

	for i := 0; i < len(expectedRes); i++ {
		err = parser.ReadRow()
		assert.NoError(t, err)
		expectedValues := expectedRes[i]
		row := parser.LastRow().Row
		assert.Len(t, expectedValues, len(row))
		for j := 0; j < len(row); j++ {
			switch v := expectedValues[j].(type) {
			case int64:
				assert.Equal(t, row[j].GetInt64(), v)
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
	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), name, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, name, ParquetFileMeta{Loc: time.UTC})
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

	for i := 0; i < 5; i++ {
		err = reader.ReadRow()
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

	store, err := storage.NewLocalStorage(dir)
	require.NoError(t, err)
	r, err := store.Open(context.TODO(), fileName, nil)
	require.NoError(t, err)
	reader, err := NewParquetParser(context.TODO(), store, r, fileName, ParquetFileMeta{})
	require.NoError(t, err)
	defer reader.Close()

	for i := range rowCnt {
		require.NoError(t, reader.ReadRow())
		require.Equal(t, string(generated[i]), reader.lastRow.Row[0].GetString())
	}
}

// This is the previous implementation used to convert decimal to string.
// We use it to generate expected results for testing.
func parquetArrayToStr(rawBytes []byte, scale int) string {
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
	for _, tc := range []testCase{
		{negMinN(16), 0},
		{negMinN(16), 8},
	} {
		tcs = append(tcs, tc)
	}

	for i, tc := range tcs {
		rawCopy := append([]byte(nil), tc.rawBytes...)
		expected := parquetArrayToStr(tc.rawBytes, tc.scale)

		var dec types.MyDecimal
		err := dec.FromParquetArray(rawCopy, tc.scale)
		require.NoError(t, err)
		require.Equal(t, expected, dec.String(),
			"raw=%x scale=%d idx=%d", tc.rawBytes, tc.scale, i)
	}
}

func TestParquetDecimalFromInt64(t *testing.T) {
	var d types.Datum

	type testCase struct {
		value    int64
		scale    int32
		expected string
	}

	tcs := []testCase{
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
		err := setParquetDecimalFromInt64(tc.value, &d,
			schema.DecimalMetadata{IsSet: true, Scale: tc.scale})
		require.NoError(t, err)
		require.Equal(t, tc.expected, d.GetMysqlDecimal().String())
	}
}
