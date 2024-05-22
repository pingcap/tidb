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
	"io"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	writer2 "github.com/xitongsys/parquet-go/writer"
)

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

	for i := 0; i < 100; i++ {
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
	for i := 0; i < 10; i++ {
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
	for i := 0; i < len(row); i++ {
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

	for i := 0; i < 5; i++ {
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
