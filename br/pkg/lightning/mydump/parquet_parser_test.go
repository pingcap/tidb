package mydump

import (
	"context"
	"io"
	"path/filepath"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/types"
	"github.com/xitongsys/parquet-go-source/local"
	writer2 "github.com/xitongsys/parquet-go/writer"
)

type testParquetParserSuite struct{}

var _ = Suite(testParquetParserSuite{})

func (s testParquetParserSuite) TestParquetParser(c *C) {
	type Test struct {
		S string `parquet:"name=sS, type=UTF8, encoding=PLAIN_DICTIONARY"`
		A int32  `parquet:"name=a_A, type=INT32"`
	}

	dir := c.MkDir()
	// prepare data
	name := "test123.parquet"
	testPath := filepath.Join(dir, name)
	pf, err := local.NewLocalFileWriter(testPath)
	c.Assert(err, IsNil)
	test := &Test{}
	writer, err := writer2.NewParquetWriter(pf, test, 2)
	c.Assert(err, IsNil)

	for i := 0; i < 100; i++ {
		test.A = int32(i)
		test.S = strconv.Itoa(i)
		c.Assert(writer.Write(test), IsNil)
	}

	c.Assert(writer.WriteStop(), IsNil)
	c.Assert(pf.Close(), IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	r, err := store.Open(context.TODO(), name)
	c.Assert(err, IsNil)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	c.Assert(err, IsNil)
	defer reader.Close()

	c.Assert(reader.Columns(), DeepEquals, []string{"ss", "a_a"})

	verifyRow := func(i int) {
		c.Assert(reader.lastRow.RowID, Equals, int64(i+1))
		c.Assert(len(reader.lastRow.Row), Equals, 2)
		c.Assert(reader.lastRow.Row[0], DeepEquals, types.NewCollationStringDatum(strconv.Itoa(i), "", 0))
		c.Assert(reader.lastRow.Row[1], DeepEquals, types.NewIntDatum(int64(i)))
	}

	// test read some rows
	for i := 0; i < 10; i++ {
		c.Assert(reader.ReadRow(), IsNil)
		verifyRow(i)
	}

	// test set pos to pos < curpos + batchReadRowSize
	c.Assert(reader.SetPos(15, 15), IsNil)
	c.Assert(reader.ReadRow(), IsNil)
	verifyRow(15)

	// test set pos to pos > curpos + batchReadRowSize
	c.Assert(reader.SetPos(80, 80), IsNil)
	for i := 80; i < 100; i++ {
		c.Assert(reader.ReadRow(), IsNil)
		verifyRow(i)
	}

	c.Assert(reader.ReadRow(), Equals, io.EOF)
}

func (s testParquetParserSuite) TestParquetVariousTypes(c *C) {
	// those deprecated TIME/TIMESTAMP types depend on the local timezone!
	prevTZ := time.Local
	time.Local = time.FixedZone("UTC+8", 8*60*60)
	defer func() {
		time.Local = prevTZ
	}()

	type Test struct {
		Date            int32 `parquet:"name=date, type=DATE"`
		TimeMillis      int32 `parquet:"name=timemillis, type=TIME_MILLIS"`
		TimeMicros      int64 `parquet:"name=timemicros, type=TIME_MICROS"`
		TimestampMillis int64 `parquet:"name=timestampmillis, type=TIMESTAMP_MILLIS"`
		TimestampMicros int64 `parquet:"name=timestampmicros, type=TIMESTAMP_MICROS"`

		Decimal1 int32 `parquet:"name=decimal1, type=DECIMAL, scale=2, precision=9, basetype=INT32"`
		Decimal2 int32 `parquet:"name=decimal2, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
		Decimal3 int64 `parquet:"name=decimal3, type=DECIMAL, scale=2, precision=18, basetype=INT64"`
		Decimal6 int32 `parquet:"name=decimal6, type=DECIMAL, scale=4, precision=4, basetype=INT32"`
	}

	dir := c.MkDir()
	// prepare data
	name := "test123.parquet"
	testPath := filepath.Join(dir, name)
	pf, err := local.NewLocalFileWriter(testPath)
	c.Assert(err, IsNil)
	test := &Test{}
	writer, err := writer2.NewParquetWriter(pf, test, 2)
	c.Assert(err, IsNil)

	v := &Test{
		Date:            18564,              // 2020-10-29
		TimeMillis:      62775123,           // 17:26:15.123 (note all time are in UTC+8!)
		TimeMicros:      62775123456,        // 17:26:15.123
		TimestampMillis: 1603963672356,      // 2020-10-29T09:27:52.356Z
		TimestampMicros: 1603963672356956,   // 2020-10-29T09:27:52.356956Z
		Decimal1:        -12345678,          // -123456.78
		Decimal2:        456,                // 0.0456
		Decimal3:        123456789012345678, // 1234567890123456.78
		Decimal6:        -1,                 // -0.0001
	}
	c.Assert(writer.Write(v), IsNil)
	c.Assert(writer.WriteStop(), IsNil)
	c.Assert(pf.Close(), IsNil)

	store, err := storage.NewLocalStorage(dir)
	c.Assert(err, IsNil)
	r, err := store.Open(context.TODO(), name)
	c.Assert(err, IsNil)
	reader, err := NewParquetParser(context.TODO(), store, r, name)
	c.Assert(err, IsNil)
	defer reader.Close()

	c.Assert(len(reader.columns), Equals, 9)

	c.Assert(reader.ReadRow(), IsNil)
	rowValue := []string{
		"2020-10-29", "17:26:15.123Z", "17:26:15.123456Z", "2020-10-29 09:27:52.356Z", "2020-10-29 09:27:52.356956Z",
		"-123456.78", "0.0456", "1234567890123456.78", "-0.0001",
	}
	row := reader.lastRow.Row
	c.Assert(len(rowValue), Equals, len(row))
	for i := 0; i < len(row); i++ {
		c.Assert(row[i].Kind(), Equals, types.KindString)
		c.Assert(rowValue[i], Equals, row[i].GetString())
	}

	type TestDecimal struct {
		Decimal1   int32  `parquet:"name=decimal1, type=DECIMAL, scale=3, precision=5, basetype=INT32"`
		DecimalRef *int32 `parquet:"name=decimal2, type=DECIMAL, scale=3, precision=5, basetype=INT32"`
	}

	cases := [][]interface{}{
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
	c.Assert(err, IsNil)
	writer, err = writer2.NewParquetWriter(pf, td, 2)
	c.Assert(err, IsNil)
	for i, testCase := range cases {
		val := testCase[0].(int32)
		td.Decimal1 = val
		if i%2 == 0 {
			td.DecimalRef = &val
		} else {
			td.DecimalRef = nil
		}
		c.Assert(writer.Write(td), IsNil)
	}
	c.Assert(writer.WriteStop(), IsNil)
	c.Assert(pf.Close(), IsNil)

	r, err = store.Open(context.TODO(), fileName)
	c.Assert(err, IsNil)
	reader, err = NewParquetParser(context.TODO(), store, r, fileName)
	c.Assert(err, IsNil)
	defer reader.Close()

	for i, testCase := range cases {
		c.Assert(reader.ReadRow(), IsNil)
		vals := []types.Datum{types.NewCollationStringDatum(testCase[1].(string), "", 0)}
		if i%2 == 0 {
			vals = append(vals, vals[0])
		} else {
			vals = append(vals, types.Datum{})
		}
		// because we always reuse the datums in reader.lastRow.Row, so we can't directly
		// compare will `DeepEqual` here
		c.Assert(len(reader.lastRow.Row), Equals, len(vals))
		for i, val := range vals {
			c.Assert(reader.lastRow.Row[i].Kind(), Equals, val.Kind())
			c.Assert(reader.lastRow.Row[i].GetValue(), Equals, val.GetValue())
		}
	}
}

func (s testParquetParserSuite) TestParquetAurora(c *C) {
	store, err := storage.NewLocalStorage("examples")
	c.Assert(err, IsNil)

	fileName := "test.parquet"
	r, err := store.Open(context.TODO(), fileName)
	c.Assert(err, IsNil)
	parser, err := NewParquetParser(context.TODO(), store, r, fileName)
	c.Assert(err, IsNil)

	c.Assert(parser.Columns(), DeepEquals, []string{"id", "val1", "val2", "d1", "d2", "d3", "d4", "d5", "d6"})

	expectedRes := [][]interface{}{
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
		c.Assert(err, IsNil)
		expectedValues := expectedRes[i]
		row := parser.LastRow().Row
		c.Assert(len(expectedValues), Equals, len(row))
		for j := 0; j < len(row); j++ {
			switch v := expectedValues[j].(type) {
			case int64:
				c.Assert(v, Equals, row[j].GetInt64())
			case string:
				c.Assert(v, Equals, row[j].GetString())
			default:
				c.Error("unexpected value: ", expectedValues[j])
			}
		}
	}

	c.Assert(parser.ReadRow(), Equals, io.EOF)
}
