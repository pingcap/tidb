// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"io"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTypeEtcSuite{})

type testTypeEtcSuite struct {
}

func testIsTypeBlob(c *C, tp byte, expect bool) {
	v := IsTypeBlob(tp)
	c.Assert(v, Equals, expect)
}

func testIsTypeChar(c *C, tp byte, expect bool) {
	v := IsTypeChar(tp)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestIsType(c *C) {
	defer testleak.AfterTest(c)()
	testIsTypeBlob(c, mysql.TypeTinyBlob, true)
	testIsTypeBlob(c, mysql.TypeMediumBlob, true)
	testIsTypeBlob(c, mysql.TypeBlob, true)
	testIsTypeBlob(c, mysql.TypeLongBlob, true)
	testIsTypeBlob(c, mysql.TypeInt24, false)

	testIsTypeChar(c, mysql.TypeString, true)
	testIsTypeChar(c, mysql.TypeVarchar, true)
	testIsTypeChar(c, mysql.TypeLong, false)
}

func testTypeStr(c *C, tp byte, expect string) {
	v := TypeStr(tp)
	c.Assert(v, Equals, expect)
}

func testTypeToStr(c *C, tp byte, charset string, expect string) {
	v := TypeToStr(tp, charset)
	c.Assert(v, Equals, expect)
}

func (s *testTypeEtcSuite) TestTypeToStr(c *C) {
	defer testleak.AfterTest(c)()
	testTypeStr(c, mysql.TypeYear, "year")
	testTypeStr(c, 0xdd, "")

	testTypeToStr(c, mysql.TypeBlob, "utf8", "text")
	testTypeToStr(c, mysql.TypeLongBlob, "utf8", "longtext")
	testTypeToStr(c, mysql.TypeTinyBlob, "utf8", "tinytext")
	testTypeToStr(c, mysql.TypeMediumBlob, "utf8", "mediumtext")
	testTypeToStr(c, mysql.TypeVarchar, "binary", "varbinary")
	testTypeToStr(c, mysql.TypeString, "binary", "binary")
	testTypeToStr(c, mysql.TypeTiny, "binary", "tinyint")
	testTypeToStr(c, mysql.TypeBlob, "binary", "blob")
	testTypeToStr(c, mysql.TypeLongBlob, "binary", "longblob")
	testTypeToStr(c, mysql.TypeTinyBlob, "binary", "tinyblob")
	testTypeToStr(c, mysql.TypeMediumBlob, "binary", "mediumblob")
	testTypeToStr(c, mysql.TypeVarchar, "utf8", "varchar")
	testTypeToStr(c, mysql.TypeString, "utf8", "char")
	testTypeToStr(c, mysql.TypeShort, "binary", "smallint")
	testTypeToStr(c, mysql.TypeInt24, "binary", "mediumint")
	testTypeToStr(c, mysql.TypeLong, "binary", "int")
	testTypeToStr(c, mysql.TypeLonglong, "binary", "bigint")
	testTypeToStr(c, mysql.TypeFloat, "binary", "float")
	testTypeToStr(c, mysql.TypeDouble, "binary", "double")
	testTypeToStr(c, mysql.TypeYear, "binary", "year")
	testTypeToStr(c, mysql.TypeDuration, "binary", "time")
	testTypeToStr(c, mysql.TypeDatetime, "binary", "datetime")
	testTypeToStr(c, mysql.TypeDate, "binary", "date")
	testTypeToStr(c, mysql.TypeTimestamp, "binary", "timestamp")
	testTypeToStr(c, mysql.TypeNewDecimal, "binary", "decimal")
	testTypeToStr(c, mysql.TypeUnspecified, "binary", "unspecified")
	testTypeToStr(c, 0xdd, "binary", "")
	testTypeToStr(c, mysql.TypeBit, "binary", "bit")
	testTypeToStr(c, mysql.TypeEnum, "binary", "enum")
	testTypeToStr(c, mysql.TypeSet, "binary", "set")
}

func (s *testTypeEtcSuite) TestEOFAsNil(c *C) {
	defer testleak.AfterTest(c)()
	err := EOFAsNil(io.EOF)
	c.Assert(err, IsNil)
}

func (s *testTypeEtcSuite) TestMaxFloat(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Flen    int
		Decimal int
		Expect  float64
	}{
		{3, 2, 9.99},
		{5, 2, 999.99},
		{10, 1, 999999999.9},
		{5, 5, 0.99999},
	}

	for _, t := range tbl {
		f := getMaxFloat(t.Flen, t.Decimal)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestRoundFloat(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  float64
		Expect float64
	}{
		{2.5, 3},
		{1.5, 2},
		{0.5, 1},
		{0.49999999999999997, 0},
		{0, 0},
		{-0.49999999999999997, 0},
		{-0.5, -1},
		{-2.5, -3},
		{-1.5, -2},
	}

	for _, t := range tbl {
		f := RoundFloat(t.Input)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestRound(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  float64
		Dec    int
		Expect float64
	}{
		{-1.23, 0, -1},
		{-1.58, 0, -2},
		{1.58, 0, 2},
		{1.298, 1, 1.3},
		{1.298, 0, 1},
		{23.298, -1, 20},
	}

	for _, t := range tbl {
		f := Round(t.Input, t.Dec)
		c.Assert(f, Equals, t.Expect)
	}
}

func (s *testTypeEtcSuite) TestTruncate(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input   float64
		Flen    int
		Decimal int
		Expect  float64
		Err     error
	}{
		{100.114, 10, 2, 100.11, nil},
		{100.115, 10, 2, 100.12, nil},
		{100.1156, 10, 3, 100.116, nil},
		{100.1156, 3, 1, 99.9, ErrOverflow},
		{1.36, 10, 2, 1.36, nil},
	}

	for _, t := range tbl {
		f, err := TruncateFloat(t.Input, t.Flen, t.Decimal)
		c.Assert(f, Equals, t.Expect)
		c.Assert(terror.ErrorEqual(err, t.Err), IsTrue)
	}
}
