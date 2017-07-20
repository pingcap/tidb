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
	"math"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct {
}

func (s *testTimeSuite) TestDateTime(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		{"2012-12-31 11:30:45", "2012-12-31 11:30:45"},
		{"0000-00-00 00:00:00", "0000-00-00 00:00:00"},
		{"0001-01-01 00:00:00", "0001-01-01 00:00:00"},
		{"00-12-31 11:30:45", "2000-12-31 11:30:45"},
		{"12-12-31 11:30:45", "2012-12-31 11:30:45"},
		{"2012-12-31", "2012-12-31 00:00:00"},
		{"20121231", "2012-12-31 00:00:00"},
		{"121231", "2012-12-31 00:00:00"},
		{"2012^12^31 11+30+45", "2012-12-31 11:30:45"},
		{"2012^12^31T11+30+45", "2012-12-31 11:30:45"},
		{"2012-2-1 11:30:45", "2012-02-01 11:30:45"},
		{"12-2-1 11:30:45", "2012-02-01 11:30:45"},
		{"20121231113045", "2012-12-31 11:30:45"},
		{"121231113045", "2012-12-31 11:30:45"},
		{"2012-02-29", "2012-02-29 00:00:00"},
		{"00-00-00", "0000-00-00 00:00:00"},
		{"00-00-00 00:00:00.123", "2000-00-00 00:00:00"},
	}

	for _, test := range table {
		t, err := ParseDatetime(test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	fspTbl := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"121231113045.123345", 6, "2012-12-31 11:30:45.123345"},
		{"20121231113045.123345", 6, "2012-12-31 11:30:45.123345"},
		{"121231113045.9999999", 6, "2012-12-31 11:30:46.000000"},
		{"170105084059.575601", 0, "2017-01-05 08:41:00"},
		{"2017-01-05 23:59:59.575601", 0, "2017-01-06 00:00:00"},
		{"2017-01-31 23:59:59.575601", 0, "2017-02-01 00:00:00"},
		{"2017-00-05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"},
	}

	for _, test := range fspTbl {
		t, err := ParseTime(test.Input, mysql.TypeDatetime, test.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	t, _ := ParseTime("121231113045.9999999", mysql.TypeDatetime, 6)
	c.Assert(t.Time.Second(), Equals, 46)
	c.Assert(t.Time.Microsecond(), Equals, 0)

	// test error
	errTable := []string{
		"1000-01-01 00:00:70",
		"1000-13-00 00:00:00",
		"10000-01-01 00:00:00",
		"1000-09-31 00:00:00",
		"1001-02-29 00:00:00",
		"2017-00-05 08:40:59.575601",
	}

	for _, test := range errTable {
		_, err := ParseDatetime(test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestTimestamp(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		{"2012-12-31 11:30:45", "2012-12-31 11:30:45"},
	}

	for _, test := range table {
		t, err := ParseTimestamp(test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"2048-12-31 11:30:45",
		"1969-12-31 11:30:45",
	}

	for _, test := range errTable {
		_, err := ParseTimestamp(test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestDate(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		{"2012-12-31", "2012-12-31"},
		{"00-12-31", "2000-12-31"},
		{"20121231", "2012-12-31"},
		{"121231", "2012-12-31"},
		{"2015-06-01 12:12:12", "2015-06-01"},
		{"0001-01-01 00:00:00", "0001-01-01"},
		{"0001-01-01", "0001-01-01"},
	}

	for _, test := range table {
		t, err := ParseDate(test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"0121231",
	}

	for _, test := range errTable {
		_, err := ParseDate(test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestTime(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		{"10:11:12", "10:11:12"},
		{"101112", "10:11:12"},
		{"112", "00:01:12"},
		{"10:11", "10:11:00"},
		{"101112.123456", "10:11:12"},
		{"1112", "00:11:12"},
		{"1", "00:00:01"},
		{"12", "00:00:12"},
		{"1 12", "36:00:00"},
		{"1 10:11:12", "34:11:12"},
		{"1 10:11:12.123456", "34:11:12"},
		{"10:11:12.123456", "10:11:12"},
		{"1 10:11", "34:11:00"},
		{"1 10", "34:00:00"},
		{"24 10", "586:00:00"},
		{"-24 10", "-586:00:00"},
		{"0 10", "10:00:00"},
		{"-10:10:10", "-10:10:10"},
		{"-838:59:59", "-838:59:59"},
		{"838:59:59", "838:59:59"},
		{"2011-11-11 00:00:01", "00:00:01"},
		{"2011-11-11", "00:00:00"},
	}

	for _, test := range table {
		t, err := ParseDuration(test.Input, MinFsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	table = []struct {
		Input  string
		Expect string
	}{
		{"101112.123456", "10:11:12.123456"},
		{"1 10:11:12.123456", "34:11:12.123456"},
		{"10:11:12.123456", "10:11:12.123456"},
	}

	for _, test := range table {
		t, err := ParseDuration(test.Input, MaxFsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"232 10",
		"-232 10",
	}

	for _, test := range errTable {
		_, err := ParseDuration(test, DefaultFsp)
		c.Assert(err, NotNil)
	}

	// test time compare
	cmpTable := []struct {
		lhs int64
		rhs int64
		ret int
	}{
		{1, 0, 1},
		{0, 1, -1},
		{0, 0, 0},
	}

	for _, t := range cmpTable {
		t1 := Duration{time.Duration(t.lhs), DefaultFsp}
		t2 := Duration{time.Duration(t.rhs), DefaultFsp}
		ret := t1.Compare(t2)
		c.Assert(ret, Equals, t.ret)
	}
}

func (s *testTimeSuite) TestDurationAdd(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input    string
		Fsp      int
		InputAdd string
		FspAdd   int
		Expect   string
	}{
		{"00:00:00.1", 1, "00:00:00.1", 1, "00:00:00.2"},
		{"00:00:00", 0, "00:00:00.1", 1, "00:00:00.1"},
		{"00:00:00.09", 2, "00:00:00.01", 2, "00:00:00.10"},
		{"00:00:00.099", 3, "00:00:00.001", 3, "00:00:00.100"},
	}
	for _, test := range table {
		t, err := ParseDuration(test.Input, test.Fsp)
		c.Assert(err, IsNil)
		ta, err := ParseDuration(test.InputAdd, test.FspAdd)
		c.Assert(err, IsNil)
		result, err := t.Add(ta)
		c.Assert(err, IsNil)
		c.Assert(result.String(), Equals, test.Expect)
	}
	t, err := ParseDuration("00:00:00", 0)
	c.Assert(err, IsNil)
	ta := new(Duration)
	result, err := t.Add(*ta)
	c.Assert(err, IsNil)
	c.Assert(result.String(), Equals, "00:00:00")

	t = Duration{Duration: math.MaxInt64, Fsp: 0}
	tatmp, err := ParseDuration("00:01:00", 0)
	c.Assert(err, IsNil)
	_, err = t.Add(tatmp)
	c.Assert(err, NotNil)
}

func (s *testTimeSuite) TestDurationSub(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input    string
		Fsp      int
		InputAdd string
		FspAdd   int
		Expect   string
	}{
		{"00:00:00.1", 1, "00:00:00.1", 1, "00:00:00.0"},
		{"00:00:00", 0, "00:00:00.1", 1, "-00:00:00.1"},
	}
	for _, test := range table {
		t, err := ParseDuration(test.Input, test.Fsp)
		c.Assert(err, IsNil)
		ta, err := ParseDuration(test.InputAdd, test.FspAdd)
		c.Assert(err, IsNil)
		result, err := t.Sub(ta)
		c.Assert(err, IsNil)
		c.Assert(result.String(), Equals, test.Expect)
	}
}

func (s *testTimeSuite) TestTimeFsp(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"00:00:00.1", 0, "00:00:00"},
		{"00:00:00.1", 1, "00:00:00.1"},
		{"00:00:00.777777", 2, "00:00:00.78"},
		{"00:00:00.777777", 6, "00:00:00.777777"},
		// fsp -1 use default 0
		{"00:00:00.777777", -1, "00:00:01"},
		{"00:00:00.001", 3, "00:00:00.001"},
	}

	for _, test := range table {
		t, err := ParseDuration(test.Input, test.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []struct {
		Input string
		Fsp   int
	}{
		{"00:00:00.1", -2},
		{"00:00:00.1", 7},
	}

	for _, test := range errTable {
		_, err := ParseDuration(test.Input, test.Fsp)
		c.Assert(err, NotNil)
	}
}
func (s *testTimeSuite) TestYear(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect int16
	}{
		{"1990", 1990},
		{"10", 2010},
		{"0", 2000},
		{"99", 1999},
	}

	for _, test := range table {
		t, err := ParseYear(test.Input)
		c.Assert(err, IsNil)
		c.Assert(t, Equals, test.Expect)
	}

	valids := []struct {
		Year   int64
		Expect bool
	}{
		{2000, true},
		{20000, false},
		{0, true},
		{-1, false},
	}

	for _, test := range valids {
		_, err := AdjustYear(test.Year)
		if test.Expect {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
		}
	}

}

func (s *testTimeSuite) getLocation(c *C) *time.Location {
	locations := []string{"Asia/Shanghai", "Europe/Berlin"}
	timeFormat := "Jan 2, 2006 at 3:04pm (MST)"

	z, err := time.LoadLocation(locations[0])
	c.Assert(err, IsNil)

	t1, err := time.ParseInLocation(timeFormat, "Jul 9, 2012 at 5:02am (CEST)", z)
	c.Assert(err, IsNil)
	t2, err := time.Parse(timeFormat, "Jul 9, 2012 at 5:02am (CEST)")
	c.Assert(err, IsNil)

	if t1.Equal(t2) {
		z, err = time.LoadLocation(locations[1])
		c.Assert(err, IsNil)
	}

	return z
}

func (s *testTimeSuite) TestCodec(c *C) {
	defer testleak.AfterTest(c)()
	// MySQL timestamp value doesn't allow month=0 or day=0.
	t, err := ParseTimestamp("2016-12-00 00:00:00")
	c.Assert(err, NotNil)

	t, err = ParseTimestamp("2010-10-10 10:11:11")
	c.Assert(err, IsNil)
	packed, err := t.ToPackedUint()
	c.Assert(err, IsNil)

	var t1 Time
	t1.Type = mysql.TypeTimestamp
	t1.Time = FromGoTime(time.Now())
	packed, err = t1.ToPackedUint()
	c.Assert(err, IsNil)

	var t2 Time
	t2.Type = mysql.TypeTimestamp
	err = t2.FromPackedUint(packed)
	c.Assert(err, IsNil)
	c.Assert(t1.String(), Equals, t2.String())

	packed, _ = ZeroDatetime.ToPackedUint()

	var t3 Time
	t3.Type = mysql.TypeDatetime
	err = t3.FromPackedUint(packed)
	c.Assert(err, IsNil)
	c.Assert(t3.String(), Equals, ZeroDatetime.String())

	t, err = ParseDatetime("0001-01-01 00:00:00")
	c.Assert(err, IsNil)
	packed, _ = t.ToPackedUint()

	var t4 Time
	t4.Type = mysql.TypeDatetime
	err = t4.FromPackedUint(packed)
	c.Assert(err, IsNil)
	c.Assert(t.String(), Equals, t4.String())

	tbl := []string{
		"2000-01-01 00:00:00.000000",
		"2000-01-01 00:00:00.123456",
		"0001-01-01 00:00:00.123456",
		"2000-06-01 00:00:00.999999",
	}

	for _, test := range tbl {
		t, err := ParseTime(test, mysql.TypeDatetime, MaxFsp)
		c.Assert(err, IsNil)

		packed, _ = t.ToPackedUint()

		var dest Time
		dest.Type = mysql.TypeDatetime
		dest.Fsp = MaxFsp
		err = dest.FromPackedUint(packed)
		c.Assert(err, IsNil)
		c.Assert(dest.String(), Equals, test)
	}
}

func (s *testTimeSuite) TestParseTimeFromNum(c *C) {
	defer testleak.AfterTest(c)()
	table := []struct {
		Input                int64
		ExpectDateTimeError  bool
		ExpectDateTimeValue  string
		ExpectTimeStampError bool
		ExpectTimeStampValue string
		ExpectDateError      bool
		ExpectDateValue      string
	}{
		{20101010111111, false, "2010-10-10 11:11:11", false, "2010-10-10 11:11:11", false, "2010-10-10"},
		{2010101011111, false, "0201-01-01 01:11:11", true, zeroDatetimeStr, false, "0201-01-01"},
		{201010101111, false, "2020-10-10 10:11:11", false, "2020-10-10 10:11:11", false, "2020-10-10"},
		{20101010111, false, "2002-01-01 01:01:11", false, "2002-01-01 01:01:11", false, "2002-01-01"},
		{2010101011, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{201010101, false, "2000-02-01 01:01:01", false, "2000-02-01 01:01:01", false, "2000-02-01"},
		{20101010, false, "2010-10-10 00:00:00", false, "2010-10-10 00:00:00", false, "2010-10-10"},
		{2010101, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{201010, false, "2020-10-10 00:00:00", false, "2020-10-10 00:00:00", false, "2020-10-10"},
		{20101, false, "2002-01-01 00:00:00", false, "2002-01-01 00:00:00", false, "2002-01-01"},
		{2010, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{201, false, "2000-02-01 00:00:00", false, "2000-02-01 00:00:00", false, "2000-02-01"},
		{20, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{2, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{0, false, zeroDatetimeStr, false, zeroDatetimeStr, false, zeroDateStr},
		{-1, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{99999999999999, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{100000000000000, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
		{10000102000000, false, "1000-01-02 00:00:00", true, zeroDatetimeStr, false, "1000-01-02"},
		{19690101000000, false, "1969-01-01 00:00:00", true, zeroDatetimeStr, false, "1969-01-01"},
		{991231235959, false, "1999-12-31 23:59:59", false, "1999-12-31 23:59:59", false, "1999-12-31"},
		{691231235959, false, "2069-12-31 23:59:59", true, zeroDatetimeStr, false, "2069-12-31"},
		{370119031407, false, "2037-01-19 03:14:07", false, "2037-01-19 03:14:07", false, "2037-01-19"},
		{380120031407, false, "2038-01-20 03:14:07", true, zeroDatetimeStr, false, "2038-01-20"},
	}

	for ith, test := range table {
		// test ParseDatetimeFromNum
		t, err := ParseDatetimeFromNum(test.Input)
		if test.ExpectDateTimeError {
			c.Assert(err, NotNil, Commentf("%d", ith))
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, mysql.TypeDatetime)
		}
		c.Assert(t.String(), Equals, test.ExpectDateTimeValue)

		// test ParseTimestampFromNum
		t, err = ParseTimestampFromNum(test.Input)
		if test.ExpectTimeStampError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil, Commentf("%d", ith))
			c.Assert(t.Type, Equals, mysql.TypeTimestamp)
		}
		c.Assert(t.String(), Equals, test.ExpectTimeStampValue)

		// test ParseDateFromNum
		t, err = ParseDateFromNum(test.Input)

		if test.ExpectDateTimeError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, mysql.TypeDate)
		}
		c.Assert(t.String(), Equals, test.ExpectDateValue)
	}
}

func (s *testTimeSuite) TestToNumber(c *C) {
	defer testleak.AfterTest(c)()
	tblDateTime := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"12-12-31 11:30:45", 0, "20121231113045"},
		{"12-12-31 11:30:45", 6, "20121231113045.000000"},
		{"12-12-31 11:30:45.123", 6, "20121231113045.123000"},
		{"12-12-31 11:30:45.123345", 0, "20121231113045"},
		{"12-12-31 11:30:45.123345", 3, "20121231113045.123"},
		{"12-12-31 11:30:45.123345", 5, "20121231113045.12335"},
		{"12-12-31 11:30:45.123345", 6, "20121231113045.123345"},
		{"12-12-31 11:30:45.1233457", 6, "20121231113045.123346"},
		{"12-12-31 11:30:45.823345", 0, "20121231113046"},
	}

	for _, test := range tblDateTime {
		t, err := ParseTime(test.Input, mysql.TypeDatetime, test.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.ToNumber().String(), Equals, test.Expect)
	}

	// Fix issue #1046
	tblDate := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"12-12-31 11:30:45", 0, "20121231"},
		{"12-12-31 11:30:45", 6, "20121231"},
		{"12-12-31 11:30:45.123", 6, "20121231"},
		{"12-12-31 11:30:45.123345", 0, "20121231"},
		{"12-12-31 11:30:45.123345", 3, "20121231"},
		{"12-12-31 11:30:45.123345", 5, "20121231"},
		{"12-12-31 11:30:45.123345", 6, "20121231"},
		{"12-12-31 11:30:45.1233457", 6, "20121231"},
		{"12-12-31 11:30:45.823345", 0, "20121231"},
	}

	for _, test := range tblDate {
		t, err := ParseTime(test.Input, mysql.TypeDate, 0)
		c.Assert(err, IsNil)
		c.Assert(t.ToNumber().String(), Equals, test.Expect)
	}

	tblDuration := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"11:30:45", 0, "113045"},
		{"11:30:45", 6, "113045.000000"},
		{"11:30:45.123", 6, "113045.123000"},
		{"11:30:45.123345", 0, "113045"},
		{"11:30:45.123345", 3, "113045.123"},
		{"11:30:45.123345", 5, "113045.12335"},
		{"11:30:45.123345", 6, "113045.123345"},
		{"11:30:45.1233456", 6, "113045.123346"},
		{"11:30:45.9233456", 0, "113046"},
		{"-11:30:45.9233456", 0, "-113046"},
	}

	for _, test := range tblDuration {
		t, err := ParseDuration(test.Input, test.Fsp)
		c.Assert(err, IsNil)
		// now we can only change Duration's Fsp to check ToNumber with different Fsp
		c.Assert(t.ToNumber().String(), Equals, test.Expect)
	}
}

func (s *testTimeSuite) TestParseFrac(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		S        string
		Fsp      int
		Ret      int
		Overflow bool
	}{
		// Round when fsp < string length.
		{"1234567", 0, 0, false},
		{"1234567", 1, 100000, false},
		{"0000567", 5, 60, false},
		{"1234567", 5, 123460, false},
		{"1234567", 6, 123457, false},
		// Fill 0 when fsp > string length.
		{"123", 4, 123000, false},
		{"123", 5, 123000, false},
		{"123", 6, 123000, false},
		{"11", 6, 110000, false},
		{"01", 3, 10000, false},
		{"012", 4, 12000, false},
		{"0123", 5, 12300, false},
		// Overflow
		{"9999999", 6, 0, true},
		{"999999", 5, 0, true},
		{"999", 2, 0, true},
		{"999", 3, 999000, false},
	}

	for _, t := range tbl {
		v, overflow, err := parseFrac(t.S, t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Ret)
		c.Assert(overflow, Equals, t.Overflow)
	}
}

func (s *testTimeSuite) TestRoundFrac(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  string
		Fsp    int
		Except string
	}{
		{"2012-12-31 11:30:45.123456", 4, "2012-12-31 11:30:45.1235"},
		{"2012-12-31 11:30:45.123456", 6, "2012-12-31 11:30:45.123456"},
		{"2012-12-31 11:30:45.123456", 0, "2012-12-31 11:30:45"},
		{"2012-12-31 11:30:45.123456", 1, "2012-12-31 11:30:45.1"},
		{"2012-12-31 11:30:45.999999", 4, "2012-12-31 11:30:46.0000"},
		{"2012-12-31 11:30:45.999999", 0, "2012-12-31 11:30:46"},
		{"2012-00-00 11:30:45.999999", 3, "2012-00-00 11:30:46.000"},
		// TODO: MySQL can handle this case, but we can't.
		// {"2012-01-00 23:59:59.999999", 3, "2012-01-01 00:00:00.000"},
	}

	for _, t := range tbl {
		v, err := ParseTime(t.Input, mysql.TypeDatetime, MaxFsp)
		c.Assert(err, IsNil)
		nv, err := v.RoundFrac(t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(nv.String(), Equals, t.Except)
	}

	tbl = []struct {
		Input  string
		Fsp    int
		Except string
	}{
		{"11:30:45.123456", 4, "11:30:45.1235"},
		{"11:30:45.123456", 6, "11:30:45.123456"},
		{"11:30:45.123456", 0, "11:30:45"},
		{"1 11:30:45.123456", 1, "35:30:45.1"},
		{"1 11:30:45.999999", 4, "35:30:46.0000"},
		{"-1 11:30:45.999999", 0, "-35:30:46"},
	}

	for _, t := range tbl {
		v, err := ParseDuration(t.Input, MaxFsp)
		c.Assert(err, IsNil)
		nv, err := v.RoundFrac(t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(nv.String(), Equals, t.Except)
	}
}

func (s *testTimeSuite) TestConvert(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  string
		Fsp    int
		Except string
	}{
		{"2012-12-31 11:30:45.123456", 4, "11:30:45.1235"},
		{"2012-12-31 11:30:45.123456", 6, "11:30:45.123456"},
		{"2012-12-31 11:30:45.123456", 0, "11:30:45"},
		{"2012-12-31 11:30:45.999999", 0, "11:30:46"},
		{"2017-01-05 08:40:59.575601", 0, "08:41:00"},
		{"2017-01-05 23:59:59.575601", 0, "00:00:00"},
		{"0000-00-00 00:00:00", 6, "00:00:00"},
	}

	for _, t := range tbl {
		v, err := ParseTime(t.Input, mysql.TypeDatetime, t.Fsp)
		c.Assert(err, IsNil)
		nv, err := v.ConvertToDuration()
		c.Assert(err, IsNil)
		c.Assert(nv.String(), Equals, t.Except)
	}

	tblDuration := []struct {
		Input string
		Fsp   int
	}{
		{"11:30:45.123456", 4},
		{"11:30:45.123456", 6},
		{"11:30:45.123456", 0},
		{"1 11:30:45.999999", 0},
	}

	for _, t := range tblDuration {
		v, err := ParseDuration(t.Input, t.Fsp)
		c.Assert(err, IsNil)
		year, month, day := time.Now().Date()
		n := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
		t, err := v.ConvertToTime(mysql.TypeDatetime)
		c.Assert(err, IsNil)
		// TODO: Consider time_zone variable.
		t1, _ := t.Time.GoTime(time.Local)
		c.Assert(t1.Sub(n), Equals, v.Duration)
	}
}

func (s *testTimeSuite) TestCompare(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"2011-10-10 11:11:11", "2011-10-10 11:11:11", 0},
		{"2011-10-10 11:11:11.123456", "2011-10-10 11:11:11.1", 1},
		{"2011-10-10 11:11:11", "2011-10-10 11:11:11.123", -1},
		{"0000-00-00 00:00:00", "2011-10-10 11:11:11", -1},
		{"0000-00-00 00:00:00", "0000-00-00 00:00:00", 0},
	}

	for _, t := range tbl {
		v1, err := ParseTime(t.Arg1, mysql.TypeDatetime, MaxFsp)
		c.Assert(err, IsNil)

		ret, err := v1.CompareString(t.Arg2)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}

	tbl = []struct {
		Arg1 string
		Arg2 string
		Ret  int
	}{
		{"11:11:11", "11:11:11", 0},
		{"11:11:11.123456", "11:11:11.1", 1},
		{"11:11:11", "11:11:11.123", -1},
	}

	for _, t := range tbl {
		v1, err := ParseDuration(t.Arg1, MaxFsp)
		c.Assert(err, IsNil)

		ret, err := v1.CompareString(t.Arg2)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}
}

func (s *testTimeSuite) TestDurationClock(c *C) {
	defer testleak.AfterTest(c)()
	// test hour, minute, second and micro second
	tbl := []struct {
		Input       string
		Hour        int
		Minute      int
		Second      int
		MicroSecond int
	}{
		{"11:11:11.11", 11, 11, 11, 110000},
		{"1 11:11:11.000011", 35, 11, 11, 11},
		{"2010-10-10 11:11:11.000011", 11, 11, 11, 11},
	}

	for _, t := range tbl {
		d, err := ParseDuration(t.Input, MaxFsp)
		c.Assert(err, IsNil)
		c.Assert(d.Hour(), Equals, t.Hour)
		c.Assert(d.Minute(), Equals, t.Minute)
		c.Assert(d.Second(), Equals, t.Second)
		c.Assert(d.MicroSecond(), Equals, t.MicroSecond)
	}
}

func (s *testTimeSuite) TestParseDateFormat(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  string
		Result []string
	}{
		{"2011-11-11 10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"  2011-11-11 10:10:10.123456  ", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"2011-11-11 10", []string{"2011", "11", "11", "10"}},
		{"2011-11-11T10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"2011:11:11T10:10:10.123456", []string{"2011", "11", "11", "10", "10", "10", "123456"}},
		{"xx2011-11-11 10:10:10", nil},
		{"T10:10:10", nil},
		{"2011-11-11x", nil},
		{"2011-11-11  10:10:10", nil},
		{"xxx 10:10:10", nil},
	}

	for _, t := range tbl {
		r := parseDateFormat(t.Input)
		c.Assert(r, DeepEquals, t.Result)
	}
}

func (s *testTimeSuite) TestTamestampDiff(c *C) {
	tests := []struct {
		unit   string
		t1     TimeInternal
		t2     TimeInternal
		expect int64
	}{
		{"MONTH", FromDate(2002, 5, 30, 0, 0, 0, 0), FromDate(2001, 1, 1, 0, 0, 0, 0), -16},
		{"YEAR", FromDate(2002, 5, 1, 0, 0, 0, 0), FromDate(2001, 1, 1, 0, 0, 0, 0), -1},
		{"MINUTE", FromDate(2003, 2, 1, 0, 0, 0, 0), FromDate(2003, 5, 1, 12, 5, 55, 0), 128885},
		{"MICROSECOND", FromDate(2002, 5, 30, 0, 0, 0, 0), FromDate(2002, 5, 30, 0, 13, 25, 0), 805000000},
		{"MICROSECOND", FromDate(2000, 1, 1, 0, 0, 0, 12345), FromDate(2000, 1, 1, 0, 0, 45, 32), 44987687},
		{"QUARTER", FromDate(2000, 1, 12, 0, 0, 0, 0), FromDate(2016, 1, 1, 0, 0, 0, 0), 63},
		{"QUARTER", FromDate(2016, 1, 1, 0, 0, 0, 0), FromDate(2000, 1, 12, 0, 0, 0, 0), -63},
	}

	for _, test := range tests {
		t1 := Time{test.t1, mysql.TypeDatetime, 6, nil}
		t2 := Time{test.t2, mysql.TypeDatetime, 6, nil}
		c.Assert(TimestampDiff(test.unit, t1, t2), Equals, test.expect)
	}
}

func (s *testTimeSuite) TestDateFSP(c *C) {
	tests := []struct {
		date   string
		expect int
	}{
		{"2004-01-01 12:00:00.111", 3},
		{"2004-01-01 12:00:00.11", 2},
		{"2004-01-01 12:00:00.111111", 6},
		{"2004-01-01 12:00:00", 0},
	}

	for _, test := range tests {
		c.Assert(DateFSP(test.date), Equals, test.expect)
	}
}

func (s *testTimeSuite) TestConvertTimeZone(c *C) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	tests := []struct {
		input  TimeInternal
		from   *time.Location
		to     *time.Location
		expect TimeInternal
	}{
		{FromDate(2017, 1, 1, 0, 0, 0, 0), time.UTC, loc, FromDate(2017, 1, 1, 8, 0, 0, 0)},
		{FromDate(2017, 1, 1, 8, 0, 0, 0), loc, time.UTC, FromDate(2017, 1, 1, 0, 0, 0, 0)},
		{FromDate(0, 0, 0, 0, 0, 0, 0), loc, time.UTC, FromDate(0, 0, 0, 0, 0, 0, 0)},
	}

	for _, test := range tests {
		var t Time
		t.Time = test.input
		t.ConvertTimeZone(test.from, test.to)
		c.Assert(compareTime(t.Time, test.expect), Equals, 0)
	}
}
