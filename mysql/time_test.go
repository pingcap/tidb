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

package mysql

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct {
}

func (s *testTimeSuite) TestDateTime(c *C) {
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
	}

	for _, test := range fspTbl {
		t, err := ParseTime(test.Input, TypeDatetime, test.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	// test error
	errTable := []string{
		"1000-00-00 00:00:00",
		"1000-01-01 00:00:70",
		"1000-13-00 00:00:00",
		"10000-01-01 00:00:00",
	}

	for _, test := range errTable {
		_, err := ParseDatetime(test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestTimestamp(c *C) {
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
	table := []struct {
		Input  string
		Expect string
	}{
		{"10:11:12", "10:11:12"},
		{"101112", "10:11:12"},
		{"10:11", "10:11:00"},
		{"101112.123456", "10:11:12"},
		{"1112", "00:11:12"},
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

func (s *testTimeSuite) TestTimeFsp(c *C) {
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
	t, err := ParseTimestamp("2010-10-10 10:11:11")
	c.Assert(err, IsNil)
	b, err := t.Marshal()
	c.Assert(err, IsNil)

	var t1 Time
	t1.Type = TypeTimestamp

	z := s.getLocation(c)

	err = t1.UnmarshalInLocation(b, z)
	c.Assert(err, IsNil)
	c.Assert(t.String(), Not(Equals), t1.String())

	err = t1.UnmarshalInLocation(b, time.Local)
	c.Assert(err, IsNil)
	c.Assert(t.String(), Equals, t1.String())

	t1.Time = time.Now()
	b, err = t1.Marshal()
	c.Assert(err, IsNil)

	var t2 Time
	t2.Type = TypeTimestamp
	err = t2.Unmarshal(b)
	c.Assert(err, IsNil)
	c.Assert(t1.String(), Equals, t2.String())

	b, err = ZeroDatetime.Marshal()
	c.Assert(err, IsNil)

	var t3 Time
	t3.Type = TypeDatetime
	err = t3.Unmarshal(b)
	c.Assert(err, IsNil)
	c.Assert(t3.String(), Equals, ZeroDatetime.String())

	t, err = ParseDatetime("0001-01-01 00:00:00")
	c.Assert(err, IsNil)
	b, err = t.Marshal()
	c.Assert(err, IsNil)

	var t4 Time
	t4.Type = TypeDatetime
	err = t4.Unmarshal(b)
	c.Assert(err, IsNil)
	c.Assert(t.String(), Equals, t4.String())

	tbl := []string{
		"2000-01-01 00:00:00.000000",
		"2000-01-01 00:00:00.123456",
		"0001-01-01 00:00:00.123456",
		"2000-06-01 00:00:00.999999",
	}

	for _, test := range tbl {
		t, err := ParseTime(test, TypeDatetime, MaxFsp)
		c.Assert(err, IsNil)

		b, err := t.Marshal()
		c.Assert(err, IsNil)

		var dest Time
		dest.Type = TypeDatetime
		dest.Fsp = MaxFsp
		err = dest.Unmarshal(b)
		c.Assert(err, IsNil)
		c.Assert(dest.String(), Equals, test)
	}
}

func (s *testTimeSuite) TestParseTimeFromNum(c *C) {
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
		{2010101011111, true, zeroDatetimeStr, true, zeroDatetimeStr, true, zeroDateStr},
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

	for _, test := range table {
		// test ParseDatetimeFromNum
		t, err := ParseDatetimeFromNum(test.Input)
		if test.ExpectDateTimeError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, TypeDatetime)
		}
		c.Assert(t.String(), Equals, test.ExpectDateTimeValue)

		// test ParseTimestampFromNum
		t, err = ParseTimestampFromNum(test.Input)
		if test.ExpectTimeStampError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, TypeTimestamp)
		}
		c.Assert(t.String(), Equals, test.ExpectTimeStampValue)

		// test ParseDateFromNum
		t, err = ParseDateFromNum(test.Input)

		if test.ExpectDateTimeError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, TypeDate)
		}
		c.Assert(t.String(), Equals, test.ExpectDateValue)
	}
}

func (s *testTimeSuite) TestToNumber(c *C) {
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
		t, err := ParseTime(test.Input, TypeDatetime, test.Fsp)
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
	tbl := []struct {
		S   string
		Fsp int
		Ret int
	}{
		{"1234567", 0, 0},
		{"1234567", 1, 100000},
		{"0000567", 5, 60},
		{"1234567", 5, 123460},
		{"1234567", 6, 123457},
		{"9999999", 6, 1000000},
	}

	for _, t := range tbl {
		v, err := parseFrac(t.S, t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.Ret, Equals, v)
	}
}

func (s *testTimeSuite) TestRoundFrac(c *C) {
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
	}

	for _, t := range tbl {
		v, err := ParseTime(t.Input, TypeDatetime, MaxFsp)
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
	tbl := []struct {
		Input  string
		Fsp    int
		Except string
	}{
		{"2012-12-31 11:30:45.123456", 4, "11:30:45.1235"},
		{"2012-12-31 11:30:45.123456", 6, "11:30:45.123456"},
		{"2012-12-31 11:30:45.123456", 0, "11:30:45"},
		{"2012-12-31 11:30:45.999999", 0, "11:30:46"},
		{"0000-00-00 00:00:00", 6, "00:00:00"},
	}

	for _, t := range tbl {
		v, err := ParseTime(t.Input, TypeDatetime, t.Fsp)
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
		t, err := v.ConvertToTime(TypeDatetime)
		c.Assert(err, IsNil)
		c.Assert(t.Time.Sub(n), Equals, v.Duration)
	}
}

func (s *testTimeSuite) TestCompare(c *C) {
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
		v1, err := ParseTime(t.Arg1, TypeDatetime, MaxFsp)
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
