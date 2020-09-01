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

package types_test

import (
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct {
}

func (s *testTimeSuite) TestDateTime(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
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
		{"00-00-00 00:00:00.123", "2000-00-00 00:00:00.123"},
		{"11111111111", "2011-11-11 11:11:01"},
		{"1701020301.", "2017-01-02 03:01:00"},
		{"1701020304.1", "2017-01-02 03:04:01.0"},
		{"1701020302.11", "2017-01-02 03:02:11.00"},
		{"170102036", "2017-01-02 03:06:00"},
		{"170102039.", "2017-01-02 03:09:00"},
		{"170102037.11", "2017-01-02 03:07:11.00"},
		{"2018-01-01 18", "2018-01-01 18:00:00"},
		{"18-01-01 18", "2018-01-01 18:00:00"},
		{"2018.01.01", "2018-01-01 00:00:00.00"},
		{"2018.01.01 00:00:00", "2018-01-01 00:00:00"},
		{"2018/01/01-00:00:00", "2018-01-01 00:00:00"},
		{"4710072", "2047-10-07 02:00:00"},
	}

	for _, test := range table {
		t, err := types.ParseDatetime(sc, test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	fspTbl := []struct {
		Input  string
		Fsp    int
		Expect string
	}{
		{"20170118.123", 6, "2017-01-18 12:03:00.000000"},
		{"121231113045.123345", 6, "2012-12-31 11:30:45.123345"},
		{"20121231113045.123345", 6, "2012-12-31 11:30:45.123345"},
		{"121231113045.9999999", 6, "2012-12-31 11:30:46.000000"},
		{"170105084059.575601", 0, "2017-01-05 08:41:00"},
		{"2017-01-05 23:59:59.575601", 0, "2017-01-06 00:00:00"},
		{"2017-01-31 23:59:59.575601", 0, "2017-02-01 00:00:00"},
		{"2017-00-05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"},
		{"2017.00.05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"},
		{"2017/00/05 23:59:58.575601", 3, "2017-00-05 23:59:58.576"},
		{"2017/00/05-23:59:58.575601", 3, "2017-00-05 23:59:58.576"},
	}

	for _, test := range fspTbl {
		t, err := types.ParseTime(sc, test.Input, mysql.TypeDatetime, test.Fsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	t, _ := types.ParseTime(sc, "121231113045.9999999", mysql.TypeDatetime, 6)
	c.Assert(t.Time.Second(), Equals, 46)
	c.Assert(t.Time.Microsecond(), Equals, 0)

	// test error
	errTable := []string{
		"1000-01-01 00:00:70",
		"1000-13-00 00:00:00",
		"1201012736.0000",
		"1201012736",
		"10000-01-01 00:00:00",
		"1000-09-31 00:00:00",
		"1001-02-29 00:00:00",
		"20170118.999",
		"2018-01",
		"2018.01",
	}

	for _, test := range errTable {
		_, err := types.ParseDatetime(sc, test)
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
		t, err := types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.UTC}, test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"2048-12-31 11:30:45",
		"1969-12-31 11:30:45",
	}

	for _, test := range errTable {
		_, err := types.ParseTimestamp(&stmtctx.StatementContext{TimeZone: time.UTC}, test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestDate(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		// standard format
		{"0001-12-13", "0001-12-13"},
		{"2011-12-13", "2011-12-13"},
		{"2011-12-13 10:10:10", "2011-12-13"},
		{"2015-06-01 12:12:12", "2015-06-01"},
		{"0001-01-01 00:00:00", "0001-01-01"},
		// 2-digit year
		{"00-12-31", "2000-12-31"},
		// alternative delimiters, any ASCII punctuation character is a valid delimiter,
		// punctuation character is defined by C++ std::ispunct: any graphical character
		// that is not alphanumeric.
		{"2011\"12\"13", "2011-12-13"},
		{"2011#12#13", "2011-12-13"},
		{"2011$12$13", "2011-12-13"},
		{"2011%12%13", "2011-12-13"},
		{"2011&12&13", "2011-12-13"},
		{"2011'12'13", "2011-12-13"},
		{"2011(12(13", "2011-12-13"},
		{"2011)12)13", "2011-12-13"},
		{"2011*12*13", "2011-12-13"},
		{"2011+12+13", "2011-12-13"},
		{"2011,12,13", "2011-12-13"},
		{"2011.12.13", "2011-12-13"},
		{"2011/12/13", "2011-12-13"},
		{"2011:12:13", "2011-12-13"},
		{"2011;12;13", "2011-12-13"},
		{"2011<12<13", "2011-12-13"},
		{"2011=12=13", "2011-12-13"},
		{"2011>12>13", "2011-12-13"},
		{"2011?12?13", "2011-12-13"},
		{"2011@12@13", "2011-12-13"},
		{"2011[12[13", "2011-12-13"},
		{"2011\\12\\13", "2011-12-13"},
		{"2011]12]13", "2011-12-13"},
		{"2011^12^13", "2011-12-13"},
		{"2011_12_13", "2011-12-13"},
		{"2011`12`13", "2011-12-13"},
		{"2011{12{13", "2011-12-13"},
		{"2011|12|13", "2011-12-13"},
		{"2011}12}13", "2011-12-13"},
		{"2011~12~13", "2011-12-13"},
		// alternative separators with time
		{"2011~12~13 12~12~12", "2011-12-13"},
		{"2011~12~13T12~12~12", "2011-12-13"},
		{"2011~12~13~12~12~12", "2011-12-13"},
		// internal format (YYYYMMDD, YYYYYMMDDHHMMSS)
		{"20111213", "2011-12-13"},
		{"111213", "2011-12-13"},
		// leading and trailing space
		{" 2011-12-13", "2011-12-13"},
		{"2011-12-13 ", "2011-12-13"},
		{"   2011-12-13    ", "2011-12-13"},
		// extra separators
		{"2011-12--13", "2011-12-13"},
		{"2011--12-13", "2011-12-13"},
		{"2011----12----13", "2011-12-13"},
		{"2011~/.12)_#13T T.12~)12[~12", "2011-12-13"},
		// combinations
		{"   2011----12----13    ", "2011-12-13"},
	}

	for _, test := range table {
		t, err := types.ParseDate(sc, test.Input)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"0121231",
		"1201012736.0000",
		"1201012736",
		"2019.01",
		// invalid separators
		"2019 01 02",
		"2019A01A02",
		"2019-01T02",
		"2011-12-13 10:10T10",
		"2019–01–02", // en dash
		"2019—01—02", // em dash
	}

	for _, test := range errTable {
		_, err := types.ParseDate(sc, test)
		c.Assert(err, NotNil)
	}
}

func (s *testTimeSuite) TestTime(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Expect string
	}{
		{"10:11:12", "10:11:12"},
		{"101112", "10:11:12"},
		{"020005", "02:00:05"},
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
		t, err := types.ParseDuration(sc, test.Input, types.MinFsp)
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
		t, err := types.ParseDuration(sc, test.Input, types.MaxFsp)
		c.Assert(err, IsNil)
		c.Assert(t.String(), Equals, test.Expect)
	}

	errTable := []string{
		"232 10",
		"-232 10",
	}

	for _, test := range errTable {
		_, err := types.ParseDuration(sc, test, types.DefaultFsp)
		c.Assert(err, NotNil)
	}

	t, err := types.ParseDuration(sc, "4294967295 0:59:59", types.DefaultFsp)
	c.Assert(err, NotNil)
	c.Assert(t.String(), Equals, "838:59:59")

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
		t1 := types.Duration{
			Duration: time.Duration(t.lhs),
			Fsp:      types.DefaultFsp,
		}
		t2 := types.Duration{
			Duration: time.Duration(t.rhs),
			Fsp:      types.DefaultFsp,
		}
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
		t, err := types.ParseDuration(nil, test.Input, test.Fsp)
		c.Assert(err, IsNil)
		ta, err := types.ParseDuration(nil, test.InputAdd, test.FspAdd)
		c.Assert(err, IsNil)
		result, err := t.Add(ta)
		c.Assert(err, IsNil)
		c.Assert(result.String(), Equals, test.Expect)
	}
	t, err := types.ParseDuration(nil, "00:00:00", 0)
	c.Assert(err, IsNil)
	ta := new(types.Duration)
	result, err := t.Add(*ta)
	c.Assert(err, IsNil)
	c.Assert(result.String(), Equals, "00:00:00")

	t = types.Duration{Duration: math.MaxInt64, Fsp: 0}
	tatmp, err := types.ParseDuration(nil, "00:01:00", 0)
	c.Assert(err, IsNil)
	_, err = t.Add(tatmp)
	c.Assert(err, NotNil)
}

func (s *testTimeSuite) TestDurationSub(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
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
		t, err := types.ParseDuration(sc, test.Input, test.Fsp)
		c.Assert(err, IsNil)
		ta, err := types.ParseDuration(sc, test.InputAdd, test.FspAdd)
		c.Assert(err, IsNil)
		result, err := t.Sub(ta)
		c.Assert(err, IsNil)
		c.Assert(result.String(), Equals, test.Expect)
	}
}

func (s *testTimeSuite) TestTimeFsp(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
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
		// fsp round overflow 60 seconds
		{"08:29:59.537368", 0, "08:30:00"},
		{"08:59:59.537368", 0, "09:00:00"},
	}

	for _, test := range table {
		t, err := types.ParseDuration(sc, test.Input, test.Fsp)
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
		_, err := types.ParseDuration(sc, test.Input, test.Fsp)
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
		t, err := types.ParseYear(test.Input)
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
		_, err := types.AdjustYear(test.Year, false)
		if test.Expect {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
		}
	}

	strYears := []struct {
		Year   int64
		Expect int64
	}{
		{0, 2000},
	}
	for _, test := range strYears {
		res, err := types.AdjustYear(test.Year, true)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.Expect)
	}

	numYears := []struct {
		Year   int64
		Expect int64
	}{
		{0, 0},
	}
	for _, test := range numYears {
		res, err := types.AdjustYear(test.Year, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.Expect)
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

	sc := &stmtctx.StatementContext{TimeZone: time.UTC}

	// MySQL timestamp value doesn't allow month=0 or day=0.
	t, err := types.ParseTimestamp(sc, "2016-12-00 00:00:00")
	c.Assert(err, NotNil)

	t, err = types.ParseTimestamp(sc, "2010-10-10 10:11:11")
	c.Assert(err, IsNil)
	_, err = t.ToPackedUint()
	c.Assert(err, IsNil)

	var t1 types.Time
	t1.Type = mysql.TypeTimestamp
	t1.Time = types.FromGoTime(time.Now())
	packed, err := t1.ToPackedUint()
	c.Assert(err, IsNil)

	var t2 types.Time
	t2.Type = mysql.TypeTimestamp
	err = t2.FromPackedUint(packed)
	c.Assert(err, IsNil)
	c.Assert(t1.String(), Equals, t2.String())

	packed, _ = types.ZeroDatetime.ToPackedUint()

	var t3 types.Time
	t3.Type = mysql.TypeDatetime
	err = t3.FromPackedUint(packed)
	c.Assert(err, IsNil)
	c.Assert(t3.String(), Equals, types.ZeroDatetime.String())

	t, err = types.ParseDatetime(nil, "0001-01-01 00:00:00")
	c.Assert(err, IsNil)
	packed, _ = t.ToPackedUint()

	var t4 types.Time
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
		t, err := types.ParseTime(nil, test, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)

		packed, _ = t.ToPackedUint()

		var dest types.Time
		dest.Type = mysql.TypeDatetime
		dest.Fsp = types.MaxFsp
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
		{2010101011111, false, "0201-01-01 01:11:11", true, types.ZeroDatetimeStr, false, "0201-01-01"},
		{201010101111, false, "2020-10-10 10:11:11", false, "2020-10-10 10:11:11", false, "2020-10-10"},
		{20101010111, false, "2002-01-01 01:01:11", false, "2002-01-01 01:01:11", false, "2002-01-01"},
		{2010101011, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{201010101, false, "2000-02-01 01:01:01", false, "2000-02-01 01:01:01", false, "2000-02-01"},
		{20101010, false, "2010-10-10 00:00:00", false, "2010-10-10 00:00:00", false, "2010-10-10"},
		{2010101, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{201010, false, "2020-10-10 00:00:00", false, "2020-10-10 00:00:00", false, "2020-10-10"},
		{20101, false, "2002-01-01 00:00:00", false, "2002-01-01 00:00:00", false, "2002-01-01"},
		{2010, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{201, false, "2000-02-01 00:00:00", false, "2000-02-01 00:00:00", false, "2000-02-01"},
		{20, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{2, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{0, false, types.ZeroDatetimeStr, false, types.ZeroDatetimeStr, false, types.ZeroDateStr},
		{-1, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{99999999999999, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{100000000000000, true, types.ZeroDatetimeStr, true, types.ZeroDatetimeStr, true, types.ZeroDateStr},
		{10000102000000, false, "1000-01-02 00:00:00", true, types.ZeroDatetimeStr, false, "1000-01-02"},
		{19690101000000, false, "1969-01-01 00:00:00", true, types.ZeroDatetimeStr, false, "1969-01-01"},
		{991231235959, false, "1999-12-31 23:59:59", false, "1999-12-31 23:59:59", false, "1999-12-31"},
		{691231235959, false, "2069-12-31 23:59:59", true, types.ZeroDatetimeStr, false, "2069-12-31"},
		{370119031407, false, "2037-01-19 03:14:07", false, "2037-01-19 03:14:07", false, "2037-01-19"},
		{380120031407, false, "2038-01-20 03:14:07", true, types.ZeroDatetimeStr, false, "2038-01-20"},
		{11111111111, false, "2001-11-11 11:11:11", false, "2001-11-11 11:11:11", false, "2001-11-11"},
	}

	for ith, test := range table {
		// testtypes.ParseDatetimeFromNum
		t, err := types.ParseDatetimeFromNum(nil, test.Input)
		if test.ExpectDateTimeError {
			c.Assert(err, NotNil, Commentf("%d", ith))
		} else {
			c.Assert(err, IsNil)
			c.Assert(t.Type, Equals, mysql.TypeDatetime)
		}
		c.Assert(t.String(), Equals, test.ExpectDateTimeValue)

		// testtypes.ParseTimestampFromNum
		t, err = types.ParseTimestampFromNum(&stmtctx.StatementContext{
			TimeZone: time.UTC,
		}, test.Input)
		if test.ExpectTimeStampError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil, Commentf("%d", ith))
			c.Assert(t.Type, Equals, mysql.TypeTimestamp)
		}
		c.Assert(t.String(), Equals, test.ExpectTimeStampValue)

		// testtypes.ParseDateFromNum
		t, err = types.ParseDateFromNum(nil, test.Input)

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
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
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
		t, err := types.ParseTime(nil, test.Input, mysql.TypeDatetime, test.Fsp)
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
		t, err := types.ParseTime(nil, test.Input, mysql.TypeDate, 0)
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
		t, err := types.ParseDuration(sc, test.Input, test.Fsp)
		c.Assert(err, IsNil)
		// now we can only changetypes.Duration's Fsp to check ToNumber with different Fsp
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
		v, overflow, err := types.ParseFrac(t.S, t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Ret)
		c.Assert(overflow, Equals, t.Overflow)
	}
}

func (s *testTimeSuite) TestRoundFrac(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	sc.TimeZone = time.UTC
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
		{"2011-11-11 10:10:10.888888", 0, "2011-11-11 10:10:11"},
		{"2011-11-11 10:10:10.111111", 0, "2011-11-11 10:10:10"},
		// TODO: MySQL can handle this case, but we can't.
		// {"2012-01-00 23:59:59.999999", 3, "2012-01-01 00:00:00.000"},
	}

	for _, t := range tbl {
		v, err := types.ParseTime(sc, t.Input, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		nv, err := v.RoundFrac(sc, t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(nv.String(), Equals, t.Except)
	}
	// test different time zone
	losAngelesTz, _ := time.LoadLocation("America/Los_Angeles")
	sc.TimeZone = losAngelesTz
	tbl = []struct {
		Input  string
		Fsp    int
		Except string
	}{
		{"2019-11-25 07:25:45.123456", 4, "2019-11-25 07:25:45.1235"},
		{"2019-11-25 07:25:45.123456", 5, "2019-11-25 07:25:45.12346"},
		{"2019-11-25 07:25:45.123456", 0, "2019-11-25 07:25:45"},
		{"2019-11-25 07:25:45.123456", 2, "2019-11-25 07:25:45.12"},
		{"2019-11-26 11:30:45.999999", 4, "2019-11-26 11:30:46.0000"},
		{"2019-11-26 11:30:45.999999", 0, "2019-11-26 11:30:46"},
		{"2019-11-26 11:30:45.999999", 3, "2019-11-26 11:30:46.000"},
	}

	for _, t := range tbl {
		v, err := types.ParseTime(sc, t.Input, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		nv, err := v.RoundFrac(sc, t.Fsp)
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
		v, err := types.ParseDuration(sc, t.Input, types.MaxFsp)
		c.Assert(err, IsNil)
		nv, err := v.RoundFrac(t.Fsp)
		c.Assert(err, IsNil)
		c.Assert(nv.String(), Equals, t.Except)
	}

	cols := []struct {
		input  time.Time
		fsp    int
		output time.Time
	}{
		{time.Date(2011, 11, 11, 10, 10, 10, 888888, time.UTC), 0, time.Date(2011, 11, 11, 10, 10, 10, 11, time.UTC)},
		{time.Date(2011, 11, 11, 10, 10, 10, 111111, time.UTC), 0, time.Date(2011, 11, 11, 10, 10, 10, 10, time.UTC)},
	}

	for _, col := range cols {
		res, err := types.RoundFrac(col.input, col.fsp)
		c.Assert(res.Second(), Equals, col.output.Second())
		c.Assert(err, IsNil)
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
		v, err := types.ParseTime(nil, t.Input, mysql.TypeDatetime, t.Fsp)
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

	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.TimeZone = time.UTC
	for _, t := range tblDuration {
		v, err := types.ParseDuration(sc, t.Input, t.Fsp)
		c.Assert(err, IsNil)
		year, month, day := time.Now().In(time.UTC).Date()
		n := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		t, err := v.ConvertToTime(sc, mysql.TypeDatetime)
		c.Assert(err, IsNil)
		// TODO: Consider time_zone variable.
		t1, _ := t.Time.GoTime(time.UTC)
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
		v1, err := types.ParseTime(nil, t.Arg1, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)

		ret, err := v1.CompareString(nil, t.Arg2)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, t.Ret)
	}

	v1, err := types.ParseTime(nil, "2011-10-10 11:11:11", mysql.TypeDatetime, types.MaxFsp)
	c.Assert(err, IsNil)
	res, err := v1.CompareString(nil, "Test should error")
	c.Assert(err, NotNil)
	c.Assert(res, Equals, 0)

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
		v1, err := types.ParseDuration(nil, t.Arg1, types.MaxFsp)
		c.Assert(err, IsNil)

		ret, err := v1.CompareString(nil, t.Arg2)
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
		d, err := types.ParseDuration(nil, t.Input, types.MaxFsp)
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
		{"2011-11-11  10:10:10", []string{"2011", "11", "11", "10", "10", "10"}},
		{"xx2011-11-11 10:10:10", nil},
		{"T10:10:10", nil},
		{"2011-11-11x", nil},
		{"xxx 10:10:10", nil},
	}

	for _, t := range tbl {
		r := types.ParseDateFormat(t.Input)
		c.Assert(r, DeepEquals, t.Result)
	}
}

func (s *testTimeSuite) TestTamestampDiff(c *C) {
	tests := []struct {
		unit   string
		t1     types.MysqlTime
		t2     types.MysqlTime
		expect int64
	}{
		{"MONTH", types.FromDate(2002, 5, 30, 0, 0, 0, 0), types.FromDate(2001, 1, 1, 0, 0, 0, 0), -16},
		{"YEAR", types.FromDate(2002, 5, 1, 0, 0, 0, 0), types.FromDate(2001, 1, 1, 0, 0, 0, 0), -1},
		{"MINUTE", types.FromDate(2003, 2, 1, 0, 0, 0, 0), types.FromDate(2003, 5, 1, 12, 5, 55, 0), 128885},
		{"MICROSECOND", types.FromDate(2002, 5, 30, 0, 0, 0, 0), types.FromDate(2002, 5, 30, 0, 13, 25, 0), 805000000},
		{"MICROSECOND", types.FromDate(2000, 1, 1, 0, 0, 0, 12345), types.FromDate(2000, 1, 1, 0, 0, 45, 32), 44987687},
		{"QUARTER", types.FromDate(2000, 1, 12, 0, 0, 0, 0), types.FromDate(2016, 1, 1, 0, 0, 0, 0), 63},
		{"QUARTER", types.FromDate(2016, 1, 1, 0, 0, 0, 0), types.FromDate(2000, 1, 12, 0, 0, 0, 0), -63},
	}

	for _, test := range tests {
		t1 := types.Time{
			Time: test.t1,
			Type: mysql.TypeDatetime,
			Fsp:  6,
		}
		t2 := types.Time{
			Time: test.t2,
			Type: mysql.TypeDatetime,
			Fsp:  6,
		}
		c.Assert(types.TimestampDiff(test.unit, t1, t2), Equals, test.expect)
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
		c.Assert(types.DateFSP(test.date), Equals, test.expect)
	}
}

func (s *testTimeSuite) TestConvertTimeZone(c *C) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	tests := []struct {
		input  types.MysqlTime
		from   *time.Location
		to     *time.Location
		expect types.MysqlTime
	}{
		{types.FromDate(2017, 1, 1, 0, 0, 0, 0), time.UTC, loc, types.FromDate(2017, 1, 1, 8, 0, 0, 0)},
		{types.FromDate(2017, 1, 1, 8, 0, 0, 0), loc, time.UTC, types.FromDate(2017, 1, 1, 0, 0, 0, 0)},
		{types.FromDate(0, 0, 0, 0, 0, 0, 0), loc, time.UTC, types.FromDate(0, 0, 0, 0, 0, 0, 0)},
	}

	for _, test := range tests {
		var t types.Time
		t.Time = test.input
		t.ConvertTimeZone(test.from, test.to)
		c.Assert(t.Compare(types.Time{Time: test.expect}), Equals, 0)
	}
}

func (s *testTimeSuite) TestTimeAdd(c *C) {
	tbl := []struct {
		Arg1 string
		Arg2 string
		Ret  string
	}{
		{"2017-01-18", "12:30:59", "2017-01-18 12:30:59"},
		{"2017-01-18 01:01:01", "12:30:59", "2017-01-18 13:32:00"},
		{"2017-01-18 01:01:01.123457", "12:30:59", "2017-01-18 13:32:0.123457"},
		{"2017-01-18 01:01:01", "838:59:59", "2017-02-22 00:01:00"},
		{"2017-08-21 15:34:42", "-838:59:59", "2017-07-17 16:34:43"},
		{"2017-08-21", "01:01:01.001", "2017-08-21 01:01:01.001"},
	}

	sc := &stmtctx.StatementContext{
		TimeZone: time.UTC,
	}
	for _, t := range tbl {
		v1, err := types.ParseTime(nil, t.Arg1, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		dur, err := types.ParseDuration(sc, t.Arg2, types.MaxFsp)
		c.Assert(err, IsNil)
		result, err := types.ParseTime(nil, t.Ret, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		v2, err := v1.Add(sc, dur)
		c.Assert(err, IsNil)
		c.Assert(v2.Compare(result), Equals, 0, Commentf("%v %v", v2.Time, result.Time))
	}
}

func (s *testTimeSuite) TestTruncateOverflowMySQLTime(c *C) {
	t := types.MaxTime + 1
	res, err := types.TruncateOverflowMySQLTime(t)
	c.Assert(types.ErrTruncatedWrongVal.Equal(err), IsTrue)
	c.Assert(res, Equals, types.MaxTime)

	t = types.MinTime - 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(types.ErrTruncatedWrongVal.Equal(err), IsTrue)
	c.Assert(res, Equals, types.MinTime)

	t = types.MaxTime
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MaxTime)

	t = types.MinTime
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MinTime)

	t = types.MaxTime - 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MaxTime-1)

	t = types.MinTime + 1
	res, err = types.TruncateOverflowMySQLTime(t)
	c.Assert(err, IsNil)
	c.Assert(res, Equals, types.MinTime+1)
}

func (s *testTimeSuite) TestCheckTimestamp(c *C) {

	shanghaiTz, _ := time.LoadLocation("Asia/Shanghai")

	tests := []struct {
		tz             *time.Location
		input          types.MysqlTime
		expectRetError bool
	}{{
		tz:             shanghaiTz,
		input:          types.FromDate(2038, 1, 19, 11, 14, 7, 0),
		expectRetError: false,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(1970, 1, 1, 8, 1, 1, 0),
		expectRetError: false,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(2038, 1, 19, 12, 14, 7, 0),
		expectRetError: true,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(1970, 1, 1, 7, 1, 1, 0),
		expectRetError: true,
	}, {
		tz:             time.UTC,
		input:          types.FromDate(2038, 1, 19, 3, 14, 7, 0),
		expectRetError: false,
	}, {
		tz:             time.UTC,
		input:          types.FromDate(1970, 1, 1, 0, 1, 1, 0),
		expectRetError: false,
	}, {
		tz:             time.UTC,
		input:          types.FromDate(2038, 1, 19, 4, 14, 7, 0),
		expectRetError: true,
	}, {
		tz:             time.UTC,
		input:          types.FromDate(1969, 1, 1, 0, 0, 0, 0),
		expectRetError: true,
	},
	}

	for _, t := range tests {
		validTimestamp := types.CheckTimestampTypeForTest(&stmtctx.StatementContext{TimeZone: t.tz}, t.input)
		if t.expectRetError {
			c.Assert(validTimestamp, NotNil, Commentf("For %s %s", t.input, t.tz))
		} else {
			c.Assert(validTimestamp, IsNil, Commentf("For %s %s", t.input, t.tz))
		}
	}

	// Issue #13605: "Invalid time format" caused by time zone issue
	// Some regions like Los Angeles use daylight saving time, see https://en.wikipedia.org/wiki/Daylight_saving_time
	losAngelesTz, _ := time.LoadLocation("America/Los_Angeles")
	LondonTz, _ := time.LoadLocation("Europe/London")

	tests = []struct {
		tz             *time.Location
		input          types.MysqlTime
		expectRetError bool
	}{{
		tz:             losAngelesTz,
		input:          types.FromDate(2018, 3, 11, 1, 0, 50, 0),
		expectRetError: false,
	}, {
		tz:             losAngelesTz,
		input:          types.FromDate(2018, 3, 11, 2, 0, 16, 0),
		expectRetError: true,
	}, {
		tz:             losAngelesTz,
		input:          types.FromDate(2018, 3, 11, 3, 0, 20, 0),
		expectRetError: false,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(2018, 3, 11, 1, 0, 50, 0),
		expectRetError: false,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(2018, 3, 11, 2, 0, 16, 0),
		expectRetError: false,
	}, {
		tz:             shanghaiTz,
		input:          types.FromDate(2018, 3, 11, 3, 0, 20, 0),
		expectRetError: false,
	}, {
		tz:             LondonTz,
		input:          types.FromDate(2019, 3, 31, 0, 0, 20, 0),
		expectRetError: false,
	}, {
		tz:             LondonTz,
		input:          types.FromDate(2019, 3, 31, 1, 0, 20, 0),
		expectRetError: true,
	}, {
		tz:             LondonTz,
		input:          types.FromDate(2019, 3, 31, 2, 0, 20, 0),
		expectRetError: false,
	},
	}

	for _, t := range tests {
		validTimestamp := types.CheckTimestampTypeForTest(&stmtctx.StatementContext{TimeZone: t.tz}, t.input)
		if t.expectRetError {
			c.Assert(validTimestamp, NotNil, Commentf("For %s %s", t.input, t.tz))
		} else {
			c.Assert(validTimestamp, IsNil, Commentf("For %s %s", t.input, t.tz))
		}
	}
}

func (s *testTimeSuite) TestExtractDurationValue(c *C) {
	tests := []struct {
		unit   string
		format string
		ans    string
		failed bool
	}{
		{
			unit:   "MICROSECOND",
			format: "50",
			ans:    "00:00:00.000050",
		},
		{
			unit:   "SECOND",
			format: "50",
			ans:    "00:00:50",
		},
		{
			unit:   "MINUTE",
			format: "10",
			ans:    "00:10:00",
		},
		{
			unit:   "HOUR",
			format: "10",
			ans:    "10:00:00",
		},
		{
			unit:   "DAY",
			format: "1",
			ans:    "24:00:00",
		},
		{
			unit:   "WEEK",
			format: "2",
			ans:    "336:00:00",
		},
		{
			unit:   "SECOND_MICROSECOND",
			format: "61.01",
			ans:    "00:01:01.010000",
		},
		{
			unit:   "MINUTE_MICROSECOND",
			format: "01:61.01",
			ans:    "00:02:01.010000",
		},
		{
			unit:   "MINUTE_SECOND",
			format: "61:61",
			ans:    "01:02:01.000000",
		},
		{
			unit:   "HOUR_MICROSECOND",
			format: "01:61:01.01",
			ans:    "02:01:01.010000",
		},
		{
			unit:   "HOUR_SECOND",
			format: "01:61:01",
			ans:    "02:01:01.000000",
		},
		{
			unit:   "HOUr_MINUTE",
			format: "2:2",
			ans:    "02:02:00",
		},
		{
			unit:   "DAY_MICRoSECOND",
			format: "1 1:1:1.02",
			ans:    "25:01:01.020000",
		},
		{
			unit:   "DAY_SeCOND",
			format: "1 02:03:04",
			ans:    "26:03:04.000000",
		},
		{
			unit:   "DAY_MINUTE",
			format: "1 1:2",
			ans:    "25:02:00",
		},
		{
			unit:   "DAY_HOUr",
			format: "1 1",
			ans:    "25:00:00",
		},
		{
			unit:   "DAY",
			format: "-35",
			failed: true,
		},
		{
			unit:   "day",
			format: "34",
			ans:    "816:00:00",
		},
		{
			unit:   "SECOND",
			format: "-3020400",
			failed: true,
		},
		{
			unit:   "MONTH",
			format: "1",
			ans:    "720:00:00",
		},
		{
			unit:   "MONTH",
			format: "-2",
			failed: true,
		},
		{
			unit:   "DAY_second",
			format: "34 23:59:59",
			failed: true,
		},
		{
			unit:   "DAY_hOUR",
			format: "-34 23",
			failed: true,
		},
	}
	failedComment := "failed at case %d, unit: %s, format: %s"
	for i, tt := range tests {
		dur, err := types.ExtractDurationValue(tt.unit, tt.format)
		if tt.failed {
			c.Assert(err, NotNil, Commentf(failedComment+", dur: %v", i, tt.unit, tt.format, dur.String()))
		} else {
			c.Assert(err, IsNil, Commentf(failedComment+", error stack", i, tt.unit, tt.format, errors.ErrorStack(err)))
			c.Assert(dur.String(), Equals, tt.ans, Commentf(failedComment, i, tt.unit, tt.format))
		}
	}
}

func (s *testTimeSuite) TestCurrentTime(c *C) {
	res := types.CurrentTime(mysql.TypeTimestamp)
	c.Assert(res.Time, NotNil)
	c.Assert(res.Type, Equals, mysql.TypeTimestamp)
	c.Assert(res.Fsp, Equals, 0)
}

func (s *testTimeSuite) TestInvalidZero(c *C) {
	in := types.Time{
		Time: types.ZeroTime,
		Type: mysql.TypeTimestamp,
		Fsp:  types.DefaultFsp,
	}
	c.Assert(in.InvalidZero(), Equals, true)
	in.Time = types.FromDate(2019, 00, 00, 00, 00, 00, 00)
	c.Assert(in.InvalidZero(), Equals, true)
	in.Time = types.FromDate(2019, 04, 12, 12, 00, 00, 00)
	c.Assert(in.InvalidZero(), Equals, false)
}

func (s *testTimeSuite) TestGetFsp(c *C) {
	res := types.GetFsp("2019:04:12 14:00:00.123456")
	c.Assert(res, Equals, 6)

	res = types.GetFsp("2019:04:12 14:00:00.1234567890")
	c.Assert(res, Equals, 6)

	res = types.GetFsp("2019:04:12 14:00:00.1")
	c.Assert(res, Equals, 1)

	res = types.GetFsp("2019:04:12 14:00:00")
	c.Assert(res, Equals, 0)
}

func (s *testTimeSuite) TestExtractDatetimeNum(c *C) {
	in := types.Time{
		Time: types.FromDate(2019, 04, 12, 14, 00, 00, 0000),
		Type: mysql.TypeTimestamp,
		Fsp:  types.DefaultFsp,
	}

	res, err := types.ExtractDatetimeNum(&in, "day")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(12))

	res, err = types.ExtractDatetimeNum(&in, "week")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(14))

	res, err = types.ExtractDatetimeNum(&in, "MONTH")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(4))

	res, err = types.ExtractDatetimeNum(&in, "QUARTER")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(2))

	res, err = types.ExtractDatetimeNum(&in, "YEAR")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(2019))

	res, err = types.ExtractDatetimeNum(&in, "DAY_MICROSECOND")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(12140000000000))

	res, err = types.ExtractDatetimeNum(&in, "DAY_SECOND")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(12140000))

	res, err = types.ExtractDatetimeNum(&in, "DAY_MINUTE")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(121400))

	res, err = types.ExtractDatetimeNum(&in, "DAY_HOUR")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(1214))

	res, err = types.ExtractDatetimeNum(&in, "YEAR_MONTH")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(201904))

	res, err = types.ExtractDatetimeNum(&in, "TEST_ERROR")
	c.Assert(res, Equals, int64(0))
	c.Assert(err, ErrorMatches, "invalid unit.*")

	in = types.Time{
		Time: types.FromDate(0000, 00, 00, 00, 00, 00, 0000),
		Type: mysql.TypeTimestamp,
		Fsp:  types.DefaultFsp,
	}

	res, err = types.ExtractDatetimeNum(&in, "day")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(0))

	res, err = types.ExtractDatetimeNum(&in, "week")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(0))

	res, err = types.ExtractDatetimeNum(&in, "MONTH")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(0))

	res, err = types.ExtractDatetimeNum(&in, "QUARTER")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(0))

	res, err = types.ExtractDatetimeNum(&in, "YEAR")
	c.Assert(err, IsNil)
	c.Assert(res, Equals, int64(0))
}

func (s *testTimeSuite) TestExtractDurationNum(c *C) {
	in := types.Duration{Duration: time.Duration(3600 * 24 * 365), Fsp: types.DefaultFsp}
	tbl := []struct {
		unit   string
		expect int64
	}{
		{"MICROSECOND", 31536},
		{"SECOND", 0},
		{"MINUTE", 0},
		{"HOUR", 0},
		{"SECOND_MICROSECOND", 31536},
		{"MINUTE_MICROSECOND", 31536},
		{"MINUTE_SECOND", 0},
		{"HOUR_MICROSECOND", 31536},
		{"HOUR_SECOND", 0},
		{"HOUR_MINUTE", 0},
	}

	for _, col := range tbl {
		res, err := types.ExtractDurationNum(&in, col.unit)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, col.expect)
	}
	res, err := types.ExtractDurationNum(&in, "TEST_ERROR")
	c.Assert(res, Equals, int64(0))
	c.Assert(err, ErrorMatches, "invalid unit.*")
}

func (s *testTimeSuite) TestParseDurationValue(c *C) {
	tbl := []struct {
		format string
		unit   string
		res1   int64
		res2   int64
		res3   int64
		res4   int64
		err    *terror.Error
	}{
		{"52", "WEEK", 0, 0, 52 * 7, 0, nil},
		{"12", "DAY", 0, 0, 12, 0, nil},
		{"04", "MONTH", 0, 04, 0, 0, nil},
		{"1", "QUARTER", 0, 1 * 3, 0, 0, nil},
		{"2019", "YEAR", 2019, 0, 0, 0, nil},
		{"10567890", "SECOND_MICROSECOND", 0, 0, 0, 10567890000, nil},
		{"10.567890", "SECOND_MICROSECOND", 0, 0, 0, 10567890000, nil},
		{"-10.567890", "SECOND_MICROSECOND", 0, 0, 0, -10567890000, nil},
		{"35:10567890", "MINUTE_SECOND", 0, 0, 122, 29190000000000, nil},      // 122 * 3600 * 24 + 29190 = 35 * 60 + 10567890
		{"3510567890", "MINUTE_SECOND", 0, 0, 40631, 49490000000000, nil},     // 40631 * 3600 * 24 + 49490 = 3510567890
		{"11:35:10.567890", "HOUR_MICROSECOND", 0, 0, 0, 41710567890000, nil}, // = (11 * 3600 + 35 * 60) * 1000000000 + 10567890000
		{"567890", "HOUR_MICROSECOND", 0, 0, 0, 567890000, nil},
		{"14:00", "HOUR_MINUTE", 0, 0, 0, 50400000000000, nil},
		{"14", "HOUR_MINUTE", 0, 0, 0, 840000000000, nil},
		{"12 14:00:00.345", "DAY_MICROSECOND", 0, 0, 12, 50400345000000, nil},
		{"12 14:00:00", "DAY_SECOND", 0, 0, 12, 50400000000000, nil},
		{"12 14:00", "DAY_MINUTE", 0, 0, 12, 50400000000000, nil},
		{"12 14", "DAY_HOUR", 0, 0, 12, 50400000000000, nil},
		{"1:1", "DAY_HOUR", 0, 0, 1, 3600000000000, nil},
		{"aa1bb1", "DAY_HOUR", 0, 0, 1, 3600000000000, nil},
		{"-1:1", "DAY_HOUR", 0, 0, -1, -3600000000000, nil},
		{"-aa1bb1", "DAY_HOUR", 0, 0, -1, -3600000000000, nil},
		{"2019-12", "YEAR_MONTH", 2019, 12, 0, 0, nil},
		{"1 1", "YEAR_MONTH", 1, 1, 0, 0, nil},
		{"aa1bb1", "YEAR_MONTH", 1, 1, 0, 0, nil},
		{"-1 1", "YEAR_MONTH", -1, -1, 0, 0, nil},
		{"-aa1bb1", "YEAR_MONTH", -1, -1, 0, 0, nil},
		{" \t\n\r\n - aa1bb1 \t\n ", "YEAR_MONTH", -1, -1, 0, 0, nil},
		{"1.111", "MICROSECOND", 0, 0, 0, 1000, types.ErrTruncatedWrongValue},
		{"1.111", "DAY", 0, 0, 1, 0, types.ErrTruncatedWrongValue},
	}
	for _, col := range tbl {
		comment := Commentf("Extract %v Unit %v", col.format, col.unit)
		res1, res2, res3, res4, err := types.ParseDurationValue(col.unit, col.format)
		c.Assert(res1, Equals, col.res1, comment)
		c.Assert(res2, Equals, col.res2, comment)
		c.Assert(res3, Equals, col.res3, comment)
		c.Assert(res4, Equals, col.res4, comment)
		if col.err == nil {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(col.err.Equal(err), IsTrue)
		}
	}

}

func (s *testTimeSuite) TestIsClockUnit(c *C) {
	tbl := []struct {
		input    string
		expected bool
	}{
		{"MICROSECOND", true},
		{"SECOND", true},
		{"MINUTE", true},
		{"HOUR", true},
		{"SECOND_MICROSECOND", true},
		{"MINUTE_MICROSECOND", true},
		{"MINUTE_SECOND", true},
		{"HOUR_MICROSECOND", true},
		{"HOUR_SECOND", true},
		{"HOUR_MINUTE", true},
		{"DAY_MICROSECOND", true},
		{"DAY_SECOND", true},
		{"DAY_MINUTE", true},
		{"DAY_HOUR", true},
		{"TEST", false},
	}
	for _, col := range tbl {
		output := types.IsClockUnit(col.input)
		c.Assert(output, Equals, col.expected)
	}
}

func (s *testTimeSuite) TestIsDateFormat(c *C) {
	input := "1234:321"
	output := types.IsDateFormat(input)
	c.Assert(output, Equals, false)

	input = "2019-04-01"
	output = types.IsDateFormat(input)
	c.Assert(output, Equals, true)

	input = "2019-4-1"
	output = types.IsDateFormat(input)
	c.Assert(output, Equals, true)
}

func (s *testTimeSuite) TestParseTimeFromInt64(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true

	input := int64(20190412140000)
	output, err := types.ParseTimeFromInt64(sc, input)
	c.Assert(err, IsNil)
	c.Assert(output.Fsp, Equals, types.DefaultFsp)
	c.Assert(output.Type, Equals, mysql.TypeDatetime)
	c.Assert(output.Time.Year(), Equals, 2019)
	c.Assert(output.Time.Month(), Equals, 04)
	c.Assert(output.Time.Day(), Equals, 12)
	c.Assert(output.Time.Hour(), Equals, 14)
	c.Assert(output.Time.Minute(), Equals, 00)
	c.Assert(output.Time.Second(), Equals, 00)
	c.Assert(output.Time.Microsecond(), Equals, 00)
}

func (s *testTimeSuite) TestGetFormatType(c *C) {
	input := "TEST"
	isDuration, isDate := types.GetFormatType(input)
	c.Assert(isDuration, Equals, false)
	c.Assert(isDate, Equals, false)

	input = "%y %m %d 2019 04 01"
	isDuration, isDate = types.GetFormatType(input)
	c.Assert(isDuration, Equals, false)
	c.Assert(isDate, Equals, true)

	input = "%h 30"
	isDuration, isDate = types.GetFormatType(input)
	c.Assert(isDuration, Equals, true)
	c.Assert(isDate, Equals, false)
}

func (s *testTimeSuite) TestgetFracIndex(c *C) {
	testCases := []struct {
		str         string
		expectIndex int
	}{
		{"2019.01.01 00:00:00", -1},
		{"2019.01.01 00:00:00.1", 19},
		{"12345.6", 5},
	}
	for _, testCase := range testCases {
		index := types.GetFracIndex(testCase.str)
		c.Assert(index, Equals, testCase.expectIndex)
	}
}

func (s *testTimeSuite) TestTimeOverflow(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	defer testleak.AfterTest(c)()
	table := []struct {
		Input  string
		Output bool
	}{
		{"2012-12-31 11:30:45", false},
		{"12-12-31 11:30:45", false},
		{"2012-12-31", false},
		{"20121231", false},
		{"2012-02-29", false},
		{"2018-01-01 18", false},
		{"18-01-01 18", false},
		{"2018.01.01", false},
		{"2018.01.01 00:00:00", false},
		{"2018/01/01-00:00:00", false},
	}

	for _, test := range table {
		t, err := types.ParseDatetime(sc, test.Input)
		c.Assert(err, IsNil)
		isOverflow, err := types.DateTimeIsOverflow(sc, t)
		c.Assert(err, IsNil)
		c.Assert(isOverflow, Equals, test.Output)
	}
}

func (s *testTimeSuite) TestTruncateFrac(c *C) {
	cols := []struct {
		input  time.Time
		fsp    int
		output time.Time
	}{
		{time.Date(2011, 11, 11, 10, 10, 10, 888888, time.UTC), 0, time.Date(2011, 11, 11, 10, 10, 10, 11, time.UTC)},
		{time.Date(2011, 11, 11, 10, 10, 10, 111111, time.UTC), 0, time.Date(2011, 11, 11, 10, 10, 10, 10, time.UTC)},
	}

	for _, col := range cols {
		res, err := types.TruncateFrac(col.input, col.fsp)
		c.Assert(res.Second(), Equals, col.output.Second())
		c.Assert(err, IsNil)
	}
}
func (s *testTimeSuite) TestTimeSub(c *C) {
	tbl := []struct {
		Arg1 string
		Arg2 string
		Ret  string
	}{
		{"2017-01-18 01:01:01", "2017-01-18 00:00:01", "01:01:00"},
		{"2017-01-18 01:01:01", "2017-01-18 01:01:01", "00:00:00"},
		{"2019-04-12 18:20:00", "2019-04-12 14:00:00", "04:20:00"},
	}

	sc := &stmtctx.StatementContext{
		TimeZone: time.UTC,
	}
	for _, t := range tbl {
		v1, err := types.ParseTime(nil, t.Arg1, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		v2, err := types.ParseTime(nil, t.Arg2, mysql.TypeDatetime, types.MaxFsp)
		c.Assert(err, IsNil)
		dur, err := types.ParseDuration(sc, t.Ret, types.MaxFsp)
		c.Assert(err, IsNil)
		rec := v1.Sub(sc, &v2)
		c.Assert(rec, Equals, dur)
	}
}

func (s *testTimeSuite) TestCheckMonthDay(c *C) {
	dates := []struct {
		date        types.MysqlTime
		isValidDate bool
	}{
		{types.FromDate(1900, 2, 29, 0, 0, 0, 0), false},
		{types.FromDate(1900, 2, 28, 0, 0, 0, 0), true},
		{types.FromDate(2000, 2, 29, 0, 0, 0, 0), true},
		{types.FromDate(2000, 1, 1, 0, 0, 0, 0), true},
		{types.FromDate(1900, 1, 1, 0, 0, 0, 0), true},
		{types.FromDate(1900, 1, 31, 0, 0, 0, 0), true},
		{types.FromDate(1900, 4, 1, 0, 0, 0, 0), true},
		{types.FromDate(1900, 4, 31, 0, 0, 0, 0), false},
		{types.FromDate(1900, 4, 30, 0, 0, 0, 0), true},
		{types.FromDate(2000, 2, 30, 0, 0, 0, 0), false},
		{types.FromDate(2000, 13, 1, 0, 0, 0, 0), false},
		{types.FromDate(4000, 2, 29, 0, 0, 0, 0), true},
		{types.FromDate(3200, 2, 29, 0, 0, 0, 0), true},
	}

	sc := &stmtctx.StatementContext{
		TimeZone:         time.UTC,
		AllowInvalidDate: false,
	}

	for _, t := range dates {
		tt := types.Time{
			Time: t.date,
			Type: mysql.TypeDate,
			Fsp:  types.DefaultFsp,
		}
		err := tt.Check(sc)
		if t.isValidDate {
			c.Check(err, IsNil)
		} else {
			c.Check(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
		}
	}
}

func (s *testTimeSuite) TestFromGoTime(c *C) {
	// Test rounding of nanosecond to millisecond.
	cases := []struct {
		input string
		yy    int
		mm    int
		dd    int
		hh    int
		min   int
		sec   int
		micro int
	}{
		{"2006-01-02T15:04:05.999999999Z", 2006, 1, 2, 15, 4, 6, 0},
		{"2006-01-02T15:04:05.999999000Z", 2006, 1, 2, 15, 4, 5, 999999},
		{"2006-01-02T15:04:05.999999499Z", 2006, 1, 2, 15, 4, 5, 999999},
		{"2006-01-02T15:04:05.999999500Z", 2006, 1, 2, 15, 4, 6, 0},
		{"2006-01-02T15:04:05.000000501Z", 2006, 1, 2, 15, 4, 5, 1},
	}

	for ith, ca := range cases {
		t, err := time.Parse(time.RFC3339Nano, ca.input)
		c.Assert(err, IsNil)

		t1 := types.FromGoTime(t)
		c.Assert(t1, Equals, types.FromDate(ca.yy, ca.mm, ca.dd, ca.hh, ca.min, ca.sec, ca.micro), Commentf("idx %d", ith))
	}

}

func benchmarkDateFormat(b *testing.B, name, str string) {
	b.Run(name, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			types.ParseDateFormat(str)
		}
	})
}

func BenchmarkParseDateFormat(b *testing.B) {
	benchmarkDateFormat(b, "date basic", "2011-12-13")
	benchmarkDateFormat(b, "date internal", "20111213")
	benchmarkDateFormat(b, "datetime basic", "2011-12-13 14:15:16")
	benchmarkDateFormat(b, "datetime internal", "20111213141516")
	benchmarkDateFormat(b, "datetime basic frac", "2011-12-13 14:15:16.123456")
	benchmarkDateFormat(b, "datetime repeated delimiters", "2011---12---13 14::15::16..123456")
}
