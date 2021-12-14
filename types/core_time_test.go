// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWeekBehaviour(t *testing.T) {
	require.Equal(t, weekBehaviour(1), weekBehaviourMondayFirst)
	require.Equal(t, weekBehaviour(2), weekBehaviourYear)
	require.Equal(t, weekBehaviour(4), weekBehaviourFirstWeekday)

	require.True(t, weekBehaviour(1).test(weekBehaviourMondayFirst))
	require.True(t, weekBehaviour(2).test(weekBehaviourYear))
	require.True(t, weekBehaviour(4).test(weekBehaviourFirstWeekday))
}

func TestWeek(t *testing.T) {
	tests := []struct {
		Input  CoreTime
		Mode   int
		Expect int
	}{
		{FromDate(2008, 2, 20, 0, 0, 0, 0), 0, 7},
		{FromDate(2008, 2, 20, 0, 0, 0, 0), 1, 8},
		{FromDate(2008, 12, 31, 0, 0, 0, 0), 1, 53},
	}

	for ith, tt := range tests {
		_, week := calcWeek(tt.Input, weekMode(tt.Mode))
		require.Equal(t, tt.Expect, week, "%d failed.", ith)
	}
}

func TestCalcDaynr(t *testing.T) {
	require.Equal(t, 0, calcDaynr(0, 0, 0))
	require.Equal(t, 3652424, calcDaynr(9999, 12, 31))
	require.Equal(t, 719528, calcDaynr(1970, 1, 1))
	require.Equal(t, 733026, calcDaynr(2006, 12, 16))
	require.Equal(t, 3654, calcDaynr(10, 1, 2))
	require.Equal(t, 733457, calcDaynr(2008, 2, 20))
}

func TestCalcTimeTimeDiff(t *testing.T) {
	tests := []struct {
		T1           CoreTime
		T2           CoreTime
		Sign         int
		ExpectSecond int
		ExpectMicro  int
	}{
		// calcTimeDiff can be used for month = 0.
		{
			FromDate(2006, 0, 1, 12, 23, 21, 0),
			FromDate(2006, 0, 3, 21, 23, 22, 0),
			1,
			57*3600 + 1,
			0,
		},
		{
			FromDate(0, 0, 0, 21, 23, 24, 0),
			FromDate(0, 0, 0, 11, 23, 22, 0),
			1,
			10*3600 + 2,
			0,
		},
		{
			FromDate(0, 0, 0, 1, 2, 3, 0),
			FromDate(0, 0, 0, 5, 2, 0, 0),
			-1,
			6*3600 + 4*60 + 3,
			0,
		},
	}

	for i, tt := range tests {
		seconds, microseconds, _ := calcTimeTimeDiff(tt.T1, tt.T2, tt.Sign)
		require.Equal(t, tt.ExpectSecond, seconds, "%d failed.", i)
		require.Equal(t, tt.ExpectMicro, microseconds, "%d failed.", i)
	}
}

func TestCompareTime(t *testing.T) {
	tests := []struct {
		T1     CoreTime
		T2     CoreTime
		Expect int
	}{
		{FromDate(0, 0, 0, 0, 0, 0, 0), FromDate(0, 0, 0, 0, 0, 0, 0), 0},
		{FromDate(0, 0, 0, 0, 1, 0, 0), FromDate(0, 0, 0, 0, 0, 0, 0), 1},
		{FromDate(2006, 1, 2, 3, 4, 5, 6), FromDate(2016, 1, 2, 3, 4, 5, 0), -1},
		{FromDate(0, 0, 0, 11, 22, 33, 0), FromDate(0, 0, 0, 12, 21, 33, 0), -1},
		{FromDate(9999, 12, 30, 23, 59, 59, 999999), FromDate(0, 1, 2, 3, 4, 5, 6), 1},
	}

	for _, tt := range tests {
		require.Equal(t, tt.Expect, compareTime(tt.T1, tt.T2))
		require.Equal(t, -tt.Expect, compareTime(tt.T2, tt.T1))
	}
}

func TestGetDateFromDaynr(t *testing.T) {
	tests := []struct {
		daynr uint
		year  uint
		month uint
		day   uint
	}{
		{730669, 2000, 7, 3},
		{720195, 1971, 10, 30},
		{719528, 1970, 01, 01},
		{719892, 1970, 12, 31},
		{730850, 2000, 12, 31},
		{730544, 2000, 2, 29},
		{204960, 561, 2, 28},
		{0, 0, 0, 0},
		{32, 0, 0, 0},
		{366, 1, 1, 1},
		{744729, 2038, 12, 31},
		{3652424, 9999, 12, 31},
	}

	for _, tt := range tests {
		yy, mm, dd := getDateFromDaynr(tt.daynr)
		require.Equal(t, tt.year, yy)
		require.Equal(t, tt.month, mm)
		require.Equal(t, tt.day, dd)
	}
}

func TestMixDateAndTime(t *testing.T) {
	tests := []struct {
		date   CoreTime
		dur    Duration
		neg    bool
		expect CoreTime
	}{
		{
			date:   FromDate(1896, 3, 4, 0, 0, 0, 0),
			dur:    NewDuration(12, 23, 24, 5, DefaultFsp),
			neg:    false,
			expect: FromDate(1896, 3, 4, 12, 23, 24, 5),
		},
		{
			date:   FromDate(1896, 3, 4, 0, 0, 0, 0),
			dur:    NewDuration(24, 23, 24, 5, DefaultFsp),
			neg:    false,
			expect: FromDate(1896, 3, 5, 0, 23, 24, 5),
		},
		{
			date:   FromDate(2016, 12, 31, 0, 0, 0, 0),
			dur:    NewDuration(24, 0, 0, 0, DefaultFsp),
			neg:    false,
			expect: FromDate(2017, 1, 1, 0, 0, 0, 0),
		},
		{
			date:   FromDate(2016, 12, 0, 0, 0, 0, 0),
			dur:    NewDuration(24, 0, 0, 0, DefaultFsp),
			neg:    false,
			expect: FromDate(2016, 12, 1, 0, 0, 0, 0),
		},
		{
			date:   FromDate(2017, 1, 12, 3, 23, 15, 0),
			dur:    NewDuration(2, 21, 10, 0, DefaultFsp),
			neg:    true,
			expect: FromDate(2017, 1, 12, 1, 2, 5, 0),
		},
	}

	for ith, tt := range tests {
		if tt.neg {
			mixDateAndDuration(&tt.date, tt.dur.Neg())
		} else {
			mixDateAndDuration(&tt.date, tt.dur)
		}
		require.Equal(t, 0, compareTime(tt.date, tt.expect), "%d", ith)
	}
}

func TestIsLeapYear(t *testing.T) {
	tests := []struct {
		T      CoreTime
		Expect bool
	}{
		{FromDate(1960, 1, 1, 0, 0, 0, 0), true},
		{FromDate(1963, 2, 21, 0, 0, 0, 0), false},
		{FromDate(2008, 11, 25, 0, 0, 0, 0), true},
		{FromDate(2017, 4, 24, 0, 0, 0, 0), false},
		{FromDate(1988, 2, 29, 0, 0, 0, 0), true},
		{FromDate(2000, 3, 15, 0, 0, 0, 0), true},
		{FromDate(1992, 5, 3, 0, 0, 0, 0), true},
		{FromDate(2024, 10, 1, 0, 0, 0, 0), true},
		{FromDate(2016, 6, 29, 0, 0, 0, 0), true},
		{FromDate(2015, 6, 29, 0, 0, 0, 0), false},
		{FromDate(2014, 9, 31, 0, 0, 0, 0), false},
		{FromDate(2001, 12, 7, 0, 0, 0, 0), false},
		{FromDate(1989, 7, 6, 0, 0, 0, 0), false},
	}

	for _, tt := range tests {
		require.Equal(t, tt.Expect, tt.T.IsLeapYear())
	}
}
func TestGetLastDay(t *testing.T) {
	tests := []struct {
		year        int
		month       int
		expectedDay int
	}{
		{2000, 1, 31},
		{2000, 2, 29},
		{2000, 4, 30},
		{1900, 2, 28},
		{1996, 2, 29},
	}

	for _, tt := range tests {
		day := GetLastDay(tt.year, tt.month)
		require.Equal(t, tt.expectedDay, day)
	}
}

func TestGetFixDays(t *testing.T) {
	tests := []struct {
		year        int
		month       int
		day         int
		ot          time.Time
		expectedDay int
	}{
		{2000, 1, 0, time.Date(2000, 1, 31, 0, 0, 0, 0, time.UTC), -2},
		{2000, 1, 12, time.Date(2000, 1, 31, 0, 0, 0, 0, time.UTC), 0},
		{2000, 1, 12, time.Date(2000, 1, 0, 0, 0, 0, 0, time.UTC), 0},
		{2000, 2, 24, time.Date(2000, 2, 10, 0, 0, 0, 0, time.UTC), 0},
		{2019, 04, 05, time.Date(2019, 04, 01, 1, 2, 3, 4, time.UTC), 0},
	}

	for _, tt := range tests {
		res := getFixDays(tt.year, tt.month, tt.day, tt.ot)
		require.Equal(t, tt.expectedDay, res)
	}
}

func TestAddDate(t *testing.T) {
	tests := []struct {
		year  int
		month int
		day   int
		ot    time.Time
	}{
		{01, 1, 0, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{02, 1, 12, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{03, 1, 12, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{04, 2, 24, time.Date(2000, 2, 10, 0, 0, 0, 0, time.UTC)},
		{01, 04, 05, time.Date(2019, 04, 01, 1, 2, 3, 4, time.UTC)},
	}

	for _, tt := range tests {
		res := AddDate(int64(tt.year), int64(tt.month), int64(tt.day), tt.ot)
		require.Equal(t, tt.year+tt.ot.Year(), res.Year())
	}
}

func TestWeekday(t *testing.T) {
	tests := []struct {
		Input  CoreTime
		Expect string
	}{
		{FromDate(2019, 01, 01, 0, 0, 0, 0), "Tuesday"},
		{FromDate(2019, 02, 31, 0, 0, 0, 0), "Sunday"},
		{FromDate(2019, 04, 31, 0, 0, 0, 0), "Wednesday"},
	}

	for _, tt := range tests {
		weekday := tt.Input.Weekday()
		require.Equal(t, tt.Expect, weekday.String())
	}
}

func TestFindZoneTransition(t *testing.T) {
	tests := []struct {
		TZ      string
		dt      string
		Expect  string
		Success bool
	}{
		{"Australia/Lord_Howe", "2020-06-29 03:45:00", "", false},
		{"Australia/Lord_Howe", "2020-10-04 02:15:00", "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", "2020-10-04 02:29:59", "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", "2020-10-04 02:29:59.99", "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", "2020-10-04 02:30:00.0001", "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", "2020-10-04 02:30:00", "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", "2020-10-04 02:30:01", "2020-10-04 02:30:00 +11 +1100", true},
		{"Europe/Vilnius", "2020-03-29 03:45:00", "2020-03-29 04:00:00 EEST +0300", true},
		{"Europe/Vilnius", "2020-10-25 03:45:00", "2020-10-25 03:00:00 EET +0200", true},
		{"Europe/Vilnius", "2020-06-29 03:45:00", "", false},
		{"Europe/Amsterdam", "2020-03-29 02:45:00", "2020-03-29 03:00:00 CEST +0200", true},
		{"Europe/Amsterdam", "2020-10-25 02:35:00", "2020-10-25 02:00:00 CET +0100", true},
		{"Europe/Amsterdam", "2020-03-29 02:59:59", "2020-03-29 03:00:00 CEST +0200", true},
		{"Europe/Amsterdam", "2020-03-29 02:59:59.999999999", "2020-03-29 03:00:00 CEST +0200", true},
		{"Europe/Amsterdam", "2020-03-29 03:00:00.000000001", "2020-03-29 03:00:00 CEST +0200", true},
	}

	for _, tt := range tests {
		loc, err := time.LoadLocation(tt.TZ)
		require.NoError(t, err)
		tm, err := time.ParseInLocation("2006-01-02 15:04:05", tt.dt, loc)
		require.NoError(t, err)
		tp, err := FindZoneTransition(tm)
		if !tt.Success {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tt.Expect, tp.Format("2006-01-02 15:04:05.999999999 MST -0700"))
		}
	}
}

func TestAdjustedGoTime(t *testing.T) {
	tests := []struct {
		TZ      string
		dt      CoreTime
		Expect  string
		Success bool
	}{
		{"Australia/Lord_Howe", FromDate(2020, 10, 04, 01, 59, 59, 997), "2020-10-04 01:59:59.000997 +1030 +1030", true},
		{"Australia/Lord_Howe", FromDate(2020, 10, 04, 02, 00, 00, 0), "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", FromDate(2020, 10, 04, 02, 15, 00, 0), "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", FromDate(2020, 10, 04, 02, 29, 59, 999999), "2020-10-04 02:30:00 +11 +1100", true},
		{"Australia/Lord_Howe", FromDate(2020, 10, 04, 02, 30, 00, 1), "2020-10-04 02:30:00.000001 +11 +1100", true},
		{"Australia/Lord_Howe", FromDate(2020, 06, 29, 03, 45, 00, 0), "2020-06-29 03:45:00 +1030 +1030", true},
		{"Australia/Lord_Howe", FromDate(2020, 04, 04, 01, 45, 00, 0), "2020-04-04 01:45:00 +11 +1100", true},
		{"Europe/Vilnius", FromDate(2020, 03, 29, 03, 45, 00, 0), "2020-03-29 04:00:00 EEST +0300", true},
		{"Europe/Vilnius", FromDate(2020, 03, 29, 03, 59, 59, 456789), "2020-03-29 04:00:00 EEST +0300", true},
		{"Europe/Vilnius", FromDate(2020, 03, 29, 04, 00, 01, 130000), "2020-03-29 04:00:01.13 EEST +0300", true},
		{"Europe/Vilnius", FromDate(2020, 10, 25, 03, 45, 00, 0), "2020-10-25 03:45:00 EET +0200", true},
		{"Europe/Vilnius", FromDate(2020, 06, 29, 03, 45, 00, 0), "2020-06-29 03:45:00 EEST +0300", true},
		{"Europe/Amsterdam", FromDate(2020, 03, 29, 02, 45, 00, 0), "2020-03-29 03:00:00 CEST +0200", true},
		{"Europe/Amsterdam", FromDate(2020, 10, 25, 02, 35, 00, 0), "2020-10-25 02:35:00 CET +0100", true},
		{"UTC", FromDate(2020, 2, 31, 02, 35, 00, 0), "", false},
	}

	for _, tt := range tests {
		loc, err := time.LoadLocation(tt.TZ)
		require.NoError(t, err)
		tp, err := tt.dt.AdjustedGoTime(loc)
		if !tt.Success {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tt.Expect, tp.Format("2006-01-02 15:04:05.999999999 MST -0700"))
		}
	}
}
