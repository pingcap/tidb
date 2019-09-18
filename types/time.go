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
	"bytes"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/logutil"
	tidbMath "github.com/pingcap/tidb/util/math"
)

// Time format without fractional seconds precision.
const (
	DateFormat = "2006-01-02"
	TimeFormat = "2006-01-02 15:04:05"
	// TimeFSPFormat is time format with fractional seconds precision.
	TimeFSPFormat = "2006-01-02 15:04:05.000000"
)

const (
	// MinYear is the minimum for mysql year type.
	MinYear int16 = 1901
	// MaxYear is the maximum for mysql year type.
	MaxYear int16 = 2155
	// MaxDuration is the maximum for duration.
	MaxDuration int64 = 838*10000 + 59*100 + 59
	// MinTime is the minimum for mysql time type.
	MinTime = -gotime.Duration(838*3600+59*60+59) * gotime.Second
	// MaxTime is the maximum for mysql time type.
	MaxTime = gotime.Duration(838*3600+59*60+59) * gotime.Second
	// ZeroDatetimeStr is the string representation of a zero datetime.
	ZeroDatetimeStr = "0000-00-00 00:00:00"
	// ZeroDateStr is the string representation of a zero date.
	ZeroDateStr = "0000-00-00"

	// TimeMaxHour is the max hour for mysql time type.
	TimeMaxHour = 838
	// TimeMaxMinute is the max minute for mysql time type.
	TimeMaxMinute = 59
	// TimeMaxSecond is the max second for mysql time type.
	TimeMaxSecond = 59
	// TimeMaxValue is the maximum value for mysql time type.
	TimeMaxValue = TimeMaxHour*10000 + TimeMaxMinute*100 + TimeMaxSecond
	// TimeMaxValueSeconds is the maximum second value for mysql time type.
	TimeMaxValueSeconds = TimeMaxHour*3600 + TimeMaxMinute*60 + TimeMaxSecond
)

const (
	// YearIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	YearIndex = 0 + iota
	// MonthIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MonthIndex
	// DayIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	DayIndex
	// HourIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	HourIndex
	// MinuteIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MinuteIndex
	// SecondIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	SecondIndex
	// MicrosecondIndex is index of 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	MicrosecondIndex
)

const (
	// YearMonthMaxCnt is max parameters count 'YEARS-MONTHS' expr Format allowed
	YearMonthMaxCnt = 2
	// DayHourMaxCnt is max parameters count 'DAYS HOURS' expr Format allowed
	DayHourMaxCnt = 2
	// DayMinuteMaxCnt is max parameters count 'DAYS HOURS:MINUTES' expr Format allowed
	DayMinuteMaxCnt = 3
	// DaySecondMaxCnt is max parameters count 'DAYS HOURS:MINUTES:SECONDS' expr Format allowed
	DaySecondMaxCnt = 4
	// DayMicrosecondMaxCnt is max parameters count 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	DayMicrosecondMaxCnt = 5
	// HourMinuteMaxCnt is max parameters count 'HOURS:MINUTES' expr Format allowed
	HourMinuteMaxCnt = 2
	// HourSecondMaxCnt is max parameters count 'HOURS:MINUTES:SECONDS' expr Format allowed
	HourSecondMaxCnt = 3
	// HourMicrosecondMaxCnt is max parameters count 'HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	HourMicrosecondMaxCnt = 4
	// MinuteSecondMaxCnt is max parameters count 'MINUTES:SECONDS' expr Format allowed
	MinuteSecondMaxCnt = 2
	// MinuteMicrosecondMaxCnt is max parameters count 'MINUTES:SECONDS.MICROSECONDS' expr Format allowed
	MinuteMicrosecondMaxCnt = 3
	// SecondMicrosecondMaxCnt is max parameters count 'SECONDS.MICROSECONDS' expr Format allowed
	SecondMicrosecondMaxCnt = 2
	// TimeValueCnt is parameters count 'YEARS-MONTHS DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' expr Format
	TimeValueCnt = 7
)

// Zero values for different types.
var (
	// ZeroDuration is the zero value for Duration type.
	ZeroDuration = Duration{Duration: gotime.Duration(0), Fsp: DefaultFsp}

	// ZeroTime is the zero value for TimeInternal type.
	ZeroTime = MysqlTime{}

	// ZeroDatetime is the zero value for datetime Time.
	ZeroDatetime = Time{
		Time: ZeroTime,
		Type: mysql.TypeDatetime,
		Fsp:  DefaultFsp,
	}

	// ZeroTimestamp is the zero value for timestamp Time.
	ZeroTimestamp = Time{
		Time: ZeroTime,
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}

	// ZeroDate is the zero value for date Time.
	ZeroDate = Time{
		Time: ZeroTime,
		Type: mysql.TypeDate,
		Fsp:  DefaultFsp,
	}
)

var (
	// MinDatetime is the minimum for mysql datetime type.
	MinDatetime = FromDate(1000, 1, 1, 0, 0, 0, 0)
	// MaxDatetime is the maximum for mysql datetime type.
	MaxDatetime = FromDate(9999, 12, 31, 23, 59, 59, 999999)

	// BoundTimezone is the timezone for min and max timestamp.
	BoundTimezone = gotime.UTC
	// MinTimestamp is the minimum for mysql timestamp type.
	MinTimestamp = Time{
		Time: FromDate(1970, 1, 1, 0, 0, 1, 0),
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}
	// MaxTimestamp is the maximum for mysql timestamp type.
	MaxTimestamp = Time{
		Time: FromDate(2038, 1, 19, 3, 14, 7, 999999),
		Type: mysql.TypeTimestamp,
		Fsp:  DefaultFsp,
	}

	// WeekdayNames lists names of weekdays, which are used in builtin time function `dayname`.
	WeekdayNames = []string{
		"Monday",
		"Tuesday",
		"Wednesday",
		"Thursday",
		"Friday",
		"Saturday",
		"Sunday",
	}

	// MonthNames lists names of months, which are used in builtin time function `monthname`.
	MonthNames = []string{
		"January", "February",
		"March", "April",
		"May", "June",
		"July", "August",
		"September", "October",
		"November", "December",
	}
)

const (
	// GoDurationDay is the gotime.Duration which equals to a Day.
	GoDurationDay = gotime.Hour * 24
	// GoDurationWeek is the gotime.Duration which equals to a Week.
	GoDurationWeek = GoDurationDay * 7
)

// FromGoTime translates time.Time to mysql time internal representation.
func FromGoTime(t gotime.Time) MysqlTime {
	// Plus 500 nanosecond for rounding of the millisecond part.
	t = t.Add(500 * gotime.Nanosecond)

	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	microsecond := t.Nanosecond() / 1000
	return FromDate(year, int(month), day, hour, minute, second, microsecond)
}

// FromDate makes a internal time representation from the given date.
func FromDate(year int, month int, day int, hour int, minute int, second int, microsecond int) MysqlTime {
	return MysqlTime{
		year:        uint16(year),
		month:       uint8(month),
		day:         uint8(day),
		hour:        uint32(hour),
		minute:      uint8(minute),
		second:      uint8(second),
		microsecond: uint32(microsecond),
	}
}

// Clock returns the hour, minute, and second within the day specified by t.
func (t Time) Clock() (hour int, minute int, second int) {
	return t.Time.Hour(), t.Time.Minute(), t.Time.Second()
}

// Time is the struct for handling datetime, timestamp and date.
// TODO: check if need a NewTime function to set Fsp default value?
type Time struct {
	Time MysqlTime
	Type uint8
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int8
}

// MaxMySQLTime returns Time with maximum mysql time type.
func MaxMySQLTime(fsp int8) Time {
	return Time{Time: FromDate(0, 0, 0, TimeMaxHour, TimeMaxMinute, TimeMaxSecond, 0), Type: mysql.TypeDuration, Fsp: fsp}
}

// CurrentTime returns current time with type tp.
func CurrentTime(tp uint8) Time {
	return Time{Time: FromGoTime(gotime.Now()), Type: tp, Fsp: 0}
}

// ConvertTimeZone converts the time value from one timezone to another.
// The input time should be a valid timestamp.
func (t *Time) ConvertTimeZone(from, to *gotime.Location) error {
	if !t.IsZero() {
		raw, err := t.Time.GoTime(from)
		if err != nil {
			return errors.Trace(err)
		}
		converted := raw.In(to)
		t.Time = FromGoTime(converted)
	}
	return nil
}

func (t Time) String() string {
	if t.Type == mysql.TypeDate {
		// We control the format, so no error would occur.
		str, err := t.DateFormat("%Y-%m-%d")
		terror.Log(errors.Trace(err))
		return str
	}

	str, err := t.DateFormat("%Y-%m-%d %H:%i:%s")
	terror.Log(errors.Trace(err))
	if t.Fsp > 0 {
		tmp := fmt.Sprintf(".%06d", t.Time.Microsecond())
		str = str + tmp[:1+t.Fsp]
	}

	return str
}

// IsZero returns a boolean indicating whether the time is equal to ZeroTime.
func (t Time) IsZero() bool {
	return compareTime(t.Time, ZeroTime) == 0
}

// InvalidZero returns a boolean indicating whether the month or day is zero.
func (t Time) InvalidZero() bool {
	return t.Time.Month() == 0 || t.Time.Day() == 0
}

const numberFormat = "%Y%m%d%H%i%s"
const dateFormat = "%Y%m%d"

// ToNumber returns a formatted number.
// e.g,
// 2012-12-12 -> 20121212
// 2012-12-12T10:10:10 -> 20121212101010
// 2012-12-12T10:10:10.123456 -> 20121212101010.123456
func (t Time) ToNumber() *MyDecimal {
	if t.IsZero() {
		return &MyDecimal{}
	}

	// Fix issue #1046
	// Prevents from converting 2012-12-12 to 20121212000000
	var tfStr string
	if t.Type == mysql.TypeDate {
		tfStr = dateFormat
	} else {
		tfStr = numberFormat
	}

	s, err := t.DateFormat(tfStr)
	if err != nil {
		logutil.BgLogger().Error("[fatal] never happen because we've control the format!")
	}

	if t.Fsp > 0 {
		s1 := fmt.Sprintf("%s.%06d", s, t.Time.Microsecond())
		s = s1[:len(s)+int(t.Fsp)+1]
	}

	// We skip checking error here because time formatted string can be parsed certainly.
	dec := new(MyDecimal)
	err = dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
	return dec
}

// Convert converts t with type tp.
func (t Time) Convert(sc *stmtctx.StatementContext, tp uint8) (Time, error) {
	if t.Type == tp || t.IsZero() {
		return Time{Time: t.Time, Type: tp, Fsp: t.Fsp}, nil
	}

	t1 := Time{Time: t.Time, Type: tp, Fsp: t.Fsp}
	err := t1.check(sc)
	return t1, errors.Trace(err)
}

// ConvertToDuration converts mysql datetime, timestamp and date to mysql time type.
// e.g,
// 2012-12-12T10:10:10 -> 10:10:10
// 2012-12-12 -> 0
func (t Time) ConvertToDuration() (Duration, error) {
	if t.IsZero() {
		return ZeroDuration, nil
	}

	hour, minute, second := t.Clock()
	frac := t.Time.Microsecond() * 1000

	d := gotime.Duration(hour*3600+minute*60+second)*gotime.Second + gotime.Duration(frac)
	// TODO: check convert validation
	return Duration{Duration: d, Fsp: t.Fsp}, nil
}

// Compare returns an integer comparing the time instant t to o.
// If t is after o, return 1, equal o, return 0, before o, return -1.
func (t Time) Compare(o Time) int {
	return compareTime(t.Time, o.Time)
}

// compareTime compare two MysqlTime.
// return:
//  0: if a == b
//  1: if a > b
// -1: if a < b
func compareTime(a, b MysqlTime) int {
	ta := datetimeToUint64(a)
	tb := datetimeToUint64(b)

	switch {
	case ta < tb:
		return -1
	case ta > tb:
		return 1
	}

	switch {
	case a.Microsecond() < b.Microsecond():
		return -1
	case a.Microsecond() > b.Microsecond():
		return 1
	}

	return 0
}

// CompareString is like Compare,
// but parses string to Time then compares.
func (t Time) CompareString(sc *stmtctx.StatementContext, str string) (int, error) {
	// use MaxFsp to parse the string
	o, err := ParseTime(sc, str, t.Type, MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return t.Compare(o), nil
}

// roundTime rounds the time value according to digits count specified by fsp.
func roundTime(t gotime.Time, fsp int8) gotime.Time {
	d := gotime.Duration(math.Pow10(9 - int(fsp)))
	return t.Round(d)
}

// RoundFrac rounds the fraction part of a time-type value according to `fsp`.
func (t Time) RoundFrac(sc *stmtctx.StatementContext, fsp int8) (Time, error) {
	if t.Type == mysql.TypeDate || t.IsZero() {
		// date type has no fsp
		return t, nil
	}

	fsp, err := CheckFsp(int(fsp))
	if err != nil {
		return t, errors.Trace(err)
	}

	if fsp == t.Fsp {
		// have same fsp
		return t, nil
	}

	var nt MysqlTime
	if t1, err := t.Time.GoTime(sc.TimeZone); err == nil {
		t1 = roundTime(t1, fsp)
		nt = FromGoTime(t1)
	} else {
		// Take the hh:mm:ss part out to avoid handle month or day = 0.
		hour, minute, second, microsecond := t.Time.Hour(), t.Time.Minute(), t.Time.Second(), t.Time.Microsecond()
		t1 := gotime.Date(1, 1, 1, hour, minute, second, microsecond*1000, gotime.Local)
		t2 := roundTime(t1, fsp)
		hour, minute, second = t2.Clock()
		microsecond = t2.Nanosecond() / 1000

		// TODO: when hh:mm:ss overflow one day after rounding, it should be add to yy:mm:dd part,
		// but mm:dd may contain 0, it makes the code complex, so we ignore it here.
		if t2.Day()-1 > 0 {
			return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t.String()))
		}
		nt = FromDate(t.Time.Year(), t.Time.Month(), t.Time.Day(), hour, minute, second, microsecond)
	}

	return Time{Time: nt, Type: t.Type, Fsp: fsp}, nil
}

// GetFsp gets the fsp of a string.
func GetFsp(s string) int8 {
	index := GetFracIndex(s)
	var fsp int
	if index < 0 {
		fsp = 0
	} else {
		fsp = len(s) - index - 1
	}

	if fsp == len(s) {
		fsp = 0
	} else if fsp > 6 {
		fsp = 6
	}
	return int8(fsp)
}

// GetFracIndex finds the last '.' for get fracStr, index = -1 means fracStr not found.
// but for format like '2019.01.01 00:00:00', the index should be -1.
func GetFracIndex(s string) (index int) {
	index = -1
	for i := len(s) - 1; i >= 0; i-- {
		if unicode.IsPunct(rune(s[i])) {
			if s[i] == '.' {
				index = i
			}
			break
		}
	}

	return index
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:11
// and 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func RoundFrac(t gotime.Time, fsp int8) (gotime.Time, error) {
	_, err := CheckFsp(int(fsp))
	if err != nil {
		return t, errors.Trace(err)
	}
	return t.Round(gotime.Duration(math.Pow10(9-int(fsp))) * gotime.Nanosecond), nil
}

// TruncateFrac truncates fractional seconds precision with new fsp and returns a new one.
// 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:10
// 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func TruncateFrac(t gotime.Time, fsp int8) (gotime.Time, error) {
	if _, err := CheckFsp(int(fsp)); err != nil {
		return t, err
	}
	return t.Truncate(gotime.Duration(math.Pow10(9-int(fsp))) * gotime.Nanosecond), nil
}

// ToPackedUint encodes Time to a packed uint64 value.
//
//    1 bit  0
//   17 bits year*13+month   (year 0-9999, month 0-12)
//    5 bits day             (0-31)
//    5 bits hour            (0-23)
//    6 bits minute          (0-59)
//    6 bits second          (0-59)
//   24 bits microseconds    (0-999999)
//
//   Total: 64 bits = 8 bytes
//
//   0YYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
//
func (t Time) ToPackedUint() (uint64, error) {
	tm := t.Time
	if t.IsZero() {
		return 0, nil
	}
	year, month, day := tm.Year(), tm.Month(), tm.Day()
	hour, minute, sec := tm.Hour(), tm.Minute(), tm.Second()
	ymd := uint64(((year*13 + month) << 5) | day)
	hms := uint64(hour<<12 | minute<<6 | sec)
	micro := uint64(tm.Microsecond())
	return ((ymd<<17 | hms) << 24) | micro, nil
}

// FromPackedUint decodes Time from a packed uint64 value.
func (t *Time) FromPackedUint(packed uint64) error {
	if packed == 0 {
		t.Time = ZeroTime
		return nil
	}
	ymdhms := packed >> 24
	ymd := ymdhms >> 17
	day := int(ymd & (1<<5 - 1))
	ym := ymd >> 5
	month := int(ym % 13)
	year := int(ym / 13)

	hms := ymdhms & (1<<17 - 1)
	second := int(hms & (1<<6 - 1))
	minute := int((hms >> 6) & (1<<6 - 1))
	hour := int(hms >> 12)
	microsec := int(packed % (1 << 24))

	t.Time = FromDate(year, month, day, hour, minute, second, microsec)

	return nil
}

// check whether t matches valid Time format.
// If allowZeroInDate is false, it returns ErrZeroDate when month or day is zero.
// FIXME: See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_no_zero_in_date
func (t *Time) check(sc *stmtctx.StatementContext) error {
	allowZeroInDate := false
	allowInvalidDate := false
	// We should avoid passing sc as nil here as far as possible.
	if sc != nil {
		allowZeroInDate = sc.IgnoreZeroInDate
		allowInvalidDate = sc.AllowInvalidDate
	}
	var err error
	switch t.Type {
	case mysql.TypeTimestamp:
		err = checkTimestampType(sc, t.Time)
	case mysql.TypeDatetime:
		err = checkDatetimeType(t.Time, allowZeroInDate, allowInvalidDate)
	case mysql.TypeDate:
		err = checkDateType(t.Time, allowZeroInDate, allowInvalidDate)
	}
	return errors.Trace(err)
}

// Check if 't' is valid
func (t *Time) Check(sc *stmtctx.StatementContext) error {
	return t.check(sc)
}

// Sub subtracts t1 from t, returns a duration value.
// Note that sub should not be done on different time types.
func (t *Time) Sub(sc *stmtctx.StatementContext, t1 *Time) Duration {
	var duration gotime.Duration
	if t.Type == mysql.TypeTimestamp && t1.Type == mysql.TypeTimestamp {
		a, err := t.Time.GoTime(sc.TimeZone)
		terror.Log(errors.Trace(err))
		b, err := t1.Time.GoTime(sc.TimeZone)
		terror.Log(errors.Trace(err))
		duration = a.Sub(b)
	} else {
		seconds, microseconds, neg := calcTimeDiff(t.Time, t1.Time, 1)
		duration = gotime.Duration(seconds*1e9 + microseconds*1e3)
		if neg {
			duration = -duration
		}
	}

	fsp := t.Fsp
	if fsp < t1.Fsp {
		fsp = t1.Fsp
	}
	return Duration{
		Duration: duration,
		Fsp:      fsp,
	}
}

// Add adds d to t, returns the result time value.
func (t *Time) Add(sc *stmtctx.StatementContext, d Duration) (Time, error) {
	sign, hh, mm, ss, micro := splitDuration(d.Duration)
	seconds, microseconds, _ := calcTimeDiff(t.Time, FromDate(0, 0, 0, hh, mm, ss, micro), -sign)
	days := seconds / secondsIn24Hour
	year, month, day := getDateFromDaynr(uint(days))
	var tm MysqlTime
	tm.year, tm.month, tm.day = uint16(year), uint8(month), uint8(day)
	calcTimeFromSec(&tm, seconds%secondsIn24Hour, microseconds)
	if t.Type == mysql.TypeDate {
		tm.hour = 0
		tm.minute = 0
		tm.second = 0
		tm.microsecond = 0
	}
	fsp := t.Fsp
	if d.Fsp > fsp {
		fsp = d.Fsp
	}
	ret := Time{
		Time: tm,
		Type: t.Type,
		Fsp:  fsp,
	}
	return ret, ret.Check(sc)
}

// TimestampDiff returns t2 - t1 where t1 and t2 are date or datetime expressions.
// The unit for the result (an integer) is given by the unit argument.
// The legal values for unit are "YEAR" "QUARTER" "MONTH" "DAY" "HOUR" "SECOND" and so on.
func TimestampDiff(unit string, t1 Time, t2 Time) int64 {
	return timestampDiff(unit, t1.Time, t2.Time)
}

// ParseDateFormat parses a formatted date string and returns separated components.
func ParseDateFormat(format string) []string {
	format = strings.TrimSpace(format)

	start := 0
	// Initialize `seps` with capacity of 6. The input `format` is typically
	// a date time of the form "2006-01-02 15:04:05", which has 6 numeric parts
	// (the fractional second part is usually removed by `splitDateTime`).
	// Setting `seps`'s capacity to 6 avoids reallocation in this common case.
	seps := make([]string, 0, 6)
	for i := 0; i < len(format); i++ {
		// Date format must start and end with number.
		if i == 0 || i == len(format)-1 {
			if !unicode.IsNumber(rune(format[i])) {
				return nil
			}

			continue
		}

		// Separator is a single none-number char.
		if !unicode.IsNumber(rune(format[i])) {
			if !unicode.IsNumber(rune(format[i-1])) {
				return nil
			}

			seps = append(seps, format[start:i])
			start = i + 1
		}

	}

	seps = append(seps, format[start:])
	return seps
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
// The only delimiter recognized between a date and time part and a fractional seconds part is the decimal point.
func splitDateTime(format string) (seps []string, fracStr string) {
	index := GetFracIndex(format)
	if index > 0 {
		fracStr = format[index+1:]
		format = format[:index]
	}

	seps = ParseDateFormat(format)
	return
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
func parseDatetime(sc *stmtctx.StatementContext, str string, fsp int8, isFloat bool) (Time, error) {
	// Try to split str with delimiter.
	// TODO: only punctuation can be the delimiter for date parts or time parts.
	// But only space and T can be the delimiter between the date and time part.
	var (
		year, month, day, hour, minute, second int
		fracStr                                string
		hhmmss                                 bool
		err                                    error
	)

	seps, fracStr := splitDateTime(str)
	var truncatedOrIncorrect bool
	switch len(seps) {
	case 1:
		l := len(seps[0])
		switch l {
		case 14: // No delimiter.
			// YYYYMMDDHHMMSS
			_, err = fmt.Sscanf(seps[0], "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			hhmmss = true
		case 12: // YYMMDDHHMMSS
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
			hhmmss = true
		case 11: // YYMMDDHHMMS
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
			hhmmss = true
		case 10: // YYMMDDHHMM
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute)
			year = adjustYear(year)
		case 9: // YYMMDDHHM
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%2d%1d", &year, &month, &day, &hour, &minute)
			year = adjustYear(year)
		case 8: // YYYYMMDD
			_, err = fmt.Sscanf(seps[0], "%4d%2d%2d", &year, &month, &day)
		case 6, 5:
			// YYMMDD && YYMMD
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d", &year, &month, &day)
			year = adjustYear(year)
		default:
			return ZeroDatetime, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(str))
		}
		if l == 5 || l == 6 || l == 8 {
			// YYMMDD or YYYYMMDD
			// We must handle float => string => datetime, the difference is that fractional
			// part of float type is discarded directly, while fractional part of string type
			// is parsed to HH:MM:SS.
			if isFloat {
				// 20170118.123423 => 2017-01-18 00:00:00
			} else {
				// '20170118.123423' => 2017-01-18 12:34:23.234
				switch len(fracStr) {
				case 0:
				case 1, 2:
					_, err = fmt.Sscanf(fracStr, "%2d ", &hour)
				case 3, 4:
					_, err = fmt.Sscanf(fracStr, "%2d%2d ", &hour, &minute)
				default:
					_, err = fmt.Sscanf(fracStr, "%2d%2d%2d ", &hour, &minute, &second)
				}
				truncatedOrIncorrect = err != nil
			}
		}
		if l == 9 || l == 10 {
			if len(fracStr) == 0 {
				second = 0
			} else {
				_, err = fmt.Sscanf(fracStr, "%2d ", &second)
			}
			truncatedOrIncorrect = err != nil
		}
		if truncatedOrIncorrect && sc != nil {
			sc.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs("datetime", str))
			err = nil
		}
	case 2:
		// YYYY-MM is not valid
		if len(fracStr) == 0 {
			return ZeroDatetime, errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(str))
		}

		// YYYY-MM.DD, DD is treat as fracStr
		err = scanTimeArgs(append(seps, fracStr), &year, &month, &day)
		fracStr = ""
	case 3:
		// YYYY-MM-DD
		err = scanTimeArgs(seps, &year, &month, &day)
	case 4:
		// YYYY-MM-DD HH
		err = scanTimeArgs(seps, &year, &month, &day, &hour)
	case 5:
		// YYYY-MM-DD HH-MM
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute)
	case 6:
		// We don't have fractional seconds part.
		// YYYY-MM-DD HH-MM-SS
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute, &second)
		hhmmss = true
	default:
		return ZeroDatetime, errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(str))
	}
	if err != nil {
		return ZeroDatetime, errors.Trace(err)
	}

	// If str is sepereated by delimiters, the first one is year, and if the year is 2 digit,
	// we should adjust it.
	// TODO: adjust year is very complex, now we only consider the simplest way.
	if len(seps[0]) == 2 {
		if year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && fracStr == "" {
			// Skip a special case "00-00-00".
		} else {
			year = adjustYear(year)
		}
	}

	var microsecond int
	var overflow bool
	if hhmmss {
		// If input string is "20170118.999", without hhmmss, fsp is meanless.
		microsecond, overflow, err = ParseFrac(fracStr, fsp)
		if err != nil {
			return ZeroDatetime, errors.Trace(err)
		}
	}

	tmp := FromDate(year, month, day, hour, minute, second, microsecond)
	if overflow {
		// Convert to Go time and add 1 second, to handle input like 2017-01-05 08:40:59.575601
		t1, err := tmp.GoTime(gotime.Local)
		if err != nil {
			return ZeroDatetime, errors.Trace(err)
		}
		tmp = FromGoTime(t1.Add(gotime.Second))
	}

	nt := Time{
		Time: tmp,
		Type: mysql.TypeDatetime,
		Fsp:  fsp}

	return nt, nil
}

func scanTimeArgs(seps []string, args ...*int) error {
	if len(seps) != len(args) {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(seps))
	}

	var err error
	for i, s := range seps {
		*args[i], err = strconv.Atoi(s)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// ParseYear parses a formatted string and returns a year number.
func ParseYear(str string) (int16, error) {
	v, err := strconv.ParseInt(str, 10, 16)
	if err != nil {
		return 0, errors.Trace(err)
	}
	y := int16(v)

	if len(str) == 4 {
		// Nothing to do.
	} else if len(str) == 2 || len(str) == 1 {
		y = int16(adjustYear(int(y)))
	} else {
		return 0, errors.Trace(ErrInvalidYearFormat)
	}

	if y < MinYear || y > MaxYear {
		return 0, errors.Trace(ErrInvalidYearFormat)
	}

	return y, nil
}

// adjustYear adjusts year according to y.
// See https://dev.mysql.com/doc/refman/5.7/en/two-digit-years.html
func adjustYear(y int) int {
	if y >= 0 && y <= 69 {
		y = 2000 + y
	} else if y >= 70 && y <= 99 {
		y = 1900 + y
	}
	return y
}

// AdjustYear is used for adjusting year and checking its validation.
func AdjustYear(y int64, shouldAdjust bool) (int64, error) {
	if y == 0 && !shouldAdjust {
		return y, nil
	}
	y = int64(adjustYear(int(y)))
	if y < int64(MinYear) || y > int64(MaxYear) {
		return 0, errors.Trace(ErrInvalidYear)
	}

	return y, nil
}

// Duration is the type for MySQL TIME type.
type Duration struct {
	gotime.Duration
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int8
}

//Add adds d to d, returns a duration value.
func (d Duration) Add(v Duration) (Duration, error) {
	if &v == nil {
		return d, nil
	}
	dsum, err := AddInt64(int64(d.Duration), int64(v.Duration))
	if err != nil {
		return Duration{}, errors.Trace(err)
	}
	if d.Fsp >= v.Fsp {
		return Duration{Duration: gotime.Duration(dsum), Fsp: d.Fsp}, nil
	}
	return Duration{Duration: gotime.Duration(dsum), Fsp: v.Fsp}, nil
}

// Sub subtracts d to d, returns a duration value.
func (d Duration) Sub(v Duration) (Duration, error) {
	if &v == nil {
		return d, nil
	}
	dsum, err := SubInt64(int64(d.Duration), int64(v.Duration))
	if err != nil {
		return Duration{}, errors.Trace(err)
	}
	if d.Fsp >= v.Fsp {
		return Duration{Duration: gotime.Duration(dsum), Fsp: d.Fsp}, nil
	}
	return Duration{Duration: gotime.Duration(dsum), Fsp: v.Fsp}, nil
}

// String returns the time formatted using default TimeFormat and fsp.
func (d Duration) String() string {
	var buf bytes.Buffer

	sign, hours, minutes, seconds, fraction := splitDuration(d.Duration)
	if sign < 0 {
		buf.WriteByte('-')
	}

	fmt.Fprintf(&buf, "%02d:%02d:%02d", hours, minutes, seconds)
	if d.Fsp > 0 {
		buf.WriteString(".")
		buf.WriteString(d.formatFrac(fraction))
	}

	p := buf.String()

	return p
}

func (d Duration) formatFrac(frac int) string {
	s := fmt.Sprintf("%06d", frac)
	return s[0:d.Fsp]
}

// ToNumber changes duration to number format.
// e.g,
// 10:10:10 -> 101010
func (d Duration) ToNumber() *MyDecimal {
	sign, hours, minutes, seconds, fraction := splitDuration(d.Duration)
	var (
		s       string
		signStr string
	)

	if sign < 0 {
		signStr = "-"
	}

	if d.Fsp == 0 {
		s = fmt.Sprintf("%s%02d%02d%02d", signStr, hours, minutes, seconds)
	} else {
		s = fmt.Sprintf("%s%02d%02d%02d.%s", signStr, hours, minutes, seconds, d.formatFrac(fraction))
	}

	// We skip checking error here because time formatted string can be parsed certainly.
	dec := new(MyDecimal)
	err := dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
	return dec
}

// ConvertToTime converts duration to Time.
// Tp is TypeDatetime, TypeTimestamp and TypeDate.
func (d Duration) ConvertToTime(sc *stmtctx.StatementContext, tp uint8) (Time, error) {
	year, month, day := gotime.Now().In(sc.TimeZone).Date()
	sign, hour, minute, second, frac := splitDuration(d.Duration)
	datePart := FromDate(year, int(month), day, 0, 0, 0, 0)
	timePart := FromDate(0, 0, 0, hour, minute, second, frac)
	mixDateAndTime(&datePart, &timePart, sign < 0)

	t := Time{
		Time: datePart,
		Type: mysql.TypeDatetime,
		Fsp:  d.Fsp,
	}
	return t.Convert(sc, tp)
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 10:10:10.999999 round 0 -> 10:10:11
// and 10:10:10.000000 round 0 -> 10:10:10
func (d Duration) RoundFrac(fsp int8) (Duration, error) {
	fsp, err := CheckFsp(int(fsp))
	if err != nil {
		return d, errors.Trace(err)
	}

	if fsp == d.Fsp {
		return d, nil
	}

	n := gotime.Date(0, 0, 0, 0, 0, 0, 0, gotime.Local)
	nd := n.Add(d.Duration).Round(gotime.Duration(math.Pow10(9-int(fsp))) * gotime.Nanosecond).Sub(n)
	return Duration{Duration: nd, Fsp: fsp}, nil
}

// Compare returns an integer comparing the Duration instant t to o.
// If d is after o, return 1, equal o, return 0, before o, return -1.
func (d Duration) Compare(o Duration) int {
	if d.Duration > o.Duration {
		return 1
	} else if d.Duration == o.Duration {
		return 0
	} else {
		return -1
	}
}

// CompareString is like Compare,
// but parses str to Duration then compares.
func (d Duration) CompareString(sc *stmtctx.StatementContext, str string) (int, error) {
	// use MaxFsp to parse the string
	o, err := ParseDuration(sc, str, MaxFsp)
	if err != nil {
		return 0, err
	}

	return d.Compare(o), nil
}

// Hour returns current hour.
// e.g, hour("11:11:11") -> 11
func (d Duration) Hour() int {
	_, hour, _, _, _ := splitDuration(d.Duration)
	return hour
}

// Minute returns current minute.
// e.g, hour("11:11:11") -> 11
func (d Duration) Minute() int {
	_, _, minute, _, _ := splitDuration(d.Duration)
	return minute
}

// Second returns current second.
// e.g, hour("11:11:11") -> 11
func (d Duration) Second() int {
	_, _, _, second, _ := splitDuration(d.Duration)
	return second
}

// MicroSecond returns current microsecond.
// e.g, hour("11:11:11.11") -> 110000
func (d Duration) MicroSecond() int {
	_, _, _, _, frac := splitDuration(d.Duration)
	return frac
}

// ParseDuration parses the time form a formatted string with a fractional seconds part,
// returns the duration type Time value.
// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
func ParseDuration(sc *stmtctx.StatementContext, str string, fsp int8) (Duration, error) {
	var (
		day, hour, minute, second int
		err                       error
		sign                      = 0
		dayExists                 = false
		origStr                   = str
	)

	fsp, err = CheckFsp(int(fsp))
	if err != nil {
		return ZeroDuration, errors.Trace(err)
	}

	if len(str) == 0 {
		return ZeroDuration, nil
	} else if str[0] == '-' {
		str = str[1:]
		sign = -1
	}

	// Time format may has day.
	if n := strings.IndexByte(str, ' '); n >= 0 {
		if day, err = strconv.Atoi(str[:n]); err == nil {
			dayExists = true
		}
		str = str[n+1:]
	}

	var (
		integeralPart = str
		fracPart      int
		overflow      bool
	)
	if n := strings.IndexByte(str, '.'); n >= 0 {
		// It has fractional precision parts.
		fracStr := str[n+1:]
		fracPart, overflow, err = ParseFrac(fracStr, fsp)
		if err != nil {
			return ZeroDuration, errors.Trace(err)
		}
		integeralPart = str[0:n]
	}

	// It tries to split integeralPart with delimiter, time delimiter must be :
	seps := strings.Split(integeralPart, ":")

	switch len(seps) {
	case 1:
		if dayExists {
			hour, err = strconv.Atoi(seps[0])
		} else {
			// No delimiter.
			switch len(integeralPart) {
			case 7: // HHHMMSS
				_, err = fmt.Sscanf(integeralPart, "%3d%2d%2d", &hour, &minute, &second)
			case 6: // HHMMSS
				_, err = fmt.Sscanf(integeralPart, "%2d%2d%2d", &hour, &minute, &second)
			case 5: // HMMSS
				_, err = fmt.Sscanf(integeralPart, "%1d%2d%2d", &hour, &minute, &second)
			case 4: // MMSS
				_, err = fmt.Sscanf(integeralPart, "%2d%2d", &minute, &second)
			case 3: // MSS
				_, err = fmt.Sscanf(integeralPart, "%1d%2d", &minute, &second)
			case 2: // SS
				_, err = fmt.Sscanf(integeralPart, "%2d", &second)
			case 1: // 0S
				_, err = fmt.Sscanf(integeralPart, "%1d", &second)
			default: // Maybe contains date.
				t, err1 := ParseDatetime(sc, str)
				if err1 != nil {
					return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
				}
				var dur Duration
				dur, err1 = t.ConvertToDuration()
				if err1 != nil {
					return ZeroDuration, errors.Trace(err)
				}
				return dur.RoundFrac(fsp)
			}
		}
	case 2:
		// HH:MM
		_, err = fmt.Sscanf(integeralPart, "%2d:%2d", &hour, &minute)
	case 3:
		// Time format maybe HH:MM:SS or HHH:MM:SS.
		// See https://dev.mysql.com/doc/refman/5.7/en/time.html
		if len(seps[0]) == 3 {
			_, err = fmt.Sscanf(integeralPart, "%3d:%2d:%2d", &hour, &minute, &second)
		} else {
			_, err = fmt.Sscanf(integeralPart, "%2d:%2d:%2d", &hour, &minute, &second)
		}
	default:
		return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
	}

	if terror.ErrorEqual(err, io.EOF) {
		err = ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
	}
	if err != nil {
		return ZeroDuration, errors.Trace(err)
	}

	if overflow {
		second++
		fracPart = 0
	}
	// Invalid TIME values are converted to '00:00:00'.
	// See https://dev.mysql.com/doc/refman/5.7/en/time.html
	if minute >= 60 || second > 60 || (!overflow && second == 60) {
		return ZeroDuration, ErrTruncatedWrongVal.GenWithStackByArgs("time", origStr)
	}
	d := gotime.Duration(day*24*3600+hour*3600+minute*60+second)*gotime.Second + gotime.Duration(fracPart)*gotime.Microsecond
	if sign == -1 {
		d = -d
	}

	d, err = TruncateOverflowMySQLTime(d)
	return Duration{Duration: d, Fsp: fsp}, errors.Trace(err)
}

// TruncateOverflowMySQLTime truncates d when it overflows, and return ErrTruncatedWrongVal.
func TruncateOverflowMySQLTime(d gotime.Duration) (gotime.Duration, error) {
	if d > MaxTime {
		return MaxTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
	} else if d < MinTime {
		return MinTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
	}

	return d, nil
}

func splitDuration(t gotime.Duration) (int, int, int, int, int) {
	sign := 1
	if t < 0 {
		t = -t
		sign = -1
	}

	hours := t / gotime.Hour
	t -= hours * gotime.Hour
	minutes := t / gotime.Minute
	t -= minutes * gotime.Minute
	seconds := t / gotime.Second
	t -= seconds * gotime.Second
	fraction := t / gotime.Microsecond

	return sign, int(hours), int(minutes), int(seconds), int(fraction)
}

var maxDaysInMonth = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func getTime(sc *stmtctx.StatementContext, num int64, tp byte) (Time, error) {
	s1 := num / 1000000
	s2 := num - s1*1000000

	year := int(s1 / 10000)
	s1 %= 10000
	month := int(s1 / 100)
	day := int(s1 % 100)

	hour := int(s2 / 10000)
	s2 %= 10000
	minute := int(s2 / 100)
	second := int(s2 % 100)

	t := Time{
		Time: FromDate(year, month, day, hour, minute, second, 0),
		Type: tp,
		Fsp:  DefaultFsp,
	}
	err := t.check(sc)
	return t, errors.Trace(err)
}

// parseDateTimeFromNum parses date time from num.
// See number_to_datetime function.
// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
func parseDateTimeFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	t := ZeroDate
	// Check zero.
	if num == 0 {
		return t, nil
	}

	// Check datetime type.
	if num >= 10000101000000 {
		t.Type = mysql.TypeDatetime
		return getTime(sc, num, t.Type)
	}

	// Check MMDD.
	if num < 101 {
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDD, year: 2000-2069
	if num <= (70-1)*10000+1231 {
		num = (num + 20000000) * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check YYMMDD.
	if num < 70*10000+101 {
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDD, year: 1970-1999
	if num <= 991231 {
		num = (num + 19000000) * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check YYYYMMDD.
	if num < 10000101 {
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust hour/min/second.
	if num <= 99991231 {
		num = num * 1000000
		return getTime(sc, num, t.Type)
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Set TypeDatetime type.
	t.Type = mysql.TypeDatetime

	// Adjust year
	// YYMMDDHHMMSS, 2000-2069
	if num <= 69*10000000000+1231235959 {
		num = num + 20000000000000
		return getTime(sc, num, t.Type)
	}

	// Check YYYYMMDDHHMMSS.
	if num < 70*10000000000+101000000 {
		return t, errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(num))
	}

	// Adjust year
	// YYMMDDHHMMSS, 1970-1999
	if num <= 991231235959 {
		num = num + 19000000000000
		return getTime(sc, num, t.Type)
	}

	return getTime(sc, num, t.Type)
}

// ParseTime parses a formatted string with type tp and specific fsp.
// Type is TypeDatetime, TypeTimestamp and TypeDate.
// Fsp is in range [0, 6].
// MySQL supports many valid datetime format, but still has some limitation.
// If delimiter exists, the date part and time part is separated by a space or T,
// other punctuation character can be used as the delimiter between date parts or time parts.
// If no delimiter, the format must be YYYYMMDDHHMMSS or YYMMDDHHMMSS
// If we have fractional seconds part, we must use decimal points as the delimiter.
// The valid datetime range is from '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
// The valid timestamp range is from '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
// The valid date range is from '1000-01-01' to '9999-12-31'
func ParseTime(sc *stmtctx.StatementContext, str string, tp byte, fsp int8) (Time, error) {
	return parseTime(sc, str, tp, fsp, false)
}

// ParseTimeFromFloatString is similar to ParseTime, except that it's used to parse a float converted string.
func ParseTimeFromFloatString(sc *stmtctx.StatementContext, str string, tp byte, fsp int8) (Time, error) {
	return parseTime(sc, str, tp, fsp, true)
}

func parseTime(sc *stmtctx.StatementContext, str string, tp byte, fsp int8, isFloat bool) (Time, error) {
	fsp, err := CheckFsp(int(fsp))
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t, err := parseDatetime(sc, str, fsp, isFloat)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t.Type = tp
	if err = t.check(sc); err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	return t, nil
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(sc *stmtctx.StatementContext, str string) (Time, error) {
	return ParseTime(sc, str, mysql.TypeDatetime, GetFsp(str))
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(sc *stmtctx.StatementContext, str string) (Time, error) {
	return ParseTime(sc, str, mysql.TypeTimestamp, GetFsp(str))
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(sc *stmtctx.StatementContext, str string) (Time, error) {
	// date has no fractional seconds precision
	return ParseTime(sc, str, mysql.TypeDate, MinFsp)
}

// ParseTimeFromNum parses a formatted int64,
// returns the value which type is tp.
func ParseTimeFromNum(sc *stmtctx.StatementContext, num int64, tp byte, fsp int8) (Time, error) {
	fsp, err := CheckFsp(int(fsp))
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t, err := parseDateTimeFromNum(sc, num)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t.Type = tp
	t.Fsp = fsp
	if err := t.check(sc); err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	return t, nil
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	return ParseTimeFromNum(sc, num, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	return ParseTimeFromNum(sc, num, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(sc *stmtctx.StatementContext, num int64) (Time, error) {
	// date has no fractional seconds precision
	return ParseTimeFromNum(sc, num, mysql.TypeDate, MinFsp)
}

// TimeFromDays Converts a day number to a date.
func TimeFromDays(num int64) Time {
	if num < 0 {
		return Time{
			Time: FromDate(0, 0, 0, 0, 0, 0, 0),
			Type: mysql.TypeDate,
			Fsp:  0,
		}
	}
	year, month, day := getDateFromDaynr(uint(num))

	return Time{
		Time: FromDate(int(year), int(month), int(day), 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0,
	}
}

func checkDateType(t MysqlTime, allowZeroInDate, allowInvalidDate bool) error {
	year, month, day := t.Year(), t.Month(), t.Day()
	if year == 0 && month == 0 && day == 0 {
		return nil
	}

	if !allowZeroInDate && (month == 0 || day == 0) {
		return ErrIncorrectDatetimeValue.GenWithStackByArgs(fmt.Sprintf("%04d-%02d-%02d", year, month, day))
	}

	if err := checkDateRange(t); err != nil {
		return errors.Trace(err)
	}

	if err := checkMonthDay(year, month, day, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDateRange(t MysqlTime) error {
	// Oddly enough, MySQL document says date range should larger than '1000-01-01',
	// but we can insert '0001-01-01' actually.
	if t.Year() < 0 || t.Month() < 0 || t.Day() < 0 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}
	if compareTime(t, MaxDatetime) > 0 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}
	return nil
}

func checkMonthDay(year, month, day int, allowInvalidDate bool) error {
	if month < 0 || month > 12 {
		return errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(fmt.Sprintf("%d-%d-%d", year, month, day)))
	}

	maxDay := 31
	if !allowInvalidDate {
		if month > 0 {
			maxDay = maxDaysInMonth[month-1]
		}
		if month == 2 && !isLeapYear(uint16(year)) {
			maxDay = 28
		}
	}

	if day < 0 || day > maxDay {
		return errors.Trace(ErrIncorrectDatetimeValue.GenWithStackByArgs(fmt.Sprintf("%d-%d-%d", year, month, day)))
	}
	return nil
}

func checkTimestampType(sc *stmtctx.StatementContext, t MysqlTime) error {
	if compareTime(t, ZeroTime) == 0 {
		return nil
	}

	if sc == nil {
		return errors.New("statementContext is required during checkTimestampType")
	}

	var checkTime MysqlTime
	if sc.TimeZone != BoundTimezone {
		convertTime := Time{Time: t, Type: mysql.TypeTimestamp}
		err := convertTime.ConvertTimeZone(sc.TimeZone, BoundTimezone)
		if err != nil {
			return err
		}
		checkTime = convertTime.Time
	} else {
		checkTime = t
	}
	if compareTime(checkTime, MaxTimestamp.Time) > 0 || compareTime(checkTime, MinTimestamp.Time) < 0 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(t))
	}

	if _, err := t.GoTime(gotime.Local); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDatetimeType(t MysqlTime, allowZeroInDate, allowInvalidDate bool) error {
	if err := checkDateType(t, allowZeroInDate, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	hour, minute, second := t.Hour(), t.Minute(), t.Second()
	if hour < 0 || hour >= 24 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(hour))
	}
	if minute < 0 || minute >= 60 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(minute))
	}
	if second < 0 || second >= 60 {
		return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(second))
	}

	return nil
}

// ExtractDatetimeNum extracts time value number from datetime unit and format.
func ExtractDatetimeNum(t *Time, unit string) (int64, error) {
	// TODO: Consider time_zone variable.
	switch strings.ToUpper(unit) {
	case "DAY":
		return int64(t.Time.Day()), nil
	case "WEEK":
		week := t.Time.Week(0)
		return int64(week), nil
	case "MONTH":
		return int64(t.Time.Month()), nil
	case "QUARTER":
		m := int64(t.Time.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return int64(t.Time.Year()), nil
	case "DAY_MICROSECOND":
		h, m, s := t.Clock()
		d := t.Time.Day()
		return int64(d*1000000+h*10000+m*100+s)*1000000 + int64(t.Time.Microsecond()), nil
	case "DAY_SECOND":
		h, m, s := t.Clock()
		d := t.Time.Day()
		return int64(d)*1000000 + int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "DAY_MINUTE":
		h, m, _ := t.Clock()
		d := t.Time.Day()
		return int64(d)*10000 + int64(h)*100 + int64(m), nil
	case "DAY_HOUR":
		h, _, _ := t.Clock()
		d := t.Time.Day()
		return int64(d)*100 + int64(h), nil
	case "YEAR_MONTH":
		y, m := t.Time.Year(), t.Time.Month()
		return int64(y)*100 + int64(m), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// ExtractDurationNum extracts duration value number from duration unit and format.
func ExtractDurationNum(d *Duration, unit string) (int64, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return int64(d.MicroSecond()), nil
	case "SECOND":
		return int64(d.Second()), nil
	case "MINUTE":
		return int64(d.Minute()), nil
	case "HOUR":
		return int64(d.Hour()), nil
	case "SECOND_MICROSECOND":
		return int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "MINUTE_MICROSECOND":
		return int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "MINUTE_SECOND":
		return int64(d.Minute()*100 + d.Second()), nil
	case "HOUR_MICROSECOND":
		return int64(d.Hour())*10000000000 + int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond()), nil
	case "HOUR_SECOND":
		return int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second()), nil
	case "HOUR_MINUTE":
		return int64(d.Hour())*100 + int64(d.Minute()), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// parseSingleTimeValue parse the format according the given unit. If we set strictCheck true, we'll check whether
// the converted value not exceed the range of MySQL's TIME type.
// The first four returned values are year, month, day and nanosecond.
func parseSingleTimeValue(unit string, format string, strictCheck bool) (int64, int64, int64, int64, error) {
	// Format is a preformatted number, it format should be A[.[B]].
	decimalPointPos := strings.IndexRune(format, '.')
	if decimalPointPos == -1 {
		decimalPointPos = len(format)
	}
	sign := int64(1)
	if len(format) > 0 && format[0] == '-' {
		sign = int64(-1)
	}
	iv, err := strconv.ParseInt(format[0:decimalPointPos], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
	}
	riv := iv // Rounded integer value

	dv := int64(0)
	lf := len(format) - 1
	// Has fraction part
	if decimalPointPos < lf {
		if lf-decimalPointPos >= 6 {
			// MySQL rounds down to 1e-6.
			if dv, err = strconv.ParseInt(format[decimalPointPos+1:decimalPointPos+7], 10, 64); err != nil {
				return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
			}
		} else {
			if dv, err = strconv.ParseInt(format[decimalPointPos+1:]+"000000"[:6-(lf-decimalPointPos)], 10, 64); err != nil {
				return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(format)
			}
		}
		if dv >= 500000 { // Round up, and we should keep 6 digits for microsecond, so dv should in [000000, 999999].
			riv += sign
		}
		if unit != "SECOND" {
			err = ErrTruncatedWrongValue.GenWithStackByArgs(format)
		}
		dv *= sign
	}
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		if strictCheck && tidbMath.Abs(riv) > TimeMaxValueSeconds*1000 {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Microsecond)
		riv %= int64(GoDurationDay / gotime.Microsecond)
		return 0, 0, dayCount, riv * int64(gotime.Microsecond), err
	case "SECOND":
		if strictCheck && tidbMath.Abs(iv) > TimeMaxValueSeconds {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := iv / int64(GoDurationDay/gotime.Second)
		iv %= int64(GoDurationDay / gotime.Second)
		return 0, 0, dayCount, iv*int64(gotime.Second) + dv*int64(gotime.Microsecond), err
	case "MINUTE":
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour*60+TimeMaxMinute {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Minute)
		riv %= int64(GoDurationDay / gotime.Minute)
		return 0, 0, dayCount, riv * int64(gotime.Minute), err
	case "HOUR":
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / 24
		riv %= 24
		return 0, 0, dayCount, riv * int64(gotime.Hour), err
	case "DAY":
		if strictCheck && tidbMath.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, riv, 0, err
	case "WEEK":
		if strictCheck && 7*tidbMath.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, 7 * riv, 0, err
	case "MONTH":
		if strictCheck && tidbMath.Abs(riv) > 1 {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, riv, 0, 0, err
	case "QUARTER":
		if strictCheck {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 3 * riv, 0, 0, err
	case "YEAR":
		if strictCheck {
			return 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return riv, 0, 0, 0, err
	}

	return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// parseTimeValue gets years, months, days, nanoseconds from a string
// nanosecond will not exceed length of single day
// MySQL permits any punctuation delimiter in the expr format.
// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
func parseTimeValue(format string, index, cnt int) (int64, int64, int64, int64, error) {
	neg := false
	originalFmt := format
	format = strings.TrimSpace(format)
	if len(format) > 0 && format[0] == '-' {
		neg = true
		format = format[1:]
	}
	fields := make([]string, TimeValueCnt)
	for i := range fields {
		fields[i] = "0"
	}
	matches := numericRegex.FindAllString(format, -1)
	if len(matches) > cnt {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	for i := range matches {
		if neg {
			fields[index] = "-" + matches[len(matches)-1-i]
		} else {
			fields[index] = matches[len(matches)-1-i]
		}
		index--
	}

	years, err := strconv.ParseInt(fields[YearIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	months, err := strconv.ParseInt(fields[MonthIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	days, err := strconv.ParseInt(fields[DayIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}

	hours, err := strconv.ParseInt(fields[HourIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	minutes, err := strconv.ParseInt(fields[MinuteIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	seconds, err := strconv.ParseInt(fields[SecondIndex], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	microseconds, err := strconv.ParseInt(alignFrac(fields[MicrosecondIndex], int(MaxFsp)), 10, 64)
	if err != nil {
		return 0, 0, 0, 0, ErrIncorrectDatetimeValue.GenWithStackByArgs(originalFmt)
	}
	seconds = hours*3600 + minutes*60 + seconds
	days += seconds / (3600 * 24)
	seconds %= 3600 * 24
	return years, months, days, seconds*int64(gotime.Second) + microseconds*int64(gotime.Microsecond), nil
}

func parseAndValidateDurationValue(format string, index, cnt int) (int64, error) {
	year, month, day, nano, err := parseTimeValue(format, index, cnt)
	if err != nil {
		return 0, err
	}
	if year != 0 || month != 0 || tidbMath.Abs(day) > TimeMaxHour/24 {
		return 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	dur := day*int64(GoDurationDay) + nano
	if tidbMath.Abs(dur) > int64(MaxTime) {
		return 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	return dur, nil
}

// ParseDurationValue parses time value from time unit and format.
// Returns y years m months d days + n nanoseconds
// Nanoseconds will no longer than one day.
func ParseDurationValue(unit string, format string) (y int64, m int64, d int64, n int64, _ error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		return parseSingleTimeValue(unit, format, false)
	case "SECOND_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
	case "MINUTE_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
	case "MINUTE_SECOND":
		return parseTimeValue(format, SecondIndex, MinuteSecondMaxCnt)
	case "HOUR_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
	case "HOUR_SECOND":
		return parseTimeValue(format, SecondIndex, HourSecondMaxCnt)
	case "HOUR_MINUTE":
		return parseTimeValue(format, MinuteIndex, HourMinuteMaxCnt)
	case "DAY_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
	case "DAY_SECOND":
		return parseTimeValue(format, SecondIndex, DaySecondMaxCnt)
	case "DAY_MINUTE":
		return parseTimeValue(format, MinuteIndex, DayMinuteMaxCnt)
	case "DAY_HOUR":
		return parseTimeValue(format, HourIndex, DayHourMaxCnt)
	case "YEAR_MONTH":
		return parseTimeValue(format, MonthIndex, YearMonthMaxCnt)
	default:
		return 0, 0, 0, 0, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// ExtractDurationValue extract the value from format to Duration.
func ExtractDurationValue(unit string, format string) (Duration, error) {
	unit = strings.ToUpper(unit)
	switch unit {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		_, month, day, nano, err := parseSingleTimeValue(unit, format, true)
		if err != nil {
			return ZeroDuration, err
		}
		dur := Duration{Duration: gotime.Duration((month*30+day)*int64(GoDurationDay) + nano)}
		if unit == "MICROSECOND" {
			dur.Fsp = MaxFsp
		}
		return dur, err
	case "SECOND_MICROSECOND":
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "MINUTE_MICROSECOND":
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "MINUTE_SECOND":
		d, err := parseAndValidateDurationValue(format, SecondIndex, MinuteSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_MICROSECOND":
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_SECOND":
		d, err := parseAndValidateDurationValue(format, SecondIndex, HourSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "HOUR_MINUTE":
		d, err := parseAndValidateDurationValue(format, MinuteIndex, HourMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "DAY_MICROSECOND":
		d, err := parseAndValidateDurationValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "DAY_SECOND":
		d, err := parseAndValidateDurationValue(format, SecondIndex, DaySecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: MaxFsp}, nil
	case "DAY_MINUTE":
		d, err := parseAndValidateDurationValue(format, MinuteIndex, DayMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "DAY_HOUR":
		d, err := parseAndValidateDurationValue(format, HourIndex, DayHourMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: 0}, nil
	case "YEAR_MONTH":
		_, err := parseAndValidateDurationValue(format, MonthIndex, YearMonthMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		// MONTH must exceed the limit of mysql's duration. So just return overflow error.
		return ZeroDuration, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	default:
		return ZeroDuration, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// IsClockUnit returns true when unit is interval unit with hour, minute or second.
func IsClockUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR",
		"SECOND_MICROSECOND", "MINUTE_MICROSECOND", "MINUTE_SECOND",
		"HOUR_MICROSECOND", "HOUR_SECOND", "HOUR_MINUTE",
		"DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR":
		return true
	default:
		return false
	}
}

// IsDateFormat returns true when the specified time format could contain only date.
func IsDateFormat(format string) bool {
	format = strings.TrimSpace(format)
	seps := ParseDateFormat(format)
	length := len(format)
	switch len(seps) {
	case 1:
		if (length == 8) || (length == 6) {
			return true
		}
	case 3:
		return true
	}
	return false
}

// ParseTimeFromInt64 parses mysql time value from int64.
func ParseTimeFromInt64(sc *stmtctx.StatementContext, num int64) (Time, error) {
	return parseDateTimeFromNum(sc, num)
}

// DateFormat returns a textual representation of the time value formatted
// according to layout.
// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t Time) DateFormat(layout string) (string, error) {
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range layout {
		if inPatternMatch {
			if err := t.convertDateFormat(b, &buf); err != nil {
				return "", errors.Trace(err)
			}
			inPatternMatch = false
			continue
		}

		// It's not in pattern match now.
		if b == '%' {
			inPatternMatch = true
		} else {
			buf.WriteRune(b)
		}
	}
	return buf.String(), nil
}

var abbrevWeekdayName = []string{
	"Sun", "Mon", "Tue",
	"Wed", "Thu", "Fri", "Sat",
}

func (t Time) convertDateFormat(b rune, buf *bytes.Buffer) error {
	switch b {
	case 'b':
		m := t.Time.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(m))
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Time.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrInvalidTimeFormat.GenWithStackByArgs(m))
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		buf.WriteString(FormatIntWidthN(t.Time.Month(), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Time.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Time.Day()), 10))
		buf.WriteString(abbrDayOfMonth(t.Time.Day()))
	case 'd':
		buf.WriteString(FormatIntWidthN(t.Time.Day(), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Time.Day()), 10))
	case 'j':
		fmt.Fprintf(buf, "%03d", t.Time.YearDay())
	case 'H':
		buf.WriteString(FormatIntWidthN(t.Time.Hour(), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Time.Hour()), 10))
	case 'h', 'I':
		t := t.Time.Hour()
		if t%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntWidthN(t%12, 2))
		}
	case 'l':
		t := t.Time.Hour()
		if t%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(t%12), 10))
		}
	case 'i':
		buf.WriteString(FormatIntWidthN(t.Time.Minute(), 2))
	case 'p':
		hour := t.Time.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Time.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Time.Minute(), t.Time.Second())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Time.Minute(), t.Time.Second())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Time.Minute(), t.Time.Second())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Time.Minute(), t.Time.Second())
		}
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Time.Hour(), t.Time.Minute(), t.Time.Second())
	case 'S', 's':
		buf.WriteString(FormatIntWidthN(t.Time.Second(), 2))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.Time.Microsecond())
	case 'U':
		w := t.Time.Week(0)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'u':
		w := t.Time.Week(1)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'V':
		w := t.Time.Week(2)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'v':
		_, w := t.Time.YearWeek(3)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'a':
		weekday := t.Time.Weekday()
		buf.WriteString(abbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.Time.Weekday().String())
	case 'w':
		buf.WriteString(strconv.FormatInt(int64(t.Time.Weekday()), 10))
	case 'X':
		year, _ := t.Time.YearWeek(2)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'x':
		year, _ := t.Time.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'Y':
		buf.WriteString(FormatIntWidthN(t.Time.Year(), 4))
	case 'y':
		str := FormatIntWidthN(t.Time.Year(), 4)
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}

	return nil
}

// FormatIntWidthN uses to format int with width. Insufficient digits are filled by 0.
func FormatIntWidthN(num, n int) string {
	numString := strconv.FormatInt(int64(num), 10)
	if len(numString) >= n {
		return numString
	}
	padBytes := make([]byte, n-len(numString))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numString
}

func abbrDayOfMonth(day int) string {
	var str string
	switch day {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}
	return str
}

// StrToDate converts date string according to format.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t *Time) StrToDate(sc *stmtctx.StatementContext, date, format string) bool {
	ctx := make(map[string]int)
	var tm MysqlTime
	if !strToDate(&tm, date, format, ctx) {
		t.Time = ZeroTime
		t.Type = mysql.TypeDatetime
		t.Fsp = 0
		return false
	}
	if err := mysqlTimeFix(&tm, ctx); err != nil {
		return false
	}

	t.Time = tm
	t.Type = mysql.TypeDatetime
	return t.check(sc) == nil
}

// mysqlTimeFix fixes the MysqlTime use the values in the context.
func mysqlTimeFix(t *MysqlTime, ctx map[string]int) error {
	// Key of the ctx is the format char, such as `%j` `%p` and so on.
	if yearOfDay, ok := ctx["%j"]; ok {
		// TODO: Implement the function that converts day of year to yy:mm:dd.
		_ = yearOfDay
	}
	if valueAMorPm, ok := ctx["%p"]; ok {
		if _, ok := ctx["%H"]; ok {
			return ErrInvalidTimeFormat.GenWithStackByArgs(t)
		}
		if t.hour == 0 {
			return ErrInvalidTimeFormat.GenWithStackByArgs(t)
		}
		if t.hour == 12 {
			// 12 is a special hour.
			switch valueAMorPm {
			case constForAM:
				t.hour = 0
			case constForPM:
				t.hour = 12
			}
			return nil
		}
		if valueAMorPm == constForPM {
			t.hour += 12
		}
	}
	return nil
}

// strToDate converts date string according to format, returns true on success,
// the value will be stored in argument t or ctx.
func strToDate(t *MysqlTime, date string, format string, ctx map[string]int) bool {
	date = skipWhiteSpace(date)
	format = skipWhiteSpace(format)

	token, formatRemain, succ := getFormatToken(format)
	if !succ {
		return false
	}

	if token == "" {
		// Extra characters at the end of date are ignored.
		return true
	}

	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		return false
	}

	return strToDate(t, dateRemain, formatRemain, ctx)
}

// getFormatToken takes one format control token from the string.
// format "%d %H %m" will get token "%d" and the remain is " %H %m".
func getFormatToken(format string) (token string, remain string, succ bool) {
	if len(format) == 0 {
		return "", "", true
	}

	// Just one character.
	if len(format) == 1 {
		if format[0] == '%' {
			return "", "", false
		}
		return format, "", true
	}

	// More than one character.
	if format[0] == '%' {
		return format[:2], format[2:], true
	}

	return format[:1], format[1:], true
}

func skipWhiteSpace(input string) string {
	for i, c := range input {
		if !unicode.IsSpace(c) {
			return input[i:]
		}
	}
	return ""
}

var weekdayAbbrev = map[string]gotime.Weekday{
	"Sun": gotime.Sunday,
	"Mon": gotime.Monday,
	"Tue": gotime.Tuesday,
	"Wed": gotime.Wednesday,
	"Thu": gotime.Tuesday,
	"Fri": gotime.Friday,
	"Sat": gotime.Saturday,
}

var monthAbbrev = map[string]gotime.Month{
	"Jan": gotime.January,
	"Feb": gotime.February,
	"Mar": gotime.March,
	"Apr": gotime.April,
	"May": gotime.May,
	"Jun": gotime.June,
	"Jul": gotime.July,
	"Aug": gotime.August,
	"Sep": gotime.September,
	"Oct": gotime.October,
	"Nov": gotime.November,
	"Dec": gotime.December,
}

type dateFormatParser func(t *MysqlTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,      // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,          // Month, numeric (0..12)
	"%d": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%e": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%f": microSeconds,          // Microseconds (000000..999999)
	"%h": hour24TwoDigits,       // Hour (01..12)
	"%H": hour24Numeric,         // Hour (00..23)
	"%I": hour12Numeric,         // Hour (01..12)
	"%i": minutesNumeric,        // Minutes, numeric (00..59)
	"%j": dayOfYearThreeDigits,  // Day of year (001..366)
	"%k": hour24Numeric,         // Hour (0..23)
	"%l": hour12Numeric,         // Hour (1..12)
	"%M": fullNameMonth,         // Month name (January..December)
	"%m": monthNumeric,          // Month, numeric (00..12)
	"%p": isAMOrPM,              // AM or PM
	"%r": time12Hour,            // Time, 12-hour (hh:mm:ss followed by AM or PM)
	"%s": secondsNumeric,        // Seconds (00..59)
	"%S": secondsNumeric,        // Seconds (00..59)
	"%T": time24Hour,            // Time, 24-hour (hh:mm:ss)
	"%Y": yearNumericFourDigits, // Year, numeric, four digits
	// Deprecated since MySQL 5.7.5
	"%y": yearNumericTwoDigits, // Year, numeric (two digits)
	// TODO: Add the following...
	// "%a": abbreviatedWeekday,         // Abbreviated weekday name (Sun..Sat)
	// "%D": dayOfMonthWithSuffix,       // Day of the month with English suffix (0th, 1st, 2nd, 3rd)
	// "%U": weekMode0,                  // Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
	// "%u": weekMode1,                  // Week (00..53), where Monday is the first day of the week; WEEK() mode 1
	// "%V": weekMode2,                  // Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
	// "%v": weekMode3,                  // Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
	// "%W": weekdayName,                // Weekday name (Sunday..Saturday)
	// "%w": dayOfWeek,                  // Day of the week (0=Sunday..6=Saturday)
	// "%X": yearOfWeek,                 // Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
	// "%x": yearOfWeek,                 // Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
}

// GetFormatType checks the type(Duration, Date or Datetime) of a format string.
func GetFormatType(format string) (isDuration, isDate bool) {
	format = skipWhiteSpace(format)
	var token string
	var succ bool
	for {
		token, format, succ = getFormatToken(format)
		if len(token) == 0 {
			break
		}
		if !succ {
			isDuration, isDate = false, false
			break
		}
		var durationTokens bool
		var dateTokens bool
		if len(token) >= 2 && token[0] == '%' {
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l':
				durationTokens = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				dateTokens = true
			}
		}
		if durationTokens {
			isDuration = true
		} else if dateTokens {
			isDate = true
		}
		if isDuration && isDate {
			break
		}
	}
	return
}

func matchDateWithToken(t *MysqlTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date, ctx)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

func parseDigits(input string, count int) (int, bool) {
	if count <= 0 || len(input) < count {
		return 0, false
	}

	v, err := strconv.ParseUint(input[:count], 10, 64)
	if err != nil {
		return int(v), false
	}
	return int(v), true
}

func hour24TwoDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v >= 24 {
		return input, false
	}
	t.hour = uint32(v)
	return input[2:], true
}

func secondsNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input)
	length := len(result)

	v, succ := parseDigits(input, length)
	if !succ || v >= 60 {
		return input, false
	}
	t.second = uint8(v)
	return input[length:], true
}

func minutesNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input)
	length := len(result)

	v, succ := parseDigits(input, length)
	if !succ || v >= 60 {
		return input, false
	}
	t.minute = uint8(v)
	return input[length:], true
}

const time12HourLen = len("hh:mm:ssAM")

func time12Hour(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	// hh:mm:ss AM
	if len(input) < time12HourLen {
		return input, false
	}
	hour, succ := parseDigits(input, 2)
	if !succ || hour > 12 || hour == 0 || input[2] != ':' {
		return input, false
	}

	minute, succ := parseDigits(input[3:], 2)
	if !succ || minute > 59 || input[5] != ':' {
		return input, false
	}

	second, succ := parseDigits(input[6:], 2)
	if !succ || second > 59 {
		return input, false
	}

	remain := skipWhiteSpace(input[8:])
	switch {
	case strings.HasPrefix(remain, "AM"):
		t.hour = uint32(hour)
	case strings.HasPrefix(remain, "PM"):
		t.hour = uint32(hour + 12)
	default:
		return input, false
	}

	t.minute = uint8(minute)
	t.second = uint8(second)
	return remain, true
}

const time24HourLen = len("hh:mm:ss")

func time24Hour(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	// hh:mm:ss
	if len(input) < time24HourLen {
		return input, false
	}

	hour, succ := parseDigits(input, 2)
	if !succ || hour > 23 || input[2] != ':' {
		return input, false
	}

	minute, succ := parseDigits(input[3:], 2)
	if !succ || minute > 59 || input[5] != ':' {
		return input, false
	}

	second, succ := parseDigits(input[6:], 2)
	if !succ || second > 59 {
		return input, false
	}

	t.hour = uint32(hour)
	t.minute = uint8(minute)
	t.second = uint8(second)
	return input[8:], true
}

const (
	constForAM = 1 + iota
	constForPM
)

func isAMOrPM(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) < 2 {
		return input, false
	}

	s := strings.ToLower(input[:2])
	switch s {
	case "am":
		ctx["%p"] = constForAM
	case "pm":
		ctx["%p"] = constForPM
	default:
		return input, false
	}
	return input[2:], true
}

// digitRegex: it was used to scan a variable-length monthly day or month in the string. Ex:  "01" or "1" or "30"
var oneOrTwoDigitRegex = regexp.MustCompile("^[0-9]{1,2}")

// twoDigitRegex: it was just for two digit number string. Ex: "01" or "12"
var twoDigitRegex = regexp.MustCompile("^[1-9][0-9]?")

// oneToSixDigitRegex: it was just for [0, 999999]
var oneToSixDigitRegex = regexp.MustCompile("^[0-9]{0,6}")

// numericRegex: it was for any numeric characters
var numericRegex = regexp.MustCompile("[0-9]+")

// parseTwoNumeric is used for pattens 0..31 0..24 0..60 and so on.
// It returns the parsed int, and remain data after parse.
func parseTwoNumeric(input string) (int, string) {
	if len(input) > 1 && input[0] == '0' {
		return 0, input[1:]
	}
	matched := twoDigitRegex.FindAllString(input, -1)
	if len(matched) == 0 {
		return 0, input
	}

	str := matched[0]
	v, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, input
	}
	return int(v), input[len(str):]
}

func dayOfMonthNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input) // 0..31
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 31 {
		return input, false
	}
	t.day = uint8(v)
	return input[length:], true
}

func hour24Numeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input) // 0..23
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 23 {
		return input, false
	}
	t.hour = uint32(v)
	ctx["%H"] = v
	return input[length:], true
}

func hour12Numeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input) // 1..12
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 12 || v == 0 {
		return input, false
	}
	t.hour = uint32(v)
	return input[length:], true
}

func microSeconds(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneToSixDigitRegex.FindString(input)
	length := len(result)
	if length == 0 {
		t.microsecond = 0
		return input, true
	}

	v, ok := parseDigits(input, length)

	if !ok {
		return input, false
	}
	for v > 0 && v*10 < 1000000 {
		v *= 10
	}
	t.microsecond = uint32(v)
	return input[length:], true
}

func yearNumericFourDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 4)
}

func yearNumericTwoDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 2)
}

func yearNumericNDigits(t *MysqlTime, input string, ctx map[string]int, n int) (string, bool) {
	effectiveCount, effectiveValue := 0, 0
	for effectiveCount+1 <= n {
		value, succeed := parseDigits(input, effectiveCount+1)
		if !succeed {
			break
		}
		effectiveCount++
		effectiveValue = value
	}
	if effectiveCount == 0 {
		return input, false
	}
	if effectiveCount <= 2 {
		effectiveValue = adjustYear(effectiveValue)
	}
	t.year = uint16(effectiveValue)
	return input[effectiveCount:], true
}

func dayOfYearThreeDigits(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 3)
	if !succ || v == 0 || v > 366 {
		return input, false
	}
	ctx["%j"] = v
	return input[3:], true
}

func abbreviatedWeekday(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) >= 3 {
		dayName := input[:3]
		if _, ok := weekdayAbbrev[dayName]; ok {
			// TODO: We need refact mysql time to support this.
			return input, false
		}
	}
	return input, false
}

func abbreviatedMonth(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) >= 3 {
		monthName := input[:3]
		if month, ok := monthAbbrev[monthName]; ok {
			t.month = uint8(month)
			return input[len(monthName):], true
		}
	}
	return input, false
}

func fullNameMonth(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	for i, month := range MonthNames {
		if strings.HasPrefix(input, month) {
			t.month = uint8(i + 1)
			return input[len(month):], true
		}
	}
	return input, false
}

func monthNumeric(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	result := oneOrTwoDigitRegex.FindString(input) // 1..12
	length := len(result)

	v, ok := parseDigits(input, length)

	if !ok || v > 12 {
		return input, false
	}
	t.month = uint8(v)
	return input[length:], true
}

//  dayOfMonthWithSuffix returns different suffix according t being which day. i.e. 0 return th. 1 return st.
func dayOfMonthWithSuffix(t *MysqlTime, input string, ctx map[string]int) (string, bool) {
	month, remain := parseOrdinalNumbers(input)
	if month >= 0 {
		t.month = uint8(month)
		return remain, true
	}
	return input, false
}

func parseOrdinalNumbers(input string) (value int, remain string) {
	for i, c := range input {
		if !unicode.IsDigit(c) {
			v, err := strconv.ParseUint(input[:i], 10, 64)
			if err != nil {
				return -1, input
			}
			value = int(v)
			break
		}
	}
	switch {
	case strings.HasPrefix(remain, "st"):
		if value == 1 {
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "nd"):
		if value == 2 {
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "th"):
		remain = remain[2:]
		return
	}
	return -1, input
}

// DateFSP gets fsp from date string.
func DateFSP(date string) (fsp int) {
	i := strings.LastIndex(date, ".")
	if i != -1 {
		fsp = len(date) - i - 1
	}
	return
}

// DateTimeIsOverflow return if this date is overflow.
// See: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DateTimeIsOverflow(sc *stmtctx.StatementContext, date Time) (bool, error) {
	tz := sc.TimeZone
	if tz == nil {
		tz = gotime.Local
	}

	var err error
	var b, e, t gotime.Time
	switch date.Type {
	case mysql.TypeDate, mysql.TypeDatetime:
		if b, err = MinDatetime.GoTime(tz); err != nil {
			return false, err
		}
		if e, err = MaxDatetime.GoTime(tz); err != nil {
			return false, err
		}
	case mysql.TypeTimestamp:
		minTS, maxTS := MinTimestamp, MaxTimestamp
		if tz != gotime.UTC {
			if err = minTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				return false, err
			}
			if err = maxTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				return false, err
			}
		}
		if b, err = minTS.Time.GoTime(tz); err != nil {
			return false, err
		}
		if e, err = maxTS.Time.GoTime(tz); err != nil {
			return false, err
		}
	default:
		return false, nil
	}

	if t, err = date.Time.GoTime(tz); err != nil {
		return false, err
	}

	inRange := (t.After(b) || t.Equal(b)) && (t.Before(e) || t.Equal(e))
	return !inRange, nil
}
