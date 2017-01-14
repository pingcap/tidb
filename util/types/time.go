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
	"math"
	"regexp"
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
)

// Portable analogs of some common call errors.
var (
	ErrInvalidTimeFormat = errors.New("invalid time format")
	ErrInvalidYearFormat = errors.New("invalid year format")
	ErrInvalidYear       = errors.New("invalid year")
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

	// MinTime is the minimum for mysql time type.
	MinTime = -gotime.Duration(838*3600+59*60+59) * gotime.Second
	// MaxTime is the maximum for mysql time type.
	MaxTime = gotime.Duration(838*3600+59*60+59) * gotime.Second

	zeroDatetimeStr = "0000-00-00 00:00:00"
	zeroDateStr     = "0000-00-00"
)

// Zero values for different types.
var (
	// ZeroDuration is the zero value for Duration type.
	ZeroDuration = Duration{Duration: gotime.Duration(0), Fsp: DefaultFsp}

	// ZeroTime is the zero value for TimeInternal type.
	ZeroTime = mysqlTime{}

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

	local = gotime.Local
)

var (
	// minDatetime is the minimum for mysql datetime type.
	minDatetime = FromDate(1000, 1, 1, 0, 0, 0, 0)
	// maxDatetime is the maximum for mysql datetime type.
	maxDatetime = FromDate(9999, 12, 31, 23, 59, 59, 999999)

	// minTimestamp is the minimum for mysql timestamp type.
	minTimestamp = gotime.Date(1970, 1, 1, 0, 0, 1, 0, gotime.UTC)
	// maxTimestamp is the maximum for mysql timestamp type.
	maxTimestamp = gotime.Date(2038, 1, 19, 3, 14, 7, 999999, gotime.UTC)

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

// TimeInternal is the internal representation for mysql time in TiDB.
type TimeInternal interface {
	Year() int
	Month() int
	Day() int
	Hour() int
	Minute() int
	Second() int
	Weekday() gotime.Weekday
	YearDay() int
	YearWeek(mode int) (int, int)
	Week(mode int) int
	Microsecond() int
	GoTime(*gotime.Location) (gotime.Time, error)
}

// FromGoTime translates time.Time to mysql time internal representation.
func FromGoTime(t gotime.Time) TimeInternal {
	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	microsecond := t.Nanosecond() / 1000
	return newMysqlTime(year, int(month), day, hour, minute, second, microsecond)
}

// FromDate makes a internal time representation from the given date.
func FromDate(year int, month int, day int, hour int, minute int, second int, microsecond int) TimeInternal {
	return newMysqlTime(year, month, day, hour, minute, second, microsecond)
}

// Clock returns the hour, minute, and second within the day specified by t.
func (t Time) Clock() (hour int, minute int, second int) {
	return t.Time.Hour(), t.Time.Minute(), t.Time.Second()
}

// Time is the struct for handling datetime, timestamp and date.
// TODO: check if need a NewTime function to set Fsp default value?
type Time struct {
	Time TimeInternal
	Type uint8
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

// CurrentTime returns current time with type tp.
func CurrentTime(tp uint8) Time {
	return Time{Time: FromGoTime(gotime.Now()), Type: tp, Fsp: 0}
}

func (t Time) String() string {
	if t.Type == mysql.TypeDate {
		// We control the format, so no error would occur.
		str, _ := t.DateFormat("%Y-%m-%d")
		return str
	}

	str, _ := t.DateFormat("%Y-%m-%d %H:%i:%s")
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
		log.Error("Fatal: never happen because we've control the format!")
	}

	if t.Fsp > 0 {
		s1 := fmt.Sprintf("%s.%06d", s, t.Time.Microsecond())
		s = s1[:len(s)+t.Fsp+1]
	}

	// We skip checking error here because time formatted string can be parsed certainly.
	dec := new(MyDecimal)
	dec.FromString([]byte(s))
	return dec
}

// Convert converts t with type tp.
func (t Time) Convert(tp uint8) (Time, error) {
	if t.Type == tp || t.IsZero() {
		return Time{Time: t.Time, Type: tp, Fsp: t.Fsp}, nil
	}

	t1 := Time{Time: t.Time, Type: tp, Fsp: t.Fsp}
	err := t1.check()
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
	return Duration{Duration: gotime.Duration(d), Fsp: t.Fsp}, nil
}

// Compare returns an integer comparing the time instant t to o.
// If t is after o, return 1, equal o, return 0, before o, return -1.
func (t Time) Compare(o Time) int {
	return compareTime(t.Time, o.Time)
}

func compareTime(a, b TimeInternal) int {
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
func (t Time) CompareString(str string) (int, error) {
	// use MaxFsp to parse the string
	o, err := ParseTime(str, t.Type, MaxFsp)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return t.Compare(o), nil
}

// roundTime rounds the time value according to digits count specified by fsp.
func roundTime(t gotime.Time, fsp int) gotime.Time {
	d := gotime.Duration(math.Pow10(9 - fsp))
	return t.Round(d)
}

func (t Time) roundFrac(fsp int) (Time, error) {
	if t.Type == mysql.TypeDate {
		// date type has no fsp
		return t, nil
	}

	fsp, err := checkFsp(fsp)
	if err != nil {
		return t, errors.Trace(err)
	}

	if fsp == t.Fsp {
		// have same fsp
		return t, nil
	}

	var nt TimeInternal
	// TODO: Consider time_zone variable.
	if t1, err := t.Time.GoTime(gotime.Local); err == nil {
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
			return t, errors.Trace(ErrInvalidTimeFormat)
		}
		nt = FromDate(t.Time.Year(), t.Time.Month(), t.Time.Day(), hour, minute, second, microsecond)
	}

	return Time{Time: nt, Type: t.Type, Fsp: fsp}, nil
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:11
// and 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func RoundFrac(t gotime.Time, fsp int) (gotime.Time, error) {
	_, err := checkFsp(fsp)
	if err != nil {
		return t, errors.Trace(err)
	}
	return t.Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond), nil
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
	if t.Type == mysql.TypeTimestamp {
		// TODO: Consider time_zone variable.
		if t1, err := t.Time.GoTime(gotime.Local); err == nil {
			utc := t1.UTC()
			tm = FromGoTime(utc)
		} else {
			// mysql timestamp month and day can't be zero.
			return 0, errors.Trace(err)
		}
	}
	year, month, day := tm.Year(), tm.Month(), tm.Day()
	hour, minute, sec := tm.Hour(), tm.Minute(), tm.Second()
	ymd := uint64(((year*13 + int(month)) << 5) | day)
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

	loc := local
	if t.Type == mysql.TypeTimestamp {
		loc = gotime.UTC
		t.Time = FromGoTime(gotime.Date(year, gotime.Month(month), day, hour, minute, second, microsec*1000, loc).In(local))
	} else {
		t.Time = FromDate(year, month, day, hour, minute, second, microsec)
		if err := t.check(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (t *Time) check() error {
	switch t.Type {
	case mysql.TypeTimestamp:
		return checkTimestampType(t.Time)
	case mysql.TypeDatetime:
		return checkDatetimeType(t.Time)
	case mysql.TypeDate:
		return checkDateType(t.Time)
	}
	return nil
}

// Sub subtracts t1 from t, returns a duration value.
// Note that sub should not be done on different time types.
func (t *Time) Sub(t1 *Time) Duration {
	var duration gotime.Duration
	if t.Type == mysql.TypeTimestamp && t1.Type == mysql.TypeTimestamp {
		// TODO: Consider time_zone variable.
		a, _ := t.Time.GoTime(gotime.Local)
		b, _ := t1.Time.GoTime(gotime.Local)
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

// TimestampDiff returns t2 - t1 where t1 and t2 are date or datetime expressions.
// The unit for the result (an integer) is given by the unit argument.
// The legal values for unit are "YEAR" "QUARTER" "MONTH" "DAY" "HOUR" "SECOND" and so on.
func TimestampDiff(unit string, t1 Time, t2 Time) int64 {
	return timestampDiff(unit, t1.Time, t2.Time)
}

func parseDateFormat(format string) []string {
	format = strings.TrimSpace(format)

	start := 0
	seps := []string{}
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

func parseDatetime(str string, fsp int) (Time, error) {
	// Try to split str with delimiter.
	// TODO: only punctuation can be the delimiter for date parts or time parts.
	// But only space and T can be the delimiter between the date and time part.
	var (
		year    int
		month   int
		day     int
		hour    int
		minute  int
		second  int
		fracStr string

		err error
	)

	seps := parseDateFormat(str)
	switch len(seps) {
	case 1:
		// No delimiter.
		if len(str) == 14 {
			// YYYYMMDDHHMMSS
			_, err = fmt.Sscanf(str, "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
		} else if len(str) == 12 {
			// YYMMDDHHMMSS
			_, err = fmt.Sscanf(str, "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
		} else if len(str) == 8 {
			// YYYYMMDD
			_, err = fmt.Sscanf(str, "%4d%2d%2d", &year, &month, &day)
		} else if len(str) == 6 {
			// YYMMDD
			_, err = fmt.Sscanf(str, "%2d%2d%2d", &year, &month, &day)
			year = adjustYear(year)
		} else {
			return ZeroDatetime, errors.Trace(ErrInvalidTimeFormat)
		}
	case 2:
		s := seps[0]
		fracStr = seps[1]

		if len(s) == 14 {
			// YYYYMMDDHHMMSS.fraction
			_, err = fmt.Sscanf(s, "%4d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
		} else if len(s) == 12 {
			// YYMMDDHHMMSS.fraction
			_, err = fmt.Sscanf(s, "%2d%2d%2d%2d%2d%2d", &year, &month, &day, &hour, &minute, &second)
			year = adjustYear(year)
		} else {
			return ZeroDatetime, errors.Trace(ErrInvalidTimeFormat)
		}
	case 3:
		// YYYY-MM-DD
		err = scanTimeArgs(seps, &year, &month, &day)
	case 6:
		// We don't have fractional seconds part.
		// YYYY-MM-DD HH-MM-SS
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute, &second)
	case 7:
		// We have fractional seconds part.
		// YYY-MM-DD HH-MM-SS.fraction
		err = scanTimeArgs(seps[0:len(seps)-1], &year, &month, &day, &hour, &minute, &second)
		fracStr = seps[len(seps)-1]
	default:
		return ZeroDatetime, errors.Trace(ErrInvalidTimeFormat)
	}

	if err != nil {
		return ZeroDatetime, errors.Trace(err)
	}

	// If str is sepereated by delimiters, the first one is year, and if the year is 2 digit,
	// we should adjust it.
	// TODO: ajust year is very complex, now we only consider the simplest way.
	if len(seps[0]) == 2 {
		year = adjustYear(year)
	}

	microsecond, overflow, err := parseFrac(fracStr, fsp)
	if err != nil {
		return ZeroDatetime, errors.Trace(err)
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
		return errors.Trace(ErrInvalidTimeFormat)
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
func AdjustYear(y int64) (int64, error) {
	y = int64(adjustYear(int(y)))
	if y < int64(MinYear) || y > int64(MaxYear) {
		return 0, errors.Trace(ErrInvalidYear)
	}

	return y, nil
}

// Duration is the type for MySQL time type.
type Duration struct {
	gotime.Duration
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
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
	sign, hours, minutes, seconds, fraction := splitDuration(gotime.Duration(d.Duration))
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
	dec.FromString([]byte(s))
	return dec
}

// ConvertToTime converts duration to Time.
// Tp is TypeDatetime, TypeTimestamp and TypeDate.
func (d Duration) ConvertToTime(tp uint8) (Time, error) {
	year, month, day := gotime.Now().Date()
	// just use current year, month and day.
	n := gotime.Date(year, month, day, 0, 0, 0, 0, gotime.Local)
	n = n.Add(d.Duration)

	t := Time{
		Time: FromGoTime(n),
		Type: mysql.TypeDatetime,
		Fsp:  d.Fsp,
	}

	return t.Convert(tp)
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 10:10:10.999999 round 0 -> 10:10:11
// and 10:10:10.000000 round 0 -> 10:10:10
func (d Duration) RoundFrac(fsp int) (Duration, error) {
	fsp, err := checkFsp(fsp)
	if err != nil {
		return d, errors.Trace(err)
	}

	if fsp == d.Fsp {
		return d, nil
	}

	n := gotime.Date(0, 0, 0, 0, 0, 0, 0, gotime.Local)
	nd := n.Add(d.Duration).Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond).Sub(n)
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
func (d Duration) CompareString(str string) (int, error) {
	// use MaxFsp to parse the string
	o, err := ParseDuration(str, MaxFsp)
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
func ParseDuration(str string, fsp int) (Duration, error) {
	var (
		day    int
		hour   int
		minute int
		second int
		frac   int

		err       error
		sign      = 0
		dayExists = false
	)

	fsp, err = checkFsp(fsp)
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

	var overflow bool
	if n := strings.IndexByte(str, '.'); n >= 0 {
		// It has fractional precesion parts.
		fracStr := str[n+1:]
		frac, overflow, err = parseFrac(fracStr, fsp)
		if err != nil {
			return ZeroDuration, errors.Trace(err)
		}
		str = str[0:n]
	}

	// It tries to split str with delimiter, time delimiter must be :
	seps := strings.Split(str, ":")

	switch len(seps) {
	case 1:
		if dayExists {
			hour, err = strconv.Atoi(seps[0])
		} else {
			// No delimiter.
			if len(str) == 6 {
				// HHMMSS
				_, err = fmt.Sscanf(str, "%2d%2d%2d", &hour, &minute, &second)
			} else if len(str) == 4 {
				// MMSS
				_, err = fmt.Sscanf(str, "%2d%2d", &minute, &second)
			} else if len(str) == 2 {
				// SS
				_, err = fmt.Sscanf(str, "%2d", &second)
			} else {
				// Maybe only contains date.
				_, err = ParseDate(str)
				if err == nil {
					return ZeroDuration, nil
				}
				return ZeroDuration, errors.Trace(ErrInvalidTimeFormat)
			}
		}
	case 2:
		// HH:MM
		_, err = fmt.Sscanf(str, "%2d:%2d", &hour, &minute)
	case 3:
		// Time format maybe HH:MM:SS or HHH:MM:SS.
		// See https://dev.mysql.com/doc/refman/5.7/en/time.html
		if !dayExists && len(seps[0]) == 3 {
			_, err = fmt.Sscanf(str, "%3d:%2d:%2d", &hour, &minute, &second)
		} else {
			_, err = fmt.Sscanf(str, "%2d:%2d:%2d", &hour, &minute, &second)
		}
	default:
		return ZeroDuration, errors.Trace(ErrInvalidTimeFormat)
	}

	if err != nil {
		return ZeroDuration, errors.Trace(err)
	}

	if overflow {
		second++
		frac = 0
	}
	d := gotime.Duration(day*24*3600+hour*3600+minute*60+second)*gotime.Second + gotime.Duration(frac)*gotime.Microsecond
	if sign == -1 {
		d = -d
	}

	if d > MaxTime {
		d = MaxTime
		err = ErrInvalidTimeFormat
	} else if d < MinTime {
		d = MinTime
		err = ErrInvalidTimeFormat
	}
	return Duration{Duration: d, Fsp: fsp}, errors.Trace(err)
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

func getTime(num int64, tp byte) (Time, error) {
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
	err := t.check()
	return t, errors.Trace(err)
}

// See number_to_datetime function.
// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
func parseDateTimeFromNum(num int64) (Time, error) {
	t := ZeroDate
	// Check zero.
	if num == 0 {
		return t, nil
	}

	// Check datetime type.
	if num >= 10000101000000 {
		return getTime(num, t.Type)
	}

	// Check MMDD.
	if num < 101 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Adjust year
	// YYMMDD, year: 2000-2069
	if num <= (70-1)*10000+1231 {
		num = (num + 20000000) * 1000000
		return getTime(num, t.Type)
	}

	// Check YYMMDD.
	if num < 70*10000+101 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Adjust year
	// YYMMDD, year: 1970-1999
	if num <= 991231 {
		num = (num + 19000000) * 1000000
		return getTime(num, t.Type)
	}

	// Check YYYYMMDD.
	if num < 10000101 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Adjust hour/min/second.
	if num <= 99991231 {
		num = num * 1000000
		return getTime(num, t.Type)
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Set TypeDatetime type.
	t.Type = mysql.TypeDatetime

	// Adjust year
	// YYMMDDHHMMSS, 2000-2069
	if num <= 69*10000000000+1231235959 {
		num = num + 20000000000000
		return getTime(num, t.Type)
	}

	// Check YYYYMMDDHHMMSS.
	if num < 70*10000000000+101000000 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Adjust year
	// YYMMDDHHMMSS, 1970-1999
	if num <= 991231235959 {
		num = num + 19000000000000
		return getTime(num, t.Type)
	}

	return getTime(num, t.Type)
}

// ParseTime parses a formatted string with type tp and specific fsp.
// Type is TypeDatetime, TypeTimestamp and TypeDate.
// Fsp is in range [0, 6].
// MySQL supports many valid datatime format, but still has some limitation.
// If delimiter exists, the date part and time part is separated by a space or T,
// other punctuation character can be used as the delimiter between date parts or time parts.
// If no delimiter, the format must be YYYYMMDDHHMMSS or YYMMDDHHMMSS
// If we have fractional seconds part, we must use decimal points as the delimiter.
// The valid datetime range is from '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
// The valid timestamp range is from '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
// The valid date range is from '1000-01-01' to '9999-12-31'
func ParseTime(str string, tp byte, fsp int) (Time, error) {
	fsp, err := checkFsp(fsp)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t, err := parseDatetime(str, fsp)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t.Type = tp
	if err = t.check(); err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	return t, nil
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(str string) (Time, error) {
	return ParseTime(str, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(str string) (Time, error) {
	return ParseTime(str, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(str string) (Time, error) {
	// date has no fractional seconds precision
	return ParseTime(str, mysql.TypeDate, MinFsp)
}

// ParseTimeFromNum parses a formatted int64,
// returns the value which type is tp.
func ParseTimeFromNum(num int64, tp byte, fsp int) (Time, error) {
	fsp, err := checkFsp(fsp)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t, err := parseDateTimeFromNum(num)
	if err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}

	t.Type = tp
	t.Fsp = fsp
	if err := t.check(); err != nil {
		return Time{Time: ZeroTime, Type: tp}, errors.Trace(err)
	}
	return t, nil
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(num int64) (Time, error) {
	return ParseTimeFromNum(num, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(num int64) (Time, error) {
	return ParseTimeFromNum(num, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(num int64) (Time, error) {
	// date has no fractional seconds precision
	return ParseTimeFromNum(num, mysql.TypeDate, MinFsp)
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

func checkDateType(t TimeInternal) error {
	year, month, day := t.Year(), t.Month(), t.Day()
	if year == 0 && month == 0 && day == 0 {
		return nil
	}

	if err := checkDateRange(t); err != nil {
		return errors.Trace(err)
	}

	if err := checkMonthDay(year, month, day); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDateRange(t TimeInternal) error {
	// Oddly enough, MySQL document says date range should larger than '1000-01-01',
	// but we can insert '0001-01-01' actually.
	if t.Year() < 0 || t.Month() < 0 || t.Day() < 0 {
		return ErrInvalidTimeFormat
	}
	if compareTime(t, maxDatetime) > 0 {
		return ErrInvalidTimeFormat
	}
	return nil
}

func checkMonthDay(year, month, day int) error {
	if month < 0 || month > 12 {
		return ErrInvalidTimeFormat
	}

	maxDay := 31
	if month > 0 {
		maxDay = maxDaysInMonth[month-1]
	}
	if month == 2 && year%4 != 0 {
		maxDay = 28
	}

	if day < 0 || day > maxDay {
		return ErrInvalidTimeFormat
	}
	return nil
}

func checkTimestampType(t TimeInternal) error {
	if compareTime(t, ZeroTime) == 0 {
		return nil
	}

	// TODO: Consider time_zone variable.
	t1, err := t.GoTime(gotime.Local)
	if err != nil {
		log.Infof("checkTimestampType failed, t=%v", t)
		return errors.Trace(err)
	}

	if t1.After(maxTimestamp) || t1.Before(minTimestamp) {
		return ErrInvalidTimeFormat
	}
	return nil
}

func checkDatetimeType(t TimeInternal) error {
	if err := checkDateType(t); err != nil {
		return errors.Trace(err)
	}

	hour, minute, second := t.Hour(), t.Minute(), t.Second()
	if hour < 0 || hour >= 24 {
		return ErrInvalidTimeFormat
	}
	if minute < 0 || minute >= 60 {
		return ErrInvalidTimeFormat
	}
	if second < 0 || second >= 60 {
		return ErrInvalidTimeFormat
	}

	return nil
}

// ExtractTimeNum extracts time value number from time unit and format.
func ExtractTimeNum(unit string, t Time) (int64, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return int64(t.Time.Microsecond()), nil
	case "SECOND":
		return int64(t.Time.Second()), nil
	case "MINUTE":
		return int64(t.Time.Minute()), nil
	case "HOUR":
		return int64(t.Time.Hour()), nil
	case "DAY":
		return int64(t.Time.Day()), nil
	case "WEEK":
		week := t.Time.Week(0)
		return int64(week), nil
	case "MONTH":
		// TODO: Consider time_zone variable.
		t1, err := t.Time.GoTime(gotime.Local)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return int64(t1.Month()), nil
	case "QUARTER":
		m := int64(t.Time.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return int64(t.Time.Year()), nil
	case "SECOND_MICROSECOND":
		return int64(t.Time.Second())*1000000 + int64(t.Time.Microsecond()), nil
	case "MINUTE_MICROSECOND":
		_, m, s := t.Clock()
		return int64(m)*100000000 + int64(s)*1000000 + int64(t.Time.Microsecond()), nil
	case "MINUTE_SECOND":
		_, m, s := t.Clock()
		return int64(m*100 + s), nil
	case "HOUR_MICROSECOND":
		h, m, s := t.Clock()
		return int64(h)*10000000000 + int64(m)*100000000 + int64(s)*1000000 + int64(t.Time.Microsecond()), nil
	case "HOUR_SECOND":
		h, m, s := t.Clock()
		return int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "HOUR_MINUTE":
		h, m, _ := t.Clock()
		return int64(h)*100 + int64(m), nil
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

func extractSingleTimeValue(unit string, format string) (int64, int64, int64, gotime.Duration, error) {
	iv, err := strconv.ParseInt(format, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	v := gotime.Duration(iv)
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return 0, 0, 0, v * gotime.Microsecond, nil
	case "SECOND":
		return 0, 0, 0, v * gotime.Second, nil
	case "MINUTE":
		return 0, 0, 0, v * gotime.Minute, nil
	case "HOUR":
		return 0, 0, 0, v * gotime.Hour, nil
	case "DAY":
		return 0, 0, iv, 0, nil
	case "WEEK":
		return 0, 0, 7 * iv, 0, nil
	case "MONTH":
		return 0, iv, 0, 0, nil
	case "QUARTER":
		return 0, 3 * iv, 0, 0, nil
	case "YEAR":
		return iv, 0, 0, 0, nil
	}

	return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// Format is `SS.FFFFFF`.
func extractSecondMicrosecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ".")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	microseconds, err := strconv.ParseInt(alignFrac(fields[1], MaxFsp), 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, gotime.Duration(seconds)*gotime.Second + gotime.Duration(microseconds)*gotime.Microsecond, nil
}

// Format is `MM:SS.FFFFFF`.
func extractMinuteMicrosecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractSecondMicrosecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}

	return 0, 0, 0, gotime.Duration(minutes)*gotime.Minute + value, nil
}

// Format is `MM:SS`.
func extractMinuteSecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, gotime.Duration(minutes)*gotime.Minute + gotime.Duration(seconds)*gotime.Second, nil
}

// Format is `HH:MM:SS.FFFFFF`.
func extractHourMicrosecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 3 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractSecondMicrosecond(fields[2])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}

	return 0, 0, 0, gotime.Duration(hours)*gotime.Hour + gotime.Duration(minutes)*gotime.Minute + value, nil
}

// Format is `HH:MM:SS`.
func extractHourSecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 3 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, gotime.Duration(hours)*gotime.Hour + gotime.Duration(minutes)*gotime.Minute + gotime.Duration(seconds)*gotime.Second, nil
}

// Format is `HH:MM`.
func extractHourMinute(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, gotime.Duration(hours)*gotime.Hour + gotime.Duration(minutes)*gotime.Minute, nil
}

// Format is `DD HH:MM:SS.FFFFFF`.
func extractDayMicrosecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourMicrosecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH:MM:SS`.
func extractDaySecond(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourSecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH:MM`.
func extractDayMinute(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourMinute(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH`.
func extractDayHour(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, gotime.Duration(hours) * gotime.Hour, nil
}

// Format is `YYYY-MM`.
func extractYearMonth(format string) (int64, int64, int64, gotime.Duration, error) {
	fields := strings.Split(format, "-")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	years, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	months, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return years, months, 0, 0, nil
}

// ExtractTimeValue extracts time value from time unit and format.
func ExtractTimeValue(unit string, format string) (int64, int64, int64, gotime.Duration, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		return extractSingleTimeValue(unit, format)
	case "SECOND_MICROSECOND":
		return extractSecondMicrosecond(format)
	case "MINUTE_MICROSECOND":
		return extractMinuteMicrosecond(format)
	case "MINUTE_SECOND":
		return extractMinuteSecond(format)
	case "HOUR_MICROSECOND":
		return extractHourMicrosecond(format)
	case "HOUR_SECOND":
		return extractHourSecond(format)
	case "HOUR_MINUTE":
		return extractHourMinute(format)
	case "DAY_MICROSECOND":
		return extractDayMicrosecond(format)
	case "DAY_SECOND":
		return extractDaySecond(format)
	case "DAY_MINUTE":
		return extractDayMinute(format)
	case "DAY_HOUR":
		return extractDayHour(format)
	case "YEAR_MONTH":
		return extractYearMonth(format)
	default:
		return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
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
	seps := parseDateFormat(format)
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
func ParseTimeFromInt64(num int64) (Time, error) {
	return parseDateTimeFromNum(num)
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
			return errors.Trace(ErrInvalidTimeFormat)
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Time.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrInvalidTimeFormat)
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		fmt.Fprintf(buf, "%02d", t.Time.Month())
	case 'c':
		fmt.Fprintf(buf, "%d", t.Time.Month())
	case 'D':
		fmt.Fprintf(buf, "%d%s", t.Time.Day(), abbrDayOfMonth(t.Time.Day()))
	case 'd':
		fmt.Fprintf(buf, "%02d", t.Time.Day())
	case 'e':
		fmt.Fprintf(buf, "%d", t.Time.Day())
	case 'j':
		fmt.Fprintf(buf, "%03d", t.Time.YearDay())
	case 'H':
		fmt.Fprintf(buf, "%02d", t.Time.Hour())
	case 'k':
		fmt.Fprintf(buf, "%d", t.Time.Hour())
	case 'h', 'I':
		t := t.Time.Hour()
		if t == 0 || t == 12 {
			fmt.Fprintf(buf, "%02d", 12)
		} else {
			fmt.Fprintf(buf, "%02d", t%12)
		}
	case 'l':
		t := t.Time.Hour()
		if t == 0 || t == 12 {
			fmt.Fprintf(buf, "%d", 12)
		} else {
			fmt.Fprintf(buf, "%d", t%12)
		}
	case 'i':
		fmt.Fprintf(buf, "%02d", t.Time.Minute())
	case 'p':
		if t.Time.Hour() < 12 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Time.Hour()
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
		fmt.Fprintf(buf, "%02d", t.Time.Second())
	case 'f':
		fmt.Fprintf(buf, "%06d", t.Time.Microsecond())
	case 'U':
		w := t.Time.Week(0)
		fmt.Fprintf(buf, "%02d", w)
	case 'u':
		w := t.Time.Week(1)
		fmt.Fprintf(buf, "%02d", w)
	case 'V':
		w := t.Time.Week(2)
		fmt.Fprintf(buf, "%02d", w)
	case 'v':
		_, w := t.Time.YearWeek(3)
		fmt.Fprintf(buf, "%02d", w)
	case 'a':
		weekday := t.Time.Weekday()
		buf.WriteString(abbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.Time.Weekday().String())
	case 'w':
		fmt.Fprintf(buf, "%d", t.Time.Weekday())
	case 'X':
		year, _ := t.Time.YearWeek(2)
		if year < 0 {
			fmt.Fprintf(buf, "%v", math.MaxUint32)
		} else {
			fmt.Fprintf(buf, "%04d", year)
		}
	case 'x':
		year, _ := t.Time.YearWeek(3)
		if year < 0 {
			fmt.Fprintf(buf, "%v", math.MaxUint32)
		} else {
			fmt.Fprintf(buf, "%04d", year)
		}
	case 'Y':
		fmt.Fprintf(buf, "%04d", t.Time.Year())
	case 'y':
		str := fmt.Sprintf("%04d", t.Time.Year())
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}

	return nil
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
func (t *Time) StrToDate(date, format string) bool {
	ctx := make(map[string]int)
	var tm mysqlTime
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
	if t.check() != nil {
		return false
	}
	return true
}

// mysqlTimeFix fixes the mysqlTime use the values in the context.
func mysqlTimeFix(t *mysqlTime, ctx map[string]int) error {
	// Key of the ctx is the format char, such as `%j` `%p` and so on.
	if yearOfDay, ok := ctx["%j"]; ok {
		// TODO: Implement the function that converts day of year to yy:mm:dd.
		_ = yearOfDay
	}
	if valueAMorPm, ok := ctx["%p"]; ok {
		if t.hour == 0 {
			return ErrInvalidTimeFormat
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
func strToDate(t *mysqlTime, date string, format string, ctx map[string]int) bool {
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

type dateFormatParser func(t *mysqlTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,           // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,               // Month, numeric (0..12)
	"%d": dayOfMonthNumericTwoDigits, // Day of the month, numeric (00..31)
	"%e": dayOfMonthNumeric,          // Day of the month, numeric (0..31)
	"%f": microSeconds,               // Microseconds (000000..999999)
	"%h": hour24TwoDigits,            // Hour (01..12)
	"%H": hour24TwoDigits,            // Hour (01..12)
	"%I": hour24TwoDigits,            // Hour (01..12)
	"%i": minutesNumeric,             // Minutes, numeric (00..59)
	"%j": dayOfYearThreeDigits,       // Day of year (001..366)
	"%k": hour24Numeric,              // Hour (0..23)
	"%l": hour12Numeric,              // Hour (1..12)
	"%M": fullNameMonth,              // Month name (January..December)
	"%m": monthNumericTwoDigits,      // Month, numeric (00..12)
	"%p": isAMOrPM,                   // AM or PM
	"%r": time12Hour,                 // Time, 12-hour (hh:mm:ss followed by AM or PM)
	"%s": secondsNumeric,             // Seconds (00..59)
	"%S": secondsNumeric,             // Seconds (00..59)
	"%T": time24Hour,                 // Time, 24-hour (hh:mm:ss)
	"%Y": yearNumericFourDigits,      // Year, numeric, four digits
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
	// Deprecated since MySQL 5.7.5
	// "%y": yearTwoDigits,         // Year, numeric (two digits)
}

func matchDateWithToken(t *mysqlTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date, ctx)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

func parseDigits(input string, count int) (int, bool) {
	if len(input) < count {
		return 0, false
	}

	v, err := strconv.ParseUint(input[:count], 10, 64)
	if err != nil {
		return int(v), false
	}
	return int(v), true
}

func hour24TwoDigits(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v >= 24 {
		return input, false
	}
	t.hour = uint8(v)
	return input[2:], true
}

func secondsNumeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v >= 60 {
		return input, false
	}
	t.second = uint8(v)
	return input[2:], true
}

func minutesNumeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v >= 60 {
		return input, false
	}
	t.minute = uint8(v)
	return input[2:], true
}

const time12HourLen = len("hh:mm:ssAM")

func time12Hour(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
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
		t.hour = uint8(hour)
	case strings.HasPrefix(remain, "PM"):
		t.hour = uint8(hour + 12)
	default:
		return input, false
	}

	t.minute = uint8(minute)
	t.second = uint8(second)
	return remain, true
}

const time24HourLen = len("hh:mm:ss")

func time24Hour(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
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

	t.hour = uint8(hour)
	t.minute = uint8(minute)
	t.second = uint8(second)
	return input[8:], true
}

const (
	constForAM = 1 + iota
	constForPM
)

func isAMOrPM(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	if strings.HasPrefix(input, "AM") {
		ctx["%p"] = constForAM
	} else if strings.HasPrefix(input, "PM") {
		ctx["%p"] = constForPM
	} else {
		return input, false
	}
	return input[2:], true
}

func dayOfMonthNumericTwoDigits(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v >= 32 {
		return input, false
	}
	t.day = uint8(v)
	return input[2:], true
}

var twoDigitRegex = regexp.MustCompile("^[1-9][0-9]?")

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

func dayOfMonthNumeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, remain := parseTwoNumeric(input) // 0..31
	if len(remain) == len(input) || v > 31 {
		return input, false
	}
	t.month = uint8(v)
	return remain, true
}

func hour24Numeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	// 0..23
	v, remain := parseTwoNumeric(input)
	if len(remain) == len(input) || v > 23 {
		return input, false
	}
	t.hour = uint8(v)
	return remain, true
}

func hour12Numeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	// 1..12
	v, remain := parseTwoNumeric(input)
	if len(remain) == len(input) || v > 12 || v == 0 {
		return input, false
	}
	t.hour = uint8(v)
	return remain, true
}

func microSeconds(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) < 6 {
		return input, false
	}
	v, err := strconv.ParseUint(input[:6], 10, 64)
	if err != nil {
		return input, false
	}
	t.microsecond = uint32(v)
	return input[6:], true
}

func yearNumericFourDigits(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 4)
	if !succ {
		return input, false
	}
	t.year = uint16(v)
	return input[4:], true
}

func dayOfYearThreeDigits(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 3)
	if !succ || v == 0 || v > 366 {
		return input, false
	}
	ctx["%j"] = v
	return input[3:], true
}

func monthNumericTwoDigits(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, succ := parseDigits(input, 2)
	if !succ || v > 12 {
		return input, false
	}

	t.month = uint8(v)
	return input[2:], true
}

func abbreviatedWeekday(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) >= 3 {
		dayName := input[:3]
		if _, ok := weekdayAbbrev[dayName]; ok {
			// TODO: We need refact mysql time to support this.
			return input, false
		}
	}
	return input, false
}

func abbreviatedMonth(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	if len(input) >= 3 {
		monthName := input[:3]
		if month, ok := monthAbbrev[monthName]; ok {
			t.month = uint8(month)
			return input[len(monthName):], true
		}
	}
	return input, false
}

func fullNameMonth(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	for i, month := range MonthNames {
		if strings.HasPrefix(input, month) {
			t.month = uint8(i + 1)
			return input[len(month):], true
		}
	}
	return input, false
}

func monthNumeric(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
	v, rem := parseTwoNumeric(input)
	if len(rem) == len(input) || v > 12 {
		return rem, false
	}
	t.month = uint8(v)
	return rem, false
}

// 0th 1st 2nd 3rd ...
func dayOfMonthWithSuffix(t *mysqlTime, input string, ctx map[string]int) (string, bool) {
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
