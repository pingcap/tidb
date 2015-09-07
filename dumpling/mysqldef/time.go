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

package mysqldef

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/juju/errors"
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
	MinTime = -time.Duration(838*3600+59*60+59) * time.Second
	// MaxTime is the maximum for mysql time type.
	MaxTime = time.Duration(838*3600+59*60+59) * time.Second

	zeroDatetimeStr  = "0000-00-00 00:00:00"
	zeroTimestampStr = "0000-00-00 00:00:00"
	zeroDateStr      = "0000-00-00"
)

// Zero values for different types.
var (
	// ZeroDuration is the zero value for Duration type.
	ZeroDuration = Duration{Duration: time.Duration(0), Fsp: DefaultFsp}

	// ZeroTime is the zero value for time.Time type.
	ZeroTime = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)

	// ZeroDatetime is the zero value for datetime Time.
	ZeroDatetime = Time{
		Time: ZeroTime,
		Type: TypeDatetime,
		Fsp:  DefaultFsp,
	}

	// ZeroTimestamp is the zero value for timestamp Time.
	ZeroTimestamp = Time{
		Time: ZeroTime,
		Type: TypeTimestamp,
		Fsp:  DefaultFsp,
	}

	// ZeroDate is the zero value for date Time.
	ZeroDate = Time{
		Time: ZeroTime,
		Type: TypeDate,
		Fsp:  DefaultFsp,
	}
)

var (
	// MinDatetime is the minimum for mysql datetime type.
	MinDatetime = time.Date(1000, 1, 1, 0, 0, 0, 0, time.Local)
	// MaxDatetime is the maximum for mysql datetime type.
	MaxDatetime = time.Date(9999, 12, 31, 23, 59, 59, 999999, time.Local)

	// MinTimestamp is the minimum for mysql timestamp type.
	MinTimestamp = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
	// MaxTimestamp is the maximum for mysql timestamp type.
	MaxTimestamp = time.Date(2038, 1, 19, 3, 14, 7, 999999, time.UTC)
)

// Time is the struct for handling datetime, timestamp and date.
// TODO: check if need a NewTime function to set Fsp default value?
type Time struct {
	time.Time
	Type uint8
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

func (t Time) String() string {
	if t.IsZero() {
		if t.Type == TypeDate {
			return zeroDateStr
		}

		return zeroDatetimeStr
	}

	if t.Type == TypeDate {
		return t.Time.Format(DateFormat)
	}

	tfStr := TimeFormat
	if t.Fsp > 0 {
		tfStr = fmt.Sprintf("%s.%s", tfStr, strings.Repeat("0", t.Fsp))
	}

	return t.Time.Format(tfStr)
}

// IsZero returns a boolean indicating whether the time is equal to ZeroTime.
func (t Time) IsZero() bool {
	return t.Time.Equal(ZeroTime)
}

// Marshal returns the binary encoding of time.
func (t Time) Marshal() ([]byte, error) {
	var (
		b   []byte
		err error
	)

	switch t.Type {
	case TypeDatetime, TypeDate:
		// We must use t's Zone not current Now Zone,
		// For EDT/EST, even we create the time with time.Local location,
		// we may still have a different zone with current Now time.
		_, offset := t.Zone()
		// For datetime and date type, we have a trick to marshal.
		// e.g, if local time is 2010-10-10T10:10:10 UTC+8
		// we will change this to 2010-10-10T10:10:10 UTC and then marshal.
		b, err = t.Time.Add(time.Duration(offset) * time.Second).UTC().MarshalBinary()
	case TypeTimestamp:
		b, err = t.Time.UTC().MarshalBinary()
	default:
		err = errors.Errorf("invalid time type %d", t.Type)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// append fsp to the end
	b = append(b, byte(t.Fsp))

	return b, nil
}

// Unmarshal decodes the binary data into Time with current local time.
func (t *Time) Unmarshal(b []byte) error {
	return t.UnmarshalInLocation(b, time.Local)
}

// UnmarshalInLocation decodes the binary data
// into Time with a specific time Location.
func (t *Time) UnmarshalInLocation(b []byte, loc *time.Location) error {
	if len(b) < 1 {
		return errors.Errorf("insufficient bytes to unmarshal time")
	}

	// Get fsp
	fsp, err := checkFsp(int(int8(b[len(b)-1])))
	if err != nil {
		return errors.Trace(err)
	}

	t.Fsp = fsp

	b = b[:len(b)-1]

	if err := t.Time.UnmarshalBinary(b); err != nil {
		return errors.Trace(err)
	}

	if t.IsZero() {
		return nil
	}

	if t.Type == TypeDatetime || t.Type == TypeDate {
		// e.g, for 2010-10-10T10:10:10 UTC, we will unmarshal to 2010-10-10T10:10:10 location
		// We must use Date to creates a time to get offset. Why not time.Now directly,
		// because creating time using Date with time.Local may still have a different zone with time.Now.
		year, month, day := t.Time.Date()
		hour, min, sec := t.Time.Clock()
		nsec := t.Time.Nanosecond()
		_, offset := time.Date(year, month, day, hour, min, sec, nsec, time.Local).Zone()

		t.Time = t.Time.Add(-time.Duration(offset) * time.Second).In(loc)
		if t.Type == TypeDate {
			// for date type ,we will only use year, month and day.
			year, month, day = t.Time.Date()
			t.Time = time.Date(year, month, day, 0, 0, 0, 0, loc)
		}
	} else if t.Type == TypeTimestamp {
		t.Time = t.Time.In(loc)
	} else {
		return errors.Errorf("invalid time type %d", t.Type)
	}

	return nil
}

const numberFormat = "20060102150405"

// ToNumber returns a formatted number.
// e.g,
// 2012-12-12T10:10:10 -> 20121212101010
// 2012-12-12T10:10:10.123456 -> 20121212101010.123456
func (t Time) ToNumber() Decimal {
	if t.IsZero() {
		return ZeroDecimal
	}

	tfStr := numberFormat
	if t.Fsp > 0 {
		tfStr = fmt.Sprintf("%s.%s", tfStr, strings.Repeat("0", t.Fsp))
	}

	s := t.Time.Format(tfStr)
	// We skip checking error here because time formatted string can be parsed certainly.
	d, _ := ParseDecimal(s)
	return d
}

// Convert converts t with type tp.
func (t Time) Convert(tp uint8) (Time, error) {
	if t.Type == tp || t.IsZero() {
		return Time{Time: t.Time, Type: tp, Fsp: t.Fsp}, nil
	}

	switch tp {
	case TypeDatetime:
		return Time{Time: t.Time, Type: TypeDatetime, Fsp: t.Fsp}, nil
	case TypeTimestamp:
		nt := Time{Time: t.Time, Type: TypeTimestamp, Fsp: t.Fsp}
		if !checkTimestamp(nt) {
			return ZeroTimestamp, errors.Trace(ErrInvalidTimeFormat)
		}
		return nt, nil
	case TypeDate:
		year, month, day := t.Time.Date()
		return Time{Time: time.Date(year, month, day, 0, 0, 0, 0, time.Local),
			Type: TypeDate, Fsp: 0}, nil
	default:
		return Time{Time: ZeroTime, Type: tp}, errors.Errorf("invalid time type %d", tp)
	}
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
	frac := t.Nanosecond()

	d := time.Duration(hour*3600+minute*60+second)*time.Second + time.Duration(frac)

	// TODO: check convert validation
	return Duration{Duration: time.Duration(d), Fsp: t.Fsp}, nil
}

// Compare returns an integer comparing the time instant t to o.
// If t is after o, return 1, equal o, return 0, before o, return -1.
func (t Time) Compare(o Time) int {
	if t.Time.After(o.Time) {
		return 1
	} else if t.Time.Equal(o.Time) {
		return 0
	} else {
		return -1
	}
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

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:11
// and 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func (t Time) RoundFrac(fsp int) (Time, error) {
	if t.Type == TypeDate {
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

	nt := t.Time.Round(time.Duration(math.Pow10(9-fsp)) * time.Nanosecond)
	return Time{Time: nt, Type: t.Type, Fsp: fsp}, nil
}

func parseDatetime(str string, fsp int) (Time, error) {
	// Try to split str with delimiter.
	// TODO: only punctuation can be the delimiter for date parts or time parts.
	// But only space and T can be the delimiter between the date and time part.
	seps := strings.FieldsFunc(str, func(c rune) bool {
		return !unicode.IsNumber(c)
	})

	var (
		year    int
		month   int
		day     int
		hour    int
		minute  int
		second  int
		frac    int
		fracStr string

		err error
	)
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

	frac, err = parseFrac(fracStr, fsp)
	if err != nil {
		return ZeroDatetime, errors.Trace(err)
	}

	t, err := newTime(year, month, day, hour, minute, second, frac)
	if err != nil {
		return ZeroDatetime, errors.Trace(err)
	}

	nt := Time{
		Time: t,
		Type: TypeDatetime,
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

func newTime(year int, month int, day int, hour int, minute int, second int, frac int) (time.Time, error) {
	if year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 {
		// Should we check fractional fractional here?
		// But go time.Time can not support zero time 0000-00-00 00:00:00.
		return ZeroTime, nil
	}

	if err := checkTime(year, month, day, hour, minute, second, frac); err != nil {
		return ZeroTime, errors.Trace(err)
	}

	return time.Date(year, time.Month(month), day, hour, minute, second, frac*1000, time.Local), nil
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
func AdjustYear(y int) (int, error) {
	y = adjustYear(y)
	if y < int(MinYear) || y > int(MaxYear) {
		return 0, errors.Trace(ErrInvalidYear)
	}

	return y, nil
}

// Duration is the type for MySQL time type.
type Duration struct {
	time.Duration
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
	format := fmt.Sprintf("%%0%dd", d.Fsp)
	s := fmt.Sprintf(format, frac)
	return s[0:d.Fsp]
}

// ToNumber changes duration to number format.
// e.g,
// 10:10:10 -> 101010
func (d Duration) ToNumber() Decimal {
	sign, hours, minutes, seconds, fraction := splitDuration(time.Duration(d.Duration))
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
	v, _ := ParseDecimal(s)
	return v
}

// ConvertToTime converts duration to Time.
// Tp is TypeDatetime, TypeTimestamp and TypeDate.
func (d Duration) ConvertToTime(tp uint8) (Time, error) {
	year, month, day := time.Now().Date()
	// just use current year, month and day.
	n := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	n = n.Add(d.Duration)

	t := Time{
		Time: n,
		Type: TypeDatetime,
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

	n := ZeroTime
	nd := n.Add(d.Duration).Round(time.Duration(math.Pow10(9-fsp)) * time.Nanosecond).Sub(n)
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
// See: http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
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

	if n := strings.IndexByte(str, '.'); n >= 0 {
		// It has fractional precesion parts.
		fracStr := str[n+1:]
		frac, err = parseFrac(fracStr, fsp)
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
		// See: https://dev.mysql.com/doc/refman/5.7/en/time.html
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

	d := time.Duration(day*24*3600+hour*3600+minute*60+second)*time.Second + time.Duration(frac)*time.Microsecond
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

func splitDuration(t time.Duration) (int, int, int, int, int) {
	sign := 1
	if t < 0 {
		t = -t
		sign = -1
	}

	hours := t / time.Hour
	t -= hours * time.Hour
	minutes := t / time.Minute
	t -= minutes * time.Minute
	seconds := t / time.Second
	t -= seconds * time.Second
	fraction := t / time.Microsecond

	return sign, int(hours), int(minutes), int(seconds), int(fraction)
}

func checkTime(year int, month int, day int, hour int, minute int, second int, frac int) error {
	// Notes: for datetime type, `insert t values("0001-01-01 00:00:00");` is valid
	// so here only check year from 0~9999.
	if (year < 0 || year > 9999) ||
		(month <= 0 || month > 12) ||
		(day <= 0 || day > 31) ||
		(hour < 0 || hour >= 24) ||
		(minute < 0 || minute >= 60) ||
		(second < 0 || second >= 60) ||
		(frac < 0) {
		return errors.Trace(ErrInvalidTimeFormat)
	}

	return nil
}

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

	if err := checkTime(year, month, day, hour, minute, second, 0); err != nil {
		return Time{
			Time: ZeroTime,
			Type: tp,
			Fsp:  DefaultFsp,
		}, err
	}

	t, err := newTime(year, month, day, hour, minute, second, 0)
	return Time{
		Time: t,
		Type: tp,
		Fsp:  DefaultFsp,
	}, errors.Trace(err)
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
	if num < 991231 {
		num = (num + 19000000) * 1000000
		return getTime(num, t.Type)
	}

	// Check YYYYMMDD.
	if num < 10000101 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Adjust hour/min/second.
	if num < 99991231 {
		num = num * 1000000
		return getTime(num, t.Type)
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return t, errors.Trace(ErrInvalidTimeFormat)
	}

	// Set TypeDatetime type.
	t.Type = TypeDatetime

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
// If delimiter exists, the date part and time part is seperated by a space or T,
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

	return t.Convert(tp)
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(str string) (Time, error) {
	return ParseTime(str, TypeDatetime, DefaultFsp)
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(str string) (Time, error) {
	return ParseTime(str, TypeTimestamp, DefaultFsp)
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(str string) (Time, error) {
	// date has no fractional seconds precision
	return ParseTime(str, TypeDate, MinFsp)
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

	if !checkDatetime(t) {
		return Time{Time: ZeroTime, Type: tp}, ErrInvalidTimeFormat
	}

	t.Fsp = fsp
	return t.Convert(tp)
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(num int64) (Time, error) {
	return ParseTimeFromNum(num, TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(num int64) (Time, error) {
	return ParseTimeFromNum(num, TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(num int64) (Time, error) {
	// date has no fractional seconds precision
	return ParseTimeFromNum(num, TypeDate, MinFsp)
}

func checkDatetime(t Time) bool {
	if t.IsZero() {
		return true
	}

	if t.Time.After(MaxDatetime) || t.Time.Before(MinDatetime) {
		return false
	}

	return true
}

func checkTimestamp(t Time) bool {
	if t.IsZero() {
		return true
	}

	if t.Time.After(MaxTimestamp) || t.Time.Before(MinTimestamp) {
		return false
	}

	return true
}
