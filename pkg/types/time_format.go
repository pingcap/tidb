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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// roundTime rounds the time value according to digits count specified by fsp.
func roundTime(t gotime.Time, fsp int) gotime.Time {
	d := gotime.Duration(math.Pow10(9 - fsp))
	return t.Round(d)
}

// RoundFrac rounds the fraction part of a time-type value according to `fsp`.
func (t Time) RoundFrac(ctx Context, fsp int) (Time, error) {
	if t.Type() == mysql.TypeDate || t.IsZero() {
		// date type has no fsp
		return t, nil
	}

	fsp, err := CheckFsp(fsp)
	if err != nil {
		return t, errors.Trace(err)
	}

	if fsp == t.Fsp() {
		// have same fsp
		return t, nil
	}

	var nt CoreTime
	if t1, err := t.GoTime(ctx.Location()); err == nil {
		t1 = roundTime(t1, fsp)
		nt = FromGoTime(t1)
	} else {
		// Take the hh:mm:ss part out to avoid handle month or day = 0.
		hour, minute, second, microsecond := t.Hour(), t.Minute(), t.Second(), t.Microsecond()
		t1 := gotime.Date(1, 1, 1, hour, minute, second, microsecond*1000, ctx.Location())
		t2 := roundTime(t1, fsp)
		hour, minute, second = t2.Clock()
		microsecond = t2.Nanosecond() / 1000

		// TODO: when hh:mm:ss overflow one day after rounding, it should be add to yy:mm:dd part,
		// but mm:dd may contain 0, it makes the code complex, so we ignore it here.
		if t2.Day()-1 > 0 {
			return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t.String()))
		}
		var ok bool
		nt, ok = FromDateChecked(t.Year(), t.Month(), t.Day(), hour, minute, second, microsecond)
		if !ok {
			return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t.String()))
		}
	}

	return NewTime(nt, t.Type(), fsp), nil
}

// MarshalJSON implements Marshaler.MarshalJSON interface.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.coreTime)
}

// UnmarshalJSON implements Unmarshaler.UnmarshalJSON interface.
func (t *Time) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &t.coreTime)
}

// GetFsp gets the fsp of a string.
func GetFsp(s string) int {
	index := GetFracIndex(s)
	var fsp int
	if index < 0 {
		fsp = 0
	} else {
		fsp = len(s) - index - 1
	}
	if fsp > 6 {
		fsp = 6
	}
	return fsp
}

// GetFracIndex finds the last '.' for get fracStr, index = -1 means fracStr not found.
// but for format like '2019.01.01 00:00:00', the index should be -1.
//
// It will not be affected by the time zone suffix.
// For format like '2020-01-01 12:00:00.123456+05:00' and `2020-01-01 12:00:00.123456-05:00`, the index should be 19.
// related issue https://github.com/pingcap/tidb/issues/35291 and https://github.com/pingcap/tidb/issues/49555
func GetFracIndex(s string) (index int) {
	tzIndex, _, _, _, _ := GetTimezone(s)
	var end int
	if tzIndex != -1 {
		end = tzIndex - 1
	} else {
		end = len(s) - 1
	}
	index = -1
	for i := end; i >= 0; i-- {
		if s[i] != '+' && s[i] != '-' && isPunctuation(s[i]) {
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
func RoundFrac(t gotime.Time, fsp int) (gotime.Time, error) {
	_, err := CheckFsp(fsp)
	if err != nil {
		return t, errors.Trace(err)
	}
	return t.Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond), nil //nolint:durationcheck
}

// TruncateFrac truncates fractional seconds precision with new fsp and returns a new one.
// 2011:11:11 10:10:10.888888 round 0 -> 2011:11:11 10:10:10
// 2011:11:11 10:10:10.111111 round 0 -> 2011:11:11 10:10:10
func TruncateFrac(t gotime.Time, fsp int) (gotime.Time, error) {
	if _, err := CheckFsp(fsp); err != nil {
		return t, err
	}
	return t.Truncate(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond), nil //nolint:durationcheck
}

// ToPackedUint encodes Time to a packed uint64 value.
//
//	 1 bit  0
//	17 bits year*13+month   (year 0-9999, month 0-12)
//	 5 bits day             (0-31)
//	 5 bits hour            (0-23)
//	 6 bits minute          (0-59)
//	 6 bits second          (0-59)
//	24 bits microseconds    (0-999999)
//
//	Total: 64 bits = 8 bytes
//
//	0YYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
func (t Time) ToPackedUint() (uint64, error) {
	tm := t
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
		t.SetCoreTime(ZeroCoreTime)
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

	t.SetCoreTime(FromDate(year, month, day, hour, minute, second, microsec))

	return nil
}

// Check function checks whether t matches valid Time format.
// If allowZeroInDate is false, it returns ErrZeroDate when month or day is zero.
// FIXME: See https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_no_zero_in_date
func (t Time) Check(ctx Context) error {
	allowZeroInDate := ctx.Flags().IgnoreZeroInDate()
	allowInvalidDate := ctx.Flags().IgnoreInvalidDateErr()
	var err error
	switch t.Type() {
	case mysql.TypeTimestamp:
		err = checkTimestampType(t.coreTime, ctx.Location())
	case mysql.TypeDatetime, mysql.TypeDate:
		err = checkDatetimeType(t.coreTime, allowZeroInDate, allowInvalidDate)
	}
	return errors.Trace(err)
}

// Sub subtracts t1 from t, returns a duration value.
// Note that sub should not be done on different time types.
func (t *Time) Sub(ctx Context, t1 *Time) Duration {
	var duration gotime.Duration
	if t.Type() == mysql.TypeTimestamp && t1.Type() == mysql.TypeTimestamp {
		a, err := t.GoTime(ctx.Location())
		terror.Log(errors.Trace(err))
		b, err := t1.GoTime(ctx.Location())
		terror.Log(errors.Trace(err))
		duration = a.Sub(b)
	} else {
		seconds, microseconds, neg := calcTimeTimeDiff(t.coreTime, t1.coreTime, 1)
		duration = gotime.Duration(seconds*1e9 + microseconds*1e3)
		if neg {
			duration = -duration
		}
	}

	fsp := t.Fsp()
	fsp1 := t1.Fsp()
	if fsp < fsp1 {
		fsp = fsp1
	}
	return Duration{
		Duration: duration,
		Fsp:      fsp,
	}
}

// Add adds d to t, returns the result time value.
func (t *Time) Add(ctx Context, d Duration) (Time, error) {
	seconds, microseconds, _ := calcTimeDurationDiff(t.coreTime, d)
	days := seconds / secondsIn24Hour
	year, month, day := getDateFromDaynr(uint(days))
	var tm Time
	tm.setYear(uint16(year))
	tm.setMonth(uint8(month))
	tm.setDay(uint8(day))
	calcTimeFromSec(&tm.coreTime, seconds%secondsIn24Hour, microseconds)
	if t.Type() == mysql.TypeDate {
		tm.setHour(0)
		tm.setMinute(0)
		tm.setSecond(0)
		tm.setMicrosecond(0)
	}
	fsp := max(d.Fsp, t.Fsp())
	ret := NewTime(tm.coreTime, t.Type(), fsp)
	return ret, ret.Check(ctx)
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
		m := t.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(m)))
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(m)))
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		buf.WriteString(FormatIntWidthN(t.Month(), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
		buf.WriteString(abbrDayOfMonth(t.Day()))
	case 'd':
		buf.WriteString(FormatIntWidthN(t.Day(), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	case 'j':
		fmt.Fprintf(buf, "%03d", t.YearDay())
	case 'H':
		buf.WriteString(FormatIntWidthN(t.Hour(), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	case 'h', 'I':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntWidthN(tt%12, 2))
		}
	case 'l':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(tt%12), 10))
		}
	case 'i':
		buf.WriteString(FormatIntWidthN(t.Minute(), 2))
	case 'p':
		hour := t.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Minute(), t.Second())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Minute(), t.Second())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Minute(), t.Second())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Minute(), t.Second())
		}
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
	case 'S', 's':
		buf.WriteString(FormatIntWidthN(t.Second(), 2))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.Microsecond())
	case 'U':
		w := t.Week(0)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'u':
		w := t.Week(1)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'V':
		w := t.Week(2)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'v':
		_, w := t.YearWeek(3)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'a':
		weekday := t.Weekday()
		buf.WriteString(abbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.Weekday().String())
	case 'w':
		buf.WriteString(strconv.FormatInt(int64(t.Weekday()), 10))
	case 'X':
		year, _ := t.YearWeek(2)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'x':
		year, _ := t.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'Y':
		buf.WriteString(FormatIntWidthN(t.Year(), 4))
	case 'y':
		str := FormatIntWidthN(t.Year(), 4)
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

