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
	"fmt"
	"math"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/parser"
)
// NewDuration construct duration with time.
func NewDuration(hour, minute, second, microsecond int, fsp int) Duration {
	return Duration{
		Duration: gotime.Duration(hour)*gotime.Hour + gotime.Duration(minute)*gotime.Minute + gotime.Duration(second)*gotime.Second + gotime.Duration(microsecond)*gotime.Microsecond, //nolint:durationcheck
		Fsp:      fsp,
	}
}

// Duration is the type for MySQL TIME type.
type Duration struct {
	gotime.Duration
	// Fsp is short for Fractional Seconds Precision.
	// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
	Fsp int
}

// MaxMySQLDuration returns Duration with maximum mysql time.
func MaxMySQLDuration(fsp int) Duration {
	return NewDuration(TimeMaxHour, TimeMaxMinute, TimeMaxSecond, 0, fsp)
}

// Neg negative d, returns a duration value.
func (d Duration) Neg() Duration {
	return Duration{
		Duration: -d.Duration,
		Fsp:      d.Fsp,
	}
}

// Add adds d to d, returns a duration value.
func (d Duration) Add(v Duration) (Duration, error) {
	if v == (Duration{}) {
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
	if v == (Duration{}) {
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

// DurationFormat returns a textual representation of the duration value formatted
// according to layout.
// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (d Duration) DurationFormat(layout string) (string, error) {
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range layout {
		if inPatternMatch {
			if err := d.convertDateFormat(b, &buf); err != nil {
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

func (d Duration) convertDateFormat(b rune, buf *bytes.Buffer) error {
	switch b {
	case 'H':
		buf.WriteString(FormatIntWidthN(d.Hour(), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(d.Hour()), 10))
	case 'h', 'I':
		t := d.Hour()
		if t%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntWidthN(t%12, 2))
		}
	case 'l':
		t := d.Hour()
		if t%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(t%12), 10))
		}
	case 'i':
		buf.WriteString(FormatIntWidthN(d.Minute(), 2))
	case 'p':
		hour := d.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := d.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, d.Minute(), d.Second())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, d.Minute(), d.Second())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, d.Minute(), d.Second())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, d.Minute(), d.Second())
		}
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", d.Hour(), d.Minute(), d.Second())
	case 'S', 's':
		buf.WriteString(FormatIntWidthN(d.Second(), 2))
	case 'f':
		fmt.Fprintf(buf, "%06d", d.MicroSecond())
	default:
		buf.WriteRune(b)
	}

	return nil
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
func (d Duration) ConvertToTime(ctx Context, tp uint8) (Time, error) {
	year, month, day := gotime.Now().In(ctx.Location()).Date()
	datePart := FromDate(year, int(month), day, 0, 0, 0, 0)
	mixDateAndDuration(&datePart, d)

	t := NewTime(datePart, mysql.TypeDatetime, d.Fsp)
	return t.Convert(ctx, tp)
}

// ConvertToTimeWithTimestamp converts duration to Time by system timestamp.
// Tp is TypeDatetime, TypeTimestamp and TypeDate.
func (d Duration) ConvertToTimeWithTimestamp(ctx Context, tp uint8, ts gotime.Time) (Time, error) {
	year, month, day := ts.In(ctx.Location()).Date()
	datePart := FromDate(year, int(month), day, 0, 0, 0, 0)
	mixDateAndDuration(&datePart, d)

	t := NewTime(datePart, mysql.TypeDatetime, d.Fsp)
	return t.Convert(ctx, tp)
}

// ConvertToYear converts duration to Year.
func (d Duration) ConvertToYear(ctx Context) (int64, error) {
	return d.ConvertToYearFromNow(ctx, gotime.Now())
}

// ConvertToYearFromNow converts duration to Year, with the `now` specified by the argument.
func (d Duration) ConvertToYearFromNow(ctx Context, now gotime.Time) (int64, error) {
	if ctx.Flags().CastTimeToYearThroughConcat() {
		// this error will never happen, because we always give a valid FSP
		dur, _ := d.RoundFrac(DefaultFsp, ctx.Location())
		// the range of a duration will never exceed the range of `mysql.TypeLonglong`
		ival, _ := dur.ToNumber().ToInt()

		return AdjustYear(ival, false)
	}

	year, month, day := now.In(ctx.Location()).Date()
	datePart := FromDate(year, int(month), day, 0, 0, 0, 0)
	mixDateAndDuration(&datePart, d)

	return AdjustYear(int64(datePart.Year()), false)
}

// RoundFrac rounds fractional seconds precision with new fsp and returns a new one.
// We will use the “round half up” rule, e.g, >= 0.5 -> 1, < 0.5 -> 0,
// so 10:10:10.999999 round 0 -> 10:10:11
// and 10:10:10.000000 round 0 -> 10:10:10
func (d Duration) RoundFrac(fsp int, loc *gotime.Location) (Duration, error) {
	tz := loc
	if tz == nil {
		logutil.BgLogger().Warn("use gotime.local because sc.timezone is nil")
		tz = gotime.Local
	}

	fsp, err := CheckFsp(fsp)
	if err != nil {
		return d, errors.Trace(err)
	}

	if fsp == d.Fsp {
		return d, nil
	}

	n := gotime.Date(0, 0, 0, 0, 0, 0, 0, tz)
	nd := n.Add(d.Duration).Round(gotime.Duration(math.Pow10(9-fsp)) * gotime.Nanosecond).Sub(n) //nolint:durationcheck
	return Duration{Duration: nd, Fsp: fsp}, nil
}

// Compare returns an integer comparing the Duration instant t to o.
// If d is after o, returns 1, equal o, returns 0, before o, returns -1.
func (d Duration) Compare(o Duration) int {
	if d.Duration > o.Duration {
		return 1
	} else if d.Duration == o.Duration {
		return 0
	}
	return -1
}

// CompareString is like Compare,
// but parses str to Duration then compares.
func (d Duration) CompareString(ctx Context, str string) (int, error) {
	// use MaxFsp to parse the string
	o, _, err := ParseDuration(ctx, str, MaxFsp)
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

func isNegativeDuration(str string) (bool, string) {
	rest, err := parser.Char(str, '-')

	if err != nil {
		return false, str
	}

	return true, rest
}

func matchColon(str string) (string, error) {
	rest := parser.Space0(str)
	rest, err := parser.Char(rest, ':')
	if err != nil {
		return str, err
	}
	rest = parser.Space0(rest)
	return rest, nil
}

func matchDayHHMMSS(str string) (int, [3]int, string, error) {
	day, rest, err := parser.Number(str)
	if err != nil {
		return 0, [3]int{}, str, err
	}

	rest, err = parser.Space(rest, 1)
	if err != nil {
		return 0, [3]int{}, str, err
	}

	hhmmss, rest, err := matchHHMMSSDelimited(rest, false)
	if err != nil {
		return 0, [3]int{}, str, err
	}

	return day, hhmmss, rest, nil
}

func matchHHMMSSDelimited(str string, requireColon bool) ([3]int, string, error) {
	hhmmss := [3]int{}

	hour, rest, err := parser.Number(str)
	if err != nil {
		return [3]int{}, str, err
	}
	hhmmss[0] = hour

	for i := 1; i < 3; i++ {
		remain, err := matchColon(rest)
		if err != nil {
			if i == 1 && requireColon {
				return [3]int{}, str, err
			}
			break
		}
		num, remain, err := parser.Number(remain)
		if err != nil {
			return [3]int{}, str, err
		}
		hhmmss[i] = num
		rest = remain
	}

	return hhmmss, rest, nil
}

func matchHHMMSSCompact(str string) ([3]int, string, error) {
	num, rest, err := parser.Number(str)
	if err != nil {
		return [3]int{}, str, err
	}
	hhmmss := [3]int{num / 10000, (num / 100) % 100, num % 100}
	return hhmmss, rest, nil
}

func hhmmssAddOverflow(hms []int, overflow bool) {
	mod := []int{-1, 60, 60}
	for i := 2; i >= 0 && overflow; i-- {
		hms[i]++
		if hms[i] == mod[i] {
			overflow = true
			hms[i] = 0
		} else {
			overflow = false
		}
	}
}

func checkHHMMSS(hms [3]int) bool {
	m, s := hms[1], hms[2]
	return m < 60 && s < 60
}

// matchFrac returns overflow, fraction, rest, error
func matchFrac(str string, fsp int) (bool, int, string, error) {
	rest, err := parser.Char(str, '.')
	if err != nil {
		return false, 0, str, nil
	}

	digits, rest, err := parser.Digit(rest, 0)
	if err != nil {
		return false, 0, str, err
	}

	frac, overflow, err := ParseFrac(digits, fsp)
	if err != nil {
		return false, 0, str, err
	}

	return overflow, frac, rest, nil
}

func matchDuration(str string, fsp int) (Duration, bool, error) {
	fsp, err := CheckFsp(fsp)
	if err != nil {
		return ZeroDuration, true, errors.Trace(err)
	}

	if len(str) == 0 {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	negative, rest := isNegativeDuration(str)
	rest = parser.Space0(rest)
	charsLen := len(rest)

	hhmmss := [3]int{}

	if day, hms, remain, err := matchDayHHMMSS(rest); err == nil {
		hms[0] += 24 * day
		rest, hhmmss = remain, hms
	} else if hms, remain, err := matchHHMMSSDelimited(rest, true); err == nil {
		rest, hhmmss = remain, hms
	} else if hms, remain, err := matchHHMMSSCompact(rest); err == nil {
		rest, hhmmss = remain, hms
	} else {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	rest = parser.Space0(rest)
	overflow, frac, rest, err := matchFrac(rest, fsp)
	if err != nil || (len(rest) > 0 && charsLen >= 12) {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	if overflow {
		hhmmssAddOverflow(hhmmss[:], overflow)
		frac = 0
	}

	if !checkHHMMSS(hhmmss) {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	if hhmmss[0] > TimeMaxHour {
		var t gotime.Duration
		if negative {
			t = MinTime
		} else {
			t = MaxTime
		}
		return Duration{t, fsp}, false, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	d := gotime.Duration(hhmmss[0]*3600+hhmmss[1]*60+hhmmss[2])*gotime.Second + gotime.Duration(frac)*gotime.Microsecond //nolint:durationcheck
	if negative {
		d = -d
	}
	d, err = TruncateOverflowMySQLTime(d)
	if err == nil && len(rest) > 0 {
		return Duration{d, fsp}, false, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}
	return Duration{d, fsp}, false, errors.Trace(err)
}

// canFallbackToDateTime return true
//  1. the string is failed to be parsed by `matchDuration`
//  2. the string is start with a series of digits whose length match the full format of DateTime literal (12, 14)
//     or the string start with a date literal.
func canFallbackToDateTime(str string) bool {
	digits, rest, err := parser.Digit(str, 1)
	if err != nil {
		return false
	}
	if len(digits) == 12 || len(digits) == 14 {
		return true
	}

	rest, err = parser.AnyPunct(rest)
	if err != nil {
		return false
	}

	_, rest, err = parser.Digit(rest, 1)
	if err != nil {
		return false
	}

	rest, err = parser.AnyPunct(rest)
	if err != nil {
		return false
	}

	_, rest, err = parser.Digit(rest, 1)
	if err != nil {
		return false
	}

	return len(rest) > 0 && (rest[0] == ' ' || rest[0] == 'T')
}

// ParseDuration parses the time form a formatted string with a fractional seconds part,
// returns the duration type Time value and bool to indicate whether the result is null.
// See http://dev.mysql.com/doc/refman/5.7/en/fractional-seconds.html
func ParseDuration(ctx Context, str string, fsp int) (Duration, bool, error) {
	rest := strings.TrimSpace(str)
	d, isNull, err := matchDuration(rest, fsp)
	if err == nil {
		return d, isNull, nil
	}
	if !canFallbackToDateTime(rest) {
		return d, isNull, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	datetime, err := ParseDatetime(ctx, rest)
	if err != nil {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	d, err = datetime.ConvertToDuration()
	if err != nil {
		return ZeroDuration, true, ErrTruncatedWrongVal.GenWithStackByArgs("time", str)
	}

	d, err = d.RoundFrac(fsp, ctx.Location())
	return d, false, err
}

// TruncateOverflowMySQLTime truncates d when it overflows, and returns ErrTruncatedWrongVal.
func TruncateOverflowMySQLTime(d gotime.Duration) (gotime.Duration, error) {
	if d > MaxTime {
		return MaxTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
	} else if d < MinTime {
		return MinTime, ErrTruncatedWrongVal.GenWithStackByArgs("time", d)
	}

	return d, nil
}

func splitDuration(t gotime.Duration) (sign int, hours int, minutes int, seconds int, fraction int) {
	sign = 1
	if t < 0 {
		t = -t
		sign = -1
	}

	hoursDuration := t / gotime.Hour
	t -= hoursDuration * gotime.Hour //nolint:durationcheck
	minutesDuration := t / gotime.Minute
	t -= minutesDuration * gotime.Minute //nolint:durationcheck
	secondsDuration := t / gotime.Second
	t -= secondsDuration * gotime.Second //nolint:durationcheck
	fractionDuration := t / gotime.Microsecond

	return sign, int(hoursDuration), int(minutesDuration), int(secondsDuration), int(fractionDuration)
}
