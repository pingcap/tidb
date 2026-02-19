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
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// StrToDate converts date string according to format.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t *Time) StrToDate(typeCtx Context, date, format string) bool {
	ctx := make(map[string]int)
	var tm CoreTime
	success, warning := strToDate(&tm, date, format, ctx)
	if !success {
		t.SetCoreTime(ZeroCoreTime)
		t.SetType(mysql.TypeDatetime)
		t.SetFsp(0)
		return false
	}
	if err := mysqlTimeFix(&tm, ctx); err != nil {
		return false
	}

	t.SetCoreTime(tm)
	t.SetType(mysql.TypeDatetime)
	if t.Check(typeCtx) != nil {
		return false
	}
	if warning {
		// Only append this warning when success but still need warning.
		// Currently this only happens when `date` has extra characters at the end.
		typeCtx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs(DateTimeStr, date))
	}
	return true
}

// mysqlTimeFix fixes the Time use the values in the context.
func mysqlTimeFix(t *CoreTime, ctx map[string]int) error {
	// Key of the ctx is the format char, such as `%j` `%p` and so on.
	if yearOfDay, ok := ctx["%j"]; ok {
		// TODO: Implement the function that converts day of year to yy:mm:dd.
		_ = yearOfDay
	}
	if valueAMorPm, ok := ctx["%p"]; ok {
		if _, ok := ctx["%H"]; ok {
			return ErrWrongValue.GenWithStackByArgs(TimeStr, t)
		}
		if t.Hour() == 0 {
			return ErrWrongValue.GenWithStackByArgs(TimeStr, t)
		}
		if t.Hour() == 12 {
			// 12 is a special hour.
			switch valueAMorPm {
			case constForAM:
				t.setHour(0)
			case constForPM:
				t.setHour(12)
			}
			return nil
		}
		if valueAMorPm == constForPM {
			t.setHour(t.getHour() + 12)
		}
	} else {
		if _, ok := ctx["%h"]; ok && t.Hour() == 12 {
			t.setHour(0)
		}
	}
	return nil
}

// strToDate converts date string according to format,
// the value will be stored in argument t or ctx.
// The second return value is true when success but still need to append a warning.
func strToDate(t *CoreTime, date string, format string, ctx map[string]int) (success bool, warning bool) {
	date = skipWhiteSpace(date)
	format = skipWhiteSpace(format)

	token, formatRemain, succ := getFormatToken(format)
	if !succ {
		return false, false
	}

	if token == "" {
		if len(date) != 0 {
			// Extra characters at the end of date are ignored, but a warning should be reported at this case.
			return true, true
		}
		// Normal case. Both token and date are empty now.
		return true, false
	}

	if len(date) == 0 {
		ctx[token] = 0
		return true, false
	}

	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		return false, false
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

var monthAbbrev = map[string]gotime.Month{
	"jan": gotime.January,
	"feb": gotime.February,
	"mar": gotime.March,
	"apr": gotime.April,
	"may": gotime.May,
	"jun": gotime.June,
	"jul": gotime.July,
	"aug": gotime.August,
	"sep": gotime.September,
	"oct": gotime.October,
	"nov": gotime.November,
	"dec": gotime.December,
}

type dateFormatParser func(t *CoreTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,      // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,          // Month, numeric (0..12)
	"%d": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%e": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%f": microSeconds,          // Microseconds (000000..999999)
	"%h": hour12Numeric,         // Hour (01..12)
	"%H": hour24Numeric,         // Hour (00..23)
	"%I": hour12Numeric,         // Hour (01..12)
	"%i": minutesNumeric,        // Minutes, numeric (00..59)
	"%j": dayOfYearNumeric,      // Day of year (001..366)
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
	"%#": skipAllNums,           // Skip all numbers
	"%.": skipAllPunct,          // Skip all punctation characters
	"%@": skipAllAlpha,          // Skip all alpha characters
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
		if len(token) >= 2 && token[0] == '%' {
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'f', 'r', 'T':
				isDuration = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				isDate = true
			}
		}
		if isDuration && isDate {
			break
		}
	}
	return
}

func matchDateWithToken(t *CoreTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date, ctx)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

// Try to parse digits with number of `limit` starting from `input`
// Return <number, n chars to step forward> if success.
// Return <_, 0> if fail.
func parseNDigits(input string, limit int) (number int, step int) {
	if limit <= 0 {
		return 0, 0
	}

	var num uint64 = 0
	step = 0
	for ; step < len(input) && step < limit && '0' <= input[step] && input[step] <= '9'; step++ {
		num = num*10 + uint64(input[step]-'0')
	}
	return int(num), step
}

func secondsNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setSecond(uint8(v))
	return input[step:], true
}

func minutesNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setMinute(uint8(v))
	return input[step:], true
}

type parseState int32

const (
	parseStateNormal    parseState = 1
	parseStateFail      parseState = 2
	parseStateEndOfLine parseState = 3
)

func parseSep(input string) (string, parseState) {
	input = skipWhiteSpace(input)
	if len(input) == 0 {
		return input, parseStateEndOfLine
	}
	if input[0] != ':' {
		return input, parseStateFail
	}
	if input = skipWhiteSpace(input[1:]); len(input) == 0 {
		return input, parseStateEndOfLine
	}
	return input, parseStateNormal
}

func time12Hour(t *CoreTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss AM
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 1..12
		if step <= 0 || hour > 12 || hour == 0 {
			return input, parseStateFail
		}
		// Handle special case: 12:34:56 AM -> 00:34:56
		// For PM, we will add 12 it later
		if hour == 12 {
			hour = 0
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))

		input = skipWhiteSpace(input[step:])
		if len(input) == 0 {
			// No "AM"/"PM" suffix, it is ok
			return input, parseStateEndOfLine
		} else if len(input) < 2 {
			// some broken char, fail
			return input, parseStateFail
		}

		switch {
		case hasCaseInsensitivePrefix(input, "AM"):
			t.setHour(uint8(hour))
		case hasCaseInsensitivePrefix(input, "PM"):
			t.setHour(uint8(hour + 12))
		default:
			return input, parseStateFail
		}

		return input[2:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

func time24Hour(t *CoreTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 0..23
		if step <= 0 || hour > 23 {
			return input, parseStateFail
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))
		return input[step:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

const (
	constForAM = 1 + iota
	constForPM
)

func isAMOrPM(_ *CoreTime, input string, ctx map[string]int) (string, bool) {
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

// oneToSixDigitRegex: it was just for [0, 999999]
var oneToSixDigitRegex = regexp.MustCompile("^[0-9]{0,6}")

// numericRegex: it was for any numeric characters
var numericRegex = regexp.MustCompile("[0-9]+")

func dayOfMonthNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..31
	if step <= 0 || v > 31 {
		return input, false
	}
	t.setDay(uint8(v))
	return input[step:], true
}

func hour24Numeric(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..23
	if step <= 0 || v > 23 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%H"] = v
	return input[step:], true
}

func hour12Numeric(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 || v == 0 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%h"] = v
	return input[step:], true
}

func microSeconds(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 6)
	if step <= 0 {
		t.setMicrosecond(0)
		return input, true
	}
	for i := step; i < 6; i++ {
		v *= 10
	}
	t.setMicrosecond(uint32(v))
	return input[step:], true
}

func yearNumericFourDigits(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 4)
}

func yearNumericTwoDigits(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 2)
}

func yearNumericNDigits(t *CoreTime, input string, _ map[string]int, n int) (string, bool) {
	year, step := parseNDigits(input, n)
	if step <= 0 {
		return input, false
	} else if step <= 2 {
		year = adjustYear(year)
	}
	t.setYear(uint16(year))
	return input[step:], true
}

func dayOfYearNumeric(_ *CoreTime, input string, ctx map[string]int) (string, bool) {
	// MySQL declares that "%j" should be "Day of year (001..366)". But actually,
	// it accepts a number that is up to three digits, which range is [1, 999].
	v, step := parseNDigits(input, 3)
	if step <= 0 || v == 0 {
		return input, false
	}
	ctx["%j"] = v
	return input[step:], true
}

func abbreviatedMonth(t *CoreTime, input string, _ map[string]int) (string, bool) {
	if len(input) >= 3 {
		monthName := strings.ToLower(input[:3])
		if month, ok := monthAbbrev[monthName]; ok {
			t.setMonth(uint8(month))
			return input[len(monthName):], true
		}
	}
	return input, false
}

func hasCaseInsensitivePrefix(input, prefix string) bool {
	if len(input) < len(prefix) {
		return false
	}
	return strings.EqualFold(input[:len(prefix)], prefix)
}

func fullNameMonth(t *CoreTime, input string, _ map[string]int) (string, bool) {
	for i, month := range MonthNames {
		if hasCaseInsensitivePrefix(input, month) {
			t.setMonth(uint8(i + 1))
			return input[len(month):], true
		}
	}
	return input, false
}

func monthNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 {
		return input, false
	}
	t.setMonth(uint8(v))
	return input[step:], true
}

// DateFSP gets fsp from date string.
func DateFSP(date string) (fsp int) {
	i := strings.LastIndex(date, ".")
	if i != -1 {
		fsp = len(date) - i - 1
	}
	return
}

// DateTimeIsOverflow returns if this date is overflow.
// See: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DateTimeIsOverflow(ctx Context, date Time) (bool, error) {
	tz := ctx.Location()
	if tz == nil {
		logutil.BgLogger().Warn("use gotime.local because sc.timezone is nil")
		tz = gotime.Local
	}

	var err error
	var b, e, t gotime.Time
	switch date.Type() {
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
		if b, err = minTS.GoTime(tz); err != nil {
			return false, err
		}
		if e, err = maxTS.GoTime(tz); err != nil {
			return false, err
		}
	default:
		return false, nil
	}

	if t, err = date.AdjustedGoTime(tz); err != nil {
		return false, err
	}

	inRange := (t.After(b) || t.Equal(b)) && (t.Before(e) || t.Equal(e))
	return !inRange, nil
}

func skipAllNums(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsNumber(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}

func skipAllPunct(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsPunct(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}

func skipAllAlpha(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsLetter(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}
