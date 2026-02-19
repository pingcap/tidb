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
	"fmt"
	"math"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ExtractDatetimeNum extracts time value number from datetime unit and format.
func ExtractDatetimeNum(t *Time, unit string) (int64, error) {
	// TODO: Consider time_zone variable.
	switch strings.ToUpper(unit) {
	case "DAY":
		return int64(t.Day()), nil
	case "WEEK":
		week := t.Week(0)
		return int64(week), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "QUARTER":
		m := int64(t.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return int64(t.Year()), nil
	case "DAY_MICROSECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d*1000000+h*10000+m*100+s)*1000000 + int64(t.Microsecond()), nil
	case "DAY_SECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d)*1000000 + int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "DAY_MINUTE":
		h, m, _ := t.Clock()
		d := t.Day()
		return int64(d)*10000 + int64(h)*100 + int64(m), nil
	case "DAY_HOUR":
		h, _, _ := t.Clock()
		d := t.Day()
		return int64(d)*100 + int64(h), nil
	case "YEAR_MONTH":
		y, m := t.Year(), t.Month()
		return int64(y)*100 + int64(m), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// ExtractDurationNum extracts duration value number from duration unit and format.
func ExtractDurationNum(d *Duration, unit string) (res int64, err error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		res = int64(d.MicroSecond())
	case "SECOND":
		res = int64(d.Second())
	case "MINUTE":
		res = int64(d.Minute())
	case "HOUR":
		res = int64(d.Hour())
	case "SECOND_MICROSECOND":
		res = int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "MINUTE_MICROSECOND":
		res = int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "MINUTE_SECOND":
		res = int64(d.Minute()*100 + d.Second())
	case "HOUR_MICROSECOND":
		res = int64(d.Hour())*10000000000 + int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "HOUR_SECOND":
		res = int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second())
	case "HOUR_MINUTE":
		res = int64(d.Hour())*100 + int64(d.Minute())
	case "DAY_MICROSECOND":
		res = int64(d.Hour()*10000+d.Minute()*100+d.Second())*1000000 + int64(d.MicroSecond())
	case "DAY_SECOND":
		res = int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second())
	case "DAY_MINUTE":
		res = int64(d.Hour())*100 + int64(d.Minute())
	case "DAY_HOUR":
		res = int64(d.Hour())
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
	if d.Duration < 0 {
		res = -res
	}
	return res, nil
}

// parseSingleTimeValue parse the format according the given unit. If we set strictCheck true, we'll check whether
// the converted value not exceed the range of MySQL's TIME type.
// The returned values are year, month, day, nanosecond and fsp.
func parseSingleTimeValue(unit string, format string, strictCheck bool) (year int64, month int64, day int64, nanosecond int64, fsp int, err error) {
	// Format is a preformatted number, it format should be A[.[B]].
	decimalPointPos := strings.IndexRune(format, '.')
	if decimalPointPos == -1 {
		decimalPointPos = len(format)
	}
	sign := int64(1)
	if len(format) > 0 && format[0] == '-' {
		sign = int64(-1)
	}

	// We should also continue even if an error occurs here
	// because the called may ignore the error and use the return value.
	iv, err := strconv.ParseInt(format[0:decimalPointPos], 10, 64)
	if err != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
	}
	riv := iv // Rounded integer value

	decimalLen := 0
	dv := int64(0)
	lf := len(format) - 1
	// Has fraction part
	if decimalPointPos < lf {
		var tmpErr error
		dvPre := oneToSixDigitRegex.FindString(format[decimalPointPos+1:]) // the numberical prefix of the fraction part
		decimalLen = len(dvPre)
		if decimalLen >= 6 {
			// MySQL rounds down to 1e-6.
			if dv, tmpErr = strconv.ParseInt(dvPre[0:6], 10, 64); tmpErr != nil && err == nil {
				err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
			}
		} else {
			if dv, tmpErr = strconv.ParseInt(dvPre+"000000"[:6-decimalLen], 10, 64); tmpErr != nil && err == nil {
				err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
			}
		}
		if dv >= 500000 { // Round up, and we should keep 6 digits for microsecond, so dv should in [000000, 999999].
			riv += sign
		}
		if unit != "SECOND" && err == nil {
			err = ErrTruncatedWrongVal.GenWithStackByArgs(format)
		}
		dv *= sign
	}
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		if strictCheck && mathutil.Abs(riv) > TimeMaxValueSeconds*1000 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Microsecond)
		riv %= int64(GoDurationDay / gotime.Microsecond)
		return 0, 0, dayCount, riv * int64(gotime.Microsecond), MaxFsp, err
	case "SECOND":
		if strictCheck && mathutil.Abs(iv) > TimeMaxValueSeconds {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := iv / int64(GoDurationDay/gotime.Second)
		iv %= int64(GoDurationDay / gotime.Second)
		return 0, 0, dayCount, iv*int64(gotime.Second) + dv*int64(gotime.Microsecond), decimalLen, err
	case "MINUTE":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour*60+TimeMaxMinute {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Minute)
		riv %= int64(GoDurationDay / gotime.Minute)
		return 0, 0, dayCount, riv * int64(gotime.Minute), 0, err
	case "HOUR":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / 24
		riv %= 24
		return 0, 0, dayCount, riv * int64(gotime.Hour), 0, err
	case "DAY":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, riv, 0, 0, err
	case "WEEK":
		if strictCheck && 7*mathutil.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, 7 * riv, 0, 0, err
	case "MONTH":
		if strictCheck && mathutil.Abs(riv) > 1 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, riv, 0, 0, 0, err
	case "QUARTER":
		if strictCheck {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 3 * riv, 0, 0, 0, err
	case "YEAR":
		if strictCheck {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return riv, 0, 0, 0, 0, err
	}

	return 0, 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// parseTimeValue gets years, months, days, nanoseconds and fsp from a string
// nanosecond will not exceed length of single day
// MySQL permits any punctuation delimiter in the expr format.
// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
func parseTimeValue(format string, index, cnt int) (years int64, months int64, days int64, nanoseconds int64, fsp int, err error) {
	neg := false
	originalFmt := format
	fsp = map[bool]int{true: MaxFsp, false: MinFsp}[index == MicrosecondIndex]
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
		return 0, 0, 0, 0, 0, ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	for i := range matches {
		if neg {
			fields[index] = "-" + matches[len(matches)-1-i]
		} else {
			fields[index] = matches[len(matches)-1-i]
		}
		index--
	}

	// ParseInt may return an error when overflowed, but we should continue to parse the rest of the string because
	// the caller may ignore the error and use the return value.
	// In this case, we should return a big value to make sure the result date after adding this interval
	// is also overflowed and NULL is returned to the user.
	years, err = strconv.ParseInt(fields[YearIndex], 10, 64)
	if err != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	var tmpErr error
	months, tmpErr = strconv.ParseInt(fields[MonthIndex], 10, 64)
	if err == nil && tmpErr != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	days, tmpErr = strconv.ParseInt(fields[DayIndex], 10, 64)
	if err == nil && tmpErr != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}

	hours, tmpErr := strconv.ParseInt(fields[HourIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	minutes, tmpErr := strconv.ParseInt(fields[MinuteIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	seconds, tmpErr := strconv.ParseInt(fields[SecondIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	microseconds, tmpErr := strconv.ParseInt(alignFrac(fields[MicrosecondIndex], MaxFsp), 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	seconds = hours*3600 + minutes*60 + seconds
	days += seconds / (3600 * 24)
	seconds %= 3600 * 24
	return years, months, days, seconds*int64(gotime.Second) + microseconds*int64(gotime.Microsecond), fsp, err
}

func parseAndValidateDurationValue(format string, index, cnt int) (int64, int, error) {
	year, month, day, nano, fsp, err := parseTimeValue(format, index, cnt)
	if err != nil {
		return 0, 0, err
	}
	if year != 0 || month != 0 || mathutil.Abs(day) > TimeMaxHour/24 {
		return 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	dur := day*int64(GoDurationDay) + nano
	if mathutil.Abs(dur) > int64(MaxTime) {
		return 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	return dur, fsp, nil
}

// ParseDurationValue parses time value from time unit and format.
// Returns y years m months d days + n nanoseconds
// Nanoseconds will no longer than one day.
func ParseDurationValue(unit string, format string) (y int64, m int64, d int64, n int64, fsp int, _ error) {
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
		return 0, 0, 0, 0, 0, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// ExtractDurationValue extract the value from format to Duration.
func ExtractDurationValue(unit string, format string) (Duration, error) {
	unit = strings.ToUpper(unit)
	switch unit {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		_, month, day, nano, fsp, err := parseSingleTimeValue(unit, format, true)
		if err != nil {
			return ZeroDuration, err
		}
		dur := Duration{Duration: gotime.Duration((month*30+day)*int64(GoDurationDay) + nano), Fsp: fsp}
		return dur, err
	case "SECOND_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "MINUTE_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "MINUTE_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, MinuteSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, HourSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_MINUTE":
		d, fsp, err := parseAndValidateDurationValue(format, MinuteIndex, HourMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, DaySecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_MINUTE":
		d, fsp, err := parseAndValidateDurationValue(format, MinuteIndex, DayMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_HOUR":
		d, fsp, err := parseAndValidateDurationValue(format, HourIndex, DayHourMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "YEAR_MONTH":
		_, _, err := parseAndValidateDurationValue(format, MonthIndex, YearMonthMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		// MONTH must exceed the limit of mysql's duration. So just returns overflow error.
		return ZeroDuration, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	default:
		return ZeroDuration, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// IsClockUnit returns true when unit is interval unit with hour, minute, second or microsecond.
func IsClockUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR",
		"SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND",
		"MINUTE_SECOND", "HOUR_SECOND", "DAY_SECOND",
		"HOUR_MINUTE", "DAY_MINUTE",
		"DAY_HOUR":
		return true
	default:
		return false
	}
}

// IsDateUnit returns true when unit is interval unit with year, quarter, month, week or day.
func IsDateUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
		"DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR",
		"YEAR_MONTH":
		return true
	default:
		return false
	}
}

// IsMicrosecondUnit returns true when unit is interval unit with microsecond.
func IsMicrosecondUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
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
		// "20129" will be parsed to 2020-12-09, which is date format.
		if (length == 8) || (length == 6) || (length == 5) {
			return true
		}
	case 3:
		return true
	}
	return false
}

