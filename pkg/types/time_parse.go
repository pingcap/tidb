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
	"regexp"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// TimestampDiff returns t2 - t1 where t1 and t2 are date or datetime expressions.
// The unit for the result (an integer) is given by the unit argument.
// The legal values for unit are "YEAR" "QUARTER" "MONTH" "DAY" "HOUR" "SECOND" and so on.
func TimestampDiff(unit string, t1 Time, t2 Time) int64 {
	return timestampDiff(unit, t1.coreTime, t2.coreTime)
}

// ParseDateFormat parses a formatted date string and returns separated components.
func ParseDateFormat(format string) []string {
	format = strings.TrimSpace(format)

	if len(format) == 0 {
		return nil
	}

	// Date format must start with number.
	if !isDigit(format[0]) {
		return nil
	}

	start := 0
	// Initialize `seps` with capacity of 6. The input `format` is typically
	// a date time of the form time.DateTime, which has 6 numeric parts
	// (the fractional second part is usually removed by `splitDateTime`).
	// Setting `seps`'s capacity to 6 avoids reallocation in this common case.
	seps := make([]string, 0, 6)

	for i := 1; i < len(format)-1; i++ {
		if isValidSeparator(format[i], len(seps)) {
			prevParts := len(seps)
			seps = append(seps, format[start:i])
			start = i + 1

			// consume further consecutive separators
			for j := i + 1; j < len(format); j++ {
				if !isValidSeparator(format[j], prevParts) {
					break
				}

				start++
				i++
			}

			continue
		}

		if !isDigit(format[i]) {
			return nil
		}
	}

	seps = append(seps, format[start:])
	return seps
}

// helper for date part splitting, punctuation characters are valid separators anywhere,
// while space and 'T' are valid separators only between date and time.
func isValidSeparator(c byte, prevParts int) bool {
	if isPunctuation(c) {
		return true
	}

	// for https://github.com/pingcap/tidb/issues/32232
	if prevParts == 2 && (c == 'T' || c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r') {
		return true
	}

	if prevParts > 4 && !isDigit(c) {
		return true
	}
	return false
}

var validIdxCombinations = map[int]struct {
	h int
	m int
}{
	100: {0, 0}, // 23:59:59Z
	30:  {2, 0}, // 23:59:59+08
	50:  {4, 2}, // 23:59:59+0800
	63:  {5, 2}, // 23:59:59+08:00
	// postgres supports the following additional syntax that deviates from ISO8601, although we won't support it
	// currently, it will be fairly easy to add in the current parsing framework
	// 23:59:59Z+08
	// 23:59:59Z+08:00
}

// GetTimezone parses the trailing timezone information of a given time string literal. If idx = -1 is returned, it
// means timezone information not found, otherwise it indicates the index of the starting index of the timezone
// information. If the timezone contains sign, hour part and/or minute part, it will be returned as is, otherwise an
// empty string will be returned.
//
// Supported syntax:
//
//	MySQL compatible: ((?P<tz_sign>[-+])(?P<tz_hour>[0-9]{2}):(?P<tz_minute>[0-9]{2})){0,1}$, see
//	  https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html and https://dev.mysql.com/doc/refman/8.0/en/datetime.html
//	  the first link specified that timezone information should be in "[H]H:MM, prefixed with a + or -" while the
//	  second link specified that for string literal, "hour values less than than 10, a leading zero is required.".
//	ISO-8601: Z|((((?P<tz_sign>[-+])(?P<tz_hour>[0-9]{2})(:(?P<tz_minute>[0-9]{2}){0,1}){0,1})|((?P<tz_minute>[0-9]{2}){0,1}){0,1}))$
//	  see https://www.cl.cam.ac.uk/~mgk25/iso-time.html
func GetTimezone(lit string) (idx int, tzSign, tzHour, tzSep, tzMinute string) {
	idx, zidx, sidx, spidx := -1, -1, -1, -1
	// idx is for the position of the starting of the timezone information
	// zidx is for the z symbol
	// sidx is for the sign
	// spidx is for the separator
	l := len(lit)
	// the following loop finds the first index of Z, sign, and separator from backwards.
	for i := l - 1; 0 <= i; i-- {
		if lit[i] == 'Z' {
			zidx = i
			break
		}
		if sidx == -1 && (lit[i] == '-' || lit[i] == '+') {
			sidx = i
		}
		if spidx == -1 && lit[i] == ':' {
			spidx = i
		}
	}
	// we could enumerate all valid combinations of these values and look it up in a table, see validIdxCombinations
	// zidx can be -1 (23:59:59+08:00), l-1 (23:59:59Z)
	// sidx can be -1, l-3, l-5, l-6
	// spidx can be -1, l-3
	k := 0
	if l-zidx == 1 {
		k += 100
	}
	if t := l - sidx; t == 3 || t == 5 || t == 6 {
		k += t * 10
	}
	if l-spidx == 3 {
		k += 3
	}
	if v, ok := validIdxCombinations[k]; ok {
		hidx, midx := l-v.h, l-v.m
		valid := func(v string) bool {
			return '0' <= v[0] && v[0] <= '9' && '0' <= v[1] && v[1] <= '9'
		}
		if sidx != -1 {
			tzSign = lit[sidx : sidx+1]
			idx = sidx
		}
		if zidx != -1 {
			idx = zidx
		}
		if (l - spidx) == 3 {
			tzSep = lit[spidx : spidx+1]
		}
		if v.h != 0 {
			tzHour = lit[hidx : hidx+2]
			if !valid(tzHour) {
				return -1, "", "", "", ""
			}
		}
		if v.m != 0 {
			tzMinute = lit[midx : midx+2]
			if !valid(tzMinute) {
				return -1, "", "", "", ""
			}
		}
		return
	}
	return -1, "", "", "", ""
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
// splitDateTime splits the string literal into 3 parts, date & time, FSP(Fractional Seconds Precision) and time zone.
// For FSP, The only delimiter recognized between a date & time part and a fractional seconds part is the decimal point,
// therefore we could look from backwards at the literal to find the index of the decimal point.
// For time zone, the possible delimiter could be +/- (w.r.t. MySQL 8.0, see
// https://dev.mysql.com/doc/refman/8.0/en/datetime.html) and Z/z (w.r.t. ISO 8601, see section Time zone in
// https://www.cl.cam.ac.uk/~mgk25/iso-time.html). We also look from backwards for the delimiter, see GetTimezone.
func splitDateTime(format string) (seps []string, fracStr string, hasTZ bool, tzSign, tzHour, tzSep, tzMinute string, truncated bool) {
	tzIndex, tzSign, tzHour, tzSep, tzMinute := GetTimezone(format)
	if tzIndex > 0 {
		hasTZ = true
		for ; tzIndex > 0 && isPunctuation(format[tzIndex-1]); tzIndex-- {
			// In case of multiple separators, e.g. 2020-10--10
			continue
		}
		format = format[:tzIndex]
	}
	fracIndex := GetFracIndex(format)
	if fracIndex > 0 {
		// Only contain digits
		fracEnd := fracIndex + 1
		for fracEnd < len(format) && isDigit(format[fracEnd]) {
			fracEnd++
		}
		truncated = (fracEnd != len(format))
		fracStr = format[fracIndex+1 : fracEnd]
		for ; fracIndex > 0 && isPunctuation(format[fracIndex-1]); fracIndex-- {
			// In case of multiple separators, e.g. 2020-10..10
			continue
		}
		format = format[:fracIndex]
	}
	seps = ParseDateFormat(format)
	return
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html.
func parseDatetime(ctx Context, str string, fsp int, isFloat bool) (Time, error) {
	var (
		year, month, day, hour, minute, second, deltaHour, deltaMinute int
		fracStr                                                        string
		tzSign, tzHour, tzSep, tzMinute                                string
		hasTZ, hhmmss                                                  bool
		err                                                            error
	)

	seps, fracStr, hasTZ, tzSign, tzHour, tzSep, tzMinute, truncatedOrIncorrect := splitDateTime(str)
	if truncatedOrIncorrect {
		ctx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs("datetime", str))
	}
	/*
		if we have timezone parsed, there are the following cases to be considered, however some of them are wrongly parsed, and we should consider absorb them back to seps.

		1. Z, then it must be time zone information, and we should not tamper with it
		2. -HH, it might be from
		    1. no fracStr
		        1. YYYY-MM-DD
		        2. YYYY-MM-DD-HH
		        3. YYYY-MM-DD HH-MM
		        4. YYYY-MM-DD HH:MM-SS
		        5. YYYY-MM-DD HH:MM:SS-HH (correct, no need absorb)
		    2. with fracStr
		        1. YYYY.MM-DD
		        2. YYYY-MM.DD-HH
		        3. YYYY-MM-DD.HH-MM
		        4. YYYY-MM-DD HH.MM-SS
		        5. YYYY-MM-DD HH:MM.SS-HH (correct, no need absorb)
		3. -HH:MM, similarly it might be from
		    1. no fracStr
		        1. YYYY-MM:DD
		        2. YYYY-MM-DD:HH
		        3. YYYY-MM-DD-HH:MM
		        4. YYYY-MM-DD HH-MM:SS
		        5. YYYY-MM-DD HH:MM-SS:HH (invalid)
		        6. YYYY-MM-DD HH:MM:SS-HH:MM (correct, no need absorb)
		    2. with fracStr
		        1. YYYY.MM-DD:HH
		        2. YYYY-MM.DD-HH:MM
		        3. YYYY-MM-DD.HH-MM:SS
		        4. YYYY-MM-DD HH.MM-SS:HH (invalid)
		        5. YYYY-MM-DD HH:MM.SS-HH:MM (correct, no need absorb)
		4. -HHMM, there should only be one case, that is both the date and time part have existed, only then could we have fracStr or time zone
		    1. YYYY-MM-DD HH:MM:SS.FSP-HHMM (correct, no need absorb)

		to summarize, FSP and timezone is only valid if we have date and time presented, otherwise we should consider absorbing
		FSP or timezone into seps. additionally, if we want to absorb timezone, we either absorb them all, or not, meaning
		we won't only absorb tzHour but not tzMinute.

		additional case to consider is that when the time literal is presented in float string (e.g. `YYYYMMDD.HHMMSS`), in
		this case, FSP should not be absorbed and only `+HH:MM` would be allowed (i.e. Z, +HHMM, +HH that comes from ISO8601
		should be banned), because it only conforms to MySQL's timezone parsing logic, but it is not valid in ISO8601.
		However, I think it is generally acceptable to allow a wider spectrum of timezone format in string literal.
	*/

	// noAbsorb tests if can absorb FSP or TZ
	noAbsorb := func(seps []string) bool {
		// if we have more than 5 parts (i.e. 6), the tailing part can't be absorbed
		// or if we only have 1 part, but its length is longer than 4, then it is at least YYMMD, in this case, FSP can
		// not be absorbed, and it will be handled later, and the leading sign prevents TZ from being absorbed, because
		// if date part has no separators, we can't use -/+ as separators between date & time.
		return len(seps) > 5 || (len(seps) == 1 && len(seps[0]) > 4)
	}
	if len(fracStr) != 0 && !isFloat {
		if !noAbsorb(seps) {
			seps = append(seps, fracStr)
			fracStr = ""
		}
	}
	if hasTZ && tzSign != "" {
		// if tzSign is empty, we can be sure that the string literal contains timezone (such as 2010-10-10T10:10:10Z),
		// therefore we could safely skip this branch.
		if !noAbsorb(seps) && !(tzMinute != "" && tzSep == "") {
			// we can't absorb timezone if there is no separate between tzHour and tzMinute
			if len(tzHour) != 0 {
				seps = append(seps, tzHour)
			}
			if len(tzMinute) != 0 {
				seps = append(seps, tzMinute)
			}
			hasTZ = false
		}
	}
	switch len(seps) {
	case 0:
		return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
	case 1:
		l := len(seps[0])
		// Values specified as numbers
		if isFloat {
			numOfTime, err := StrToInt(ctx, seps[0], false)
			if err != nil {
				return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
			}

			dateTime, err := ParseDatetimeFromNum(ctx, numOfTime)
			if err != nil {
				return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
			}

			year, month, day, hour, minute, second =
				dateTime.Year(), dateTime.Month(), dateTime.Day(), dateTime.Hour(), dateTime.Minute(), dateTime.Second()

			// case: 0.XXX or like "20170118.999"
			if seps[0] == "0" || (l >= 9 && l <= 14) {
				hhmmss = true
			}

			break
		}

		// Values specified as strings
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
		case 7: // YYMMDDH
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d%1d", &year, &month, &day, &hour)
			year = adjustYear(year)
		case 6, 5:
			// YYMMDD && YYMMD
			_, err = fmt.Sscanf(seps[0], "%2d%2d%2d", &year, &month, &day)
			year = adjustYear(year)
		default:
			return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, str))
		}
		if l == 5 || l == 6 || l == 8 {
			// YYMMDD or YYYYMMDD
			// We must handle float => string => datetime, the difference is that fractional
			// part of float type is discarded directly, while fractional part of string type
			// is parsed to HH:MM:SS.
			if !isFloat {
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
			// 20170118.123423 => 2017-01-18 00:00:00
		}
		if l == 9 || l == 10 {
			if len(fracStr) == 0 {
				second = 0
			} else {
				_, err = fmt.Sscanf(fracStr, "%2d ", &second)
			}
			truncatedOrIncorrect = err != nil
		}
		if truncatedOrIncorrect {
			ctx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs("datetime", str))
			err = nil
		}
	case 2:
		return ZeroDatetime, errors.Trace(ErrWrongValue.FastGenByArgs(DateTimeStr, str))
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
		// For case like `2020-05-28 23:59:59 00:00:00`, the seps should be > 6, the reluctant parts should be truncated.
		seps = seps[:6]
		// YYYY-MM-DD HH-MM-SS
		ctx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs("datetime", str))
		err = scanTimeArgs(seps, &year, &month, &day, &hour, &minute, &second)
		hhmmss = true
	}
	if err != nil {
		if err == io.EOF {
			return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
		}
		return ZeroDatetime, errors.Trace(err)
	}

	// If str is sepereated by delimiters, the first one is year, and if the year is 1/2 digit,
	// we should adjust it.
	// TODO: adjust year is very complex, now we only consider the simplest way.
	if len(seps[0]) <= 2 && !isFloat {
		if !(year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && fracStr == "") {
			year = adjustYear(year)
		}
		// Skip a special case "00-00-00".
	}

	var microsecond int
	var overflow bool
	if hhmmss {
		// If input string is "20170118.999", without hhmmss, fsp is meaningless.
		// TODO: this case is not only meaningless, but erroneous, please confirm.
		microsecond, overflow, err = ParseFrac(fracStr, fsp)
		if err != nil {
			return ZeroDatetime, errors.Trace(err)
		}
	}

	tmp, ok := FromDateChecked(year, month, day, hour, minute, second, microsecond)
	if !ok {
		return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
	}
	if overflow {
		// Convert to Go time and add 1 second, to handle input like 2017-01-05 08:40:59.575601
		var t1 gotime.Time
		if t1, err = tmp.GoTime(ctx.Location()); err != nil {
			return ZeroDatetime, errors.Trace(err)
		}
		tmp = FromGoTime(t1.Add(gotime.Second))
	}
	if hasTZ {
		// without hhmmss, timezone is also meaningless
		if !hhmmss {
			return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStack(DateTimeStr, str))
		}
		if len(tzHour) != 0 {
			deltaHour = int((tzHour[0]-'0')*10 + (tzHour[1] - '0'))
		}
		if len(tzMinute) != 0 {
			deltaMinute = int((tzMinute[0]-'0')*10 + (tzMinute[1] - '0'))
		}
		// allowed delta range is [-14:00, 14:00], and we will intentionally reject -00:00
		if deltaHour > 14 || deltaMinute > 59 || (deltaHour == 14 && deltaMinute != 0) || (tzSign == "-" && deltaHour == 0 && deltaMinute == 0) {
			return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, str))
		}
		// by default, if the temporal string literal does not contain timezone information, it will be in the timezone
		// specified by the time_zone system variable. However, if the timezone is specified in the string literal, we
		// will use the specified timezone to interpret the string literal and convert it into the system timezone.
		offset := deltaHour*60*60 + deltaMinute*60
		if tzSign == "-" {
			offset = -offset
		}
		loc := gotime.FixedZone(fmt.Sprintf("UTC%s%s:%s", tzSign, tzHour, tzMinute), offset)
		t1, err := tmp.GoTime(loc)
		if err != nil {
			return ZeroDatetime, errors.Trace(err)
		}
		t1 = t1.In(ctx.Location())
		tmp = FromGoTime(t1)
	}

	nt := NewTime(tmp, mysql.TypeDatetime, fsp)

	return nt, nil
}

func scanTimeArgs(seps []string, args ...*int) error {
	if len(seps) != len(args) {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, seps))
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

	if len(str) == 2 || len(str) == 1 {
		y = int16(adjustYear(int(y)))
	} else if len(str) != 4 {
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
func AdjustYear(y int64, adjustZero bool) (int64, error) {
	if y == 0 && !adjustZero {
		return y, nil
	}
	y = int64(adjustYear(int(y)))
	if y < 0 {
		return 0, errors.Trace(ErrWarnDataOutOfRange)
	}
	if y < int64(MinYear) {
		return int64(MinYear), errors.Trace(ErrWarnDataOutOfRange)
	}
	if y > int64(MaxYear) {
		return int64(MaxYear), errors.Trace(ErrWarnDataOutOfRange)
	}

	return y, nil
}
