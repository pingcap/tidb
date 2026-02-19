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
	gotime "time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)
var maxDaysInMonth = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func getTime(ctx Context, num, originNum int64, tp byte) (Time, error) {
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

	ct, ok := FromDateChecked(year, month, day, hour, minute, second, 0)
	if !ok {
		numStr := strconv.FormatInt(originNum, 10)
		return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, numStr))
	}
	t := NewTime(ct, tp, DefaultFsp)
	err := t.Check(ctx)
	return t, errors.Trace(err)
}

// parseDateTimeFromNum parses date time from num.
// See number_to_datetime function.
// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
func parseDateTimeFromNum(ctx Context, num int64) (Time, error) {
	t := ZeroDate
	// Check zero.
	if num == 0 {
		return t, nil
	}
	originNum := num

	// Check datetime type.
	if num >= 10000101000000 {
		t.SetType(mysql.TypeDatetime)
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check MMDD.
	if num < 101 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDD, year: 2000-2069
	if num <= (70-1)*10000+1231 {
		num = (num + 20000000) * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check YYMMDD.
	if num < 70*10000+101 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDD, year: 1970-1999
	if num <= 991231 {
		num = (num + 19000000) * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Adjust hour/min/second.
	if num <= 99991231 {
		num = num * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Set TypeDatetime type.
	t.SetType(mysql.TypeDatetime)

	// Adjust year
	// YYMMDDHHMMSS, 2000-2069
	if num <= 69*10000000000+1231235959 {
		num = num + 20000000000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check YYYYMMDDHHMMSS.
	if num < 70*10000000000+101000000 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDDHHMMSS, 1970-1999
	if num <= 991231235959 {
		num = num + 19000000000000
		return getTime(ctx, num, originNum, t.Type())
	}

	return getTime(ctx, num, originNum, t.Type())
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
// explicitTz is used to handle a data race of timeZone, refer to https://github.com/pingcap/tidb/issues/40710. It only works for timestamp now, be careful to use it!
func ParseTime(ctx Context, str string, tp byte, fsp int) (Time, error) {
	return parseTime(ctx, str, tp, fsp, false)
}

// ParseTimeFromFloatString is similar to ParseTime, except that it's used to parse a float converted string.
func ParseTimeFromFloatString(ctx Context, str string, tp byte, fsp int) (Time, error) {
	// MySQL compatibility: 0.0 should not be converted to null, see #11203
	if len(str) >= 3 && str[:3] == "0.0" {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), nil
	}
	return parseTime(ctx, str, tp, fsp, true)
}

func parseTime(ctx Context, str string, tp byte, fsp int, isFloat bool) (Time, error) {
	fsp, err := CheckFsp(fsp)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t, err := parseDatetime(ctx, str, fsp, isFloat)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t.SetType(tp)
	if err = t.Check(ctx); err != nil {
		if tp == mysql.TypeTimestamp && !t.IsZero() {
			tAdjusted, errAdjusted := adjustTimestampErrForDST(ctx.Location(), str, tp, t, err)
			if ErrTimestampInDSTTransition.Equal(errAdjusted) {
				return tAdjusted, errors.Trace(errAdjusted)
			}
		}
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}
	return t, nil
}

func adjustTimestampErrForDST(loc *gotime.Location, str string, tp byte, t Time, err error) (Time, error) {
	if tp != mysql.TypeTimestamp || t.IsZero() {
		return t, err
	}
	minTS, maxTS := MinTimestamp, MaxTimestamp
	minErr := minTS.ConvertTimeZone(gotime.UTC, loc)
	maxErr := maxTS.ConvertTimeZone(gotime.UTC, loc)
	if minErr == nil && maxErr == nil &&
		t.Compare(minTS) > 0 && t.Compare(maxTS) < 0 {
		// Handle the case when the timestamp given is in the DST transition
		if tAdjusted, err2 := t.AdjustedGoTime(loc); err2 == nil {
			t.SetCoreTime(FromGoTime(tAdjusted))
			return t, errors.Trace(ErrTimestampInDSTTransition.GenWithStackByArgs(str, loc.String()))
		}
	}
	return t, err
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(ctx Context, str string) (Time, error) {
	return ParseTime(ctx, str, mysql.TypeDatetime, GetFsp(str))
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(ctx Context, str string) (Time, error) {
	return ParseTime(ctx, str, mysql.TypeTimestamp, GetFsp(str))
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(ctx Context, str string) (Time, error) {
	// date has no fractional seconds precision
	return ParseTime(ctx, str, mysql.TypeDate, MinFsp)
}

// ParseTimeFromYear parse a `YYYY` formed year to corresponded Datetime type.
// Note: the invoker must promise the `year` is in the range [MinYear, MaxYear].
func ParseTimeFromYear(year int64) (Time, error) {
	if year == 0 {
		return NewTime(ZeroCoreTime, mysql.TypeDate, DefaultFsp), nil
	}

	dt := FromDate(int(year), 0, 0, 0, 0, 0, 0)
	return NewTime(dt, mysql.TypeDatetime, DefaultFsp), nil
}

// ParseTimeFromNum parses a formatted int64,
// returns the value which type is tp.
func ParseTimeFromNum(ctx Context, num int64, tp byte, fsp int) (Time, error) {
	// MySQL compatibility: 0 should not be converted to null, see #11203
	if num == 0 {
		zt := NewTime(ZeroCoreTime, tp, DefaultFsp)
		if !ctx.Flags().IgnoreZeroDateErr() {
			switch tp {
			case mysql.TypeTimestamp:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(TimestampStr, "0")
			case mysql.TypeDate:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(DateStr, "0")
			case mysql.TypeDatetime:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(DateTimeStr, "0")
			}
		}
		return zt, nil
	}
	fsp, err := CheckFsp(fsp)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t, err := parseDateTimeFromNum(ctx, num)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t.SetType(tp)
	t.SetFsp(fsp)
	if err := t.Check(ctx); err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}
	return t, nil
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(ctx Context, num int64) (Time, error) {
	return ParseTimeFromNum(ctx, num, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(ctx Context, num int64) (Time, error) {
	return ParseTimeFromNum(ctx, num, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(ctx Context, num int64) (Time, error) {
	// date has no fractional seconds precision
	return ParseTimeFromNum(ctx, num, mysql.TypeDate, MinFsp)
}

// TimeFromDays Converts a day number to a date.
func TimeFromDays(num int64) Time {
	if num < 0 {
		return NewTime(FromDate(0, 0, 0, 0, 0, 0, 0), mysql.TypeDate, 0)
	}
	year, month, day := getDateFromDaynr(uint(num))
	ct, ok := FromDateChecked(int(year), int(month), int(day), 0, 0, 0, 0)
	if !ok {
		return NewTime(FromDate(0, 0, 0, 0, 0, 0, 0), mysql.TypeDate, 0)
	}
	return NewTime(ct, mysql.TypeDate, 0)
}

func checkDateType(t CoreTime, allowZeroInDate, allowInvalidDate bool) error {
	year, month, day := t.Year(), t.Month(), t.Day()
	if year == 0 && month == 0 && day == 0 {
		return nil
	}

	if !allowZeroInDate && (month == 0 || day == 0) {
		return ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%04d-%02d-%02d", year, month, day))
	}

	if err := checkDateRange(t); err != nil {
		return errors.Trace(err)
	}

	if err := checkMonthDay(year, month, day, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDateRange(t CoreTime) error {
	// Oddly enough, MySQL document says date range should larger than '1000-01-01',
	// but we can insert '0001-01-01' actually.
	if t.Year() < 0 || t.Month() < 0 || t.Day() < 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}
	if compareTime(t, MaxDatetime) > 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}
	return nil
}

func checkMonthDay(year, month, day int, allowInvalidDate bool) error {
	if month < 0 || month > 12 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%d-%d-%d", year, month, day)))
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
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%d-%d-%d", year, month, day)))
	}
	return nil
}

func checkTimestampType(t CoreTime, tz *gotime.Location) error {
	if compareTime(t, ZeroCoreTime) == 0 {
		return nil
	}

	var checkTime CoreTime
	if tz != BoundTimezone {
		convertTime := NewTime(t, mysql.TypeTimestamp, DefaultFsp)
		err := convertTime.ConvertTimeZone(tz, BoundTimezone)
		if err != nil {
			_, err2 := adjustTimestampErrForDST(tz, t.String(), mysql.TypeTimestamp, Time{t}, err)
			return err2
		}
		checkTime = convertTime.coreTime
	} else {
		checkTime = t
	}
	if compareTime(checkTime, MaxTimestamp.coreTime) > 0 || compareTime(checkTime, MinTimestamp.coreTime) < 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}

	if _, err := t.GoTime(tz); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDatetimeType(t CoreTime, allowZeroInDate, allowInvalidDate bool) error {
	if err := checkDateType(t, allowZeroInDate, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	hour, minute, second := t.Hour(), t.Minute(), t.Second()
	if hour < 0 || hour >= 24 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(hour)))
	}
	if minute < 0 || minute >= 60 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(minute)))
	}
	if second < 0 || second >= 60 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(second)))
	}

	return nil
}

// ParseTimeFromInt64 parses mysql time value from int64.
func ParseTimeFromInt64(ctx Context, num int64) (Time, error) {
	return parseDateTimeFromNum(ctx, num)
}

// ParseTimeFromFloat64 parses mysql time value from float64.
// It is used in scenarios that distinguish date and datetime, e.g., date_add/sub() with first argument being real.
// For example, 20010203 parses to date (no HMS) and 20010203040506 parses to datetime (with HMS).
func ParseTimeFromFloat64(ctx Context, f float64) (Time, error) {
	intPart := int64(f)
	t, err := parseDateTimeFromNum(ctx, intPart)
	if err != nil {
		return ZeroTime, err
	}
	if t.Type() == mysql.TypeDatetime {
		// US part is only kept when the integral part is recognized as datetime.
		fracPart := uint32(math.Round((f - float64(intPart)) * 1000000.0))
		ct := t.CoreTime()
		ct.setMicrosecond(fracPart)
		t.SetCoreTime(ct)
	}
	return t, err
}

// ParseTimeFromDecimal parses mysql time value from decimal.
// It is used in scenarios that distinguish date and datetime, e.g., date_add/sub() with first argument being decimal.
// For example, 20010203 parses to date (no HMS) and 20010203040506 parses to datetime (with HMS).
func ParseTimeFromDecimal(ctx Context, dec *MyDecimal) (t Time, err error) {
	intPart, err := dec.ToInt()
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, err
	}
	fsp := min(MaxFsp, int(dec.GetDigitsFrac()))
	t, err = parseDateTimeFromNum(ctx, intPart)
	if err != nil {
		return ZeroTime, err
	}
	t.SetFsp(fsp)
	if fsp == 0 || t.Type() == mysql.TypeDate {
		// Shortcut for integer value or date value (fractional part omitted).
		return t, err
	}

	intPartDec := new(MyDecimal).FromInt(intPart)
	fracPartDec := new(MyDecimal)
	err = DecimalSub(dec, intPartDec, fracPartDec)
	if err != nil {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}
	million := new(MyDecimal).FromInt(1000000)
	msPartDec := new(MyDecimal)
	err = DecimalMul(fracPartDec, million, msPartDec)
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}
	msPart, err := msPartDec.ToInt()
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}

	ct := t.CoreTime()
	ct.setMicrosecond(uint32(msPart))
	t.SetCoreTime(ct)

	return t, nil
}
