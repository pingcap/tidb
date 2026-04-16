package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

const (
	intervalMonthsPerYear = int64(12)
	intervalSecondsPerDay = int64(24 * 60 * 60)
)

func validateIntervalYearToMonthTotalMonths(totalMonths int64, p int) bool {
	return validateInterval(totalMonths, intervalMonthsPerYear, p)
}

func validateIntervalDayToSecondTotalSeconds(totalSeconds int64, p int) bool {
	return validateInterval(totalSeconds, intervalSecondsPerDay, p)
}

func validateInterval(v, unit int64, p int) bool {
	if p < 1 || p > 9 {
		return false
	}
	return (mathutil.Abs(v) / unit) < int64(powers10[p])
}

// ConvertIntervalStringToInt converts a interval string to int(total months or seconds ).
func ConvertIntervalStringToInt(s string, subType byte, flen int) (int64, error) {
	switch subType {
	case mysql.SubTypeIntervalYearToMonth:
		return convertIntervalYearToMonthStringToInt(s, flen)
	case mysql.SubTypeIntervalDayToSecond:
		return convertIntervalDayToSecondStringToInt(s, flen)
	default:
		return 0, errors.New("unexpected subType: " + strconv.Itoa(int(subType)))
	}
}

func convertIntervalYearToMonthStringToInt(s string, p int) (int64, error) {
	str := strings.TrimSpace(s)
	if len(str) == 0 {
		return 0, errors.Trace(ErrInvalidIntervalValue)
	}
	year, month, neg, ok := parseIntervalYearToMonthStrict(str, p)
	if !ok {
		return 0, errors.Trace(ErrInvalidIntervalValue)
	}
	totalMonths := year*intervalMonthsPerYear + month
	if neg && totalMonths != 0 {
		totalMonths = -totalMonths
	}
	return totalMonths, nil
}

func convertIntervalDayToSecondStringToInt(s string, p int) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, errors.Trace(ErrInvalidIntervalValue)
	}
	day, hour, minute, second, neg, ok := parseIntervalDayToSecondStrict(s, p)
	if !ok {
		return 0, errors.Trace(ErrInvalidIntervalValue)
	}
	totalSeconds := day*86400 + hour*3600 + minute*60 + second
	if neg && totalSeconds != 0 {
		totalSeconds = -totalSeconds
	}
	return totalSeconds, nil
}

func skipIntervalASCIISpaces(s string, i, n int) int {
	for i < n {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func scanIntervalUnsignedInt(s string, i, n int, maxDigits int) (val int64, next int, ok bool) {
	digits := 0
	for i < n {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		if digits >= maxDigits {
			return 0, i, false
		}
		val = val*10 + int64(c-'0')
		digits++
		i++
	}
	if digits == 0 {
		return 0, i, false
	}
	return val, i, true
}

// parseIntervalYearToMonthStrict parses a trimmed string s in the format:
//
//	([+-])?<yearDigits>[space]*-[space]*<monthDigits>[space]*
//
// It enforces:
// - yearDigits has 1..p digits
// - monthDigits has 1..2 digits and its numeric value is 0..11
func parseIntervalYearToMonthStrict(s string, p int) (year int64, month int64, neg bool, ok bool) {
	i, n := 0, len(s)
	if n == 0 {
		return
	}

	switch s[0] {
	case '+':
		i++
	case '-':
		neg = true
		i++
	}
	if i >= n {
		return
	}

	scanOK := false
	year, i, scanOK = scanIntervalUnsignedInt(s, i, n, p)
	if !scanOK {
		return
	}

	i = skipIntervalASCIISpaces(s, i, n)
	if i >= n || s[i] != '-' {
		return
	}
	i++
	i = skipIntervalASCIISpaces(s, i, n)

	month, i, scanOK = scanIntervalUnsignedInt(s, i, n, 2)
	if !scanOK {
		return
	}
	if month > 11 {
		return
	}

	i = skipIntervalASCIISpaces(s, i, n)
	if i != n {
		return
	}

	ok = true
	return
}

// parseIntervalDayToSecondStrict parses a trimmed string s in the format:
//
//	([+-])?<dayDigits>[space]+<hourDigits>[space]*:[space]*<minuteDigits>[space]*:[space]*<secondDigits>[space]*
//
// It enforces:
// - dayDigits has 1..p digits
// - hourDigits has 1..2 digits and its numeric value is 0..23
// - minuteDigits/secondDigits have 1..2 digits and their numeric values are 0..59
// - no fractional seconds and no extra trailing characters (except ASCII spaces)
func parseIntervalDayToSecondStrict(s string, p int) (day, hour, minute, second int64, neg bool, ok bool) {
	i, n := 0, len(s)
	if n == 0 {
		return
	}

	switch s[0] {
	case '+':
		i++
	case '-':
		neg = true
		i++
	}
	if i >= n {
		return
	}

	scanOK := false
	day, i, scanOK = scanIntervalUnsignedInt(s, i, n, p)
	if !scanOK {
		return
	}

	// Require at least one ASCII whitespace between <day> and the time part.
	if i >= n {
		return
	}
	switch s[i] {
	case ' ', '\t', '\n', '\r':
		// OK.
	default:
		return
	}
	i = skipIntervalASCIISpaces(s, i, n)
	if i >= n {
		return
	}

	hour, i, scanOK = scanIntervalUnsignedInt(s, i, n, 2)
	if !scanOK || hour > 23 {
		return
	}

	i = skipIntervalASCIISpaces(s, i, n)
	if i >= n || s[i] != ':' {
		return
	}
	i++
	i = skipIntervalASCIISpaces(s, i, n)

	minute, i, scanOK = scanIntervalUnsignedInt(s, i, n, 2)
	if !scanOK || minute > 59 {
		return
	}

	i = skipIntervalASCIISpaces(s, i, n)
	if i >= n || s[i] != ':' {
		return
	}
	i++
	i = skipIntervalASCIISpaces(s, i, n)

	second, i, scanOK = scanIntervalUnsignedInt(s, i, n, 2)
	if !scanOK || second > 59 {
		return
	}

	i = skipIntervalASCIISpaces(s, i, n)
	if i != n {
		return
	}

	ok = true
	return
}

// FormatIntervalYearToMonth formats the canonical `INTERVAL YEAR TO MONTH` text representation
// for the given total-months value.
//
// The storage value is the total number of months (e.g. "2-11" => 35).
// The formatted form is "<sign?><years>-<months>", where months is always in [0, 11].
func FormatIntervalYearToMonth(totalMonths int64) string {
	neg := totalMonths < 0
	sign := ""
	if neg {
		sign = "-"
		totalMonths = -totalMonths
	}
	years := totalMonths / intervalMonthsPerYear
	months := totalMonths % intervalMonthsPerYear
	return fmt.Sprintf("%s%d-%d", sign, years, months)
}

// FormatIntervalDayToSecond formats the canonical `INTERVAL DAY TO SECOND` text representation
// for the given total-seconds value.
//
// The storage value is the total number of seconds (e.g. "2 03:04:05" => 183845).
// The formatted form is "<sign?><days> <HH>:<MM>:<SS>", where HH/MM/SS are 2-digit and
// always in valid ranges.
func FormatIntervalDayToSecond(totalSeconds int64) string {
	neg := totalSeconds < 0
	sign := ""
	if neg {
		sign = "-"
		totalSeconds = -totalSeconds
	}
	day := totalSeconds / intervalSecondsPerDay
	rem := totalSeconds % intervalSecondsPerDay
	hour := rem / 3600
	rem %= 3600
	minute := rem / 60
	second := rem % 60
	return fmt.Sprintf("%s%d %02d:%02d:%02d", sign, day, hour, minute, second)
}
