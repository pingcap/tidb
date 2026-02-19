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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Time format without fractional seconds precision.
const (
	DateFormat = gotime.DateOnly
	TimeFormat = gotime.DateTime
	// TimeFSPFormat is time format with fractional seconds precision.
	TimeFSPFormat = "2006-01-02 15:04:05.000000"
	// UTCTimeFormat is used to parse and format gotime.
	UTCTimeFormat = "2006-01-02 15:04:05 UTC"
)

const (
	// MinYear is the minimum for mysql year type.
	MinYear int16 = 1901
	// MaxYear is the maximum for mysql year type.
	MaxYear int16 = 2155
	// MaxDuration is the maximum for duration.
	MaxDuration int64 = 838*10000 + 59*100 + 59
	// MinTime is the minimum for mysql time type.
	MinTime = -(838*gotime.Hour + 59*gotime.Minute + 59*gotime.Second)
	// MaxTime is the maximum for mysql time type.
	MaxTime = 838*gotime.Hour + 59*gotime.Minute + 59*gotime.Second
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

	// ZeroCoreTime is the zero value for Time type.
	ZeroTime = Time{}

	// ZeroDatetime is the zero value for datetime Time.
	ZeroDatetime = NewTime(ZeroCoreTime, mysql.TypeDatetime, DefaultFsp)

	// ZeroTimestamp is the zero value for timestamp Time.
	ZeroTimestamp = NewTime(ZeroCoreTime, mysql.TypeTimestamp, DefaultFsp)

	// ZeroDate is the zero value for date Time.
	ZeroDate = NewTime(ZeroCoreTime, mysql.TypeDate, DefaultFsp)
)

var (
	// MinDatetime is the minimum for Golang Time type.
	MinDatetime = FromDate(1, 1, 1, 0, 0, 0, 0)
	// MaxDatetime is the maximum for mysql datetime type.
	MaxDatetime = FromDate(9999, 12, 31, 23, 59, 59, 999999)

	// BoundTimezone is the timezone for min and max timestamp.
	BoundTimezone = gotime.UTC
	// MinTimestamp is the minimum for mysql timestamp type.
	MinTimestamp = NewTime(FromDate(1970, 1, 1, 0, 0, 1, 0), mysql.TypeTimestamp, DefaultFsp)
	// MaxTimestamp is the maximum for mysql timestamp type.
	MaxTimestamp = NewTime(FromDate(2038, 1, 19, 3, 14, 7, 999999), mysql.TypeTimestamp, DefaultFsp)

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
func FromGoTime(t gotime.Time) CoreTime {
	// Plus 500 nanosecond for rounding of the millisecond part.
	t = t.Add(500 * gotime.Nanosecond)

	year, month, day := t.Date()
	hour, minute, second := t.Clock()
	microsecond := t.Nanosecond() / 1000
	return FromDate(year, int(month), day, hour, minute, second, microsecond)
}

// FromDate makes a internal time representation from the given date.
func FromDate(year int, month int, day int, hour int, minute int, second int, microsecond int) CoreTime {
	v := uint64(ZeroCoreTime)
	v |= (uint64(microsecond) << microsecondBitFieldOffset) & microsecondBitFieldMask
	v |= (uint64(second) << secondBitFieldOffset) & secondBitFieldMask
	v |= (uint64(minute) << minuteBitFieldOffset) & minuteBitFieldMask
	v |= (uint64(hour) << hourBitFieldOffset) & hourBitFieldMask
	v |= (uint64(day) << dayBitFieldOffset) & dayBitFieldMask
	v |= (uint64(month) << monthBitFieldOffset) & monthBitFieldMask
	v |= (uint64(year) << yearBitFieldOffset) & yearBitFieldMask
	return CoreTime(v)
}

// FromDateChecked makes a internal time representation from the given date with field overflow check.
func FromDateChecked(year, month, day, hour, minute, second, microsecond int) (CoreTime, bool) {
	if uint64(year) >= (1<<yearBitFieldWidth) ||
		uint64(month) >= (1<<monthBitFieldWidth) ||
		uint64(day) >= (1<<dayBitFieldWidth) ||
		uint64(hour) >= (1<<hourBitFieldWidth) ||
		uint64(minute) >= (1<<minuteBitFieldWidth) ||
		uint64(second) >= (1<<secondBitFieldWidth) ||
		uint64(microsecond) >= (1<<microsecondBitFieldWidth) {
		return ZeroCoreTime, false
	}
	return FromDate(year, month, day, hour, minute, second, microsecond), true
}

// coreTime is an alias to CoreTime, embedd in Time.
type coreTime = CoreTime

// Time is the struct for handling datetime, timestamp and date.
type Time struct {
	coreTime
}

// Clock returns the hour, minute, and second within the day specified by t.
func (t Time) Clock() (hour int, minute int, second int) {
	return t.Hour(), t.Minute(), t.Second()
}

const (
	// Core time bit fields.
	yearBitFieldOffset, yearBitFieldWidth               uint64 = 50, 14
	monthBitFieldOffset, monthBitFieldWidth             uint64 = 46, 4
	dayBitFieldOffset, dayBitFieldWidth                 uint64 = 41, 5
	hourBitFieldOffset, hourBitFieldWidth               uint64 = 36, 5
	minuteBitFieldOffset, minuteBitFieldWidth           uint64 = 30, 6
	secondBitFieldOffset, secondBitFieldWidth           uint64 = 24, 6
	microsecondBitFieldOffset, microsecondBitFieldWidth uint64 = 4, 20

	// fspTt bit field.
	// `fspTt` format:
	// | fsp: 3 bits | type: 1 bit |
	// When `fsp` is valid (in range [0, 6]):
	// 1. `type` bit 0 represent `DateTime`
	// 2. `type` bit 1 represent `Timestamp`
	//
	// Since s`Date` does not require `fsp`, we could use `fspTt` == 0b1110 to represent it.
	fspTtBitFieldOffset, fspTtBitFieldWidth uint64 = 0, 4

	yearBitFieldMask        uint64 = ((1 << yearBitFieldWidth) - 1) << yearBitFieldOffset
	monthBitFieldMask       uint64 = ((1 << monthBitFieldWidth) - 1) << monthBitFieldOffset
	dayBitFieldMask         uint64 = ((1 << dayBitFieldWidth) - 1) << dayBitFieldOffset
	hourBitFieldMask        uint64 = ((1 << hourBitFieldWidth) - 1) << hourBitFieldOffset
	minuteBitFieldMask      uint64 = ((1 << minuteBitFieldWidth) - 1) << minuteBitFieldOffset
	secondBitFieldMask      uint64 = ((1 << secondBitFieldWidth) - 1) << secondBitFieldOffset
	microsecondBitFieldMask uint64 = ((1 << microsecondBitFieldWidth) - 1) << microsecondBitFieldOffset
	fspTtBitFieldMask       uint64 = ((1 << fspTtBitFieldWidth) - 1) << fspTtBitFieldOffset

	fspTtForDate         uint   = 0b1110
	fspBitFieldMask      uint64 = 0b1110
	coreTimeBitFieldMask        = ^fspTtBitFieldMask
)

// NewTime constructs time from core time, type and fsp.
func NewTime(coreTime CoreTime, tp uint8, fsp int) Time {
	t := ZeroTime
	p := (*uint64)(&t.coreTime)
	*p |= uint64(coreTime) & coreTimeBitFieldMask
	if tp == mysql.TypeDate {
		*p |= uint64(fspTtForDate)
		return t
	}
	if fsp == UnspecifiedFsp {
		fsp = DefaultFsp
	}
	*p |= uint64(fsp) << 1
	if tp == mysql.TypeTimestamp {
		*p |= 1
	}
	return t
}

func (t Time) getFspTt() uint {
	return uint(uint64(t.coreTime) & fspTtBitFieldMask)
}

func (t *Time) setFspTt(fspTt uint) {
	*(*uint64)(&t.coreTime) &= ^(fspTtBitFieldMask)
	*(*uint64)(&t.coreTime) |= uint64(fspTt)
}

// Type returns type value.
func (t Time) Type() uint8 {
	if t.getFspTt() == fspTtForDate {
		return mysql.TypeDate
	}
	if uint64(t.coreTime)&1 == 1 {
		return mysql.TypeTimestamp
	}
	return mysql.TypeDatetime
}

// Fsp returns fsp value.
func (t Time) Fsp() int {
	fspTt := t.getFspTt()
	if fspTt == fspTtForDate {
		return 0
	}
	return int(fspTt >> 1)
}

// SetType updates the type in Time.
// Only DateTime/Date/Timestamp is valid.
func (t *Time) SetType(tp uint8) {
	fspTt := t.getFspTt()
	if fspTt == fspTtForDate && tp != mysql.TypeDate {
		fspTt = 0
	}
	switch tp {
	case mysql.TypeDate:
		fspTt = fspTtForDate
	case mysql.TypeTimestamp:
		fspTt |= 1
	case mysql.TypeDatetime:
		fspTt &= ^(uint(1))
	}
	t.setFspTt(fspTt)
}

// SetFsp updates the fsp in Time.
func (t *Time) SetFsp(fsp int) {
	if t.getFspTt() == fspTtForDate {
		return
	}
	if fsp == UnspecifiedFsp {
		fsp = DefaultFsp
	}
	*(*uint64)(&t.coreTime) &= ^(fspBitFieldMask)
	*(*uint64)(&t.coreTime) |= uint64(fsp) << 1
}

// CoreTime returns core time.
func (t Time) CoreTime() CoreTime {
	return CoreTime(uint64(t.coreTime) & coreTimeBitFieldMask)
}

// SetCoreTime updates core time.
func (t *Time) SetCoreTime(ct CoreTime) {
	*(*uint64)(&t.coreTime) &= ^coreTimeBitFieldMask
	*(*uint64)(&t.coreTime) |= uint64(ct) & coreTimeBitFieldMask
}

// CurrentTime returns current time with type tp.
func CurrentTime(tp uint8) Time {
	return NewTime(FromGoTime(gotime.Now()), tp, 0)
}

// ConvertTimeZone converts the time value from one timezone to another.
// The input time should be a valid timestamp.
func (t *Time) ConvertTimeZone(from, to *gotime.Location) error {
	if !t.IsZero() {
		raw, err := t.GoTime(from)
		if err != nil {
			return errors.Trace(err)
		}
		converted := raw.In(to)
		t.SetCoreTime(FromGoTime(converted))
	}
	return nil
}

func (t Time) String() string {
	if t.Type() == mysql.TypeDate {
		// We control the format, so no error would occur.
		str, err := t.DateFormat("%Y-%m-%d")
		terror.Log(errors.Trace(err))
		return str
	}

	str, err := t.DateFormat("%Y-%m-%d %H:%i:%s")
	terror.Log(errors.Trace(err))
	fsp := t.Fsp()
	if fsp > 0 {
		tmp := fmt.Sprintf(".%06d", t.Microsecond())
		str = str + tmp[:1+fsp]
	}

	return str
}

// IsZero returns a boolean indicating whether the time is equal to ZeroCoreTime.
func (t Time) IsZero() bool {
	return compareTime(t.coreTime, ZeroCoreTime) == 0
}

// InvalidZero returns a boolean indicating whether the month or day is zero.
// Several functions are strict when passed a DATE() function value as their argument and reject incomplete dates with a day part of zero:
// CONVERT_TZ(), DATE_ADD(), DATE_SUB(), DAYOFYEAR(), TIMESTAMPDIFF(),
// TO_DAYS(), TO_SECONDS(), WEEK(), WEEKDAY(), WEEKOFYEAR(), YEARWEEK().
// Mysql Doc: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html
func (t Time) InvalidZero() bool {
	return t.Month() == 0 || t.Day() == 0
}

const numberFormat = "%Y%m%d%H%i%s"
const dateFormat = "%Y%m%d"

// ToNumber returns a formatted number.
// e.g,
// 2012-12-12 -> 20121212
// 2012-12-12T10:10:10 -> 20121212101010
// 2012-12-12T10:10:10.123456 -> 20121212101010.123456
func (t Time) ToNumber() *MyDecimal {
	dec := new(MyDecimal)
	t.FillNumber(dec)
	return dec
}

// FillNumber is the same as ToNumber,
// but reuses input decimal instead of allocating one.
func (t Time) FillNumber(dec *MyDecimal) {
	if t.IsZero() {
		dec.FromInt(0)
		return
	}

	// Fix issue #1046
	// Prevents from converting 2012-12-12 to 20121212000000
	var tfStr string
	if t.Type() == mysql.TypeDate {
		tfStr = dateFormat
	} else {
		tfStr = numberFormat
	}

	s, err := t.DateFormat(tfStr)
	if err != nil {
		logutil.BgLogger().Error("never happen because we've control the format!", zap.String("category", "fatal"))
	}

	fsp := t.Fsp()
	if fsp > 0 {
		s1 := fmt.Sprintf("%s.%06d", s, t.Microsecond())
		s = s1[:len(s)+fsp+1]
	}
	// We skip checking error here because time formatted string can be parsed certainly.
	err = dec.FromString([]byte(s))
	terror.Log(errors.Trace(err))
}

// Convert converts t with type tp.
func (t Time) Convert(ctx Context, tp uint8) (Time, error) {
	t1 := t
	if t.Type() == tp || t.IsZero() {
		t1.SetType(tp)
		return t1, nil
	}

	t1.SetType(tp)
	err := t1.Check(ctx)
	if tp == mysql.TypeTimestamp && ErrTimestampInDSTTransition.Equal(err) {
		tAdj, adjErr := t1.AdjustedGoTime(ctx.Location())
		if adjErr == nil {
			ctx.AppendWarning(err)
			return Time{FromGoTime(tAdj)}, nil
		}
	}
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
	frac := t.Microsecond() * 1000

	d := gotime.Duration(hour*3600+minute*60+second)*gotime.Second + gotime.Duration(frac) //nolint:durationcheck
	// TODO: check convert validation
	return Duration{Duration: d, Fsp: t.Fsp()}, nil
}

// Compare returns an integer comparing the time instant t to o.
// If t is after o, returns 1, equal o, returns 0, before o, returns -1.
func (t Time) Compare(o Time) int {
	return compareTime(t.coreTime, o.coreTime)
}

// CompareString is like Compare,
// but parses string to Time then compares.
func (t Time) CompareString(ctx Context, str string) (int, error) {
	// use MaxFsp to parse the string
	o, err := ParseTime(ctx, str, t.Type(), MaxFsp)
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

