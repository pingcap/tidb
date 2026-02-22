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

