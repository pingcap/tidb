// Copyright 2016 PingCAP, Inc.
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

package types

import (
	gotime "time"

	"github.com/juju/errors"
)

type mysqlTime struct {
	year        uint16 // year <= 9999
	month       uint8  // month <= 12
	day         uint8  // day <= 31
	hour        uint8  // hour <= 23
	minute      uint8  // minute <= 59
	second      uint8  // second <= 59
	microsecond uint32
}

func (t mysqlTime) Year() int {
	return int(t.year)
}

func (t mysqlTime) Month() int {
	return int(t.month)
}

func (t mysqlTime) Day() int {
	return int(t.day)
}

func (t mysqlTime) Hour() int {
	return int(t.hour)
}

func (t mysqlTime) Minute() int {
	return int(t.minute)
}

func (t mysqlTime) Second() int {
	return int(t.second)
}

func (t mysqlTime) Microsecond() int {
	return int(t.microsecond)
}

func (t mysqlTime) Weekday() gotime.Weekday {
	t1, err := t.GoTime()
	if err != nil {
		// TODO: Fix here.
		return 0
	}
	return t1.Weekday()
}

func (t mysqlTime) YearDay() int {
	t1, err := t.GoTime()
	if err != nil {
		// TODO: Fix here.
		return 0
	}
	return t1.YearDay()
}

func (t mysqlTime) ISOWeek() (int, int) {
	t1, err := t.GoTime()
	if err != nil {
		// TODO: Fix here.
		return 0, 0
	}
	return t1.ISOWeek()
}

func (t mysqlTime) GoTime() (gotime.Time, error) {
	// gotime.Time can't represent month 0 or day 0, date contains 0 would be converted to a nearest date,
	// For example, 2006-12-00 00:00:00 would become 2015-11-30 23:59:59.
	tm := gotime.Date(t.Year(), gotime.Month(t.Month()), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Microsecond()*1000, gotime.Local)
	year, month, day := tm.Date()
	hour, minute, second := tm.Clock()
	microsec := tm.Nanosecond() / 1000
	// This function will check the result, and return an error if it's not the same with the origin input.
	if year != t.Year() || int(month) != t.Month() || day != t.Day() ||
		hour != t.Hour() || minute != t.Minute() || second != t.Second() ||
		microsec != t.Microsecond() {
		return tm, errors.Trace(ErrInvalidTimeFormat)
	}
	return tm, nil
}

func newMysqlTime(year, month, day, hour, minute, second, microsecond int) mysqlTime {
	return mysqlTime{
		uint16(year),
		uint8(month),
		uint8(day),
		uint8(hour),
		uint8(minute),
		uint8(second),
		uint32(microsecond),
	}
}
