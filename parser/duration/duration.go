// Copyright 2023 PingCAP, Inc.
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

// Package duration provides a customized duration, which supports unit 'd', 'h' and 'm'
package duration

import (
	"strconv"
	"time"
	"unicode"

	"github.com/pingcap/errors"
)

// Duration is a format which supports 'd', 'h' and 'm', but doesn't support other formats and floats
type Duration struct {
	Day    int `json:"day"`
	Hour   int `json:"hour"`
	Minute int `json:"minute"`
}

func readInt(s string) (int, string, error) {
	numbers := ""
	for pos, ch := range s {
		if !unicode.IsDigit(ch) {
			numbers = s[:pos]
			break
		}
	}
	if len(numbers) > 0 {
		i, err := strconv.Atoi(numbers)
		if err != nil {
			return 0, s, err
		}
		return i, s[len(numbers):], nil
	}
	return 0, s, errors.New("fail to read an integer")
}

// ParseDuration parses the duration which contains 'd', 'h' and 'm'
func ParseDuration(s string) (Duration, error) {
	duration := Duration{}

	if s == "0" {
		return Duration{}, nil
	}

	var err error
	i := 0
	for len(s) > 0 {
		i, s, err = readInt(s)
		if err != nil {
			return Duration{}, err
		}
		switch s[0] {
		case 'd':
			duration.Day += i
		case 'h':
			duration.Hour += i
		case 'm':
			duration.Minute += i
		default:
			return Duration{}, errors.Errorf("unknown unit %c", s[0])
		}

		s = s[1:]
	}

	return duration.reorg(), nil
}

func (d Duration) reorg() Duration {
	if d.Minute >= 60 {
		d.Hour += d.Minute / 60
		d.Minute = d.Minute % 60
	}
	if d.Hour >= 24 {
		d.Day += d.Hour / 24
		d.Hour = d.Hour % 24
	}

	return d
}

// String formats the duration
func (d Duration) String() string {
	d = d.reorg()

	str := ""
	if d.Day > 0 {
		str += strconv.Itoa(d.Day) + "d"
	}
	if d.Hour > 0 {
		str += strconv.Itoa(d.Hour) + "h"
	}
	if d.Minute > 0 {
		str += strconv.Itoa(d.Minute) + "m"
	}

	if len(str) == 0 {
		return "0m"
	}
	return str
}

// GoDuration returns the equal time.Duration
func (d Duration) GoDuration() time.Duration {
	return time.Hour*time.Duration(d.Day*24+d.Hour) + time.Minute*time.Duration(d.Minute)
}
