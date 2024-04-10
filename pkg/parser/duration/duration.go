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

func readFloat(s string) (float64, string, error) {
	numbers := ""
	for pos, ch := range s {
		if !unicode.IsDigit(ch) && ch != '.' {
			numbers = s[:pos]
			break
		}
	}
	if len(numbers) > 0 {
		i, err := strconv.ParseFloat(numbers, 64)
		if err != nil {
			return 0, s, err
		}
		return i, s[len(numbers):], nil
	}
	return 0, s, errors.New("fail to read an integer")
}

// ParseDuration parses the duration which contains 'd', 'h' and 'm'
func ParseDuration(s string) (time.Duration, error) {
	duration := time.Duration(0)

	if s == "0" {
		return 0, nil
	}

	var err error
	var i float64
	for len(s) > 0 {
		i, s, err = readFloat(s)
		if err != nil {
			return 0, err
		}
		switch s[0] {
		case 'd':
			duration += time.Duration(i * float64(time.Hour*24))
		case 'h':
			duration += time.Duration(i * float64(time.Hour))
		case 'm':
			duration += time.Duration(i * float64(time.Minute))
		default:
			return 0, errors.Errorf("unknown unit %c", s[0])
		}

		s = s[1:]
	}

	return duration, nil
}
