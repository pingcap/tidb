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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// #nosec G404

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	alphabet       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	yearFormat     = "2006"
	dateFormat     = time.DateOnly
	timeFormat     = time.TimeOnly
	dateTimeFormat = time.DateTime

	// Used by randString
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randInt(minv, maxv int) int {
	return minv + rand.Intn(maxv-minv+1)
}

func randInt64(minv, maxv int64) int64 {
	return minv + rand.Int63n(maxv-minv+1)
}

// reference: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(alphabet) {
			b[i] = alphabet[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func randDate(col *column) string {
	if col.hist != nil {
		return col.hist.randDate("DAY", "%Y-%m-%d", dateFormat)
	}

	minv, maxv := col.min, col.max
	if minv == "" {
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}

	minTime, err := time.Parse(dateFormat, minv)
	if err != nil {
		log.Warn("parse min date failed", zap.Error(err))
	}
	if maxv == "" {
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour) // nolint: durationcheck
		return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	}

	maxTime, err := time.Parse(dateFormat, maxv)
	if err != nil {
		log.Warn("parse max date failed", zap.Error(err))
	}
	days := int(maxTime.Sub(minTime).Hours() / 24)
	t := minTime.Add(time.Duration(randInt(0, days)) * 24 * time.Hour) // nolint: durationcheck
	return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
}

func randTime(col *column) string {
	if col.hist != nil {
		return col.hist.randDate("SECOND", "%H:%i:%s", timeFormat)
	}
	minv, maxv := col.min, col.max
	if minv == "" || maxv == "" {
		hour := randInt(0, 23)
		minute := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%02d:%02d:%02d", hour, minute, sec)
	}

	minTime, err := time.Parse(timeFormat, minv)
	if err != nil {
		log.Warn("parse minv time failed", zap.Error(err))
	}
	maxTime, err := time.Parse(timeFormat, maxv)
	if err != nil {
		log.Warn("parse maxv time failed", zap.Error(err))
	}
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

func randTimestamp(col *column) string {
	if col.hist != nil {
		return col.hist.randDate("SECOND", "%Y-%m-%d %H:%i:%s", dateTimeFormat)
	}
	minv, maxv := col.min, col.max
	if minv == "" {
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		hour := randInt(0, 23)
		minute := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, sec)
	}

	minTime, err := time.Parse(dateTimeFormat, minv)
	if err != nil {
		log.Warn("parse minv timestamp failed", zap.Error(err))
	}
	if maxv == "" {
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour) // nolint: durationcheck
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	maxTime, err := time.Parse(dateTimeFormat, maxv)
	if err != nil {
		log.Warn("parse maxv timestamp failed", zap.Error(err))
	}
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func randYear(col *column) string {
	if col.hist != nil {
		return col.hist.randDate("YEAR", "%Y", yearFormat)
	}
	minv, maxv := col.min, col.max
	if minv == "" || maxv == "" {
		return fmt.Sprintf("%04d", time.Now().Year()-randInt(0, 10))
	}

	minTime, err := time.Parse(yearFormat, minv)
	if err != nil {
		log.Warn("parse minv year failed", zap.Error(err))
	}
	maxTime, err := time.Parse(yearFormat, maxv)
	if err != nil {
		log.Warn("parse maxv year failed", zap.Error(err))
	}
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d", t.Year())
}
