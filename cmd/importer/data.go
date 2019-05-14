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

package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/cznic/mathutil"
)

var defaultStep int64 = 1

type datum struct {
	sync.Mutex

	intValue    int64
	minIntValue int64
	maxIntValue int64
	timeValue   time.Time
	remains     uint64
	repeats     uint64
	step        int64

	init     bool
	useRange bool
}

func newDatum() *datum {
	return &datum{intValue: -1, step: 1, repeats: 1, remains: 1}
}

func (d *datum) setInitInt64Value(step int64, min int64, max int64) {
	d.Lock()
	defer d.Unlock()

	if d.init {
		return
	}

	d.step = step

	if min != -1 {
		d.minIntValue = min
		d.intValue = min
	}

	if min < max {
		d.maxIntValue = max
		d.useRange = true
	}

	d.init = true
}

func (d *datum) nextInt64() int64 {
	d.Lock()
	defer d.Unlock()

	if d.remains <= 0 {
		d.intValue += d.step
		d.remains = d.repeats
	}
	if d.useRange {
		d.intValue = mathutil.MinInt64(d.intValue, d.maxIntValue)
	}
	d.remains--
	return d.intValue
}

func (d *datum) nextFloat64() float64 {
	data := d.nextInt64()
	return float64(data)
}

func (d *datum) nextString(n int) string {
	data := d.nextInt64()

	var value []byte
	for ; ; n-- {
		if n == 0 {
			break
		}

		idx := data % int64(len(alphabet))
		data = data / int64(len(alphabet))

		value = append(value, alphabet[idx])

		if data == 0 {
			break
		}
	}

	for i, j := 0, len(value)-1; i < j; i, j = i+1, j-1 {
		value[i], value[j] = value[j], value[i]
	}

	return string(value)
}

func (d *datum) nextTime() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else if d.remains <= 0 {
		d.timeValue = d.timeValue.Add(time.Duration(d.step) * time.Second)
		d.remains = d.repeats
	}
	d.remains--
	return fmt.Sprintf("%02d:%02d:%02d", d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextDate() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else if d.remains <= 0 {
		d.timeValue = d.timeValue.AddDate(0, 0, int(d.step))
		d.remains = d.repeats
	}
	d.remains--
	return fmt.Sprintf("%04d-%02d-%02d", d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day())
}

func (d *datum) nextTimestamp() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else if d.remains <= 0 {
		d.timeValue = d.timeValue.Add(time.Duration(d.step) * time.Second)
	}
	d.remains--
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day(),
		d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextYear() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else if d.remains <= 0 {
		d.timeValue = d.timeValue.AddDate(int(d.step), 0, 0)
		d.remains = d.repeats
	}
	d.remains--
	return fmt.Sprintf("%04d", d.timeValue.Year())
}
