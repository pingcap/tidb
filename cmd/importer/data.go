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

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/mathutil"
)

type datum struct {
	sync.Mutex

	intValue    int64
	minIntValue int64
	maxIntValue int64
	timeValue   time.Time
	remains     uint64
	repeats     uint64
	step        int64
	probability uint32

	init     bool
	useRange bool
}

func newDatum() *datum {
	return &datum{step: 1, repeats: 1, remains: 1, probability: 100}
}

func (d *datum) setInitInt64Value(min int64, max int64) {
	d.Lock()
	defer d.Unlock()

	if d.init {
		return
	}

	d.minIntValue = min
	d.maxIntValue = max
	d.useRange = true
	if d.step < 0 {
		d.intValue = (min + max) / 2
	}

	d.init = true
}

// #nosec G404
func (d *datum) updateRemains() {
	if uint32(rand.Int31n(100))+1 <= 100-d.probability {
		d.remains -= uint64(rand.Int63n(int64(d.remains))) + 1
	} else {
		d.remains--
	}
}

func (d *datum) nextInt64() int64 {
	d.Lock()
	defer d.Unlock()

	if d.useRange {
		d.intValue = mathutil.Min(d.intValue, d.maxIntValue)
		d.intValue = mathutil.Max(d.intValue, d.minIntValue)
	}
	d.updateRemains()
	return d.intValue
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
	}
	d.updateRemains()
	return fmt.Sprintf("%02d:%02d:%02d", d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextDate() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	}
	d.updateRemains()
	return fmt.Sprintf("%04d-%02d-%02d", d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day())
}

func (d *datum) nextTimestamp() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	}
	d.updateRemains()
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day(),
		d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) nextYear() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	}
	d.updateRemains()
	return fmt.Sprintf("%04d", d.timeValue.Year())
}
