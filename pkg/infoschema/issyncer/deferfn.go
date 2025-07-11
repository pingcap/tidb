// Copyright 2025 PingCAP, Inc.
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

package issyncer

import (
	"sync"
	"time"
)

type deferFn struct {
	sync.Mutex
	data []deferFnRecord
}

type deferFnRecord struct {
	fn   func()
	fire time.Time
}

func (df *deferFn) add(fn func(), fire time.Time) {
	df.Lock()
	defer df.Unlock()
	df.data = append(df.data, deferFnRecord{fn: fn, fire: fire})
}

func (df *deferFn) check() {
	now := time.Now()
	df.Lock()
	defer df.Unlock()

	// iterate the slice, call the defer function and remove it.
	rm := 0
	for i := range df.data {
		record := &df.data[i]
		if now.After(record.fire) {
			record.fn()
			rm++
		} else {
			df.data[i-rm] = df.data[i]
		}
	}
	df.data = df.data[:len(df.data)-rm]
}
