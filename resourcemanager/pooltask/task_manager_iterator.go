// Copyright 2022 PingCAP, Inc.
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

package pooltask

import (
	"time"
)

var startTime = time.Now()

func (t *TaskManager[T, U, C, CT, TF]) getNeedToBoostTask() (tid uint64, result *TaskBox[T, U, C, CT, TF]) {
	// boost task,
	// 1、less run time, more possible to boost
	// 2、the count of running task is less than concurrency
	var minTS = startTime
	for i := 0; i < shard; i++ {
		isBreak := func(index int) bool {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				newResult, min, breakFund := canBoost[T, U, C, CT, TF](stats, minTS)
				if breakFund {
					result = newResult // set nil
					return true
				}
				if newResult != nil {
					result = newResult
					minTS = min
					tid = id
				}
			}
			return false
		}(i)
		if isBreak {
			break
		}
	}
	return tid, result
}

func canBoost[T any, U any, C any, CT any, TF Context[CT]](m *meta, min time.Time) (*TaskBox[T, U, C, CT, TF], time.Time, bool) {

	// need to add
	if m.createTS.After(min) {
		box := getTask[T, U, C, CT, TF](m)
		if box != nil {
			return box, m.createTS, false
		}
	}
	return nil, startTime, false
}

func (t *TaskManager[T, U, C, CT, TF]) pauseTask() {
	// pause task,
	// 1、more run time, more possible to pause
	// 2、if task have been boosted, first to pause.
	var maxDuration time.Duration
	var result *TaskBox[T, U, C, CT, TF]
	for i := 0; i < shard; i++ {
		isBoost := func(index int) (isBoost bool) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for _, stats := range t.task[i].stats {
				newResult, newMaxDuration, isBoost := canPause[T, U, C, CT, TF](stats, maxDuration)
				if isBoost {
					result = newResult
					return true
				}
				if newResult != nil {
					result = newResult
					maxDuration = newMaxDuration
				}
			}
			return false
		}(i)
		if isBoost {
			break
		}
	}
	if result != nil {
		result.status.CompareAndSwap(RunningTask, StopTask)
	}
}

func canPause[T any, U any, C any, CT any, TF Context[CT]](m *meta, max time.Duration) (result *TaskBox[T, U, C, CT, TF], nm time.Duration, isBool bool) {
	if m.origin < m.running.Load() {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, nm, true
		}
	}
	d := time.Since(m.createTS)
	if d > max {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, nm, true
		}
	}
	return nil, nm, false
}

func findTask[T any, U any, C any, CT any, TF Context[CT]](m *meta, status int32) *TaskBox[T, U, C, CT, TF] {
	for e := m.stats.Front(); e != nil; e = e.Next() {
		if box, ok := e.Value.(*TaskBox[T, U, C, CT, TF]); ok {
			if box.status.Load() == status {
				return box
			}
		}
	}
	return nil
}

func getTask[T any, U any, C any, CT any, TF Context[CT]](m *meta) *TaskBox[T, U, C, CT, TF] {
	for e := m.stats.Front(); e != nil; e = e.Next() {
		if box, ok := e.Value.(*TaskBox[T, U, C, CT, TF]); ok {
			return box
		}
	}
	return nil
}
