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

func (t *TaskManager[T, U, C, CT, TF]) getNeedToBoostTask() (tid uint64, result *TaskBox[T, U, C, CT, TF]) {
	// boost pooltask,
	// 1、less run time, more possible to boost
	var mints = time.Now()
	for i := 0; i < shard; i++ {
		func(index int) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				if stats.createTs.Before(mints) {
					mints = stats.createTs
					if tmp, ok := stats.stats.Front().Value.(*TaskBox[T, U, C, CT, TF]); ok {
						result = tmp.Clone()
						tid = id
					}
				}
			}
		}(i)
	}
	return tid, result
}

func (t *TaskManager[T, U, C, CT, TF]) pauseTask() {
	// boost pooltask,
	// 1、more run time, more possible to boost
	var maxDuration time.Duration
	var result *TaskBox[T, U, C, CT, TF]
	for i := 0; i < shard; i++ {
		func(index int) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			// TODO: for-if-for-if-if is so dirty
			for _, stats := range t.task[i].stats {
				newResult, newMaxDuration, IsnUll := canPause[T, U, C, CT, TF](stats, maxDuration)
				if !IsnUll {
					result = newResult
					maxDuration = newMaxDuration
				}
			}
		}(i)
	}
	if result != nil {
		result.status.CompareAndSwap(RunningTask, PendingTask)
	}
}

func canPause[T any, U any, C any, CT any, TF Context[CT]](m *meta, max time.Duration) (result *TaskBox[T, U, C, CT, TF], nm time.Duration, isNull bool) {
	d := time.Since(m.createTs)
	if d > max {
		for e := m.stats.Front(); e != nil; e = e.Next() {
			if box, ok := e.Value.(*TaskBox[T, U, C, CT, TF]); ok {
				if box.status.Load() == RunningTask {
					return box, d, false
				}
			}
		}
	}
	return result, nm, true
}
