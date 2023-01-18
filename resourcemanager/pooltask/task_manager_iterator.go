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
	"container/list"
	"time"
)

var startTime = time.Now()

func (t *TaskManager[T, U, C, CT, TF]) getBoostTask() (tid uint64, result *TaskBox[T, U, C, CT, TF]) {
	// boost task
	// 1、the count of running task is less than concurrency
	// 2、less run time, more possible to boost
	var minTS = startTime
	for i := 0; i < shard; i++ {
		isBreak := func(index int) bool {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				if result == nil {
					result = findTask[T, U, C, CT, TF](stats, RunningTask).Value.(tContainer[T, U, C, CT, TF]).task
					minTS = stats.createTS
					tid = id
					continue
				}
				newResult, min, breakFind := canBoost[T, U, C, CT, TF](stats, minTS)
				if breakFind {
					result = newResult
					tid = id
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

func (t *TaskManager[T, U, C, CT, TF]) pauseTask() {
	// pause task,
	// 1、more run time, more possible to pause
	// 2、if task have been boosted, first to pause.
	var maxTS time.Time
	var tid uint64
	var result *list.Element
	for i := 0; i < shard; i++ {
		isBoost := func(index int) (isBoost bool) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				if result == nil {
					result = findTask[T, U, C, CT, TF](stats, RunningTask)
					tid = id
					maxTS = stats.createTS
					continue
				}
				newResult, isBoost := canPause[T, U, C, CT, TF](stats, maxTS)
				if isBoost {
					result = newResult
					tid = id
					return true
				}
				if newResult != nil {
					result = newResult
					tid = id
					maxTS = stats.createTS
				}
			}
			return false
		}(i)
		if isBoost {
			break
		}
	}
	if result != nil {
		result.Value.(tContainer[T, U, C, CT, TF]).task.status.CompareAndSwap(RunningTask, StopTask)
		// delete it from list
		shardID := getShardID(tid)
		t.task[shardID].rw.Lock()
		defer t.task[shardID].rw.Unlock()
		t.task[shardID].stats[tid].stats.Remove(result)
	}
}

func canPause[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF], min time.Time) (result *list.Element, isBoost bool) {
	if m.origin < m.running.Load() {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, true
		}
	}
	if m.createTS.Before(min) {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, false
		}
	}
	return nil, false
}

func canBoost[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF], min time.Time) (*TaskBox[T, U, C, CT, TF], time.Time, bool) {
	if m.running.Load() < m.origin {
		return nil, m.createTS, true
	}
	if m.createTS.After(min) {
		box := getTask[T, U, C, CT, TF](m)
		if box != nil {
			return box, m.createTS, false
		}
	}
	return nil, startTime, false
}

func findTask[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF], status int32) *list.Element {
	for e := m.stats.Front(); e != nil; e = e.Next() {
		box := e.Value.(tContainer[T, U, C, CT, TF])
		if box.task.status.Load() == status {
			return e
		}
	}
	return nil
}

func getTask[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF]) *TaskBox[T, U, C, CT, TF] {
	for e := m.stats.Front(); e != nil; e = e.Next() {
		if box, ok := e.Value.(tContainer[T, U, C, CT, TF]); ok {
			return box.task
		}
	}
	return nil
}
