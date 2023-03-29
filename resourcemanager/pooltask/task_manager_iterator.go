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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pooltask

import (
	"container/list"
	"time"
)

func (t *TaskManager[T, U, C, CT, TF]) getBoostTask() (tid uint64, result *TaskBox[T, U, C, CT, TF]) {
	// boost task
	// 1、the count of running task is less than concurrency
	// 2、less run time, more possible to boost
	tid, element := t.iter(canBoost[T, U, C, CT, TF])
	if element != nil {
		return tid, element.Value.(tContainer[T, U, C, CT, TF]).task
	}
	return 0, nil
}

func (t *TaskManager[T, U, C, CT, TF]) pauseTask() {
	// pause task,
	// 1、more run time, more possible to pause
	// 2、if task have been boosted, first to pause.
	tid, result := t.iter(canPause[T, U, C, CT, TF])
	if result != nil {
		result.Value.(tContainer[T, U, C, CT, TF]).task.status.CompareAndSwap(RunningTask, StopTask)
		// delete it from list
		shardID := getShardID(tid)
		t.task[shardID].rw.Lock()
		defer t.task[shardID].rw.Unlock()
		t.task[shardID].stats[tid].stats.Remove(result)
	}
}

func (t *TaskManager[T, U, C, CT, TF]) iter(fn func(m *meta[T, U, C, CT, TF], max time.Time) (*list.Element, bool)) (tid uint64, result *list.Element) {
	var compareTS time.Time
	for i := 0; i < shard; i++ {
		breakFind := func(index int) (breakFind bool) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				if result == nil {
					result = findTask[T, U, C, CT, TF](stats, RunningTask)
					tid = id
					compareTS = stats.createTS
					continue
				}
				newResult, pauseFind := fn(stats, compareTS)
				if pauseFind {
					result = newResult
					tid = id
					compareTS = stats.createTS
					return true
				}
				if newResult != nil {
					result = newResult
					tid = id
					compareTS = stats.createTS
				}
			}
			return false
		}(shard)
		if breakFind {
			break
		}
	}
	return tid, result
}

func canPause[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF], max time.Time) (result *list.Element, isBreak bool) {
	if m.initialConcurrency < m.running.Load() {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, true
		}
	}
	if m.createTS.Before(max) {
		box := findTask[T, U, C, CT, TF](m, RunningTask)
		if box != nil {
			return box, false
		}
	}
	return nil, false
}

func canBoost[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF], min time.Time) (result *list.Element, isBreak bool) {
	if m.running.Load() < m.initialConcurrency {
		box := getTask[T, U, C, CT, TF](m)
		if box != nil {
			return box, true
		}
	}
	if m.createTS.After(min) {
		box := getTask[T, U, C, CT, TF](m)
		if box != nil {
			return box, false
		}
	}
	return nil, false
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

func getTask[T any, U any, C any, CT any, TF Context[CT]](m *meta[T, U, C, CT, TF]) *list.Element {
	e := m.stats.Front()
	if e != nil {
		return e
	}
	return nil
}
