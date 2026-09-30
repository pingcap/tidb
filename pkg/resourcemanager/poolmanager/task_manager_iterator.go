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

package poolmanager

import (
	"time"
)

func (t *TaskManager) getBoostTask() (tid uint64, result *Meta) {
	// boost task
	// 1、the count of running task is less than concurrency
	// 2、less run time, more possible to boost
	return t.iter(canBoost)
}

func (t *TaskManager) pauseTask() {
	// pause task,
	// 1、more run time, more possible to pause
	// 2、if task have been boosted, first to pause.
	_, result := t.iter(canPause)
	if result != nil {
		if result.exitCh != nil {
			select {
			case result.exitCh <- struct{}{}:
			default:
			}
		}
	}
}

func (t *TaskManager) iter(fn func(m *Meta, max time.Time) (bool, bool)) (tid uint64, result *Meta) {
	var compareTS time.Time
	for i := 0; i < shard; i++ {
		breakFind := func(int) (breakFind bool) {
			t.task[i].rw.RLock()
			defer t.task[i].rw.RUnlock()
			for id, stats := range t.task[i].stats {
				if result == nil {
					if stats.running.Load() != 0 {
						result = stats
					}
					tid = id
					compareTS = stats.createTS
					continue
				}
				isFind, pauseFind := fn(stats, compareTS)
				if isFind {
					tid = id
					result = stats
					compareTS = stats.createTS
				}
				if pauseFind {
					return true
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

func canPause(m *Meta, min time.Time) (find bool, toBreakFind bool) {
	if m.initialConcurrency < m.running.Load() {
		if m.running.Load() != 0 {
			return true, true
		}
	}
	if m.createTS.Before(min) {
		if m.running.Load() != 0 {
			return true, false
		}
	}
	return false, false
}

func canBoost(m *Meta, max time.Time) (find bool, toBreakFind bool) {
	if m.running.Load() < m.initialConcurrency {
		return true, true
	}
	if m.createTS.After(max) {
		return true, false
	}
	return false, false
}
