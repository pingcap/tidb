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

// Overclock is to increase the concurrency of pool.
func (t *TaskManager[T, U, C, CT, TF]) Overclock(capacity int) (tid uint64, task *TaskBox[T, U, C, CT, TF]) {
	if t.running.Load() >= int32(capacity) {
		return
	}
	return t.getBoostTask()
}

// Downclock is to decrease the concurrency of pool.
func (t *TaskManager[T, U, C, CT, TF]) Downclock(capacity int) {
	if t.running.Load() <= int32(capacity) {
		return
	}
	t.pauseTask()
}
