// Copyright 2026 PingCAP, Inc.
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

package autoanalyze

var (
	// OrderByUsedSlots exports orderByUsedSlots for test.
	OrderByUsedSlots = orderByUsedSlots
	// PlaceTask places a task the way the schedulers of this process do, for test.
	PlaceTask = placed.place
	// ReleaseTask releases the place of a task for test.
	ReleaseTask = placed.release
)

// PlacedTasks returns the number of tasks that hold a place, for test.
func PlacedTasks() int {
	placed.mu.Lock()
	defer placed.mu.Unlock()
	return len(placed.nodes)
}
