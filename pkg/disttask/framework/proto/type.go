// Copyright 2024 PingCAP, Inc.
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

package proto

const (
	// TaskTypeExample is TaskType of Example.
	TaskTypeExample TaskType = "Example"
	// ImportInto is TaskType of ImportInto.
	ImportInto TaskType = "ImportInto"
	// Backfill is TaskType of add index Backfilling process.
	Backfill TaskType = "backfill"
)

// Type2Int converts task type to int.
func Type2Int(t TaskType) int {
	switch t {
	case TaskTypeExample:
		return 1
	case ImportInto:
		return 2
	case Backfill:
		return 3
	default:
		return 0
	}
}

// Int2Type converts int to task type.
func Int2Type(i int) TaskType {
	switch i {
	case 1:
		return TaskTypeExample
	case 2:
		return ImportInto
	case 3:
		return Backfill
	default:
		return ""
	}
}
