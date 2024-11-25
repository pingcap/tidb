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

// ModificationType is the type of task modification.
type ModificationType string

// String implements fmt.Stringer interface.
func (t ModificationType) String() string {
	return string(t)
}

const (
	// ModifyConcurrency is the type for modifying task concurrency.
	ModifyConcurrency ModificationType = "modify_concurrency"
)

// ModifyParam is the parameter for task modification.
type ModifyParam struct {
	PrevState     TaskState      `json:"prev_state"`
	Modifications []Modification `json:"modifications"`
}

// Modification is one modification for task.
type Modification struct {
	Type ModificationType `json:"type"`
	To   int64            `json:"to"`
}
