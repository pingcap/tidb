// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

// ActionType is the type for DDL action.
type ActionType byte

// List DDL actions.
const (
	ActionNone ActionType = iota
	ActionCreateSchema
	ActionDropSchema
	ActionCreateTable
	ActionDropTable
	ActionAddColumn
	ActionDropColumn
	ActionAddIndex
	ActionDropIndex
	ActionAddConstraint
	ActionDropConstraint
)

// Job is for a DDL operation.
type Job struct {
	ID   int64      `json:"id"`
	Type ActionType `json:"type"`
	// SchemaID is 0 if creating schema.
	SchemaID int64 `json:"schema_id"`
	// TableID is 0 if creating table.
	TableID int64         `json:"table_id"`
	Args    []interface{} `json:"args"`
	State   JobState      `json:"state"`
	Error   string        `json:"err"`
}

// JobState is for job.
type JobState byte

// List job states.
const (
	JobNone JobState = iota
	JobRunning
	JobDone
	JobCancelled
)

// String implements fmt.Stringer interface.
func (s JobState) String() string {
	switch s {
	case JobRunning:
		return "running"
	case JobDone:
		return "done"
	case JobCancelled:
		return "cancelled"
	default:
		return "none"
	}
}

// Owner is for DDL Owner.
type Owner struct {
	OwnerID      string `json:"owner_id"`
	LastUpdateTS int64  `json:"last_update_ts"`
}
