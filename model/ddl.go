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

import (
	"encoding/json"

	"github.com/juju/errors"
)

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
	ID       int64         `json:"id"`
	Type     ActionType    `json:"type"`
	SchemaID int64         `json:"schema_id"`
	TableID  int64         `json:"table_id"`
	State    JobState      `json:"state"`
	Error    string        `json:"err"`
	Args     []interface{} `json:"-"`
	// we must use json raw message for delay parsing special args.
	RawArgs json.RawMessage `json:"raw_args"`
}

// Encode encodes job with json format.
func (j *Job) Encode() ([]byte, error) {
	var err error
	j.RawArgs, err = json.Marshal(j.Args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var b []byte
	b, err = json.Marshal(j)
	return b, errors.Trace(err)
}

// Decode decodes job from the json buffer, we must use DecodeArgs later to
// decode special args for this job.
func (j *Job) Decode(b []byte) error {
	err := json.Unmarshal(b, j)
	return errors.Trace(err)
}

// DecodeArgs decodes job args.
func (j *Job) DecodeArgs(args ...interface{}) error {
	j.Args = args
	err := json.Unmarshal(j.RawArgs, &j.Args)
	return errors.Trace(err)
}

// JobState is for job state.
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
