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
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
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
	ActionAddForeignKey
	ActionDropForeignKey
	ActionTruncateTable
	ActionModifyColumn
	ActionRenameTable
	ActionSetDefaultValue
)

func (action ActionType) String() string {
	switch action {
	case ActionCreateSchema:
		return "create schema"
	case ActionDropSchema:
		return "drop schema"
	case ActionCreateTable:
		return "create table"
	case ActionDropTable:
		return "drop table"
	case ActionAddColumn:
		return "add column"
	case ActionDropColumn:
		return "drop column"
	case ActionAddIndex:
		return "add index"
	case ActionDropIndex:
		return "drop index"
	case ActionAddForeignKey:
		return "add foreign key"
	case ActionDropForeignKey:
		return "drop foreign key"
	case ActionTruncateTable:
		return "truncate table"
	case ActionModifyColumn:
		return "modify column"
	case ActionRenameTable:
		return "rename table"
	case ActionSetDefaultValue:
		return "set default value"
	default:
		return "none"
	}
}

// HistoryInfo is used for binlog.
type HistoryInfo struct {
	SchemaVersion int64
	DBInfo        *DBInfo
	TableInfo     *TableInfo
}

// AddDBInfo adds schema version and schema information that are used for binlog.
// dbInfo is added in the following operations: create database, drop database.
func (h *HistoryInfo) AddDBInfo(schemaVer int64, dbInfo *DBInfo) {
	h.SchemaVersion = schemaVer
	h.DBInfo = dbInfo
}

// AddTableInfo adds schema version and table information that are used for binlog.
// tblInfo is added except for the following operations: create database, drop database.
func (h *HistoryInfo) AddTableInfo(schemaVer int64, tblInfo *TableInfo) {
	h.SchemaVersion = schemaVer
	h.TableInfo = tblInfo
}

// Job is for a DDL operation.
type Job struct {
	ID       int64         `json:"id"`
	Type     ActionType    `json:"type"`
	SchemaID int64         `json:"schema_id"`
	TableID  int64         `json:"table_id"`
	State    JobState      `json:"state"`
	Error    *terror.Error `json:"err"`
	// Every time we meet an error when running job, we will increase it.
	ErrorCount int64 `json:"err_count"`
	// The number of rows that are processed.
	RowCount int64         `json:"row_count"`
	Mu       sync.Mutex    `json:"-"`
	Args     []interface{} `json:"-"`
	// We must use json raw message to delay parsing special args.
	RawArgs     json.RawMessage `json:"raw_args"`
	SchemaState SchemaState     `json:"schema_state"`
	// Snapshot version for this job.
	SnapshotVer uint64 `json:"snapshot_ver"`
	// unix nano seconds
	// TODO: Use timestamp allocated by TSO.
	LastUpdateTS int64 `json:"last_update_ts"`
	// Query string of the ddl job.
	Query      string       `json:"query"`
	BinlogInfo *HistoryInfo `json:"binlog"`
}

// SetRowCount sets the number of rows. Make sure it can pass `make race`.
func (job *Job) SetRowCount(count int64) {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	job.RowCount = count
}

// GetRowCount gets the number of rows. Make sure it can pass `make race`.
func (job *Job) GetRowCount() int64 {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	return job.RowCount
}

// Encode encodes job with json format.
func (job *Job) Encode() ([]byte, error) {
	var err error
	job.RawArgs, err = json.Marshal(job.Args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var b []byte
	job.Mu.Lock()
	defer job.Mu.Unlock()
	b, err = json.Marshal(job)

	return b, errors.Trace(err)
}

// Decode decodes job from the json buffer, we must use DecodeArgs later to
// decode special args for this job.
func (job *Job) Decode(b []byte) error {
	err := json.Unmarshal(b, job)
	return errors.Trace(err)
}

// DecodeArgs decodes job args.
func (job *Job) DecodeArgs(args ...interface{}) error {
	job.Args = args
	err := json.Unmarshal(job.RawArgs, &job.Args)
	return errors.Trace(err)
}

// String implements fmt.Stringer interface.
func (job *Job) String() string {
	rowCount := job.GetRowCount()
	return fmt.Sprintf("ID:%d, Type:%s, State:%s, SchemaState:%s, SchemaID:%d, TableID:%d, RowCount:%d, ArgLen:%d",
		job.ID, job.Type, job.State, job.SchemaState, job.SchemaID, job.TableID, rowCount, len(job.Args))
}

// IsFinished returns whether job is finished or not.
// If the job state is Done or Cancelled, it is finished.
func (job *Job) IsFinished() bool {
	return job.State == JobDone || job.State == JobRollbackDone || job.State == JobCancelled
}

// IsDone returns whether job is done.
func (job *Job) IsDone() bool {
	return job.State == JobDone
}

// IsRunning returns whether job is still running or not.
func (job *Job) IsRunning() bool {
	return job.State == JobRunning
}

// JobState is for job state.
type JobState byte

// List job states.
const (
	JobNone JobState = iota
	JobRunning
	// When DDL encouterred an unrecoverable error at reorganization state,
	// some keys has been added already, we need to remove them.
	// JobRollback is the state to do rollback work.
	JobRollback
	JobRollbackDone
	JobDone
	JobCancelled
)

// String implements fmt.Stringer interface.
func (s JobState) String() string {
	switch s {
	case JobRunning:
		return "running"
	case JobRollback:
		return "rollback"
	case JobRollbackDone:
		return "rollback done"
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
	OwnerID string `json:"owner_id"`
	// unix nano seconds
	// TODO: Use timestamp allocated by TSO.
	LastUpdateTS int64 `json:"last_update_ts"`
}

// String implements fmt.Stringer interface.
func (o *Owner) String() string {
	return fmt.Sprintf("ID:%s, LastUpdateTS:%d", o.OwnerID, o.LastUpdateTS)
}

// SchemaDiff contains the schema modification at a particular schema version.
// It is used to reduce schema reload cost.
type SchemaDiff struct {
	Version  int64      `json:"version"`
	Type     ActionType `json:"type"`
	SchemaID int64      `json:"schema_id"`
	TableID  int64      `json:"table_id"`

	// OldTableID is the table ID before truncate, only used by truncate table DDL.
	OldTableID int64 `json:"old_table_id"`
	// OldSchemaID is the schema ID before rename table, only used by rename table DDL.
	OldSchemaID int64 `json:"old_schema_id"`
}
