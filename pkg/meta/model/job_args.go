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

package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

// getOrDecodeArgsV2 get the argsV2 from job, if the argsV2 is nil, decode rawArgsV2
// and fill argsV2.
func getOrDecodeArgsV2[T JobArgs](job *Job) (T, error) {
	if job.ArgsV2 != nil {
		return job.ArgsV2.(T), nil
	}
	var v T
	if err := json.Unmarshal(job.RawArgsV2, &v); err != nil {
		return v, errors.Trace(err)
	}
	job.ArgsV2 = v
	return v, nil
}

// JobArgs is the interface for job arguments.
type JobArgs interface {
	// fillJob fills the job args for submitting job. we make it private to avoid
	// calling it directly, use Job.FillArgs to fill the job args.
	fillJob(job *Job)
}

// TruncateTableArgs is the arguments for truncate table job.
type TruncateTableArgs struct {
	FKCheck         bool    `json:"fk_check,omitempty"`
	NewTableID      int64   `json:"new_table_id,omitempty"`
	NewPartitionIDs []int64 `json:"new_partition_ids,omitempty"`
	OldPartitionIDs []int64 `json:"old_partition_ids,omitempty"`

	// context vars
	NewPartIDsWithPolicy []int64 `json:"-"`
	OldPartIDsWithPolicy []int64 `json:"-"`
}

func (a *TruncateTableArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		// Args[0] is the new table ID, args[2] is the ids for table partitions, we
		// add a placeholder here, they will be filled by job submitter.
		// the last param is not required for execution, we need it to calculate
		// number of new IDs to generate.
		job.Args = []any{a.NewTableID, a.FKCheck, a.NewPartitionIDs, len(a.OldPartitionIDs)}
		return
	}
	job.ArgsV2 = a
}

// GetTruncateTableArgsBeforeRun gets the truncate table args that we set before
// running the job. the args might be changed after the job run on JobVersion1.
func GetTruncateTableArgsBeforeRun(job *Job) (*TruncateTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			newTableID      int64
			fkCheck         bool
			newPartitionIDs []int64
		)
		err := job.DecodeArgs(&newTableID, &fkCheck, &newPartitionIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &TruncateTableArgs{
			NewTableID:      newTableID,
			FKCheck:         fkCheck,
			NewPartitionIDs: newPartitionIDs,
		}, nil
	}

	argsV2, err := getOrDecodeArgsV2[*TruncateTableArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return argsV2, nil
}

// GetTruncateTableArgsAfterRun gets the truncate table args after running the job.
func GetTruncateTableArgsAfterRun(job *Job) (*TruncateTableArgs, error) {
	if job.Version == JobVersion1 {
		var startKey []byte
		var oldPartitionIDs []int64
		if err := job.DecodeArgs(&startKey, &oldPartitionIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &TruncateTableArgs{OldPartitionIDs: oldPartitionIDs}, nil
	}

	argsV2, err := getOrDecodeArgsV2[*TruncateTableArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return argsV2, nil
}

// RenameTableArgs is the arguements for rename table DDL job.
type RenameTableArgs struct {
	// for Args
	OldSchemaID  int64        `json:"old_schema_id,omitempty"`
	SchemaName   pmodel.CIStr `json:"schema_name,omitempty"`
	NewTableName pmodel.CIStr `json:"new_table_name,omitempty"`

	// for CtxVars
	// SchemaIDs contains {oldSchemaID, newSchemaID}
	SchemaIDs []int64 `json:"-"`
	// TableIDs contains {tableID}
	TableIDs []int64 `json:"-"`
}

func (rt *RenameTableArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{rt.OldSchemaID, rt.NewTableName, rt.SchemaName}
		job.CtxVars = []any{rt.SchemaIDs, rt.TableIDs}
	} else {
		job.ArgsV2 = rt
	}
}

// NewRenameTableArgs creates a struct with given parameters.
func NewRenameTableArgs(
	oldSchemaID int64, newSchemaID int64, schemaName pmodel.CIStr,
	tableID int64, newTableName pmodel.CIStr,
) *RenameTableArgs {
	return &RenameTableArgs{
		OldSchemaID:  oldSchemaID,
		SchemaName:   schemaName,
		NewTableName: newTableName,
		SchemaIDs:    []int64{oldSchemaID, newSchemaID},
		TableIDs:     []int64{tableID},
	}
}

// GetRenameTableArgs get the arguements from job.
func GetRenameTableArgs(job *Job) (*RenameTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			oldSchemaID  int64
			schemaName   pmodel.CIStr
			newTableName pmodel.CIStr
		)

		// decode args and cache in args.
		if len(job.Args) == 0 {
			err := job.DecodeArgs(&oldSchemaID, &newTableName, &schemaName)
			if err != nil {
				return nil, errors.Trace(err)
			}
			job.Args = []any{oldSchemaID, newTableName, schemaName}
		} else {
			oldSchemaID = job.Args[0].(int64)
			schemaName = job.Args[1].(pmodel.CIStr)
			newTableName = job.Args[2].(pmodel.CIStr)
		}

		args := RenameTableArgs{
			OldSchemaID:  oldSchemaID,
			SchemaName:   schemaName,
			NewTableName: newTableName,
		}
		if len(job.CtxVars) > 0 {
			args.SchemaIDs = job.CtxVars[0].([]int64)
			args.TableIDs = job.CtxVars[1].([]int64)
		}
		return &args, nil
	}

	argsV2, err := getOrDecodeArgsV2[*RenameTableArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return argsV2, err
}

// GetRenameTableUniqueIDs gets unique IDs from job for rename table type.
func GetRenameTableUniqueIDs(job *Job, schema bool) []int64 {
	args := &RenameTableArgs{}

	if job.Version == JobVersion1 {
		args.SchemaIDs = job.CtxVars[0].([]int64)
		args.TableIDs = job.CtxVars[1].([]int64)
	} else {
		args = job.ArgsV2.(*RenameTableArgs)
	}

	if schema {
		return args.SchemaIDs
	}
	return args.TableIDs
}
