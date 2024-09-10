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
	"github.com/pingcap/tidb/pkg/util/intest"
)

// getOrDecodeArgsV2 get the argsV2 from job, if the argsV2 is nil, decode rawArgsV2
// and fill argsV2.
func getOrDecodeArgsV2[T JobArgs](job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion2, "job version is not v2")
	if len(job.Args) > 0 {
		return job.Args[0].(T), nil
	}
	var v T
	if err := json.Unmarshal(job.RawArgs, &v); err != nil {
		return v, errors.Trace(err)
	}
	job.Args = []any{v}
	return v, nil
}

// JobArgs is the interface for job arguments.
type JobArgs interface {
	// fillJob fills the job args for submitting job. we make it private to avoid
	// calling it directly, use Job.FillArgs to fill the job args.
	fillJob(job *Job)
}

// FinishedJobArgs is the interface for finished job arguments.
// in most cases, job args are cleared out after the job is finished, but some jobs
// will write some args back to the job for other components.
type FinishedJobArgs interface {
	// fillFinishedJob fills the job args for finished job. we make it private to avoid
	// calling it directly, use Job.FillFinishedArgs to fill the job args.
	fillFinishedJob(job *Job)
}

// CreateSchemaArgs is the arguments for create schema job.
type CreateSchemaArgs struct {
	DBInfo *DBInfo `json:"db_info,omitempty"`
}

func (a *CreateSchemaArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.DBInfo}
		return
	}
	job.Args = []any{a}
}

// GetCreateSchemaArgs gets the args for create schema job.
func GetCreateSchemaArgs(job *Job) (*CreateSchemaArgs, error) {
	if job.Version == JobVersion1 {
		dbInfo := &DBInfo{}
		err := job.DecodeArgs(dbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &CreateSchemaArgs{DBInfo: dbInfo}, nil
	}

	argsV2, err := getOrDecodeArgsV2[*CreateSchemaArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return argsV2, nil
}

// DropSchemaArgs is the arguments for drop schema job.
type DropSchemaArgs struct {
	// this is the args for job submission, it's invalid if the job is finished.
	FKCheck bool `json:"fk_check,omitempty"`
	// this is the args for finished job. this list include all partition IDs too.
	AllDroppedTableIDs []int64 `json:"all_dropped_table_ids,omitempty"`
}

func (a *DropSchemaArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.FKCheck}
		return
	}
	job.Args = []any{a}
}

func (a *DropSchemaArgs) fillFinishedJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.AllDroppedTableIDs}
		return
	}
	job.Args = []any{a}
}

// GetDropSchemaArgs gets the args for drop schema job.
func GetDropSchemaArgs(job *Job) (*DropSchemaArgs, error) {
	return getDropSchemaArgs(job, false)
}

// GetFinishedDropSchemaArgs gets the args for drop schema job after the job is finished.
func GetFinishedDropSchemaArgs(job *Job) (*DropSchemaArgs, error) {
	return getDropSchemaArgs(job, true)
}

func getDropSchemaArgs(job *Job, argsOfFinished bool) (*DropSchemaArgs, error) {
	if job.Version == JobVersion1 {
		if argsOfFinished {
			var physicalTableIDs []int64
			if err := job.DecodeArgs(&physicalTableIDs); err != nil {
				return nil, err
			}
			return &DropSchemaArgs{AllDroppedTableIDs: physicalTableIDs}, nil
		}
		var fkCheck bool
		if err := job.DecodeArgs(&fkCheck); err != nil {
			return nil, err
		}
		return &DropSchemaArgs{FKCheck: fkCheck}, nil
	}
	return getOrDecodeArgsV2[*DropSchemaArgs](job)
}

// ModifySchemaArgs is the arguments for modify schema job.
type ModifySchemaArgs struct {
	// below 2 are used for modify schema charset and collate.
	ToCharset string `json:"to_charset,omitempty"`
	ToCollate string `json:"to_collate,omitempty"`
	// used for modify schema placement policy.
	// might be nil, means set it to default.
	PolicyRef *PolicyRefInfo `json:"policy_ref,omitempty"`
}

func (a *ModifySchemaArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		if job.Type == ActionModifySchemaCharsetAndCollate {
			job.Args = []any{a.ToCharset, a.ToCollate}
		} else if job.Type == ActionModifySchemaDefaultPlacement {
			job.Args = []any{a.PolicyRef}
		}
		return
	}
	job.Args = []any{a}
}

// GetModifySchemaArgs gets the modify schema args.
func GetModifySchemaArgs(job *Job) (*ModifySchemaArgs, error) {
	if job.Version == JobVersion1 {
		var (
			toCharset string
			toCollate string
			policyRef *PolicyRefInfo
		)
		if job.Type == ActionModifySchemaCharsetAndCollate {
			if err := job.DecodeArgs(&toCharset, &toCollate); err != nil {
				return nil, errors.Trace(err)
			}
		} else if job.Type == ActionModifySchemaDefaultPlacement {
			if err := job.DecodeArgs(&policyRef); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return &ModifySchemaArgs{
			ToCharset: toCharset,
			ToCollate: toCollate,
			PolicyRef: policyRef,
		}, nil
	}
	return getOrDecodeArgsV2[*ModifySchemaArgs](job)
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

// This structure will be replaced by RenameTableArgs after previous PR is merged
type RenameTableInfo struct {
	OldSchemaID   int64         `json:"old_schema_id,omitempty"`
	OldSchemaName *pmodel.CIStr `json:"old_schema_name,omitempty"`
	OldTableName  *pmodel.CIStr `json:"old_table_name,omitempty"`
	NewSchemaID   int64         `json:"new_schema_id,omitempty"`
	NewTableName  *pmodel.CIStr `json:"new_table_name,omitempty"`
	TableID       int64         `json:"table_id,omitempty"`
}

// RenameTablesArgs is the arguments for rename tables job.
type RenameTablesArgs struct {
	RenameTableInfos []*RenameTableInfo `json:"rename_table_infos,omitempty"`
}

func (a *RenameTablesArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		n := len(a.RenameTableInfos)
		oldSchemaIDs := make([]int64, n)
		oldSchemaNames := make([]*pmodel.CIStr, n)
		oldTableNames := make([]*pmodel.CIStr, n)
		newSchemaIDs := make([]int64, n)
		newTableNames := make([]*pmodel.CIStr, n)
		tableIDs := make([]int64, n)

		for i, info := range a.RenameTableInfos {
			oldSchemaIDs[i] = info.OldSchemaID
			oldSchemaNames[i] = info.OldSchemaName
			oldTableNames[i] = info.OldTableName
			newSchemaIDs[i] = info.NewSchemaID
			newTableNames[i] = info.NewTableName
			tableIDs[i] = info.TableID
		}

		// To make it compatible with previous create metas
		job.Args = []any{oldSchemaIDs, newSchemaIDs, oldTableNames, tableIDs, oldSchemaNames, newTableNames}
		return
	}
	job.Args = []any{a}
}

// GetRenameTablesArgsFromV1 get v2 args from v1
func GetRenameTablesArgsFromV1(
	oldSchemaIDs []int64,
	oldSchemaNames []*pmodel.CIStr,
	oldTableNames []*pmodel.CIStr,
	newSchemaIDs []int64,
	newTableNames []*pmodel.CIStr,
	tableIDs []int64,
) *RenameTablesArgs {
	infos := make([]*RenameTableInfo, 0, len(oldSchemaIDs))
	for i, oldSchemaID := range oldSchemaIDs {
		infos = append(infos, &RenameTableInfo{
			OldSchemaID:   oldSchemaID,
			OldSchemaName: oldSchemaNames[i],
			OldTableName:  oldTableNames[i],
			NewSchemaID:   newSchemaIDs[i],
			NewTableName:  newTableNames[i],
			TableID:       tableIDs[i],
		})
	}

	return &RenameTablesArgs{
		RenameTableInfos: infos,
	}
}

// GetRenameTablesArgs gets the rename tables args.
func GetRenameTablesArgs(job *Job) (*RenameTablesArgs, error) {
	if job.Version == JobVersion1 {
		var (
			oldSchemaIDs   []int64
			oldSchemaNames []*pmodel.CIStr
			oldTableNames  []*pmodel.CIStr
			newSchemaIDs   []int64
			newTableNames  []*pmodel.CIStr
			tableIDs       []int64
		)
		if err := job.DecodeArgs(
			&oldSchemaIDs, &newSchemaIDs, &oldTableNames,
			&tableIDs, &oldSchemaNames, &newTableNames); err != nil {
			return nil, errors.Trace(err)
		}

		return GetRenameTablesArgsFromV1(
			oldSchemaIDs, oldSchemaNames, oldTableNames,
			newSchemaIDs, newTableNames, tableIDs), nil
	}
	return getOrDecodeArgsV2[*RenameTablesArgs](job)
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
	job.Args = []any{a}
}

func (a *TruncateTableArgs) fillFinishedJob(job *Job) {
	if job.Version == JobVersion1 {
		// the first param is the start key of the old table, it's not used anywhere
		// now, so we fill an empty byte slice here.
		// we can call tablecodec.EncodeTablePrefix(tableID) to get it.
		job.Args = []any{[]byte{}, a.OldPartitionIDs}
		return
	}
	job.Args = []any{a}
}

// GetTruncateTableArgs gets the truncate table args.
func GetTruncateTableArgs(job *Job) (*TruncateTableArgs, error) {
	return getTruncateTableArgs(job, false)
}

// GetFinishedTruncateTableArgs gets the truncate table args after the job is finished.
func GetFinishedTruncateTableArgs(job *Job) (*TruncateTableArgs, error) {
	return getTruncateTableArgs(job, true)
}

func getTruncateTableArgs(job *Job, argsOfFinished bool) (*TruncateTableArgs, error) {
	if job.Version == JobVersion1 {
		if argsOfFinished {
			var startKey []byte
			var oldPartitionIDs []int64
			if err := job.DecodeArgs(&startKey, &oldPartitionIDs); err != nil {
				return nil, errors.Trace(err)
			}
			return &TruncateTableArgs{OldPartitionIDs: oldPartitionIDs}, nil
		}

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

	return getOrDecodeArgsV2[*TruncateTableArgs](job)
}
