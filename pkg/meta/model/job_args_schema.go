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
	"github.com/pingcap/errors"
)

// CreateSchemaArgs is the arguments for create schema job.
type CreateSchemaArgs struct {
	DBInfo *DBInfo `json:"db_info,omitempty"`
}

func (a *CreateSchemaArgs) getArgsV1(*Job) []any {
	return []any{a.DBInfo}
}

func (a *CreateSchemaArgs) decodeV1(job *Job) error {
	a.DBInfo = &DBInfo{}
	return errors.Trace(job.decodeArgs(a.DBInfo))
}

// GetCreateSchemaArgs gets the args for create schema job.
func GetCreateSchemaArgs(job *Job) (*CreateSchemaArgs, error) {
	return getOrDecodeArgs[*CreateSchemaArgs](&CreateSchemaArgs{}, job)
}

// DropSchemaArgs is the arguments for drop schema job.
type DropSchemaArgs struct {
	// this is the args for job submission, it's invalid if the job is finished.
	FKCheck bool `json:"fk_check,omitempty"`
	// this is the args for finished job. this list include all partition IDs too.
	AllDroppedTableIDs []int64 `json:"all_dropped_table_ids,omitempty"`
}

func (a *DropSchemaArgs) getArgsV1(*Job) []any {
	return []any{a.FKCheck}
}

func (a *DropSchemaArgs) getFinishedArgsV1(*Job) []any {
	return []any{a.AllDroppedTableIDs}
}

func (a *DropSchemaArgs) decodeV1(job *Job) error {
	return job.decodeArgs(&a.FKCheck)
}

// GetDropSchemaArgs gets the args for drop schema job.
func GetDropSchemaArgs(job *Job) (*DropSchemaArgs, error) {
	return getOrDecodeArgs[*DropSchemaArgs](&DropSchemaArgs{}, job)
}

// GetFinishedDropSchemaArgs gets the args for drop schema job after the job is finished.
func GetFinishedDropSchemaArgs(job *Job) (*DropSchemaArgs, error) {
	if job.Version == JobVersion1 {
		var physicalTableIDs []int64
		if err := job.decodeArgs(&physicalTableIDs); err != nil {
			return nil, err
		}
		return &DropSchemaArgs{AllDroppedTableIDs: physicalTableIDs}, nil
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

func (a *ModifySchemaArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionModifySchemaCharsetAndCollate {
		return []any{a.ToCharset, a.ToCollate}
	}
	return []any{a.PolicyRef}
}

func (a *ModifySchemaArgs) decodeV1(job *Job) error {
	if job.Type == ActionModifySchemaCharsetAndCollate {
		return errors.Trace(job.decodeArgs(&a.ToCharset, &a.ToCollate))
	}
	return errors.Trace(job.decodeArgs(&a.PolicyRef))
}

// GetModifySchemaArgs gets the modify schema args.
func GetModifySchemaArgs(job *Job) (*ModifySchemaArgs, error) {
	return getOrDecodeArgs[*ModifySchemaArgs](&ModifySchemaArgs{}, job)
}
