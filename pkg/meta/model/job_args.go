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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

type SubJobArgs interface {
	fillSubJob(subJob *SubJob)
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

// DropIndexArgs is the argument for drop index.
// IndexIDs may have different length with IndexNames and IfExists.
type DropIndexArgs struct {
	IndexNames   []pmodel.CIStr `json:"index_names,omitempty"`
	IfExists     []bool         `json:"if_exists,omitempty"`
	IndexIDs     []int64        `json:"index_ids,omitempty"`
	PartitionIDs []int64        `json:"partition_ids,omitempty"`
}

func (a *DropIndexArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		// This is to make the args compatible with old logic:
		// 1. For drop index, the first and second args are CIStr and bool.
		// 2. For rollback add index, these args could be slices.
		if len(a.IndexNames) == 1 {
			job.Args = []any{a.IndexNames[0], a.IfExists[0], a.IndexIDs, a.PartitionIDs}
		} else {
			job.Args = []any{a.IndexNames, a.IfExists, a.IndexIDs, a.PartitionIDs}
		}
		return
	}
	job.Args = []any{a}
}

// GetDropIndexArgs gets the drop index args.
// It's used for both drop index and rollback add index.
func GetDropIndexArgs(job *Job) (*DropIndexArgs, error) {
	if job.Version == JobVersion1 {
		// A temp workaround
		if len(job.Args) > 0 && len(job.RawArgs) == 0 {
			b, err := json.Marshal(job.Args)
			if err != nil {
				return nil, errors.Trace(err)
			}
			job.RawArgs = b
		}

		indexNames := make([]pmodel.CIStr, 1)
		ifExists := make([]bool, 1)
		indexIDs := make([]int64, 1)
		var partitionIDs []int64

		var rawArgs []json.RawMessage
		if err := json.Unmarshal(job.RawArgs, &rawArgs); err != nil {
			return nil, errors.Trace(err)
		}

		// idxNames can be slice or single CIStr
		if err := job.DecodeArgs(&indexNames, &ifExists, &indexIDs, &partitionIDs); err != nil {
			if err = job.DecodeArgs(&indexNames[0], &ifExists[0], &indexIDs, &partitionIDs); err != nil {
				return nil, errors.Trace(err)
			}
		}

		return &DropIndexArgs{
			IndexNames:   indexNames,
			IfExists:     ifExists,
			IndexIDs:     indexIDs,
			PartitionIDs: partitionIDs,
		}, nil
	}
	return getOrDecodeArgsV2[*DropIndexArgs](job)
}

// IndexArg is the argument for single add index operation
// For NonPK, used fields are (listed in order of v1)
// Unique, IndexName, IndexPartSpecifications, IndexOption, SQLMode, Warning, Global
// For PK
// Unique, IndexName, IndexPartSpecifications, IndexOptions, HiddelCols, Global
type IndexArg struct {
	Global                  bool                          `json:"global,omitempty"`
	Unique                  bool                          `json:"unique,omitempty"`
	IndexName               pmodel.CIStr                  `json:"index_name,omitempty"`
	IndexPartSpecifications []*ast.IndexPartSpecification `json:"index_part_specifications"`
	IndexOption             *ast.IndexOption              `json:"index_option,omitempty"`
	HiddenCols              []*ColumnInfo                 `json:"hidden_cols"`

	// For PK
	SQLMode mysql.SQLMode `json:"sql_mode,omitempty"`
	Warning string        `json:"warnings,omitempty"`

	// IfExist will be used in onDropIndex.
	AddIndexID int64 `json:"add_index_id,omitempty"`
	IfExist    bool  `json:"if_exist,omitempty"`
	IsGlobal   bool  `json:"is_global,omitempty"`
}

// AddIndexArgs is the argument for add index.
type AddIndexArgs struct {
	IndexArgs []*IndexArg `json:"index_args,omitempty"`

	// PartitionIDs will be used in onDropIndex.
	PartitionIDs []int64 `json:"partition_ids"`

	// Since most of the argument processing logic of PK and index is same,
	// We use this variable to distinguish them.
	IsPK bool `json:"is_pk"`

	// This is to dintinguish finished and running args, used for expectedDeleteRangeCnt
	IsFinishedArg bool `json:"is_finished,omitempty"`
}

// getV1Args is used to get arglists for v1
func (a *AddIndexArgs) getV1Args() []any {
	if a.IsPK {
		arg := a.IndexArgs[0]
		return []any{
			arg.Unique, arg.IndexName, arg.IndexPartSpecifications,
			arg.IndexOption, arg.SQLMode, arg.Warning, arg.Global,
		}
	}

	n := len(a.IndexArgs)
	unique := make([]bool, n)
	indexName := make([]pmodel.CIStr, n)
	indexPartSpecification := make([][]*ast.IndexPartSpecification, n)
	indexOption := make([]*ast.IndexOption, n)
	hiddenCols := make([][]*ColumnInfo, n)
	global := make([]bool, n)

	for i, arg := range a.IndexArgs {
		unique[i] = arg.Unique
		indexName[i] = arg.IndexName
		indexPartSpecification[i] = arg.IndexPartSpecifications
		indexOption[i] = arg.IndexOption
		hiddenCols[i] = arg.HiddenCols
		global[i] = arg.Global
	}

	// This is to make the args compatible with old logic, same like DropIndexArgs
	if n == 1 {
		return []any{unique[0], indexName[0], indexPartSpecification[0], indexOption[0], hiddenCols[0], global[0]}
	} else {
		return []any{unique, indexName, indexPartSpecification, indexOption, hiddenCols, global}
	}
}

// MergeIndexArg is used to merge from
func (a *AddIndexArgs) MergeIndexArg(Args []any, jobVersion JobVersion) {
	var indexArg *IndexArg
	if jobVersion == JobVersion1 {
		indexArg = &IndexArg{
			Unique:                  *Args[0].(*bool),
			IndexName:               *Args[1].(*pmodel.CIStr),
			IndexPartSpecifications: *Args[2].(*[]*ast.IndexPartSpecification),
			IndexOption:             *Args[3].(**ast.IndexOption),
			HiddenCols:              *Args[4].(*[]*ColumnInfo),
			Global:                  *Args[5].(*bool),
		}
	} else {
		indexArg = Args[0].(*AddIndexArgs).IndexArgs[0]
	}
	a.IndexArgs = append(a.IndexArgs, indexArg)
}

func (a *AddIndexArgs) fillSubJob(job *SubJob) {
	intest.Assert(!a.IsPK, "a should not be add primary key argument")
	if job.Version == JobVersion1 {
		job.Args = a.getV1Args()
	} else {
		job.Args = []any{a}
	}
}

func (a *AddIndexArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = a.getV1Args()
	} else {
		job.Args = []any{a}
	}
}

func (a *AddIndexArgs) fillFinishedJob(job *Job) {
	if job.Version == JobVersion1 {
		n := len(a.IndexArgs)
		addIndexIDs := make([]int64, n)
		ifExists := make([]bool, n)
		isGlobals := make([]bool, n)
		for i, arg := range a.IndexArgs {
			addIndexIDs[i] = arg.AddIndexID
			ifExists[i] = arg.IfExist
			isGlobals[i] = arg.Global
		}
		job.Args = []any{addIndexIDs, ifExists, a.PartitionIDs, isGlobals}
		return
	}
	job.Args = []any{a}
}

// GetAddIndexArgs gets the add index args.
func GetAddIndexArgs(job *Job) (*AddIndexArgs, error) {
	if job.Version == JobVersion1 {
		if job.Type == ActionAddIndex {
			return getAddIndexArgs(job)
		}
		intest.Assert(job.Type == ActionAddPrimaryKey, "Type should be ActionAddPrimaryKey")
		return getAddPrimaryKeyArgs(job)
	}
	args, err := getOrDecodeArgsV2[*AddIndexArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if args.IsFinishedArg {
		return nil, errors.Errorf("Got finished AddIndexArgs, expect running AddIndexArgs")
	}
	return args, nil
}

// GetFinishedAddIndexArgs gets the add index args after the job is finished.
func GetFinishedAddIndexArgs(job *Job) (*AddIndexArgs, error) {
	if job.Version == JobVersion1 {
		addIndexIDs := make([]int64, 1)
		ifExists := make([]bool, 1)
		isGlobals := make([]bool, 1)
		var partitionIDs []int64

		if err := job.DecodeArgs(&addIndexIDs[0], &ifExists[0], &partitionIDs, &isGlobals[0]); err != nil {
			if err = job.DecodeArgs(&addIndexIDs, &ifExists, &partitionIDs, &isGlobals); err != nil {
				return nil, errors.Trace(err)
			}
		}
		a := &AddIndexArgs{PartitionIDs: partitionIDs, IsFinishedArg: true}
		for i, addIndexID := range addIndexIDs {
			a.IndexArgs = append(a.IndexArgs, &IndexArg{
				AddIndexID: addIndexID,
				IfExist:    ifExists[i],
				IsGlobal:   isGlobals[i],
			})
		}
		return a, nil
	}

	args, err := getOrDecodeArgsV2[*AddIndexArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !args.IsFinishedArg {
		return nil, errors.Errorf("Got running AddIndexArgs, expect finished AddIndexArgs")
	}
	return args, nil
}

func getAddPrimaryKeyArgs(job *Job) (*AddIndexArgs, error) {
	intest.Assert(job.Version == JobVersion1, "job version is not v1")

	// A temp workaround
	if len(job.Args) > 0 && len(job.RawArgs) == 0 {
		b, err := json.Marshal(job.Args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		job.RawArgs = b
	}

	var (
		unique                  bool
		indexName               model.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
		sqlMode                 mysql.SQLMode
		warning                 string
		global                  bool
	)
	if err := job.DecodeArgs(
		&unique, &indexName, &indexPartSpecifications,
		&indexOption, &sqlMode, &warning, &global); err != nil {
		return nil, errors.Trace(err)
	}

	return &AddIndexArgs{
		IsPK: true,
		IndexArgs: []*IndexArg{{
			Global:                  global,
			Unique:                  unique,
			IndexName:               indexName,
			IndexPartSpecifications: indexPartSpecifications,
			IndexOption:             indexOption,
			SQLMode:                 sqlMode,
			Warning:                 warning,
		}},
	}, nil
}

func getAddIndexArgs(job *Job) (*AddIndexArgs, error) {
	intest.Assert(job.Version == JobVersion1, "job version is not v1")

	// A temp workaround
	if len(job.Args) > 0 && len(job.RawArgs) == 0 {
		b, err := json.Marshal(job.Args)
		if err != nil {
			return nil, errors.Trace(err)
		}
		job.RawArgs = b
	}

	uniques := make([]bool, 1)
	indexNames := make([]model.CIStr, 1)
	indexPartSpecifications := make([][]*ast.IndexPartSpecification, 1)
	indexOptions := make([]*ast.IndexOption, 1)
	hiddenCols := make([][]*ColumnInfo, 1)
	globals := make([]bool, 1)

	if err := job.DecodeArgs(
		&uniques, &indexNames, &indexPartSpecifications,
		&indexOptions, &hiddenCols, &globals); err != nil {
		if err = job.DecodeArgs(
			&uniques[0], &indexNames[0], &indexPartSpecifications[0],
			&indexOptions[0], &hiddenCols[0], &globals[0]); err != nil {
			return nil, errors.Trace(err)
		}
	}

	a := AddIndexArgs{}
	for i, unique := range uniques {
		a.IndexArgs = append(a.IndexArgs, &IndexArg{
			Unique:                  unique,
			IndexName:               indexNames[i],
			IndexPartSpecifications: indexPartSpecifications[i],
			IndexOption:             indexOptions[i],
			HiddenCols:              hiddenCols[i],
			Global:                  globals[i],
		})
	}
	return &a, nil
}
