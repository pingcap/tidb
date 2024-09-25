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
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// getOrDecodeArgsV2 get the argsV2 from job, if the argsV2 is nil, decode rawArgsV2
// and fill argsV2.
func getOrDecodeArgsV2[T JobArgs](job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion2, "job version is not v2")
	if len(job.Args) > 0 {
		intest.Assert(len(job.Args) == 1, "job args length is not 1")
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

// CreateTableArgs is the arguments for create table/view/sequence job.
type CreateTableArgs struct {
	TableInfo *TableInfo `json:"table_info,omitempty"`
	// below 2 are used for create view.
	OnExistReplace bool  `json:"on_exist_replace,omitempty"`
	OldViewTblID   int64 `json:"old_view_tbl_id,omitempty"`
	// used for create table.
	FKCheck bool `json:"fk_check,omitempty"`
}

func (a *CreateTableArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		switch job.Type {
		case ActionCreateTable:
			job.Args = []any{a.TableInfo, a.FKCheck}
		case ActionCreateView:
			job.Args = []any{a.TableInfo, a.OnExistReplace, a.OldViewTblID}
		case ActionCreateSequence:
			job.Args = []any{a.TableInfo}
		}
		return
	}
	job.Args = []any{a}
}

// GetCreateTableArgs gets the create-table args.
func GetCreateTableArgs(job *Job) (*CreateTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			tableInfo      = &TableInfo{}
			onExistReplace bool
			oldViewTblID   int64
			fkCheck        bool
		)
		switch job.Type {
		case ActionCreateTable:
			if err := job.DecodeArgs(tableInfo, &fkCheck); err != nil {
				return nil, errors.Trace(err)
			}
		case ActionCreateView:
			if err := job.DecodeArgs(tableInfo, &onExistReplace, &oldViewTblID); err != nil {
				return nil, errors.Trace(err)
			}
		case ActionCreateSequence:
			if err := job.DecodeArgs(tableInfo); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return &CreateTableArgs{
			TableInfo:      tableInfo,
			OnExistReplace: onExistReplace,
			OldViewTblID:   oldViewTblID,
			FKCheck:        fkCheck,
		}, nil
	}
	return getOrDecodeArgsV2[*CreateTableArgs](job)
}

// BatchCreateTableArgs is the arguments for batch create table job.
type BatchCreateTableArgs struct {
	Tables []*CreateTableArgs `json:"tables,omitempty"`
}

func (a *BatchCreateTableArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		infos := make([]*TableInfo, 0, len(a.Tables))
		for _, info := range a.Tables {
			infos = append(infos, info.TableInfo)
		}
		job.Args = []any{infos, a.Tables[0].FKCheck}
		return
	}
	job.Args = []any{a}
}

// GetBatchCreateTableArgs gets the batch create-table args.
func GetBatchCreateTableArgs(job *Job) (*BatchCreateTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			tableInfos []*TableInfo
			fkCheck    bool
		)
		if err := job.DecodeArgs(&tableInfos, &fkCheck); err != nil {
			return nil, errors.Trace(err)
		}
		args := &BatchCreateTableArgs{Tables: make([]*CreateTableArgs, 0, len(tableInfos))}
		for _, info := range tableInfos {
			args.Tables = append(args.Tables, &CreateTableArgs{TableInfo: info, FKCheck: fkCheck})
		}
		return args, nil
	}
	return getOrDecodeArgsV2[*BatchCreateTableArgs](job)
}

// DropTableArgs is the arguments for drop table/view/sequence job.
// when dropping multiple objects, each object will have a separate job
type DropTableArgs struct {
	// below fields are only for drop table.
	// when dropping multiple tables, the Identifiers is the same.
	Identifiers []ast.Ident `json:"identifiers,omitempty"`
	FKCheck     bool        `json:"fk_check,omitempty"`

	// below fields are finished job args
	StartKey        []byte   `json:"start_key,omitempty"`
	OldPartitionIDs []int64  `json:"old_partition_ids,omitempty"`
	OldRuleIDs      []string `json:"old_rule_ids,omitempty"`
}

func (a *DropTableArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		// only drop table job has in args.
		if job.Type == ActionDropTable {
			job.Args = []any{a.Identifiers, a.FKCheck}
		}
		return
	}
	job.Args = []any{a}
}

func (a *DropTableArgs) fillFinishedJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.StartKey, a.OldPartitionIDs, a.OldRuleIDs}
		return
	}
	job.Args = []any{a}
}

func (a *DropTableArgs) decodeV1(job *Job) error {
	intest.Assert(job.Type == ActionDropTable, "only drop table job can call GetDropTableArgs")
	return job.DecodeArgs(&a.Identifiers, &a.FKCheck)
}

// GetDropTableArgs gets the drop-table args.
func GetDropTableArgs(job *Job) (*DropTableArgs, error) {
	if job.Version == JobVersion1 {
		args := &DropTableArgs{}
		if err := args.decodeV1(job); err != nil {
			return nil, errors.Trace(err)
		}
		return args, nil
	}
	return getOrDecodeArgsV2[*DropTableArgs](job)
}

// GetFinishedDropTableArgs gets the drop-table args after the job is finished.
func GetFinishedDropTableArgs(job *Job) (*DropTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			startKey        []byte
			oldPartitionIDs []int64
			oldRuleIDs      []string
		)
		if err := job.DecodeArgs(&startKey, &oldPartitionIDs, &oldRuleIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &DropTableArgs{
			StartKey:        startKey,
			OldPartitionIDs: oldPartitionIDs,
			OldRuleIDs:      oldRuleIDs,
		}, nil
	}
	return getOrDecodeArgsV2[*DropTableArgs](job)
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

// TablePartitionArgs is the arguments for table partition related jobs, including:
//   - ActionAlterTablePartitioning
//   - ActionRemovePartitioning
//   - ActionReorganizePartition
//   - ActionAddTablePartition: don't have finished args if success.
//   - ActionDropTablePartition
//
// when rolling back, args of ActionAddTablePartition will be changed to be the same
// as ActionDropTablePartition, and it will have finished args, but not used anywhere,
// for other types, their args will be decoded as if its args is the same of ActionDropTablePartition.
type TablePartitionArgs struct {
	PartNames []string       `json:"part_names,omitempty"`
	PartInfo  *PartitionInfo `json:"part_info,omitempty"`

	// set on finished
	OldPhysicalTblIDs []int64 `json:"old_physical_tbl_ids,omitempty"`
}

func (a *TablePartitionArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		if job.Type == ActionAddTablePartition {
			job.Args = []any{a.PartInfo}
		} else if job.Type == ActionDropTablePartition {
			job.Args = []any{a.PartNames}
		} else {
			job.Args = []any{a.PartNames, a.PartInfo}
		}
		return
	}
	job.Args = []any{a}
}

func (a *TablePartitionArgs) fillFinishedJob(job *Job) {
	if job.Version == JobVersion1 {
		intest.Assert(job.Type != ActionAddTablePartition || job.State == JobStateRollbackDone,
			"add table partition job should not call fillFinishedJob if not rollback")
		job.Args = []any{a.OldPhysicalTblIDs}
		return
	}
	job.Args = []any{a}
}

func (a *TablePartitionArgs) decodeV1(job *Job) error {
	var (
		partNames []string
		partInfo  = &PartitionInfo{}
	)
	if job.Type == ActionAddTablePartition {
		if job.State == JobStateRollingback {
			if err := job.DecodeArgs(&partNames); err != nil {
				return err
			}
		} else {
			if err := job.DecodeArgs(partInfo); err != nil {
				return err
			}
		}
	} else if job.Type == ActionDropTablePartition {
		if err := job.DecodeArgs(&partNames); err != nil {
			return err
		}
	} else {
		if err := job.DecodeArgs(&partNames, partInfo); err != nil {
			return err
		}
	}
	a.PartNames = partNames
	a.PartInfo = partInfo
	return nil
}

// GetTablePartitionArgs gets the table partition args.
func GetTablePartitionArgs(job *Job) (*TablePartitionArgs, error) {
	if job.Version == JobVersion1 {
		args := &TablePartitionArgs{}
		if err := args.decodeV1(job); err != nil {
			return nil, errors.Trace(err)
		}
		return args, nil
	}
	args, err := getOrDecodeArgsV2[*TablePartitionArgs](job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// when it's ActionDropTablePartition job, or roll-backing a ActionAddTablePartition
	// job, our execution part expect a non-nil PartInfo.
	if args.PartInfo == nil {
		args.PartInfo = &PartitionInfo{}
	}
	return args, nil
}

// GetFinishedTablePartitionArgs gets the table partition args after the job is finished.
func GetFinishedTablePartitionArgs(job *Job) (*TablePartitionArgs, error) {
	if job.Version == JobVersion1 {
		var oldPhysicalTblIDs []int64
		if err := job.DecodeArgs(&oldPhysicalTblIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &TablePartitionArgs{OldPhysicalTblIDs: oldPhysicalTblIDs}, nil
	}
	return getOrDecodeArgsV2[*TablePartitionArgs](job)
}

// FillRollbackArgsForAddPartition fills the rollback args for add partition job.
// see details in TablePartitionArgs.
func FillRollbackArgsForAddPartition(job *Job, args *TablePartitionArgs) {
	intest.Assert(job.Type == ActionAddTablePartition, "only for add partition job")
	fake := &Job{
		Version: job.Version,
		Type:    ActionDropTablePartition,
	}
	// PartInfo cannot be saved, onDropTablePartition expects that PartInfo is empty
	// in this case
	fake.FillArgs(&TablePartitionArgs{
		PartNames: args.PartNames,
	})
	job.Args = fake.Args
}

// RenameTableArgs is the arguments for rename table DDL job.
// It's also used for rename tables.
type RenameTableArgs struct {
	// for Args
	OldSchemaID   int64        `json:"old_schema_id,omitempty"`
	OldSchemaName pmodel.CIStr `json:"old_schema_name,omitempty"`
	NewTableName  pmodel.CIStr `json:"new_table_name,omitempty"`

	// for rename tables
	OldTableName pmodel.CIStr `json:"old_table_name,omitempty"`
	NewSchemaID  int64        `json:"new_schema_id,omitempty"`
	TableID      int64        `json:"table_id,omitempty"`
}

func (rt *RenameTableArgs) fillJob(job *Job) {
	if job.Version <= JobVersion1 {
		job.Args = []any{rt.OldSchemaID, rt.NewTableName, rt.OldSchemaName}
	} else {
		job.Args = []any{rt}
	}
}

// GetRenameTableArgs get the arguments from job.
func GetRenameTableArgs(job *Job) (*RenameTableArgs, error) {
	var (
		oldSchemaID   int64
		oldSchemaName pmodel.CIStr
		newTableName  pmodel.CIStr
		args          *RenameTableArgs
		err           error
	)

	if job.Version == JobVersion1 {
		// decode args and cache in args.
		err = job.DecodeArgs(&oldSchemaID, &newTableName, &oldSchemaName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		args = &RenameTableArgs{
			OldSchemaID:   oldSchemaID,
			OldSchemaName: oldSchemaName,
			NewTableName:  newTableName,
		}
	} else {
		// for version V2
		args, err = getOrDecodeArgsV2[*RenameTableArgs](job)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// NewSchemaID is used for checkAndRenameTables, which is not set for rename table.
	args.NewSchemaID = job.SchemaID
	return args, nil
}

// UpdateRenameTableArgs updates the rename table args.
// need to reset the old schema ID to new schema ID.
func UpdateRenameTableArgs(job *Job) error {
	var err error

	// for job version1
	if job.Version == JobVersion1 {
		// update schemaID and marshal()
		job.Args[0] = job.SchemaID
		job.RawArgs, err = json.Marshal(job.Args)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		argsV2, err := getOrDecodeArgsV2[*RenameTableArgs](job)
		if err != nil {
			return errors.Trace(err)
		}

		// update schemaID and marshal()
		argsV2.OldSchemaID = job.SchemaID
		job.Args = []any{argsV2}
		job.RawArgs, err = json.Marshal(job.Args[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CheckConstraintArgs is the arguments for both AlterCheckConstraint and DropCheckConstraint job.
type CheckConstraintArgs struct {
	ConstraintName pmodel.CIStr `json:"constraint_name,omitempty"`
	Enforced       bool         `json:"enforced,omitempty"`
}

func (a *CheckConstraintArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.ConstraintName, a.Enforced}
		return
	}
	job.Args = []any{a}
}

// ResourceGroupArgs is the arguments for resource group job.
type ResourceGroupArgs struct {
	// for DropResourceGroup we only use it to store the name, other fields are invalid.
	RGInfo *ResourceGroupInfo `json:"rg_info,omitempty"`
}

func (a *ResourceGroupArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		if job.Type == ActionCreateResourceGroup {
			// what's the second parameter for? we keep it for compatibility.
			job.Args = []any{a.RGInfo, false}
		} else if job.Type == ActionAlterResourceGroup {
			job.Args = []any{a.RGInfo}
		} else if job.Type == ActionDropResourceGroup {
			// it's not used anywhere.
			job.Args = []any{a.RGInfo.Name}
		}
		return
	}
	job.Args = []any{a}
}

// GetResourceGroupArgs gets the resource group args.
func GetResourceGroupArgs(job *Job) (*ResourceGroupArgs, error) {
	if job.Version == JobVersion1 {
		rgInfo := ResourceGroupInfo{}
		if job.Type == ActionCreateResourceGroup || job.Type == ActionAlterResourceGroup {
			if err := job.DecodeArgs(&rgInfo); err != nil {
				return nil, errors.Trace(err)
			}
		} else if job.Type == ActionDropResourceGroup {
			var rgName pmodel.CIStr
			if err := job.DecodeArgs(&rgName); err != nil {
				return nil, errors.Trace(err)
			}
			rgInfo.Name = rgName
		}
		return &ResourceGroupArgs{RGInfo: &rgInfo}, nil
	}
	return getOrDecodeArgsV2[*ResourceGroupArgs](job)
}

// RebaseAutoIDArgs is the arguments for ActionRebaseAutoID DDL.
// It is also for ActionRebaseAutoRandomBase.
type RebaseAutoIDArgs struct {
	NewBase int64 `json:"new_base,omitempty"`
	Force   bool  `json:"force,omitempty"`
}

func (a *RebaseAutoIDArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.NewBase, a.Force}
	} else {
		job.Args = []any{a}
	}
}

// GetRebaseAutoIDArgs the args for ActionRebaseAutoID/ActionRebaseAutoRandomBase ddl.
func GetRebaseAutoIDArgs(job *Job) (*RebaseAutoIDArgs, error) {
	var (
		newBase int64
		force   bool
	)

	if job.Version == JobVersion1 {
		if err := job.DecodeArgs(&newBase, &force); err != nil {
			return nil, errors.Trace(err)
		}
		return &RebaseAutoIDArgs{
			NewBase: newBase,
			Force:   force,
		}, nil
	}

	// for version V2
	return getOrDecodeArgsV2[*RebaseAutoIDArgs](job)
}

// ModifyTableCommentArgs is the arguments for ActionModifyTableComment ddl.
type ModifyTableCommentArgs struct {
	Comment string `json:"comment,omitempty"`
}

func (a *ModifyTableCommentArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.Comment}
	} else {
		job.Args = []any{a}
	}
}

// GetModifyTableCommentArgs gets the args for ActionModifyTableComment.
func GetModifyTableCommentArgs(job *Job) (*ModifyTableCommentArgs, error) {
	if job.Version == JobVersion1 {
		var comment string
		if err := job.DecodeArgs(&comment); err != nil {
			return nil, errors.Trace(err)
		}
		return &ModifyTableCommentArgs{
			Comment: comment,
		}, nil
	}

	return getOrDecodeArgsV2[*ModifyTableCommentArgs](job)
}

// ModifyTableCharsetAndCollateArgs is the arguments for ActionModifyTableCharsetAndCollate ddl.
type ModifyTableCharsetAndCollateArgs struct {
	ToCharset          string `json:"to_charset,omitempty"`
	ToCollate          string `json:"to_collate,omitempty"`
	NeedsOverwriteCols bool   `json:"needs_overwrite_cols,omitempty"`
}

func (a *ModifyTableCharsetAndCollateArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.ToCharset, a.ToCollate, a.NeedsOverwriteCols}
	} else {
		job.Args = []any{a}
	}
}

// GetModifyTableCharsetAndCollateArgs gets the args for ActionModifyTableCharsetAndCollate ddl.
func GetModifyTableCharsetAndCollateArgs(job *Job) (*ModifyTableCharsetAndCollateArgs, error) {
	if job.Version == JobVersion1 {
		args := &ModifyTableCharsetAndCollateArgs{}
		err := job.DecodeArgs(&args.ToCharset, &args.ToCollate, &args.NeedsOverwriteCols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return args, nil
	}

	return getOrDecodeArgsV2[*ModifyTableCharsetAndCollateArgs](job)
}

// AlterIndexVisibilityArgs is the arguments for ActionAlterIndexVisibility ddl.
type AlterIndexVisibilityArgs struct {
	IndexName pmodel.CIStr `json:"index_name,omitempty"`
	Invisible bool         `json:"invisible,omitempty"`
}

func (a *AlterIndexVisibilityArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.IndexName, a.Invisible}
	} else {
		job.Args = []any{a}
	}
}

// GetAlterIndexVisibilityArgs gets the args for AlterIndexVisibility ddl.
func GetAlterIndexVisibilityArgs(job *Job) (*AlterIndexVisibilityArgs, error) {
	if job.Version == JobVersion1 {
		var (
			indexName pmodel.CIStr
			invisible bool
		)
		if err := job.DecodeArgs(&indexName, &invisible); err != nil {
			return nil, errors.Trace(err)
		}
		return &AlterIndexVisibilityArgs{
			IndexName: indexName,
			Invisible: invisible,
		}, nil
	}

	return getOrDecodeArgsV2[*AlterIndexVisibilityArgs](job)
}

// AddForeignKeyArgs is the arguments for ActionAddForeignKey ddl.
type AddForeignKeyArgs struct {
	FkInfo  *FKInfo `json:"fk_info,omitempty"`
	FkCheck bool    `json:"fk_check,omitempty"`
}

func (a *AddForeignKeyArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.FkInfo, a.FkCheck}
	} else {
		job.Args = []any{a}
	}
}

// GetAddForeignKeyArgs get the args for AddForeignKey ddl.
func GetAddForeignKeyArgs(job *Job) (*AddForeignKeyArgs, error) {
	if job.Version == JobVersion1 {
		var (
			fkInfo  *FKInfo
			fkCheck bool
		)
		if err := job.DecodeArgs(&fkInfo, &fkCheck); err != nil {
			return nil, errors.Trace(err)
		}
		return &AddForeignKeyArgs{
			FkInfo:  fkInfo,
			FkCheck: fkCheck,
		}, nil
	}

	return getOrDecodeArgsV2[*AddForeignKeyArgs](job)
}

// DropForeignKeyArgs is the arguments for DropForeignKey ddl.
type DropForeignKeyArgs struct {
	FkName pmodel.CIStr `json:"fk_name,omitempty"`
}

func (a *DropForeignKeyArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.FkName}
	} else {
		job.Args = []any{a}
	}
}

// GetDropForeignKeyArgs gets the args for DropForeignKey ddl.
func GetDropForeignKeyArgs(job *Job) (*DropForeignKeyArgs, error) {
	if job.Version == JobVersion1 {
		var fkName pmodel.CIStr
		if err := job.DecodeArgs(&fkName); err != nil {
			return nil, errors.Trace(err)
		}
		return &DropForeignKeyArgs{FkName: fkName}, nil
	}

	return getOrDecodeArgsV2[*DropForeignKeyArgs](job)
}

// DropColumnArgs is the arguments of dropping column job.
type DropColumnArgs struct {
	ColName  pmodel.CIStr `json:"column_name,omitempty"`
	IfExists bool         `json:"if_exists,omitempty"`
	// below 2 fields are filled during running.
	IndexIDs     []int64 `json:"index_ids,omitempty"`
	PartitionIDs []int64 `json:"partition_ids,omitempty"`
}

func (a *DropColumnArgs) fillJob(job *Job) {
	if job.Version <= JobVersion1 {
		job.Args = []any{a.ColName, a.IfExists, a.IndexIDs, a.PartitionIDs}
	} else {
		job.Args = []any{a}
	}
}

// GetDropColumnArgs gets the args for drop column ddl.
func GetDropColumnArgs(job *Job) (*DropColumnArgs, error) {
	var (
		colName      pmodel.CIStr
		ifExists     bool
		indexIDs     []int64
		partitionIDs []int64
	)

	if job.Version <= JobVersion1 {
		err := job.DecodeArgs(&colName, &ifExists, &indexIDs, &partitionIDs)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return &DropColumnArgs{
			ColName:      colName,
			IfExists:     ifExists,
			IndexIDs:     indexIDs,
			PartitionIDs: partitionIDs,
		}, nil
	}

	return getOrDecodeArgsV2[*DropColumnArgs](job)
}

// RenameTablesArgs is the arguments for rename tables job.
type RenameTablesArgs struct {
	RenameTableInfos []*RenameTableArgs `json:"rename_table_infos,omitempty"`
}

func (a *RenameTablesArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		n := len(a.RenameTableInfos)
		oldSchemaIDs := make([]int64, n)
		oldSchemaNames := make([]pmodel.CIStr, n)
		oldTableNames := make([]pmodel.CIStr, n)
		newSchemaIDs := make([]int64, n)
		newTableNames := make([]pmodel.CIStr, n)
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
		job.Args = []any{oldSchemaIDs, newSchemaIDs, newTableNames, tableIDs, oldSchemaNames, oldTableNames}
		return
	}

	job.Args = []any{a}
}

// GetRenameTablesArgsFromV1 get v2 args from v1
func GetRenameTablesArgsFromV1(
	oldSchemaIDs []int64,
	oldSchemaNames []pmodel.CIStr,
	oldTableNames []pmodel.CIStr,
	newSchemaIDs []int64,
	newTableNames []pmodel.CIStr,
	tableIDs []int64,
) *RenameTablesArgs {
	infos := make([]*RenameTableArgs, 0, len(oldSchemaIDs))
	for i, oldSchemaID := range oldSchemaIDs {
		infos = append(infos, &RenameTableArgs{
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
			oldSchemaNames []pmodel.CIStr
			oldTableNames  []pmodel.CIStr
			newSchemaIDs   []int64
			newTableNames  []pmodel.CIStr
			tableIDs       []int64
		)
		if err := job.DecodeArgs(
			&oldSchemaIDs, &newSchemaIDs, &newTableNames,
			&tableIDs, &oldSchemaNames, &oldTableNames); err != nil {
			return nil, errors.Trace(err)
		}

		return GetRenameTablesArgsFromV1(
			oldSchemaIDs, oldSchemaNames, oldTableNames,
			newSchemaIDs, newTableNames, tableIDs), nil
	}
	return getOrDecodeArgsV2[*RenameTablesArgs](job)
}

// GetCheckConstraintArgs gets the AlterCheckConstraint args.
func GetCheckConstraintArgs(job *Job) (*CheckConstraintArgs, error) {
	if job.Version == JobVersion1 {
		var (
			constraintName pmodel.CIStr
			enforced       bool
		)
		err := job.DecodeArgs(&constraintName, &enforced)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &CheckConstraintArgs{
			ConstraintName: constraintName,
			Enforced:       enforced,
		}, nil
	}

	return getOrDecodeArgsV2[*CheckConstraintArgs](job)
}

// AddCheckConstraintArgs is the arguemnt for add check constraint
type AddCheckConstraintArgs struct {
	Constraint *ConstraintInfo `json:"constraint_info"`
}

func (a *AddCheckConstraintArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		job.Args = []any{a.Constraint}
		return
	}
	job.Args = []any{a}
}

// GetAddCheckConstraintArgs gets the AddCheckConstraint args.
func GetAddCheckConstraintArgs(job *Job) (*AddCheckConstraintArgs, error) {
	if job.Version == JobVersion1 {
		var constraintInfo ConstraintInfo
		err := job.DecodeArgs(&constraintInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &AddCheckConstraintArgs{
			Constraint: &constraintInfo,
		}, nil
	}
	return getOrDecodeArgsV2[*AddCheckConstraintArgs](job)
}

// PlacementPolicyArgs is the argument for create/alter/drop placement policy
type PlacementPolicyArgs struct {
	Policy         *PolicyInfo  `json:"policy,omitempty"`
	ReplaceOnExist bool         `json:"replace_on_exist,omitempty"`
	PolicyName     pmodel.CIStr `json:"policy_name,omitempty"`

	// it's set for alter/drop policy in v2
	PolicyID int64 `json:"policy_id"`
}

func (a *PlacementPolicyArgs) fillJob(job *Job) {
	if job.Version == JobVersion1 {
		if job.Type == ActionCreatePlacementPolicy {
			job.Args = []any{a.Policy, a.ReplaceOnExist}
		} else if job.Type == ActionAlterPlacementPolicy {
			job.Args = []any{a.Policy}
		} else {
			intest.Assert(job.Type == ActionDropPlacementPolicy, "Invalid job type for PlacementPolicyArgs")
			job.Args = []any{a.PolicyName}
		}
		return
	}
	job.Args = []any{a}
}

// GetPlacementPolicyArgs gets the placement policy args.
func GetPlacementPolicyArgs(job *Job) (*PlacementPolicyArgs, error) {
	if job.Version == JobVersion1 {
		args := &PlacementPolicyArgs{PolicyID: job.SchemaID}
		var err error

		if job.Type == ActionCreatePlacementPolicy {
			err = job.DecodeArgs(&args.Policy, &args.ReplaceOnExist)
		} else if job.Type == ActionAlterPlacementPolicy {
			err = job.DecodeArgs(&args.Policy)
		} else {
			intest.Assert(job.Type == ActionDropPlacementPolicy, "Invalid job type for PlacementPolicyArgs")
			err = job.DecodeArgs(&args.PolicyName)
		}

		if err != nil {
			return nil, errors.Trace(err)
		}

		return args, err
	}

	return getOrDecodeArgsV2[*PlacementPolicyArgs](job)
}
