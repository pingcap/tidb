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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// RenameTableArgs is the arguments for rename table DDL job.
// It's also used for rename tables.
type RenameTableArgs struct {
	// for Args
	OldSchemaID   int64     `json:"old_schema_id,omitempty"`
	OldSchemaName ast.CIStr `json:"old_schema_name,omitempty"`
	NewTableName  ast.CIStr `json:"new_table_name,omitempty"`

	// for rename tables
	OldTableName ast.CIStr `json:"old_table_name,omitempty"`
	NewSchemaID  int64     `json:"new_schema_id,omitempty"`
	TableID      int64     `json:"table_id,omitempty"`

	// runtime info
	OldSchemaIDForSchemaDiff int64 `json:"-"`
}

func (rt *RenameTableArgs) getArgsV1(*Job) []any {
	return []any{rt.OldSchemaID, rt.NewTableName, rt.OldSchemaName}
}

func (rt *RenameTableArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&rt.OldSchemaID, &rt.NewTableName, &rt.OldSchemaName))
}

// GetRenameTableArgs get the arguments from job.
func GetRenameTableArgs(job *Job) (*RenameTableArgs, error) {
	args, err := getOrDecodeArgs[*RenameTableArgs](&RenameTableArgs{}, job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// NewSchemaID is used for checkAndRenameTables, which is not set for rename table.
	args.NewSchemaID = job.SchemaID
	return args, nil
}

// ResourceGroupArgs is the arguments for resource group job.
type ResourceGroupArgs struct {
	// for DropResourceGroup we only use it to store the name, other fields are invalid.
	RGInfo *ResourceGroupInfo `json:"rg_info,omitempty"`
}

func (a *ResourceGroupArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionCreateResourceGroup {
		// what's the second parameter for? we keep it for compatibility.
		return []any{a.RGInfo, false}
	} else if job.Type == ActionAlterResourceGroup {
		return []any{a.RGInfo}
	} else if job.Type == ActionDropResourceGroup {
		// it's not used anywhere.
		return []any{a.RGInfo.Name}
	}
	return nil
}

func (a *ResourceGroupArgs) decodeV1(job *Job) error {
	a.RGInfo = &ResourceGroupInfo{}
	if job.Type == ActionCreateResourceGroup || job.Type == ActionAlterResourceGroup {
		return errors.Trace(job.decodeArgs(a.RGInfo))
	} else if job.Type == ActionDropResourceGroup {
		return errors.Trace(job.decodeArgs(&a.RGInfo.Name))
	}
	return nil
}

// GetResourceGroupArgs gets the resource group args.
func GetResourceGroupArgs(job *Job) (*ResourceGroupArgs, error) {
	return getOrDecodeArgs[*ResourceGroupArgs](&ResourceGroupArgs{}, job)
}

// RebaseAutoIDArgs is the arguments for ActionRebaseAutoID DDL.
// It is also for ActionRebaseAutoRandomBase.
type RebaseAutoIDArgs struct {
	NewBase int64 `json:"new_base,omitempty"`
	Force   bool  `json:"force,omitempty"`
}

func (a *RebaseAutoIDArgs) getArgsV1(*Job) []any {
	return []any{a.NewBase, a.Force}
}

func (a *RebaseAutoIDArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.NewBase, &a.Force))
}

// GetRebaseAutoIDArgs the args for ActionRebaseAutoID/ActionRebaseAutoRandomBase ddl.
func GetRebaseAutoIDArgs(job *Job) (*RebaseAutoIDArgs, error) {
	return getOrDecodeArgs[*RebaseAutoIDArgs](&RebaseAutoIDArgs{}, job)
}

// ModifyTableCommentArgs is the arguments for ActionModifyTableComment ddl.
type ModifyTableCommentArgs struct {
	Comment string `json:"comment,omitempty"`
}

func (a *ModifyTableCommentArgs) getArgsV1(*Job) []any {
	return []any{a.Comment}
}

func (a *ModifyTableCommentArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Comment))
}

// GetModifyTableCommentArgs gets the args for ActionModifyTableComment.
func GetModifyTableCommentArgs(job *Job) (*ModifyTableCommentArgs, error) {
	return getOrDecodeArgs[*ModifyTableCommentArgs](&ModifyTableCommentArgs{}, job)
}

// ModifyTableCharsetAndCollateArgs is the arguments for ActionModifyTableCharsetAndCollate ddl.
type ModifyTableCharsetAndCollateArgs struct {
	ToCharset          string `json:"to_charset,omitempty"`
	ToCollate          string `json:"to_collate,omitempty"`
	NeedsOverwriteCols bool   `json:"needs_overwrite_cols,omitempty"`
}

func (a *ModifyTableCharsetAndCollateArgs) getArgsV1(*Job) []any {
	return []any{a.ToCharset, a.ToCollate, a.NeedsOverwriteCols}
}

func (a *ModifyTableCharsetAndCollateArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.ToCharset, &a.ToCollate, &a.NeedsOverwriteCols))
}

// GetModifyTableCharsetAndCollateArgs gets the args for ActionModifyTableCharsetAndCollate ddl.
func GetModifyTableCharsetAndCollateArgs(job *Job) (*ModifyTableCharsetAndCollateArgs, error) {
	return getOrDecodeArgs[*ModifyTableCharsetAndCollateArgs](&ModifyTableCharsetAndCollateArgs{}, job)
}

// AlterIndexVisibilityArgs is the arguments for ActionAlterIndexVisibility ddl.
type AlterIndexVisibilityArgs struct {
	IndexName ast.CIStr `json:"index_name,omitempty"`
	Invisible bool      `json:"invisible,omitempty"`
}

func (a *AlterIndexVisibilityArgs) getArgsV1(*Job) []any {
	return []any{a.IndexName, a.Invisible}
}

func (a *AlterIndexVisibilityArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.IndexName, &a.Invisible))
}

// GetAlterIndexVisibilityArgs gets the args for AlterIndexVisibility ddl.
func GetAlterIndexVisibilityArgs(job *Job) (*AlterIndexVisibilityArgs, error) {
	return getOrDecodeArgs[*AlterIndexVisibilityArgs](&AlterIndexVisibilityArgs{}, job)
}

// AddForeignKeyArgs is the arguments for ActionAddForeignKey ddl.
type AddForeignKeyArgs struct {
	FkInfo  *FKInfo `json:"fk_info,omitempty"`
	FkCheck bool    `json:"fk_check,omitempty"`
}

func (a *AddForeignKeyArgs) getArgsV1(*Job) []any {
	return []any{a.FkInfo, a.FkCheck}
}

func (a *AddForeignKeyArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.FkInfo, &a.FkCheck))
}

// GetAddForeignKeyArgs get the args for AddForeignKey ddl.
func GetAddForeignKeyArgs(job *Job) (*AddForeignKeyArgs, error) {
	return getOrDecodeArgs[*AddForeignKeyArgs](&AddForeignKeyArgs{}, job)
}

// DropForeignKeyArgs is the arguments for DropForeignKey ddl.
type DropForeignKeyArgs struct {
	FkName ast.CIStr `json:"fk_name,omitempty"`
}

func (a *DropForeignKeyArgs) getArgsV1(*Job) []any {
	return []any{a.FkName}
}

func (a *DropForeignKeyArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.FkName))
}

// GetDropForeignKeyArgs gets the args for DropForeignKey ddl.
func GetDropForeignKeyArgs(job *Job) (*DropForeignKeyArgs, error) {
	return getOrDecodeArgs[*DropForeignKeyArgs](&DropForeignKeyArgs{}, job)
}

// TableColumnArgs is the arguments for dropping column ddl or Adding column ddl.
type TableColumnArgs struct {
	// follow items for add column.
	Col    *ColumnInfo         `json:"column_info,omitempty"`
	Pos    *ast.ColumnPosition `json:"position,omitempty"`
	Offset int                 `json:"offset,omitempty"`
	// it's shared by add/drop column.
	IgnoreExistenceErr bool `json:"ignore_existence_err,omitempty"`

	// for drop column.
	// below 2 fields are filled during running, and PartitionIDs is only effective
	// when len(IndexIDs) > 0.
	IndexIDs     []int64 `json:"index_ids,omitempty"`
	PartitionIDs []int64 `json:"partition_ids,omitempty"`
}

func (a *TableColumnArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionDropColumn {
		// if this job is submitted by new version node, but run with older version
		// node, older node will try to append args at runtime, so we check it here
		// to make sure the appended args can be decoded.
		if len(a.IndexIDs) > 0 {
			return []any{a.Col.Name, a.IgnoreExistenceErr, a.IndexIDs, a.PartitionIDs}
		}
		return []any{a.Col.Name, a.IgnoreExistenceErr}
	}
	return []any{a.Col, a.Pos, a.Offset, a.IgnoreExistenceErr}
}

func (a *TableColumnArgs) decodeV1(job *Job) error {
	a.Col = &ColumnInfo{}
	a.Pos = &ast.ColumnPosition{}

	// when rollbacking add-columm, it's arguments is same as drop-column
	if job.Type == ActionDropColumn || job.State == JobStateRollingback {
		return errors.Trace(job.decodeArgs(&a.Col.Name, &a.IgnoreExistenceErr, &a.IndexIDs, &a.PartitionIDs))
	}
	// for add column ddl.
	return errors.Trace(job.decodeArgs(a.Col, a.Pos, &a.Offset, &a.IgnoreExistenceErr))
}

// FillRollBackArgsForAddColumn fills the args for rollback add column ddl.
func FillRollBackArgsForAddColumn(job *Job, args *TableColumnArgs) {
	intest.Assert(job.Type == ActionAddColumn, "only for add column job")
	fakeJob := &Job{
		Version: job.Version,
		Type:    ActionDropColumn,
	}
	fakeJob.FillArgs(args)
	job.args = fakeJob.args
}

// GetTableColumnArgs gets the args for dropping column ddl or Adding column ddl.
func GetTableColumnArgs(job *Job) (*TableColumnArgs, error) {
	return getOrDecodeArgs[*TableColumnArgs](&TableColumnArgs{}, job)
}
