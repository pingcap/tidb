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
)

// RenameTablesArgs is the arguments for rename tables job.
type RenameTablesArgs struct {
	RenameTableInfos []*RenameTableArgs `json:"rename_table_infos,omitempty"`
}

func (a *RenameTablesArgs) getArgsV1(*Job) []any {
	n := len(a.RenameTableInfos)
	oldSchemaIDs := make([]int64, n)
	oldSchemaNames := make([]ast.CIStr, n)
	oldTableNames := make([]ast.CIStr, n)
	newSchemaIDs := make([]int64, n)
	newTableNames := make([]ast.CIStr, n)
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
	return []any{oldSchemaIDs, newSchemaIDs, newTableNames, tableIDs, oldSchemaNames, oldTableNames}
}

func (a *RenameTablesArgs) decodeV1(job *Job) error {
	var (
		oldSchemaIDs   []int64
		oldSchemaNames []ast.CIStr
		oldTableNames  []ast.CIStr
		newSchemaIDs   []int64
		newTableNames  []ast.CIStr
		tableIDs       []int64
	)
	if err := job.decodeArgs(
		&oldSchemaIDs, &newSchemaIDs, &newTableNames,
		&tableIDs, &oldSchemaNames, &oldTableNames); err != nil {
		return errors.Trace(err)
	}

	// If the job is run on older TiDB versions(<=8.1), it will incorrectly remove
	// the last arg oldTableNames.
	// See https://github.com/pingcap/tidb/blob/293331cd9211c214f3431ff789210374378e9697/pkg/ddl/ddl_worker.go#L1442-L1447
	if len(oldTableNames) == 0 && len(oldSchemaIDs) != 0 {
		oldTableNames = make([]ast.CIStr, len(oldSchemaIDs))
	}

	a.RenameTableInfos = GetRenameTablesArgsFromV1(
		oldSchemaIDs, oldSchemaNames, oldTableNames,
		newSchemaIDs, newTableNames, tableIDs,
	)
	return nil
}

// GetRenameTablesArgsFromV1 get v2 args from v1
func GetRenameTablesArgsFromV1(
	oldSchemaIDs []int64,
	oldSchemaNames []ast.CIStr,
	oldTableNames []ast.CIStr,
	newSchemaIDs []int64,
	newTableNames []ast.CIStr,
	tableIDs []int64,
) []*RenameTableArgs {
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

	return infos
}

// GetRenameTablesArgs gets the rename-tables args.
func GetRenameTablesArgs(job *Job) (*RenameTablesArgs, error) {
	return getOrDecodeArgs[*RenameTablesArgs](&RenameTablesArgs{}, job)
}

// AlterSequenceArgs is the arguments for alter sequence ddl job.
type AlterSequenceArgs struct {
	Ident      ast.Ident             `json:"ident,omitempty"`
	SeqOptions []*ast.SequenceOption `json:"seq_options,omitempty"`
}

func (a *AlterSequenceArgs) getArgsV1(*Job) []any {
	return []any{a.Ident, a.SeqOptions}
}

func (a *AlterSequenceArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Ident, &a.SeqOptions))
}

// GetAlterSequenceArgs gets the args for alter Sequence ddl job.
func GetAlterSequenceArgs(job *Job) (*AlterSequenceArgs, error) {
	return getOrDecodeArgs[*AlterSequenceArgs](&AlterSequenceArgs{}, job)
}

// ModifyTableAutoIDCacheArgs is the arguments for Modify Table AutoID Cache ddl job.
type ModifyTableAutoIDCacheArgs struct {
	NewCache int64 `json:"new_cache,omitempty"`
}

func (a *ModifyTableAutoIDCacheArgs) getArgsV1(*Job) []any {
	return []any{a.NewCache}
}

func (a *ModifyTableAutoIDCacheArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.NewCache))
}

// GetModifyTableAutoIDCacheArgs gets the args for modify table autoID cache ddl job.
func GetModifyTableAutoIDCacheArgs(job *Job) (*ModifyTableAutoIDCacheArgs, error) {
	return getOrDecodeArgs[*ModifyTableAutoIDCacheArgs](&ModifyTableAutoIDCacheArgs{}, job)
}

// ShardRowIDArgs is the arguments for shard row ID ddl job.
type ShardRowIDArgs struct {
	ShardRowIDBits uint64 `json:"shard_row_id_bits,omitempty"`
}

func (a *ShardRowIDArgs) getArgsV1(*Job) []any {
	return []any{a.ShardRowIDBits}
}

func (a *ShardRowIDArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.ShardRowIDBits))
}

// GetShardRowIDArgs gets the args for shard row ID ddl job.
func GetShardRowIDArgs(job *Job) (*ShardRowIDArgs, error) {
	return getOrDecodeArgs[*ShardRowIDArgs](&ShardRowIDArgs{}, job)
}

// AlterTTLInfoArgs is the arguments for alter ttl info job.
type AlterTTLInfoArgs struct {
	TTLInfo            *TTLInfo `json:"ttl_info,omitempty"`
	TTLEnable          *bool    `json:"ttl_enable,omitempty"`
	TTLCronJobSchedule *string  `json:"ttl_cron_job_schedule,omitempty"`
}

func (a *AlterTTLInfoArgs) getArgsV1(*Job) []any {
	return []any{a.TTLInfo, a.TTLEnable, a.TTLCronJobSchedule}
}

func (a *AlterTTLInfoArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TTLInfo, &a.TTLEnable, &a.TTLCronJobSchedule))
}

// GetAlterTTLInfoArgs gets the args for alter ttl info job.
func GetAlterTTLInfoArgs(job *Job) (*AlterTTLInfoArgs, error) {
	return getOrDecodeArgs[*AlterTTLInfoArgs](&AlterTTLInfoArgs{}, job)
}

// CheckConstraintArgs is the arguments for both AlterCheckConstraint and DropCheckConstraint job.
type CheckConstraintArgs struct {
	ConstraintName ast.CIStr `json:"constraint_name,omitempty"`
	Enforced       bool      `json:"enforced,omitempty"`
}

func (a *CheckConstraintArgs) getArgsV1(*Job) []any {
	return []any{a.ConstraintName, a.Enforced}
}

func (a *CheckConstraintArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.ConstraintName, &a.Enforced))
}

// GetCheckConstraintArgs gets the AlterCheckConstraint args.
func GetCheckConstraintArgs(job *Job) (*CheckConstraintArgs, error) {
	return getOrDecodeArgs[*CheckConstraintArgs](&CheckConstraintArgs{}, job)
}

// AddCheckConstraintArgs is the args for add check constraint
type AddCheckConstraintArgs struct {
	Constraint *ConstraintInfo `json:"constraint_info"`
}

func (a *AddCheckConstraintArgs) getArgsV1(*Job) []any {
	return []any{a.Constraint}
}

func (a *AddCheckConstraintArgs) decodeV1(job *Job) error {
	a.Constraint = &ConstraintInfo{}
	return errors.Trace(job.decodeArgs(&a.Constraint))
}

// GetAddCheckConstraintArgs gets the AddCheckConstraint args.
func GetAddCheckConstraintArgs(job *Job) (*AddCheckConstraintArgs, error) {
	return getOrDecodeArgs[*AddCheckConstraintArgs](&AddCheckConstraintArgs{}, job)
}
