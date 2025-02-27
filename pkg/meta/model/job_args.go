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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/intest"
	pdhttp "github.com/tikv/pd/client/http"
)

// AutoIDGroup represents a group of auto IDs of a specific table.
type AutoIDGroup struct {
	RowID       int64
	IncrementID int64
	RandomID    int64
}

// RecoverTableInfo contains information needed by DDL.RecoverTable.
type RecoverTableInfo struct {
	SchemaID      int64
	TableInfo     *TableInfo
	DropJobID     int64
	SnapshotTS    uint64
	AutoIDs       AutoIDGroup
	OldSchemaName string
	OldTableName  string
}

// RecoverSchemaInfo contains information needed by DDL.RecoverSchema.
type RecoverSchemaInfo struct {
	*DBInfo
	RecoverTableInfos []*RecoverTableInfo
	// LoadTablesOnExecute is the new logic to avoid a large RecoverTabsInfo can't be
	// persisted. If it's true, DDL owner will recover RecoverTabsInfo instead of the
	// job submit node.
	LoadTablesOnExecute bool
	DropJobID           int64
	SnapshotTS          uint64
	OldSchemaName       ast.CIStr
}

// getOrDecodeArgsV1 get the args v1 from job, if the job.Args is nil, decode job.RawArgs
// and cache in job.Args.
// as there is no way to create a generic struct with a type parameter in Go, we
// have to pass one instance of the struct to the function.
func getOrDecodeArgsV1[T JobArgs](args T, job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion1, "job version is not v1")
	var v T
	if err := args.decodeV1(job); err != nil {
		return v, errors.Trace(err)
	}
	return args, nil
}

// getOrDecodeArgsV2 get the args v2 from job, if the job.Args is nil, decode job.RawArgs
// and cache in job.Args.
func getOrDecodeArgsV2[T JobArgs](job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion2, "job version is not v2")
	if len(job.args) > 0 {
		intest.Assert(len(job.args) == 1, "job args length is not 1")
		return job.args[0].(T), nil
	}
	var v T
	if err := json.Unmarshal(job.RawArgs, &v); err != nil {
		return v, errors.Trace(err)
	}
	job.args = []any{v}
	return v, nil
}

func getOrDecodeArgs[T JobArgs](args T, job *Job) (T, error) {
	if job.Version == JobVersion1 {
		return getOrDecodeArgsV1[T](args, job)
	}
	return getOrDecodeArgsV2[T](job)
}

// JobArgs is the interface for job arguments.
type JobArgs interface {
	// getArgsV1 gets the job args for v1. we make it private to avoid calling it
	// directly, use Job.FillArgs to fill the job args.
	getArgsV1(job *Job) []any
	decodeV1(job *Job) error
}

// FinishedJobArgs is the interface for finished job arguments.
// in most cases, job args are cleared out after the job is finished, but some jobs
// will write some args back to the job for other components.
type FinishedJobArgs interface {
	JobArgs
	// getFinishedArgsV1 fills the job args for finished job. we make it private
	// to avoid calling it directly, use Job.FillFinishedArgs to fill the job args.
	getFinishedArgsV1(job *Job) []any
}

// EmptyArgs is the args for ddl job with no args.
type EmptyArgs struct{}

func (*EmptyArgs) getArgsV1(*Job) []any {
	return nil
}

func (*EmptyArgs) decodeV1(*Job) error {
	return nil
}

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

// CreateTableArgs is the arguments for create table/view/sequence job.
type CreateTableArgs struct {
	TableInfo *TableInfo `json:"table_info,omitempty"`
	// below 2 are used for create view.
	OnExistReplace bool  `json:"on_exist_replace,omitempty"`
	OldViewTblID   int64 `json:"old_view_tbl_id,omitempty"`
	// used for create table.
	FKCheck bool `json:"fk_check,omitempty"`
}

func (a *CreateTableArgs) getArgsV1(job *Job) []any {
	switch job.Type {
	case ActionCreateTable:
		return []any{a.TableInfo, a.FKCheck}
	case ActionCreateView:
		return []any{a.TableInfo, a.OnExistReplace, a.OldViewTblID}
	case ActionCreateSequence:
		return []any{a.TableInfo}
	}
	return nil
}

func (a *CreateTableArgs) decodeV1(job *Job) error {
	a.TableInfo = &TableInfo{}
	switch job.Type {
	case ActionCreateTable:
		return errors.Trace(job.decodeArgs(a.TableInfo, &a.FKCheck))
	case ActionCreateView:
		return errors.Trace(job.decodeArgs(a.TableInfo, &a.OnExistReplace, &a.OldViewTblID))
	case ActionCreateSequence:
		return errors.Trace(job.decodeArgs(a.TableInfo))
	}
	return nil
}

// GetCreateTableArgs gets the create-table args.
func GetCreateTableArgs(job *Job) (*CreateTableArgs, error) {
	return getOrDecodeArgs[*CreateTableArgs](&CreateTableArgs{}, job)
}

// BatchCreateTableArgs is the arguments for batch create table job.
type BatchCreateTableArgs struct {
	Tables []*CreateTableArgs `json:"tables,omitempty"`
}

func (a *BatchCreateTableArgs) getArgsV1(*Job) []any {
	infos := make([]*TableInfo, 0, len(a.Tables))
	for _, info := range a.Tables {
		infos = append(infos, info.TableInfo)
	}
	return []any{infos, a.Tables[0].FKCheck}
}

func (a *BatchCreateTableArgs) decodeV1(job *Job) error {
	var (
		tableInfos []*TableInfo
		fkCheck    bool
	)
	if err := job.decodeArgs(&tableInfos, &fkCheck); err != nil {
		return errors.Trace(err)
	}
	a.Tables = make([]*CreateTableArgs, 0, len(tableInfos))
	for _, info := range tableInfos {
		a.Tables = append(a.Tables, &CreateTableArgs{TableInfo: info, FKCheck: fkCheck})
	}
	return nil
}

// GetBatchCreateTableArgs gets the batch create-table args.
func GetBatchCreateTableArgs(job *Job) (*BatchCreateTableArgs, error) {
	return getOrDecodeArgs[*BatchCreateTableArgs](&BatchCreateTableArgs{}, job)
}

// DropTableArgs is the arguments for drop table/view/sequence job.
// when dropping multiple objects, each object will have a separate job
type DropTableArgs struct {
	// below fields are only for drop table.
	// when dropping multiple tables, the Identifiers is the same, but each drop-table
	// runs in a separate job.
	Identifiers []ast.Ident `json:"identifiers,omitempty"`
	FKCheck     bool        `json:"fk_check,omitempty"`

	// below fields are finished job args
	StartKey        []byte   `json:"start_key,omitempty"`
	OldPartitionIDs []int64  `json:"old_partition_ids,omitempty"`
	OldRuleIDs      []string `json:"old_rule_ids,omitempty"`
}

func (a *DropTableArgs) getArgsV1(job *Job) []any {
	// only drop-table job has in args, drop view/sequence job has no args.
	if job.Type == ActionDropTable {
		return []any{a.Identifiers, a.FKCheck}
	}
	return nil
}

func (a *DropTableArgs) getFinishedArgsV1(*Job) []any {
	return []any{a.StartKey, a.OldPartitionIDs, a.OldRuleIDs}
}

func (a *DropTableArgs) decodeV1(job *Job) error {
	if job.Type == ActionDropTable {
		return job.decodeArgs(&a.Identifiers, &a.FKCheck)
	}
	return nil
}

// GetDropTableArgs gets the drop-table args.
func GetDropTableArgs(job *Job) (*DropTableArgs, error) {
	return getOrDecodeArgs[*DropTableArgs](&DropTableArgs{}, job)
}

// GetFinishedDropTableArgs gets the drop-table args after the job is finished.
func GetFinishedDropTableArgs(job *Job) (*DropTableArgs, error) {
	if job.Version == JobVersion1 {
		var (
			startKey        []byte
			oldPartitionIDs []int64
			oldRuleIDs      []string
		)
		if err := job.decodeArgs(&startKey, &oldPartitionIDs, &oldRuleIDs); err != nil {
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

// TruncateTableArgs is the arguments for truncate table/partition job.
type TruncateTableArgs struct {
	FKCheck         bool    `json:"fk_check,omitempty"`
	NewTableID      int64   `json:"new_table_id,omitempty"`
	NewPartitionIDs []int64 `json:"new_partition_ids,omitempty"`
	OldPartitionIDs []int64 `json:"old_partition_ids,omitempty"`

	// context vars
	NewPartIDsWithPolicy           []int64 `json:"-"`
	OldPartIDsWithPolicy           []int64 `json:"-"`
	ShouldUpdateAffectedPartitions bool    `json:"-"`
}

func (a *TruncateTableArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionTruncateTable {
		// Args[0] is the new table ID, args[2] is the ids for table partitions, we
		// add a placeholder here, they will be filled by job submitter.
		// the last param is not required for execution, we need it to calculate
		// number of new IDs to generate.
		return []any{a.NewTableID, a.FKCheck, a.NewPartitionIDs, len(a.OldPartitionIDs)}
	}
	return []any{a.OldPartitionIDs, a.NewPartitionIDs}
}

func (a *TruncateTableArgs) decodeV1(job *Job) error {
	if job.Type == ActionTruncateTable {
		return errors.Trace(job.decodeArgs(&a.NewTableID, &a.FKCheck, &a.NewPartitionIDs))
	}
	return errors.Trace(job.decodeArgs(&a.OldPartitionIDs, &a.NewPartitionIDs))
}

func (a *TruncateTableArgs) getFinishedArgsV1(job *Job) []any {
	if job.Type == ActionTruncateTable {
		// the first param is the start key of the old table, it's not used anywhere
		// now, so we fill an empty byte slice here.
		// we can call tablecodec.EncodeTablePrefix(tableID) to get it.
		return []any{[]byte{}, a.OldPartitionIDs}
	}
	return []any{a.OldPartitionIDs}
}

// GetTruncateTableArgs gets the truncate table args.
func GetTruncateTableArgs(job *Job) (*TruncateTableArgs, error) {
	return getOrDecodeArgs[*TruncateTableArgs](&TruncateTableArgs{}, job)
}

// GetFinishedTruncateTableArgs gets the truncate table args after the job is finished.
func GetFinishedTruncateTableArgs(job *Job) (*TruncateTableArgs, error) {
	if job.Version == JobVersion1 {
		if job.Type == ActionTruncateTable {
			var startKey []byte
			var oldPartitionIDs []int64
			if err := job.decodeArgs(&startKey, &oldPartitionIDs); err != nil {
				return nil, errors.Trace(err)
			}
			return &TruncateTableArgs{OldPartitionIDs: oldPartitionIDs}, nil
		}
		var oldPartitionIDs []int64
		if err := job.decodeArgs(&oldPartitionIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &TruncateTableArgs{OldPartitionIDs: oldPartitionIDs}, nil
	}

	return getOrDecodeArgsV2[*TruncateTableArgs](job)
}

// TableIDIndexID contains TableID+IndexID of index ranges to be deleted
type TableIDIndexID struct {
	TableID int64
	IndexID int64
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
	OldPhysicalTblIDs []int64          `json:"old_physical_tbl_ids,omitempty"`
	OldGlobalIndexes  []TableIDIndexID `json:"old_global_indexes,omitempty"`

	// runtime info
	NewPartitionIDs []int64 `json:"-"`
}

func (a *TablePartitionArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionAddTablePartition {
		return []any{a.PartInfo}
	} else if job.Type == ActionDropTablePartition {
		return []any{a.PartNames}
	}
	return []any{a.PartNames, a.PartInfo}
}

func (a *TablePartitionArgs) getFinishedArgsV1(job *Job) []any {
	intest.Assert(job.Type != ActionAddTablePartition || job.State == JobStateRollbackDone,
		"add table partition job should not call getFinishedArgsV1 if not rollback")
	return []any{a.OldPhysicalTblIDs, a.OldGlobalIndexes}
}

func (a *TablePartitionArgs) decodeV1(job *Job) error {
	var (
		partNames []string
		partInfo  = &PartitionInfo{}
	)
	if job.Type == ActionAddTablePartition {
		if job.State == JobStateRollingback {
			if err := job.decodeArgs(&partNames); err != nil {
				return err
			}
		} else {
			if err := job.decodeArgs(partInfo); err != nil {
				return err
			}
		}
	} else if job.Type == ActionDropTablePartition {
		if err := job.decodeArgs(&partNames); err != nil {
			return err
		}
	} else {
		if err := job.decodeArgs(&partNames, partInfo); err != nil {
			return err
		}
	}
	a.PartNames = partNames
	a.PartInfo = partInfo
	return nil
}

// GetTablePartitionArgs gets the table partition args.
func GetTablePartitionArgs(job *Job) (*TablePartitionArgs, error) {
	args, err := getOrDecodeArgs[*TablePartitionArgs](&TablePartitionArgs{}, job)
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
		var oldIndexes []TableIDIndexID
		if err := job.decodeArgs(&oldPhysicalTblIDs, &oldIndexes); err != nil {
			return nil, errors.Trace(err)
		}
		return &TablePartitionArgs{OldPhysicalTblIDs: oldPhysicalTblIDs, OldGlobalIndexes: oldIndexes}, nil
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
	job.args = fake.args
}

// ExchangeTablePartitionArgs is the arguments for exchange table partition job.
// pt: the partition table to exchange
// nt: the non-partition table to exchange with
type ExchangeTablePartitionArgs struct {
	PartitionID    int64  `json:"partition_id,omitempty"`
	PTSchemaID     int64  `json:"pt_schema_id,omitempty"`
	PTTableID      int64  `json:"pt_table_id,omitempty"`
	PartitionName  string `json:"partition_name,omitempty"`
	WithValidation bool   `json:"with_validation,omitempty"`
}

func (a *ExchangeTablePartitionArgs) getArgsV1(*Job) []any {
	return []any{a.PartitionID, a.PTSchemaID, a.PTTableID, a.PartitionName, a.WithValidation}
}

func (a *ExchangeTablePartitionArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.PartitionID, &a.PTSchemaID, &a.PTTableID, &a.PartitionName, &a.WithValidation))
}

// GetExchangeTablePartitionArgs gets the exchange table partition args.
func GetExchangeTablePartitionArgs(job *Job) (*ExchangeTablePartitionArgs, error) {
	return getOrDecodeArgs[*ExchangeTablePartitionArgs](&ExchangeTablePartitionArgs{}, job)
}

// AlterTablePartitionArgs is the arguments for alter table partition job.
// it's used for:
//   - ActionAlterTablePartitionAttributes
//   - ActionAlterTablePartitionPlacement
type AlterTablePartitionArgs struct {
	PartitionID   int64             `json:"partition_id,omitempty"`
	LabelRule     *pdhttp.LabelRule `json:"label_rule,omitempty"`
	PolicyRefInfo *PolicyRefInfo    `json:"policy_ref_info,omitempty"`
}

func (a *AlterTablePartitionArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionAlterTablePartitionAttributes {
		return []any{a.PartitionID, a.LabelRule}
	}
	return []any{a.PartitionID, a.PolicyRefInfo}
}

func (a *AlterTablePartitionArgs) decodeV1(job *Job) error {
	if job.Type == ActionAlterTablePartitionAttributes {
		return errors.Trace(job.decodeArgs(&a.PartitionID, &a.LabelRule))
	}
	return errors.Trace(job.decodeArgs(&a.PartitionID, &a.PolicyRefInfo))
}

// GetAlterTablePartitionArgs gets the alter table partition args.
func GetAlterTablePartitionArgs(job *Job) (*AlterTablePartitionArgs, error) {
	return getOrDecodeArgs[*AlterTablePartitionArgs](&AlterTablePartitionArgs{}, job)
}

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

// AlterTablePlacementArgs is the arguments for alter table placements ddl job.
type AlterTablePlacementArgs struct {
	PlacementPolicyRef *PolicyRefInfo `json:"placement_policy_ref,omitempty"`
}

func (a *AlterTablePlacementArgs) getArgsV1(*Job) []any {
	return []any{a.PlacementPolicyRef}
}

func (a *AlterTablePlacementArgs) decodeV1(job *Job) error {
	// when the target policy is 'default', policy info is nil
	return errors.Trace(job.decodeArgs(&a.PlacementPolicyRef))
}

// GetAlterTablePlacementArgs gets the args for alter table placements ddl job.
func GetAlterTablePlacementArgs(job *Job) (*AlterTablePlacementArgs, error) {
	return getOrDecodeArgs[*AlterTablePlacementArgs](&AlterTablePlacementArgs{}, job)
}

// SetTiFlashReplicaArgs is the arguments for setting TiFlash replica ddl.
type SetTiFlashReplicaArgs struct {
	TiflashReplica ast.TiFlashReplicaSpec `json:"tiflash_replica,omitempty"`
}

func (a *SetTiFlashReplicaArgs) getArgsV1(*Job) []any {
	return []any{a.TiflashReplica}
}

func (a *SetTiFlashReplicaArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TiflashReplica))
}

// GetSetTiFlashReplicaArgs gets the args for setting TiFlash replica ddl.
func GetSetTiFlashReplicaArgs(job *Job) (*SetTiFlashReplicaArgs, error) {
	return getOrDecodeArgs[*SetTiFlashReplicaArgs](&SetTiFlashReplicaArgs{}, job)
}

// UpdateTiFlashReplicaStatusArgs is the arguments for updating TiFlash replica status ddl.
type UpdateTiFlashReplicaStatusArgs struct {
	Available  bool  `json:"available,omitempty"`
	PhysicalID int64 `json:"physical_id,omitempty"`
}

func (a *UpdateTiFlashReplicaStatusArgs) getArgsV1(*Job) []any {
	return []any{a.Available, a.PhysicalID}
}

func (a *UpdateTiFlashReplicaStatusArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Available, &a.PhysicalID))
}

// GetUpdateTiFlashReplicaStatusArgs gets the args for updating TiFlash replica status ddl.
func GetUpdateTiFlashReplicaStatusArgs(job *Job) (*UpdateTiFlashReplicaStatusArgs, error) {
	return getOrDecodeArgs[*UpdateTiFlashReplicaStatusArgs](&UpdateTiFlashReplicaStatusArgs{}, job)
}

// LockTablesArgs is the argument for LockTables.
type LockTablesArgs struct {
	LockTables    []TableLockTpInfo `json:"lock_tables,omitempty"`
	IndexOfLock   int               `json:"index_of_lock,omitempty"`
	UnlockTables  []TableLockTpInfo `json:"unlock_tables,omitempty"`
	IndexOfUnlock int               `json:"index_of_unlock,omitempty"`
	SessionInfo   SessionInfo       `json:"session_info,omitempty"`
	IsCleanup     bool              `json:"is_cleanup:omitempty"`
}

func (a *LockTablesArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *LockTablesArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetLockTablesArgs get the LockTablesArgs argument.
func GetLockTablesArgs(job *Job) (*LockTablesArgs, error) {
	return getOrDecodeArgs[*LockTablesArgs](&LockTablesArgs{}, job)
}

// RepairTableArgs is the argument for repair table
type RepairTableArgs struct {
	TableInfo *TableInfo `json:"table_info"`
}

func (a *RepairTableArgs) getArgsV1(*Job) []any {
	return []any{a.TableInfo}
}

func (a *RepairTableArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TableInfo))
}

// GetRepairTableArgs get the repair table args.
func GetRepairTableArgs(job *Job) (*RepairTableArgs, error) {
	return getOrDecodeArgs[*RepairTableArgs](&RepairTableArgs{}, job)
}

// AlterTableAttributesArgs is the argument for alter table attributes
type AlterTableAttributesArgs struct {
	LabelRule *pdhttp.LabelRule `json:"label_rule,omitempty"`
}

func (a *AlterTableAttributesArgs) getArgsV1(*Job) []any {
	return []any{a.LabelRule}
}

func (a *AlterTableAttributesArgs) decodeV1(job *Job) error {
	a.LabelRule = &pdhttp.LabelRule{}
	return errors.Trace(job.decodeArgs(a.LabelRule))
}

// GetAlterTableAttributesArgs get alter table attribute args from job.
func GetAlterTableAttributesArgs(job *Job) (*AlterTableAttributesArgs, error) {
	return getOrDecodeArgs[*AlterTableAttributesArgs](&AlterTableAttributesArgs{}, job)
}

// RecoverArgs is the argument for recover table/schema.
type RecoverArgs struct {
	RecoverInfo *RecoverSchemaInfo `json:"recover_info,omitempty"`
	CheckFlag   int64              `json:"check_flag,omitempty"`

	// used during runtime
	AffectedPhysicalIDs []int64 `json:"-"`
}

func (a *RecoverArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionRecoverTable {
		return []any{a.RecoverTableInfos()[0], a.CheckFlag}
	}
	return []any{a.RecoverInfo, a.CheckFlag}
}

func (a *RecoverArgs) decodeV1(job *Job) error {
	var (
		recoverTableInfo  *RecoverTableInfo
		recoverSchemaInfo = &RecoverSchemaInfo{}
		recoverCheckFlag  int64
	)
	if job.Type == ActionRecoverTable {
		err := job.decodeArgs(&recoverTableInfo, &recoverCheckFlag)
		if err != nil {
			return errors.Trace(err)
		}
		recoverSchemaInfo.RecoverTableInfos = []*RecoverTableInfo{recoverTableInfo}
	} else {
		err := job.decodeArgs(recoverSchemaInfo, &recoverCheckFlag)
		if err != nil {
			return errors.Trace(err)
		}
	}
	a.RecoverInfo = recoverSchemaInfo
	a.CheckFlag = recoverCheckFlag
	return nil
}

// RecoverTableInfos get all the recover infos.
func (a *RecoverArgs) RecoverTableInfos() []*RecoverTableInfo {
	return a.RecoverInfo.RecoverTableInfos
}

// GetRecoverArgs get the recover table/schema args.
func GetRecoverArgs(job *Job) (*RecoverArgs, error) {
	return getOrDecodeArgs[*RecoverArgs](&RecoverArgs{}, job)
}

// PlacementPolicyArgs is the argument for create/alter/drop placement policy
type PlacementPolicyArgs struct {
	Policy         *PolicyInfo `json:"policy,omitempty"`
	ReplaceOnExist bool        `json:"replace_on_exist,omitempty"`
	PolicyName     ast.CIStr   `json:"policy_name,omitempty"`

	// it's set for alter/drop policy in v2
	PolicyID int64 `json:"policy_id"`
}

func (a *PlacementPolicyArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionCreatePlacementPolicy {
		return []any{a.Policy, a.ReplaceOnExist}
	} else if job.Type == ActionAlterPlacementPolicy {
		return []any{a.Policy}
	}
	return []any{a.PolicyName}
}

func (a *PlacementPolicyArgs) decodeV1(job *Job) error {
	a.PolicyID = job.SchemaID

	if job.Type == ActionCreatePlacementPolicy {
		return errors.Trace(job.decodeArgs(&a.Policy, &a.ReplaceOnExist))
	} else if job.Type == ActionAlterPlacementPolicy {
		return errors.Trace(job.decodeArgs(&a.Policy))
	}
	return errors.Trace(job.decodeArgs(&a.PolicyName))
}

// GetPlacementPolicyArgs gets the placement policy args.
func GetPlacementPolicyArgs(job *Job) (*PlacementPolicyArgs, error) {
	return getOrDecodeArgs[*PlacementPolicyArgs](&PlacementPolicyArgs{}, job)
}

// SetDefaultValueArgs is the argument for setting default value ddl.
type SetDefaultValueArgs struct {
	Col *ColumnInfo `json:"column_info,omitempty"`
}

func (a *SetDefaultValueArgs) getArgsV1(*Job) []any {
	return []any{a.Col}
}

func (a *SetDefaultValueArgs) decodeV1(job *Job) error {
	a.Col = &ColumnInfo{}
	return errors.Trace(job.decodeArgs(a.Col))
}

// GetSetDefaultValueArgs get the args for setting default value ddl.
func GetSetDefaultValueArgs(job *Job) (*SetDefaultValueArgs, error) {
	return getOrDecodeArgs[*SetDefaultValueArgs](&SetDefaultValueArgs{}, job)
}

// KeyRange is copied from kv.KeyRange to avoid cycle import.
// Unused fields are removed.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// FlashbackClusterArgs is the argument for flashback cluster.
type FlashbackClusterArgs struct {
	FlashbackTS        uint64         `json:"flashback_ts,omitempty"`
	PDScheduleValue    map[string]any `json:"pd_schedule_value,omitempty"`
	EnableGC           bool           `json:"enable_gc,omitempty"`
	EnableAutoAnalyze  bool           `json:"enable_auto_analyze,omitempty"`
	EnableTTLJob       bool           `json:"enable_ttl_job,omitempty"`
	SuperReadOnly      bool           `json:"super_read_only,omitempty"`
	LockedRegionCnt    uint64         `json:"locked_region_cnt,omitempty"`
	StartTS            uint64         `json:"start_ts,omitempty"`
	CommitTS           uint64         `json:"commit_ts,omitempty"`
	FlashbackKeyRanges []KeyRange     `json:"key_ranges,omitempty"`
}

func (a *FlashbackClusterArgs) getArgsV1(*Job) []any {
	enableAutoAnalyze := "ON"
	superReadOnly := "ON"
	enableTTLJob := "ON"
	if !a.EnableAutoAnalyze {
		enableAutoAnalyze = "OFF"
	}
	if !a.SuperReadOnly {
		superReadOnly = "OFF"
	}
	if !a.EnableTTLJob {
		enableTTLJob = "OFF"
	}

	return []any{
		a.FlashbackTS, a.PDScheduleValue, a.EnableGC,
		enableAutoAnalyze, superReadOnly, a.LockedRegionCnt,
		a.StartTS, a.CommitTS, enableTTLJob, a.FlashbackKeyRanges,
	}
}

func (a *FlashbackClusterArgs) decodeV1(job *Job) error {
	var autoAnalyzeValue, readOnlyValue, ttlJobEnableValue string

	if err := job.decodeArgs(
		&a.FlashbackTS, &a.PDScheduleValue, &a.EnableGC,
		&autoAnalyzeValue, &readOnlyValue, &a.LockedRegionCnt,
		&a.StartTS, &a.CommitTS, &ttlJobEnableValue, &a.FlashbackKeyRanges,
	); err != nil {
		return errors.Trace(err)
	}

	if autoAnalyzeValue == "ON" {
		a.EnableAutoAnalyze = true
	}
	if readOnlyValue == "ON" {
		a.SuperReadOnly = true
	}
	if ttlJobEnableValue == "ON" {
		a.EnableTTLJob = true
	}

	return nil
}

// GetFlashbackClusterArgs get the flashback cluster argument from job.
func GetFlashbackClusterArgs(job *Job) (*FlashbackClusterArgs, error) {
	return getOrDecodeArgs[*FlashbackClusterArgs](&FlashbackClusterArgs{}, job)
}

// IndexOp is used to identify arguemnt type, which is only used for v1 index args.
// TODO(joechenrh): remove this type after totally switched to v2
type IndexOp byte

// List op types.
const (
	OpAddIndex = iota
	OpDropIndex
	OpRollbackAddIndex
)

// IndexArg is the argument for single add/drop/rename index operation.
// Different types of job use different fields.
// Below lists used fields for each type (listed in order of the layout in v1)
//
//	Adding NonPK: Unique, IndexName, IndexPartSpecifications, IndexOption, SQLMode, Warning(not stored, always nil), Global
//	Adding PK: Unique, IndexName, IndexPartSpecifications, IndexOptions, HiddelCols, Global
//	Adding vector index: IndexName, IndexPartSpecifications, IndexOption, FuncExpr
//	Drop index: IndexName, IfExist, IndexID
//	Rollback add index: IndexName, IfExist, IsVector
//	Rename index: IndexName
type IndexArg struct {
	// Global is never used, we only use Global in IndexOption. Can be deprecated later.
	Global                  bool                          `json:"-"`
	Unique                  bool                          `json:"unique,omitempty"`
	IndexName               ast.CIStr                     `json:"index_name,omitempty"`
	IndexPartSpecifications []*ast.IndexPartSpecification `json:"index_part_specifications"`
	IndexOption             *ast.IndexOption              `json:"index_option,omitempty"`
	HiddenCols              []*ColumnInfo                 `json:"hidden_cols,omitempty"`

	// For vector index
	FuncExpr string `json:"func_expr,omitempty"`
	IsVector bool   `json:"is_vector,omitempty"`

	// For PK
	IsPK    bool          `json:"is_pk,omitempty"`
	SQLMode mysql.SQLMode `json:"sql_mode,omitempty"`

	// IfExist will be used in onDropIndex.
	IndexID  int64 `json:"index_id,omitempty"`
	IfExist  bool  `json:"if_exist,omitempty"`
	IsGlobal bool  `json:"is_global,omitempty"`

	// Only used for job args v2.
	SplitOpt *IndexArgSplitOpt `json:"split_opt,omitempty"`
}

// IndexArgSplitOpt is a field of IndexArg used by index presplit.
type IndexArgSplitOpt struct {
	Lower      []string   `json:"lower,omitempty"`
	Upper      []string   `json:"upper,omitempty"`
	Num        int64      `json:"num,omitempty"`
	ValueLists [][]string `json:"value_lists,omitempty"`
}

// ModifyIndexArgs is the argument for add/drop/rename index jobs,
// which includes PK and vector index.
type ModifyIndexArgs struct {
	IndexArgs []*IndexArg `json:"index_args,omitempty"`

	// Belows is used for finished args.
	PartitionIDs []int64 `json:"partition_ids,omitempty"`

	// This is only used for getFinishedArgsV1 to distinguish different type of job in v1,
	// since they need different arguments layout.
	// TODO(joechenrh): remove this flag after totally switched to v2
	OpType IndexOp `json:"-"`
}

func (a *ModifyIndexArgs) getArgsV1(job *Job) []any {
	if job.Type == ActionRenameIndex {
		return []any{a.IndexArgs[0].IndexName, a.IndexArgs[1].IndexName}
	}

	// Drop index
	if job.Type == ActionDropIndex || job.Type == ActionDropPrimaryKey {
		if len(a.IndexArgs) == 1 {
			return []any{a.IndexArgs[0].IndexName, a.IndexArgs[0].IfExist}
		}
		indexNames := make([]ast.CIStr, len(a.IndexArgs))
		ifExists := make([]bool, len(a.IndexArgs))
		for i, idxArg := range a.IndexArgs {
			indexNames[i] = idxArg.IndexName
			ifExists[i] = idxArg.IfExist
		}
		return []any{indexNames, ifExists}
	}

	// Add vector index
	if job.Type == ActionAddVectorIndex {
		arg := a.IndexArgs[0]
		return []any{arg.IndexName, arg.IndexPartSpecifications[0], arg.IndexOption, arg.FuncExpr}
	}

	// Add primary key
	if job.Type == ActionAddPrimaryKey {
		arg := a.IndexArgs[0]

		// The sixth argument is set and never used.
		// Leave it as nil to make it compatible with history job.
		return []any{
			arg.Unique, arg.IndexName, arg.IndexPartSpecifications,
			arg.IndexOption, arg.SQLMode, nil, arg.Global,
		}
	}

	// Add index
	n := len(a.IndexArgs)
	unique := make([]bool, n)
	indexName := make([]ast.CIStr, n)
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

	// This is to make the args compatible with old logic
	if n == 1 {
		return []any{unique[0], indexName[0], indexPartSpecification[0], indexOption[0], hiddenCols[0], global[0]}
	}

	return []any{unique, indexName, indexPartSpecification, indexOption, hiddenCols, global}
}

func (a *ModifyIndexArgs) decodeV1(job *Job) error {
	var err error
	switch job.Type {
	case ActionRenameIndex:
		err = a.decodeRenameIndexV1(job)
	case ActionAddIndex:
		err = a.decodeAddIndexV1(job)
	case ActionAddVectorIndex:
		err = a.decodeAddVectorIndexV1(job)
	case ActionAddPrimaryKey:
		err = a.decodeAddPrimaryKeyV1(job)
	default:
		err = errors.Errorf("Invalid job type for decoding %d", job.Type)
	}
	return errors.Trace(err)
}

func (a *ModifyIndexArgs) decodeRenameIndexV1(job *Job) error {
	var from, to ast.CIStr
	if err := job.decodeArgs(&from, &to); err != nil {
		return errors.Trace(err)
	}
	a.IndexArgs = []*IndexArg{
		{IndexName: from},
		{IndexName: to},
	}
	return nil
}

func (a *ModifyIndexArgs) decodeDropIndexV1(job *Job) error {
	indexNames := make([]ast.CIStr, 1)
	ifExists := make([]bool, 1)
	if err := job.decodeArgs(&indexNames[0], &ifExists[0]); err != nil {
		if err = job.decodeArgs(&indexNames, &ifExists); err != nil {
			return errors.Trace(err)
		}
	}

	a.IndexArgs = make([]*IndexArg, len(indexNames))
	for i, indexName := range indexNames {
		a.IndexArgs[i] = &IndexArg{
			IndexName: indexName,
			IfExist:   ifExists[i],
		}
	}
	return nil
}

func (a *ModifyIndexArgs) decodeAddIndexV1(job *Job) error {
	uniques := make([]bool, 1)
	indexNames := make([]ast.CIStr, 1)
	indexPartSpecifications := make([][]*ast.IndexPartSpecification, 1)
	indexOptions := make([]*ast.IndexOption, 1)
	hiddenCols := make([][]*ColumnInfo, 1)
	globals := make([]bool, 1)

	if err := job.decodeArgs(
		&uniques, &indexNames, &indexPartSpecifications,
		&indexOptions, &hiddenCols, &globals); err != nil {
		if err = job.decodeArgs(
			&uniques[0], &indexNames[0], &indexPartSpecifications[0],
			&indexOptions[0], &hiddenCols[0], &globals[0]); err != nil {
			return errors.Trace(err)
		}
	}

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
	return nil
}

func (a *ModifyIndexArgs) decodeAddPrimaryKeyV1(job *Job) error {
	a.IndexArgs = []*IndexArg{{IsPK: true}}
	var unused any
	if err := job.decodeArgs(
		&a.IndexArgs[0].Unique, &a.IndexArgs[0].IndexName, &a.IndexArgs[0].IndexPartSpecifications,
		&a.IndexArgs[0].IndexOption, &a.IndexArgs[0].SQLMode,
		&unused, &a.IndexArgs[0].Global); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (a *ModifyIndexArgs) decodeAddVectorIndexV1(job *Job) error {
	var (
		indexName              ast.CIStr
		indexPartSpecification *ast.IndexPartSpecification
		indexOption            *ast.IndexOption
		funcExpr               string
	)

	if err := job.decodeArgs(
		&indexName, &indexPartSpecification, &indexOption, &funcExpr); err != nil {
		return errors.Trace(err)
	}

	a.IndexArgs = []*IndexArg{{
		IndexName:               indexName,
		IndexPartSpecifications: []*ast.IndexPartSpecification{indexPartSpecification},
		IndexOption:             indexOption,
		FuncExpr:                funcExpr,
		IsVector:                true,
	}}
	return nil
}

func (a *ModifyIndexArgs) getFinishedArgsV1(job *Job) []any {
	// Add index
	if a.OpType == OpAddIndex {
		if job.Type == ActionAddVectorIndex {
			return []any{a.IndexArgs[0].IndexID, a.IndexArgs[0].IfExist, a.PartitionIDs, a.IndexArgs[0].IsGlobal}
		}

		n := len(a.IndexArgs)
		indexIDs := make([]int64, n)
		ifExists := make([]bool, n)
		isGlobals := make([]bool, n)
		for i, arg := range a.IndexArgs {
			indexIDs[i] = arg.IndexID
			ifExists[i] = arg.IfExist
			isGlobals[i] = arg.Global
		}
		return []any{indexIDs, ifExists, a.PartitionIDs, isGlobals}
	}

	// Below is to make the args compatible with old logic:
	// 1. For drop index, arguments are [CIStr, bool, int64, []int64, bool].
	// 3. For rollback add index, arguments are [[]CIStr, []bool, []int64].
	if a.OpType == OpRollbackAddIndex {
		indexNames := make([]ast.CIStr, len(a.IndexArgs))
		ifExists := make([]bool, len(a.IndexArgs))
		for i, idxArg := range a.IndexArgs {
			indexNames[i] = idxArg.IndexName
			ifExists[i] = idxArg.IfExist
		}
		return []any{indexNames, ifExists, a.PartitionIDs}
	}

	idxArg := a.IndexArgs[0]
	return []any{idxArg.IndexName, idxArg.IfExist, idxArg.IndexID, a.PartitionIDs, idxArg.IsVector}
}

// GetRenameIndexes get name of renamed index.
func (a *ModifyIndexArgs) GetRenameIndexes() (from, to ast.CIStr) {
	from, to = a.IndexArgs[0].IndexName, a.IndexArgs[1].IndexName
	return
}

// GetModifyIndexArgs gets the add/rename index args.
func GetModifyIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	return getOrDecodeArgs(&ModifyIndexArgs{}, job)
}

// GetDropIndexArgs is only used to get drop index arg.
// The logic is separated from ModifyIndexArgs.decodeV1.
// TODO(joechenrh): replace this function with GetModifyIndexArgs after totally switched to v2.
func GetDropIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	if job.Version == JobVersion2 {
		return getOrDecodeArgsV2[*ModifyIndexArgs](job)
	}

	// For add index jobs(ActionAddIndex, etc.) in v1, it can store both drop index arguments and add index arguments.
	// The logic in ModifyIndexArgs.decodeV1 maybe:
	//		Decode rename index args if type == ActionRenameIndex
	//		Try decode drop index args
	//		Try decode add index args if failed
	// So we separate this from decodeV1 to avoid unnecessary "try decode" logic.
	a := &ModifyIndexArgs{}
	err := a.decodeDropIndexV1(job)
	return a, errors.Trace(err)
}

// GetFinishedModifyIndexArgs gets the add/drop index args.
func GetFinishedModifyIndexArgs(job *Job) (*ModifyIndexArgs, error) {
	if job.Version == JobVersion2 {
		return getOrDecodeArgsV2[*ModifyIndexArgs](job)
	}

	if job.IsRollingback() || job.Type == ActionDropIndex || job.Type == ActionDropPrimaryKey {
		indexNames := make([]ast.CIStr, 1)
		ifExists := make([]bool, 1)
		indexIDs := make([]int64, 1)
		var partitionIDs []int64
		isVector := false
		var err error

		if job.IsRollingback() {
			// Rollback add indexes
			err = job.decodeArgs(&indexNames, &ifExists, &partitionIDs, &isVector)
		} else {
			// Finish drop index
			err = job.decodeArgs(&indexNames[0], &ifExists[0], &indexIDs[0], &partitionIDs, &isVector)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		a := &ModifyIndexArgs{
			PartitionIDs: partitionIDs,
		}
		a.IndexArgs = make([]*IndexArg, len(indexNames))
		for i, indexName := range indexNames {
			a.IndexArgs[i] = &IndexArg{
				IndexName: indexName,
				IfExist:   ifExists[i],
				IsVector:  isVector,
			}
		}
		// For drop index, store index id in IndexArgs, no impact on other situations.
		// Currently, there is only one indexID since drop index is not supported in multi schema change.
		// TODO(joechenrh): modify this and corresponding logic if we need support drop multi indexes in V1.
		a.IndexArgs[0].IndexID = indexIDs[0]

		return a, nil
	}

	// Add index/vector index/PK
	addIndexIDs := make([]int64, 1)
	ifExists := make([]bool, 1)
	isGlobals := make([]bool, 1)
	var partitionIDs []int64

	// add vector index args doesn't store slice.
	if err := job.decodeArgs(&addIndexIDs[0], &ifExists[0], &partitionIDs, &isGlobals[0]); err != nil {
		if err = job.decodeArgs(&addIndexIDs, &ifExists, &partitionIDs, &isGlobals); err != nil {
			return nil, errors.Errorf("Failed to decode finished arguments from job version 1")
		}
	}
	a := &ModifyIndexArgs{PartitionIDs: partitionIDs}
	for i, indexID := range addIndexIDs {
		a.IndexArgs = append(a.IndexArgs, &IndexArg{
			IndexID:  indexID,
			IfExist:  ifExists[i],
			IsGlobal: isGlobals[i],
		})
	}
	return a, nil
}

// ModifyColumnArgs is the argument for modify column.
type ModifyColumnArgs struct {
	Column           *ColumnInfo         `json:"column,omitempty"`
	OldColumnName    ast.CIStr           `json:"old_column_name,omitempty"`
	Position         *ast.ColumnPosition `json:"position,omitempty"`
	ModifyColumnType byte                `json:"modify_column_type,omitempty"`
	NewShardBits     uint64              `json:"new_shard_bits,omitempty"`
	// ChangingColumn is the temporary column derived from OldColumn
	ChangingColumn *ColumnInfo `json:"changing_column,omitempty"`
	// ChangingIdxs is only used in test, so don't persist it
	ChangingIdxs []*IndexInfo `json:"-"`
	// RedundantIdxs stores newly-created temp indexes which can be overwritten by other temp indexes.
	// These idxs will be added to finished args after job done.
	RedundantIdxs []int64 `json:"removed_idxs,omitempty"`

	// Finished args
	// IndexIDs stores index ids to be added to gc table.
	IndexIDs     []int64 `json:"index_ids,omitempty"`
	PartitionIDs []int64 `json:"partition_ids,omitempty"`
}

func (a *ModifyColumnArgs) getArgsV1(*Job) []any {
	// during upgrade, if https://github.com/pingcap/tidb/issues/54689 triggered,
	// older node might run the job submitted by new version, but it expects 5
	// args initially, and append the later 3 at runtime.
	if a.ChangingColumn == nil {
		return []any{a.Column, a.OldColumnName, a.Position, a.ModifyColumnType, a.NewShardBits}
	}
	return []any{
		a.Column, a.OldColumnName, a.Position, a.ModifyColumnType,
		a.NewShardBits, a.ChangingColumn, a.ChangingIdxs, a.RedundantIdxs,
	}
}

func (a *ModifyColumnArgs) decodeV1(job *Job) error {
	return job.decodeArgs(
		&a.Column, &a.OldColumnName, &a.Position, &a.ModifyColumnType,
		&a.NewShardBits, &a.ChangingColumn, &a.ChangingIdxs, &a.RedundantIdxs,
	)
}

func (a *ModifyColumnArgs) getFinishedArgsV1(*Job) []any {
	return []any{a.IndexIDs, a.PartitionIDs}
}

// GetModifyColumnArgs get the modify column argument from job.
func GetModifyColumnArgs(job *Job) (*ModifyColumnArgs, error) {
	return getOrDecodeArgs(&ModifyColumnArgs{}, job)
}

// GetFinishedModifyColumnArgs get the finished modify column argument from job.
func GetFinishedModifyColumnArgs(job *Job) (*ModifyColumnArgs, error) {
	if job.Version == JobVersion1 {
		var (
			indexIDs     []int64
			partitionIDs []int64
		)
		if err := job.decodeArgs(&indexIDs, &partitionIDs); err != nil {
			return nil, errors.Trace(err)
		}
		return &ModifyColumnArgs{
			IndexIDs:     indexIDs,
			PartitionIDs: partitionIDs,
		}, nil
	}
	return getOrDecodeArgsV2[*ModifyColumnArgs](job)
}
