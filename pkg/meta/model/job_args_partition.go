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
	"github.com/pingcap/tidb/pkg/util/intest"
	pdhttp "github.com/tikv/pd/client/http"
)

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
