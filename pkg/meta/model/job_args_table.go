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
