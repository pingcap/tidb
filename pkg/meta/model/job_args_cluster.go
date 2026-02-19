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


// RefreshMetaArgs is the argument for RefreshMeta.
// InvolvedDB/InvolvedTable used for setting InvolvingSchemaInfo to
// indicates the schema info involved in the DDL job.
type RefreshMetaArgs struct {
	SchemaID      int64  `json:"schema_id,omitempty"`
	TableID       int64  `json:"table_id,omitempty"`
	InvolvedDB    string `json:"involved_db,omitempty"`
	InvolvedTable string `json:"involved_table,omitempty"`
}

func (a *RefreshMetaArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *RefreshMetaArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetRefreshMetaArgs get the refresh meta argument.
func GetRefreshMetaArgs(job *Job) (*RefreshMetaArgs, error) {
	return getOrDecodeArgs[*RefreshMetaArgs](&RefreshMetaArgs{}, job)
}

// AlterTableAffinityArgs is the argument for AlterTableAffinity
type AlterTableAffinityArgs struct {
	Affinity *TableAffinityInfo `json:"affinity,omitempty"`
}

func (a *AlterTableAffinityArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *AlterTableAffinityArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetAlterTableAffinityArgs get the alter table affinity argument.
func GetAlterTableAffinityArgs(job *Job) (*AlterTableAffinityArgs, error) {
	return getOrDecodeArgs[*AlterTableAffinityArgs](&AlterTableAffinityArgs{}, job)
}
