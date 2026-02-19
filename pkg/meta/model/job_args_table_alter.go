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
	pdhttp "github.com/tikv/pd/client/http"
)

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
	// Note that ResetAvailable is only used in v2 job.
	ResetAvailable bool `json:"reset_available,omitempty"`
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

// AlterTableModeArgs is the argument for AlterTableMode.
type AlterTableModeArgs struct {
	TableMode TableMode `json:"table_mode,omitempty"`
	SchemaID  int64     `json:"schema_id,omitempty"`
	TableID   int64     `json:"table_id,omitempty"`
}

func (a *AlterTableModeArgs) getArgsV1(*Job) []any {
	return []any{a}
}

func (a *AlterTableModeArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(a))
}

// GetAlterTableModeArgs get the AlterTableModeArgs argument.
func GetAlterTableModeArgs(job *Job) (*AlterTableModeArgs, error) {
	return getOrDecodeArgs[*AlterTableModeArgs](&AlterTableModeArgs{}, job)
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
