// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func handleEngineAttributeForCreateTable(input string, tbInfo *model.TableInfo) error {
	attr, err := model.ParseEngineAttributeFromString(input)
	if err != nil {
		return dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}

	// Keep the original string for SHOW CREATE TABLE.
	tbInfo.EngineAttribute = input

	if attr.StorageClass != nil {
		settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
		if err != nil {
			return errors.Trace(err)
		}

		logutil.BgLogger().Info("storage class: create table with settings",
			zap.Int64("tableID", tbInfo.ID), zap.Any("settings", settings))

		if err = BuildStorageClassForTable(tbInfo, settings); err != nil {
			return errors.Trace(err)
		}
	}

	// Handle other fields in the future.

	return nil
}

func getStorageClassSettingsFromTableInfo(tbInfo *model.TableInfo) (*model.StorageClassSettings, error) {
	attr, err := model.ParseEngineAttributeFromString(tbInfo.EngineAttribute)
	if err != nil {
		return nil, dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}

	if attr.StorageClass == nil {
		return nil, nil
	}

	settings, err := BuildStorageClassSettingsFromJSON(attr.StorageClass)
	if err != nil {
		return nil, errors.Trace(err)
	}

	logutil.BgLogger().Info("storage class: get settings from table info",
		zap.Int64("tableID", tbInfo.ID), zap.Any("settings", settings))
	return settings, nil
}

func rebuildStorageClassForPartitions(tbInfo *model.TableInfo, partitions []model.PartitionDefinition) error {
	settings, err := getStorageClassSettingsFromTableInfo(tbInfo)
	if err != nil || settings == nil {
		return errors.Trace(err)
	}
	return BuildStorageClassForPartitions(partitions, tbInfo, settings)
}

func onModifyTableEngineAttribute(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifyTableEngineAttributeArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	attr, err := model.ParseEngineAttributeFromString(args.EngineAttribute)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return ver, nil
	}
	oldState := snapshotStorageClassTransitionState(tblInfo)

	// Keep the original string for SHOW CREATE TABLE.
	tblInfo.EngineAttribute = args.EngineAttribute

	if err := onAlterTableStorageClassSettings(attr.StorageClass, tblInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if attr.StorageClass != nil {
		if err := markExplicitStorageClassTransition(jobCtx, job, tblInfo, oldState); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

type physicalStorageClassTransitionState struct {
	physicalID    int64
	partitionID   int64
	partitionName string
	tier          string
	target        string
	startTS       uint64
	schemaName    string
	tableName     string
}

type tableStorageClassTransitionState struct {
	physical map[int64]physicalStorageClassTransitionState
}

func snapshotStorageClassTransitionState(tblInfo *model.TableInfo) tableStorageClassTransitionState {
	state := tableStorageClassTransitionState{physical: make(map[int64]physicalStorageClassTransitionState)}
	state.physical[tblInfo.ID] = physicalStorageClassTransitionState{
		physicalID: tblInfo.ID,
		tier:       tblInfo.StorageClassTier,
		target:     tblInfo.StorageClassTransitionTarget,
		startTS:    tblInfo.StorageClassTransitionStartTS,
		schemaName: tblInfo.StorageClassTransitionSchemaName,
		tableName:  tblInfo.StorageClassTransitionTableName,
	}
	if tblInfo.Partition != nil {
		for _, partition := range tblInfo.Partition.Definitions {
			state.physical[partition.ID] = physicalStorageClassTransitionState{
				physicalID:    partition.ID,
				partitionID:   partition.ID,
				partitionName: partition.StorageClassTransitionPartitionName,
				tier:          partition.StorageClassTier,
				target:        partition.StorageClassTransitionTarget,
				startTS:       partition.StorageClassTransitionStartTS,
				schemaName:    partition.StorageClassTransitionSchemaName,
				tableName:     partition.StorageClassTransitionTableName,
			}
		}
	}
	return state
}

func normalizedStorageClassTransitionTarget(tier string) string {
	if tier == "" {
		return model.StorageClassTierDefault
	}
	return tier
}

func (s physicalStorageClassTransitionState) operationKey(tableID int64) (storageClassTransitionKey, bool) {
	if s.startTS == 0 || s.target == "" {
		return storageClassTransitionKey{}, false
	}
	return storageClassTransitionKey{tableID: tableID, target: normalizedStorageClassTransitionTarget(s.target), startTS: s.startTS}, true
}

func markExplicitStorageClassTransition(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, old tableStorageClassTransitionState) error {
	startTS := job.RealStartTS
	if startTS == 0 {
		startTS = job.StartTS
	}
	if startTS == 0 {
		startTS = jobCtx.metaMut.StartTS
	}
	if startTS == 0 {
		return errors.New("storage class transition start TSO is unavailable")
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	schemaName := dbInfo.Name.O
	tableName := tblInfo.Name.O
	updateStorageClassTransitionMarkers(tblInfo, old, startTS, schemaName, tableName)
	return nil
}

func updateStorageClassTransitionMarkers(
	tblInfo *model.TableInfo,
	old tableStorageClassTransitionState,
	startTS uint64,
	schemaName, tableName string,
) {
	current := snapshotStorageClassTransitionState(tblInfo)
	changedPhysicalIDs := make(map[int64]struct{})
	supersededKeys := make(map[storageClassTransitionKey]struct{})
	for physicalID, currentState := range current.physical {
		oldState, ok := old.physical[physicalID]
		if !ok || normalizedStorageClassTransitionTarget(oldState.tier) == normalizedStorageClassTransitionTarget(currentState.tier) {
			continue
		}
		changedPhysicalIDs[physicalID] = struct{}{}
		if key, active := oldState.operationKey(tblInfo.ID); active {
			supersededKeys[key] = struct{}{}
		}
	}

	// Supersede the whole old logical operation, even when the new DDL changes
	// only one of its physical targets. Unchanged members are restarted under
	// the new start TSO so one operation cannot be both active and superseded.
	for key := range supersededKeys {
		members := make([]physicalStorageClassTransitionState, 0)
		for physicalID, oldState := range old.physical {
			oldKey, active := oldState.operationKey(tblInfo.ID)
			if !active || oldKey != key {
				continue
			}
			members = append(members, oldState)
			changedPhysicalIDs[physicalID] = struct{}{}
		}
		appendPendingStorageClassTransitionHistory(
			tblInfo,
			key,
			members,
			model.StorageClassTransitionStateSuperseded,
			startTS,
			0,
			0,
			false,
		)
	}

	for physicalID := range changedPhysicalIDs {
		state, ok := current.physical[physicalID]
		if !ok {
			continue
		}
		setStorageClassTransitionMarker(
			tblInfo,
			physicalID,
			normalizedStorageClassTransitionTarget(state.tier),
			startTS,
			schemaName,
			tableName,
		)
	}
}

func appendPendingStorageClassTransitionHistory(
	tblInfo *model.TableInfo,
	key storageClassTransitionKey,
	members []physicalStorageClassTransitionState,
	state string,
	finishTS uint64,
	totalReplicas uint64,
	completedReplicas uint64,
	statusValid bool,
) {
	if len(members) == 0 {
		return
	}
	for _, history := range tblInfo.StorageClassTransitionPendingHistory {
		if history.StartTS == key.startTS && normalizedStorageClassTransitionTarget(history.Target) == key.target {
			return
		}
	}
	slices.SortFunc(members, func(a, b physicalStorageClassTransitionState) int {
		return cmp.Compare(a.physicalID, b.physicalID)
	})
	history := model.StorageClassTransitionHistory{
		Target:            key.target,
		State:             state,
		StartTS:           key.startTS,
		FinishTS:          finishTS,
		SchemaName:        members[0].schemaName,
		TableName:         firstNonEmpty(members[0].tableName, tblInfo.Name.O),
		Targets:           make([]model.StorageClassTransitionTarget, 0, len(members)),
		TotalReplicas:     totalReplicas,
		CompletedReplicas: completedReplicas,
		StatusValid:       statusValid,
	}
	for _, member := range members {
		history.Targets = append(history.Targets, model.StorageClassTransitionTarget{
			PhysicalID: member.physicalID, PartitionID: member.partitionID, PartitionName: member.partitionName,
		})
	}
	tblInfo.StorageClassTransitionPendingHistory = append(tblInfo.StorageClassTransitionPendingHistory, history)
}

func setStorageClassTransitionMarker(
	tblInfo *model.TableInfo,
	physicalID int64,
	target string,
	startTS uint64,
	schemaName, tableName string,
) {
	if physicalID == tblInfo.ID {
		tblInfo.StorageClassTransitionTarget = target
		tblInfo.StorageClassTransitionStartTS = startTS
		tblInfo.StorageClassTransitionSchemaName = schemaName
		tblInfo.StorageClassTransitionTableName = tableName
		return
	}
	if tblInfo.Partition == nil {
		return
	}
	for i := range tblInfo.Partition.Definitions {
		partition := &tblInfo.Partition.Definitions[i]
		if partition.ID != physicalID {
			continue
		}
		partition.StorageClassTransitionTarget = target
		partition.StorageClassTransitionStartTS = startTS
		partition.StorageClassTransitionSchemaName = schemaName
		partition.StorageClassTransitionTableName = tableName
		partition.StorageClassTransitionPartitionName = partition.Name.O
		return
	}
}

func onAlterTableStorageClassSettings(storageClass json.RawMessage, tblInfo *model.TableInfo) error {
	if storageClass == nil {
		return nil
	}

	settings, err := BuildStorageClassSettingsFromJSON(storageClass)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("storage class: alter table settings",
		zap.Int64("tableID", tblInfo.ID), zap.Any("settings", settings))

	if err = BuildStorageClassForTable(tblInfo, settings); err != nil {
		return errors.Trace(err)
	}
	if tblInfo.Partition != nil && len(tblInfo.Partition.Definitions) > 0 {
		if err = BuildStorageClassForPartitions(tblInfo.Partition.Definitions, tblInfo, settings); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// AlterTableEngineAttribute updates the table engine attribute.
func (e *executor) AlterTableEngineAttribute(ctx sessionctx.Context, ident ast.Ident, engineAttribute string) error {
	is := e.infoCache.GetLatest()
	schema, ok := is.SchemaByName(ident.Schema)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(ident.Schema)
	}

	tb, err := is.TableByName(e.ctx, ident.Schema, ident.Name)
	if err != nil {
		return errors.Trace(infoschema.ErrTableNotExists.GenWithStackByArgs(ident.Schema, ident.Name))
	}
	if _, err = model.ParseEngineAttributeFromString(engineAttribute); err != nil {
		return dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}

	job := &model.Job{
		Version:        model.GetJobVerInUse(),
		SchemaID:       schema.ID,
		TableID:        tb.Meta().ID,
		SchemaName:     schema.Name.L,
		TableName:      tb.Meta().Name.L,
		Type:           model.ActionModifyEngineAttribute,
		BinlogInfo:     &model.HistoryInfo{},
		CDCWriteSource: ctx.GetSessionVars().CDCWriteSource,
		SQLMode:        ctx.GetSessionVars().SQLMode,
	}
	args := &model.ModifyTableEngineAttributeArgs{
		EngineAttribute: engineAttribute,
	}
	err = e.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}
