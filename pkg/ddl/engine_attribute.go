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
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
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

func (w *worker) onModifyTableEngineAttribute(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
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
	oldState := snapshotPhysicalStorageClasses(tblInfo)

	// Keep the original string for SHOW CREATE TABLE.
	tblInfo.EngineAttribute = args.EngineAttribute

	if err := onAlterTableStorageClassSettings(attr.StorageClass, tblInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if attr.StorageClass != nil && kerneltype.IsNextGen() {
		pending, err := w.prepareExplicitStorageClassTransition(jobCtx, job, tblInfo, oldState)
		if err != nil {
			return ver, errors.Trace(err)
		}
		pending.schemaVersion = ver
		if pending.schemaVersion == 0 && job.MultiSchemaInfo != nil && job.MultiSchemaInfo.SkipVersion {
			pending.schemaVersion = jobCtx.sharedMultiSchemaVersion
		}
		if pending.schemaVersion <= 0 {
			return ver, errors.New("storage class transition schema version is unavailable")
		}
		if jobCtx.deferStorageClassTransitionStaging {
			jobCtx.pendingStorageClassTransitions = append(jobCtx.pendingStorageClassTransitions, pending)
		} else if err := pending.stage(jobCtx.stepCtx, w.sess); err != nil {
			return ver, errors.Trace(err)
		}
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

type pendingStorageClassTransition struct {
	tblInfo       *model.TableInfo
	old           map[int64]physicalStorageClass
	schemaVersion int64
	startTS       uint64
	schemaName    string
	tableName     string
}

func (w *worker) prepareExplicitStorageClassTransition(
	jobCtx *jobContext,
	job *model.Job,
	tblInfo *model.TableInfo,
	old map[int64]physicalStorageClass,
) (pendingStorageClassTransition, error) {
	startTS := job.RealStartTS
	if startTS == 0 {
		startTS = job.StartTS
	}
	if startTS == 0 {
		startTS = jobCtx.metaMut.StartTS
	}
	if startTS == 0 {
		return pendingStorageClassTransition{}, errors.New("storage class transition start TSO is unavailable")
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return pendingStorageClassTransition{}, errors.Trace(err)
	}
	return pendingStorageClassTransition{
		tblInfo:    tblInfo.Clone(),
		old:        old,
		startTS:    startTS,
		schemaName: dbInfo.Name.O,
		tableName:  tblInfo.Name.O,
	}, nil
}

func (pending pendingStorageClassTransition) stage(
	ctx context.Context,
	se *sess.Session,
) error {
	return stageStorageClassTransitions(
		ctx,
		se,
		pending.tblInfo,
		pending.old,
		pending.schemaVersion,
		pending.startTS,
		pending.schemaName,
		pending.tableName,
	)
}

func (w *worker) flushPendingStorageClassTransitions(jobCtx *jobContext) error {
	for _, pending := range jobCtx.pendingStorageClassTransitions {
		if err := pending.stage(jobCtx.stepCtx, w.sess); err != nil {
			return errors.Trace(err)
		}
	}
	jobCtx.pendingStorageClassTransitions = nil
	return nil
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
