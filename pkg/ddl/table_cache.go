// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)
func onAlterCacheTable(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tbInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If the table is already in the cache state
	if tbInfo.TableCacheStatusType == model.TableCacheStatusEnable {
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	}

	if tbInfo.TempTableType != model.TempTableNone {
		return ver, errors.Trace(dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("alter temporary table cache"))
	}

	if tbInfo.Partition != nil {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("partition mode"))
	}

	switch tbInfo.TableCacheStatusType {
	case model.TableCacheStatusDisable:
		// disable -> switching
		tbInfo.TableCacheStatusType = model.TableCacheStatusSwitching
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
	case model.TableCacheStatusSwitching:
		// switching -> enable
		tbInfo.TableCacheStatusType = model.TableCacheStatusEnable
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("alter table cache", tbInfo.TableCacheStatusType.String())
	}
	return ver, err
}

func onAlterNoCacheTable(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	tbInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If the table is not in the cache state
	if tbInfo.TableCacheStatusType == model.TableCacheStatusDisable {
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
		return ver, nil
	}

	switch tbInfo.TableCacheStatusType {
	case model.TableCacheStatusEnable:
		//  enable ->  switching
		tbInfo.TableCacheStatusType = model.TableCacheStatusSwitching
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
	case model.TableCacheStatusSwitching:
		// switching -> disable
		tbInfo.TableCacheStatusType = model.TableCacheStatusDisable
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tbInfo, true)
		if err != nil {
			return ver, err
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbInfo)
	default:
		job.State = model.JobStateCancelled
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("alter table no cache", tbInfo.TableCacheStatusType.String())
	}
	return ver, err
}

func onRefreshMeta(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	_, err = model.GetRefreshMetaArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	// update schema version
	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	return ver, nil
}

func onAlterTableAffinity(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterTableAffinityArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	oldTblInfo := tblInfo.Clone()

	if err = validateTableAffinity(tblInfo, args.Affinity); err != nil {
		job.State = model.JobStateCancelled
		return 0, errors.Trace(err)
	}
	tblInfo.Affinity = args.Affinity

	// Create new affinity groups first (critical operation - must succeed)
	if tblInfo.Affinity != nil {
		if err = createTableAffinityGroupsInPD(jobCtx, tblInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}

	// Delete old affinity groups (best-effort cleanup - ignore errors)
	// ALTER TABLE AFFINITY: only delete when old table had affinity configuration
	// This ensures 'ALTER TABLE AFFINITY = 'none'' correctly cleans up stale affinity groups
	// Skip deletion if the affinity level remains the same to ensure idempotency
	if oldTblInfo.Affinity != nil {
		// Only delete if affinity is removed or level changed (same level means same group IDs)
		if tblInfo.Affinity == nil || oldTblInfo.Affinity.Level != tblInfo.Affinity.Level {
			if err := deleteTableAffinityGroupsInPD(jobCtx, oldTblInfo, nil); err != nil {
				logutil.DDLLogger().Error("failed to delete old affinity groups from PD", zap.Error(err), zap.Int64("tableID", oldTblInfo.ID))
			}
		}
	}

	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}
