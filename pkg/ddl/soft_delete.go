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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// onSoftDeleteInfoChange handles changes to soft delete configuration
func onSoftDeleteInfoChange(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterSoftDeleteInfoArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	softDeleteInfo := &args.SoftdeleteInfo

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// If disabling soft-delete, just update config and return (no need to delete column)
	if !softDeleteInfo.Enable {
		tblInfo.SoftdeleteInfo = softDeleteInfo
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	}

	var columnInfo *model.ColumnInfo
	for _, col := range tblInfo.Columns {
		if col.ID == model.ExtraSoftDeleteTimeID {
			columnInfo = col
			break
		}
	}
	if columnInfo == nil {
		softDeleteCol := model.NewExtraSoftDeleteTimeColInfo()
		softDeleteCol.State = model.StateNone
		softDeleteCol.Offset = len(tblInfo.Columns)
		tblInfo.Columns = append(tblInfo.Columns, softDeleteCol)
		columnInfo = softDeleteCol
	}

	originalState := columnInfo.State
	switch columnInfo.State {
	case model.StateNone:
		// none -> delete only
		columnInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
		return ver, nil
	case model.StateDeleteOnly:
		// delete only -> write only
		columnInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		return ver, nil
	case model.StateWriteOnly:
		// write only -> reorganization
		columnInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != columnInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
		job.MarkNonRevertible()
		return ver, nil
	case model.StateWriteReorganization:
		// reorganization -> public
		columnInfo.State = model.StatePublic
		tblInfo.SoftdeleteInfo = softDeleteInfo
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	case model.StatePublic:
		tblInfo.SoftdeleteInfo = softDeleteInfo
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("column", columnInfo.State)
	}
}

// getDefaultSoftDeleteInfo returns a default SoftdeleteInfo structure.
// Inherits from DBInfo where fields are set (non-empty), uses system defaults otherwise.
func getDefaultSoftDeleteInfo(dbInfo *model.DBInfo) *model.SoftdeleteInfo {
	info := &model.SoftdeleteInfo{
		Enable:      false,
		Retention:   model.DefaultSoftDeleteRetention,
		JobEnable:   true,
		JobInterval: model.DefaultSoftDeleteJobInterval,
	}

	if dbInfo == nil {
		return info
	}

	// Inherit from database if set
	if dbInfo.SoftDeleteEnable != "" {
		info.Enable = (dbInfo.SoftDeleteEnable == "ON")
	}
	if dbInfo.SoftDeleteRetention != "" {
		info.Retention = dbInfo.SoftDeleteRetention
	}
	if dbInfo.SoftDeleteJobEnable != "" {
		info.JobEnable = (dbInfo.SoftDeleteJobEnable == "ON")
	}
	if dbInfo.SoftDeleteJobInterval != "" {
		info.JobInterval = dbInfo.SoftDeleteJobInterval
	}

	return info
}

// checkSoftDeleteInfoValid validates soft delete configuration
func checkSoftDeleteInfoValid(tblInfo *model.TableInfo, isCreateTable bool) error {
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("soft delete")
	}

	if tblInfo.SoftdeleteInfo == nil {
		return nil
	}

	// In CREATE TABLE, soft delete options were set without enabling it
	if isCreateTable && !tblInfo.SoftdeleteInfo.Enable {
		return errors.Trace(dbterror.ErrSetSoftDeleteOptionForNonSoftDeleteTable.FastGenByArgs("SOFTDELETE"))
	}

	// Validate retention duration (if not empty)
	if len(tblInfo.SoftdeleteInfo.Retention) > 0 {
		if _, err := tblInfo.SoftdeleteInfo.GetRetention(); err != nil {
			return errors.Trace(err)
		}
	}

	// Validate job interval duration
	if len(tblInfo.SoftdeleteInfo.JobInterval) > 0 {
		if _, err := tblInfo.SoftdeleteInfo.GetJobInterval(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
