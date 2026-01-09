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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func onModifySchemaSoftDeleteAndActiveActive(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetModifySchemaSoftDeleteAndActiveActiveArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	dbInfo.SoftdeleteInfo = args.SoftDelete
	dbInfo.IsActiveActive = args.ActiveActive

	if err = jobCtx.metaMut.UpdateDatabase(dbInfo); err != nil {
		return ver, errors.Trace(err)
	}
	if ver, err = updateSchemaVersion(jobCtx, job); err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbInfo)
	return ver, nil
}

// onSoftDeleteInfoChange handles changes to soft delete configuration
func onSoftDeleteInfoChange(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	args, err := model.GetAlterSoftDeleteInfoArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// If disabling soft-delete, just update config and return (no need to delete column)
	if args.SoftDelete == nil {
		tblInfo.SoftdeleteInfo = args.SoftDelete
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	}

	var columnInfo *model.ColumnInfo
	for _, col := range tblInfo.Columns {
		if col.Name == model.ExtraSoftDeleteTimeName {
			columnInfo = col
			break
		}
	}
	if columnInfo == nil {
		softDeleteCol := model.NewExtraSoftDeleteTimeColInfo()
		softDeleteCol.ID = allocateConstraintID(tblInfo)
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
		tblInfo.SoftdeleteInfo = args.SoftDelete
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	case model.StatePublic:
		tblInfo.SoftdeleteInfo = args.SoftDelete
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

func getSoftDeleteAndActiveActive(oldInfo *model.SoftdeleteInfo, newInfo *model.SoftDeleteInfoArg, oldActiveActive bool, isActiveActive *bool) (*model.SoftdeleteInfo, bool, error) {
	newActiveActive := oldActiveActive
	if isActiveActive != nil {
		newActiveActive = *isActiveActive
	}

	if newInfo == nil || !newInfo.Handled {
		newSoft := oldInfo
		if newSoft != nil {
			newSoft = newSoft.Clone()
		}
		if newActiveActive && newSoft == nil {
			return nil, false, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ActiveActive enabled but SoftDelete disabled")
		}
		return newSoft, newActiveActive, nil
	}

	if newInfo.HasEnable && !newInfo.Enable {
		if newInfo.Retention != "" || newInfo.RetentionUnit != ast.TimeUnitInvalid || newInfo.JobInterval != "" || newInfo.HasJobEnable {
			return nil, false, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("having SOFTDELETE options but SOFTDELETE = 'OFF'")
		}
		if newActiveActive {
			return nil, false, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ACTIVE_ACTIVE = 'ON' but SOFTDELETE = 'OFF'")
		}
		return nil, newActiveActive, nil
	}

	if oldInfo == nil {
		if newInfo.Retention == "" {
			return nil, false, dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("must sepecify SOFTDELETE = RETENTION xxx")
		}
		oldInfo = &model.SoftdeleteInfo{
			Retention:   model.DefaultSoftDeleteRetention,
			JobEnable:   true,
			JobInterval: model.DefaultSoftDeleteJobInterval,
		}
	} else {
		oldInfo = oldInfo.Clone()
	}

	if newInfo.Retention != "" {
		oldInfo.Retention = newInfo.Retention
	}
	if newInfo.RetentionUnit != ast.TimeUnitInvalid {
		oldInfo.RetentionUnit = newInfo.RetentionUnit
	}
	if newInfo.JobInterval != "" {
		oldInfo.JobInterval = newInfo.JobInterval
	}
	if newInfo.HasJobEnable {
		oldInfo.JobEnable = newInfo.JobEnable
	}

	return oldInfo, newActiveActive, nil
}

// checkSoftDeleteAndActiveActive validates soft delete configuration
func checkSoftDeleteAndActiveActive(tblInfo *model.TableInfo, is infoschema.InfoSchema, dbName ast.CIStr) error {
	if tblInfo.TempTableType != model.TempTableNone && (tblInfo.IsActiveActive || tblInfo.SoftdeleteInfo != nil) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SOFTDELETE and ACTIVE_ACTIVE is unspported on temp table")
	}

	if info := tblInfo.SoftdeleteInfo; info != nil {
		if len(tblInfo.ForeignKeys) > 0 {
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SOFTDELETE is unspported on table with foreign key"))
		}

		if is != nil && len(is.GetTableReferredForeignKeys(dbName.L, tblInfo.Name.L)) > 0 {
			return errors.Trace(dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("SOFTDELETE is unspported on referenced foreign key table"))
		}

		// Validate retention duration
		if _, err := tblInfo.SoftdeleteInfo.GetRetention(); err != nil {
			return errors.Trace(err)
		}

		// Validate job interval duration
		if _, err := tblInfo.SoftdeleteInfo.GetJobInterval(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}
