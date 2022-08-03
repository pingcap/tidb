// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/dbterror"
)

func onCreateForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var fkInfo model.FKInfo
	err = job.DecodeArgs(&fkInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fkc := ForeignKeyChecker{
		schemaID: job.SchemaID,
		tbInfo:   tblInfo,
	}
	switch job.SchemaState {
	case model.StateNone:
		// none -> write-only
		_, referTableInfo, err := fkc.getParentTableFromStorage(d, &fkInfo, t)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		err = checkTableForeignKey(referTableInfo, tblInfo, &fkInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		fkInfo.ID = allocateIndexID(tblInfo)
		tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)
		fkInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		return ver, nil
	case model.StateWriteOnly:
		// write-only -> write-reorg
		// check foreign constrain
		// todo: execute internal sql:

		// update parent table info.
		referDBInfo, referTableInfo, err := fkc.getParentTableFromStorage(d, &fkInfo, t)
		if err != nil {
			return ver, err
		}
		referTableInfo.ReferredForeignKeys = append(referTableInfo.ReferredForeignKeys, &model.ReferredFKInfo{
			ChildSchema: model.NewCIStr(job.SchemaName),
			ChildTable:  tblInfo.Name,
			ChildFKName: fkInfo.Name,
			Cols:        fkInfo.RefCols,
		})
		originalSchemaID, originalTableID := job.SchemaID, job.TableID
		job.SchemaID, job.TableID = referDBInfo.ID, referTableInfo.ID
		ver, err = updateVersionAndTableInfo(d, t, job, referTableInfo, true)
		job.SchemaID, job.TableID = originalSchemaID, originalTableID
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// write-reorg -> public
		tblInfo.ForeignKeys[len(tblInfo.ForeignKeys)-1].State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStack("foreign key", fkInfo.State)
	}
	return ver, nil
}

func onDropForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var (
		fkName model.CIStr
		found  bool
		fkInfo model.FKInfo
	)
	err = job.DecodeArgs(&fkName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			found = true
			fkInfo = *fk
		}
	}

	if !found {
		job.State = model.JobStateCancelled
		return ver, infoschema.ErrForeignKeyNotExists.GenWithStackByArgs(fkName)
	}

	nfks := tblInfo.ForeignKeys[:0]
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L != fkName.L {
			nfks = append(nfks, fk)
		}
	}
	tblInfo.ForeignKeys = nfks

	originalState := fkInfo.State
	switch fkInfo.State {
	case model.StatePublic:
		// We just support record the foreign key, so we just make it none.
		// public -> none
		fkInfo.State = model.StateNone
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		job.SchemaState = fkInfo.State
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("foreign key", fkInfo.State)
	}

}
