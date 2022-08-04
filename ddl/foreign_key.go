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
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/sqlexec"
)

func (w *worker) onCreateForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var fkInfo model.FKInfo
	var fkChecks bool
	err = job.DecodeArgs(&fkInfo, &fkChecks)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	fkc := ForeignKeyHelper{
		schemaID: job.SchemaID,
		tbInfo:   tblInfo,
	}
	switch job.SchemaState {
	case model.StateNone:
		// none -> write-only
		_, referTableInfo, err := fkc.getParentTableFromStorage(d, t, fkInfo.RefSchema, fkInfo.RefTable)
		if err != nil {
			if fkChecks || !infoschema.ErrTableNotExists.Equal(err) {
				job.State = model.JobStateCancelled
				return ver, err
			}
		}
		if referTableInfo != nil {
			err = checkTableForeignKey(referTableInfo, tblInfo, &fkInfo)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, err
			}
			fkInfo.State = model.StateWriteOnly
			job.SchemaState = model.StateWriteOnly
		} else {
			fkInfo.State = model.StateWriteReorganization
			job.SchemaState = model.StateWriteReorganization
		}
		fkInfo.ID = allocateIndexID(tblInfo)
		tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	case model.StateWriteOnly:
		// write-only -> write-reorg
		// check foreign constrain
		err = addForeignKeyConstrainCheck(w, job.SchemaName, tblInfo.Name.L, &fkInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		// update parent table info.
		referDBInfo, referTableInfo, err := fkc.getParentTableFromStorage(d, t, fkInfo.RefSchema, fkInfo.RefTable)
		if err != nil {
			job.State = model.JobStateCancelled
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

func addForeignKeyConstrainCheck(w *worker, schema, table string, fkInfo *model.FKInfo) error {
	// Get sessionctx from context resource pool.
	sctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(sctx)

	return checkForeignKeyConstrain(w.ctx, sctx, schema, table, fkInfo)
}

func checkForeignKeyConstrain(ctx context.Context, sctx sessionctx.Context, schema, table string, fkInfo *model.FKInfo) error {
	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramsList := make([]interface{}, 0, 4+len(fkInfo.Cols)*2)
	paramsList = append(paramsList, schema, table)
	for i, col := range fkInfo.Cols {
		if i == 0 {
			buf.WriteString("%n is not null")
			paramsList = append(paramsList, col.L)
		} else {
			buf.WriteString(" and %n is not null")
			paramsList = append(paramsList, col.L)
		}
	}
	buf.WriteString(" and (")
	for i, col := range fkInfo.Cols {
		if i == 0 {
			buf.WriteString("%n")
		} else {
			buf.WriteString(",%n")
		}
		paramsList = append(paramsList, col.L)
	}
	buf.WriteString(") not in (select ")
	for i, col := range fkInfo.RefCols {
		if i == 0 {
			buf.WriteString("%n")
		} else {
			buf.WriteString(",%n")
		}
		paramsList = append(paramsList, col.L)
	}
	buf.WriteString(" from %n.%n ) limit 1")
	paramsList = append(paramsList, fkInfo.RefSchema.L, fkInfo.RefTable.L)
	rows, _, err := sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, nil, buf.String(), paramsList...)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return dbterror.ErrNoReferencedRow2.GenWithStackByArgs(fkInfo.String(schema, table))
	}
	return nil
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
