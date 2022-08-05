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
	"sort"
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
		fkInfo.ID = allocateFKIndexID(tblInfo)
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

	tblInfos, err := adjustReferTableInfoAfterDropForeignKey(d, t, job, tblInfo, &fkInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateVersionAndMultiTableInfosWithCheck(t, job, tblInfos, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	job.SchemaState = fkInfo.State
	return ver, nil
}

func adjustReferTableInfoAfterDropForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, fk *model.FKInfo) ([]schemaIDAndTableInfo, error) {
	InfoList := []schemaIDAndTableInfo{{schemaID: job.SchemaID, tblInfo: tblInfo}}
	fkc := ForeignKeyHelper{
		schemaID: job.SchemaID,
		tbInfo:   tblInfo,
	}
	infos := make(map[int64]schemaIDAndTableInfo)
	infos[tblInfo.ID] = schemaIDAndTableInfo{schemaID: job.SchemaID, tblInfo: tblInfo}

	referDBInfo, referTblInfo, err := fkc.getParentTableFromStorage(d, t, fk.RefSchema, fk.RefTable)
	if err != nil {
		if infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err) {
			return InfoList, nil
		}
		return nil, err
	}
	for i, referredFK := range referTblInfo.ReferredForeignKeys {
		if referredFK.ChildSchema.L == job.SchemaName && referredFK.ChildTable.L == tblInfo.Name.L && referredFK.ChildFKName.L == fk.Name.L {
			referTblInfo.ReferredForeignKeys = append(referTblInfo.ReferredForeignKeys[:i], referTblInfo.ReferredForeignKeys[i+1:]...)
			break
		}
	}
	infos[referTblInfo.ID] = schemaIDAndTableInfo{
		schemaID: referDBInfo.ID,
		tblInfo:  referTblInfo,
	}
	InfoList = InfoList[:0]
	for _, info := range infos {
		InfoList = append(InfoList, info)
	}
	sort.Slice(InfoList, func(i, j int) bool {
		return InfoList[i].tblInfo.ID < InfoList[j].tblInfo.ID
	})
	return InfoList, nil
}
