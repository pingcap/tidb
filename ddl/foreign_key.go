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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
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
	fkInfo.ID = AllocateIndexID(tblInfo)
	tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)

	originalState := fkInfo.State
	switch fkInfo.State {
	case model.StateNone:
		// We just support record the foreign key, so we just make it public.
		// none -> public
		fkInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != fkInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStack("foreign key", fkInfo.State)
	}
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

func allocateFKIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxFKIndexID++
	return tblInfo.MaxFKIndexID
}

func checkTableForeignKeysValid(sctx sessionctx.Context, is infoschema.InfoSchema, schema string, tbInfo *model.TableInfo) error {
	if !config.ForeignKeyEnabled() {
		return nil
	}
	fkCheck := sctx.GetSessionVars().ForeignKeyChecks
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}
		err := checkTableForeignKeyValid(is, schema, tbInfo, fk, fkCheck)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTableForeignKeyValid(is infoschema.InfoSchema, schema string, tbInfo *model.TableInfo, fk *model.FKInfo, fkCheck bool) error {
	var referTblInfo *model.TableInfo
	if fk.RefSchema.L == schema && fk.RefTable.L == tbInfo.Name.L {
		referTblInfo = tbInfo
	} else {
		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			if (infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)) && !fkCheck {
				return nil
			}
			return err
		}
		referTblInfo = referTable.Meta()
	}
	return checkTableForeignKey(referTblInfo, tbInfo, fk)
}

func checkTableForeignKeyValidInOwner(d *ddlCtx, job *model.Job, tbInfo *model.TableInfo, fkCheck bool) error {
	if !config.ForeignKeyEnabled() {
		return nil
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < 1 {
			continue
		}

		var referTableInfo *model.TableInfo
		if fk.RefSchema.L == job.SchemaName && fk.RefTable.L == tbInfo.Name.L {
			referTableInfo = tbInfo
		} else {
			is := d.infoCache.GetLatest()
			refTbl, err := is.TableByName(fk.RefSchema, fk.RefTable)
			if err != nil {
				if !fkCheck && (infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)) {
					continue
				}
				return err
			}
			referTableInfo = refTbl.Meta()
		}

		err := checkTableForeignKey(referTableInfo, tbInfo, fk)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTableForeignKey(referTblInfo, tblInfo *model.TableInfo, fkInfo *model.FKInfo) error {
	// check refer columns in paren table.
	for i := range fkInfo.RefCols {
		refCol := model.FindColumnInfo(referTblInfo.Columns, fkInfo.RefCols[i].L)
		if refCol == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(fkInfo.RefCols[i].O)
		}
		if refCol.IsGenerated() && !refCol.GeneratedStored {
			return infoschema.ErrForeignKeyCannotUseVirtualColumn.GenWithStackByArgs(fkInfo.Name.O, fkInfo.RefCols[i].O)
		}
		col := model.FindColumnInfo(tblInfo.Columns, fkInfo.Cols[i].L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(fkInfo.Cols[i].O)
		}
		if col.GetType() != refCol.GetType() ||
			mysql.HasUnsignedFlag(col.GetFlag()) != mysql.HasUnsignedFlag(refCol.GetFlag()) ||
			col.GetCharset() != refCol.GetCharset() ||
			col.GetCollate() != refCol.GetCollate() {
			return dbterror.ErrFKIncompatibleColumns.GenWithStackByArgs(col.Name, refCol.Name, fkInfo.Name)
		}
		if len(fkInfo.RefCols) == 1 && mysql.HasPriKeyFlag(refCol.GetFlag()) && referTblInfo.PKIsHandle {
			return nil
		}
	}
	// check refer columns should have index.
	if model.FindIndexByColumns(referTblInfo, fkInfo.RefCols...) == nil {
		return infoschema.ErrFkNoIndexParent.GenWithStackByArgs(fkInfo.Name.O, fkInfo.RefTable.O)
	}
	return nil
}
