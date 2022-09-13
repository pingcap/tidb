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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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
	tblInfo.MaxForeignKeyID++
	return tblInfo.MaxForeignKeyID
}

func checkTableForeignKeysValid(sctx sessionctx.Context, is infoschema.InfoSchema, schema string, tbInfo *model.TableInfo) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	fkCheck := sctx.GetSessionVars().ForeignKeyChecks
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		err := checkTableForeignKeyValid(is, schema, tbInfo, fk, fkCheck)
		if err != nil {
			return err
		}
	}

	referredFKInfos := is.GetTableReferredForeignKeys(schema, tbInfo.Name.L)
	for _, referredFK := range referredFKInfos {
		childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			return err
		}
		fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
		if fk == nil {
			continue
		}
		err = checkTableForeignKey(tbInfo, childTable.Meta(), fk)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkTableForeignKeyValid(is infoschema.InfoSchema, schema string, tbInfo *model.TableInfo, fk *model.FKInfo, fkCheck bool) error {
	var referTblInfo *model.TableInfo
	if fk.RefSchema.L == schema && fk.RefTable.L == tbInfo.Name.L {
		same := true
		for i, col := range fk.Cols {
			if col.L != fk.RefCols[i].L {
				same = false
				break
			}
		}
		if same {
			// self-reference with same columns is not support.
			return infoschema.ErrCannotAddForeign
		}
		referTblInfo = tbInfo
	} else {
		referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
		if err != nil {
			if (infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)) && !fkCheck {
				return nil
			}
			return infoschema.ErrForeignKeyCannotOpenParent.GenWithStackByArgs(fk.RefTable.O)
		}
		referTblInfo = referTable.Meta()
	}
	return checkTableForeignKey(referTblInfo, tbInfo, fk)
}

func checkTableForeignKeyValidInOwner(d *ddlCtx, t *meta.Meta, job *model.Job, tbInfo *model.TableInfo, fkCheck bool) (retryable bool, _ error) {
	if !variable.EnableForeignKey.Load() {
		return false, nil
	}
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return true, err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() != currVer {
		return true, errors.New("need wait owner to load latest schema")
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		var referTableInfo *model.TableInfo
		if fk.RefSchema.L == job.SchemaName && fk.RefTable.L == tbInfo.Name.L {
			referTableInfo = tbInfo
		} else {
			referTable, err := is.TableByName(fk.RefSchema, fk.RefTable)
			if err != nil {
				if !fkCheck && (infoschema.ErrTableNotExists.Equal(err) || infoschema.ErrDatabaseNotExists.Equal(err)) {
					continue
				}
				return false, err
			}
			referTableInfo = referTable.Meta()
		}

		err := checkTableForeignKey(referTableInfo, tbInfo, fk)
		if err != nil {
			return false, err
		}
	}
	referredFKInfos := is.GetTableReferredForeignKeys(job.SchemaName, tbInfo.Name.L)
	for _, referredFK := range referredFKInfos {
		childTable, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
		if err != nil {
			return false, err
		}
		fk := model.FindFKInfoByName(childTable.Meta().ForeignKeys, referredFK.ChildFKName.L)
		if fk == nil {
			continue
		}
		err = checkTableForeignKey(tbInfo, childTable.Meta(), fk)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

func checkTableForeignKey(referTblInfo, tblInfo *model.TableInfo, fkInfo *model.FKInfo) error {
	if referTblInfo.TempTableType != model.TempTableNone || tblInfo.TempTableType != model.TempTableNone {
		return infoschema.ErrCannotAddForeign
	}

	// check refer columns in parent table.
	for i := range fkInfo.RefCols {
		refCol := model.FindColumnInfo(referTblInfo.Columns, fkInfo.RefCols[i].L)
		if refCol == nil {
			return infoschema.ErrForeignKeyNoColumnInParent.GenWithStackByArgs(fkInfo.RefCols[i], fkInfo.Name, fkInfo.RefTable)
		}
		if refCol.IsGenerated() && !refCol.GeneratedStored {
			return infoschema.ErrForeignKeyCannotUseVirtualColumn.GenWithStackByArgs(fkInfo.Name, fkInfo.RefCols[i])
		}
		col := model.FindColumnInfo(tblInfo.Columns, fkInfo.Cols[i].L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(fkInfo.Cols[i])
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
		return infoschema.ErrForeignKeyNoIndexInParent.GenWithStackByArgs(fkInfo.Name, fkInfo.RefTable)
	}
	return nil
}
