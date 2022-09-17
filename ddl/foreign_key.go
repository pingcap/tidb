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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
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

func getAndCheckLatestInfoSchema(d *ddlCtx, t *meta.Meta) (infoschema.InfoSchema, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}
	is := d.infoCache.GetLatest()
	if is.SchemaMetaVersion() != currVer {
		return nil, errors.New("need wait owner to load latest schema")
	}
	return is, nil
}

func checkTableForeignKeyValidInOwner(d *ddlCtx, t *meta.Meta, job *model.Job, tbInfo *model.TableInfo, fkCheck bool) (retryable bool, _ error) {
	if !variable.EnableForeignKey.Load() {
		return false, nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return true, err
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

func checkModifyColumnWithForeignKeyConstraint(is infoschema.InfoSchema, dbName string, tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	if newCol.GetType() == originalCol.GetType() && newCol.GetFlen() == originalCol.GetFlen() && newCol.GetDecimal() == originalCol.GetDecimal() {
		return nil
	}
	// WARN: is maybe nil.
	if is == nil {
		return nil
	}
	for _, fkInfo := range tbInfo.ForeignKeys {
		for i, col := range fkInfo.Cols {
			if col.L == originalCol.Name.L {
				if !is.TableExists(fkInfo.RefSchema, fkInfo.RefTable) {
					continue
				}
				referTable, err := is.TableByName(fkInfo.RefSchema, fkInfo.RefTable)
				if err != nil {
					return err
				}
				referCol := model.FindColumnInfo(referTable.Meta().Columns, fkInfo.RefCols[i].L)
				if referCol == nil {
					continue
				}
				if newCol.GetType() != referCol.GetType() {
					return dbterror.ErrFKIncompatibleColumns.GenWithStackByArgs(originalCol.Name, fkInfo.RefCols[i], fkInfo.Name)
				}
				if newCol.GetFlen() < referCol.GetFlen() || newCol.GetFlen() < originalCol.GetFlen() ||
					(newCol.GetType() == mysql.TypeNewDecimal && (newCol.GetFlen() != originalCol.GetFlen() || newCol.GetDecimal() != originalCol.GetDecimal())) {
					return dbterror.ErrForeignKeyColumnCannotChange.GenWithStackByArgs(originalCol.Name, fkInfo.Name)
				}
			}
		}
	}
	referredFKs := is.GetTableReferredForeignKeys(dbName, tbInfo.Name.L)
	for _, referredFK := range referredFKs {
		for i, col := range referredFK.Cols {
			if col.L == originalCol.Name.L {
				if !is.TableExists(referredFK.ChildSchema, referredFK.ChildTable) {
					continue
				}
				childTblInfo, err := is.TableByName(referredFK.ChildSchema, referredFK.ChildTable)
				if err != nil {
					return err
				}
				fk := model.FindFKInfoByName(childTblInfo.Meta().ForeignKeys, referredFK.ChildFKName.L)
				childCol := model.FindColumnInfo(childTblInfo.Meta().Columns, fk.Cols[i].L)
				if childCol == nil {
					continue
				}
				if newCol.GetType() != childCol.GetType() {
					return dbterror.ErrFKIncompatibleColumns.GenWithStackByArgs(childCol.Name, originalCol.Name, referredFK.ChildFKName)
				}
				if newCol.GetFlen() < childCol.GetFlen() || newCol.GetFlen() < originalCol.GetFlen() ||
					(newCol.GetType() == mysql.TypeNewDecimal && (newCol.GetFlen() != childCol.GetFlen() || newCol.GetDecimal() != childCol.GetDecimal())) {
					return dbterror.ErrForeignKeyColumnCannotChangeChild.GenWithStackByArgs(originalCol.Name, referredFK.ChildFKName, referredFK.ChildSchema.L+"."+referredFK.ChildTable.L)
				}
			}
		}
	}

	return nil
}

func checkTableHasForeignKeyReferred(is infoschema.InfoSchema, schema, tbl string, ignoreTables []ast.Ident, fkCheck bool) *model.ReferredFKInfo {
	if !fkCheck {
		return nil
	}
	referredFKs := is.GetTableReferredForeignKeys(schema, tbl)
	for _, referredFK := range referredFKs {
		found := false
		for _, tb := range ignoreTables {
			if referredFK.ChildSchema.L == tb.Schema.L && referredFK.ChildTable.L == tb.Name.L {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if is.TableExists(referredFK.ChildSchema, referredFK.ChildTable) {
			return referredFK
		}
	}
	return nil
}

func checkDropTableHasForeignKeyReferredInOwner(d *ddlCtx, t *meta.Meta, job *model.Job) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	var objectIdents []ast.Ident
	var fkCheck bool
	err := job.DecodeArgs(&objectIdents, &fkCheck)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	referredFK, err := checkTableHasForeignKeyReferredInOwner(d, t, job.SchemaName, job.TableName, objectIdents, fkCheck)
	if err != nil {
		return err
	}
	if referredFK != nil {
		job.State = model.JobStateCancelled
		msg := fmt.Sprintf("`%s`.`%s` CONSTRAINT `%s`", referredFK.ChildSchema, referredFK.ChildTable, referredFK.ChildFKName)
		return errors.Trace(dbterror.ErrTruncateIllegalForeignKey.GenWithStackByArgs(msg))
	}
	return nil
}

func checkTruncateTableHasForeignKeyReferredInOwner(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, fkCheck bool) error {
	referredFK, err := checkTableHasForeignKeyReferredInOwner(d, t, job.SchemaName, job.TableName, []ast.Ident{{Name: tblInfo.Name, Schema: model.NewCIStr(job.SchemaName)}}, fkCheck)
	if err != nil {
		return err
	}
	if referredFK != nil {
		job.State = model.JobStateCancelled
		msg := fmt.Sprintf("`%s`.`%s` CONSTRAINT `%s`", referredFK.ChildSchema, referredFK.ChildTable, referredFK.ChildFKName)
		return errors.Trace(dbterror.ErrTruncateIllegalForeignKey.GenWithStackByArgs(msg))
	}
	return nil
}

func checkTableHasForeignKeyReferredInOwner(d *ddlCtx, t *meta.Meta, schema, tbl string, ignoreTables []ast.Ident, fkCheck bool) (_ *model.ReferredFKInfo, _ error) {
	if !variable.EnableForeignKey.Load() {
		return nil, nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return nil, err
	}
	referredFK := checkTableHasForeignKeyReferred(is, schema, tbl, ignoreTables, fkCheck)
	return referredFK, nil
}

func checkIndexNeededInForeignKey(is infoschema.InfoSchema, dbName string, tbInfo *model.TableInfo, idxInfo *model.IndexInfo) error {
	referredFKs := is.GetTableReferredForeignKeys(dbName, tbInfo.Name.L)
	if len(tbInfo.ForeignKeys) == 0 && len(referredFKs) == 0 {
		return nil
	}
	remainIdxs := make([]*model.IndexInfo, 0, len(tbInfo.Indices))
	for _, idx := range tbInfo.Indices {
		if idx.ID == idxInfo.ID {
			continue
		}
		remainIdxs = append(remainIdxs, idx)
	}
	checkFn := func(cols []model.CIStr) error {
		if !model.IsIndexPrefixCovered(tbInfo, idxInfo, cols...) {
			return nil
		}
		if tbInfo.PKIsHandle && len(cols) == 1 {
			refColInfo := model.FindColumnInfo(tbInfo.Columns, cols[0].L)
			if refColInfo != nil && mysql.HasPriKeyFlag(refColInfo.GetFlag()) {
				return nil
			}
		}
		for _, index := range remainIdxs {
			if model.IsIndexPrefixCovered(tbInfo, index, cols...) {
				return nil
			}
		}
		return dbterror.ErrDropIndexNeededInForeignKey.GenWithStackByArgs(idxInfo.Name)
	}
	for _, fk := range tbInfo.ForeignKeys {
		if fk.Version < model.FKVersion1 {
			continue
		}
		err := checkFn(fk.Cols)
		if err != nil {
			return err
		}
	}
	for _, referredFK := range referredFKs {
		err := checkFn(referredFK.Cols)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIndexNeededInForeignKeyInOwner(d *ddlCtx, t *meta.Meta, job *model.Job, dbName string, tbInfo *model.TableInfo, idxInfo *model.IndexInfo) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return err
	}
	err = checkIndexNeededInForeignKey(is, dbName, tbInfo, idxInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return err
	}
	return nil
}

func checkDropColumnWithForeignKeyConstraint(is infoschema.InfoSchema, dbName string, tbInfo *model.TableInfo, colName string) error {
	for _, fkInfo := range tbInfo.ForeignKeys {
		for _, col := range fkInfo.Cols {
			if col.L == colName {
				return dbterror.ErrFkColumnCannotDrop.GenWithStackByArgs(colName, fkInfo.Name)
			}
		}
	}
	referredFKs := is.GetTableReferredForeignKeys(dbName, tbInfo.Name.L)
	for _, referredFK := range referredFKs {
		for _, col := range referredFK.Cols {
			if col.L == colName {
				return dbterror.ErrFkColumnCannotDropChild.GenWithStackByArgs(colName, referredFK.ChildFKName, referredFK.ChildTable)
			}
		}
	}
	return nil
}

func checkDropColumnWithForeignKeyConstraintInOwner(d *ddlCtx, t *meta.Meta, job *model.Job, tbInfo *model.TableInfo, colName string) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkDropColumnWithForeignKeyConstraint(is, job.SchemaName, tbInfo, colName)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	return nil
}
