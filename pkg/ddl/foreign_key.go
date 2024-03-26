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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (w *worker) onCreateForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	var fkInfo model.FKInfo
	var fkCheck bool
	err = job.DecodeArgs(&fkInfo, &fkCheck)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if job.IsRollingback() {
		return dropForeignKey(d, t, job, tblInfo, fkInfo.Name)
	}
	switch job.SchemaState {
	case model.StateNone:
		err = checkAddForeignKeyValidInOwner(d, t, job.SchemaName, tblInfo, &fkInfo, fkCheck)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		fkInfo.State = model.StateWriteOnly
		fkInfo.ID = allocateFKIndexID(tblInfo)
		tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		return ver, nil
	case model.StateWriteOnly:
		err = checkForeignKeyConstrain(w, job.SchemaName, tblInfo.Name.L, &fkInfo, fkCheck)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, err
		}
		tblInfo.ForeignKeys[len(tblInfo.ForeignKeys)-1].State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		tblInfo.ForeignKeys[len(tblInfo.ForeignKeys)-1].State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.SchemaState = model.StatePublic
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

	var fkName model.CIStr
	err = job.DecodeArgs(&fkName)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return dropForeignKey(d, t, job, tblInfo, fkName)
}

func dropForeignKey(d *ddlCtx, t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, fkName model.CIStr) (ver int64, err error) {
	var fkInfo *model.FKInfo
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			fkInfo = fk
			break
		}
	}
	if fkInfo == nil {
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
	ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	if job.IsRollingback() {
		job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	} else {
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	}
	job.SchemaState = model.StateNone
	return ver, err
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

func getAndCheckLatestInfoSchema(d *ddlCtx, _ *meta.Meta) (infoschema.InfoSchema, error) {
	// TODO(crazycs520): fix me, need to make sure the `d.infoCache` is the latest infoschema.
	return d.infoCache.GetLatest(), nil
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
	if referTblInfo.TTLInfo != nil {
		return dbterror.ErrUnsupportedTTLReferencedByFK
	}
	if referTblInfo.GetPartitionInfo() != nil || tblInfo.GetPartitionInfo() != nil {
		return infoschema.ErrForeignKeyOnPartitioned
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
	if model.FindIndexByColumns(referTblInfo, referTblInfo.Indices, fkInfo.RefCols...) == nil {
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
				if !isAcceptableForeignKeyColumnChange(newCol, originalCol, referCol) {
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
				if !isAcceptableForeignKeyColumnChange(newCol, originalCol, childCol) {
					return dbterror.ErrForeignKeyColumnCannotChangeChild.GenWithStackByArgs(originalCol.Name, referredFK.ChildFKName, referredFK.ChildSchema.L+"."+referredFK.ChildTable.L)
				}
			}
		}
	}

	return nil
}

func isAcceptableForeignKeyColumnChange(newCol, originalCol, relatedCol *model.ColumnInfo) bool {
	switch newCol.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		// For integer data types, value from GetFlen indicates the minimum display width and is unrelated to the range of values a type can store.
		// We don't have to prevent the length change. See: https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
		return true
	}

	if newCol.GetFlen() < relatedCol.GetFlen() {
		return false
	}
	if newCol.GetFlen() < originalCol.GetFlen() {
		return false
	}
	if newCol.GetType() == mysql.TypeNewDecimal {
		if newCol.GetFlen() != originalCol.GetFlen() || newCol.GetDecimal() != originalCol.GetDecimal() {
			return false
		}
	}

	return true
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

type foreignKeyHelper struct {
	loaded map[schemaAndTable]schemaIDAndTableInfo
}

type schemaAndTable struct {
	schema string
	table  string
}

func newForeignKeyHelper() foreignKeyHelper {
	return foreignKeyHelper{loaded: make(map[schemaAndTable]schemaIDAndTableInfo)}
}

func (h *foreignKeyHelper) addLoadedTable(schemaName, tableName string, schemaID int64, tblInfo *model.TableInfo) {
	k := schemaAndTable{schema: schemaName, table: tableName}
	h.loaded[k] = schemaIDAndTableInfo{schemaID: schemaID, tblInfo: tblInfo}
}

func (h *foreignKeyHelper) getLoadedTables() []schemaIDAndTableInfo {
	tableList := make([]schemaIDAndTableInfo, 0, len(h.loaded))
	for _, info := range h.loaded {
		tableList = append(tableList, info)
	}
	return tableList
}

func (h *foreignKeyHelper) getTableFromStorage(is infoschema.InfoSchema, t *meta.Meta, schema, table model.CIStr) (result schemaIDAndTableInfo, _ error) {
	k := schemaAndTable{schema: schema.L, table: table.L}
	if info, ok := h.loaded[k]; ok {
		return info, nil
	}
	db, ok := is.SchemaByName(schema)
	if !ok {
		return result, errors.Trace(infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema))
	}
	tb, err := is.TableByName(schema, table)
	if err != nil {
		return result, errors.Trace(err)
	}
	tbInfo, err := getTableInfo(t, tb.Meta().ID, db.ID)
	if err != nil {
		return result, errors.Trace(err)
	}
	result.schemaID, result.tblInfo = db.ID, tbInfo
	h.loaded[k] = result
	return result, nil
}

func checkDatabaseHasForeignKeyReferred(is infoschema.InfoSchema, schema model.CIStr, fkCheck bool) error {
	if !fkCheck {
		return nil
	}
	tables := is.SchemaTables(schema)
	tableNames := make([]ast.Ident, len(tables))
	for i := range tables {
		tableNames[i] = ast.Ident{Schema: schema, Name: tables[i].Meta().Name}
	}
	for _, tbl := range tables {
		if referredFK := checkTableHasForeignKeyReferred(is, schema.L, tbl.Meta().Name.L, tableNames, fkCheck); referredFK != nil {
			return errors.Trace(dbterror.ErrForeignKeyCannotDropParent.GenWithStackByArgs(tbl.Meta().Name, referredFK.ChildFKName, referredFK.ChildTable))
		}
	}
	return nil
}

func checkDatabaseHasForeignKeyReferredInOwner(d *ddlCtx, t *meta.Meta, job *model.Job) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	var fkCheck bool
	err := job.DecodeArgs(&fkCheck)
	if err != nil {
		job.State = model.JobStateCancelled
		return errors.Trace(err)
	}
	if !fkCheck {
		return nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkDatabaseHasForeignKeyReferred(is, model.NewCIStr(job.SchemaName), fkCheck)
	if err != nil {
		job.State = model.JobStateCancelled
	}
	return errors.Trace(err)
}

func checkFKDupName(tbInfo *model.TableInfo, fkName model.CIStr) error {
	for _, fkInfo := range tbInfo.ForeignKeys {
		if fkName.L == fkInfo.Name.L {
			return dbterror.ErrFkDupName.GenWithStackByArgs(fkName.O)
		}
	}
	return nil
}

func checkAddForeignKeyValid(is infoschema.InfoSchema, schema string, tbInfo *model.TableInfo, fk *model.FKInfo, fkCheck bool) error {
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	err := checkTableForeignKeyValid(is, schema, tbInfo, fk, fkCheck)
	if err != nil {
		return err
	}
	return nil
}

func checkAddForeignKeyValidInOwner(d *ddlCtx, t *meta.Meta, schema string, tbInfo *model.TableInfo, fk *model.FKInfo, fkCheck bool) error {
	err := checkFKDupName(tbInfo, fk.Name)
	if err != nil {
		return err
	}
	if !variable.EnableForeignKey.Load() {
		return nil
	}
	is, err := getAndCheckLatestInfoSchema(d, t)
	if err != nil {
		return errors.Trace(err)
	}
	err = checkAddForeignKeyValid(is, schema, tbInfo, fk, fkCheck)
	if err != nil {
		return errors.Trace(err)
	}
	// check foreign key columns should have index.
	if len(fk.Cols) == 1 && tbInfo.PKIsHandle {
		pkCol := tbInfo.GetPkColInfo()
		if pkCol != nil && pkCol.Name.L == fk.Cols[0].L {
			return nil
		}
	}
	if model.FindIndexByColumns(tbInfo, tbInfo.Indices, fk.Cols...) == nil {
		return errors.Errorf("Failed to add the foreign key constraint. Missing index for '%s' foreign key columns in the table '%s'", fk.Name, tbInfo.Name)
	}
	return nil
}

func checkForeignKeyConstrain(w *worker, schema, table string, fkInfo *model.FKInfo, fkCheck bool) error {
	if !fkCheck {
		return nil
	}
	sctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	originValue := sctx.GetSessionVars().OptimizerEnableNAAJ
	sctx.GetSessionVars().OptimizerEnableNAAJ = true
	defer func() {
		sctx.GetSessionVars().OptimizerEnableNAAJ = originValue
		w.sessPool.Put(sctx)
	}()

	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramsList := make([]any, 0, 4+len(fkInfo.Cols)*2)
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
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(w.ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, buf.String(), paramsList...)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return dbterror.ErrNoReferencedRow2.GenWithStackByArgs(fkInfo.String(schema, table))
	}
	return nil
}
