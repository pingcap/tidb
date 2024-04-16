// Copyright 2023-2023 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func (w *worker) onAddCheckConstraint(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		return rollingBackAddConstraint(d, t, job)
	}

	failpoint.Inject("errorBeforeDecodeArgs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error before decode args"))
		}
	})

	dbInfo, tblInfo, constraintInfoInMeta, constraintInfoInJob, err := checkAddCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if constraintInfoInMeta == nil {
		// It's first time to run add constraint job, so there is no constraint info in meta.
		// Use the raw constraint info from job directly and modify table info here.
		constraintInfoInJob.ID = allocateConstraintID(tblInfo)
		// Reset constraint name according to real-time constraints name at this point.
		constrNames := map[string]bool{}
		for _, constr := range tblInfo.Constraints {
			constrNames[constr.Name.L] = true
		}
		setNameForConstraintInfo(tblInfo.Name.L, constrNames, []*model.ConstraintInfo{constraintInfoInJob})
		// Double check the constraint dependency.
		existedColsMap := make(map[string]struct{})
		cols := tblInfo.Columns
		for _, v := range cols {
			if v.State == model.StatePublic {
				existedColsMap[v.Name.L] = struct{}{}
			}
		}
		dependedCols := constraintInfoInJob.ConstraintCols
		for _, k := range dependedCols {
			if _, ok := existedColsMap[k.L]; !ok {
				// The table constraint depended on a non-existed column.
				return ver, dbterror.ErrTableCheckConstraintReferUnknown.GenWithStackByArgs(constraintInfoInJob.Name, k)
			}
		}

		tblInfo.Constraints = append(tblInfo.Constraints, constraintInfoInJob)
		constraintInfoInMeta = constraintInfoInJob
	}

	// If not enforced, add it directly.
	if !constraintInfoInMeta.Enforced {
		constraintInfoInMeta.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return ver, nil
	}

	switch constraintInfoInMeta.State {
	case model.StateNone:
		job.SchemaState = model.StateWriteOnly
		constraintInfoInMeta.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
	case model.StateWriteOnly:
		job.SchemaState = model.StateWriteReorganization
		constraintInfoInMeta.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
	case model.StateWriteReorganization:
		err = w.verifyRemainRecordsForCheckConstraint(dbInfo, tblInfo, constraintInfoInMeta)
		if err != nil {
			if dbterror.ErrCheckConstraintIsViolated.Equal(err) {
				job.State = model.JobStateRollingback
			}
			return ver, errors.Trace(err)
		}
		constraintInfoInMeta.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("constraint", constraintInfoInMeta.State)
	}

	return ver, errors.Trace(err)
}

func checkAddCheckConstraint(t *meta.Meta, job *model.Job) (*model.DBInfo, *model.TableInfo, *model.ConstraintInfo, *model.ConstraintInfo, error) {
	schemaID := job.SchemaID
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, nil, nil, nil, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, nil, errors.Trace(err)
	}
	constraintInfo1 := &model.ConstraintInfo{}
	err = job.DecodeArgs(constraintInfo1)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, errors.Trace(err)
	}
	// do the double-check with constraint existence.
	constraintInfo2 := tblInfo.FindConstraintInfoByName(constraintInfo1.Name.L)
	if constraintInfo2 != nil {
		if constraintInfo2.State == model.StatePublic {
			// We already have a constraint with the same constraint name.
			job.State = model.JobStateCancelled
			return nil, nil, nil, nil, infoschema.ErrColumnExists.GenWithStackByArgs(constraintInfo1.Name)
		}
		// if not, that means constraint was in intermediate state.
	}

	err = checkConstraintNamesNotExists(t, schemaID, []*model.ConstraintInfo{constraintInfo1})
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, nil, err
	}

	return dbInfo, tblInfo, constraintInfo2, constraintInfo1, nil
}

// onDropCheckConstraint can be called from two case:
// 1: rollback in add constraint.(in rollback function the job.args will be changed)
// 2: user drop constraint ddl.
func onDropCheckConstraint(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, constraintInfo, err := checkDropCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch constraintInfo.State {
	case model.StatePublic:
		job.SchemaState = model.StateWriteOnly
		constraintInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
	case model.StateWriteOnly:
		// write only state constraint will still take effect to check the newly inserted data.
		// So the dependent column shouldn't be dropped even in this intermediate state.
		constraintInfo.State = model.StateNone
		// remove the constraint from tableInfo.
		for i, constr := range tblInfo.Constraints {
			if constr.Name.L == constraintInfo.Name.L {
				tblInfo.Constraints = append(tblInfo.Constraints[0:i], tblInfo.Constraints[i+1:]...)
			}
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLJob.GenWithStackByArgs("constraint", tblInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropCheckConstraint(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.ConstraintInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var constrName model.CIStr
	err = job.DecodeArgs(&constrName)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	// double check with constraint existence.
	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}
	return tblInfo, constraintInfo, nil
}

func (w *worker) onAlterCheckConstraint(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	dbInfo, tblInfo, constraintInfo, enforced, err := checkAlterCheckConstraint(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.IsRollingback() {
		return rollingBackAlterConstraint(d, t, job)
	}

	// Current State is desired.
	if constraintInfo.State == model.StatePublic && constraintInfo.Enforced == enforced {
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		return
	}

	// enforced will fetch table data and check the constraint.
	if enforced {
		switch constraintInfo.State {
		case model.StatePublic:
			job.SchemaState = model.StateWriteReorganization
			constraintInfo.State = model.StateWriteReorganization
			constraintInfo.Enforced = enforced
			ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		case model.StateWriteReorganization:
			job.SchemaState = model.StateWriteOnly
			constraintInfo.State = model.StateWriteOnly
			ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		case model.StateWriteOnly:
			err = w.verifyRemainRecordsForCheckConstraint(dbInfo, tblInfo, constraintInfo)
			if err != nil {
				if dbterror.ErrCheckConstraintIsViolated.Equal(err) {
					job.State = model.JobStateRollingback
				}
				return ver, errors.Trace(err)
			}
			constraintInfo.State = model.StatePublic
			ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		}
	} else {
		constraintInfo.Enforced = enforced
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true)
		if err != nil {
			// update version and tableInfo error will cause retry.
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	}
	return ver, err
}

func checkAlterCheckConstraint(t *meta.Meta, job *model.Job) (*model.DBInfo, *model.TableInfo, *model.ConstraintInfo, bool, error) {
	schemaID := job.SchemaID
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, false, errors.Trace(err)
	}

	var (
		enforced   bool
		constrName model.CIStr
	)
	err = job.DecodeArgs(&constrName, &enforced)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, false, errors.Trace(err)
	}
	// do the double check with constraint existence.
	constraintInfo := tblInfo.FindConstraintInfoByName(constrName.L)
	if constraintInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, nil, false, dbterror.ErrConstraintNotFound.GenWithStackByArgs(constrName)
	}
	return dbInfo, tblInfo, constraintInfo, enforced, nil
}

func allocateConstraintID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxConstraintID++
	return tblInfo.MaxConstraintID
}

func buildConstraintInfo(tblInfo *model.TableInfo, dependedCols []model.CIStr, constr *ast.Constraint, state model.SchemaState) (*model.ConstraintInfo, error) {
	constraintName := model.NewCIStr(constr.Name)
	if err := checkTooLongConstraint(constraintName); err != nil {
		return nil, errors.Trace(err)
	}

	// Restore check constraint expression to string.
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	sb.Reset()
	err := constr.Expr.Restore(restoreCtx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create constraint info.
	constraintInfo := &model.ConstraintInfo{
		Name:           constraintName,
		Table:          tblInfo.Name,
		ConstraintCols: dependedCols,
		ExprString:     sb.String(),
		Enforced:       constr.Enforced,
		InColumn:       constr.InColumn,
		State:          state,
	}

	return constraintInfo, nil
}

func checkTooLongConstraint(constr model.CIStr) error {
	if len(constr.L) > mysql.MaxConstraintIdentifierLen {
		return dbterror.ErrTooLongIdent.GenWithStackByArgs(constr)
	}
	return nil
}

// findDependentColsInExpr returns a set of string, which indicates
// the names of the columns that are dependent by exprNode.
func findDependentColsInExpr(expr ast.ExprNode) map[string]struct{} {
	colNames := FindColumnNamesInExpr(expr)
	colsMap := make(map[string]struct{}, len(colNames))
	for _, depCol := range colNames {
		colsMap[depCol.Name.L] = struct{}{}
	}
	return colsMap
}

func (w *worker) verifyRemainRecordsForCheckConstraint(dbInfo *model.DBInfo, tableInfo *model.TableInfo, constr *model.ConstraintInfo) error {
	// Inject a fail-point to skip the remaining records check.
	failpoint.Inject("mockVerifyRemainDataSuccess", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil)
		}
	})
	// Get sessionctx from ddl context resource pool in ddl worker.
	var sctx sessionctx.Context
	sctx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(sctx)

	// If there is any row can't pass the check expression, the add constraint action will error.
	// It's no need to construct expression node out and pull the chunk rows through it. Here we
	// can let the check expression restored string as the filter in where clause directly.
	// Prepare internal SQL to fetch data from physical table under this filter.
	sql := fmt.Sprintf("select 1 from `%s`.`%s` where not %s limit 1", dbInfo.Name.L, tableInfo.Name.L, constr.ExprString)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		return errors.Trace(err)
	}
	rowCount := len(rows)
	if rowCount != 0 {
		return dbterror.ErrCheckConstraintIsViolated.GenWithStackByArgs(constr.Name.L)
	}
	return nil
}

func setNameForConstraintInfo(tableLowerName string, namesMap map[string]bool, infos []*model.ConstraintInfo) {
	cnt := 1
	constraintPrefix := tableLowerName + "_chk_"
	for _, constrInfo := range infos {
		if constrInfo.Name.O == "" {
			constrName := fmt.Sprintf("%s%d", constraintPrefix, cnt)
			for {
				// loop until find constrName that haven't been used.
				if !namesMap[constrName] {
					namesMap[constrName] = true
					break
				}
				cnt++
				constrName = fmt.Sprintf("%s%d", constraintPrefix, cnt)
			}
			constrInfo.Name = model.NewCIStr(constrName)
		}
	}
}

// IsColumnDroppableWithCheckConstraint check whether the column in check-constraint whose dependent col is more than 1
func IsColumnDroppableWithCheckConstraint(col model.CIStr, tblInfo *model.TableInfo) error {
	for _, cons := range tblInfo.Constraints {
		if len(cons.ConstraintCols) > 1 {
			for _, colName := range cons.ConstraintCols {
				if colName.L == col.L {
					return dbterror.ErrCantDropColWithCheckConstraint.GenWithStackByArgs(cons.Name, col)
				}
			}
		}
	}
	return nil
}

// IsColumnRenameableWithCheckConstraint check whether the column is referenced in check-constraint
func IsColumnRenameableWithCheckConstraint(col model.CIStr, tblInfo *model.TableInfo) error {
	for _, cons := range tblInfo.Constraints {
		for _, colName := range cons.ConstraintCols {
			if colName.L == col.L {
				return dbterror.ErrCantDropColWithCheckConstraint.GenWithStackByArgs(cons.Name, col)
			}
		}
	}
	return nil
}
