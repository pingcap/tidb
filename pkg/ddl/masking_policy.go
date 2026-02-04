// Copyright 2026 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
)

func (w *worker) onCreateMaskingPolicy(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetMaskingPolicyArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if args.Policy == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("masking policy args missing policy info")
	}
	policyInfo, replaceOnExist := args.Policy, args.ReplaceOnExist
	policyInfo.State = model.StateNone

	if err := validateMaskingPolicyTarget(jobCtx.stepCtx, jobCtx.infoCache, policyInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	metaMut := jobCtx.metaMut
	existPolicy, err := getMaskingPolicyByName(jobCtx.infoCache, metaMut, policyInfo.Name)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	existOnColumn, err := getMaskingPolicyByTableColumn(jobCtx.infoCache, metaMut, policyInfo.TableID, policyInfo.ColumnID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	if existPolicy != nil {
		if existPolicy.TableID != policyInfo.TableID || existPolicy.ColumnID != policyInfo.ColumnID {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrMaskingPolicyExists.GenWithStackByArgs(existPolicy.Name.O)
		}
		if !replaceOnExist {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrMaskingPolicyExists.GenWithStackByArgs(existPolicy.Name.O)
		}

		replacePolicy := existPolicy.Clone()
		replacePolicy.Expression = policyInfo.Expression
		replacePolicy.Status = policyInfo.Status
		replacePolicy.FunctionType = policyInfo.FunctionType
		replacePolicy.UpdatedAt = policyInfo.UpdatedAt
		if err = metaMut.UpdateMaskingPolicy(replacePolicy); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if err = w.updateMaskingPolicyInSysTable(jobCtx, replacePolicy); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		job.SchemaID = replacePolicy.ID
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	}

	if existOnColumn != nil {
		job.State = model.JobStateCancelled
		return ver, dbterror.ErrMaskingPolicyExists.GenWithStackByArgs(existOnColumn.Name.O)
	}

	switch policyInfo.State {
	case model.StateNone:
		policyInfo.State = model.StatePublic
		if err = metaMut.CreateMaskingPolicy(policyInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if err = w.insertMaskingPolicyIntoSysTable(jobCtx, policyInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		job.SchemaID = policyInfo.ID
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("masking policy", policyInfo.State)
	}
}

func (w *worker) onAlterMaskingPolicy(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetMaskingPolicyArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if args.Policy == nil {
		job.State = model.JobStateCancelled
		return ver, errors.New("masking policy args missing policy info")
	}

	metaMut := jobCtx.metaMut
	oldPolicy, err := metaMut.GetMaskingPolicy(args.PolicyID)
	if err != nil {
		if errors.ErrorEqual(err, meta.ErrMaskingPolicyNotExists) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	if err := validateMaskingPolicyTarget(jobCtx.stepCtx, jobCtx.infoCache, oldPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	newPolicy := oldPolicy.Clone()
	newPolicy.Expression = args.Policy.Expression
	newPolicy.Status = args.Policy.Status
	newPolicy.FunctionType = args.Policy.FunctionType
	newPolicy.UpdatedAt = args.Policy.UpdatedAt
	if err = metaMut.UpdateMaskingPolicy(newPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if err = w.updateMaskingPolicyInSysTable(jobCtx, newPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
	return ver, nil
}

func (w *worker) onDropMaskingPolicy(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetMaskingPolicyArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	metaMut := jobCtx.metaMut
	policyInfo, err := metaMut.GetMaskingPolicy(args.PolicyID)
	if err != nil {
		if errors.ErrorEqual(err, meta.ErrMaskingPolicyNotExists) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	policyInfo.State = model.StateNone
	if err = metaMut.DropMaskingPolicy(policyInfo.ID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if err = w.deleteMaskingPolicyFromSysTable(jobCtx, policyInfo.ID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	return ver, nil
}

func getMaskingPolicyByName(infoCache *infoschema.InfoCache, t *meta.Mutator, policyName ast.CIStr) (*model.MaskingPolicyInfo, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}

	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		policy, ok := is.MaskingPolicyByName(policyName)
		if ok {
			return policy, nil
		}
		return nil, nil
	}
	policies, err := t.ListMaskingPolicies()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.Name.L == policyName.L {
			return policy, nil
		}
	}
	return nil, nil
}

func getMaskingPolicyByTableColumn(infoCache *infoschema.InfoCache, t *meta.Mutator, tableID, columnID int64) (*model.MaskingPolicyInfo, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}

	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		policy, ok := is.MaskingPolicyByTableColumn(tableID, columnID)
		if ok {
			return policy, nil
		}
		return nil, nil
	}
	policies, err := t.ListMaskingPolicies()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.TableID == tableID && policy.ColumnID == columnID {
			return policy, nil
		}
	}
	return nil, nil
}

func validateMaskingPolicyTarget(ctx context.Context, infoCache *infoschema.InfoCache, policy *model.MaskingPolicyInfo) error {
	is := infoCache.GetLatest()
	dbInfo, ok := is.SchemaByName(policy.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(policy.DBName)
	}
	tbl, err := is.TableByName(ctx, policy.DBName, policy.TableName)
	if err != nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(policy.DBName, policy.TableName)
	}
	tblInfo := tbl.Meta()
	if err = checkMaskingPolicyTable(dbInfo, tblInfo); err != nil {
		return err
	}
	col := model.FindColumnInfo(tblInfo.Columns, policy.ColumnName.L)
	if col == nil || col.ID != policy.ColumnID {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(policy.ColumnName, policy.TableName)
	}
	if err = checkMaskingPolicyColumn(col); err != nil {
		return err
	}
	return nil
}

func checkMaskingPolicyTable(schema *model.DBInfo, tblInfo *model.TableInfo) error {
	if tblInfo.IsView() || tblInfo.IsSequence() {
		return dbterror.ErrWrongObject.GenWithStackByArgs(schema.Name, tblInfo.Name, "BASE TABLE")
	}
	if tblInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrOptOnTemporaryTable.GenWithStackByArgs("masking policy")
	}
	if filter.IsSystemSchema(schema.Name.L) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("masking policy on system table")
	}
	return nil
}

func checkMaskingPolicyColumn(col *model.ColumnInfo) error {
	if col.IsGenerated() {
		return dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("masking policy on generated column")
	}
	if !isMaskingPolicySupportedType(&col.FieldType) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("masking policy on unsupported column type")
	}
	return nil
}

func isMaskingPolicySupportedType(ft *types.FieldType) bool {
	tp := ft.GetType()
	if types.IsTypeChar(tp) || types.IsTypeVarchar(tp) {
		return true
	}
	if types.IsTypeBlob(tp) && !types.IsBinaryStr(ft) {
		return true
	}
	if types.IsTypeTime(tp) || tp == mysql.TypeDuration || tp == mysql.TypeYear {
		return true
	}
	return false
}

func buildMaskingPolicyInfo(
	ctx sessionctx.Context,
	schema *model.DBInfo,
	tbl table.Table,
	policyName ast.CIStr,
	columnName ast.CIStr,
	expr ast.ExprNode,
	state ast.MaskingPolicyState,
) (*model.MaskingPolicyInfo, error) {
	tblInfo := tbl.Meta()
	if err := checkMaskingPolicyTable(schema, tblInfo); err != nil {
		return nil, err
	}
	col := table.FindCol(tbl.Cols(), columnName.L)
	if col == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(columnName, tblInfo.Name)
	}
	if err := checkMaskingPolicyColumn(col.ColumnInfo); err != nil {
		return nil, err
	}
	exprStr, err := restoreMaskingExpression(expr)
	if err != nil {
		return nil, err
	}
	status := maskingPolicyStatusFromState(state)
	funcType := maskingPolicyFuncTypeFromExpr(expr)
	now := time.Now()
	createdBy := ""
	if user := ctx.GetSessionVars().User; user != nil {
		createdBy = user.String()
	}
	return &model.MaskingPolicyInfo{
		Name:         policyName,
		DBName:       schema.Name,
		TableName:    tblInfo.Name,
		TableID:      tblInfo.ID,
		ColumnName:   col.Name,
		ColumnID:     col.ID,
		Expression:   exprStr,
		Status:       status,
		FunctionType: funcType,
		CreatedAt:    now,
		UpdatedAt:    now,
		CreatedBy:    createdBy,
		State:        model.StateNone,
	}, nil
}

func restoreMaskingExpression(expr ast.ExprNode) (string, error) {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(rCtx); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}

func maskingPolicyStatusFromState(state ast.MaskingPolicyState) model.MaskingPolicyStatus {
	if state.Explicit && !state.Enabled {
		return model.MaskingPolicyStatusDisable
	}
	return model.MaskingPolicyStatusEnable
}

func maskingPolicyFuncTypeFromExpr(expr ast.ExprNode) model.MaskingPolicyFuncType {
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return model.MaskingPolicyFuncTypeCustom
	}
	switch strings.ToLower(fn.FnName.L) {
	case "mask_full":
		return model.MaskingPolicyFuncTypeFull
	case "mask_partial":
		return model.MaskingPolicyFuncTypePartial
	case "mask_null":
		return model.MaskingPolicyFuncTypeNull
	case "mask_date":
		return model.MaskingPolicyFuncTypeCustom
	default:
		return model.MaskingPolicyFuncTypeCustom
	}
}

func (w *worker) insertMaskingPolicyIntoSysTable(jobCtx *jobContext, policy *model.MaskingPolicyInfo) error {
	const insertSQL = `INSERT INTO mysql.tidb_masking_policy
		(policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, function_type, created_at, updated_at, created_by)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`
	_, err := w.sess.Execute(jobCtx.stepCtx, insertSQL, "create-masking-policy",
		policy.ID,
		policy.Name.O,
		policy.DBName.O,
		policy.TableName.O,
		policy.TableID,
		policy.ColumnName.O,
		policy.ColumnID,
		policy.Expression,
		policy.Status.String(),
		string(policy.FunctionType),
		policy.CreatedAt,
		policy.UpdatedAt,
		policy.CreatedBy,
	)
	return errors.Trace(err)
}

func (w *worker) updateMaskingPolicyInSysTable(jobCtx *jobContext, policy *model.MaskingPolicyInfo) error {
	const updateSQL = `UPDATE mysql.tidb_masking_policy
		SET policy_name = %?, db_name = %?, table_name = %?, table_id = %?, column_name = %?, column_id = %?, expression = %?,
			status = %?, function_type = %?, updated_at = %?
		WHERE policy_id = %?`
	_, err := w.sess.Execute(jobCtx.stepCtx, updateSQL, "update-masking-policy",
		policy.Name.O,
		policy.DBName.O,
		policy.TableName.O,
		policy.TableID,
		policy.ColumnName.O,
		policy.ColumnID,
		policy.Expression,
		policy.Status.String(),
		string(policy.FunctionType),
		policy.UpdatedAt,
		policy.ID,
	)
	return errors.Trace(err)
}

func (w *worker) deleteMaskingPolicyFromSysTable(jobCtx *jobContext, policyID int64) error {
	const deleteSQL = "DELETE FROM mysql.tidb_masking_policy WHERE policy_id = %?"
	_, err := w.sess.Execute(jobCtx.stepCtx, deleteSQL, "drop-masking-policy", policyID)
	return errors.Trace(err)
}
