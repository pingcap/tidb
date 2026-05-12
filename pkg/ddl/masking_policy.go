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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
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

	existPolicy, err := w.getMaskingPoliciesByTableIDFromSysTable(jobCtx.stepCtx, policyInfo.TableID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	for _, p := range existPolicy {
		if p.Name.L == policyInfo.Name.L {
			if p.ColumnID != policyInfo.ColumnID {
				job.State = model.JobStateCancelled
				return ver, errors.Errorf("masking policy %s already exists on another column", p.Name.O)
			}
			if !replaceOnExist {
				job.State = model.JobStateCancelled
				return ver, errors.Errorf("masking policy %s already exists", policyInfo.Name.O)
			}

			replacePolicy := p.Clone()
			replacePolicy.Expression = policyInfo.Expression
			replacePolicy.Status = policyInfo.Status
			replacePolicy.MaskingType = policyInfo.MaskingType
			replacePolicy.RestrictOps = policyInfo.RestrictOps
			replacePolicy.UpdatedAt = policyInfo.UpdatedAt
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
	}

	switch policyInfo.State {
	case model.StateNone:
		policyInfo.State = model.StatePublic
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

	oldPolicy, err := w.getMaskingPolicyByIDFromSysTable(jobCtx.stepCtx, args.PolicyID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if oldPolicy == nil {
		job.State = model.JobStateCancelled
		policyName := args.PolicyName
		if args.Policy != nil {
			policyName = args.Policy.Name
		}
		return ver, errors.Errorf("masking policy %s doesn't exist", policyName.O)
	}

	if err := validateMaskingPolicyTarget(jobCtx.stepCtx, jobCtx.infoCache, oldPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	newPolicy := oldPolicy.Clone()
	newPolicy.Expression = args.Policy.Expression
	newPolicy.Status = args.Policy.Status
	newPolicy.MaskingType = args.Policy.MaskingType
	newPolicy.RestrictOps = args.Policy.RestrictOps
	newPolicy.UpdatedAt = args.Policy.UpdatedAt
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

	policyInfo, err := w.getMaskingPolicyByIDFromSysTable(jobCtx.stepCtx, args.PolicyID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if policyInfo == nil {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("masking policy %s doesn't exist", args.PolicyName.O)
	}
	policyInfo.State = model.StateNone
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

func (w *worker) getMaskingPolicyByNameFromSysTable(ctx context.Context, policyName ast.CIStr) (*model.MaskingPolicyInfo, error) {
	policies, err := w.queryMaskingPoliciesFromSysTable(ctx, queryMaskingPolicyByNameFromSysTable, policyName.O)
	if err != nil {
		return nil, err
	}
	if len(policies) == 0 {
		return nil, nil
	}
	return policies[0], nil
}

func (w *worker) getMaskingPolicyByTableColumnFromSysTable(ctx context.Context, tableID, columnID int64) (*model.MaskingPolicyInfo, error) {
	policies, err := w.queryMaskingPoliciesFromSysTable(ctx, queryMaskingPolicyByTableColumnFromSysTable, tableID, columnID)
	if err != nil {
		return nil, err
	}
	if len(policies) == 0 {
		return nil, nil
	}
	return policies[0], nil
}

func (w *worker) getMaskingPolicyByIDFromSysTable(ctx context.Context, policyID int64) (*model.MaskingPolicyInfo, error) {
	policies, err := w.queryMaskingPoliciesFromSysTable(ctx, queryMaskingPolicyByIDFromSysTable, policyID)
	if err != nil {
		return nil, err
	}
	if len(policies) == 0 {
		return nil, nil
	}
	return policies[0], nil
}

func (w *worker) getMaskingPoliciesByTableIDFromSysTable(ctx context.Context, tableID int64) ([]*model.MaskingPolicyInfo, error) {
	return w.queryMaskingPoliciesFromSysTable(ctx, queryMaskingPolicyByTableIDFromSysTable, tableID)
}

func (w *worker) getMaskingPoliciesByTableColumnFromSysTable(ctx context.Context, tableID, columnID int64) ([]*model.MaskingPolicyInfo, error) {
	return w.queryMaskingPoliciesFromSysTable(ctx, queryMaskingPolicyByTableColumnFromSysTable, tableID, columnID)
}

const (
	queryMaskingPolicyFromSysTable = `SELECT policy_id, policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by
		FROM mysql.tidb_masking_policy`
	queryMaskingPolicyByNameFromSysTable        = queryMaskingPolicyFromSysTable + ` WHERE policy_name = %? ORDER BY policy_id`
	queryMaskingPolicyByTableColumnFromSysTable = queryMaskingPolicyFromSysTable + ` WHERE table_id = %? AND column_id = %? ORDER BY policy_id`
	queryMaskingPolicyByIDFromSysTable          = queryMaskingPolicyFromSysTable + ` WHERE policy_id = %? ORDER BY policy_id`
	queryMaskingPolicyByTableIDFromSysTable     = queryMaskingPolicyFromSysTable + ` WHERE table_id = %? ORDER BY policy_id`
)

func (w *worker) queryMaskingPoliciesFromSysTable(ctx context.Context, query string, args ...any) ([]*model.MaskingPolicyInfo, error) {
	rows, err := w.sess.Execute(ctx, query, "query-masking-policy", args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	policies := make([]*model.MaskingPolicyInfo, 0, len(rows))
	for _, row := range rows {
		policy, err := maskingPolicyFromSysTableRow(row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies = append(policies, policy)
	}
	return policies, nil
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
	return checkMaskingPolicyColumn(col)
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
	if types.IsTypeBlob(tp) {
		return true
	}
	if types.IsTypeNumeric(tp) {
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
	restrictOps ast.MaskingPolicyRestrictOps,
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

	if err := validateMaskingPolicyExpression(ctx, tblInfo, col.ColumnInfo, exprStr); err != nil {
		return nil, err
	}

	status := maskingPolicyStatusFromState(state)
	maskingType := maskingPolicyTypeFromExpr(expr)
	now := time.Now()
	createdBy := ""
	sessVars := ctx.GetSessionVars() //nolint:forbidigo
	if user := sessVars.User; user != nil {
		createdBy = user.String()
	}
	return &model.MaskingPolicyInfo{
		Name:        policyName,
		DBName:      schema.Name,
		TableName:   tblInfo.Name,
		TableID:     tblInfo.ID,
		ColumnName:  col.Name,
		ColumnID:    col.ID,
		Expression:  exprStr,
		Status:      status,
		MaskingType: maskingType,
		RestrictOps: restrictOps,
		CreatedAt:   now,
		UpdatedAt:   now,
		CreatedBy:   createdBy,
		State:       model.StateNone,
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

func validateMaskingPolicyExpression(ctx sessionctx.Context, tblInfo *model.TableInfo, _ *model.ColumnInfo, exprStr string) error {
	_, err := expression.ParseSimpleExpr(ctx.GetExprCtx(), exprStr, expression.WithTableInfo("", tblInfo))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func maskingPolicyStatusFromState(state ast.MaskingPolicyState) model.MaskingPolicyStatus {
	if state.Explicit && !state.Enabled {
		return model.MaskingPolicyStatusDisable
	}
	return model.MaskingPolicyStatusEnable
}

func maskingPolicyTypeFromExpr(expr ast.ExprNode) model.MaskingPolicyType {
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return model.MaskingPolicyTypeCustom
	}
	switch strings.ToLower(fn.FnName.L) {
	case "mask_full":
		return model.MaskingPolicyTypeFull
	case "mask_partial":
		return model.MaskingPolicyTypePartial
	case "mask_null":
		return model.MaskingPolicyTypeNull
	case "mask_date":
		return model.MaskingPolicyTypeDate
	default:
		return model.MaskingPolicyTypeCustom
	}
}

func (w *worker) insertMaskingPolicyIntoSysTable(jobCtx *jobContext, policy *model.MaskingPolicyInfo) error {
	const insertSQL = `INSERT INTO mysql.tidb_masking_policy
		(policy_name, db_name, table_name, table_id, column_name, column_id, expression, status, masking_type, restrict_on, created_at, updated_at, created_by)
		VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`
	_, err := w.sess.Execute(jobCtx.stepCtx, insertSQL, "create-masking-policy",
		policy.Name.O,
		policy.DBName.O,
		policy.TableName.O,
		policy.TableID,
		policy.ColumnName.O,
		policy.ColumnID,
		policy.Expression,
		policy.Status.String(),
		string(policy.MaskingType),
		maskingPolicyRestrictOpsToString(policy.RestrictOps),
		policy.CreatedAt,
		policy.UpdatedAt,
		policy.CreatedBy,
	)
	if err != nil {
		return errors.Trace(err)
	}
	rows, err := w.sess.Execute(jobCtx.stepCtx, "SELECT LAST_INSERT_ID()", "last-insert-id-masking-policy")
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) != 1 {
		return errors.Errorf("unexpected last insert id row count: %d", len(rows))
	}
	policy.ID = rows[0].GetInt64(0)
	return nil
}

func (w *worker) updateMaskingPolicyInSysTable(jobCtx *jobContext, policy *model.MaskingPolicyInfo) error {
	const updateSQL = `UPDATE mysql.tidb_masking_policy
		SET policy_name = %?, db_name = %?, table_name = %?, table_id = %?, column_name = %?, column_id = %?, expression = %?,
			status = %?, masking_type = %?, restrict_on = %?, updated_at = %?
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
		string(policy.MaskingType),
		maskingPolicyRestrictOpsToString(policy.RestrictOps),
		policy.UpdatedAt,
		policy.ID,
	)
	return errors.Trace(err)
}

func maskingPolicyRestrictOpsToString(ops ast.MaskingPolicyRestrictOps) string {
	if ops == ast.MaskingPolicyRestrictOpNone {
		return "NONE"
	}

	vals := make([]string, 0, 4)
	if ops&ast.MaskingPolicyRestrictOpInsertIntoSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameInsertIntoSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpUpdateSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameUpdateSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpDeleteSelect != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameDeleteSelect)
	}
	if ops&ast.MaskingPolicyRestrictOpCTAS != 0 {
		vals = append(vals, ast.MaskingPolicyRestrictNameCTAS)
	}
	return strings.Join(vals, ",")
}

func (w *worker) deleteMaskingPolicyFromSysTable(jobCtx *jobContext, policyID int64) error {
	const deleteSQL = "DELETE FROM mysql.tidb_masking_policy WHERE policy_id = %?"
	_, err := w.sess.Execute(jobCtx.stepCtx, deleteSQL, "drop-masking-policy", policyID)
	return errors.Trace(err)
}

func (w *worker) dropMaskingPoliciesOnTable(jobCtx *jobContext, tableID int64) error {
	policies, err := w.getMaskingPoliciesByTableIDFromSysTable(jobCtx.stepCtx, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if err := w.deleteMaskingPolicyFromSysTable(jobCtx, policy.ID); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (w *worker) dropMaskingPoliciesOnColumn(jobCtx *jobContext, tableID, columnID int64) error {
	policies, err := w.getMaskingPoliciesByTableColumnFromSysTable(jobCtx.stepCtx, tableID, columnID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if err := w.deleteMaskingPolicyFromSysTable(jobCtx, policy.ID); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func maskingPolicyFromSysTableRow(row chunk.Row) (*model.MaskingPolicyInfo, error) {
	status, err := maskingPolicyStatusFromString(row.GetString(8))
	if err != nil {
		return nil, err
	}
	restrictOps, err := maskingPolicyRestrictOpsFromString(row.GetString(10))
	if err != nil {
		return nil, err
	}
	createdAt, err := row.GetTime(11).GoTime(time.Local)
	if err != nil {
		return nil, errors.Trace(err)
	}
	updatedAt, err := row.GetTime(12).GoTime(time.Local)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &model.MaskingPolicyInfo{
		ID:          row.GetInt64(0),
		Name:        ast.NewCIStr(row.GetString(1)),
		DBName:      ast.NewCIStr(row.GetString(2)),
		TableName:   ast.NewCIStr(row.GetString(3)),
		TableID:     row.GetInt64(4),
		ColumnName:  ast.NewCIStr(row.GetString(5)),
		ColumnID:    row.GetInt64(6),
		Expression:  row.GetString(7),
		Status:      status,
		MaskingType: maskingPolicyTypeFromString(row.GetString(9)),
		RestrictOps: restrictOps,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		CreatedBy:   row.GetString(13),
		State:       model.StatePublic,
	}, nil
}

func maskingPolicyStatusFromString(status string) (model.MaskingPolicyStatus, error) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ENABLE", "ENABLED":
		return model.MaskingPolicyStatusEnable, nil
	case "DISABLE", "DISABLED":
		return model.MaskingPolicyStatusDisable, nil
	default:
		return model.MaskingPolicyStatusDisable, errors.Errorf("unknown masking policy status: %s", status)
	}
}

func maskingPolicyTypeFromString(tp string) model.MaskingPolicyType {
	switch model.MaskingPolicyType(strings.ToUpper(strings.TrimSpace(tp))) {
	case model.MaskingPolicyTypeFull,
		model.MaskingPolicyTypePartial,
		model.MaskingPolicyTypeNull,
		model.MaskingPolicyTypeDate,
		model.MaskingPolicyTypeCustom:
		return model.MaskingPolicyType(strings.ToUpper(strings.TrimSpace(tp)))
	default:
		return model.MaskingPolicyTypeCustom
	}
}

func maskingPolicyRestrictOpsFromString(restrictOn string) (ast.MaskingPolicyRestrictOps, error) {
	restrictOn = strings.TrimSpace(strings.ToUpper(restrictOn))
	if restrictOn == "" || restrictOn == "NONE" {
		return ast.MaskingPolicyRestrictOpNone, nil
	}
	ops := ast.MaskingPolicyRestrictOpNone
	for _, token := range strings.Split(restrictOn, ",") {
		switch strings.TrimSpace(token) {
		case ast.MaskingPolicyRestrictNameInsertIntoSelect:
			ops |= ast.MaskingPolicyRestrictOpInsertIntoSelect
		case ast.MaskingPolicyRestrictNameUpdateSelect:
			ops |= ast.MaskingPolicyRestrictOpUpdateSelect
		case ast.MaskingPolicyRestrictNameDeleteSelect:
			ops |= ast.MaskingPolicyRestrictOpDeleteSelect
		case ast.MaskingPolicyRestrictNameCTAS:
			ops |= ast.MaskingPolicyRestrictOpCTAS
		case "NONE", "":
		default:
			return ast.MaskingPolicyRestrictOpNone, errors.Errorf("unknown masking policy restrict option: %s", token)
		}
	}
	return ops, nil
}
