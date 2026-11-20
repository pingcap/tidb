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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	maskingmeta "github.com/pingcap/tidb/pkg/meta/masking"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
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
	submittedColumn := policyInfo.ColumnName

	if err := validateMaskingPolicyTarget(jobCtx.stepCtx, jobCtx.infoCache, policyInfo); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	// A rename may finish between submitter validation and worker execution.
	// Rebind and validate the expression against the refreshed column metadata.
	policyInfo.Expression, err = rewriteMaskingPolicyExprColumnName(policyInfo.Expression, submittedColumn, policyInfo.ColumnName)
	if err == nil {
		policyInfo.Expression, err = normalizeMaskingPolicyExprColumnNames(policyInfo.Expression)
	}
	if err == nil {
		tbl, _ := jobCtx.infoCache.GetLatest().TableByID(jobCtx.stepCtx, policyInfo.TableID)
		col := findColumnInfoByID(tbl.Meta().Columns, policyInfo.ColumnID)
		err = validateMaskingPolicyExpression(w.sess.Context, tbl.Meta(), col, policyInfo.Expression)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	rebindMaskingPolicyJob(job, policyInfo, submittedColumn)

	existPolicy, err := w.getMaskingPoliciesByTableIDFromSysTable(jobCtx.stepCtx, policyInfo.TableID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	for _, p := range existPolicy {
		if p.Name.L == policyInfo.Name.L {
			if p.ColumnID != policyInfo.ColumnID {
				job.State = model.JobStateCancelled
				return ver, errors.WithMessage(meta.ErrMaskingPolicyExists, "masking policy already exists on another column")
			}
			if !replaceOnExist {
				job.State = model.JobStateCancelled
				return ver, errors.WithMessage(meta.ErrMaskingPolicyExists, "masking policy already exists")
			}

			replacePolicy := p.Clone()
			// Keep persisted names/IDs synced if table/column was renamed after policy creation.
			replacePolicy.DBName = policyInfo.DBName
			replacePolicy.TableName = policyInfo.TableName
			replacePolicy.TableID = policyInfo.TableID
			replacePolicy.ColumnName = policyInfo.ColumnName
			replacePolicy.ColumnID = policyInfo.ColumnID
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
		return ver, meta.ErrMaskingPolicyNotExists.GenWithStackByArgs(policyName.O)
	}

	if err := validateMaskingPolicyTarget(jobCtx.stepCtx, jobCtx.infoCache, oldPolicy); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	newPolicy := oldPolicy.Clone()
	submittedColumn := newPolicy.ColumnName
	switch args.AlterAction {
	case ast.AlterTableEnableMaskingPolicy, ast.AlterTableDisableMaskingPolicy:
		newPolicy.Status = args.Policy.Status
	case ast.AlterTableModifyMaskingPolicyExpression:
		newPolicy.Expression = args.Policy.Expression
		newPolicy.MaskingType = args.Policy.MaskingType
	case ast.AlterTableModifyMaskingPolicyRestrictOn:
		newPolicy.RestrictOps = args.Policy.RestrictOps
	default:
		// Jobs written before AlterAction was introduced contain a full replacement.
		newPolicy.Expression = args.Policy.Expression
		newPolicy.Status = args.Policy.Status
		newPolicy.MaskingType = args.Policy.MaskingType
		newPolicy.RestrictOps = args.Policy.RestrictOps
	}
	if args.AlterAction != ast.AlterTableEnableMaskingPolicy &&
		args.AlterAction != ast.AlterTableDisableMaskingPolicy &&
		args.AlterAction != ast.AlterTableModifyMaskingPolicyRestrictOn {
		// A rename can finish after the submitter validated this expression.
		// Bind its column reference to the current name before validating again.
		submittedColumn = args.Policy.ColumnName
		if submittedColumn.L != newPolicy.ColumnName.L {
			newPolicy.Expression, err = rewriteMaskingPolicyExprColumnName(newPolicy.Expression, args.Policy.ColumnName, newPolicy.ColumnName)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
		}
		tbl, ok := jobCtx.infoCache.GetLatest().TableByID(jobCtx.stepCtx, newPolicy.TableID)
		if !ok {
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrTableNotExists.GenWithStackByArgs(newPolicy.DBName, newPolicy.TableName)
		}
		col := findColumnInfoByID(tbl.Meta().Columns, newPolicy.ColumnID)
		if col == nil {
			job.State = model.JobStateCancelled
			return ver, infoschema.ErrColumnNotExists.GenWithStackByArgs(newPolicy.ColumnName, newPolicy.TableName)
		}
		if err := validateMaskingPolicyExpression(w.sess.Context, tbl.Meta(), col, newPolicy.Expression); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	}
	rebindMaskingPolicyJob(job, newPolicy, submittedColumn)
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
		return ver, meta.ErrMaskingPolicyNotExists.GenWithStackByArgs(args.PolicyName.O)
	}
	policyInfo.State = model.StateNone
	// A table rename keeps the names of its policies in the system table synced.
	rebindMaskingPolicyJob(job, policyInfo, policyInfo.ColumnName)
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

var (
	queryMaskingPolicyByNameFromSysTable        = strings.Join([]string{maskingmeta.SelectSQL(), "WHERE policy_name = %? ORDER BY policy_id"}, " ")
	queryMaskingPolicyByTableColumnFromSysTable = strings.Join([]string{maskingmeta.SelectSQL(), "WHERE table_id = %? AND column_id = %? ORDER BY policy_id"}, " ")
	queryMaskingPolicyByIDFromSysTable          = strings.Join([]string{maskingmeta.SelectSQL(), "WHERE policy_id = %? ORDER BY policy_id"}, " ")
	queryMaskingPolicyByTableIDFromSysTable     = strings.Join([]string{maskingmeta.SelectSQL(), "WHERE table_id = %? ORDER BY policy_id"}, " ")
)

func (w *worker) queryMaskingPoliciesFromSysTable(ctx context.Context, query string, args ...any) ([]*model.MaskingPolicyInfo, error) {
	w.maskingPolicyTxn = true
	failpoint.Inject("mockMissingMaskingPolicySysTable", func() {
		failpoint.Return(nil, infoschema.ErrTableNotExists.GenWithStackByArgs("mysql", "tidb_masking_policy"))
	})
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
	tbl, ok := is.TableByID(ctx, policy.TableID)
	if !ok {
		return infoschema.ErrTableNotExists.GenWithStackByArgs(policy.DBName, policy.TableName)
	}
	tblInfo := tbl.Meta()

	dbInfo, ok := is.SchemaByID(tblInfo.DBID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(policy.DBName)
	}

	col := findColumnInfoByID(tblInfo.Columns, policy.ColumnID)
	if col == nil {
		return infoschema.ErrColumnNotExists.GenWithStackByArgs(policy.ColumnName, tblInfo.Name)
	}

	// Keep names in system table synced after table/column rename.
	policy.DBName = dbInfo.Name
	policy.TableName = tblInfo.Name
	policy.ColumnName = col.Name

	if err := checkMaskingPolicyTable(dbInfo, tblInfo); err != nil {
		return err
	}
	return checkMaskingPolicyColumn(col)
}

func findColumnInfoByID(cols []*model.ColumnInfo, columnID int64) *model.ColumnInfo {
	for _, col := range cols {
		if col.ID == columnID {
			return col
		}
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
	// show create table will display the policy name in comment
	// Disable some special characters to avoid such case:
	// mysql> create masking policy `p*/x` on t(id) as case when ...
	//
	// mysql> show create table t;
	// +-------+---------------------------------------------------------------------------------------------------------------------------------------------+
	// | Table | Create Table                                                                                                                                |
	// +-------+---------------------------------------------------------------------------------------------------------------------------------------------+
	// | t     | CREATE TABLE `t` (
	//   `id` blob DEFAULT NULL /* MASKING POLICY `p*/x` ENABLED */
	// ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin |
	// +-------+---------------------------------------------------------------------------------------------------------------------------------------------+
	// 1 row in set (0.001 sec)
	//
	if strings.ContainsAny(policyName.O, "/*") {
		return nil, errors.New("Cannot use character * or / in masking policy name")
	}

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
	now := time.Now().UTC()
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

func validateMaskingPolicyExpression(ctx sessionctx.Context, tblInfo *model.TableInfo, targetCol *model.ColumnInfo, exprStr string) error {
	// Worker sessions do not load common global variables. Supply the default
	// when validating an AES signature, without changing a submitter's setting.
	if _, exists := ctx.GetSessionVars().GetSystemVar(variable.BlockEncryptionMode); !exists {
		if err := ctx.GetSessionVars().SetSystemVar(variable.BlockEncryptionMode, variable.DefBlockEncryptionMode); err != nil {
			return err
		}
	}
	expr, err := expression.ParseSimpleExpr(ctx.GetExprCtx(), exprStr, expression.WithTableInfo("", tblInfo), expression.WithDisableConstantFolding())
	if err != nil {
		return errors.Trace(err)
	}
	// Ensure the expression only references the target column.
	// This prevents policies like ON t(a) AS b which would break if column b is dropped.
	cols := expression.ExtractColumns(expr)
	for _, col := range cols {
		if col.ID != targetCol.ID {
			return meta.ErrMaskingPolicyExprInvalidColumn.FastGenByArgs(targetCol.Name.O)
		}
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
		maskingmeta.RestrictOpsToString(policy.RestrictOps),
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
	failpoint.Inject("mockMaskingPolicyUpdateError", func(val failpoint.Value) {
		if enabled, ok := val.(bool); ok && enabled {
			failpoint.Return(errors.New("mock masking policy update error"))
		}
	})
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
		maskingmeta.RestrictOpsToString(policy.RestrictOps),
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

// restoreMaskingPoliciesForRecoveredTable reads policies before DROP removed
// them. Snapshot reads use a separate session, leaving the worker's metadata
// transaction intact for the policy writes and publication of the table.
func (w *worker) restoreMaskingPoliciesForRecoveredTable(jobCtx *jobContext, info *model.RecoverTableInfo) error {
	rows, _, err := w.sess.GetRestrictedSQLExecutor().ExecRestrictedSQL(jobCtx.stepCtx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(info.SnapshotTS)},
		queryMaskingPolicyByTableIDFromSysTable, info.TableInfo.ID)
	if err != nil {
		// A table dropped before masking was introduced has no policy table in
		// its snapshot. Other failures must prevent an unprotected recovery.
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		return errors.Trace(err)
	}
	db, err := jobCtx.metaMut.GetDatabase(info.SchemaID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		policy, err := maskingmeta.DecodeMaskingPolicyRow(row)
		if err != nil {
			return err
		}
		col := findColumnInfoByID(info.TableInfo.Columns, policy.ColumnID)
		if col == nil {
			return infoschema.ErrColumnNotExists.GenWithStackByArgs(policy.ColumnName, info.TableInfo.Name)
		}
		policy.DBName = db.Name
		policy.TableName = info.TableInfo.Name
		policy.ColumnName = col.Name
		policy.Expression, err = normalizeMaskingPolicyExprColumnNames(policy.Expression)
		if err != nil {
			return err
		}
		w.maskingPolicyTxn = true
		if err := w.insertMaskingPolicyIntoSysTable(jobCtx, policy); err != nil {
			return err
		}
		failpoint.Inject("mockRestoreMaskingPolicyError", func() {
			failpoint.Return(errors.New("mock masking policy recovery error"))
		})
	}
	return nil
}

// dropMaskingPoliciesByDBName deletes all masking policies for a given database
// in a single SQL statement using the db_name column.
func (w *worker) dropMaskingPoliciesByDBName(jobCtx *jobContext, dbName string) error {
	w.maskingPolicyTxn = true
	const deleteSQL = "DELETE FROM mysql.tidb_masking_policy WHERE LOWER(db_name) = %?"
	_, err := w.sess.Execute(jobCtx.stepCtx, deleteSQL, "drop-masking-policies-by-db", strings.ToLower(dbName))
	if err != nil {
		// Masking-policy cleanup for DROP DATABASE is best-effort. If the policy
		// system table does not exist, there are no policy rows to delete.
		if infoschema.ErrTableNotExists.Equal(err) {
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

// updateMaskingPolicyTableID updates the table_id in
// mysql.tidb_masking_policy when TRUNCATE, repartitioning or EXCHANGE PARTITION
// replaces the table ID. Column IDs remain the same, preserving column bindings.
func (w *worker) updateMaskingPolicyTableID(jobCtx *jobContext, oldTableID, newTableID int64) error {
	policies, err := w.getMaskingPoliciesByTableIDFromSysTable(jobCtx.stepCtx, oldTableID)
	if err != nil {
		return errors.Trace(err)
	}
	const updateSQL = `UPDATE mysql.tidb_masking_policy
		SET table_id = %?, updated_at = %?
		WHERE policy_id = %?`
	now := time.Now().UTC()
	for _, policy := range policies {
		failpoint.Inject("mockMaskingPolicyUpdateError", func(val failpoint.Value) {
			if enabled, ok := val.(bool); ok && enabled {
				failpoint.Return(errors.New("mock masking policy update error"))
			}
		})
		_, err := w.sess.Execute(jobCtx.stepCtx, updateSQL, "update-masking-policy-table-id",
			newTableID, now, policy.ID,
		)
		if err != nil {
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

// updateMaskingPolicyNamesAfterRename updates the db_name and table_name in
// mysql.tidb_masking_policy after a table is renamed.
func (w *worker) updateMaskingPolicyNamesAfterRename(
	ctx context.Context,
	tableID int64,
	_ /* oldDBName */, newDBName ast.CIStr,
	_ /* oldTableName */, newTableName ast.CIStr,
) error {
	policies, err := w.getMaskingPoliciesByTableIDFromSysTable(ctx, tableID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.DBName.L == newDBName.L && policy.TableName.L == newTableName.L {
			continue
		}

		newPolicy := policy.Clone()
		newPolicy.DBName = newDBName
		newPolicy.TableName = newTableName
		newPolicy.UpdatedAt = time.Now().UTC()

		newPolicy.Expression, err = normalizeMaskingPolicyExprColumnNames(policy.Expression)
		if err != nil {
			return errors.Trace(err)
		}
		if err = w.updateMaskingPolicyNamesInSysTable(ctx, newPolicy); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// updateMaskingPolicyNamesInSysTable updates names and the normalized expression.
func (w *worker) updateMaskingPolicyNamesInSysTable(ctx context.Context, policy *model.MaskingPolicyInfo) error {
	failpoint.Inject("mockMaskingPolicyUpdateError", func(val failpoint.Value) {
		if enabled, ok := val.(bool); ok && enabled {
			failpoint.Return(errors.New("mock masking policy update error"))
		}
	})
	const updateSQL = `UPDATE mysql.tidb_masking_policy
		SET db_name = %?, table_name = %?, expression = %?, updated_at = %?
		WHERE policy_id = %?`
	_, err := w.sess.Execute(ctx, updateSQL, "update-masking-policy-names",
		policy.DBName.O,
		policy.TableName.O,
		policy.Expression,
		policy.UpdatedAt,
		policy.ID,
	)
	return errors.Trace(err)
}

// syncMaskingPolicyForModifiedColumn updates masking policy metadata in
// mysql.tidb_masking_policy when a column is renamed or its ID changes.
func (w *worker) syncMaskingPolicyForModifiedColumn(
	jobCtx *jobContext,
	tblInfo *model.TableInfo,
	oldCol *model.ColumnInfo,
	newCol *model.ColumnInfo,
) error {
	if tblInfo == nil || oldCol == nil || newCol == nil {
		return nil
	}

	policies, err := w.getMaskingPoliciesByTableIDFromSysTable(jobCtx.stepCtx, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, policy := range policies {
		if policy.TableID != tblInfo.ID {
			continue
		}
		if policy.ColumnID != oldCol.ID && policy.ColumnName.L != oldCol.Name.L && policy.ColumnName.L != newCol.Name.L {
			continue
		}

		// Reject type changes that are unsupported by masking policy.
		if err := checkMaskingPolicyColumn(newCol); err != nil {
			return err
		}

		newPolicy := policy.Clone()
		newPolicy.TableName = tblInfo.Name
		newPolicy.ColumnID = newCol.ID
		newPolicy.ColumnName = newCol.Name
		if policy.ColumnName.L != newCol.Name.L {
			newExpr, err := rewriteMaskingPolicyExprColumnName(policy.Expression, policy.ColumnName, newCol.Name)
			if err != nil {
				return errors.Trace(err)
			}
			newPolicy.Expression = newExpr
		}
		newPolicy.UpdatedAt = time.Now().UTC()

		if err := w.updateMaskingPolicyInSysTable(jobCtx, newPolicy); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type renameMaskingExprVisitor struct {
	oldCol ast.CIStr
	newCol ast.CIStr
}

// Only the bound column may be referenced. Removing its qualifiers preserves
// that binding when a table is renamed, including a move to another database.
type normalizeMaskingExprVisitor struct{}

func (*normalizeMaskingExprVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if col, ok := in.(*ast.ColumnNameExpr); ok {
		col.Name.Schema = ast.CIStr{}
		col.Name.Table = ast.CIStr{}
	}
	return in, false
}

func (*normalizeMaskingExprVisitor) Leave(in ast.Node) (ast.Node, bool) { return in, true }

func normalizeMaskingPolicyExprColumnNames(expr string) (string, error) {
	// #nosec G202: expression here is parsed as SQL AST for rewrite, not executed against storage.
	stmt, err := parser.New().ParseOneStmt("SELECT "+expr, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok || selectStmt.Fields == nil || len(selectStmt.Fields.Fields) != 1 {
		return "", errors.New("invalid masking policy expression")
	}
	out, ok := selectStmt.Fields.Fields[0].Expr.Accept(&normalizeMaskingExprVisitor{})
	if !ok {
		return "", errors.New("failed to normalize masking policy expression")
	}
	return restoreMaskingExpression(out.(ast.ExprNode))
}

func (v *renameMaskingExprVisitor) Enter(in ast.Node) (ast.Node, bool) {
	colExpr, ok := in.(*ast.ColumnNameExpr)
	if !ok {
		return in, false
	}
	if colExpr.Name.Name.L != v.oldCol.L {
		return in, false
	}
	colExpr.Name.Name = v.newCol
	return in, false
}

func (*renameMaskingExprVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func rewriteMaskingPolicyExprColumnName(expr string, oldCol, newCol ast.CIStr) (string, error) {
	if oldCol.L == newCol.L {
		return expr, nil
	}
	// #nosec G202: expression here is parsed as SQL AST for rewrite, not executed against storage.
	stmt, err := parser.New().ParseOneStmt("SELECT "+expr, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok || selectStmt.Fields == nil || len(selectStmt.Fields.Fields) != 1 {
		return "", errors.New("invalid masking policy expression")
	}
	outExpr := selectStmt.Fields.Fields[0].Expr
	rewritten, ok := outExpr.Accept(&renameMaskingExprVisitor{oldCol: oldCol, newCol: newCol})
	if !ok {
		return "", errors.New("failed to rewrite masking policy expression")
	}
	return restoreMaskingExpression(rewritten.(ast.ExprNode))
}

// rebindMaskingPolicyJob replaces the submitted names and expression in
// job.Query, and the job's schema and table names, with the ones the worker
// bound, keeping the other options. A table or column renamed after submission
// no longer exists under the submitted name, so the submitted text would leave
// in DDL history a statement that names objects which are not there: the
// history records the statement as it took effect.
func rebindMaskingPolicyJob(job *model.Job, policy *model.MaskingPolicyInfo, submittedColumn ast.CIStr) {
	if job.SchemaName == policy.DBName.L && job.TableName == policy.TableName.L && submittedColumn.L == policy.ColumnName.L {
		return
	}
	job.SchemaName, job.TableName = policy.DBName.L, policy.TableName.L
	if job.Query == "" {
		return
	}
	query, err := buildReboundMaskingPolicyQuery(job, policy)
	if err != nil {
		logutil.DDLLogger().Warn("failed to rewrite masking policy job query",
			zap.Int64("jobID", job.ID), zap.String("query", job.Query), zap.Error(err))
		return
	}
	job.Query = query
}

func buildReboundMaskingPolicyQuery(job *model.Job, policy *model.MaskingPolicyInfo) (string, error) {
	p := parser.New()
	p.SetSQLMode(job.SQLMode)
	stmt, err := p.ParseOneStmt(job.Query, job.Charset, job.Collate)
	if err != nil {
		return "", errors.Trace(err)
	}
	// #nosec G202: expression here is parsed as SQL AST for rewrite, not executed against storage.
	exprStmt, err := parser.New().ParseOneStmt("SELECT "+policy.Expression, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}
	selectStmt, ok := exprStmt.(*ast.SelectStmt)
	if !ok || selectStmt.Fields == nil || len(selectStmt.Fields.Fields) != 1 {
		return "", errors.New("invalid masking policy expression")
	}
	expr := selectStmt.Fields.Fields[0].Expr

	switch s := stmt.(type) {
	case *ast.CreateMaskingPolicyStmt:
		s.Table.Schema, s.Table.Name = policy.DBName, policy.TableName
		s.Column = &ast.ColumnName{Name: policy.ColumnName}
		s.Expr = expr
	case *ast.AlterTableStmt:
		if len(s.Specs) != 1 {
			return "", errors.New("unexpected masking policy statement")
		}
		s.Table.Schema, s.Table.Name = policy.DBName, policy.TableName
		switch spec := s.Specs[0]; spec.Tp {
		case ast.AlterTableAddMaskingPolicy:
			spec.MaskingPolicyColumn = &ast.ColumnName{Name: policy.ColumnName}
			spec.MaskingPolicyExpr = expr
		case ast.AlterTableModifyMaskingPolicyExpression:
			spec.MaskingPolicyExpr = expr
		}
	default:
		return "", errors.New("unexpected masking policy statement")
	}
	var sb strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}

func maskingPolicyFromSysTableRow(row chunk.Row) (*model.MaskingPolicyInfo, error) {
	return maskingmeta.DecodeMaskingPolicyRow(row)
}
