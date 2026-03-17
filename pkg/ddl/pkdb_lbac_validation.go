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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	lbac "github.com/pingcap/tidb/pkg/privilege/lbac"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func validateTablePolicyAndLabels(execCtx context.Context, ctx sessionctx.Context, tbInfo *model.TableInfo) error {
	if !variable.EnableLBAC.Load() {
		return nil
	}
	hasColumnSecurityLabel := false
	for _, col := range tbInfo.Columns {
		if col.SecurityLabel != nil && col.SecurityLabel.L != "" {
			hasColumnSecurityLabel = true
			break
		}
	}
	if tbInfo.SecurityPolicy == nil {
		if hasColumnSecurityLabel {
			return lbac.ErrSecurityPolicyMissing
		}
		return nil
	}
	execCtx = kv.WithInternalSourceType(execCtx, kv.InternalTxnLabelSecurity)
	exec := ctx.GetRestrictedSQLExecutor()
	policyName := tbInfo.SecurityPolicy.L
	if policyName == "" {
		return lbac.PolicyError(lbac.ErrInvalidPolicyName, policyName)
	}
	if err := EnsurePolicyExists(execCtx, exec, policyName); err != nil {
		return lbac.PolicyError(err, policyName)
	}

	for _, col := range tbInfo.Columns {
		if err := validateColumnSecurityLabel(execCtx, exec, col, policyName); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnSecurityLabelForTable(execCtx context.Context, ctx sessionctx.Context, tbInfo *model.TableInfo, col *model.ColumnInfo) error {
	if !variable.EnableLBAC.Load() {
		return nil
	}

	execCtx = kv.WithInternalSourceType(execCtx, kv.InternalTxnLabelSecurity)
	exec := ctx.GetRestrictedSQLExecutor()
	policyName := ""
	if tbInfo.SecurityPolicy != nil {
		policyName = tbInfo.SecurityPolicy.L
	}
	if policyName != "" {
		if err := EnsurePolicyExists(execCtx, exec, policyName); err != nil {
			return lbac.PolicyError(err, policyName)
		}
	}
	return validateColumnSecurityLabel(execCtx, exec, col, policyName)
}

func validateColumnSecurityLabel(
	execCtx context.Context,
	exec sqlexec.RestrictedSQLExecutor,
	col *model.ColumnInfo,
	policyName string,
) error {
	if col.SecurityLabel == nil || col.SecurityLabel.L == "" {
		return nil
	}
	if policyName == "" {
		return lbac.ErrSecurityPolicyMissing
	}
	labelName := col.SecurityLabel.L
	if err := EnsureLabelExists(execCtx, exec, policyName, labelName); err != nil {
		return lbac.LabelError(err, labelName)
	}
	return nil
}

// EnsurePolicyExists validates that a policy exists in mysql.security_policies.
func EnsurePolicyExists(execCtx context.Context, exec sqlexec.RestrictedSQLExecutor, policyName string) error {
	sql := `SELECT COUNT(*) FROM mysql.security_policies WHERE name = %?`
	rows, _, err := exec.ExecRestrictedSQL(execCtx, nil, sql, policyName)
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[0].GetInt64(0) == 0 {
		return lbac.ErrPolicyNotFound
	}
	return nil
}

// EnsureLabelExists validates that a label exists for a policy in mysql.security_labels.
func EnsureLabelExists(execCtx context.Context, exec sqlexec.RestrictedSQLExecutor, policyName, labelName string) error {
	sql := `SELECT COUNT(*) FROM mysql.security_labels WHERE name = %? AND policy_name = %?`
	rows, _, err := exec.ExecRestrictedSQL(execCtx, nil, sql, labelName, policyName)
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[0].GetInt64(0) == 0 {
		return lbac.ErrLabelNotFound
	}
	return nil
}
