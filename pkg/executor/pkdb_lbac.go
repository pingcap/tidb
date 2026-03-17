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

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/privilege/lbac"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func executeCreateSecurityLabelComponent(goCtx context.Context, sctx sessionctx.Context, stmt *ast.CreateSecurityLabelComponentStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	var payload any
	switch stmt.Type {
	case ast.LBACComponentTypeArray, ast.LBACComponentTypeSet:
		if len(stmt.Elements) == 0 {
			return lbac.ComponentError(lbac.ErrEmptyComponentValues, stmt.Name.O)
		}
		if err := validateUniqueStrings(stmt.Elements); err != nil {
			return lbac.ComponentError(err, stmt.Name.O)
		}
		payload = stmt.Elements
	case ast.LBACComponentTypeTree:
		if stmt.Tree == nil || len(stmt.Tree.Nodes) == 0 {
			return lbac.ComponentError(lbac.ErrInvalidComponentValue, stmt.Name.O)
		}
		nodes, err := flattenTreeNodes(stmt.Tree.Nodes)
		if err != nil {
			return lbac.ComponentError(err, stmt.Name.O)
		}
		payload = nodes
	default:
		return lbac.ComponentError(lbac.ErrInvalidComponentType, stmt.Name.O)
	}

	valuesJSON, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to marshal component values")
	}

	// Insert component into database
	sql := "INSERT INTO mysql.security_label_components (name, type, component_values) VALUES (%?, %?, %?)"
	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
		stmt.Name.L,
		stmt.Type.String(),
		string(valuesJSON),
	)
	if err != nil {
		if isDuplicateError(err) {
			return lbac.ComponentError(lbac.ErrComponentAlreadyExists, stmt.Name.O)
		}
		return errors.Wrap(err, "failed to create security label component")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeDropSecurityLabelComponent(goCtx context.Context, sctx sessionctx.Context, stmt *ast.DropSecurityLabelComponentStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	if err := ensureComponentsExist(goCtx, exec, []string{stmt.Name.L}); err != nil {
		return lbac.ComponentError(err, stmt.Name.O)
	}

	// Check if component is being used by any policies
	sql := `SELECT COUNT(*) FROM mysql.security_policies p WHERE JSON_CONTAINS(p.component_names, JSON_QUOTE(%?), '$')`
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to check component usage")
	}
	if len(rows) > 0 && rows[0].GetInt64(0) > 0 {
		return lbac.ComponentError(lbac.ErrComponentInUse, stmt.Name.O)
	}

	// Delete component
	sql = `DELETE FROM mysql.security_label_components WHERE name = %?`
	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to drop security label component")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeCreateSecurityPolicy(goCtx context.Context, sctx sessionctx.Context, stmt *ast.CreateSecurityPolicyStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	// Extract component names
	componentNames := make([]string, len(stmt.Components))
	for i, comp := range stmt.Components {
		componentNames[i] = comp.L
	}
	if len(componentNames) == 0 {
		return lbac.PolicyError(lbac.ErrEmptyPolicyComponents, stmt.Name.O)
	}
	if err := validateUniqueStrings(componentNames); err != nil {
		if err == lbac.ErrDuplicateComponentValue {
			return lbac.PolicyError(lbac.ErrDuplicateComponentInPolicy, stmt.Name.O)
		}
		return lbac.PolicyError(err, stmt.Name.O)
	}
	if err := ensureComponentsExist(goCtx, exec, componentNames); err != nil {
		return lbac.PolicyError(err, stmt.Name.O)
	}

	// Prepare component names as JSON
	componentsJSON, err := json.Marshal(componentNames)
	if err != nil {
		return errors.Wrap(err, "failed to marshal component names")
	}
	// Insert policy into database
	sql := `INSERT INTO mysql.security_policies (name, component_names, write_control) VALUES (%?, %?, %?)`
	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
		stmt.Name.L,
		string(componentsJSON),
		stmt.Option.String(),
	)
	if err != nil {
		if isDuplicateError(err) {
			return lbac.PolicyError(lbac.ErrPolicyAlreadyExists, stmt.Name.O)
		}
		return errors.Wrap(err, "failed to create security policy")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeDropSecurityPolicy(goCtx context.Context, sctx sessionctx.Context, stmt *ast.DropSecurityPolicyStmt) error {
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	if err := ddl.EnsurePolicyExists(goCtx, exec, stmt.Name.L); err != nil {
		return lbac.PolicyError(err, stmt.Name.O)
	}

	// Check if policy is being used by any labels
	sql := `SELECT COUNT(*) FROM mysql.security_labels WHERE policy_name = %?`
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to check policy usage")
	}

	if len(rows) > 0 && rows[0].GetInt64(0) > 0 {
		return lbac.PolicyError(lbac.ErrPolicyInUse, stmt.Name.O)
	}

	infoSchema := domain.GetDomain(sctx).InfoSchema()
	_, _, found, err := findTableUsingPolicy(infoSchema, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to check policy table usage")
	}
	if found {
		return lbac.PolicyError(lbac.ErrPolicyInUseByTables, stmt.Name.O)
	}

	sql = `SELECT user_name, host FROM mysql.user_exemptions WHERE policy_name = %? LIMIT 1`
	rows, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to check policy exemption usage")
	}
	if len(rows) > 0 {
		return lbac.PolicyError(lbac.ErrPolicyInUseByExemptions, stmt.Name.O)
	}

	// Delete policy
	sql = `DELETE FROM mysql.security_policies WHERE name = %?`
	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.Name.L)
	if err != nil {
		return errors.Wrap(err, "failed to drop security policy")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeCreateSecurityLabel(goCtx context.Context, sctx sessionctx.Context, stmt *ast.CreateSecurityLabelStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	if len(stmt.Components) == 0 {
		return lbac.LabelError(lbac.ErrEmptyLabelComponents, stmt.LabelName.O)
	}

	policyComponents, err := loadPolicyComponents(goCtx, exec, stmt.PolicyName.L)
	if err != nil {
		return lbac.PolicyError(err, stmt.PolicyName.O)
	}
	componentDefs, err := loadComponentDefs(goCtx, exec, policyComponents)
	if err != nil {
		return lbac.ComponentError(err, stmt.PolicyName.O)
	}

	specs := make(map[string]*ast.SecurityLabelComponentSpec, len(stmt.Components))
	for _, comp := range stmt.Components {
		if _, exists := specs[comp.ComponentName.L]; exists {
			return lbac.LabelError(lbac.ErrInvalidComponentForLabel, stmt.LabelName.O)
		}
		specs[comp.ComponentName.L] = comp
	}
	allowedComponents := make(map[string]struct{}, len(policyComponents))
	for _, name := range policyComponents {
		allowedComponents[name] = struct{}{}
	}
	for name := range specs {
		if _, ok := allowedComponents[name]; !ok {
			return lbac.LabelError(lbac.ErrInvalidComponentForLabel, stmt.LabelName.O)
		}
	}

	components := make(map[string]any, len(policyComponents))
	for _, compName := range policyComponents {
		spec, exists := specs[compName]
		if !exists {
			return lbac.LabelError(lbac.ErrInvalidComponentForLabel, stmt.LabelName.O)
		}
		def, exists := componentDefs[compName]
		if !exists {
			return lbac.LabelError(lbac.ErrInvalidComponentForLabel, stmt.LabelName.O)
		}
		values := spec.Values
		switch def.Type {
		case ast.LBACComponentTypeArray, ast.LBACComponentTypeTree:
			if len(values) != 1 {
				return lbac.LabelError(lbac.ErrInvalidComponentValue, stmt.LabelName.O)
			}
			if err := def.validateValue(values[0]); err != nil {
				return lbac.LabelError(err, stmt.LabelName.O)
			}
			components[compName] = values[0]
		case ast.LBACComponentTypeSet:
			if len(values) == 0 {
				return lbac.LabelError(lbac.ErrInvalidComponentValue, stmt.LabelName.O)
			}
			if err := validateUniqueStrings(values); err != nil {
				return lbac.LabelError(err, stmt.LabelName.O)
			}
			for _, value := range values {
				if err := def.validateValue(value); err != nil {
					return lbac.LabelError(err, stmt.LabelName.O)
				}
			}
			components[compName] = values
		default:
			return lbac.LabelError(lbac.ErrInvalidComponentType, stmt.LabelName.O)
		}
	}

	componentsJSON, err := json.Marshal(components)
	if err != nil {
		return errors.Wrap(err, "failed to marshal label components")
	}

	// Insert label into database
	sql := `INSERT INTO mysql.security_labels (name, policy_name, components) VALUES (%?, %?, %?)`

	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
		stmt.LabelName.L,
		stmt.PolicyName.L,
		string(componentsJSON),
	)

	if err != nil {
		if isDuplicateError(err) {
			return lbac.LabelError(lbac.ErrLabelAlreadyExists, stmt.LabelName.O)
		}
		return errors.Wrap(err, "failed to create security label")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeDropSecurityLabel(goCtx context.Context, sctx sessionctx.Context, stmt *ast.DropSecurityLabelStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	if err := ddl.EnsureLabelExists(goCtx, exec, stmt.PolicyName.L, stmt.LabelName.L); err != nil {
		return lbac.LabelError(err, stmt.LabelName.O)
	}

	// Check if label is being used by any users
	sql := `
		SELECT COUNT(*)
		FROM mysql.user_security_labels
		WHERE label_name = %?
	`

	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.LabelName.L)
	if err != nil {
		return errors.Wrap(err, "failed to check label usage")
	}

	if len(rows) > 0 && rows[0].GetInt64(0) > 0 {
		return lbac.LabelError(lbac.ErrLabelInUse, stmt.LabelName.O)
	}

	infoSchema := domain.GetDomain(sctx).InfoSchema()
	_, _, _, found, err := findTableUsingLabel(infoSchema, stmt.LabelName.L)
	if err != nil {
		return errors.Wrap(err, "failed to check label column usage")
	}
	if found {
		return lbac.LabelError(lbac.ErrLabelInUseByColumns, stmt.LabelName.O)
	}

	// Delete label
	sql = `
		DELETE FROM mysql.security_labels
		WHERE name = %? AND policy_name = %?
	`

	_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql, stmt.LabelName.L, stmt.PolicyName.L)
	if err != nil {
		return errors.Wrap(err, "failed to drop security label")
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func requireUserExists(ctx context.Context, sctx sessionctx.Context, op string, userSpec *ast.UserSpec) (lbac.UserHost, error) {
	userName := userSpec.User.Username
	host := userSpec.User.Hostname
	if host == "" {
		host = "%"
	}
	exists, err := userExists(ctx, sctx, userName, host)
	if err != nil {
		return lbac.UserHost{}, err
	}
	if !exists {
		return lbac.UserHost{}, exeerrors.ErrCannotUser.GenWithStackByArgs(op, userSpec.User.String())
	}
	return lbac.UserHost{
		User: userName,
		Host: host,
	}, nil
}

func executeGrantSecurityLabel(goCtx context.Context, sctx sessionctx.Context, stmt *ast.GrantSecurityLabelStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	if err := ddl.EnsureLabelExists(goCtx, exec, stmt.PolicyName.L, stmt.LabelName.L); err != nil {
		return lbac.LabelError(err, stmt.LabelName.O)
	}

	accessType := mergeAccessType(stmt.AccessTypes...)
	if accessType == ast.SecurityLabelAccessTypeNone {
		return lbac.UserError(lbac.ErrInvalidAccessType, "", "")
	}

	users := make([]lbac.UserHost, 0, len(stmt.Users))
	for _, userSpec := range stmt.Users {
		u, err := requireUserExists(goCtx, sctx, "GRANT SECURITY LABEL", userSpec)
		if err != nil {
			return err
		}
		users = append(users, u)
	}
	for _, user := range users {
		userName := user.User
		host := user.Host

		existingAccessType, err := getExistingAccessType(goCtx, exec, userName, host, stmt.LabelName.L)
		if err != nil {
			return errors.Wrap(err, "failed to load existing security label grants")
		}

		if existingAccessType != ast.SecurityLabelAccessTypeNone {
			mergedType := mergeAccessType(existingAccessType, accessType)
			updateSQL := `
				UPDATE mysql.user_security_labels
				SET access_types = %?
				WHERE user_name = %? AND host = %? AND label_name = %?
			`
			_, _, err = exec.ExecRestrictedSQL(goCtx, nil, updateSQL,
				mergedType.String(),
				userName,
				host,
				stmt.LabelName.L,
			)
			if err != nil {
				return errors.Wrap(err, "failed to update security label grant")
			}
			continue
		}

		sql := `
			INSERT INTO mysql.user_security_labels
			(user_name, host, label_name, access_types)
			VALUES (%?, %?, %?, %?)
		`

		_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
			userName,
			host,
			stmt.LabelName.L,
			accessType.String(),
		)
		if err != nil {
			if isDuplicateError(err) {
				return lbac.UserError(lbac.ErrUserLabelAlreadyExists, userName, host)
			}
			return errors.Wrap(err, "failed to grant security label")
		}
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeRevokeSecurityLabel(goCtx context.Context, sctx sessionctx.Context, stmt *ast.RevokeSecurityLabelStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	sql := `
		DELETE FROM mysql.user_security_labels
		WHERE user_name = %? AND host = %? AND label_name = %?
	`
	for _, userSpec := range stmt.Users {
		user, err := requireUserExists(goCtx, sctx, "REVOKE SECURITY LABEL", userSpec)
		if err != nil {
			return err
		}

		_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
			user.User,
			user.Host,
			stmt.LabelName.L,
		)
		if err != nil {
			return errors.Wrap(err, "failed to revoke security label")
		}
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeGrantExemption(goCtx context.Context, sctx sessionctx.Context, stmt *ast.GrantExemptionStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	// Get current user from session context
	policyName := stmt.PolicyName.L
	ruleName := stmt.Rule.StoreValue()
	if err := ddl.EnsurePolicyExists(goCtx, exec, policyName); err != nil {
		return lbac.PolicyError(err, stmt.PolicyName.O)
	}

	for _, userSpec := range stmt.Users {
		user, err := requireUserExists(goCtx, sctx, "GRANT EXEMPTION", userSpec)
		if err != nil {
			return err
		}
		// Insert user exemption
		sql := `INSERT INTO mysql.user_exemptions (user_name, host, policy_name, rule) VALUES (%?, %?, %?, %?)`
		_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
			user.User,
			user.Host,
			policyName,
			ruleName,
		)

		if err != nil {
			if isDuplicateError(err) {
				return lbac.UserError(lbac.ErrUserExemptionAlreadyExists, user.User, user.Host)
			}
			return errors.Wrap(err, "failed to grant exemption")
		}
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

func executeRevokeExemption(goCtx context.Context, sctx sessionctx.Context, stmt *ast.RevokeExemptionStmt) error {
	// Get executor from context
	exec := sctx.GetRestrictedSQLExecutor()
	goCtx = kv.WithInternalSourceType(goCtx, kv.InternalTxnLabelSecurity)

	policyName := stmt.PolicyName.L
	ruleName := stmt.Rule.StoreValue()
	if err := ddl.EnsurePolicyExists(goCtx, exec, policyName); err != nil {
		return lbac.PolicyError(err, stmt.PolicyName.O)
	}
	for _, userSpec := range stmt.Users {
		user, err := requireUserExists(goCtx, sctx, "REVOKE EXEMPTION", userSpec)
		if err != nil {
			return err
		}
		// Delete user exemption
		sql := `DELETE FROM mysql.user_exemptions WHERE user_name = %? AND host = %? AND policy_name = %? AND rule = %?`
		_, _, err = exec.ExecRestrictedSQL(goCtx, nil, sql,
			user.User,
			user.Host,
			policyName,
			ruleName,
		)

		if err != nil {
			return errors.Wrap(err, "failed to revoke exemption")
		}
	}

	return domain.GetDomain(sctx).NotifyUpdatePrivilege()
}

// Helper functions for error checking

func isDuplicateError(err error) bool {
	if err == nil {
		return false
	}
	return kv.ErrKeyExists.Equal(err)
}

type componentDef struct {
	Type       ast.LBACComponentType
	Values     []string
	TreeParent map[string]string
}

func (d *componentDef) validateValue(value string) error {
	switch d.Type {
	case ast.LBACComponentTypeArray, ast.LBACComponentTypeSet:
		for _, elem := range d.Values {
			if elem == value {
				return nil
			}
		}
		return lbac.ErrInvalidComponentValue
	case ast.LBACComponentTypeTree:
		if d.TreeParent != nil {
			if _, ok := d.TreeParent[value]; ok {
				return nil
			}
		}
		for _, elem := range d.Values {
			if elem == value {
				return nil
			}
		}
		return lbac.ErrInvalidComponentValue
	default:
		return lbac.ErrInvalidComponentType
	}
}

func validateUniqueStrings(values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return lbac.ErrInvalidComponentValue
		}
		if _, exists := seen[value]; exists {
			return lbac.ErrDuplicateComponentValue
		}
		seen[value] = struct{}{}
	}
	return nil
}

func flattenTreeNodes(nodes []*ast.LBACTreeNode) ([]lbac.TreeNodeSpec, error) {
	seen := make(map[string]struct{}, len(nodes))
	parentSet := make(map[string]struct{}, len(nodes))
	var result []lbac.TreeNodeSpec
	rootCount := 0
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if node.Name == "" {
			return nil, lbac.ErrInvalidComponentValue
		}
		if _, exists := seen[node.Name]; exists {
			return nil, lbac.ErrDuplicateComponentValue
		}
		seen[node.Name] = struct{}{}
		parent := strings.TrimSpace(node.Parent)
		if node.IsRoot || parent == "" {
			rootCount++
			parent = ""
		} else {
			parentSet[parent] = struct{}{}
		}
		result = append(result, lbac.TreeNodeSpec{Name: node.Name, Parent: parent})
	}
	if rootCount == 0 || rootCount > 1 {
		return nil, lbac.ErrInvalidComponentValue
	}
	for parent := range parentSet {
		if _, exists := seen[parent]; !exists {
			return nil, lbac.ErrInvalidComponentValue
		}
	}
	return result, nil
}

func ensureComponentsExist(goCtx context.Context, exec sqlexec.RestrictedSQLExecutor, componentNames []string) error {
	placeholders, args := buildPlaceholders(componentNames)
	sql := fmt.Sprintf(`SELECT name FROM mysql.security_label_components WHERE name IN (%s)`, placeholders)
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, args...)
	if err != nil {
		return err
	}
	found := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		found[row.GetString(0)] = struct{}{}
	}
	for _, name := range componentNames {
		if _, ok := found[name]; !ok {
			return lbac.ComponentError(lbac.ErrComponentNotFound, name)
		}
	}
	return nil
}

func loadPolicyComponents(goCtx context.Context, exec sqlexec.RestrictedSQLExecutor, policyName string) ([]string, error) {
	sql := `SELECT component_names FROM mysql.security_policies WHERE name = %?`
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, policyName)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, lbac.ErrPolicyNotFound
	}
	var componentNames []string
	componentsJSON, err := rows[0].GetJSON(0).MarshalJSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to read policy components")
	}
	if err := json.Unmarshal(componentsJSON, &componentNames); err != nil {
		return nil, errors.Wrap(err, "failed to parse policy components")
	}
	if len(componentNames) == 0 {
		return nil, lbac.ErrEmptyPolicyComponents
	}
	return componentNames, nil
}

func loadComponentDefs(goCtx context.Context, exec sqlexec.RestrictedSQLExecutor, componentNames []string) (map[string]*componentDef, error) {
	placeholders, args := buildPlaceholders(componentNames)
	sql := fmt.Sprintf("SELECT name, type, component_values FROM mysql.security_label_components WHERE name IN (%s)", placeholders)
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, args...)
	if err != nil {
		return nil, err
	}
	defs := make(map[string]*componentDef, len(rows))
	for _, row := range rows {
		name := row.GetString(0)
		typeName := row.GetEnum(1).String()
		componentType, ok := ast.ParseLBACComponentType(typeName)
		if !ok {
			return nil, lbac.ErrInvalidComponentType
		}
		def := &componentDef{Type: componentType}
		valuesJSON, err := row.GetJSON(2).MarshalJSON()
		if err != nil {
			return nil, errors.Wrap(err, "failed to read component values")
		}
		values, treeParent, err := lbac.ParseComponentValues(componentType, valuesJSON)
		if err != nil {
			return nil, err
		}
		def.Values = values
		def.TreeParent = treeParent
		defs[name] = def
	}
	for _, name := range componentNames {
		if _, ok := defs[name]; !ok {
			return nil, lbac.ErrComponentNotFound
		}
	}
	return defs, nil
}

func findTableUsingPolicy(is infoschema.InfoSchema, policyName string) (string, string, bool, error) {
	if is == nil || policyName == "" {
		return "", "", false, nil
	}
	ctx := context.Background()
	for _, schemaName := range is.AllSchemaNames() {
		tables, err := is.SchemaTableInfos(ctx, schemaName)
		if err != nil {
			return "", "", false, err
		}
		for _, tableInfo := range tables {
			if tableInfo == nil || tableInfo.SecurityPolicy == nil {
				continue
			}
			if tableInfo.SecurityPolicy.L == policyName {
				return schemaName.O, tableInfo.Name.O, true, nil
			}
		}
	}
	return "", "", false, nil
}

func findTableUsingLabel(is infoschema.InfoSchema, labelName string) (string, string, string, bool, error) {
	if is == nil || labelName == "" {
		return "", "", "", false, nil
	}
	ctx := context.Background()
	for _, schemaName := range is.AllSchemaNames() {
		tables, err := is.SchemaTableInfos(ctx, schemaName)
		if err != nil {
			return "", "", "", false, err
		}
		for _, tableInfo := range tables {
			if tableInfo == nil {
				continue
			}
			for _, col := range tableInfo.Columns {
				if col == nil || col.SecurityLabel == nil {
					continue
				}
				if col.SecurityLabel.L == labelName {
					return schemaName.O, tableInfo.Name.O, col.Name.O, true, nil
				}
			}
		}
	}
	return "", "", "", false, nil
}

func buildPlaceholders(values []string) (string, []any) {
	placeholders := make([]string, 0, len(values))
	args := make([]any, 0, len(values))
	for _, value := range values {
		placeholders = append(placeholders, "%?")
		args = append(args, value)
	}
	return strings.Join(placeholders, ","), args
}

func getExistingAccessType(goCtx context.Context, exec sqlexec.RestrictedSQLExecutor, userName, host, labelName string) (ast.SecurityLabelAccessType, error) {
	sql := `SELECT access_types FROM mysql.user_security_labels WHERE user_name = %? AND host = %? AND label_name = %?`
	rows, _, err := exec.ExecRestrictedSQL(goCtx, nil, sql, userName, host, labelName)
	if err != nil || len(rows) == 0 {
		return ast.SecurityLabelAccessTypeNone, err
	}
	accessType, ok := ast.ParseSecurityLabelAccessType(rows[0].GetEnum(0).String())
	if !ok {
		return ast.SecurityLabelAccessTypeNone, lbac.ErrInvalidAccessType
	}
	return accessType, nil
}

func mergeAccessType(accessTypes ...ast.SecurityLabelAccessType) ast.SecurityLabelAccessType {
	merged := ast.SecurityLabelAccessTypeNone
	for _, accessType := range accessTypes {
		merged |= accessType
	}
	return merged
}
