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

package privileges

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/privilege/lbac"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	sqlLoadSecurityLabelComponents = "SELECT name, type, component_values FROM mysql.security_label_components"
	sqlLoadSecurityPolicies        = "SELECT name, component_names, write_control FROM mysql.security_policies"
	sqlLoadSecurityLabels          = "SELECT name, policy_name, components FROM mysql.security_labels"
	sqlLoadUserSecurityLabels      = "SELECT user_name, host, label_name, access_types FROM mysql.user_security_labels"
	sqlLoadUserExemptions          = "SELECT user_name, host, policy_name, rule FROM mysql.user_exemptions"
)

// LoadLBAC loads LBAC-related privilege tables into memory.
func (p *MySQLPrivilege) LoadLBAC(ctx sqlexec.RestrictedSQLExecutor) error {
	if !variable.EnableLBAC.Load() {
		return nil
	}
	if err := p.loadTable(ctx, sqlLoadSecurityLabelComponents, p.decodeLBACComponentRow); err != nil {
		return errors.Trace(err)
	}
	if err := p.loadTable(ctx, sqlLoadSecurityPolicies, p.decodeLBACPolicyRow); err != nil {
		return errors.Trace(err)
	}
	if err := p.loadTable(ctx, sqlLoadSecurityLabels, p.decodeLBACLabelRow); err != nil {
		return errors.Trace(err)
	}
	if err := p.loadTable(ctx, sqlLoadUserSecurityLabels, p.decodeLBACUserLabelRow); err != nil {
		return errors.Trace(err)
	}
	if err := p.loadTable(ctx, sqlLoadUserExemptions, p.decodeLBACUserExemptionRow); err != nil {
		return errors.Trace(err)
	}
	p.buildLBACCache()
	return nil
}

func (p *immutable) decodeLBACComponentRow(row chunk.Row, fs []*resolve.ResultField) error {
	var value lbac.Component
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "name":
			value.Name = row.GetString(i)
		case "type":
			componentType, ok := ast.ParseLBACComponentType(row.GetEnum(i).String())
			if !ok {
				return lbac.ErrInvalidComponentType
			}
			value.Type = componentType
		case "component_values":
			valuesJSON, err := row.GetJSON(i).MarshalJSON()
			if err != nil {
				return errors.Wrap(err, "failed to read component values")
			}
			values, treeParent, err := lbac.ParseComponentValues(value.Type, valuesJSON)
			if err != nil {
				return err
			}
			value.Values = values
			value.TreeParent = treeParent
		}
	}
	p.lbacComponents = append(p.lbacComponents, value)
	return nil
}

func (p *immutable) decodeLBACPolicyRow(row chunk.Row, fs []*resolve.ResultField) error {
	var value lbac.Policy
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "name":
			value.Name = row.GetString(i)
		case "component_names":
			componentsJSON, err := row.GetJSON(i).MarshalJSON()
			if err != nil {
				return errors.Wrap(err, "failed to read policy components")
			}
			if err := json.Unmarshal(componentsJSON, &value.ComponentNames); err != nil {
				return errors.Wrap(err, "failed to parse policy components")
			}
		case "write_control":
			v := ast.ParseSecurityPolicyOption(row.GetEnum(i).String())
			value.Option = v
		}
	}
	p.lbacPolicies = append(p.lbacPolicies, value)
	return nil
}

func (p *immutable) decodeLBACLabelRow(row chunk.Row, fs []*resolve.ResultField) error {
	var value lbac.Label
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "name":
			value.Name = row.GetString(i)
		case "policy_name":
			value.PolicyName = row.GetString(i)
		case "components":
			componentsJSON, err := row.GetJSON(i).MarshalJSON()
			if err != nil {
				return errors.Wrap(err, "failed to read label components")
			}
			components, err := lbac.ParseLabelComponents(componentsJSON)
			if err != nil {
				return err
			}
			value.Components = components
		}
	}
	p.lbacLabels = append(p.lbacLabels, value)
	return nil
}

func (p *immutable) decodeLBACUserLabelRow(row chunk.Row, fs []*resolve.ResultField) error {
	var value lbac.UserLabel
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "user_name":
			value.UserName = row.GetString(i)
		case "host":
			value.Host = row.GetString(i)
		case "label_name":
			value.LabelName = row.GetString(i)
		case "access_types":
			accessType, ok := ast.ParseSecurityLabelAccessType(row.GetEnum(i).String())
			if !ok {
				return lbac.ErrInvalidAccessType
			}
			value.AccessType = accessType
		}
	}
	p.lbacUserLabels = append(p.lbacUserLabels, value)
	return nil
}

func (p *immutable) decodeLBACUserExemptionRow(row chunk.Row, fs []*resolve.ResultField) error {
	var value lbac.UserExemption
	for i, f := range fs {
		switch f.ColumnAsName.L {
		case "user_name":
			value.UserName = row.GetString(i)
		case "host":
			value.Host = row.GetString(i)
		case "policy_name":
			value.PolicyName = row.GetString(i)
		case "rule":
			value.Rule = strings.ToUpper(row.GetString(i))
		}
	}
	p.lbacUserExemptions = append(p.lbacUserExemptions, value)
	return nil
}

func (p *MySQLPrivilege) buildLBACCache() {
	p.lbac = lbac.BuildCache(
		p.lbacComponents,
		p.lbacPolicies,
		p.lbacLabels,
		p.lbacUserLabels,
		p.lbacUserExemptions,
	)
}

func compareLBACComponent(x, y lbac.Component) int {
	return strings.Compare(x.Name, y.Name)
}

func compareLBACPolicy(x, y lbac.Policy) int {
	return strings.Compare(x.Name, y.Name)
}

func compareLBACLabel(x, y lbac.Label) int {
	return strings.Compare(x.Name, y.Name)
}

func compareLBACUserLabel(x, y lbac.UserLabel) int {
	if x.UserName != y.UserName {
		return strings.Compare(x.UserName, y.UserName)
	}
	if x.Host != y.Host {
		return strings.Compare(x.Host, y.Host)
	}
	return strings.Compare(x.LabelName, y.LabelName)
}

func compareLBACUserExemption(x, y lbac.UserExemption) int {
	if x.UserName != y.UserName {
		return strings.Compare(x.UserName, y.UserName)
	}
	if x.Host != y.Host {
		return strings.Compare(x.Host, y.Host)
	}
	if x.PolicyName != y.PolicyName {
		return strings.Compare(x.PolicyName, y.PolicyName)
	}
	return strings.Compare(x.Rule, y.Rule)
}
