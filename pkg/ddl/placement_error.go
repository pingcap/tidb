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
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	pd "github.com/tikv/pd/client/http"
)

var placementRuleErrorRE = regexp.MustCompile(`rule '([^']+)' from rule group '([^']+)'`)

type placementRuleErrorContext struct {
	SchemaName string
	TableName  string
	PolicyName string
	Bundles    []*placement.Bundle
}

func placementRuleErrorWithContext(err error, ctx placementRuleErrorContext) error {
	if !infosync.ErrHTTPServiceError.Equal(err) {
		return err
	}

	originalMsg := strings.TrimSpace(placementHTTPServiceErrorMessage(err))
	ruleID, ruleGroupID, ok := extractPlacementRuleError(originalMsg)
	if !ok {
		return err
	}

	enhancedMsg := originalMsg + "; " + formatPlacementRuleErrorContext(ctx, ruleGroupID, ruleID)
	if strings.Contains(originalMsg, "can not match any store") {
		enhancedMsg += ". No store matches the failed rule constraints; check available store labels with SHOW PLACEMENT LABELS."
	}
	return infosync.ErrHTTPServiceError.FastGen("%s", enhancedMsg)
}

func placementHTTPServiceErrorMessage(err error) string {
	if terr, ok := errors.Cause(err).(*errors.Error); ok {
		return terr.GetMsg()
	}
	return err.Error()
}

func extractPlacementRuleError(msg string) (ruleID, ruleGroupID string, ok bool) {
	matches := placementRuleErrorRE.FindStringSubmatch(msg)
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func formatPlacementRuleErrorContext(ctx placementRuleErrorContext, ruleGroupID, ruleID string) string {
	parts := []string{
		fmt.Sprintf("table=%s", formatPlacementTableName(ctx.SchemaName, ctx.TableName)),
		fmt.Sprintf("policy=%s", formatPlacementIdentifier(ctx.PolicyName)),
		fmt.Sprintf("rule_group=%s", formatPlacementIdentifier(ruleGroupID)),
		fmt.Sprintf("rule=%s", formatPlacementIdentifier(ruleID)),
	}

	if rule := findPlacementRule(ctx.Bundles, ruleGroupID, ruleID); rule != nil {
		parts = append(parts,
			fmt.Sprintf("role=%s", rule.Role),
			fmt.Sprintf("count=%d", rule.Count),
			fmt.Sprintf("label_constraints=%s", formatPlacementLabelConstraints(rule.LabelConstraints)),
		)
		return "TiDB placement context: " + strings.Join(parts, ", ")
	}

	return "TiDB placement context: " + strings.Join(parts, ", ") + " (rule detail not found in current TiDB bundle)"
}

func findPlacementRule(bundles []*placement.Bundle, ruleGroupID, ruleID string) *pd.Rule {
	for _, bundle := range bundles {
		if bundle == nil {
			continue
		}
		for _, rule := range bundle.Rules {
			if rule == nil {
				continue
			}
			if rule.GroupID == ruleGroupID && rule.ID == ruleID {
				return rule
			}
		}
	}
	return nil
}

func formatPlacementTableName(schemaName, tableName string) string {
	if tableName == "" {
		return "<unknown>"
	}
	if schemaName == "" {
		return formatPlacementIdentifier(tableName)
	}
	return formatPlacementIdentifier(schemaName) + "." + formatPlacementIdentifier(tableName)
}

func formatPlacementIdentifier(identifier string) string {
	if identifier == "" {
		return "<unknown>"
	}
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func formatPlacementLabelConstraints(labelConstraints []pd.LabelConstraint) string {
	if len(labelConstraints) == 0 {
		return "[]"
	}
	constraints := make([]string, 0, len(labelConstraints))
	for _, constraint := range labelConstraints {
		constraints = append(constraints, formatPlacementLabelConstraint(constraint))
	}
	return "[" + strings.Join(constraints, ", ") + "]"
}

func formatPlacementLabelConstraint(constraint pd.LabelConstraint) string {
	switch constraint.Op {
	case pd.In:
		return fmt.Sprintf("%s in (%s)", constraint.Key, strings.Join(constraint.Values, ","))
	case pd.NotIn:
		return fmt.Sprintf("%s notIn (%s)", constraint.Key, strings.Join(constraint.Values, ","))
	case pd.Exists, pd.NotExists:
		return fmt.Sprintf("%s %s", constraint.Key, constraint.Op)
	default:
		if len(constraint.Values) == 0 {
			return fmt.Sprintf("%s %s", constraint.Key, constraint.Op)
		}
		return fmt.Sprintf("%s %s (%s)", constraint.Key, constraint.Op, strings.Join(constraint.Values, ","))
	}
}

func placementPolicyRefName(policyRefInfo *model.PolicyRefInfo) string {
	if policyRefInfo == nil {
		return ""
	}
	if policyRefInfo.Name.O != "" {
		return policyRefInfo.Name.O
	}
	return policyRefInfo.Name.L
}
