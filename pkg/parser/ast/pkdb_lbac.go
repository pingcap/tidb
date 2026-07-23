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

package ast

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// CreateSecurityLabelComponentStmt represents CREATE SECURITY LABEL COMPONENT statement
type CreateSecurityLabelComponentStmt struct {
	ddlNode

	Name model.CIStr
	LBACComponentDef
}

// LBACComponentDef is a helper structure for LBAC component definitions
type LBACComponentDef struct {
	Type     LBACComponentType
	Elements []string
	Tree     *LBACTreeDef
}

type LBACComponentType byte

const (
	LBACComponentTypeUnknown LBACComponentType = iota
	LBACComponentTypeArray
	LBACComponentTypeSet
	LBACComponentTypeTree
)

func (t LBACComponentType) String() string {
	switch t {
	case LBACComponentTypeArray:
		return "ARRAY"
	case LBACComponentTypeSet:
		return "SET"
	case LBACComponentTypeTree:
		return "TREE"
	default:
		return ""
	}
}

// ParseLBACComponentType parses ARRAY/SET/TREE (case-insensitive) into LBACComponentType.
func ParseLBACComponentType(value string) (LBACComponentType, bool) {
	switch strings.ToUpper(value) {
	case "ARRAY":
		return LBACComponentTypeArray, true
	case "SET":
		return LBACComponentTypeSet, true
	case "TREE":
		return LBACComponentTypeTree, true
	default:
		return LBACComponentTypeUnknown, false
	}
}

// Restore implements Node interface.
func (n *CreateSecurityLabelComponentStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE SECURITY LABEL COMPONENT ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" ")
	ctx.WriteKeyWord(n.Type.String())

	if n.Type == LBACComponentTypeArray || n.Type == LBACComponentTypeSet {
		ctx.WritePlain(" (")
		for i, elem := range n.Elements {
			if i > 0 {
				ctx.WritePlain(",")
			}
			ctx.WriteString(elem)
		}
		ctx.WritePlain(")")
	} else if n.Type == LBACComponentTypeTree && n.Tree != nil {
		ctx.WritePlain(" (")
		if err := n.Tree.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while restore CreateSecurityLabelComponentStmt.Tree")
		}
		ctx.WritePlain(")")
	}
	return nil
}

func (n *CreateSecurityLabelComponentStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSecurityLabelComponentStmt)
	return v.Leave(n)
}

// LBACTreeDef represents a tree structure for TREE components
type LBACTreeDef struct {
	Nodes []*LBACTreeNode
}

// Restore formats the tree definition for SQL generation
func (n *LBACTreeDef) Restore(ctx *format.RestoreCtx) error {
	for i, node := range n.Nodes {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		if err := node.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while restore LBACTreeDef.Nodes[%d]", i)
		}
	}
	return nil
}

// LBACTreeNode represents a node in the tree structure
type LBACTreeNode struct {
	Name     string
	Parent   string // Empty for root
	IsRoot   bool
	Children []*LBACTreeNode // Parsed after the tree is built
}

// Restore formats the tree node for SQL generation
func (n *LBACTreeNode) Restore(ctx *format.RestoreCtx) error {
	if n.IsRoot {
		ctx.WriteString(n.Name)
		ctx.WritePlain(" ROOT")
	} else {
		ctx.WriteString(n.Name)
		ctx.WritePlain(" UNDER ")
		ctx.WriteString(n.Parent)
	}
	return nil
}

// DropSecurityLabelComponentStmt represents DROP SECURITY LABEL COMPONENT statement
type DropSecurityLabelComponentStmt struct {
	ddlNode

	Name model.CIStr
}

// Restore implements Node interface.
func (n *DropSecurityLabelComponentStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SECURITY LABEL COMPONENT ")
	ctx.WriteName(n.Name.O)
	return nil
}

// CreateSecurityPolicyStmt represents CREATE SECURITY POLICY statement
type CreateSecurityPolicyStmt struct {
	ddlNode

	Name       model.CIStr
	Components []model.CIStr
	Rule       SecurityPolicyRuleOption
	Option     SecurityPolicyOption
}

type SecurityPolicyRuleOption int

const (
	SecurityPolicyRuleNone SecurityPolicyRuleOption = iota
	SecurityPolicyRuleLBACRules
)

type SecurityPolicyOption int

const (
	SecurityPolicyNone SecurityPolicyOption = iota
	SecurityPolicyOverride
	SecurityPolicyRestrict
)

func (n SecurityPolicyOption) String() string {
	switch n {
	case SecurityPolicyOverride:
		return "OVERRIDE"
	case SecurityPolicyRestrict:
		return "RESTRICT"
	default:
		return "RESTRICT"
	}
}

// ParseSecurityPolicyOption parses OVERRIDE/RESTRICT (case-insensitive).
func ParseSecurityPolicyOption(value string) SecurityPolicyOption {
	switch strings.ToUpper(value) {
	case "OVERRIDE":
		return SecurityPolicyOverride
	case "RESTRICT":
		return SecurityPolicyRestrict
	default:
		return SecurityPolicyRestrict
	}
}

// Restore implements Node interface.
func (n *CreateSecurityPolicyStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE SECURITY POLICY ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" COMPONENTS ")
	for i, comp := range n.Components {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(comp.O)
	}
	switch n.Rule {
	case SecurityPolicyRuleNone:
		// do nothing
	case SecurityPolicyRuleLBACRules:
		ctx.WriteKeyWord(" WITH LBACRULES")
	}
	switch n.Option {
	case SecurityPolicyNone:
		// do nothing
	case SecurityPolicyOverride:
		ctx.WriteKeyWord(" OVERRIDE NOT AUTHORIZED WRITE SECURITY LABEL")
	case SecurityPolicyRestrict:
		ctx.WriteKeyWord(" RESTRICT NOT AUTHORIZED WRITE SECURITY LABEL")
	}
	return nil
}

// DropSecurityPolicyStmt represents DROP SECURITY POLICY statement
type DropSecurityPolicyStmt struct {
	ddlNode

	Name model.CIStr
}

// Restore implements Node interface.
func (n *DropSecurityPolicyStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SECURITY POLICY ")
	ctx.WriteName(n.Name.O)
	return nil
}

// CreateSecurityLabelStmt represents CREATE SECURITY LABEL statement
type CreateSecurityLabelStmt struct {
	ddlNode

	PolicyName model.CIStr
	LabelName  model.CIStr
	Components []*SecurityLabelComponentSpec
}

// Restore implements Node interface.
func (n *CreateSecurityLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE SECURITY LABEL ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WritePlain(".")
	ctx.WriteName(n.LabelName.O)
	for _, comp := range n.Components {
		ctx.WriteKeyWord(" COMPONENT ")
		ctx.WriteName(comp.ComponentName.O)
		for i, value := range comp.Values {
			if i == 0 {
				ctx.WritePlain(" '")
			} else {
				ctx.WritePlain(", '")
			}
			// Escape single quotes in the value
			escapedValue := strings.ReplaceAll(value, "'", "''")
			ctx.WritePlain(escapedValue)
			ctx.WritePlain("'")
		}
	}
	return nil
}

// SecurityLabelComponentSpec represents a component specification in security label
type SecurityLabelComponentSpec struct {
	ComponentName model.CIStr
	Values        []string
}

// DropSecurityLabelStmt represents DROP SECURITY LABEL statement
type DropSecurityLabelStmt struct {
	ddlNode

	PolicyName model.CIStr
	LabelName  model.CIStr
}

// Restore implements Node interface.
func (n *DropSecurityLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SECURITY LABEL ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WritePlain(".")
	ctx.WriteName(n.LabelName.O)
	return nil
}

// GrantSecurityLabelStmt represents GRANT SECURITY LABEL statement
type GrantSecurityLabelStmt struct {
	ddlNode

	PolicyName  model.CIStr
	LabelName   model.CIStr
	Users       []*UserSpec
	AccessTypes []SecurityLabelAccessType
}

type SecurityLabelAccessType byte

const (
	SecurityLabelAccessTypeNone SecurityLabelAccessType = 0
	SecurityLabelAccessTypeRead SecurityLabelAccessType = 1 << iota
	SecurityLabelAccessTypeWrite
	SecurityLabelAccessTypeAll = SecurityLabelAccessTypeRead | SecurityLabelAccessTypeWrite
)

func (t SecurityLabelAccessType) String() string {
	switch t {
	case SecurityLabelAccessTypeRead:
		return "READ"
	case SecurityLabelAccessTypeWrite:
		return "WRITE"
	case SecurityLabelAccessTypeAll:
		return "ALL"
	default:
		return ""
	}
}

// ParseSecurityLabelAccessType parses READ/WRITE/ALL (case-insensitive).
func ParseSecurityLabelAccessType(value string) (SecurityLabelAccessType, bool) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	switch normalized {
	case "READ":
		return SecurityLabelAccessTypeRead, true
	case "WRITE":
		return SecurityLabelAccessTypeWrite, true
	case "ALL":
		return SecurityLabelAccessTypeAll, true
	default:
		return SecurityLabelAccessTypeNone, false
	}
}

// Restore implements Node interface.
func (n *GrantSecurityLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT SECURITY LABEL ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WritePlain(".")
	ctx.WriteName(n.LabelName.O)
	ctx.WriteKeyWord(" TO ")
	for i, user := range n.Users {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord("USER ")
		if user != nil && user.User != nil {
			if err := user.User.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantSecurityLabelStmt.Users[%d]", i)
			}
		}
	}
	ctx.WriteKeyWord(" FOR ")
	for i, accessType := range n.AccessTypes {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		switch accessType {
		case SecurityLabelAccessTypeRead:
			ctx.WriteKeyWord("READ ACCESS")
		case SecurityLabelAccessTypeWrite:
			ctx.WriteKeyWord("WRITE ACCESS")
		case SecurityLabelAccessTypeAll:
			ctx.WriteKeyWord("ALL ACCESS")
		}
	}
	return nil
}

// GrantExemptionStmt represents GRANT EXEMPTION statement
type GrantExemptionStmt struct {
	ddlNode

	Rule       ExemptionRule
	PolicyName model.CIStr
	Users      []*UserSpec
}

type ExemptionRule uint8

const (
	ExemptionRuleAll ExemptionRule = iota
	ExemptionRuleInvalid
)

func (r ExemptionRule) String() string {
	switch r {
	case ExemptionRuleAll:
		return "ALL"
	default:
		return ""
	}
}

func (r ExemptionRule) StoreValue() string {
	switch r {
	case ExemptionRuleAll:
		return "all"
	default:
		return ""
	}
}

// Restore implements Node interface.
func (n *GrantExemptionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT EXEMPTION ON RULE ")
	ctx.WriteKeyWord(n.Rule.String())
	ctx.WriteKeyWord(" FOR ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WriteKeyWord(" TO ")
	for i, user := range n.Users {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord("USER ")
		if user != nil && user.User != nil {
			if err := user.User.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore GrantExemptionStmt.Users[%d]", i)
			}
		}
	}
	return nil
}

// Accept implements Node interface.
func (n *GrantExemptionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantExemptionStmt)
	return v.Leave(n)
}

// RevokeExemptionStmt represents REVOKE EXEMPTION statement
type RevokeExemptionStmt struct {
	ddlNode

	Rule       ExemptionRule
	PolicyName model.CIStr
	Users      []*UserSpec
}

// Restore implements Node interface.
func (n *RevokeExemptionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE EXEMPTION ON RULE ")
	ctx.WriteKeyWord(n.Rule.String())
	ctx.WriteKeyWord(" FOR ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WriteKeyWord(" FROM ")
	for i, user := range n.Users {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord("USER ")
		if user != nil && user.User != nil {
			if err := user.User.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore RevokeExemptionStmt.Users[%d]", i)
			}
		}
	}
	return nil
}

// Accept implements Node interface.
func (n *RevokeExemptionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeExemptionStmt)
	return v.Leave(n)
}

// RevokeSecurityLabelStmt represents REVOKE SECURITY LABEL statement
type RevokeSecurityLabelStmt struct {
	ddlNode

	PolicyName model.CIStr
	LabelName  model.CIStr
	Users      []*UserSpec
}

// Restore implements Node interface.
func (n *RevokeSecurityLabelStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE SECURITY LABEL ")
	ctx.WriteName(n.PolicyName.O)
	ctx.WritePlain(".")
	ctx.WriteName(n.LabelName.O)
	ctx.WriteKeyWord(" FROM ")
	for i, user := range n.Users {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord("USER ")
		if user != nil && user.User != nil {
			if err := user.User.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while restore RevokeSecurityLabelStmt.Users[%d]", i)
			}
		}
	}
	return nil
}

// Accept implements Node interface.
func (n *DropSecurityLabelComponentStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSecurityLabelComponentStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *CreateSecurityPolicyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSecurityPolicyStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *DropSecurityPolicyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSecurityPolicyStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *CreateSecurityLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateSecurityLabelStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *DropSecurityLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropSecurityLabelStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *GrantSecurityLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantSecurityLabelStmt)
	return v.Leave(n)
}

// Accept implements Node interface.
func (n *RevokeSecurityLabelStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeSecurityLabelStmt)
	return v.Leave(n)
}
