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

package util

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// OwnerResolutionKind describes whether an operand has one provable identity
// in the join group's owner query block.
type OwnerResolutionKind uint8

const (
	// OwnerAbsent means the rewritten operand exposes no derived-alias
	// candidate and does not itself belong to the owner query block.
	OwnerAbsent OwnerResolutionKind = iota
	// OwnerUnique means exactly one identity is visible in the owner query block.
	OwnerUnique
	// OwnerAmbiguous means multiple identities are visible in the owner query block.
	OwnerAmbiguous
	// OwnerBrokenAliasChain means a recorded derived alias cannot be resolved to
	// the owner query block.
	OwnerBrokenAliasChain
)

// OwnerResolution is the strict owner-visible identity result. Identity is
// non-nil only for OwnerUnique.
type OwnerResolution struct {
	Kind     OwnerResolutionKind
	Identity *h.HintedTable
}

// IdentityOccurrenceKind describes why an identity was observed in a
// rewritten operand.
type IdentityOccurrenceKind uint8

const (
	// ConcreteOccurrence is a real base/output identity that legacy LEADING
	// syntax may select but may not use to preserve a derived boundary.
	ConcreteOccurrence IdentityOccurrenceKind = iota
	// AliasCandidateOccurrence marks a plan node whose query block has recorded
	// derived-alias metadata. LEADING policy may project it to a qualified QB.
	AliasCandidateOccurrence
)

// IdentityOccurrence retains source query-block and plan-node provenance so
// policy code cannot collapse self-join occurrences by name.
type IdentityOccurrence struct {
	Identity h.HintedTable
	StartQB  int
	NodeID   int
	Kind     IdentityOccurrenceKind
}

// OperandIdentityFacts contains policy-free facts observed in one rewritten
// join operand.
type OperandIdentityFacts struct {
	Owner       OwnerResolution
	Occurrences []IdentityOccurrence
}

func hintedTableIdentityKey(table *h.HintedTable) string {
	if table == nil {
		return ""
	}
	return table.DBName.L + "\x00" + table.TblName.L
}

func hintTableToHintedTable(sctx base.PlanContext, hintTable *ast.HintTable, selectOffset int) *h.HintedTable {
	if hintTable == nil {
		return nil
	}
	dbName := hintTable.DBName
	if dbName.L == "" {
		dbName = ast.NewCIStr(sctx.GetSessionVars().CurrentDB)
	}
	return &h.HintedTable{DBName: dbName, TblName: hintTable.TableName, SelectOffset: selectOffset}
}

func planChildren(p base.Plan) []base.Plan {
	switch x := p.(type) {
	case base.LogicalPlan:
		children := x.Children()
		result := make([]base.Plan, len(children))
		for i := range children {
			result[i] = children[i]
		}
		return result
	case base.PhysicalPlan:
		children := x.Children()
		result := make([]base.Plan, len(children))
		for i := range children {
			result[i] = children[i]
		}
		return result
	default:
		return nil
	}
}

func sameHintedTableIdentity(left, right *h.HintedTable) bool {
	return left != nil && right != nil &&
		left.DBName.L == right.DBName.L &&
		left.TblName.L == right.TblName.L
}

type operandSubtreeFacts struct {
	leafCount      int
	leafIdentity   *h.HintedTable
	leavesHaveName bool
	concrete       []IdentityOccurrence
}

// ExtractOperandIdentityFacts walks p once and extracts strict owner identity,
// concrete provenance, and derived-alias candidates. It deliberately does not
// read an AST hint or decide which compatibility syntax is accepted.
func ExtractOperandIdentityFacts(p base.Plan, ownerQB int) OperandIdentityFacts {
	facts := OperandIdentityFacts{Owner: OwnerResolution{Kind: OwnerAbsent}}
	if p == nil || ownerQB < 0 {
		return facts
	}

	ownerIdentities := make(map[string]*h.HintedTable)
	hasAliasCandidate := false
	brokenAliasChain := false

	addOwnerIdentity := func(identity *h.HintedTable) {
		if identity == nil || identity.TblName.L == "" {
			return
		}
		ownerIdentities[hintedTableIdentityKey(identity)] = identity
	}

	var walk func(base.Plan) operandSubtreeFacts
	walk = func(cur base.Plan) operandSubtreeFacts {
		children := planChildren(cur)
		summary := operandSubtreeFacts{leavesHaveName: true}
		for _, child := range children {
			childSummary := walk(child)
			summary.leafCount += childSummary.leafCount
			summary.leavesHaveName = summary.leavesHaveName && childSummary.leavesHaveName
			if childSummary.leafCount > 0 {
				if summary.leafIdentity == nil {
					summary.leafIdentity = childSummary.leafIdentity
				} else if !sameHintedTableIdentity(summary.leafIdentity, childSummary.leafIdentity) {
					summary.leafIdentity = nil
				}
			}
			summary.concrete = append(summary.concrete, childSummary.concrete...)
		}

		offset := cur.QueryBlockOffset()
		if len(children) == 0 {
			summary.leafCount = 1
			summary.leafIdentity = ExtractTableAlias(cur, offset)
			summary.leavesHaveName = summary.leafIdentity != nil
			if offset > 0 && offset != ownerQB && summary.leafIdentity != nil {
				summary.concrete = []IdentityOccurrence{{
					Identity: *summary.leafIdentity,
					StartQB:  offset,
					NodeID:   cur.ID(),
					Kind:     ConcreteOccurrence,
				}}
			}
		}

		if offset > 0 && offset != ownerQB {
			if _, ok := LookupDirectSelectBlockAlias(cur.SCtx(), offset); ok {
				hasAliasCandidate = true
				if visible, ok := ResolveVisibleHintTableStrict(cur.SCtx(), offset, ownerQB); ok {
					addOwnerIdentity(hintTableToHintedTable(cur.SCtx(), visible, ownerQB))
				} else {
					brokenAliasChain = true
				}

				identity := h.HintedTable{SelectOffset: offset}
				if output := ExtractTableAlias(cur, offset); output != nil {
					identity = *output
				}
				facts.Occurrences = append(facts.Occurrences, IdentityOccurrence{
					Identity: identity,
					StartQB:  offset,
					NodeID:   cur.ID(),
					Kind:     AliasCandidateOccurrence,
				})
			}

			if len(children) > 0 && summary.leafCount == 1 && summary.leavesHaveName {
				wrapperIdentity := ExtractTableAlias(cur, offset)
				if sameHintedTableIdentity(wrapperIdentity, summary.leafIdentity) {
					summary.concrete = []IdentityOccurrence{{
						Identity: *wrapperIdentity,
						StartQB:  offset,
						NodeID:   cur.ID(),
						Kind:     ConcreteOccurrence,
					}}
				}
			}
		} else if offset == ownerQB && len(children) == 0 {
			addOwnerIdentity(ExtractTableAlias(cur, ownerQB))
		}

		return summary
	}

	subtree := walk(p)
	facts.Occurrences = append(facts.Occurrences, subtree.concrete...)

	switch {
	case brokenAliasChain:
		facts.Owner.Kind = OwnerBrokenAliasChain
	case len(ownerIdentities) > 1:
		facts.Owner.Kind = OwnerAmbiguous
	case len(ownerIdentities) == 1:
		facts.Owner.Kind = OwnerUnique
		for _, identity := range ownerIdentities {
			facts.Owner.Identity = identity
		}
	case !hasAliasCandidate && p.QueryBlockOffset() == ownerQB:
		if identity := ExtractTableAlias(p, ownerQB); identity != nil {
			facts.Owner = OwnerResolution{Kind: OwnerUnique, Identity: identity}
		}
	}
	return facts
}

// ResolveJoinOperandHintIdentity returns the strict identity visible in
// ownerQB. Generation and canonical hint consumers must use only this wrapper,
// never LEADING compatibility policy.
func ResolveJoinOperandHintIdentity(p base.Plan, ownerQB int) (*h.HintedTable, bool) {
	facts := ExtractOperandIdentityFacts(p, ownerQB)
	return facts.Owner.Identity, facts.Owner.Kind == OwnerUnique
}

// JoinOperandHasDerivedAliasCandidate reports whether p contains an operand
// query block with recorded derived-table identity relative to ownerQB.
func JoinOperandHasDerivedAliasCandidate(p base.Plan, ownerQB int) bool {
	if p == nil || ownerQB < 0 {
		return false
	}
	facts := ExtractOperandIdentityFacts(p, ownerQB)
	for _, occurrence := range facts.Occurrences {
		if occurrence.Kind == AliasCandidateOccurrence {
			return true
		}
	}
	return false
}
