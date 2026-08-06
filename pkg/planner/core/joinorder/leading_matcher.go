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

package joinorder

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// LeadingMatchKind identifies both the matching syntax and the capabilities it
// grants. Legacy concrete matches can select an operand but cannot preserve a
// derived-table boundary.
type LeadingMatchKind uint8

const (
	LeadingNoMatch LeadingMatchKind = iota
	LeadingCanonicalOwner
	LeadingLegacyRaw
	LeadingLegacyQualifiedConcrete
	LeadingQualifiedOwnerVisible
	LeadingLegacyPositionalOwnerVisible
	LeadingAmbiguous
)

// LeadingMatch is the result of applying LEADING compatibility policy to one
// operand's extracted facts.
type LeadingMatch struct {
	Kind LeadingMatchKind
}

// Matched reports whether the hint may select this operand.
func (m LeadingMatch) Matched() bool {
	switch m.Kind {
	case LeadingCanonicalOwner,
		LeadingLegacyRaw,
		LeadingLegacyQualifiedConcrete,
		LeadingQualifiedOwnerVisible,
		LeadingLegacyPositionalOwnerVisible:
		return true
	default:
		return false
	}
}

// PreserveBoundary reports whether the hint names an identity visible in the
// owner query block and may therefore keep a derived join group intact.
func (m LeadingMatch) PreserveBoundary() bool {
	switch m.Kind {
	case LeadingCanonicalOwner,
		LeadingQualifiedOwnerVisible,
		LeadingLegacyPositionalOwnerVisible:
		return true
	default:
		return false
	}
}

func leadingHintMatchesIdentity(table *ast.HintTable, identity *h.HintedTable) bool {
	if table == nil || identity == nil {
		return false
	}
	dbMatch := table.DBName.L == "" || table.DBName.L == identity.DBName.L || table.DBName.L == "*"
	return dbMatch && table.TableName.L == identity.TblName.L
}

func leadingSelectOffset(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
}

func matchingConcreteOccurrences(
	table *ast.HintTable,
	facts plannerutil.OperandIdentityFacts,
	qualifiedQB int,
) []plannerutil.IdentityOccurrence {
	matches := make([]plannerutil.IdentityOccurrence, 0, 1)
	seenNode := make(map[int]struct{})
	for _, occurrence := range facts.Occurrences {
		if occurrence.Kind != plannerutil.ConcreteOccurrence ||
			(qualifiedQB >= 0 && occurrence.StartQB != qualifiedQB) ||
			!leadingHintMatchesIdentity(table, &occurrence.Identity) {
			continue
		}
		if _, exists := seenNode[occurrence.NodeID]; exists {
			continue
		}
		seenNode[occurrence.NodeID] = struct{}{}
		matches = append(matches, occurrence)
	}
	return matches
}

func ownerVisibleQualifiedIdentity(
	facts plannerutil.OperandIdentityFacts,
	aliases []h.SelectBlockAlias,
	expectedQB int,
) (*h.HintedTable, bool, bool) {
	identities := make(map[string]h.HintedTable)
	for _, occurrence := range facts.Occurrences {
		if occurrence.Kind != plannerutil.AliasCandidateOccurrence {
			continue
		}
		identity := occurrence.Identity
		switch {
		case occurrence.StartQB == expectedQB:
			if identity.TblName.L == "" {
				continue
			}
			identity.SelectOffset = expectedQB
		default:
			resolved, ok := h.ResolveSelectBlockAlias(aliases, occurrence.StartQB, expectedQB)
			if !ok {
				continue
			}
			dbName := resolved.DBName
			if dbName.L == "" {
				dbName = identity.DBName
			}
			identity = h.HintedTable{
				DBName:       dbName,
				TblName:      resolved.TableName,
				SelectOffset: expectedQB,
			}
		}
		key := identity.DBName.L + "\x00" + identity.TblName.L
		identities[key] = identity
	}
	if len(identities) > 1 {
		return nil, false, true
	}
	for _, identity := range identities {
		copy := identity
		return &copy, true, false
	}
	return nil, false, false
}

func matchPositionalOwnerVisible(
	table *ast.HintTable,
	ownerIdentity *h.HintedTable,
	aliases []h.SelectBlockAlias,
	ownerQB,
	expectedPosition int,
) LeadingMatch {
	if ownerIdentity == nil || expectedPosition <= 0 {
		return LeadingMatch{}
	}
	position := 0
	for _, alias := range aliases {
		if alias.VisibleOffset != ownerQB || alias.TableName.L == "" {
			continue
		}
		position++
		if position != expectedPosition {
			continue
		}
		dbName := alias.DBName
		if dbName.L == "" {
			dbName = ownerIdentity.DBName
		}
		positionalIdentity := &h.HintedTable{
			DBName:       dbName,
			TblName:      alias.TableName,
			SelectOffset: ownerQB,
		}
		if leadingHintMatchesIdentity(table, positionalIdentity) &&
			positionalIdentity.DBName.L == ownerIdentity.DBName.L &&
			positionalIdentity.TblName.L == ownerIdentity.TblName.L &&
			ownerIdentity.SelectOffset == ownerQB {
			return LeadingMatch{Kind: LeadingLegacyPositionalOwnerVisible}
		}
		return LeadingMatch{}
	}
	return LeadingMatch{}
}

// MatchLeadingHint applies LEADING replay policy to policy-free operand facts.
// Ambiguity in the hint's active concrete namespace terminates matching before
// owner-visible or positional fallbacks can reinterpret the same name.
func MatchLeadingHint(
	table *ast.HintTable,
	facts plannerutil.OperandIdentityFacts,
	aliases []h.SelectBlockAlias,
	ownerQB int,
) LeadingMatch {
	if table == nil || ownerQB < 0 {
		return LeadingMatch{}
	}
	if facts.Owner.Kind == plannerutil.OwnerAmbiguous {
		return LeadingMatch{Kind: LeadingAmbiguous}
	}

	if table.QBName.L == "" {
		concrete := matchingConcreteOccurrences(table, facts, -1)
		if len(concrete) > 1 {
			return LeadingMatch{Kind: LeadingAmbiguous}
		}
		switch facts.Owner.Kind {
		case plannerutil.OwnerUnique:
			if leadingHintMatchesIdentity(table, facts.Owner.Identity) {
				return LeadingMatch{Kind: LeadingCanonicalOwner}
			}
		case plannerutil.OwnerAbsent, plannerutil.OwnerBrokenAliasChain:
			// These states admit only the narrow unqualified concrete path.
		default:
			return LeadingMatch{}
		}
		if len(concrete) == 1 {
			return LeadingMatch{Kind: LeadingLegacyRaw}
		}
		return LeadingMatch{}
	}

	// Absent and broken owners permit only unqualified concrete compatibility.
	if facts.Owner.Kind != plannerutil.OwnerUnique {
		return LeadingMatch{}
	}
	expectedQB := leadingSelectOffset(table.QBName.L)
	if expectedQB < 0 {
		return LeadingMatch{}
	}
	concrete := matchingConcreteOccurrences(table, facts, expectedQB)
	if len(concrete) > 1 {
		return LeadingMatch{Kind: LeadingAmbiguous}
	}
	if expectedQB == ownerQB {
		if leadingHintMatchesIdentity(table, facts.Owner.Identity) {
			return LeadingMatch{Kind: LeadingCanonicalOwner}
		}
		return LeadingMatch{}
	}
	if len(concrete) == 1 {
		return LeadingMatch{Kind: LeadingLegacyQualifiedConcrete}
	}

	visible, ok, ambiguous := ownerVisibleQualifiedIdentity(facts, aliases, expectedQB)
	if ambiguous {
		return LeadingMatch{Kind: LeadingAmbiguous}
	}
	if ok && leadingHintMatchesIdentity(table, visible) &&
		visible.DBName.L == facts.Owner.Identity.DBName.L &&
		visible.TblName.L == facts.Owner.Identity.TblName.L &&
		facts.Owner.Identity.SelectOffset == ownerQB {
		return LeadingMatch{Kind: LeadingQualifiedOwnerVisible}
	}

	return matchPositionalOwnerVisible(table, facts.Owner.Identity, aliases, ownerQB, expectedQB)
}

func matchLeadingHintToPlan(p base.Plan, table *ast.HintTable, ownerQB int) LeadingMatch {
	if p == nil {
		return LeadingMatch{}
	}
	var aliases []h.SelectBlockAlias
	if stored := p.SCtx().GetSessionVars().PlannerSelectBlockAliasInfo.Load(); stored != nil {
		aliases = *stored
	}
	return MatchLeadingHint(table, plannerutil.ExtractOperandIdentityFacts(p, ownerQB), aliases, ownerQB)
}
