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

package joinorder_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestMatchLeadingHintTableToOperandFailClosed(t *testing.T) {
	identity := func(table string, offset int) *h.HintedTable {
		return &h.HintedTable{DBName: ast.NewCIStr("test"), TblName: ast.NewCIStr(table), SelectOffset: offset}
	}
	occurrence := func(table string, qb, nodeID int, kind plannerutil.IdentityOccurrenceKind) plannerutil.IdentityOccurrence {
		return plannerutil.IdentityOccurrence{Identity: *identity(table, qb), StartQB: qb, NodeID: nodeID, Kind: kind}
	}
	hintTable := func(table string) *ast.HintTable {
		return &ast.HintTable{TableName: ast.NewCIStr(table)}
	}
	qualifiedHint := func(table string, qb int) *ast.HintTable {
		return &ast.HintTable{TableName: ast.NewCIStr(table), QBName: ast.NewCIStr("sel_" + string(rune('0'+qb)))}
	}
	uniqueOwner := func(table string) plannerutil.OwnerResolution {
		return plannerutil.OwnerResolution{Kind: plannerutil.OwnerUnique, Identity: identity(table, 1)}
	}
	match := func(table *ast.HintTable, facts plannerutil.OperandIdentityFacts, aliases []h.SelectBlockAlias) joinorder.LeadingMatch {
		return joinorder.MatchLeadingHint(table, facts, aliases, 1)
	}

	t.Run("owner state admission matrix", func(t *testing.T) {
		concrete := []plannerutil.IdentityOccurrence{occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence)}
		tests := []struct {
			name  string
			owner plannerutil.OwnerResolution
			want  joinorder.LeadingMatchKind
		}{
			{name: "unique", owner: uniqueOwner("dt"), want: joinorder.LeadingLegacyRaw},
			{name: "absent", owner: plannerutil.OwnerResolution{Kind: plannerutil.OwnerAbsent}, want: joinorder.LeadingLegacyRaw},
			{name: "broken", owner: plannerutil.OwnerResolution{Kind: plannerutil.OwnerBrokenAliasChain}, want: joinorder.LeadingLegacyRaw},
			{name: "ambiguous", owner: plannerutil.OwnerResolution{Kind: plannerutil.OwnerAmbiguous}, want: joinorder.LeadingAmbiguous},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				got := match(hintTable("t2"), plannerutil.OperandIdentityFacts{Owner: test.owner, Occurrences: concrete}, nil)
				require.Equal(t, test.want, got.Kind)
			})
		}
		require.Equal(t, joinorder.LeadingQualifiedDecorrelatedConcrete,
			match(qualifiedHint("t2", 2), plannerutil.OperandIdentityFacts{
				Owner:       plannerutil.OwnerResolution{Kind: plannerutil.OwnerAbsent},
				Occurrences: concrete,
			}, nil).Kind)
	})

	t.Run("broken alias itself and qualified paths are rejected", func(t *testing.T) {
		facts := plannerutil.OperandIdentityFacts{
			Owner: plannerutil.OwnerResolution{Kind: plannerutil.OwnerBrokenAliasChain},
			Occurrences: []plannerutil.IdentityOccurrence{
				occurrence("d2", 2, 9, plannerutil.AliasCandidateOccurrence),
				occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence),
			},
		}
		require.Equal(t, joinorder.LeadingNoMatch, match(hintTable("d2"), facts, nil).Kind)
		require.Equal(t, joinorder.LeadingLegacyRaw, match(hintTable("t2"), facts, nil).Kind)
		require.Equal(t, joinorder.LeadingNoMatch, match(qualifiedHint("t2", 2), facts, nil).Kind)
	})

	t.Run("match kinds and capabilities", func(t *testing.T) {
		aliases := []h.SelectBlockAlias{
			{SelectOffset: 9, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("unrelated")}, {},
			{SelectOffset: 2, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("dt")},
		}
		tests := []struct {
			name     string
			table    *ast.HintTable
			facts    plannerutil.OperandIdentityFacts
			aliases  []h.SelectBlockAlias
			kind     joinorder.LeadingMatchKind
			matched  bool
			preserve bool
		}{
			{
				name: "canonical owner", table: hintTable("dt"),
				facts: plannerutil.OperandIdentityFacts{Owner: uniqueOwner("dt")},
				kind:  joinorder.LeadingCanonicalOwner, matched: true, preserve: true,
			},
			{
				name: "raw concrete", table: hintTable("t2"),
				facts: plannerutil.OperandIdentityFacts{Owner: uniqueOwner("dt"), Occurrences: []plannerutil.IdentityOccurrence{occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence)}},
				kind:  joinorder.LeadingLegacyRaw, matched: true,
			},
			{
				name: "qualified concrete", table: qualifiedHint("t2", 2),
				facts: plannerutil.OperandIdentityFacts{Owner: uniqueOwner("dt"), Occurrences: []plannerutil.IdentityOccurrence{occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence)}},
				kind:  joinorder.LeadingLegacyQualifiedConcrete, matched: true,
			},
			{
				name: "qualified decorrelated concrete", table: qualifiedHint("t2", 2),
				facts: plannerutil.OperandIdentityFacts{Owner: plannerutil.OwnerResolution{Kind: plannerutil.OwnerAbsent}, Occurrences: []plannerutil.IdentityOccurrence{occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence)}},
				kind:  joinorder.LeadingQualifiedDecorrelatedConcrete, matched: true, preserve: true,
			},
			{
				name: "qualified owner visible", table: qualifiedHint("dt", 2),
				facts:   plannerutil.OperandIdentityFacts{Owner: uniqueOwner("dt"), Occurrences: []plannerutil.IdentityOccurrence{occurrence("dt", 2, 9, plannerutil.AliasCandidateOccurrence)}},
				aliases: aliases, kind: joinorder.LeadingQualifiedOwnerVisible, matched: true, preserve: true,
			},
			{
				name: "positional owner visible", table: qualifiedHint("dt", 2),
				facts:   plannerutil.OperandIdentityFacts{Owner: uniqueOwner("dt")},
				aliases: aliases, kind: joinorder.LeadingLegacyPositionalOwnerVisible, matched: true, preserve: true,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				got := match(test.table, test.facts, test.aliases)
				require.Equal(t, test.kind, got.Kind)
				require.Equal(t, test.matched, got.Matched())
				require.Equal(t, test.preserve, got.PreserveBoundary())
			})
		}
	})

	t.Run("ambiguity cannot fall through", func(t *testing.T) {
		facts := plannerutil.OperandIdentityFacts{
			Owner: uniqueOwner("t2"),
			Occurrences: []plannerutil.IdentityOccurrence{
				occurrence("t2", 2, 10, plannerutil.ConcreteOccurrence),
				occurrence("t2", 2, 11, plannerutil.ConcreteOccurrence),
			},
		}
		aliases := []h.SelectBlockAlias{{}, {}, {SelectOffset: 2, VisibleOffset: 1, DBName: ast.NewCIStr("test"), TableName: ast.NewCIStr("t2")}}
		for _, table := range []*ast.HintTable{hintTable("t2"), qualifiedHint("t2", 2)} {
			got := match(table, facts, aliases)
			require.Equal(t, joinorder.LeadingAmbiguous, got.Kind)
			require.False(t, got.Matched())
			require.False(t, got.PreserveBoundary())
		}
	})

	t.Run("multiple operands matching one hint are rejected", func(t *testing.T) {
		ctx := mock.NewContext()
		newDataSource := func() *logicalop.DataSource {
			plan := logicalop.DataSource{}.Init(ctx, 1)
			plan.SetOutputNames(types.NameSlice{&types.FieldName{
				DBName: ast.NewCIStr("test"), TblName: ast.NewCIStr("t1"), ColName: ast.NewCIStr("a"),
			}})
			return plan
		}
		plans := []base.LogicalPlan{newDataSource(), newDataSource()}
		_, remaining, ok := joinorder.FindAndRemovePlanByAstHint(ctx, plans, hintTable("t1"), 1, func(plan base.LogicalPlan) base.LogicalPlan { return plan })
		require.False(t, ok)
		require.Len(t, remaining, 2)
	})
}
