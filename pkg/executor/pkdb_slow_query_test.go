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

package executor

import (
	"bufio"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestPKDBParseSlowLogEscapedNewlines(t *testing.T) {
	const query = `select 1,\n  'a', -- comment\n  100;`
	slowLog := `# Time: 2026-08-17T14:09:13.980688+08:00
# Query_time: 1
` + query

	sctx := mock.NewContext()
	retriever, err := newSlowQueryRetriever()
	require.NoError(t, err)
	require.NoError(t, retriever.initialize(context.Background(), sctx))

	rows, err := parseLog(retriever, sctx, bufio.NewReader(strings.NewReader(slowLog)))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	queryColumn := -1
	for i, column := range retriever.outputCols {
		if column.Name.L == strings.ToLower(variable.SlowLogQuerySQLStr) {
			queryColumn = i
			break
		}
	}
	require.NotEqual(t, -1, queryColumn)

	parsedQuery, err := rows[0][queryColumn].ToString()
	require.NoError(t, err)
	require.Equal(t, query, parsedQuery)
}

func TestPKDBSlowQueryDigestParseFilter(t *testing.T) {
	const (
		digest1     = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		digest2     = "1111111111111111111111111111111111111111111111111111111111111111"
		otherDigest = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	)
	const slowLog = `# Time: 2026-08-17T14:09:13.000000+08:00
# Digest: ` + otherDigest + `
# Query_time: 1
select 1;
# Time: 2026-08-17T14:09:14.000000+08:00
# Digest: ` + digest1 + `
# Query_time: 2
select 2;
# Time: 2026-08-17T14:09:15.000000+08:00
# Digest: ` + digest2 + `
# Query_time: 3
select 3;
`
	// Records without a Digest keep the legacy parse behavior. The retained
	// Selection is responsible for excluding them from SQL query results.
	const legacyLog = `# Time: 2026-08-17T14:09:16.000000+08:00
# Query_time: 4
select 4;
`
	for _, tc := range []struct {
		name        string
		digests     set.StringSet
		log         string
		wantQueries []string
	}{
		{"no_filter", nil, slowLog, []string{"select 1;", "select 2;", "select 3;"}},
		{"single_digest", set.NewStringSet(digest1), slowLog, []string{"select 2;"}},
		{"multiple_digests", set.NewStringSet(digest1, digest2), slowLog, []string{"select 2;", "select 3;"}},
		{"unmatched_digest", set.NewStringSet("unmatched"), slowLog, nil},
		{"missing_digest", set.NewStringSet(digest1), slowLog + legacyLog, []string{"select 2;", "select 4;"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sctx := mock.NewContext()
			retriever, err := newSlowQueryRetriever()
			require.NoError(t, err)
			// Digest filtering must also work when only Query is projected.
			queryColumn := model.FindColumnInfo(retriever.outputCols, variable.SlowLogQuerySQLStr)
			require.NotNil(t, queryColumn)
			retriever.outputCols = []*model.ColumnInfo{queryColumn}
			retriever.extractor = &plannercore.SlowQueryExtractor{Digests: tc.digests}
			require.NoError(t, retriever.initialize(context.Background(), sctx))
			rows, err := parseLog(retriever, sctx, bufio.NewReader(strings.NewReader(tc.log)))
			require.NoError(t, err)
			var queries []string
			for _, row := range rows {
				queries = append(queries, row[0].GetString())
			}
			require.Equal(t, tc.wantQueries, queries)
		})
	}
}

func TestPKDBPBPlanBuilderExtractsSlowQueryDigest(t *testing.T) {
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	data := infoschema.NewData()
	schemaCacheSize := variable.SchemaCacheSize.Load()
	newISBuilder := infoschema.NewBuilder(nil, nil, data, schemaCacheSize > 0)
	require.NoError(t, newISBuilder.InitWithDBInfos(nil, nil, nil, 0))
	is := newISBuilder.Build(math.MaxUint64)
	tbl, err := is.TableByName(t.Context(), metadef.InformationSchemaName, pmodel.NewCIStr(infoschema.ClusterTableSlowLog))
	require.NoError(t, err)

	digestColumnInfo := model.FindColumnInfo(tbl.Meta().Columns, variable.SlowLogDigestStr)
	require.NotNil(t, digestColumnInfo)

	sctx := mock.NewContext()
	digestColumn := &expression.Column{
		Index:   digestColumnInfo.Offset,
		ID:      digestColumnInfo.ID,
		RetType: &digestColumnInfo.FieldType,
	}
	pbColumns := util.ColumnsToProto(tbl.Meta().Columns, tbl.Meta().PKIsHandle, false, false)
	newFunction := func(name string, args ...expression.Expression) expression.Expression {
		fn, err := expression.NewFunction(
			sctx.GetExprCtx(),
			name,
			types.NewFieldType(mysql.TypeTiny),
			args...,
		)
		require.NoError(t, err)
		return fn
	}
	buildPlan := func(conditions []expression.Expression) base.PhysicalPlan {
		pbConditions, err := expression.ExpressionsToPBList(
			sctx.GetExprCtx().GetEvalCtx(),
			conditions,
			new(mock.Client),
		)
		require.NoError(t, err)

		executors := []*tipb.Executor{
			{
				Tp: tipb.ExecType_TypeTableScan,
				TblScan: &tipb.TableScan{
					TableId: tbl.Meta().ID,
					Columns: pbColumns,
				},
			},
			{
				Tp: tipb.ExecType_TypeSelection,
				Selection: &tipb.Selection{
					Conditions: pbConditions,
				},
			},
		}

		physicalPlan, err := plannercore.NewPBPlanBuilder(sctx, is, nil).Build(executors)
		require.NoError(t, err)
		return physicalPlan
	}

	for _, tc := range []struct {
		name        string
		conditions  []expression.Expression
		wantDigests set.StringSet
	}{
		{
			name:        "column on left",
			conditions:  []expression.Expression{newFunction(ast.EQ, digestColumn, expression.NewStrConst(digest))},
			wantDigests: set.NewStringSet(digest),
		},
		{
			name:        "column on right",
			conditions:  []expression.Expression{newFunction(ast.EQ, expression.NewStrConst(digest), digestColumn)},
			wantDigests: set.NewStringSet(digest),
		},
		{
			name: "function expression falls back",
			conditions: []expression.Expression{
				newFunction(ast.EQ, newFunction(ast.Upper, digestColumn), expression.NewStrConst(strings.ToUpper(digest))),
			},
			wantDigests: set.NewStringSet(),
		},
		{
			name: "contradictory equalities preserve predicates",
			conditions: []expression.Expression{
				newFunction(ast.EQ, digestColumn, expression.NewStrConst(digest)),
				newFunction(ast.EQ, digestColumn, expression.NewStrConst(strings.Repeat("f", 64))),
			},
			wantDigests: set.NewStringSet(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physicalPlan := buildPlan(tc.conditions)
			selection, ok := physicalPlan.(*plannercore.PhysicalSelection)
			require.True(t, ok, "digest predicates must remain in remote Selection")
			require.Len(t, selection.Conditions, len(tc.conditions))
			physicalPlan = selection.Children()[0]
			memTable, ok := physicalPlan.(*plannercore.PhysicalMemTable)
			require.True(t, ok)
			extractor := memTable.Extractor.(*plannercore.SlowQueryExtractor)
			require.False(t, extractor.SkipRequest)
			require.Equal(t, tc.wantDigests, extractor.Digests)
		})
	}
}
