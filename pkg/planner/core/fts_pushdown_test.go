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

package core_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestMatchAgainstBooleanPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table articles (
		id int primary key,
		title varchar(200),
		body text,
		fulltext index idx_title(title),
		fulltext index idx_title_body(title, body)
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=OFF")

	queries := []struct {
		sql       string
		name      string
		columnNum int
	}{
		{
			sql:       "select id from articles where match(title) against('+tidb -mysql' in boolean mode)",
			name:      "single_column_match",
			columnNum: 1,
		},
		{
			sql:       "select id from articles where match(title, body) against('+tidb -mysql' in boolean mode)",
			name:      "multi_column_match",
			columnNum: 2,
		},
	}
	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			plan := compilePhysicalPlan(t, tk, query.sql)
			scan := findFTSTableScan(t, plan)
			// MATCH columns must be read by TiFlash even when they are not
			// projected by the SQL query; the scalar Selection evaluates them.
			require.Len(t, plan.Schema().Columns, 1)
			scanColumnNames := make(map[string]struct{}, len(scan.Columns))
			for _, col := range scan.Columns {
				scanColumnNames[col.Name.L] = struct{}{}
			}
			require.Contains(t, scanColumnNames, "title")
			if query.columnNum == 2 {
				require.Contains(t, scanColumnNames, "body")
			}

			pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
			require.NoError(t, err)
			require.NotNil(t, pb.TblScan)
			metadata := assertFTSScalarSelection(t, tk, plan, query.columnNum)
			require.NotNil(t, metadata.GetBooleanQuery())
			require.NotZero(t, metadata.GetVersion())

			explainRows := tk.MustQuery("explain format='brief' " + query.sql).Rows()
			explain := fmt.Sprint(explainRows)
			require.Contains(t, explain, "tiflash")
			require.Contains(t, strings.ToLower(explain), "selection")
		})
	}
}

func TestMatchAgainstNgramBooleanPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table ngram_articles (
		id int primary key,
		title varchar(200),
		fulltext index idx_title_ngram(title) with parser ngram
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("ngram_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=OFF")

	sql := "select id from ngram_articles where match(title) against('+tidb' in boolean mode)"
	plan := compilePhysicalPlan(t, tk, sql)
	scan := findFTSTableScan(t, plan)
	require.Len(t, plan.Schema().Columns, 1)
	var hasTitle bool
	for _, col := range scan.Columns {
		if col.Name.L == "title" {
			hasTitle = true
			break
		}
	}
	require.True(t, hasTitle, "the TiFlash table scan must include the NGRAM MATCH column")
	metadata := assertFTSScalarSelection(t, tk, plan, 1)
	require.Equal(t, string(model.FullTextParserTypeNgramV1), metadata.GetBooleanQuery().GetQueryTokenizer())
	require.Equal(t, uint32(2), metadata.GetBooleanQuery().GetNgramTokenSize())

	pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.TblScan)

	explainRows := tk.MustQuery("explain format='brief' " + sql).Rows()
	require.Contains(t, fmt.Sprint(explainRows), "tiflash")
}

func TestMultipleMatchAgainstBooleanPredicatesUseTiFlashSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table multi_match_articles (
		id int primary key,
		title varchar(200),
		fulltext index idx_title(title)
	)`)

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("multi_match_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=OFF")

	sql := "select id from multi_match_articles where " +
		"match(title) against('+tidb' in boolean mode) OR " +
		"match(title) against('+mysql' in boolean mode)"
	plan := compilePhysicalPlan(t, tk, sql)
	// Multi-predicate Boolean expressions are represented by a TiFlash
	// Selection containing scalar FTS calls, not by one scan-level query.
	selection := findFTSSelection(t, plan)
	selectionPB, err := selection.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	var booleanFunctionCount int
	for _, condition := range selectionPB.GetSelection().GetConditions() {
		booleanFunctionCount += countScalarFunctionExpr(condition, tipb.ScalarFuncSig_FTSMatchBooleanExpression)
	}
	require.Equal(t, 2, booleanFunctionCount)
	explain := strings.ToLower(fmt.Sprint(tk.MustQuery("explain format='brief' " + sql).Rows()))
	require.Contains(t, explain, "mpp[tiflash]")
	require.Contains(t, explain, "selection")
	require.Contains(t, explain, "match_against")
}

func TestMatchAgainstStandardAnalyzerSettingsPushdownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table standard_articles (
		id int primary key,
		body text,
		fulltext index idx_body(body)
	)`)

	oldMinTokenSize := tk.MustQuery("select @@global.innodb_ft_min_token_size").Rows()[0][0]
	oldMaxTokenSize := tk.MustQuery("select @@global.innodb_ft_max_token_size").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set global innodb_ft_min_token_size=%v", oldMinTokenSize))
		tk.MustExec(fmt.Sprintf("set global innodb_ft_max_token_size=%v", oldMaxTokenSize))
	}()
	tk.MustExec("set global innodb_ft_min_token_size=1")
	tk.MustExec("set global innodb_ft_max_token_size=10")
	tk.MustExec("set session innodb_ft_enable_stopword=OFF")

	dom := domain.GetDomain(tk.Session())
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("standard_articles"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustExec("set @@session.tidb_enable_local_match_against=OFF")

	plan := compilePhysicalPlan(t, tk, "select id from standard_articles where match(body) against('+the' in boolean mode)")
	scan := findFTSTableScan(t, plan)
	metadata := assertFTSScalarSelection(t, tk, plan, 1)
	booleanQuery := metadata.GetBooleanQuery()
	require.NotNil(t, booleanQuery)
	require.Equal(t, uint32(1), booleanQuery.GetInnodbFtMinTokenSize())
	require.Equal(t, uint32(10), booleanQuery.GetInnodbFtMaxTokenSize())
	require.False(t, booleanQuery.GetInnodbFtEnableStopword())

	pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.TblScan)
}

func assertFTSScalarSelection(t *testing.T, tk *testkit.TestKit, plan base.Plan, matchColumnCount int) *tipb.FTSMatchBooleanMetadata {
	t.Helper()
	selection := findFTSSelection(t, plan)
	pb, err := selection.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.GetSelection())
	var ftsExpr *tipb.Expr
	for _, condition := range pb.GetSelection().GetConditions() {
		if found := findScalarFunctionExpr(condition, tipb.ScalarFuncSig_FTSMatchBooleanExpression); found != nil {
			ftsExpr = found
			break
		}
	}
	require.NotNil(t, ftsExpr, "Boolean MATCH must be encoded as its dedicated scalar function")
	require.Len(t, ftsExpr.GetChildren(), matchColumnCount+1)
	metadata := &tipb.FTSMatchBooleanMetadata{}
	require.NoError(t, proto.Unmarshal(ftsExpr.GetVal(), metadata))
	require.Equal(t, uint32(1), metadata.GetVersion())
	require.NotNil(t, metadata.GetBooleanQuery())
	return metadata
}

func findFTSSelection(t *testing.T, plan base.Plan) *core.PhysicalSelection {
	t.Helper()
	var result *core.PhysicalSelection
	var visit func(base.Plan)
	visit = func(plan base.Plan) {
		if result != nil || plan == nil {
			return
		}
		if selection, ok := plan.(*core.PhysicalSelection); ok {
			result = selection
			return
		}
		if reader, ok := plan.(*core.PhysicalTableReader); ok {
			for _, child := range reader.TablePlans {
				visit(child)
			}
			return
		}
		if physical, ok := plan.(base.PhysicalPlan); ok {
			for _, child := range physical.Children() {
				visit(child)
			}
		}
	}
	visit(plan)
	require.NotNil(t, result, "expected TiFlash scalar Selection for Boolean MATCH")
	return result
}

func findScalarFunctionExpr(expr *tipb.Expr, sig tipb.ScalarFuncSig) *tipb.Expr {
	if expr == nil {
		return nil
	}
	if expr.GetTp() == tipb.ExprType_ScalarFunc && expr.GetSig() == sig {
		return expr
	}
	for _, child := range expr.GetChildren() {
		if found := findScalarFunctionExpr(child, sig); found != nil {
			return found
		}
	}
	return nil
}

func countScalarFunctionExpr(expr *tipb.Expr, sig tipb.ScalarFuncSig) int {
	if expr == nil {
		return 0
	}
	count := 0
	if expr.GetTp() == tipb.ExprType_ScalarFunc && expr.GetSig() == sig {
		count++
	}
	for _, child := range expr.GetChildren() {
		count += countScalarFunctionExpr(child, sig)
	}
	return count
}

func compilePhysicalPlan(t *testing.T, tk *testkit.TestKit, sql string) base.Plan {
	t.Helper()
	ctx := context.Background()
	statements, err := tk.Session().Parse(ctx, sql)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	stmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, statements[0])
	require.NoError(t, err)
	return stmt.Plan
}

func findFTSTableScan(t *testing.T, plan base.Plan) *core.PhysicalTableScan {
	t.Helper()
	var result *core.PhysicalTableScan
	var visit func(base.Plan)
	visit = func(plan base.Plan) {
		if result != nil || plan == nil {
			return
		}
		if scan, ok := plan.(*core.PhysicalTableScan); ok {
			result = scan
			return
		}
		if reader, ok := plan.(*core.PhysicalTableReader); ok {
			for _, child := range reader.TablePlans {
				visit(child)
			}
			return
		}
		if physical, ok := plan.(base.PhysicalPlan); ok {
			for _, child := range physical.Children() {
				visit(child)
			}
		}
	}
	visit(plan)
	require.NotNil(t, result, "expected a physical table scan in the plan")
	return result
}
