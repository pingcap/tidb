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
	"testing"

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
		indexName string
		columnNum int
	}{
		{
			sql:       "select id from articles where match(title) against('+tidb -mysql' in boolean mode)",
			indexName: "idx_title",
			columnNum: 1,
		},
		{
			sql:       "select id from articles where match(title, body) against('+tidb -mysql' in boolean mode)",
			indexName: "idx_title_body",
			columnNum: 2,
		},
	}
	for _, query := range queries {
		t.Run(query.indexName, func(t *testing.T) {
			plan := compilePhysicalPlan(t, tk, query.sql)
			scan := findFTSTableScan(t, plan)
			require.NotNil(t, scan.FtsQueryInfo)

			var expectedIndexID int64
			for _, index := range tbl.Meta().Indices {
				if index.Name.L == query.indexName {
					expectedIndexID = index.ID
					break
				}
			}
			require.NotZero(t, expectedIndexID)
			require.Equal(t, expectedIndexID, scan.FtsQueryInfo.IndexId)
			require.Equal(t, tipb.ScalarFuncSig_FTSMatchExpression, scan.FtsQueryInfo.QueryFunc)
			require.NotNil(t, scan.FtsQueryInfo.BooleanQuery)
			require.Len(t, scan.FtsQueryInfo.Columns, query.columnNum)

			pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
			require.NoError(t, err)
			require.NotNil(t, pb.TblScan)
			require.Len(t, pb.TblScan.UsedColumnarIndexes, 1)
			indexInfo := pb.TblScan.UsedColumnarIndexes[0]
			require.Equal(t, tipb.ColumnarIndexType_TypeFulltext, indexInfo.IndexType)
			ftsInfo, ok := indexInfo.Index.(*tipb.ColumnarIndexInfo_FtsQueryInfo)
			require.True(t, ok)
			require.NotNil(t, ftsInfo.FtsQueryInfo.BooleanQuery)

			explainRows := tk.MustQuery("explain format='brief' " + query.sql).Rows()
			explain := fmt.Sprint(explainRows)
			require.Contains(t, explain, "tiflash")
			require.NotContains(t, explain, "Selection")
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
	scan := findFTSTableScan(t, compilePhysicalPlan(t, tk, sql))
	require.NotNil(t, scan.FtsQueryInfo)
	require.Equal(t, string(model.FullTextParserTypeNgramV1), scan.FtsQueryInfo.QueryTokenizer)
	require.NotNil(t, scan.FtsQueryInfo.BooleanQuery)
	require.Equal(t, uint32(2), scan.FtsQueryInfo.BooleanQuery.NgramTokenSize)

	pb, err := scan.ToPB(tk.Session().GetBuildPBCtx(), kv.TiFlash)
	require.NoError(t, err)
	require.NotNil(t, pb.TblScan)
	require.Len(t, pb.TblScan.UsedColumnarIndexes, 1)
	ftsInfo, ok := pb.TblScan.UsedColumnarIndexes[0].Index.(*tipb.ColumnarIndexInfo_FtsQueryInfo)
	require.True(t, ok)
	require.Equal(t, uint32(2), ftsInfo.FtsQueryInfo.BooleanQuery.NgramTokenSize)

	explainRows := tk.MustQuery("explain format='brief' " + sql).Rows()
	require.Contains(t, fmt.Sprint(explainRows), "tiflash")
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
