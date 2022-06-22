// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/require"
)

type plannerSuite struct {
	p   *parser.Parser
	is  infoschema.InfoSchema
	ctx sessionctx.Context
}

func createPlannerSuite() (s *plannerSuite) {
	s = new(plannerSuite)
	tblInfos := []*model.TableInfo{
		MockSignedTable(),
		MockUnsignedTable(),
		MockView(),
		MockNoPKTable(),
		MockRangePartitionTable(),
		MockHashPartitionTable(),
		MockListPartitionTable(),
	}
	id := int64(0)
	for _, tblInfo := range tblInfos {
		tblInfo.ID = id
		id += 1
		pi := tblInfo.GetPartitionInfo()
		if pi == nil {
			continue
		}
		for _, def := range pi.Definitions {
			def.ID = id
			id += 1
		}
	}
	s.is = infoschema.MockInfoSchema(tblInfos)
	s.ctx = MockContext()
	domain.GetDomain(s.ctx).MockInfoCacheAndLoadInfoSchema(s.is)
	s.ctx.GetSessionVars().EnableWindowFunction = true
	s.p = parser.New()
	s.p.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})
	return
}

func TestPredicatePushDown(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	s := createPlannerSuite()
	ctx := context.Background()
	for ith, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[ith] = ToString(p)
		})
		require.Equal(t, output[ith], ToString(p), fmt.Sprintf("for %s %d", ca, ith))
	}
}

// Issue: 31399
func TestImplicitCastNotNullFlag(t *testing.T) {
	ctx := context.Background()
	ca := "select count(*) from t3 group by a having bit_and(b) > 1;"
	comment := fmt.Sprintf("for %s", ca)
	s := createPlannerSuite()
	stmt, err := s.p.ParseOneStmt(ca, "", "")
	require.NoError(t, err, comment)
	p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
	require.NoError(t, err)
	p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder|flagPrunColumns|flagEliminateProjection, p.(LogicalPlan))
	require.NoError(t, err)
	// AggFuncs[0] is count; AggFuncs[1] is bit_and, args[0] is return type of the implicit cast
	castNotNullFlag := (p.(*LogicalProjection).children[0].(*LogicalSelection).children[0].(*LogicalAggregation).AggFuncs[1].Args[0].GetType().GetFlag()) & mysql.NotNullFlag
	var nullableFlag uint = 0
	require.Equal(t, nullableFlag, castNotNullFlag)
}

func TestEliminateProjectionUnderUnion(t *testing.T) {
	ctx := context.Background()
	ca := "Select a from t3 join ( (select 127 as IDD from t3) union all (select 1 as IDD from t3) ) u on t3.b = u.IDD;"
	comment := fmt.Sprintf("for %s", ca)
	s := createPlannerSuite()
	stmt, err := s.p.ParseOneStmt(ca, "", "")
	require.NoError(t, err, comment)
	p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
	require.NoError(t, err)
	p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder|flagPrunColumns|flagEliminateProjection, p.(LogicalPlan))
	require.NoError(t, err)
	// after folding constants, the null flag should keep the same with the old one's (i.e., the schema's).
	schemaNullFlag := p.(*LogicalProjection).children[0].(*LogicalJoin).children[1].Children()[1].(*LogicalProjection).schema.Columns[0].RetType.GetFlag() & mysql.NotNullFlag
	exprNullFlag := p.(*LogicalProjection).children[0].(*LogicalJoin).children[1].Children()[1].(*LogicalProjection).Exprs[0].GetType().GetFlag() & mysql.NotNullFlag
	require.Equal(t, exprNullFlag, schemaNullFlag)
}

func TestJoinPredicatePushDown(t *testing.T) {
	var (
		input  []string
		output []struct {
			Left  string
			Right string
		}
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err, comment)
		proj, ok := p.(*LogicalProjection)
		require.True(t, ok, comment)
		join, ok := proj.children[0].(*LogicalJoin)
		require.True(t, ok, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		require.True(t, ok, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		require.True(t, ok, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		testdata.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		require.Equal(t, output[i].Left, leftCond, comment)
		require.Equal(t, output[i].Right, rightCond, comment)
	}
}

func TestOuterWherePredicatePushDown(t *testing.T) {
	var (
		input  []string
		output []struct {
			Sel   string
			Left  string
			Right string
		}
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err, comment)
		proj, ok := p.(*LogicalProjection)
		require.True(t, ok, comment)
		selection, ok := proj.children[0].(*LogicalSelection)
		require.True(t, ok, comment)
		selCond := fmt.Sprintf("%s", selection.Conditions)
		testdata.OnRecord(func() {
			output[i].Sel = selCond
		})
		require.Equal(t, output[i].Sel, selCond, comment)
		join, ok := selection.children[0].(*LogicalJoin)
		require.True(t, ok, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		require.True(t, ok, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		require.True(t, ok, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		testdata.OnRecord(func() {
			output[i].Left, output[i].Right = leftCond, rightCond
		})
		require.Equal(t, output[i].Left, leftCond, comment)
		require.Equal(t, output[i].Right, rightCond, comment)
	}
}

func TestSimplifyOuterJoin(t *testing.T) {
	var (
		input  []string
		output []struct {
			Best     string
			JoinType string
		}
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err, comment)
		planString := ToString(p)
		testdata.OnRecord(func() {
			output[i].Best = planString
		})
		require.Equal(t, output[i].Best, planString, comment)
		join, ok := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		if !ok {
			join, ok = p.(LogicalPlan).Children()[0].Children()[0].(*LogicalJoin)
			require.True(t, ok, comment)
		}
		testdata.OnRecord(func() {
			output[i].JoinType = join.JoinType.String()
		})
		require.Equal(t, output[i].JoinType, join.JoinType.String(), comment)
	}
}

func TestAntiSemiJoinConstFalse(t *testing.T) {
	tests := []struct {
		sql      string
		best     string
		joinType string
	}{
		{
			sql:      "select a from t t1 where not exists (select a from t t2 where t1.a = t2.a and t2.b = 1 and t2.b = 2)",
			best:     "Join{DataScan(t1)->DataScan(t2)}(test.t.a,test.t.a)->Projection",
			joinType: "anti semi join",
		},
	}

	s := createPlannerSuite()
	ctx := context.Background()
	for _, ca := range tests {
		comment := fmt.Sprintf("for %s", ca.sql)
		stmt, err := s.p.ParseOneStmt(ca.sql, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err, comment)
		require.Equal(t, ca.best, ToString(p), comment)
		join, _ := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		require.Equal(t, ca.joinType, join.JoinType.String(), comment)
	}
}

func TestDeriveNotNullConds(t *testing.T) {
	var (
		input  []string
		output []struct {
			Plan  string
			Left  string
			Right string
		}
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain|flagDecorrelate, p.(LogicalPlan))
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {
			output[i].Plan = ToString(p)
		})
		require.Equal(t, output[i].Plan, ToString(p), comment)
		join := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		left := join.Children()[0].(*DataSource)
		right := join.Children()[1].(*DataSource)
		leftConds := fmt.Sprintf("%s", left.pushedDownConds)
		rightConds := fmt.Sprintf("%s", right.pushedDownConds)
		testdata.OnRecord(func() {
			output[i].Left, output[i].Right = leftConds, rightConds
		})
		require.Equal(t, output[i].Left, leftConds, comment)
		require.Equal(t, output[i].Right, rightConds, comment)
	}
}

func TestExtraPKNotNullFlag(t *testing.T) {
	sql := "select count(*) from t3"
	s := createPlannerSuite()
	ctx := context.Background()
	comment := fmt.Sprintf("for %s", sql)
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, comment)
	p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
	require.NoError(t, err, comment)
	ds := p.(*LogicalProjection).children[0].(*LogicalAggregation).children[0].(*DataSource)
	require.Equal(t, "_tidb_rowid", ds.Columns[2].Name.L)
	require.Equal(t, mysql.PriKeyFlag|mysql.NotNullFlag, ds.Columns[2].GetFlag())
	require.Equal(t, mysql.PriKeyFlag|mysql.NotNullFlag, ds.schema.Columns[2].RetType.GetFlag())
}

func buildLogicPlan4GroupBy(s *plannerSuite, t *testing.T, sql string) (Plan, error) {
	sqlMode := s.ctx.GetSessionVars().SQLMode
	mockedTableInfo := MockSignedTable()
	// mock the table info here for later use
	// enable only full group by
	s.ctx.GetSessionVars().SQLMode = sqlMode | mysql.ModeOnlyFullGroupBy
	defer func() { s.ctx.GetSessionVars().SQLMode = sqlMode }() // restore it
	comment := fmt.Sprintf("for %s", sql)
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, comment)

	stmt.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).TableInfo = mockedTableInfo

	p, _, err := BuildLogicalPlanForTest(context.Background(), s.ctx, stmt, s.is)
	return p, err
}

func TestGroupByWhenNotExistCols(t *testing.T) {
	sqlTests := []struct {
		sql              string
		expectedErrMatch string
	}{
		{
			sql:              "select a from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has an as column alias
			sql:              "select a as tempField from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has as table alias
			sql:              "select tempTable.a from t as tempTable group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.tempTable\\.a'.*",
		},
		{
			// has a func call
			sql:              "select length(a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(b + a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) as tempField from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
	}
	s := createPlannerSuite()
	for _, test := range sqlTests {
		sql := test.sql
		p, err := buildLogicPlan4GroupBy(s, t, sql)
		require.Nil(t, p)
		require.Error(t, err)
		require.Regexp(t, test.expectedErrMatch, err.Error())
	}
}

func TestDupRandJoinCondsPushDown(t *testing.T) {
	sql := "select * from t as t1 join t t2 on t1.a > rand() and t1.a > rand()"
	comment := fmt.Sprintf("for %s", sql)
	s := createPlannerSuite()
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, comment)
	p, _, err := BuildLogicalPlanForTest(context.Background(), s.ctx, stmt, s.is)
	require.NoError(t, err, comment)
	p, err = logicalOptimize(context.TODO(), flagPredicatePushDown, p.(LogicalPlan))
	require.NoError(t, err, comment)
	proj, ok := p.(*LogicalProjection)
	require.True(t, ok, comment)
	join, ok := proj.children[0].(*LogicalJoin)
	require.True(t, ok, comment)
	leftPlan, ok := join.children[0].(*LogicalSelection)
	require.True(t, ok, comment)
	leftCond := fmt.Sprintf("%s", leftPlan.Conditions)
	// Condition with mutable function cannot be de-duplicated when push down join conds.
	require.Equal(t, "[gt(cast(test.t.a, double BINARY), rand()) gt(cast(test.t.a, double BINARY), rand())]", leftCond, comment)
}

func TestTablePartition(t *testing.T) {
	definitions := []model.PartitionDefinition{
		{
			ID:       41,
			Name:     model.NewCIStr("p1"),
			LessThan: []string{"16"},
		},
		{
			ID:       42,
			Name:     model.NewCIStr("p2"),
			LessThan: []string{"32"},
		},
		{
			ID:       43,
			Name:     model.NewCIStr("p3"),
			LessThan: []string{"64"},
		},
		{
			ID:       44,
			Name:     model.NewCIStr("p4"),
			LessThan: []string{"128"},
		},
		{
			ID:       45,
			Name:     model.NewCIStr("p5"),
			LessThan: []string{"maxvalue"},
		},
	}
	is := MockPartitionInfoSchema(definitions)
	// is1 equals to is without maxvalue partition.
	definitions1 := make([]model.PartitionDefinition, len(definitions)-1)
	copy(definitions1, definitions)
	is1 := MockPartitionInfoSchema(definitions1)
	isChoices := []infoschema.InfoSchema{is, is1}

	var (
		input []struct {
			SQL   string
			IsIdx int
		}
		output []string
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca.SQL)
		stmt, err := s.p.ParseOneStmt(ca.SQL, "", "")
		require.NoError(t, err, comment)
		testdata.OnRecord(func() {

		})
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, isChoices[ca.IsIdx])
		require.NoError(t, err)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPrunColumns|flagPrunColumnsAgain|flagPredicatePushDown|flagPartitionProcessor, p.(LogicalPlan))
		require.NoError(t, err)
		planString := ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equal(t, output[i], ToString(p), fmt.Sprintf("for %v", ca))
	}
}

func TestSubquery(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for ith, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)

		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagDecorrelate|flagPrunColumns|flagPrunColumnsAgain, lp)
			require.NoError(t, err)
		}
		testdata.OnRecord(func() {
			output[ith] = ToString(p)
		})
		require.Equal(t, output[ith], ToString(p), fmt.Sprintf("for %s %d", ca, ith))
	}
}

func TestPlanBuilder(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, ca := range input {
		comment := fmt.Sprintf("for %s", ca)
		stmt, err := s.p.ParseOneStmt(ca, "", "")
		require.NoError(t, err, comment)

		s.ctx.GetSessionVars().SetHashJoinConcurrency(1)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagPrunColumns|flagPrunColumnsAgain, lp)
			require.NoError(t, err)
		}
		testdata.OnRecord(func() {
			output[i] = ToString(p)
		})
		require.Equal(t, output[i], ToString(p), fmt.Sprintf("for %s", ca))
	}
}

func TestJoinReOrder(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, tt := range input {
		comment := fmt.Sprintf("for %s", tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder, p.(LogicalPlan))
		require.NoError(t, err)
		planString := ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equal(t, output[i], planString, fmt.Sprintf("for %s", tt))
	}
}

func TestEagerAggregation(t *testing.T) {
	var input []string
	var output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	s.ctx.GetSessionVars().AllowAggPushDown = true
	defer func() {
		s.ctx.GetSessionVars().AllowAggPushDown = false
	}()
	for ith, tt := range input {
		comment := fmt.Sprintf("for %s", tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain|flagPushDownAgg, p.(LogicalPlan))
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[ith] = ToString(p)
		})
		require.Equal(t, output[ith], ToString(p), fmt.Sprintf("for %s %d", tt, ith))
	}
}

func TestColumnPruning(t *testing.T) {
	var (
		input  []string
		output []map[int][]string
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:\"%s\"", i, tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		lp, err := logicalOptimize(ctx, flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i] = make(map[int][]string)
		})
		checkDataSourceCols(lp, t, output[i], comment)
	}
}

func TestSortByItemsPruning(t *testing.T) {
	var (
		input  []string
		output [][]string
	)
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	testdata.OnRecord(func() {
		output = make([][]string, len(input))
	})

	s := createPlannerSuite()
	ctx := context.Background()
	for i, tt := range input {
		comment := fmt.Sprintf("for %s", tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		lp, err := logicalOptimize(ctx, flagEliminateProjection|flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err)
		checkOrderByItems(lp, t, &output[i], comment)
	}
}

func TestProjectionEliminator(t *testing.T) {
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select 1+num from (select 1+a as num from t) t1;",
			best: "DataScan(t)->Projection",
		}, {
			sql:  "select count(*) from t where a in (select b from t2 where  a is null);",
			best: "Join{DataScan(t)->Dual->Aggr(firstrow(test.t2.b))}(test.t.a,test.t2.b)->Aggr(count(1))->Projection",
		},
	}

	s := createPlannerSuite()
	ctx := context.Background()
	for ith, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPrunColumns|flagPrunColumnsAgain|flagEliminateProjection, p.(LogicalPlan))
		require.NoError(t, err)
		require.Equal(t, tt.best, ToString(p), fmt.Sprintf("for %s %d", tt.sql, ith))
	}
}

func TestCS3389(t *testing.T) {
	s := createPlannerSuite()
	ctx := context.Background()
	stmt, err := s.p.ParseOneStmt("select count(*) from t where a in (select b from t2 where  a is null);", "", "")
	require.NoError(t, err)
	p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
	require.NoError(t, err)
	p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPrunColumns|flagPrunColumnsAgain|flagEliminateProjection|flagJoinReOrder, p.(LogicalPlan))
	require.NoError(t, err)

	// Assert that all Projection is not empty and there is no Projection between Aggregation and Join.
	proj, isProj := p.(*LogicalProjection)
	require.True(t, isProj)
	require.True(t, len(proj.Exprs) > 0)
	child := proj.Children()[0]
	agg, isAgg := child.(*LogicalAggregation)
	require.True(t, isAgg)
	child = agg.Children()[0]
	_, isJoin := child.(*LogicalJoin)
	require.True(t, isJoin)
}

func TestAllocID(t *testing.T) {
	ctx := MockContext()
	pA := DataSource{}.Init(ctx, 0)
	pB := DataSource{}.Init(ctx, 0)
	require.Equal(t, pB.id, pA.id+1)
}

func checkDataSourceCols(p LogicalPlan, t *testing.T, ans map[int][]string, comment string) {
	switch v := p.(type) {
	case *DataSource, *LogicalUnionAll, *LogicalLimit:
		testdata.OnRecord(func() {
			ans[p.ID()] = make([]string, p.Schema().Len())
		})
		colList, ok := ans[p.ID()]
		require.True(t, ok, fmt.Sprintf("For %s %T ID %d Not found", comment, v, p.ID()))
		require.Equal(t, len(colList), len(p.Schema().Columns), comment)
		for i, col := range p.Schema().Columns {
			testdata.OnRecord(func() {
				colList[i] = col.String()
			})
			require.Equal(t, colList[i], col.String(), comment)
		}
	}
	for _, child := range p.Children() {
		checkDataSourceCols(child, t, ans, comment)
	}
}

func checkOrderByItems(p LogicalPlan, t *testing.T, colList *[]string, comment string) {
	switch p := p.(type) {
	case *LogicalSort:
		testdata.OnRecord(func() {
			*colList = make([]string, len(p.ByItems))
		})
		for i, col := range p.ByItems {
			testdata.OnRecord(func() {
				(*colList)[i] = col.String()
			})
			s := col.String()
			require.Equal(t, (*colList)[i], s, comment)
		}
	}
	children := p.Children()
	require.LessOrEqual(t, len(children), 1, fmt.Sprintf("For %v Expected <= 1 Child", comment))
	for _, child := range children {
		checkOrderByItems(child, t, colList, comment)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		sql string
		err *terror.Error
	}{
		{
			sql: "select date_format((1,2), '%H');",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select cast((1,2) as date)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) between (3,4) and (5,6)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) rlike '1'",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) like '1'",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select case(1,2) when(1,2) then true end",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) in ((3,4),(5,6))",
			err: nil,
		},
		{
			sql: "select row(1,(2,3)) in (select a,b from t)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select row(1,2) in (select a,b from t)",
			err: nil,
		},
		{
			sql: "select (1,2) in ((3,4),5)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) is true",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) is null",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (+(1,2))=(1,2)",
			err: nil,
		},
		{
			sql: "select (-(1,2))=(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2)||(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) < (3,4)",
			err: nil,
		},
		{
			sql: "select (1,2) < 3",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select 1, * from t",
			err: ErrInvalidWildCard,
		},
		{
			sql: "select *, 1 from t",
			err: nil,
		},
		{
			sql: "select 1, t.* from t",
			err: nil,
		},
		{
			sql: "select 1 from t t1, t t2 where t1.a > all((select a) union (select a))",
			err: ErrAmbiguous,
		},
		{
			sql: "insert into t set a = 1, b = a + 1",
			err: nil,
		},
		{
			sql: "insert into t set a = 1, b = values(a) + 1",
			err: nil,
		},
		{
			sql: "select a, b, c from t order by 0",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a, b, c from t order by 4",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a as c1, b as c1 from t order by c1",
			err: ErrAmbiguous,
		},
		{
			sql: "(select a as b, b from t) union (select a, b from t) order by b",
			err: ErrAmbiguous,
		},
		{
			sql: "(select a as b, b from t) union (select a, b from t) order by a",
			err: ErrUnknownColumn,
		},
		{
			sql: "select * from t t1 use index(e)",
			err: ErrKeyDoesNotExist,
		},
		{
			sql: "select a from t having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t group by c2 + 1 having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a as b, b from t having b",
			err: ErrAmbiguous,
		},
		{
			sql: "select a + 1 from t having a",
			err: ErrUnknownColumn,
		},
		{ // issue (#20509)
			sql: "select * from t left join t2 on t.a=t2.a having not (t.a <=> t2.a)",
			err: nil,
		},
		{
			sql: "select a from t having sum(avg(a))",
			err: ErrInvalidGroupFuncUse,
		},
		{
			sql: "select concat(c_str, d_str) from t group by `concat(c_str, d_str)`",
			err: nil,
		},
		{
			sql: "select concat(c_str, d_str) from t group by `concat(c_str,d_str)`",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t b having b.a",
			err: nil,
		},
		{
			sql: "select b.a from t b having b.a",
			err: nil,
		},
		{
			sql: "select b.a from t b having a",
			err: nil,
		},
		{
			sql: "select a+1 from t having t.a",
			err: ErrUnknownColumn,
		},
	}

	s := createPlannerSuite()
	ctx := context.Background()
	for _, tt := range tests {
		sql := tt.sql
		comment := fmt.Sprintf("for %s", sql)
		stmt, err := s.p.ParseOneStmt(sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		_, _, err = BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		if tt.err == nil {
			require.NoError(t, err, comment)
		} else {
			require.True(t, tt.err.Equal(err), comment)
		}
	}
}

func checkUniqueKeys(p LogicalPlan, t *testing.T, ans map[int][][]string, sql string) {
	testdata.OnRecord(func() {
		ans[p.ID()] = make([][]string, len(p.Schema().Keys))
	})
	keyList, ok := ans[p.ID()]
	require.True(t, ok, fmt.Sprintf("for %s, %v not found", sql, p.ID()))
	require.Equal(t, len(keyList), len(p.Schema().Keys), fmt.Sprintf("for %s, %v, the number of key doesn't match, the schema is %s", sql, p.ID(), p.Schema()))
	for i := range keyList {
		testdata.OnRecord(func() {
			keyList[i] = make([]string, len(p.Schema().Keys[i]))
		})
		require.Equal(t, len(keyList[i]), len(p.Schema().Keys[i]), fmt.Sprintf("for %s, %v %v, the number of column doesn't match", sql, p.ID(), keyList[i]))
		for j := range keyList[i] {
			testdata.OnRecord(func() {
				keyList[i][j] = p.Schema().Keys[i][j].String()
			})
			require.Equal(t, keyList[i][j], p.Schema().Keys[i][j].String(), fmt.Sprintf("for %s, %v %v, column dosen't match", sql, p.ID(), keyList[i]))
		}
	}
	testdata.OnRecord(func() {
		ans[p.ID()] = keyList
	})
	for _, child := range p.Children() {
		checkUniqueKeys(child, t, ans, sql)
	}
}

func TestUniqueKeyInfo(t *testing.T) {
	var input []string
	var output []map[int][][]string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	testdata.OnRecord(func() {
		output = make([]map[int][][]string, len(input))
	})

	s := createPlannerSuite()
	ctx := context.Background()
	for ith, tt := range input {
		comment := fmt.Sprintf("for %s %d", tt, ith)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)

		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)
		lp, err := logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo, p.(LogicalPlan))
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[ith] = make(map[int][][]string)
		})
		checkUniqueKeys(lp, t, output[ith], tt)
	}
}

func TestAggPrune(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.Background()
	for i, tt := range input {
		comment := fmt.Sprintf("for %s", tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		domain.GetDomain(s.ctx).MockInfoCacheAndLoadInfoSchema(s.is)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err)

		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagPrunColumnsAgain|flagBuildKeyInfo|flagEliminateAgg|flagEliminateProjection, p.(LogicalPlan))
		require.NoError(t, err)
		planString := ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equal(t, output[i], planString, comment)
	}
}

func TestVisitInfo(t *testing.T) {
	tests := []struct {
		sql string
		ans []visitInfo
	}{
		{
			sql: "insert into t (a) values (1)",
			ans: []visitInfo{
				{mysql.InsertPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "delete from t where a = 1",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "delete from t order by a",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "delete from t",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil, false, "", false},
			},
		},
		/* Not currently supported. See https://github.com/pingcap/tidb/issues/23644
		{
			sql: "delete from t where 1=1",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil, false, "", false},
			},
		},
		*/
		{
			sql: "delete from a1 using t as a1 inner join t as a2 where a1.a = a2.a",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "update t set a = 7 where a = 1",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "update t, (select * from t) a1 set t.a = a1.a;",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "update t a1 set a1.a = a1.a + 1",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "select a, sum(e) from t group by a",
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "truncate table t",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "drop table t",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "create table t (a int)",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "create table t1 like t",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t1", "", nil, false, "", false},
				{mysql.SelectPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "create database test",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "", "", nil, false, "", false},
			},
		},
		{
			sql: "drop database test",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "", "", nil, false, "", false},
			},
		},
		{
			sql: "create index t_1 on t (a)",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "drop index e on t",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: `grant all privileges on test.* to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "", "", nil, false, "", false},
				{mysql.InsertPriv, "test", "", "", nil, false, "", false},
				{mysql.UpdatePriv, "test", "", "", nil, false, "", false},
				{mysql.DeletePriv, "test", "", "", nil, false, "", false},
				{mysql.CreatePriv, "test", "", "", nil, false, "", false},
				{mysql.DropPriv, "test", "", "", nil, false, "", false},
				{mysql.GrantPriv, "test", "", "", nil, false, "", false},
				{mysql.ReferencesPriv, "test", "", "", nil, false, "", false},
				{mysql.LockTablesPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateTMPTablePriv, "test", "", "", nil, false, "", false},
				{mysql.EventPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateRoutinePriv, "test", "", "", nil, false, "", false},
				{mysql.AlterRoutinePriv, "test", "", "", nil, false, "", false},
				{mysql.AlterPriv, "test", "", "", nil, false, "", false},
				{mysql.ExecutePriv, "test", "", "", nil, false, "", false},
				{mysql.IndexPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateViewPriv, "test", "", "", nil, false, "", false},
				{mysql.ShowViewPriv, "test", "", "", nil, false, "", false},
				{mysql.TriggerPriv, "test", "", "", nil, false, "", false},
			},
		},
		{
			sql: `grant all privileges on *.* to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "", "", "", nil, false, "", false},
				{mysql.InsertPriv, "", "", "", nil, false, "", false},
				{mysql.UpdatePriv, "", "", "", nil, false, "", false},
				{mysql.DeletePriv, "", "", "", nil, false, "", false},
				{mysql.CreatePriv, "", "", "", nil, false, "", false},
				{mysql.DropPriv, "", "", "", nil, false, "", false},
				{mysql.ProcessPriv, "", "", "", nil, false, "", false},
				{mysql.ReferencesPriv, "", "", "", nil, false, "", false},
				{mysql.AlterPriv, "", "", "", nil, false, "", false},
				{mysql.ShowDBPriv, "", "", "", nil, false, "", false},
				{mysql.SuperPriv, "", "", "", nil, false, "", false},
				{mysql.ExecutePriv, "", "", "", nil, false, "", false},
				{mysql.IndexPriv, "", "", "", nil, false, "", false},
				{mysql.CreateUserPriv, "", "", "", nil, false, "", false},
				{mysql.CreateTablespacePriv, "", "", "", nil, false, "", false},
				{mysql.TriggerPriv, "", "", "", nil, false, "", false},
				{mysql.CreateViewPriv, "", "", "", nil, false, "", false},
				{mysql.ShowViewPriv, "", "", "", nil, false, "", false},
				{mysql.CreateRolePriv, "", "", "", nil, false, "", false},
				{mysql.DropRolePriv, "", "", "", nil, false, "", false},
				{mysql.CreateTMPTablePriv, "", "", "", nil, false, "", false},
				{mysql.LockTablesPriv, "", "", "", nil, false, "", false},
				{mysql.CreateRoutinePriv, "", "", "", nil, false, "", false},
				{mysql.AlterRoutinePriv, "", "", "", nil, false, "", false},
				{mysql.EventPriv, "", "", "", nil, false, "", false},
				{mysql.ShutdownPriv, "", "", "", nil, false, "", false},
				{mysql.ReloadPriv, "", "", "", nil, false, "", false},
				{mysql.FilePriv, "", "", "", nil, false, "", false},
				{mysql.ConfigPriv, "", "", "", nil, false, "", false},
				{mysql.ReplicationClientPriv, "", "", "", nil, false, "", false},
				{mysql.ReplicationSlavePriv, "", "", "", nil, false, "", false},
				{mysql.GrantPriv, "", "", "", nil, false, "", false},
			},
		},
		{
			sql: `grant select on test.ttt to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "ttt", "", nil, false, "", false},
				{mysql.GrantPriv, "test", "ttt", "", nil, false, "", false},
			},
		},
		{
			sql: `grant select on ttt to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "ttt", "", nil, false, "", false},
				{mysql.GrantPriv, "test", "ttt", "", nil, false, "", false},
			},
		},
		{
			sql: `revoke all privileges on test.* from 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "", "", nil, false, "", false},
				{mysql.InsertPriv, "test", "", "", nil, false, "", false},
				{mysql.UpdatePriv, "test", "", "", nil, false, "", false},
				{mysql.DeletePriv, "test", "", "", nil, false, "", false},
				{mysql.CreatePriv, "test", "", "", nil, false, "", false},
				{mysql.DropPriv, "test", "", "", nil, false, "", false},
				{mysql.GrantPriv, "test", "", "", nil, false, "", false},
				{mysql.ReferencesPriv, "test", "", "", nil, false, "", false},
				{mysql.LockTablesPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateTMPTablePriv, "test", "", "", nil, false, "", false},
				{mysql.EventPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateRoutinePriv, "test", "", "", nil, false, "", false},
				{mysql.AlterRoutinePriv, "test", "", "", nil, false, "", false},
				{mysql.AlterPriv, "test", "", "", nil, false, "", false},
				{mysql.ExecutePriv, "test", "", "", nil, false, "", false},
				{mysql.IndexPriv, "test", "", "", nil, false, "", false},
				{mysql.CreateViewPriv, "test", "", "", nil, false, "", false},
				{mysql.ShowViewPriv, "test", "", "", nil, false, "", false},
				{mysql.TriggerPriv, "test", "", "", nil, false, "", false},
			},
		},
		{
			sql: `revoke connection_admin on *.* from u1`,
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", nil, false, "CONNECTION_ADMIN", true},
			},
		},
		{
			sql: `revoke connection_admin, select on *.* from u1`,
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", nil, false, "CONNECTION_ADMIN", true},
				{mysql.SelectPriv, "", "", "", nil, false, "", false},
				{mysql.GrantPriv, "", "", "", nil, false, "", false},
			},
		},
		{
			sql: `revoke all privileges on *.* FROM u1`,
			ans: []visitInfo{
				{mysql.SelectPriv, "", "", "", nil, false, "", false},
				{mysql.InsertPriv, "", "", "", nil, false, "", false},
				{mysql.UpdatePriv, "", "", "", nil, false, "", false},
				{mysql.DeletePriv, "", "", "", nil, false, "", false},
				{mysql.CreatePriv, "", "", "", nil, false, "", false},
				{mysql.DropPriv, "", "", "", nil, false, "", false},
				{mysql.ProcessPriv, "", "", "", nil, false, "", false},
				{mysql.ReferencesPriv, "", "", "", nil, false, "", false},
				{mysql.AlterPriv, "", "", "", nil, false, "", false},
				{mysql.ShowDBPriv, "", "", "", nil, false, "", false},
				{mysql.SuperPriv, "", "", "", nil, false, "", false},
				{mysql.ExecutePriv, "", "", "", nil, false, "", false},
				{mysql.IndexPriv, "", "", "", nil, false, "", false},
				{mysql.CreateUserPriv, "", "", "", nil, false, "", false},
				{mysql.CreateTablespacePriv, "", "", "", nil, false, "", false},
				{mysql.TriggerPriv, "", "", "", nil, false, "", false},
				{mysql.CreateViewPriv, "", "", "", nil, false, "", false},
				{mysql.ShowViewPriv, "", "", "", nil, false, "", false},
				{mysql.CreateRolePriv, "", "", "", nil, false, "", false},
				{mysql.DropRolePriv, "", "", "", nil, false, "", false},
				{mysql.CreateTMPTablePriv, "", "", "", nil, false, "", false},
				{mysql.LockTablesPriv, "", "", "", nil, false, "", false},
				{mysql.CreateRoutinePriv, "", "", "", nil, false, "", false},
				{mysql.AlterRoutinePriv, "", "", "", nil, false, "", false},
				{mysql.EventPriv, "", "", "", nil, false, "", false},
				{mysql.ShutdownPriv, "", "", "", nil, false, "", false},
				{mysql.ReloadPriv, "", "", "", nil, false, "", false},
				{mysql.FilePriv, "", "", "", nil, false, "", false},
				{mysql.ConfigPriv, "", "", "", nil, false, "", false},
				{mysql.ReplicationClientPriv, "", "", "", nil, false, "", false},
				{mysql.ReplicationSlavePriv, "", "", "", nil, false, "", false},
				{mysql.GrantPriv, "", "", "", nil, false, "", false},
			},
		},
		{
			sql: `set password for 'root'@'%' = 'xxxxx'`,
			ans: []visitInfo{},
		},
		{
			sql: `show create table test.ttt`,
			ans: []visitInfo{
				{mysql.AllPrivMask, "test", "ttt", "", nil, false, "", false},
			},
		},
		{
			sql: "alter table t add column a int(4)",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "rename table t_old to t_new",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t_old", "", nil, false, "", false},
				{mysql.DropPriv, "test", "t_old", "", nil, false, "", false},
				{mysql.CreatePriv, "test", "t_new", "", nil, false, "", false},
				{mysql.InsertPriv, "test", "t_new", "", nil, false, "", false},
			},
		},
		{
			sql: "alter table t_old rename to t_new",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t_old", "", nil, false, "", false},
				{mysql.DropPriv, "test", "t_old", "", nil, false, "", false},
				{mysql.CreatePriv, "test", "t_new", "", nil, false, "", false},
				{mysql.InsertPriv, "test", "t_new", "", nil, false, "", false},
			},
		},
		{
			sql: "alter table t drop partition p0;",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t", "", nil, false, "", false},
				{mysql.DropPriv, "test", "t", "", nil, false, "", false},
			},
		},
		{
			sql: "flush privileges",
			ans: []visitInfo{
				{mysql.ReloadPriv, "", "", "", ErrSpecificAccessDenied, false, "", false},
			},
		},
		{
			sql: "SET GLOBAL wait_timeout=12345",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "SYSTEM_VARIABLES_ADMIN", false},
			},
		},
		{
			sql: "create placement policy x LEARNERS=1",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "PLACEMENT_ADMIN", false},
			},
		},
		{
			sql: "drop placement policy if exists x",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "PLACEMENT_ADMIN", false},
			},
		},
		{
			sql: "BACKUP DATABASE test TO 'local:///tmp/a'",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "BACKUP_ADMIN", false},
			},
		},
		{
			sql: "RESTORE DATABASE test FROM 'local:///tmp/a'",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "RESTORE_ADMIN", false},
			},
		},
		{
			sql: "SHOW BACKUPS",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "BACKUP_ADMIN", false},
			},
		},
		{
			sql: "SHOW RESTORES",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "RESTORE_ADMIN", false},
			},
		},
		{
			sql: "GRANT rolename TO user1",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "ROLE_ADMIN", false},
			},
		},
		{
			sql: "REVOKE rolename FROM user1",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "ROLE_ADMIN", false},
			},
		},
		{
			sql: "GRANT BACKUP_ADMIN ON *.* TO user1",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "BACKUP_ADMIN", true},
			},
		},
		{
			sql: "GRANT BACKUP_ADMIN ON *.* TO user1 WITH GRANT OPTION",
			ans: []visitInfo{
				{mysql.ExtendedPriv, "", "", "", ErrSpecificAccessDenied, false, "BACKUP_ADMIN", true},
			},
		},
		{
			sql: "RENAME USER user1 to user1_tmp",
			ans: []visitInfo{
				{mysql.CreateUserPriv, "", "", "", ErrSpecificAccessDenied, false, "", false},
			},
		},
		{
			sql: "SHOW CONFIG",
			ans: []visitInfo{
				{mysql.ConfigPriv, "", "", "", ErrSpecificAccessDenied, false, "", false},
			},
		},
	}

	s := createPlannerSuite()
	for _, tt := range tests {
		comment := fmt.Sprintf("for %s", tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)

		// TODO: to fix, Table 'test.ttt' doesn't exist
		_ = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		builder.ctx.GetSessionVars().SetHashJoinConcurrency(1)
		_, err = builder.Build(context.TODO(), stmt)
		require.NoError(t, err, comment)

		checkVisitInfo(t, builder.visitInfo, tt.ans, comment)
	}
}

type visitInfoArray []visitInfo

func (v visitInfoArray) Len() int {
	return len(v)
}

func (v visitInfoArray) Less(i, j int) bool {
	if v[i].privilege < v[j].privilege {
		return true
	}
	if v[i].db < v[j].db {
		return true
	}
	if v[i].table < v[j].table {
		return true
	}
	if v[i].column < v[j].column {
		return true
	}

	return false
}

func (v visitInfoArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func unique(v []visitInfo) []visitInfo {
	repeat := 0
	for i := 1; i < len(v); i++ {
		if v[i] == v[i-1] {
			repeat++
		} else {
			v[i-repeat] = v[i]
		}
	}
	return v[:len(v)-repeat]
}

func checkVisitInfo(t *testing.T, v1, v2 []visitInfo, comment string) {
	sort.Sort(visitInfoArray(v1))
	sort.Sort(visitInfoArray(v2))
	v1 = unique(v1)
	v2 = unique(v2)

	require.Equal(t, len(v2), len(v1), comment)
	for i := 0; i < len(v1); i++ {
		// loose compare errors for code match
		require.True(t, terror.ErrorEqual(v1[i].err, v2[i].err), fmt.Sprintf("err1 %v, err2 %v for %s", v1[i].err, v2[i].err, comment))
		// compare remainder
		v1[i].err = v2[i].err
		require.Equal(t, v2[i], v1[i], comment)
	}
}

func TestUnion(t *testing.T) {
	var input []string
	var output []struct {
		Best string
		Err  bool
	}
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		plan, err := builder.Build(ctx, stmt)
		testdata.OnRecord(func() {
			output[i].Err = err != nil
		})
		if output[i].Err {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err, comment)
		p := plan.(LogicalPlan)
		p, err = logicalOptimize(ctx, builder.optFlag, p)
		testdata.OnRecord(func() {
			output[i].Best = ToString(p)
		})
		require.NoError(t, err)
		require.Equal(t, output[i].Best, ToString(p), comment)
	}
}

func TestTopNPushDown(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i] = ToString(p)
		})
		require.Equal(t, output[i], ToString(p), comment)
	}
}

func TestNameResolver(t *testing.T) {
	tests := []struct {
		sql string
		err string
	}{
		{"select a from t", ""},
		{"select c3 from t", "[planner:1054]Unknown column 'c3' in 'field list'"},
		{"select c1 from t4", "[schema:1146]Table 'test.t4' doesn't exist"},
		{"select * from t", ""},
		{"select t.* from t", ""},
		{"select t2.* from t", "[planner:1051]Unknown table 't2'"},
		{"select b as a, c as a from t group by a", "[planner:1052]Column 'a' in group statement is ambiguous"},
		{"select 1 as a, b as a, c as a from t group by a", ""},
		{"select a, b as a from t group by a+1", ""},
		{"select c, a as c from t order by c+1", ""},
		{"select * from t as t1, t as t2 join t as t3 on t2.a = t3.a", ""},
		{"select * from t as t1, t as t2 join t as t3 on t1.c1 = t2.a", "[planner:1054]Unknown column 't1.c1' in 'on clause'"},
		{"select a from t group by a having a = 3", ""},
		{"select a from t group by a having c2 = 3", "[planner:1054]Unknown column 'c2' in 'having clause'"},
		{"select a from t where exists (select b)", ""},
		{"select cnt from (select count(a) as cnt from t group by b) as t2 group by cnt", ""},
		{"select a from t where t11.a < t.a", "[planner:1054]Unknown column 't11.a' in 'where clause'"},
		{"select a from t having t11.c1 < t.a", "[planner:1054]Unknown column 't11.c1' in 'having clause'"},
		{"select a from t where t.a < t.a order by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'order clause'"},
		{"select a from t group by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'group statement'"},
		{"delete a from (select * from t ) as a, t", "[planner:1288]The target table a of the DELETE is not updatable"},
		{"delete b from (select * from t ) as a, t", "[planner:1109]Unknown table 'b' in MULTI DELETE"},
		{"select '' as fakeCol from t group by values(fakeCol)", "[planner:1054]Unknown column '' in 'VALUES() function'"},
		{"update t, (select * from t) as b set b.a = t.a", "[planner:1288]The target table b of the UPDATE is not updatable"},
		{"select row_number() over () from t group by 1", "[planner:1056]Can't group on 'row_number() over ()'"},
		{"select row_number() over () as x from t group by 1", "[planner:1056]Can't group on 'x'"},
		{"select sum(a) as x from t group by 1", "[planner:1056]Can't group on 'x'"},
	}

	s := createPlannerSuite()
	ctx := context.Background()
	for _, test := range tests {
		comment := fmt.Sprintf("for %s", test.sql)
		stmt, err := s.p.ParseOneStmt(test.sql, "", "")
		require.NoError(t, err, comment)
		s.ctx.GetSessionVars().SetHashJoinConcurrency(1)

		_, _, err = BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		if test.err == "" {
			require.NoError(t, err)
		} else {
			require.EqualError(t, err, test.err)
		}
	}
}

func TestOuterJoinEliminator(t *testing.T) {
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)

	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		stmt, err := s.p.ParseOneStmt(tt, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err)
		planString := ToString(p)
		testdata.OnRecord(func() {
			output[i] = planString
		})
		require.Equal(t, output[i], planString, comment)
	}
}

func TestSelectView(t *testing.T) {
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from v",
			best: "DataScan(t)->Projection",
		},
		{
			sql:  "select v.b, v.c, v.d from v",
			best: "DataScan(t)->Projection",
		},
	}
	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		builder, _ := NewPlanBuilder().Init(MockContext(), s.is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err)
		require.Equal(t, tt.best, ToString(p), comment)
	}
}

type plannerSuiteWithOptimizeVars struct {
	*plannerSuite
	optimizeVars map[string]string
}

func TestWindowFunction(t *testing.T) {
	s := new(plannerSuiteWithOptimizeVars)
	s.plannerSuite = createPlannerSuite()

	s.optimizeVars = map[string]string{
		variable.TiDBWindowConcurrency: "1",
	}
	defer func() {
		s.optimizeVars = nil
	}()
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	s.doTestWindowFunction(t, input, output)
}

func TestWindowParallelFunction(t *testing.T) {
	s := new(plannerSuiteWithOptimizeVars)
	s.plannerSuite = createPlannerSuite()

	s.optimizeVars = map[string]string{
		variable.TiDBWindowConcurrency: "4",
	}
	defer func() {
		s.optimizeVars = nil
	}()
	var input, output []string
	planSuiteUnexportedData.GetTestCases(t, &input, &output)
	s.doTestWindowFunction(t, input, output)
}

func (s *plannerSuiteWithOptimizeVars) doTestWindowFunction(t *testing.T, input, output []string) {
	ctx := context.TODO()
	for i, tt := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt)
		p, stmt, err := s.optimize(ctx, tt)
		if err != nil {
			testdata.OnRecord(func() {
				output[i] = err.Error()
			})
			require.EqualError(t, err, output[i], comment)
			continue
		}
		testdata.OnRecord(func() {
			output[i] = ToString(p)
		})
		require.Equal(t, output[i], ToString(p), comment)

		var sb strings.Builder
		// After restore, the result should be the same.
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		require.NoError(t, err)
		p, _, err = s.optimize(ctx, sb.String())
		if err != nil {
			require.EqualError(t, err, output[i], comment)
			continue
		}
		require.Equal(t, output[i], ToString(p), comment)
	}
}

func (s *plannerSuiteWithOptimizeVars) optimize(ctx context.Context, sql string) (PhysicalPlan, ast.Node, error) {
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, nil, err
	}
	err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	if err != nil {
		return nil, nil, err
	}

	sctx := MockContext()
	for k, v := range s.optimizeVars {
		if err = sctx.GetSessionVars().SetSystemVar(k, v); err != nil {
			return nil, nil, err
		}
	}
	builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
	p, err := builder.Build(ctx, stmt)
	if err != nil {
		return nil, nil, err
	}
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	if err != nil {
		return nil, nil, err
	}
	p, _, err = physicalOptimize(p.(LogicalPlan), &PlanCounterDisabled)
	return p.(PhysicalPlan), stmt, err
}

func byItemsToProperty(byItems []*util.ByItems) *property.PhysicalProperty {
	pp := &property.PhysicalProperty{}
	for _, item := range byItems {
		pp.SortItems = append(pp.SortItems, property.SortItem{Col: item.Expr.(*expression.Column), Desc: item.Desc})
	}
	return pp
}

func pathsName(paths []*candidatePath) string {
	var names []string
	for _, path := range paths {
		if path.path.IsTablePath() {
			names = append(names, "PRIMARY_KEY")
		} else {
			names = append(names, path.path.Index.Name.O)
		}
	}
	return strings.Join(names, ",")
}

func TestSkylinePruning(t *testing.T) {
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql:    "select * from t",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where a > 1",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t where a > 1 order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select f from t where f > 1",
			result: "f,f_g",
		},
		{
			sql:    "select f from t where f > 1 order by a",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1 and g > 1",
			result: "PRIMARY_KEY,g,f_g",
		},
		{
			sql:    "select count(1) from t",
			result: "PRIMARY_KEY,c_d_e,f,g,f_g,c_d_e_str,e_d_c_str_prefix",
		},
		{
			sql:    "select * from t where f > 3 and g = 5",
			result: "PRIMARY_KEY,g,f_g",
		},
		{
			sql:    "select * from t where g = 5 order by f",
			result: "PRIMARY_KEY,g,f_g",
		},
		{
			sql:    "select * from t where d = 3 order by c, e",
			result: "PRIMARY_KEY,c_d_e",
		},
		{
			sql:    "select * from t where d = 1 and f > 1 and g > 1 order by c, e",
			result: "PRIMARY_KEY,c_d_e,g,f_g",
		},
	}
	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			require.EqualError(t, err, tt.result, comment)
			continue
		}
		require.NoError(t, err, comment)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err, comment)
		lp := p.(LogicalPlan)
		_, err = lp.recursiveDeriveStats(nil)
		require.NoError(t, err, comment)
		var ds *DataSource
		var byItems []*util.ByItems
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			case *LogicalSort:
				byItems = v.ByItems
				lp = lp.Children()[0]
			case *LogicalProjection:
				newItems := make([]*util.ByItems, 0, len(byItems))
				for _, col := range byItems {
					idx := v.schema.ColumnIndex(col.Expr.(*expression.Column))
					switch expr := v.Exprs[idx].(type) {
					case *expression.Column:
						newItems = append(newItems, &util.ByItems{Expr: expr, Desc: col.Desc})
					}
				}
				byItems = newItems
				lp = lp.Children()[0]
			default:
				lp = lp.Children()[0]
			}
		}
		paths := ds.skylinePruning(byItemsToProperty(byItems))
		require.Equal(t, tt.result, pathsName(paths), comment)
	}
}

func TestFastPlanContextTables(t *testing.T) {
	tests := []struct {
		sql      string
		fastPlan bool
	}{
		{
			"select * from t where a=1",
			true,
		},
		{

			"update t set f=0 where a=43215",
			true,
		},
		{
			"delete from t where a =43215",
			true,
		},
		{
			"select * from t where a>1",
			false,
		},
	}
	s := createPlannerSuite()
	s.ctx.GetSessionVars().SnapshotInfoschema = s.is
	for _, tt := range tests {
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		s.ctx.GetSessionVars().StmtCtx.Tables = nil
		p := TryFastPlan(s.ctx, stmt)
		if tt.fastPlan {
			require.NotNil(t, p)
			require.Equal(t, 1, len(s.ctx.GetSessionVars().StmtCtx.Tables))
			require.Equal(t, "t", s.ctx.GetSessionVars().StmtCtx.Tables[0].Table)
			require.Equal(t, "test", s.ctx.GetSessionVars().StmtCtx.Tables[0].DB)
		} else {
			require.Nil(t, p)
			require.Equal(t, 0, len(s.ctx.GetSessionVars().StmtCtx.Tables))
		}
	}
}

func TestUpdateEQCond(t *testing.T) {
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select t1.a from t t1, t t2 where t1.a = t2.a+1",
			best: "Join{DataScan(t1)->DataScan(t2)->Projection}(test.t.a,Column#25)->Projection->Projection",
		},
	}
	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err)
		sctx := MockContext()
		builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
		domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
		p, err := builder.Build(ctx, stmt)
		require.NoError(t, err)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		require.NoError(t, err)
		require.Equal(t, tt.best, ToString(p), comment)
	}
}

func TestConflictedJoinTypeHints(t *testing.T) {
	sql := "select /*+ INL_JOIN(t1) HASH_JOIN(t1) */ * from t t1, t t2 where t1.e = t2.e"
	s := createPlannerSuite()
	ctx := context.TODO()
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err)
	sctx := MockContext()
	builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
	p, err := builder.Build(ctx, stmt)
	require.NoError(t, err)
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	require.NoError(t, err)
	proj, ok := p.(*LogicalProjection)
	require.True(t, ok)
	join, ok := proj.Children()[0].(*LogicalJoin)
	require.True(t, ok)
	require.Nil(t, join.hintInfo)
	require.Equal(t, uint(0), join.preferJoinType)
}

func TestSimplyOuterJoinWithOnlyOuterExpr(t *testing.T) {
	s := createPlannerSuite()
	sql := "select * from t t1 right join t t0 ON TRUE where CONCAT_WS(t0.e=t0.e, 0, NULL) IS NULL"
	ctx := context.TODO()
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err)
	sctx := MockContext()
	builder, _ := NewPlanBuilder().Init(sctx, s.is, &hint.BlockHintProcessor{})
	domain.GetDomain(sctx).MockInfoCacheAndLoadInfoSchema(s.is)
	p, err := builder.Build(ctx, stmt)
	require.NoError(t, err)
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	require.NoError(t, err)
	proj, ok := p.(*LogicalProjection)
	require.True(t, ok)
	join, ok := proj.Children()[0].(*LogicalJoin)
	require.True(t, ok)
	// previous wrong JoinType is InnerJoin
	require.Equal(t, RightOuterJoin, join.JoinType)
}

func TestResolvingCorrelatedAggregate(t *testing.T) {
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select (select count(a)) from t",
			best: "Apply{DataScan(t)->Aggr(count(test.t.a))->Dual->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select (select count(n.a) from t) from t n",
			best: "Apply{DataScan(n)->Aggr(count(test.t.a))->DataScan(t)->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select (select sum(count(a))) from t",
			best: "Apply{DataScan(t)->Aggr(count(test.t.a))->Dual->Aggr(sum(Column#13))->MaxOneRow}->Projection",
		},
		{
			sql:  "select (select sum(count(n.a)) from t) from t n",
			best: "Apply{DataScan(n)->Aggr(count(test.t.a))->DataScan(t)->Aggr(sum(Column#25))->MaxOneRow}->Projection",
		},
		{
			sql:  "select (select cnt from (select count(a) as cnt) n) from t",
			best: "Apply{DataScan(t)->Aggr(count(test.t.a))->Dual->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select sum(a), sum(a), count(a), (select count(a)) from t",
			best: "Apply{DataScan(t)->Aggr(sum(test.t.a),count(test.t.a))->Dual->Projection->MaxOneRow}->Projection",
		},
	}

	s := createPlannerSuite()
	ctx := context.TODO()
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagEliminateProjection|flagPrunColumns|flagPrunColumnsAgain, p.(LogicalPlan))
		require.NoError(t, err, comment)
		require.Equal(t, tt.best, ToString(p), comment)
	}
}

func TestFastPathInvalidBatchPointGet(t *testing.T) {
	// #22040
	tt := []struct {
		sql      string
		fastPlan bool
	}{
		{
			// column count doesn't match, not use idx
			sql:      "select * from t where (a,b) in ((1,2),1)",
			fastPlan: false,
		},
		{
			// column count doesn't match, not use idx
			sql:      "select * from t where (a,b) in (1,2)",
			fastPlan: false,
		},
		{
			// column count doesn't match, use idx
			sql:      "select * from t where (f,g) in ((1,2),1)",
			fastPlan: false,
		},
		{
			// column count doesn't match, use idx
			sql:      "select * from t where (f,g) in (1,2)",
			fastPlan: false,
		},
	}
	s := createPlannerSuite()
	for i, tc := range tt {
		comment := fmt.Sprintf("case:%v sql:%s", i, tc.sql)
		stmt, err := s.p.ParseOneStmt(tc.sql, "", "")
		require.NoError(t, err, comment)
		err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
		require.NoError(t, err, comment)
		plan := TryFastPlan(s.ctx, stmt)
		if tc.fastPlan {
			require.NotNil(t, plan)
		} else {
			require.Nil(t, plan)
		}
	}
}

func TestTraceFastPlan(t *testing.T) {
	s := createPlannerSuite()
	s.ctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = true
	defer func() {
		s.ctx.GetSessionVars().StmtCtx.EnableOptimizeTrace = false
	}()
	s.ctx.GetSessionVars().SnapshotInfoschema = s.is
	sql := "select * from t where a=1"
	comment := fmt.Sprintf("sql:%s", sql)
	stmt, err := s.p.ParseOneStmt(sql, "", "")
	require.NoError(t, err, comment)
	err = Preprocess(s.ctx, stmt, WithPreprocessorReturn(&PreprocessorReturn{InfoSchema: s.is}))
	require.NoError(t, err, comment)
	plan := TryFastPlan(s.ctx, stmt)
	require.NotNil(t, plan)
	require.NotNil(t, s.ctx.GetSessionVars().StmtCtx.OptimizeTracer)
	require.NotNil(t, s.ctx.GetSessionVars().StmtCtx.OptimizeTracer.FinalPlan)
	require.True(t, s.ctx.GetSessionVars().StmtCtx.OptimizeTracer.IsFastPlan)
}

func TestWindowLogicalPlanAmbiguous(t *testing.T) {
	sql := "select a, max(a) over(), sum(a) over() from t"
	var planString string
	// The ambiguous logical plan which contains window function can usually be found in 100 iterations.
	iterations := 100
	s := createPlannerSuite()
	for i := 0; i < iterations; i++ {
		stmt, err := s.p.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		p, _, err := BuildLogicalPlanForTest(context.Background(), s.ctx, stmt, s.is)
		require.NoError(t, err)
		if planString == "" {
			planString = ToString(p)
		} else {
			require.Equal(t, ToString(p), planString)
		}
	}
}

func TestRemoveOrderbyInSubquery(t *testing.T) {
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t order by a",
			best: "DataScan(t)->Projection->Sort",
		},
		{
			sql:  "select (select 1) from t order by a",
			best: "DataScan(t)->Projection->Sort->Projection",
		},
		{
			sql:  "select count(*) from (select b from t order by a) n",
			best: "DataScan(t)->Projection->Projection->Aggr(count(1),firstrow(test.t.b))->Projection",
		},
		{
			sql:  "select count(1) from (select b from t order by a limit 1) n",
			best: "DataScan(t)->Projection->Sort->Limit->Projection->Aggr(count(1),firstrow(test.t.b))->Projection",
		},
	}

	s := createPlannerSuite()
	s.ctx.GetSessionVars().RemoveOrderbyInSubquery = true
	ctx := context.TODO()
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.p.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, comment)
		p, _, err := BuildLogicalPlanForTest(ctx, s.ctx, stmt, s.is)
		require.NoError(t, err, comment)
		require.Equal(t, tt.best, ToString(p), comment)
	}
}
