package core_test

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExplainedPhysicalPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var input []string
	var output []struct {
		SQL  string
		Main []*core.ExplainedPhysicalOperator
		CTEs [][]*core.ExplainedPhysicalOperator
	}
	planSuiteData := core.GetExplainedSuiteData()
	planSuiteData.GetTestCases(t, &input, &output)
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})

	for i, test := range input {
		comment := fmt.Sprintf("case:%v sql:%s", i, test)
		stmt, err := p.ParseOneStmt(test, "", "")
		require.NoError(t, err, comment)
		p, _, err := planner.Optimize(context.Background(), tk.Session(), stmt, is)
		require.NoError(t, err, comment)

		explained := core.ExplainPhysicalPlan(p, tk.Session())
		for _, op := range explained.Main {
			op.Origin = nil
		}
		for _, cte := range explained.CTEs {
			for _, op := range cte {
				op.Origin = nil
			}
		}

		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Main = explained.Main
			output[i].CTEs = explained.CTEs
		})
		require.Equal(t, output[i].Main, explained.Main)
		require.Equal(t, output[i].CTEs, explained.CTEs)
	}
}
