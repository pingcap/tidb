// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
	"testing"
)

// ExplainedPhysicalOperatorForTest contains fields of ExplainedOperator that is needed for tests.
type ExplainedPhysicalOperatorForTest struct {
	ExplainID          string
	TextTreeExplainID  string
	Depth              uint64
	DriverSide         core.DriverSide
	IsRoot             bool
	StoreType          kv.StoreType
	ReqType            core.ReadReqType
	StatsInfoAvailable bool
	EstRows            float64
	ExplainInfo        string
	IsPhysicalPlan     bool
	EstCost            float64
}

func simplifyExplainedPhysicalOperator(e *core.ExplainedOperator) *ExplainedPhysicalOperatorForTest {
	return &ExplainedPhysicalOperatorForTest{
		ExplainID:          e.ExplainID,
		TextTreeExplainID:  e.TextTreeExplainID,
		Depth:              e.Depth,
		DriverSide:         e.DriverSide,
		IsRoot:             e.IsRoot,
		StoreType:          e.StoreType,
		ReqType:            e.ReqType,
		StatsInfoAvailable: e.StatsInfoAvailable,
		EstRows:            e.EstRows,
		ExplainInfo:        e.ExplainInfo,
		IsPhysicalPlan:     e.IsPhysicalPlan,
		EstCost:            e.EstCost,
	}
}

func simplifyExplainedPlan(p []*core.ExplainedOperator) []*ExplainedPhysicalOperatorForTest {
	var res []*ExplainedPhysicalOperatorForTest
	for _, op := range p {
		res = append(res, simplifyExplainedPhysicalOperator(op))
	}
	return res
}

func TestExplainedPhysicalPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	var input []string
	var output []struct {
		SQL  string
		Main []*ExplainedPhysicalOperatorForTest
		CTEs [][]*ExplainedPhysicalOperatorForTest
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
		main := simplifyExplainedPlan(explained.Main)
		var ctes [][]*ExplainedPhysicalOperatorForTest
		for _, cte := range explained.CTEs {
			ctes = append(ctes, simplifyExplainedPlan(cte))
		}

		testdata.OnRecord(func() {
			output[i].SQL = test
			output[i].Main = main
			output[i].CTEs = ctes
		})
		require.Equal(t, output[i].Main, main)
		require.Equal(t, output[i].CTEs, ctes)
	}
}
