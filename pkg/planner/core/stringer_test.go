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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
)

func TestPlanStringer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index idx(a))")
	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "show columns from t like 'a'",
			plan: "Show(field:[a])",
		},
		{
			sql:  "show columns from t like 'a%'",
			plan: "Show(field_pattern:[a%])",
		},
		{
			sql:  "show columns from t where field = 'a'",
			plan: "Show->Sel([eq(Column#13, a)])->Projection",
		},
		{
			sql:  "desc t",
			plan: "Show",
		},
		{
			sql:  "desc t a",
			plan: "Show(field:[a])",
		},
		{
			sql:  "show tables in test like 't'",
			plan: "Show(table:[t])",
		},
		{
			sql:  "show tables in test like 'T'",
			plan: "Show(table:[t])",
		},
		{
			sql:  "show tables in test like 't%'",
			plan: "Show(table_pattern:[t%])",
		},
		{
			sql:  "show tables in test like '%T%'",
			plan: "Show(table_pattern:[%t%])",
		},
		{
			sql:  "show databases like 't'",
			plan: "Show(database:[t])",
		},
		{
			sql:  "show databases like 'T'",
			plan: "Show(database:[t])",
		},
		{
			sql:  "show databases like 't%'",
			plan: "Show(database_pattern:[t%])",
		},
		{
			sql:  "show databases like '%T%'",
			plan: "Show(database_pattern:[%t%])",
		},
		{
			sql:  "show table status in test like 'T%'",
			plan: "Show(table_pattern:[t%])",
		},
		{
			sql:  "show table status in test like '%T%'",
			plan: "Show(table_pattern:[%t%])",
		},
		{
			sql:  "show collation like 't'",
			plan: "Show(collation:[t])",
		},
		{
			sql:  "show collation like 'T'",
			plan: "Show(collation:[t])",
		},
		{
			sql:  "show collation like 't%'",
			plan: "Show(collation_pattern:[t%])",
		},
		{
			sql:  "show collation like '%T%'",
			plan: "Show(collation_pattern:[%t%])",
		},
	}
	parser := parser.New()
	for _, tt := range tests {
		stmt, err := parser.ParseOneStmt(tt.sql, "", "")
		require.NoError(t, err, "for %s", tt.sql)
		ret := &core.PreprocessorReturn{}
		builder, _ := core.NewPlanBuilder().Init(tk.Session().GetPlanCtx(), ret.InfoSchema, hint.NewQBHintHandler(nil))
		p, err := builder.Build(context.TODO(), stmt)
		require.NoError(t, err, "for %s", tt.sql)
		p, err = core.LogicalOptimize(context.TODO(), builder.GetOptFlag(), p.(base.LogicalPlan))
		require.NoError(t, err, "for %s", tt.sql)
		require.Equal(t, tt.plan, core.ToString(p))
	}
}
