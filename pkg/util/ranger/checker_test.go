// Copyright 2025 PingCAP, Inc.
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

package ranger_test

import (
	"context"
	"strings"
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestNoNullIndexChecker(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(60), c decimal(10, 2), d tinyint(2))")

	cases := []struct {
		column   string
		exprStrs []string
		expected bool
	}{
		{
			column:   "a",
			exprStrs: []string{"a = 1"},
			expected: true,
		},
		{
			column:   "a",
			exprStrs: []string{"a is NULL", "a is not NULL"},
			expected: true,
		},
		{
			column:   "a",
			exprStrs: []string{"a = NULL"},
			expected: true,
		},
		{
			column:   "a",
			exprStrs: []string{"a = 1", "b is not NULL"},
			expected: true,
		},
		{
			column:   "a",
			exprStrs: []string{"b is not NULL"},
			expected: false,
		},
		{
			column:   "b",
			exprStrs: []string{"b = 'test'"},
			expected: true,
		},
		{
			column:   "b",
			exprStrs: []string{"b is NULL"},
			expected: false,
		},
		{
			column:   "c",
			exprStrs: []string{"c > 0", "c < 100"},
			expected: true,
		},
		{
			column:   "c",
			exprStrs: []string{"c is NULL", "c > 0"},
			expected: true,
		},
		{
			column:   "d",
			exprStrs: []string{"d = 1"},
			expected: true,
		},
		{
			column:   "d",
			exprStrs: []string{"d is not NULL", "d > 0"},
			expected: true,
		},
		{
			column:   "d",
			exprStrs: []string{"d is NULL", "d = 1"},
			expected: true,
		},
	}

	for _, c := range cases {
		t.Run(strings.Join(c.exprStrs, ","), func(t *testing.T) {
			// it's a workaround to create `expression.Column` and `expression.Expression` for conditions
			// and make sure they have the same `UniqueID` for each columns
			sql := "select " + c.column + " from t where (" + strings.Join(c.exprStrs, ") and (") + ")"
			stmts, err := session.Parse(tk.Session(), sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			ret := &plannercore.PreprocessorReturn{}
			err = plannercore.Preprocess(context.Background(), tk.Session(), stmts[0], plannercore.WithPreprocessorReturn(ret))
			require.NoError(t, err)
			p, err := plannercore.BuildLogicalPlanForTest(context.Background(), tk.Session(), stmts[0], ret.InfoSchema)
			require.NoError(t, err)
			projection := p.(*plannercore.LogicalProjection)
			column := projection.Schema().Columns[0]
			selection := projection.Children()[0].(*plannercore.LogicalSelection)

			result := ranger.CheckColumnIsNotNullWithCNFConditions(tk.Session().GetPlanCtx(), column, selection.Conditions)
			require.Equal(t, c.expected, result)
		})
	}
}
