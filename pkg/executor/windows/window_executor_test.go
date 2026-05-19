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

package windows_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	windowexec "github.com/pingcap/tidb/pkg/executor/windows"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestWindowExecutorsBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer tk.MustExec("set @@tidb_enable_window_function = 0")

	for _, pipelined := range []int{0, 1} {
		tk.MustExec(fmt.Sprintf("set @@tidb_enable_pipelined_window_function = %d", pipelined))
		// The window's ORDER BY only defines numbering inside each partition. Add an
		// outer ORDER BY so the result set order is deterministic across executors.
		tk.MustQuery("select a, row_number() over(partition by a order by b) as rn from t order by a, rn").
			Check(testkit.Rows("1 1", "1 2", "2 1", "2 2"))
		tk.MustQuery("select a, sum(b) over(order by a, b rows between 1 preceding and current row) as s from t order by a, b").
			Check(testkit.Rows("1 1", "1 3", "2 3", "2 3"))
	}
}

func TestBuildStreamWindowExec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_pipelined_window_function = 0")
	sctx := tk.Session()

	colA := &expression.Column{Index: 0, UniqueID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colB := &expression.Column{Index: 1, UniqueID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}
	childSchema := expression.NewSchema(colA, colB)
	childExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        sctx,
		DataSchema: childSchema,
		Ndvs:       []int{-2, -2},
		Datums: [][]any{
			{int64(1), int64(1), int64(2), int64(2)},
			{int64(1), int64(2), int64(1), int64(2)},
		},
		Rows: 4,
	})
	childExec.PrepareChunks()

	windowFunc, err := aggregation.NewWindowFuncDesc(sctx.GetExprCtx(), ast.WindowFuncRowNumber, nil, false)
	require.NoError(t, err)
	windowSchema := childSchema.Clone()
	windowSchema.Append(&expression.Column{Index: 2, UniqueID: 3, RetType: types.NewFieldType(mysql.TypeLonglong)})
	windowPlan := physicalWindowForTest(sctx.GetPlanCtx(), windowSchema, colA, colB, windowFunc)

	streamExec, err := windowexec.BuildStream(sctx, windowPlan, childExec)
	require.NoError(t, err)
	require.IsType(t, &windowexec.StreamWindowExec{}, streamExec)

	ctx := context.Background()
	require.NoError(t, streamExec.Open(ctx))
	defer func() {
		require.NoError(t, streamExec.Close())
	}()

	chk := exec.NewFirstChunk(streamExec)
	rows := make([]string, 0, 4)
	for {
		require.NoError(t, streamExec.Next(ctx, chk))
		if chk.NumRows() == 0 {
			break
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			rows = append(rows, fmt.Sprintf("%d %d %d", row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)))
		}
	}
	require.Equal(t, []string{"1 1 1", "1 2 2", "2 1 1", "2 2 2"}, rows)
}

func physicalWindowForTest(
	sctx base.PlanContext,
	schema *expression.Schema,
	partitionCol *expression.Column,
	orderCol *expression.Column,
	windowFunc *aggregation.WindowFuncDesc,
) *physicalop.PhysicalWindow {
	windowPlan := physicalop.PhysicalWindow{
		WindowFuncDescs: []*aggregation.WindowFuncDesc{windowFunc},
		PartitionBy:     []property.SortItem{{Col: partitionCol}},
		OrderBy:         []property.SortItem{{Col: orderCol}},
		Frame: &logicalop.WindowFrame{
			Type:  ast.Rows,
			Start: &logicalop.FrameBound{Type: ast.CurrentRow},
			End:   &logicalop.FrameBound{Type: ast.CurrentRow},
		},
	}.Init(sctx, nil, 0)
	windowPlan.SetSchema(schema)
	return windowPlan
}

func TestWindowReturnColumnNullableAttribute(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer tk.MustExec("set @@tidb_enable_window_function = 0")
	tk.MustExec("drop table if exists agg")
	tk.MustExec("create table agg(p int not null, o int not null, v int not null)")
	tk.MustExec("insert into agg values (0,0,1), (1,1,2), (1,2,3), (1,3,4)")

	checkNullable := func(funcName string, isNullable bool) {
		rs, err := tk.Exec(fmt.Sprintf("select %s over (partition by p order by o rows between 1 preceding and 1 following) as a from agg", funcName))
		tk.RequireNoError(err)
		retField := rs.Fields()[0]
		if isNullable {
			tk.RequireNotEqual(mysql.NotNullFlag, retField.Column.FieldType.GetFlag()&mysql.NotNullFlag)
		} else {
			tk.RequireEqual(mysql.NotNullFlag, retField.Column.FieldType.GetFlag()&mysql.NotNullFlag)
		}
		tk.RequireNoError(rs.Close())
	}

	checkNullable("sum(v)", true)
	checkNullable("count(v)", false)
	checkNullable("row_number()", false)
	checkNullable("rank()", false)
	checkNullable("dense_rank()", false)
}
