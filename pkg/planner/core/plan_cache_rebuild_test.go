// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheClone(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.Session().GetSessionVars().EnableInstancePlanCache = true
	tk2.Session().GetSessionVars().EnableInstancePlanCache = true
	tk1.MustExec(`use test`)
	tk2.MustExec(`use test`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, primary key(a), key(b), unique key(d))`)

	for i := -20; i < 20; i++ {
		tk1.MustExec(fmt.Sprintf("insert into t values (%v,%v,%v,%v)", i, rand.Intn(20), rand.Intn(20), -i))
	}

	// TableScan
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where a<?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where a>=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t use index(primary) where a<? and b<?'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t use index(primary) where a<? and b+?=10'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select a+b from t where a<?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)

	// IndexScan
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select b from t use index(b) where b<=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select b from t use index(b) where b>?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select b+a*2 from t use index(b) where b>?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t use index(b) where a<? and b<?'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t use index(b) where a<? and b+?=10'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)

	// TODO: PointGet doesn't support Clone
	// PointPlan
	//testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where a=?'`,
	//	`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	//testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where d=?'`,
	//	`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	//testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where a in (?,?)'`,
	//	`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	//testCachedPlanClone(t, tk1, tk2, `prepare st from 'select * from t where d in (?,?)'`,
	//	`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
}

func testCachedPlanClone(t *testing.T, tk1, tk2 *testkit.TestKit, prep, set, exec1, exec2 string) {
	tk1.MustExec(prep)
	tk1.MustExec(set)
	tk1.MustQuery(exec1) // generate the first cached plan

	tk2.MustExec(prep)
	tk2.MustExec(set)
	checked := false
	ctx := context.WithValue(context.Background(), core.PlanCacheKeyTestClone{}, func(plan, cloned base.Plan) {
		checked = true
		// TODO: check cloned is deeply cloned from plan.
	})
	tk2.MustQueryWithContext(ctx, exec2)
	require.True(t, checked)
}

func TestCheckPlanDeepClone(t *testing.T) {
	// totally same pointer
	ts1 := &core.PhysicalTableScan{}
	require.Equal(t, core.CheckPlanDeepClone(ts1, ts1).Error(), "same pointer, path *PhysicalTableScan")

	// share the same slice
	ts2 := &core.PhysicalTableScan{}
	ts1.AccessCondition = make([]expression.Expression, 10)
	ts2.AccessCondition = ts1.AccessCondition
	require.Equal(t, core.CheckPlanDeepClone(ts1, ts2).Error(), "same slice pointers, path *PhysicalTableScan.AccessCondition")

	// same slice element
	ts2.AccessCondition = make([]expression.Expression, 10)
	expr := &expression.Column{}
	ts1.AccessCondition[0] = expr
	ts2.AccessCondition[0] = expr
	require.Equal(t, core.CheckPlanDeepClone(ts1, ts2).Error(), "same pointer, path *PhysicalTableScan.AccessCondition[0]")

	// same slice[0].pointer.pointer
	ts2.AccessCondition[0] = new(expression.Column)
	ts1.AccessCondition[0].(*expression.Column).RetType = new(types.FieldType)
	ts2.AccessCondition[0].(*expression.Column).RetType = ts1.AccessCondition[0].(*expression.Column).RetType
	require.Equal(t, core.CheckPlanDeepClone(ts1, ts2).Error(), "same pointer, path *PhysicalTableScan.AccessCondition[0].RetType")

	// same interface
	child := &core.PhysicalTableScan{}
	ts1.SetProbeParents([]base.PhysicalPlan{child})
	ts2.SetProbeParents([]base.PhysicalPlan{child})
	require.Equal(t, core.CheckPlanDeepClone(ts1, ts2).Error(), "same pointer, path *PhysicalTableScan.physicalSchemaProducer.basePhysicalPlan.probeParents[0]")

	// same map
	l1 := &core.PhysicalLock{}
	l2 := &core.PhysicalLock{}
	l1.TblID2Handle = make(map[int64][]util.HandleCols)
	l2.TblID2Handle = l1.TblID2Handle
	require.Equal(t, core.CheckPlanDeepClone(l1, l2).Error(), "same map pointers, path *PhysicalLock.TblID2Handle")

	// same pointer in map
	l2.TblID2Handle = make(map[int64][]util.HandleCols)
	cols := make([]util.HandleCols, 10)
	l1.TblID2Handle[1] = cols
	l2.TblID2Handle[1] = cols
	require.Equal(t, core.CheckPlanDeepClone(l1, l2).Error(), "same slice pointers, path *PhysicalLock.TblID2Handle[int64]")

	// same sctx
	l1.TblID2Handle[1] = nil
	l2.TblID2Handle[1] = nil
	ctx := core.MockContext()
	defer ctx.Close()
	l1.SetSCtx(ctx)
	l2.SetSCtx(ctx)
	require.Equal(t, core.CheckPlanDeepClone(l1, l2).Error(), "same pointer, path *PhysicalLock.basePhysicalPlan.Plan.ctx")
}
