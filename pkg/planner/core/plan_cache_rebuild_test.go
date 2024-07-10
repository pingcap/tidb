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
	"encoding/json"
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
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, primary key(a), key(b), unique key(d))`)

	for i := -20; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v,%v,%v,%v)", i, rand.Intn(20), rand.Intn(20), -i))
	}

	// TableScan
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where a<?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where a>=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t use index(primary) where a<? and b<?'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t use index(primary) where a<? and b+?=10'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk, `prepare st from 'select a+b, b+? from t where a<?'`,
		`set @a1=1,@b1=a,@a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)

	// IndexScan
	testCachedPlanClone(t, tk, `prepare st from 'select b from t use index(b) where b<=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select b from t use index(b) where b>?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select b+a*2 from t use index(b) where b>?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t use index(b) where a<? and b<?'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t use index(b) where a<? and b+?=10'`,
		`set @a1=1, @b1=1, @a2=2, @b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)

	// TODO: PointGet doesn't support Clone
	// PointPlan
	//testCachedPlanClone(t, tk, `prepare st from 'select * from t where a=?'`,
	//	`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	//testCachedPlanClone(t, tk, `prepare st from 'select * from t where d=?'`,
	//	`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	//testCachedPlanClone(t, tk, `prepare st from 'select * from t where a in (?,?)'`,
	//	`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	//testCachedPlanClone(t, tk, `prepare st from 'select * from t where d in (?,?)'`,
	//	`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
}

func testCachedPlanClone(t *testing.T, tk *testkit.TestKit, prep, set, exec1, exec2 string) {
	tk.MustExec(prep)
	tk.MustExec(set)
	tk.MustQuery(exec1) // generate the first cached plan

	// check adjusting the cloned plan should have no effect on the original plan
	var original base.Plan
	var originalFingerprint string
	before := func(cachedVal *core.PlanCacheValue) {
		// get the current cached plan and its fingerprint
		original, originalFingerprint = cachedVal.Plan, planFingerprint(t, cachedVal.Plan)
		// replace the cached plan with a cloned one
		cloned, err := original.(base.PhysicalPlan).Clone(original.SCtx())
		require.NoError(t, err)
		cachedVal.Plan = cloned
	}
	after := func(cachedVal *core.PlanCacheValue) {
		cloned := cachedVal.Plan
		require.True(t, originalFingerprint != planFingerprint(t, cloned))   // this cloned one have been adjusted by the optimizer
		require.True(t, originalFingerprint == planFingerprint(t, original)) // the prior one should keep unchanged
	}
	ctx := context.WithValue(context.Background(), core.PlanCacheKeyTestBeforeAdjust{}, before)
	ctx = context.WithValue(ctx, core.PlanCacheKeyTestAfterAdjust{}, after)
	tk.MustQueryWithContext(ctx, exec2)

	// check the cloned plan should have the same result as the original plan
	originalRes := tk.MustQuery(exec2).Sort()
	clonePlan := func(cachedVal *core.PlanCacheValue) {
		cloned, err := original.(base.PhysicalPlan).Clone(original.SCtx())
		require.NoError(t, err)
		cachedVal.Plan = cloned
	}
	ctx = context.WithValue(context.Background(), core.PlanCacheKeyTestBeforeAdjust{}, clonePlan)
	clonedRes := tk.MustQueryWithContext(ctx, exec2)
	originalRes.Equal(clonedRes.Sort().Rows())
}

func planFingerprint(t *testing.T, p base.Plan) string {
	switch x := p.(type) {
	case base.PhysicalPlan:
		return physicalPlanFingerprint(t, x)
	default:
		// TODO: support Update/Insert/Delete plan
		t.Fatalf("unexpected plan type %T", x)
		return ""
	}
}

func physicalPlanFingerprint(t *testing.T, p base.PhysicalPlan) string {
	v, err := json.Marshal(p)
	require.NoError(t, err)
	childPrints := make([]string, len(p.Children()))
	for i, child := range p.Children() {
		childPrints[i] = physicalPlanFingerprint(t, child)
	}
	return fmt.Sprintf("%s(%s)", string(v), childPrints)
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
