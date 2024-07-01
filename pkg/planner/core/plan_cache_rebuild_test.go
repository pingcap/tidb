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

	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
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

	// PointPlan
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where a=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where d=?'`,
		`set @a1=1, @a2=2`, `execute st using @a1`, `execute st using @a2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where a in (?,?)'`,
		`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
	testCachedPlanClone(t, tk, `prepare st from 'select * from t where d in (?,?)'`,
		`set @a1=1,@b1=1, @a2=2,@b2=2`, `execute st using @a1,@b1`, `execute st using @a2,@b2`)
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
		cloned, ok := original.CloneForPlanCache()
		require.True(t, ok)
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
		cloned, ok := cachedVal.Plan.CloneForPlanCache()
		require.True(t, ok)
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
