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
	"reflect"
	"testing"
	"unsafe"

	"github.com/pingcap/errors"
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

func TestCheckPlanClone(t *testing.T) {
	// totally same pointer
	ts1 := &core.PhysicalTableScan{}
	require.Equal(t, checkUnclearPlanCacheClone(ts1, ts1).Error(), "same pointer, path *core.PhysicalTableScan")

	// share the same slice
	ts2 := &core.PhysicalTableScan{}
	ts1.AccessCondition = make([]expression.Expression, 10)
	ts2.AccessCondition = ts1.AccessCondition
	require.Equal(t, checkUnclearPlanCacheClone(ts1, ts2).Error(), "same slice pointers, path *core.PhysicalTableScan.AccessCondition")

	// same slice element
	ts2.AccessCondition = make([]expression.Expression, 10)
	expr := &expression.Column{}
	ts1.AccessCondition[0] = expr
	ts2.AccessCondition[0] = expr
	require.Equal(t, checkUnclearPlanCacheClone(ts1, ts2).Error(), "same pointer, path *core.PhysicalTableScan.AccessCondition[0](*expression.Column)")

	// same slice[0].pointer.pointer
	ts2.AccessCondition[0] = new(expression.Column)
	ts1.AccessCondition[0].(*expression.Column).RetType = new(types.FieldType)
	ts2.AccessCondition[0].(*expression.Column).RetType = ts1.AccessCondition[0].(*expression.Column).RetType
	require.Equal(t, checkUnclearPlanCacheClone(ts1, ts2).Error(), "same pointer, path *core.PhysicalTableScan.AccessCondition[0](*expression.Column).RetType")

	// same interface
	child := &core.PhysicalTableScan{}
	ts1.SetProbeParents([]base.PhysicalPlan{child})
	ts2.SetProbeParents([]base.PhysicalPlan{child})
	require.Equal(t, checkUnclearPlanCacheClone(ts1, ts2).Error(), "same pointer, path *core.PhysicalTableScan.physicalSchemaProducer.basePhysicalPlan.probeParents[0](*core.PhysicalTableScan)")

	// same map
	l1 := &core.PhysicalLock{}
	l2 := &core.PhysicalLock{}
	l1.TblID2Handle = make(map[int64][]util.HandleCols)
	l2.TblID2Handle = l1.TblID2Handle
	require.Equal(t, checkUnclearPlanCacheClone(l1, l2).Error(), "same map pointers, path *core.PhysicalLock.TblID2Handle")

	// same pointer in map
	l2.TblID2Handle = make(map[int64][]util.HandleCols)
	cols := make([]util.HandleCols, 10)
	l1.TblID2Handle[1] = cols
	l2.TblID2Handle[1] = cols
	require.Equal(t, checkUnclearPlanCacheClone(l1, l2).Error(), "same slice pointers, path *core.PhysicalLock.TblID2Handle[int64]")

	// same sctx
	l1.TblID2Handle[1] = nil
	l2.TblID2Handle[1] = nil
	ctx := core.MockContext()
	defer ctx.Close()
	l1.SetSCtx(ctx)
	l2.SetSCtx(ctx)
	require.Equal(t, checkUnclearPlanCacheClone(l1, l2).Error(), "same pointer, path *core.PhysicalLock.basePhysicalPlan.Plan.ctx(*mock.Context)")

	// test tag
	type S struct {
		p1 *int `plan-cache-clone:"shallow"`
		p2 *int
	}
	s1 := new(S)
	s2 := new(S)
	s1.p2 = new(int)
	s2.p2 = s1.p2
	require.Equal(t, checkUnclearPlanCacheClone(s1, s2).Error(), "same pointer, path *core_test.S.p2")
	s2.p2 = new(int)
	s1.p1 = new(int)
	s2.p1 = s1.p1
	require.NoError(t, checkUnclearPlanCacheClone(s1, s2))
}

// checkUnclearPlanCacheClone checks whether this cloned plan is safe for instance plan cache.
// All fields in the plan should be deeply cloned except the fields with tag "plan-cache-shallow-clone:'true'".
func checkUnclearPlanCacheClone(plan, cloned any) error {
	return planCacheUnclearCloneCheck(reflect.ValueOf(plan), reflect.ValueOf(cloned), reflect.TypeOf(plan).String(), nil)
}

func planCacheUnclearCloneCheck(v1, v2 reflect.Value, path string, visited map[visit]bool) error {
	if !v1.IsValid() || !v2.IsValid() {
		if v1.IsValid() != v2.IsValid() {
			return errors.Errorf("invalid")
		}
		return nil
	}

	if v1.Type() != v2.Type() {
		return errors.Errorf("different type %v, %v, path %v", v1.Type(), v2.Type(), path)
	}

	if visited == nil {
		visited = make(map[visit]bool)
	}
	hard := func(k reflect.Kind) bool {
		switch k {
		case reflect.Map, reflect.Slice, reflect.Ptr, reflect.Interface:
			return true
		}
		return false
	}
	if v1.CanAddr() && v2.CanAddr() && hard(v1.Kind()) { // avoid dead loop
		addr1 := unsafe.Pointer(v1.UnsafeAddr())
		addr2 := unsafe.Pointer(v2.UnsafeAddr())
		if uintptr(addr1) > uintptr(addr2) {
			addr1, addr2 = addr2, addr1
		}
		typ := v1.Type()
		v := visit{addr1, addr2, typ}
		if visited[v] {
			return nil
		}
		visited[v] = true
	}

	switch v1.Kind() {
	case reflect.Array:
		for i := 0; i < v1.Len(); i++ {
			if err := planCacheUnclearCloneCheck(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), visited); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if (v1.IsNil() && v2.IsNil()) || (v1.Len() == 0 && v2.Len() == 0) {
			return nil
		}
		if v1.Len() != v2.Len() {
			return errors.Errorf("different slice lengths, len %v, %v, path %v", v1.Len(), v2.Len(), path)
		}
		if v1.IsNil() != v2.IsNil() {
			if v1.Len() == 0 && v2.Len() == 0 {
				return nil // nil and an empty slice are accepted
			}
			return errors.Errorf("different slices nil %v, %v, path %v", v1.IsNil(), v2.IsNil(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("same slice pointers, path %v", path)
		}
		for i := 0; i < v1.Len(); i++ {
			if err := planCacheUnclearCloneCheck(v1.Index(i), v2.Index(i), fmt.Sprintf("%v[%v]", path, i), visited); err != nil {
				return err
			}
		}
	case reflect.Interface:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() {
			return errors.Errorf("invalid interfaces, path %v", path)
		}
		return planCacheUnclearCloneCheck(v1.Elem(), v2.Elem(), fmt.Sprintf("%v(%v)", path, v1.Elem().Type().String()), visited)
	case reflect.Ptr:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("same pointer, path %v", path)
		}
		return planCacheUnclearCloneCheck(v1.Elem(), v2.Elem(), path, visited)
	case reflect.Struct:
		for i, n := 0, v1.NumField(); i < n; i++ {
			tag := v1.Type().Field(i).Tag.Get("plan-cache-clone")
			if tag == "shallow" {
				continue
			}
			fieldName := v1.Type().Field(i).Name
			if err := planCacheUnclearCloneCheck(v1.Field(i), v2.Field(i), fmt.Sprintf("%v.%v", path, fieldName), visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		if v1.IsNil() && v2.IsNil() {
			return nil
		}
		if v1.IsNil() != v2.IsNil() || v1.Len() != v2.Len() {
			return errors.Errorf("different maps nil: %v, %v, len: %v, %v, path: %v", v1.IsNil(), v2.IsNil(), v1.Len(), v2.Len(), path)
		}
		if v1.Pointer() == v2.Pointer() {
			return errors.Errorf("same map pointers, path %v", path)
		}
		if len(v1.MapKeys()) != len(v2.MapKeys()) {
			return errors.Errorf("invalid map")
		}
		for _, k := range v1.MapKeys() {
			val1 := v1.MapIndex(k)
			val2 := v2.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				return errors.Errorf("invalid map value at %v", fmt.Sprintf("%v[%v]", path, k.Type().Name()))
			}
			if err := planCacheUnclearCloneCheck(val1, val2, fmt.Sprintf("%v[%v]", path, k.Type().Name()), visited); err != nil {
				return err
			}
		}
	default:
		return nil
	}
	return nil
}

type visit struct {
	a1  unsafe.Pointer
	a2  unsafe.Pointer
	typ reflect.Type
}
