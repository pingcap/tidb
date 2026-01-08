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

package physicalop

import (
	"strconv"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalLock is the physical operator of lock, which is used for `select ... for update` clause.
type PhysicalLock struct {
	BasePhysicalPlan

	Lock *ast.SelectLockInfo `plan-cache-clone:"shallow"`

	TblID2Handle       map[int64][]util.HandleCols
	TblID2PhysTblIDCol map[int64]*expression.Column
}

// ExhaustPhysicalPlans4LogicalLock exhausts PhysicalLock plans from LogicalLock.
func ExhaustPhysicalPlans4LogicalLock(lp base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	p := lp.(*logicalop.LogicalLock)
	if prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Lock` is not supported now.")
		return nil, true, nil
	}
	childProp := prop.CloneEssentialFields()
	lock := PhysicalLock{
		Lock:               p.Lock,
		TblID2Handle:       p.TblID2Handle,
		TblID2PhysTblIDCol: p.TblID2PhysTblIDCol,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
	return []base.PhysicalPlan{lock}, true, nil
}

// Init initializes PhysicalLock.
func (p PhysicalLock) Init(ctx base.PlanContext, stats *property.StatsInfo, props ...*property.PhysicalProperty) *PhysicalLock {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeLock, &p, 0)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// MemoryUsage return the memory usage of PhysicalLock
func (p *PhysicalLock) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfPointer + size.SizeOfMap*2
	if p.Lock != nil {
		sum += int64(unsafe.Sizeof(ast.SelectLockInfo{}))
	}

	for _, vals := range p.TblID2Handle {
		sum += size.SizeOfInt64 + size.SizeOfSlice + int64(cap(vals))*size.SizeOfInterface
		for _, val := range vals {
			sum += val.MemoryUsage()
		}
	}
	for _, val := range p.TblID2PhysTblIDCol {
		sum += size.SizeOfInt64 + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalLock) ExplainInfo() string {
	var str strings.Builder
	str.WriteString(p.Lock.LockType.String())
	str.WriteString(" ")
	str.WriteString(strconv.FormatUint(p.Lock.WaitSec, 10))
	return str.String()
}

// ResolveIndices implements Plan interface.
func (p *PhysicalLock) ResolveIndices() (err error) {
	err = p.BasePhysicalPlan.ResolveIndices()
	if err != nil {
		return err
	}
	for i, cols := range p.TblID2Handle {
		for j, col := range cols {
			resolvedCol, err := col.ResolveIndices(p.Children()[0].Schema())
			if err != nil {
				return err
			}
			p.TblID2Handle[i][j] = resolvedCol
		}
	}
	return nil
}

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalLock) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalLock)
	*cloned = *p
	basePlan, baseOK := p.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	if p.TblID2Handle != nil {
		cloned.TblID2Handle = make(map[int64][]util.HandleCols, len(p.TblID2Handle))
		for k, v := range p.TblID2Handle {
			cloned.TblID2Handle[k] = util.CloneHandleCols(v)
		}
	}
	if p.TblID2PhysTblIDCol != nil {
		cloned.TblID2PhysTblIDCol = make(map[int64]*expression.Column, len(p.TblID2PhysTblIDCol))
		for k, v := range p.TblID2PhysTblIDCol {
			cloned.TblID2PhysTblIDCol[k] = v.Clone().(*expression.Column)
		}
	}
	return cloned, true
}
