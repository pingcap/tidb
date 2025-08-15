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
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// PhysicalCTE is for CTE.
type PhysicalCTE struct {
	PhysicalSchemaProducer

	SeedPlan  base.PhysicalPlan
	RecurPlan base.PhysicalPlan
	CTE       *logicalop.CTEClass
	CteAsName ast.CIStr
	CteName   ast.CIStr

	ReaderReceiver *PhysicalExchangeReceiver
	StorageSender  *PhysicalExchangeSender
}

// Init only assigns type and context.
func (p PhysicalCTE) Init(ctx base.PlanContext, stats *property.StatsInfo) *PhysicalCTE {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeCTE, &p, 0)
	p.SetStats(stats)
	return &p
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4PhysicalPlan(p.SeedPlan)
	if p.RecurPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(p.RecurPlan)...)
	}
	return corCols
}

// OperatorInfo implements dataAccesser interface.
func (p *PhysicalCTE) OperatorInfo(_ bool) string {
	return fmt.Sprintf("data:%s", (*CTEDefinition)(p).ExplainID())
}

// ExplainInfo implements Plan interface.
func (p *PhysicalCTE) ExplainInfo() string {
	return p.AccessObject().String() + ", " + p.OperatorInfo(false)
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTE) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.SCtx() != nil && p.SCtx().GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.TP()
		}
		return p.TP() + "_" + strconv.Itoa(p.ID())
	})
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTE) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalCTE)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	if p.SeedPlan != nil {
		cloned.SeedPlan, err = p.SeedPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	if p.RecurPlan != nil {
		cloned.RecurPlan, err = p.RecurPlan.Clone(newCtx)
		if err != nil {
			return nil, err
		}
	}
	cloned.CteAsName, cloned.CteName = p.CteAsName, p.CteName
	cloned.CTE = p.CTE
	if p.StorageSender != nil {
		clonedSender, err := p.StorageSender.Clone(newCtx)
		if err != nil {
			return nil, err
		}
		cloned.StorageSender = clonedSender.(*PhysicalExchangeSender)
	}
	if p.ReaderReceiver != nil {
		clonedReceiver, err := p.ReaderReceiver.Clone(newCtx)
		if err != nil {
			return nil, err
		}
		cloned.ReaderReceiver = clonedReceiver.(*PhysicalExchangeReceiver)
	}
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalCTE
func (p *PhysicalCTE) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalCTE) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalCTE(p, taskType, option)
}

// AccessObject implements DataAccesser interface.
func (p *PhysicalCTE) AccessObject() base.AccessObject {
	if p.CteName == p.CteAsName {
		return access.OtherAccessObject(fmt.Sprintf("CTE:%s", p.CteName.L))
	}
	return access.OtherAccessObject(fmt.Sprintf("CTE:%s AS %s", p.CteName.L, p.CteAsName.L))
}

// CTEDefinition is CTE definition for explain.
type CTEDefinition PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (p *CTEDefinition) ExplainInfo() string {
	var res string
	if p.RecurPlan != nil {
		res = "Recursive CTE"
	} else {
		res = "Non-Recursive CTE"
	}
	if p.CTE.HasLimit {
		offset, count := p.CTE.LimitBeg, p.CTE.LimitEnd-p.CTE.LimitBeg
		switch p.SCtx().GetSessionVars().EnableRedactLog {
		case errors.RedactLogMarker:
			res += fmt.Sprintf(", limit(offset:‹%v›, count:‹%v›)", offset, count)
		case errors.RedactLogDisable:
			res += fmt.Sprintf(", limit(offset:%v, count:%v)", offset, count)
		case errors.RedactLogEnable:
			res += ", limit(offset:?, count:?)"
		}
	}
	return res
}

// ExplainID overrides the ExplainID.
func (p *CTEDefinition) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *CTEDefinition) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.SeedPlan != nil {
		sum += p.SeedPlan.MemoryUsage()
	}
	if p.RecurPlan != nil {
		sum += p.RecurPlan.MemoryUsage()
	}
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// PhysicalCTEStorage is used for representing CTE storage, or CTE producer in other words.
type PhysicalCTEStorage PhysicalCTE

// ExplainInfo overrides the ExplainInfo
func (*PhysicalCTEStorage) ExplainInfo() string {
	return "Non-Recursive CTE Storage"
}

// ExplainID overrides the ExplainID.
func (p *PhysicalCTEStorage) ExplainID(_ ...bool) fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "CTE_" + strconv.Itoa(p.CTE.IDForStorage)
	})
}

// MemoryUsage return the memory usage of CTEDefinition
func (p *PhysicalCTEStorage) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.CteAsName.MemoryUsage()
	if p.CTE != nil {
		sum += p.CTE.MemoryUsage()
	}
	return
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalCTEStorage) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned, err := (*PhysicalCTE)(p).Clone(newCtx)
	if err != nil {
		return nil, err
	}
	return (*PhysicalCTEStorage)(cloned.(*PhysicalCTE)), nil
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalCTEStorage) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalCTEStorage(p, tasks...)
}
