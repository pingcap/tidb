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

	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalTableDual is the physical operator of dual.
type PhysicalTableDual struct {
	PhysicalSchemaProducer

	RowCount int

	// names is used for OutputNames() method. Dual may be inited when building point get plan.
	// So it needs to hold names for itself.
	names []*types.FieldName
}

// Init initializes PhysicalTableDual.
func (p PhysicalTableDual) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int) *PhysicalTableDual {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeDual, &p, offset)
	p.SetStats(stats)
	return &p
}

// OutputNames returns the outputting names of each column.
func (p *PhysicalTableDual) OutputNames() types.NameSlice {
	return p.names
}

// SetOutputNames sets the outputting name by the given slice.
func (p *PhysicalTableDual) SetOutputNames(names types.NameSlice) {
	p.names = names
}

// MemoryUsage return the memory usage of PhysicalTableDual
func (p *PhysicalTableDual) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt + size.SizeOfSlice + int64(cap(p.names))*size.SizeOfPointer
	for _, name := range p.names {
		sum += name.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalTableDual) ExplainInfo() string {
	var str strings.Builder
	str.WriteString("rows:")
	str.WriteString(strconv.Itoa(p.RowCount))
	return str.String()
}

func (p *PhysicalTableDual) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalTableDual)
	*cloned = *p
	basePlan, baseOK := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *basePlan
	cloned.names = util.CloneFieldNames(p.names)
	return cloned, true
}
