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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalMemTable reads memory table.
type PhysicalMemTable struct {
	PhysicalSchemaProducer

	DBName         ast.CIStr
	Table          *model.TableInfo
	Columns        []*model.ColumnInfo
	Extractor      base.MemTablePredicateExtractor
	QueryTimeRange util.QueryTimeRange
}

// MemoryUsage return the memory usage of PhysicalMemTable
func (p *PhysicalMemTable) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.DBName.MemoryUsage() + size.SizeOfPointer + size.SizeOfSlice +
		int64(cap(p.Columns))*size.SizeOfPointer + size.SizeOfInterface + p.QueryTimeRange.MemoryUsage()
	return
}

// Init initializes PhysicalMemTable.
func (p PhysicalMemTable) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int) *PhysicalMemTable {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeMemTableScan, &p, offset)
	p.SetStats(stats)
	return &p
}

// ExplainInfo implements Plan interface.
func (p *PhysicalMemTable) ExplainInfo() string {
	accessObject, operatorInfo := p.AccessObject().String(), p.OperatorInfo(false)
	if len(operatorInfo) == 0 {
		return accessObject
	}
	return accessObject + ", " + operatorInfo
}

// AccessObject implements DataAccesser interface.
func (p *PhysicalMemTable) AccessObject() base.AccessObject {
	return &access.ScanAccessObject{
		Database: p.DBName.O,
		Table:    p.Table.Name.O,
	}
}

// OperatorInfo implements DataAccesser interface.
func (p *PhysicalMemTable) OperatorInfo(_ bool) string {
	if p.Extractor != nil {
		return p.Extractor.ExplainInfo(p)
	}
	return ""
}

// findBestTask4LogicalMemTable converts the LogicalMemTable to PhysicalMemTable.
func findBestTask4LogicalMemTable(super *logicalop.LogicalMemTable, prop *property.PhysicalProperty) (t base.Task, err error) {
	if prop.IndexJoinProp != nil {
		// even enforce hint can not work with this.
		return base.InvalidTask, nil
	}
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalMemTable](super)
	if prop.MPPPartitionTp != property.AnyType {
		return base.InvalidTask, nil
	}

	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for p.findBestTask.
	// Before function return, reset it for enforcing task prop.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		// still use the super.
		t, err = FindBestTask(super, prop)
		if err != nil {
			return nil, err
		}
		prop.CanAddEnforcer = true
		if t != base.InvalidTask {
			return
		}
		// Next, get the bestTask with enforced prop
		prop.SortItems = []property.SortItem{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.CanAddEnforcer {
			*prop = *oldProp
			t = EnforceProperty(prop, t, p.Plan.SCtx(), nil)
			prop.CanAddEnforcer = true
		}
	}()

	if !prop.IsSortItemEmpty() {
		return base.InvalidTask, nil
	}
	memTable := PhysicalMemTable{
		DBName:         p.DBName,
		Table:          p.TableInfo,
		Columns:        p.Columns,
		Extractor:      p.Extractor,
		QueryTimeRange: p.QueryTimeRange,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
	memTable.SetSchema(p.Schema())
	rt := &RootTask{}
	rt.SetPlan(memTable)
	return rt, nil
}
