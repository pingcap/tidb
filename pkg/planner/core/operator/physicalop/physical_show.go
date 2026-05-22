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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalShow represents a show plan.
type PhysicalShow struct {
	PhysicalSchemaProducer

	logicalop.ShowContents

	Extractor base.ShowPredicateExtractor
}

// Init initializes PhysicalShow.
func (p PhysicalShow) Init(ctx base.PlanContext) *PhysicalShow {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeShow, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.SetStats(&property.StatsInfo{RowCount: 1})
	return &p
}

// MemoryUsage return the memory usage of PhysicalShow
func (p *PhysicalShow) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + p.ShowContents.MemoryUsage() + size.SizeOfInterface
	return
}

// PhysicalShowDDLJobs is for showing DDL job list.
type PhysicalShowDDLJobs struct {
	PhysicalSchemaProducer

	JobNumber int64
}

// Init initializes PhysicalShowDDLJobs.
func (p PhysicalShowDDLJobs) Init(ctx base.PlanContext) *PhysicalShowDDLJobs {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeShowDDLJobs, &p, 0)
	// Just use pseudo stats to avoid panic.
	p.SetStats(&property.StatsInfo{RowCount: 1})
	return &p
}

// MemoryUsage return the memory usage of PhysicalShowDDLJobs
func (p *PhysicalShowDDLJobs) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}
	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfInt64
}

func findBestTask4LogicalShowDDLJobs(super base.LogicalPlan, prop *property.PhysicalProperty) (base.Task, error) {
	if prop.IndexJoinProp != nil {
		// even enforce hint can not work with this.
		return base.InvalidTask, nil
	}
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalShowDDLJobs](super)
	if !prop.IsSortItemEmpty() {
		return base.InvalidTask, nil
	}
	pShow := PhysicalShowDDLJobs{JobNumber: p.JobNumber}.Init(p.SCtx())
	pShow.SetSchema(p.Schema())
	rt := &RootTask{}
	rt.SetPlan(pShow)
	return rt, nil
}

func findBestTask4LogicalShow(super base.LogicalPlan, prop *property.PhysicalProperty) (base.Task, error) {
	if prop.IndexJoinProp != nil {
		// even enforce hint can not work with this.
		return base.InvalidTask, nil
	}
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalShow](super)
	if !prop.IsSortItemEmpty() {
		return base.InvalidTask, nil
	}
	pShow := PhysicalShow{ShowContents: p.ShowContents, Extractor: p.Extractor}.Init(p.SCtx())
	pShow.SetSchema(p.Schema())
	rt := &RootTask{}
	rt.SetPlan(pShow)
	return rt, nil
}
