// Copyright 2023 PingCAP, Inc.
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

package baseimpl

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// Plan Should be used as embedded struct in Plan implementations.
type Plan struct {
	ctx     context.PlanContext
	stats   *property.StatsInfo
	tp      string
	id      int
	qbBlock int // Query Block offset
}

// NewBasePlan creates a new base plan.
func NewBasePlan(ctx context.PlanContext, tp string, qbBlock int) Plan {
	id := ctx.GetSessionVars().PlanID.Add(1)
	return Plan{
		tp:      tp,
		id:      int(id),
		ctx:     ctx,
		qbBlock: qbBlock,
	}
}

// SCtx is to get the sessionctx from the plan.
func (p *Plan) SCtx() context.PlanContext {
	return p.ctx
}

// SetSCtx is to set the sessionctx for the plan.
func (p *Plan) SetSCtx(ctx context.PlanContext) {
	p.ctx = ctx
}

// OutputNames returns the outputting names of each column.
func (*Plan) OutputNames() types.NameSlice {
	return nil
}

// SetOutputNames sets the outputting name by the given slice.
func (*Plan) SetOutputNames(_ types.NameSlice) {}

// ReplaceExprColumns implements Plan interface.
func (*Plan) ReplaceExprColumns(_ map[string]*expression.Column) {}

// ID is to get the id.
func (p *Plan) ID() int {
	return p.id
}

// SetID is to set id.
func (p *Plan) SetID(id int) {
	p.id = id
}

// StatsInfo is to get the stats info.
func (p *Plan) StatsInfo() *property.StatsInfo {
	return p.stats
}

// ExplainInfo is to get the explain information.
func (*Plan) ExplainInfo() string {
	return "N/A"
}

// ExplainID is to get the explain ID.
func (p *Plan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.ctx != nil && p.ctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.tp
		}
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// TP is to get the tp.
func (p *Plan) TP() string {
	return p.tp
}

// SetTP is to set the tp.
func (p *Plan) SetTP(tp string) {
	p.tp = tp
}

// QueryBlockOffset is to get the select block offset.
func (p *Plan) QueryBlockOffset() int {
	return p.qbBlock
}

// SetStats sets the stats
func (p *Plan) SetStats(s *property.StatsInfo) {
	p.stats = s
}

// PlanSize is the size of BasePlan.
const PlanSize = int64(unsafe.Sizeof(Plan{}))

// MemoryUsage return the memory usage of BasePlan
func (p *Plan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = PlanSize + int64(len(p.tp))
	return sum
}

// BuildPlanTrace is to build the plan trace.
func (p *Plan) BuildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP()}
	return planTrace
}
