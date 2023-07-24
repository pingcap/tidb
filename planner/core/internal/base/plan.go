package base

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tracing"
)

// BasePlan implements base Plan interface.
// Should be used as embedded struct in Plan implementations.
type BasePlan struct {
	tp          string
	id          int
	ctx         sessionctx.Context
	stats       *property.StatsInfo
	blockOffset int
}

func NewBasePlan(ctx sessionctx.Context, tp string, offset int) BasePlan {
	id := ctx.GetSessionVars().PlanID.Add(1)
	return BasePlan{
		tp:          tp,
		id:          int(id),
		ctx:         ctx,
		blockOffset: offset,
	}
}

func (p *BasePlan) SCtx() sessionctx.Context {
	return p.ctx
}

func (p *BasePlan) SetSCtx(ctx sessionctx.Context) {
	p.ctx = ctx
}

func (p *BasePlan) BlockOffset() int {
	return p.blockOffset
}

// OutputNames returns the outputting names of each column.
func (*BasePlan) OutputNames() types.NameSlice {
	return nil
}

func (*BasePlan) SetOutputNames(_ types.NameSlice) {}

func (*BasePlan) ReplaceExprColumns(_ map[string]*expression.Column) {}

// ID implements Plan ID interface.
func (p *BasePlan) ID() int {
	return p.id
}

func (p *BasePlan) SetID(id int) {
	p.id = id
}

// property.StatsInfo implements the Plan interface.
func (p *BasePlan) StatsInfo() *property.StatsInfo {
	return p.stats
}

// ExplainInfo implements Plan interface.
func (*BasePlan) ExplainInfo() string {
	return "N/A"
}

func (p *BasePlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		if p.ctx != nil && p.ctx.GetSessionVars().StmtCtx.IgnoreExplainIDSuffix {
			return p.tp
		}
		return p.tp + "_" + strconv.Itoa(p.id)
	})
}

// TP implements Plan interface.
func (p *BasePlan) TP() string {
	return p.tp
}

// TP implements Plan interface.
func (p *BasePlan) SetTP(tp string) {
	p.tp = tp
}

func (p *BasePlan) SelectBlockOffset() int {
	return p.blockOffset
}

// SetStats sets BasePlan.stats
func (p *BasePlan) SetStats(s *property.StatsInfo) {
	p.stats = s
}

// BasePlanSize is the size of BasePlan.
const BasePlanSize = int64(unsafe.Sizeof(BasePlan{}))

// MemoryUsage return the memory usage of BasePlan
func (p *BasePlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = BasePlanSize + int64(len(p.tp))
	return sum
}

// BuildPlanTrace implements Plan
func (p *BasePlan) BuildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP()}
	return planTrace
}
