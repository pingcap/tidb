package implementation

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
)

// TiDBHashAggImpl is the implementation of PhysicalHashAgg in TiDB layer.
type TiDBHashAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiDBHashAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, true)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (agg *TiDBHashAggImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	hashAgg.SetChildren(children[0].GetPlan())
	// Inject extraProjection if the AggFuncs or GroupByItems contain ScalarFunction.
	plannercore.InjectProjBelowAgg(hashAgg, hashAgg.AggFuncs, hashAgg.GroupByItems)
	return agg
}

// NewTiDBHashAggImpl creates a new TiDBHashAggImpl.
func NewTiDBHashAggImpl(agg *plannercore.PhysicalHashAgg) *TiDBHashAggImpl {
	return &TiDBHashAggImpl{baseImpl{plan: agg}}
}

// TiKVHashAggImpl is the implementation of PhysicalHashAgg in TiKV layer.
type TiKVHashAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiKVHashAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, false)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// NewTiKVHashAggImpl creates a new TiKVHashAggImpl.
func NewTiKVHashAggImpl(agg *plannercore.PhysicalHashAgg) *TiKVHashAggImpl {
	return &TiKVHashAggImpl{baseImpl{plan: agg}}
}

// StreamAggImpl is the implementation of PhysicalStreamAgg.
type StreamAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *StreamAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, false)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// NewStreamAggImpl creates a new StreamAggImpl.
func NewStreamAggImpl(agg *plannercore.PhysicalStreamAgg) *StreamAggImpl {
	return &StreamAggImpl{baseImpl{plan: agg}}
}
