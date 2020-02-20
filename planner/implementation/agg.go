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

// TiDBStreamAggImpl is the implementation of PhysicalStreamAgg in TiDB layer.
type TiDBStreamAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiDBStreamAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	streamAgg := agg.plan.(*plannercore.PhysicalStreamAgg)
	selfCost := streamAgg.GetCost(children[0].GetPlan().Stats().RowCount, true)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// NewTiDBStreamAggImpl creates a new TiDBStreamAggImpl.
func NewTiDBStreamAggImpl(agg *plannercore.PhysicalStreamAgg) *TiDBStreamAggImpl {
	return &TiDBStreamAggImpl{baseImpl{plan: agg}}
}

// AttachChildren implements Implementation AttachChildren interface.
func (agg *TiDBStreamAggImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	streamAgg := agg.plan.(*plannercore.PhysicalStreamAgg)
	streamAgg.SetChildren(children[0].GetPlan())
	// Inject extraProjection if the AggFuncs or GroupByItems contain ScalarFunction.
	plannercore.InjectProjBelowAgg(streamAgg, streamAgg.AggFuncs, streamAgg.GroupByItems)
	return agg
}

// TiKVStreamAggImpl is the implementation of PhysicalStreamAgg in TiKV layer.
type TiKVStreamAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiKVStreamAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	streamAgg := agg.plan.(*plannercore.PhysicalStreamAgg)
	selfCost := streamAgg.GetCost(children[0].GetPlan().Stats().RowCount, false)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// NewTiKVStreamAggImpl creates a new TiKVStreamAggImpl.
func NewTiKVStreamAggImpl(agg *plannercore.PhysicalStreamAgg) *TiKVStreamAggImpl {
	return &TiKVStreamAggImpl{baseImpl{plan: agg}}
}
