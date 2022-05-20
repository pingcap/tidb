// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/zap"
)

const (
	// SelectionFactor is the default factor of the selectivity.
	// For example, If we have no idea how to estimate the selectivity
	// of a Selection or a JoinCondition, we can use this default value.
	SelectionFactor = 0.8
	distinctFactor  = 0.8

	// If the actual row count is much more than the limit count, the unordered scan may cost much more than keep order.
	// So when a limit exists, we don't apply the DescScanFactor.
	smallScanThreshold = 10000
)

var aggFuncFactor = map[string]float64{
	ast.AggFuncCount:       1.0,
	ast.AggFuncSum:         1.0,
	ast.AggFuncAvg:         2.0,
	ast.AggFuncFirstRow:    0.1,
	ast.AggFuncMax:         1.0,
	ast.AggFuncMin:         1.0,
	ast.AggFuncGroupConcat: 1.0,
	ast.AggFuncBitOr:       0.9,
	ast.AggFuncBitXor:      0.9,
	ast.AggFuncBitAnd:      0.9,
	ast.AggFuncVarPop:      3.0,
	ast.AggFuncVarSamp:     3.0,
	ast.AggFuncStddevPop:   3.0,
	ast.AggFuncStddevSamp:  3.0,
	"default":              1.5,
}

// PlanCounterTp is used in hint nth_plan() to indicate which plan to use.
type PlanCounterTp int64

// PlanCounterDisabled is the default value of PlanCounterTp, indicating that optimizer needn't force a plan.
var PlanCounterDisabled PlanCounterTp = -1

// Dec minus PlanCounterTp value by x.
func (c *PlanCounterTp) Dec(x int64) {
	if *c <= 0 {
		return
	}
	*c = PlanCounterTp(int64(*c) - x)
	if *c < 0 {
		*c = 0
	}
}

// Empty indicates whether the PlanCounterTp is clear now.
func (c *PlanCounterTp) Empty() bool {
	return *c == 0
}

// IsForce indicates whether to force a plan.
func (c *PlanCounterTp) IsForce() bool {
	return *c != -1
}

var invalidTask = &rootTask{cst: math.MaxFloat64}

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*util.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{SortItems: propItems}, true
}

// GetPropByOrderByItemsContainScalarFunc will check if this sort property can be pushed or not. In order to simplify the
// problem, we only consider the case that all expression are columns or some special scalar functions.
func GetPropByOrderByItemsContainScalarFunc(items []*util.ByItems) (*property.PhysicalProperty, bool, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	onlyColumn := true
	for _, item := range items {
		switch expr := item.Expr.(type) {
		case *expression.Column:
			propItems = append(propItems, property.SortItem{Col: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleColumn(item.Desc)
			if col == nil {
				return nil, false, false
			}
			propItems = append(propItems, property.SortItem{Col: col, Desc: desc})
			onlyColumn = false
		default:
			return nil, false, false
		}
	}
	return &property.PhysicalProperty{SortItems: propItems}, true, onlyColumn
}

func (p *LogicalTableDual) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (task, int64, error) {
	// If the required property is not empty and the row count > 1,
	// we cannot ensure this required property.
	// But if the row count is 0 or 1, we don't need to care about the property.
	if (!prop.IsSortItemEmpty() && p.RowCount > 1) || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	dual := PhysicalTableDual{
		RowCount: p.RowCount,
	}.Init(p.ctx, p.stats, p.blockOffset)
	dual.SetSchema(p.schema)
	planCounter.Dec(1)
	return &rootTask{p: dual, isEmpty: p.RowCount == 0}, 1, nil
}

func (p *LogicalShow) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (task, int64, error) {
	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	pShow := PhysicalShow{ShowContents: p.ShowContents, Extractor: p.Extractor}.Init(p.ctx)
	pShow.SetSchema(p.schema)
	planCounter.Dec(1)
	return &rootTask{p: pShow}, 1, nil
}

func (p *LogicalShowDDLJobs) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (task, int64, error) {
	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	pShow := PhysicalShowDDLJobs{JobNumber: p.JobNumber}.Init(p.ctx)
	pShow.SetSchema(p.schema)
	planCounter.Dec(1)
	return &rootTask{p: pShow}, 1, nil
}

// rebuildChildTasks rebuilds the childTasks to make the clock_th combination.
func (p *baseLogicalPlan) rebuildChildTasks(childTasks *[]task, pp PhysicalPlan, childCnts []int64, planCounter int64, TS uint64, opt *physicalOptimizeOp) error {
	// The taskMap of children nodes should be rolled back first.
	for _, child := range p.children {
		child.rollBackTaskMap(TS)
	}

	multAll := int64(1)
	var curClock PlanCounterTp
	for _, x := range childCnts {
		multAll *= x
	}
	*childTasks = (*childTasks)[:0]
	for j, child := range p.children {
		multAll /= childCnts[j]
		curClock = PlanCounterTp((planCounter-1)/multAll + 1)
		childTask, _, err := child.findBestTask(pp.GetChildReqProps(j), &curClock, opt)
		planCounter = (planCounter-1)%multAll + 1
		if err != nil {
			return err
		}
		if curClock != 0 {
			return errors.Errorf("PlanCounterTp planCounter is not handled")
		}
		if childTask != nil && childTask.invalid() {
			return errors.Errorf("The current plan is invalid, please skip this plan")
		}
		*childTasks = append(*childTasks, childTask)
	}
	return nil
}

func (p *baseLogicalPlan) enumeratePhysicalPlans4Task(physicalPlans []PhysicalPlan, prop *property.PhysicalProperty, addEnforcer bool, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (task, int64, error) {
	var bestTask task = invalidTask
	var curCntPlan, cntPlan int64
	childTasks := make([]task, 0, len(p.children))
	childCnts := make([]int64, len(p.children))
	cntPlan = 0
	for _, pp := range physicalPlans {
		// Find best child tasks firstly.
		childTasks = childTasks[:0]
		// The curCntPlan records the number of possible plans for pp
		curCntPlan = 1
		TimeStampNow := p.GetLogicalTS4TaskMap()
		savedPlanID := p.ctx.GetSessionVars().PlanID
		for j, child := range p.children {
			childProp := pp.GetChildReqProps(j)
			childTask, cnt, err := child.findBestTask(childProp, &PlanCounterDisabled, opt)
			childCnts[j] = cnt
			if err != nil {
				return nil, 0, err
			}
			curCntPlan = curCntPlan * cnt
			if childTask != nil && childTask.invalid() {
				break
			}
			childTasks = append(childTasks, childTask)
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != len(p.children) {
			continue
		}

		// If the target plan can be found in this physicalPlan(pp), rebuild childTasks to build the corresponding combination.
		if planCounter.IsForce() && int64(*planCounter) <= curCntPlan {
			p.ctx.GetSessionVars().PlanID = savedPlanID
			curCntPlan = int64(*planCounter)
			err := p.rebuildChildTasks(&childTasks, pp, childCnts, int64(*planCounter), TimeStampNow, opt)
			if err != nil {
				return nil, 0, err
			}
		}

		// Combine best child tasks with parent physical plan.
		curTask := pp.attach2Task(childTasks...)

		if curTask.invalid() {
			continue
		}

		// An optimal task could not satisfy the property, so it should be converted here.
		if _, ok := curTask.(*rootTask); !ok && prop.TaskTp == property.RootTaskType {
			curTask = curTask.convertToRootTask(p.ctx)
		}

		// Enforce curTask property
		if addEnforcer {
			curTask = enforceProperty(prop, curTask, p.basePlan.ctx)
		}

		// Optimize by shuffle executor to running in parallel manner.
		if _, isMpp := curTask.(*mppTask); !isMpp && prop.IsSortItemEmpty() {
			// Currently, we do not regard shuffled plan as a new plan.
			curTask = optimizeByShuffle(curTask, p.basePlan.ctx)
		}

		cntPlan += curCntPlan
		planCounter.Dec(curCntPlan)

		if planCounter.Empty() {
			bestTask = curTask
			break
		}
		opt.appendCandidate(p, curTask.plan(), prop)
		// Get the most efficient one.
		if curIsBetter, err := compareTaskCost(p.ctx, curTask, bestTask); err != nil {
			return nil, 0, err
		} else if curIsBetter {
			bestTask = curTask
		}
	}
	return bestTask, cntPlan, nil
}

// compareTaskCost compares cost of curTask and bestTask and returns whether curTask's cost is smaller than bestTask's.
func compareTaskCost(ctx sessionctx.Context, curTask, bestTask task) (curIsBetter bool, err error) {
	if curTask.invalid() {
		return false, nil
	}
	if bestTask.invalid() {
		return true, nil
	}
	if ctx.GetSessionVars().EnableNewCostInterface { // use the new cost interface
		curCost, err := getTaskPlanCost(curTask)
		if err != nil {
			return false, err
		}
		bestCost, err := getTaskPlanCost(bestTask)
		if err != nil {
			return false, err
		}
		return curCost < bestCost, nil
	}
	return curTask.cost() < bestTask.cost(), nil
}

func getTaskPlanCost(t task) (float64, error) {
	if t.invalid() {
		return math.MaxFloat64, nil
	}
	var taskType property.TaskType
	switch t.(type) {
	case *rootTask:
		taskType = property.RootTaskType
	case *copTask: // no need to know whether the task is single-read or double-read, so both CopSingleReadTaskType and CopDoubleReadTaskType are OK
		taskType = property.CopSingleReadTaskType
	case *mppTask:
		taskType = property.MppTaskType
	default:
		return 0, errors.New("unknown task type")
	}
	return t.plan().GetPlanCost(taskType, 0)
}

type physicalOptimizeOp struct {
	// tracer is goring to track optimize steps during physical optimizing
	tracer *tracing.PhysicalOptimizeTracer
}

func defaultPhysicalOptimizeOption() *physicalOptimizeOp {
	return &physicalOptimizeOp{}
}

func (op *physicalOptimizeOp) withEnableOptimizeTracer(tracer *tracing.PhysicalOptimizeTracer) *physicalOptimizeOp {
	op.tracer = tracer
	return op
}

func (op *physicalOptimizeOp) buildPhysicalOptimizeTraceInfo(p LogicalPlan) {
	if op == nil || op.tracer == nil {
		return
	}
	name := tracing.CodecPlanName(p.TP(), p.ID())
	if _, ok := op.tracer.State[name]; !ok {
		op.tracer.State[name] = make(map[string]*tracing.PlanTrace)
	}
}

func (op *physicalOptimizeOp) appendCandidate(lp LogicalPlan, pp PhysicalPlan, prop *property.PhysicalProperty) {
	if op == nil || op.tracer == nil || pp == nil {
		return
	}
	PhysicalPlanTrace := &tracing.PlanTrace{TP: pp.TP(), ID: pp.ID(),
		ExplainInfo: pp.ExplainInfo(), Cost: pp.Cost(), ProperType: prop.String()}
	name := tracing.CodecPlanName(lp.TP(), lp.ID())
	key := tracing.CodecPlanName(pp.TP(), pp.ID())
	pps := op.tracer.State[name]
	if pps == nil {
		op.buildPhysicalOptimizeTraceInfo(lp)
	}
	pps[key] = PhysicalPlanTrace
	op.tracer.State[name] = pps
}

// findBestTask implements LogicalPlan interface.
func (p *baseLogicalPlan) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (bestTask task, cntPlan int64, err error) {
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, 1, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	bestTask = p.getTask(prop)
	if bestTask != nil {
		planCounter.Dec(1)
		return bestTask, 1, nil
	}

	canAddEnforcer := prop.CanAddEnforcer

	if prop.TaskTp != property.RootTaskType && !prop.IsFlashProp() {
		// Currently all plan cannot totally push down to TiKV.
		p.storeTask(prop, invalidTask)
		return invalidTask, 0, nil
	}

	bestTask = invalidTask
	cntPlan = 0
	// prop should be read only because its cached hashcode might be not consistent
	// when it is changed. So we clone a new one for the temporary changes.
	newProp := prop.CloneEssentialFields()
	var plansFitsProp, plansNeedEnforce []PhysicalPlan
	var hintWorksWithProp bool
	// Maybe the plan can satisfy the required property,
	// so we try to get the task without the enforced sort first.
	plansFitsProp, hintWorksWithProp, err = p.self.exhaustPhysicalPlans(newProp)
	if err != nil {
		return nil, 0, err
	}
	if !hintWorksWithProp && !newProp.IsSortItemEmpty() {
		// If there is a hint in the plan and the hint cannot satisfy the property,
		// we enforce this property and try to generate the PhysicalPlan again to
		// make sure the hint can work.
		canAddEnforcer = true
	}

	if canAddEnforcer {
		// Then, we use the empty property to get physicalPlans and
		// try to get the task with an enforced sort.
		newProp.SortItems = []property.SortItem{}
		newProp.SortItemsForPartition = []property.SortItem{}
		newProp.ExpectedCnt = math.MaxFloat64
		newProp.MPPPartitionCols = nil
		newProp.MPPPartitionTp = property.AnyType
		var hintCanWork bool
		plansNeedEnforce, hintCanWork, err = p.self.exhaustPhysicalPlans(newProp)
		if err != nil {
			return nil, 0, err
		}
		if hintCanWork && !hintWorksWithProp {
			// If the hint can work with the empty property, but cannot work with
			// the required property, we give up `plansFitProp` to make sure the hint
			// can work.
			plansFitsProp = nil
		}
		if !hintCanWork && !hintWorksWithProp && !prop.CanAddEnforcer {
			// If the original property is not enforced and hint cannot
			// work anyway, we give up `plansNeedEnforce` for efficiency,
			plansNeedEnforce = nil
		}
		newProp = prop
	}

	var cnt int64
	var curTask task
	opt.buildPhysicalOptimizeTraceInfo(p)
	if bestTask, cnt, err = p.enumeratePhysicalPlans4Task(plansFitsProp, newProp, false, planCounter, opt); err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		goto END
	}

	curTask, cnt, err = p.enumeratePhysicalPlans4Task(plansNeedEnforce, newProp, true, planCounter, opt)
	if err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		bestTask = curTask
		goto END
	}
	opt.appendCandidate(p, curTask.plan(), prop)
	if curIsBetter, err := compareTaskCost(p.ctx, curTask, bestTask); err != nil {
		return nil, 0, err
	} else if curIsBetter {
		bestTask = curTask
	}

END:
	p.storeTask(prop, bestTask)
	return bestTask, cntPlan, nil
}

func (p *LogicalMemTable) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (t task, cntPlan int64, err error) {
	if prop.MPPPartitionTp != property.AnyType {
		return invalidTask, 0, nil
	}

	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for p.findBestTask.
	// Before function return, reset it for enforcing task prop.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		cnt := int64(0)
		t, cnt, err = p.findBestTask(prop, planCounter, opt)
		if err != nil {
			return nil, 0, err
		}
		prop.CanAddEnforcer = true
		if t != invalidTask {
			cntPlan = cnt
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
			t = enforceProperty(prop, t, p.basePlan.ctx)
			prop.CanAddEnforcer = true
		}
	}()

	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return invalidTask, 0, nil
	}
	memTable := PhysicalMemTable{
		DBName:         p.DBName,
		Table:          p.TableInfo,
		Columns:        p.Columns,
		Extractor:      p.Extractor,
		QueryTimeRange: p.QueryTimeRange,
	}.Init(p.ctx, p.stats, p.blockOffset)
	memTable.SetSchema(p.schema)
	planCounter.Dec(1)
	return &rootTask{p: memTable}, 1, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			result, _, err := expression.EvalBool(ds.ctx, []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, err
			}
			if !result {
				dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
				dual.SetSchema(ds.schema)
				return &rootTask{
					p: dual,
				}, nil
			}
		}
	}
	return nil, nil
}

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path              *util.AccessPath
	accessCondsColMap util.Col2Len // accessCondsColMap maps Column.UniqueID to column length for the columns in AccessConds.
	indexCondsColMap  util.Col2Len // indexCondsColMap maps Column.UniqueID to column length for the columns in AccessConds and indexFilters.
	isMatchProp       bool
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if !l {
		return -1
	}
	return 1
}

func compareIndexBack(lhs, rhs *candidatePath) (int, bool) {
	result := compareBool(lhs.path.IsSingleScan, rhs.path.IsSingleScan)
	if result == 0 && !lhs.path.IsSingleScan {
		// if both lhs and rhs need to access table after IndexScan, we utilize the set of columns that occurred in AccessConds and IndexFilters
		// to compare how many table rows will be accessed.
		return util.CompareCol2Len(lhs.indexCondsColMap, rhs.indexCondsColMap)
	}
	return result, true
}

// compareCandidates is the core of skyline pruning. It compares the two candidate paths on three dimensions:
// (1): the set of columns that occurred in the access condition,
// (2): does it require a double scan,
// (3): whether or not it matches the physical property.
// If `x` is not worse than `y` at all factors,
// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
func compareCandidates(lhs, rhs *candidatePath) int {
	accessResult, comparable1 := util.CompareCol2Len(lhs.accessCondsColMap, rhs.accessCondsColMap)
	if !comparable1 {
		return 0
	}
	scanResult, comparable2 := compareIndexBack(lhs, rhs)
	if !comparable2 {
		return 0
	}
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := accessResult + scanResult + matchResult
	if accessResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		return 1
	}
	if accessResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		return -1
	}
	return 0
}

func (ds *DataSource) isMatchProp(path *util.AccessPath, prop *property.PhysicalProperty) bool {
	var isMatchProp bool
	if path.IsIntHandlePath {
		pkCol := ds.getPKIsHandleCol()
		if len(prop.SortItems) == 1 && pkCol != nil {
			isMatchProp = prop.SortItems[0].Col.Equal(nil, pkCol)
			if path.StoreType == kv.TiFlash {
				isMatchProp = isMatchProp && !prop.SortItems[0].Desc
			}
		}
		return isMatchProp
	}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.

	// Basically, if `prop.SortItems` is the prefix of `path.IdxCols`, then `isMatchProp` is true. However, we need to consider
	// the situations when some columns of `path.IdxCols` are evaluated as constant. For example:
	// ```
	// create table t(a int, b int, c int, d int, index idx_a_b_c(a, b, c), index idx_d_c_b_a(d, c, b, a));
	// select * from t where a = 1 order by b, c;
	// select * from t where b = 1 order by a, c;
	// select * from t where d = 1 and b = 2 order by c, a;
	// select * from t where d = 1 and b = 2 order by c, b, a;
	// ```
	// In the first two `SELECT` statements, `idx_a_b_c` matches the sort order. In the last two `SELECT` statements, `idx_d_c_b_a`
	// matches the sort order. Hence, we use `path.ConstCols` to deal with the above situations.
	if !prop.IsSortItemEmpty() && all && len(path.IdxCols) >= len(prop.SortItems) {
		isMatchProp = true
		i := 0
		for _, sortItem := range prop.SortItems {
			found := false
			for ; i < len(path.IdxCols); i++ {
				if path.IdxColLens[i] == types.UnspecifiedLength && sortItem.Col.Equal(nil, path.IdxCols[i]) {
					found = true
					i++
					break
				}
				if path.ConstCols == nil || i >= len(path.ConstCols) || !path.ConstCols[i] {
					break
				}
			}
			if !found {
				isMatchProp = false
				break
			}
		}
	}
	return isMatchProp
}

func (ds *DataSource) getTableCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.isMatchProp = ds.isMatchProp(path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(path.AccessConds, nil, nil)
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.isMatchProp = ds.isMatchProp(path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	return candidate
}

func (ds *DataSource) getIndexMergeCandidate(path *util.AccessPath) *candidatePath {
	candidate := &candidatePath{path: path}
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		// We should check whether the possible access path is valid first.
		if path.StoreType != kv.TiFlash && prop.IsFlashProp() {
			continue
		}
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, ds.getIndexMergeCandidate(path))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath() {
			currentCandidate = ds.getTableCandidate(path, prop)
		} else {
			if len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() || path.Forced || path.IsSingleScan {
				// We will use index to generate physical plan if any of the following conditions is satisfied:
				// 1. This path's access cond is not nil.
				// 2. We have a non-empty prop to match.
				// 3. This index is forced to choose.
				// 4. The needed columns are all covered by index columns(and handleCol).
				currentCandidate = ds.getIndexCandidate(path, prop)
			} else {
				continue
			}
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
			result := compareCandidates(candidates[i], currentCandidate)
			if result == 1 {
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else if result == -1 {
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}

	if ds.ctx.GetSessionVars().GetAllowPreferRangeScan() && len(candidates) > 1 {
		// If a candidate path is TiFlash-path or forced-path, we just keep them. For other candidate paths, if there exists
		// any range scan path, we remove full scan paths and keep range scan paths.
		preferredPaths := make([]*candidatePath, 0, len(candidates))
		var hasRangeScanPath bool
		for _, c := range candidates {
			if c.path.Forced || c.path.StoreType == kv.TiFlash {
				preferredPaths = append(preferredPaths, c)
				continue
			}
			var unsignedIntHandle bool
			if c.path.IsIntHandlePath && ds.tableInfo.PKIsHandle {
				if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
					unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
				}
			}
			if !ranger.HasFullRange(c.path.Ranges, unsignedIntHandle) {
				preferredPaths = append(preferredPaths, c)
				hasRangeScanPath = true
			}
		}
		if hasRangeScanPath {
			return preferredPaths
		}
	}

	return candidates
}

func (ds *DataSource) getPruningInfo(candidates []*candidatePath, prop *property.PhysicalProperty) string {
	if !ds.ctx.GetSessionVars().StmtCtx.InVerboseExplain || len(candidates) == len(ds.possibleAccessPaths) {
		return ""
	}
	if len(candidates) == 1 && len(candidates[0].path.Ranges) == 0 {
		// For TableDual, we don't need to output pruning info.
		return ""
	}
	names := make([]string, 0, len(candidates))
	var tableName string
	if ds.TableAsName.O == "" {
		tableName = ds.tableInfo.Name.O
	} else {
		tableName = ds.TableAsName.O
	}
	getSimplePathName := func(path *util.AccessPath) string {
		if path.IsTablePath() {
			if path.StoreType == kv.TiFlash {
				return tableName + "(tiflash)"
			}
			return tableName
		}
		return path.Index.Name.O
	}
	for _, cand := range candidates {
		if cand.path.PartialIndexPaths != nil {
			partialNames := make([]string, 0, len(cand.path.PartialIndexPaths))
			for _, partialPath := range cand.path.PartialIndexPaths {
				partialNames = append(partialNames, getSimplePathName(partialPath))
			}
			names = append(names, fmt.Sprintf("IndexMerge{%s}", strings.Join(partialNames, ",")))
		} else {
			names = append(names, getSimplePathName(cand.path))
		}
	}
	items := make([]string, 0, len(prop.SortItems))
	for _, item := range prop.SortItems {
		items = append(items, item.String())
	}
	return fmt.Sprintf("[%s] remain after pruning paths for %s given Prop{SortItems: [%s], TaskTp: %s}",
		strings.Join(names, ","), tableName, strings.Join(items, " "), prop.TaskTp)
}

func (ds *DataSource) isPointGetConvertableSchema() bool {
	for _, col := range ds.Columns {
		if col.Name.L == model.ExtraHandleName.L {
			continue
		}

		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return false
		}
	}
	return true
}

// findBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (t task, cntPlan int64, err error) {
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		planCounter.Dec(1)
		return nil, 1, nil
	}

	t = ds.getTask(prop)
	if t != nil {
		cntPlan = 1
		planCounter.Dec(1)
		return
	}
	var cnt int64
	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		t, cnt, err = ds.findBestTask(prop, planCounter, opt)
		if err != nil {
			return nil, 0, err
		}
		prop.CanAddEnforcer = true
		if t != invalidTask {
			ds.storeTask(prop, t)
			cntPlan = cnt
			return
		}
		// Next, get the bestTask with enforced prop
		prop.SortItems = []property.SortItem{}
		prop.MPPPartitionTp = property.AnyType
	} else if prop.MPPPartitionTp != property.AnyType {
		return invalidTask, 0, nil
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.CanAddEnforcer {
			*prop = *oldProp
			t = enforceProperty(prop, t, ds.basePlan.ctx)
			prop.CanAddEnforcer = true
		}
		ds.storeTask(prop, t)
		if ds.SampleInfo != nil && !t.invalid() {
			if _, ok := t.plan().(*PhysicalTableSample); !ok {
				warning := expression.ErrInvalidTableSample.GenWithStackByArgs("plan not supported")
				ds.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
			}
		}
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		planCounter.Dec(1)
		return t, 1, err
	}

	t = invalidTask
	candidates := ds.skylinePruning(prop)
	pruningInfo := ds.getPruningInfo(candidates, prop)
	defer func() {
		if err == nil && t != nil && !t.invalid() && pruningInfo != "" {
			if ds.ctx.GetSessionVars().StmtCtx.OptimInfo == nil {
				ds.ctx.GetSessionVars().StmtCtx.OptimInfo = make(map[int]string)
			}
			ds.ctx.GetSessionVars().StmtCtx.OptimInfo[t.plan().ID()] = pruningInfo
		}
	}()

	opt.buildPhysicalOptimizeTraceInfo(ds)
	cntPlan = 0
	for _, candidate := range candidates {
		path := candidate.path
		if path.PartialIndexPaths != nil {
			idxMergeTask, err := ds.convertToIndexMergeScan(prop, candidate, opt)
			if err != nil {
				return nil, 0, err
			}
			if !idxMergeTask.invalid() {
				cntPlan += 1
				planCounter.Dec(1)
			}
			appendCandidate(ds, idxMergeTask, prop, opt)
			if idxMergeTask.cost() < t.cost() || planCounter.Empty() {
				t = idxMergeTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			// We should uncache the tableDual plan.
			if expression.MaybeOverOptimized4PlanCache(ds.ctx, path.AccessConds) {
				ds.ctx.GetSessionVars().StmtCtx.SkipPlanCache = true
			}
			dual := PhysicalTableDual{}.Init(ds.ctx, ds.stats, ds.blockOffset)
			dual.SetSchema(ds.schema)
			cntPlan += 1
			planCounter.Dec(1)
			return &rootTask{
				p: dual,
			}, cntPlan, nil
		}
		canConvertPointGet := len(path.Ranges) > 0 && path.StoreType == kv.TiKV && ds.isPointGetConvertableSchema()

		if canConvertPointGet && expression.MaybeOverOptimized4PlanCache(ds.ctx, path.AccessConds) {
			canConvertPointGet = ds.canConvertToPointGetForPlanCache(path)
		}

		if canConvertPointGet && !path.IsIntHandlePath {
			// We simply do not build [batch] point get for prefix indexes. This can be optimized.
			canConvertPointGet = path.Index.Unique && !path.Index.HasPrefixIndex()
			// If any range cannot cover all columns of the index, we cannot build [batch] point get.
			idxColsLen := len(path.Index.Columns)
			for _, ran := range path.Ranges {
				if len(ran.LowVal) != idxColsLen {
					canConvertPointGet = false
					break
				}
			}
		}
		var hashPartColName *ast.ColumnName
		if tblInfo := ds.table.Meta(); canConvertPointGet && tblInfo.GetPartitionInfo() != nil {
			// We do not build [batch] point get for dynamic table partitions now. This can be optimized.
			if ds.ctx.GetSessionVars().UseDynamicPartitionPrune() {
				canConvertPointGet = false
			}
			if canConvertPointGet && len(path.Ranges) > 1 {
				// We can only build batch point get for hash partitions on a simple column now. This is
				// decided by the current implementation of `BatchPointGetExec::initialize()`, specifically,
				// the `getPhysID()` function. Once we optimize that part, we can come back and enable
				// BatchPointGet plan for more cases.
				hashPartColName = getHashPartitionColumnName(ds.ctx, tblInfo)
				if hashPartColName == nil {
					canConvertPointGet = false
				}
			}
			if canConvertPointGet {
				// If the schema contains ExtraPidColID, do not convert to point get.
				// Because the point get executor can not handle the extra partition ID column now.
				for _, col := range ds.schema.Columns {
					if col.ID == model.ExtraPidColID {
						canConvertPointGet = false
						break
					}
				}
			}
		}
		if canConvertPointGet {
			allRangeIsPoint := true
			for _, ran := range path.Ranges {
				if !ran.IsPointNonNullable(ds.ctx) {
					// unique indexes can have duplicated NULL rows so we cannot use PointGet if there is NULL
					allRangeIsPoint = false
					break
				}
			}
			if allRangeIsPoint {
				var pointGetTask task
				if len(path.Ranges) == 1 {
					pointGetTask = ds.convertToPointGet(prop, candidate, opt)
				} else {
					pointGetTask = ds.convertToBatchPointGet(prop, candidate, hashPartColName, opt)
				}
				appendCandidate(ds, pointGetTask, prop, opt)
				if !pointGetTask.invalid() {
					cntPlan += 1
					planCounter.Dec(1)
				}
				if pointGetTask.cost() < t.cost() || planCounter.Empty() {
					t = pointGetTask
					if planCounter.Empty() {
						return
					}
					continue
				}
			}
		}
		if path.IsTablePath() {
			if ds.preferStoreType&preferTiFlash != 0 && path.StoreType == kv.TiKV {
				continue
			}
			if ds.preferStoreType&preferTiKV != 0 && path.StoreType == kv.TiFlash {
				continue
			}
			var tblTask task
			if ds.SampleInfo != nil {
				tblTask, err = ds.convertToSampleTable(prop, candidate, opt)
			} else {
				tblTask, err = ds.convertToTableScan(prop, candidate, opt)
			}
			if err != nil {
				return nil, 0, err
			}
			if !tblTask.invalid() {
				cntPlan += 1
				planCounter.Dec(1)
			}
			appendCandidate(ds, tblTask, prop, opt)
			if tblTask.cost() < t.cost() || planCounter.Empty() {
				t = tblTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// TiFlash storage do not support index scan.
		if ds.preferStoreType&preferTiFlash != 0 {
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate, opt)
		if err != nil {
			return nil, 0, err
		}
		if !idxTask.invalid() {
			cntPlan += 1
			planCounter.Dec(1)
		}
		appendCandidate(ds, idxTask, prop, opt)
		if idxTask.cost() < t.cost() || planCounter.Empty() {
			t = idxTask
		}
		if planCounter.Empty() {
			return t, cntPlan, nil
		}
	}

	return
}

func (ds *DataSource) canConvertToPointGetForPlanCache(path *util.AccessPath) bool {
	// PointGet might contain some over-optimized assumptions, like `a>=1 and a<=1` --> `a=1`, but
	// these assumptions may be broken after parameters change.
	// So for safety, we narrow down the scope and just generate PointGet in some particular and simple scenarios.

	// scenario 1: each column corresponds to a single EQ, `a=1 and b=2 and c=3` --> `[1, 2, 3]`
	if len(path.Ranges) > 0 && path.Ranges[0].Width() == len(path.AccessConds) {
		for _, accessCond := range path.AccessConds {
			f, ok := accessCond.(*expression.ScalarFunction)
			if !ok {
				return false
			}
			if f.FuncName.L != ast.EQ {
				return false
			}
		}
		return true
	}
	return false
}

func (ds *DataSource) convertToIndexMergeScan(prop *property.PhysicalProperty, candidate *candidatePath, opt *physicalOptimizeOp) (task task, err error) {
	if prop.TaskTp != property.RootTaskType || !prop.IsSortItemEmpty() {
		return invalidTask, nil
	}
	path := candidate.path
	var totalCost float64
	scans := make([]PhysicalPlan, 0, len(path.PartialIndexPaths))
	cop := &copTask{
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
	}
	cop.partitionInfo = PartitionInfo{
		PruningConds:   pushDownNot(ds.ctx, ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	for _, partPath := range path.PartialIndexPaths {
		var scan PhysicalPlan
		var partialCost float64
		if partPath.IsTablePath() {
			scan, partialCost = ds.convertToPartialTableScan(prop, partPath)
		} else {
			scan, partialCost = ds.convertToPartialIndexScan(prop, partPath)
		}
		scans = append(scans, scan)
		totalCost += partialCost
	}
	totalRowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.stats.RowCount {
		totalRowCount *= prop.ExpectedCnt / ds.stats.RowCount
	}
	ts, partialCost, remainingFilters, err := ds.buildIndexMergeTableScan(prop, path.TableFilters, totalRowCount)
	if err != nil {
		return nil, err
	}
	totalCost += partialCost
	cop.tablePlan = ts
	cop.idxMergePartPlans = scans
	cop.cst = totalCost
	if remainingFilters != nil {
		cop.rootTaskConds = remainingFilters
	}
	task = cop.convertToRootTask(ds.ctx)
	ds.addSelection4PlanCache(task.(*rootTask), ds.tableStats.ScaleByExpectCnt(totalRowCount), prop)
	return task, nil
}

func (ds *DataSource) convertToPartialIndexScan(prop *property.PhysicalProperty, path *util.AccessPath) (
	indexPlan PhysicalPlan,
	partialCost float64) {
	is, partialCost, rowCount := ds.getOriginalPhysicalIndexScan(prop, path, false, false)
	rowSize := is.stats.HistColl.GetAvgRowSize(is.ctx, is.schema.Columns, true, false)
	// TODO: Consider using isCoveringIndex() to avoid another TableRead
	indexConds := path.IndexFilters
	sessVars := ds.ctx.GetSessionVars()
	if indexConds != nil {
		var selectivity float64
		partialCost += rowCount * sessVars.CopCPUFactor
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		rowCount = is.stats.RowCount * selectivity
		stats := &property.StatsInfo{RowCount: rowCount}
		stats.StatsVersion = ds.statisticTable.Version
		if ds.statisticTable.Pseudo {
			stats.StatsVersion = statistics.PseudoVersion
		}
		indexPlan := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, ds.blockOffset)
		indexPlan.SetChildren(is)
		partialCost += rowCount * rowSize * sessVars.GetNetworkFactor(ds.tableInfo)
		return indexPlan, partialCost
	}
	partialCost += rowCount * rowSize * sessVars.GetNetworkFactor(ds.tableInfo)
	indexPlan = is
	return indexPlan, partialCost
}

func checkColinSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

func (ds *DataSource) convertToPartialTableScan(prop *property.PhysicalProperty, path *util.AccessPath) (
	tablePlan PhysicalPlan, partialCost float64) {
	ts, partialCost, rowCount := ds.getOriginalPhysicalTableScan(prop, path, false)
	overwritePartialTableScanSchema(ds, ts)
	// remove ineffetive filter condition after overwriting physicalscan schema
	newFilterConds := make([]expression.Expression, 0, len(path.TableFilters))
	for _, cond := range ts.filterCondition {
		cols := expression.ExtractColumns(cond)
		if checkColinSchema(cols, ts.schema) {
			newFilterConds = append(newFilterConds, cond)
		}
	}
	ts.filterCondition = newFilterConds
	rowSize := ds.TblColHists.GetAvgRowSize(ds.ctx, ts.schema.Columns, false, false)
	sessVars := ds.ctx.GetSessionVars()
	if len(ts.filterCondition) > 0 {
		selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, ts.filterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		tablePlan = PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*rowCount), ds.blockOffset)
		tablePlan.SetChildren(ts)
		partialCost += rowCount * sessVars.CopCPUFactor
		partialCost += selectivity * rowCount * rowSize * sessVars.GetNetworkFactor(ds.tableInfo)
		return tablePlan, partialCost
	}
	partialCost += rowCount * rowSize * sessVars.GetNetworkFactor(ds.tableInfo)
	tablePlan = ts
	return tablePlan, partialCost
}

// overwritePartialTableScanSchema change the schema of partial table scan to handle columns.
func overwritePartialTableScanSchema(ds *DataSource, ts *PhysicalTableScan) {
	handleCols := ds.handleCols
	if handleCols == nil {
		handleCols = NewIntHandleCols(ds.newExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	infoCols := make([]*model.ColumnInfo, 0, hdColNum)
	for i := 0; i < hdColNum; i++ {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
		infoCols = append(infoCols, col.ToInfo())
	}
	ts.schema = expression.NewSchema(exprCols...)
	ts.Columns = infoCols
}

// setIndexMergeTableScanHandleCols set the handle columns of the table scan.
func setIndexMergeTableScanHandleCols(ds *DataSource, ts *PhysicalTableScan) (err error) {
	handleCols := ds.handleCols
	if handleCols == nil {
		handleCols = NewIntHandleCols(ds.newExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	for i := 0; i < hdColNum; i++ {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
	}
	ts.HandleCols, err = handleCols.ResolveIndices(expression.NewSchema(exprCols...))
	return
}

// buildIndexMergeTableScan() returns Selection that will be pushed to TiKV.
// Filters that cannot be pushed to TiKV are also returned, and an extra Selection above IndexMergeReader will be constructed later.
func (ds *DataSource) buildIndexMergeTableScan(prop *property.PhysicalProperty, tableFilters []expression.Expression,
	totalRowCount float64) (PhysicalPlan, float64, []expression.Expression, error) {
	var partialCost float64
	sessVars := ds.ctx.GetSessionVars()
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
		HandleCols:      ds.handleCols,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(ds.ctx, ds.blockOffset)
	ts.SetSchema(ds.schema.Clone())
	err := setIndexMergeTableScanHandleCols(ds, ts)
	if err != nil {
		return nil, 0, nil, err
	}
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	rowSize := ts.getScanRowSize()
	partialCost += totalRowCount * rowSize * sessVars.GetScanFactor(ds.tableInfo)
	ts.stats = ds.tableStats.ScaleByExpectCnt(totalRowCount)
	if ds.statisticTable.Pseudo {
		ts.stats.StatsVersion = statistics.PseudoVersion
	}
	if len(tableFilters) > 0 {
		pushedFilters, remainingFilters := extractFiltersForIndexMerge(sessVars.StmtCtx, ds.ctx.GetClient(), tableFilters)
		pushedFilters1, remainingFilters1 := SplitSelCondsWithVirtualColumn(pushedFilters)
		pushedFilters = pushedFilters1
		remainingFilters = append(remainingFilters, remainingFilters1...)
		if len(pushedFilters) != 0 {
			partialCost += totalRowCount * sessVars.CopCPUFactor
			selectivity, _, err := ds.tableStats.HistColl.Selectivity(ds.ctx, pushedFilters, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = SelectionFactor
			}
			sel := PhysicalSelection{Conditions: pushedFilters}.Init(ts.ctx, ts.stats.ScaleByExpectCnt(selectivity*totalRowCount), ts.blockOffset)
			sel.SetChildren(ts)
			return sel, partialCost, remainingFilters, nil
		}
		return ts, partialCost, remainingFilters, nil
	}
	return ts, partialCost, nil, nil
}

// extractFiltersForIndexMerge returns:
// `pushed`: exprs that can be pushed to TiKV.
// `remaining`: exprs that can NOT be pushed to TiKV but can be pushed to other storage engines.
// Why do we need this func?
// IndexMerge only works on TiKV, so we need to find all exprs that cannot be pushed to TiKV, and add a new Selection above IndexMergeReader.
// 	But the new Selection should exclude the exprs that can NOT be pushed to ALL the storage engines.
// 	Because these exprs have already been put in another Selection(check rule_predicate_push_down).
func extractFiltersForIndexMerge(sc *stmtctx.StatementContext, client kv.Client, filters []expression.Expression) (pushed []expression.Expression, remaining []expression.Expression) {
	for _, expr := range filters {
		if expression.CanExprsPushDown(sc, []expression.Expression{expr}, client, kv.TiKV) {
			pushed = append(pushed, expr)
			continue
		}
		if expression.CanExprsPushDown(sc, []expression.Expression{expr}, client, kv.UnSpecified) {
			remaining = append(remaining, expr)
		}
	}
	return
}

func indexCoveringCol(col *expression.Column, indexCols []*expression.Column, idxColLens []int) bool {
	for i, indexCol := range indexCols {
		isFullLen := idxColLens[i] == types.UnspecifiedLength || idxColLens[i] == col.RetType.GetFlen()
		if indexCol != nil && col.EqualByExprAndID(nil, indexCol) && isFullLen {
			return true
		}
	}
	return false
}

func (ds *DataSource) isCoveringIndex(columns, indexColumns []*expression.Column, idxColLens []int, tblInfo *model.TableInfo) bool {
	for _, col := range columns {
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.GetFlag()) {
			continue
		}
		if col.ID == model.ExtraHandleID {
			continue
		}
		coveredByPlainIndex := indexCoveringCol(col, indexColumns, idxColLens)
		coveredByClusteredIndex := indexCoveringCol(col, ds.commonHandleCols, ds.commonHandleLens)
		if !coveredByPlainIndex && !coveredByClusteredIndex {
			return false
		}
		isClusteredNewCollationIdx := collate.NewCollationEnabled() &&
			col.GetType().EvalType() == types.ETString &&
			!mysql.HasBinaryFlag(col.GetType().GetFlag())
		if !coveredByPlainIndex && coveredByClusteredIndex && isClusteredNewCollationIdx && ds.table.Meta().CommonHandleVersion == 0 {
			return false
		}
	}
	return true
}

// If there is a table reader which needs to keep order, we should append a pk to table scan.
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) (*expression.Column, bool) {
	handleCols := ds.handleCols
	if handleCols != nil {
		return handleCols.GetCol(0), false
	}
	handleCol := ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.Columns = append(ts.Columns, model.NewExtraHandleColInfo())
	return handleCol, true
}

// addSelection4PlanCache adds an extra safeguard selection upon this root task for safety.
// When reusing cached plans and rebuilding range for them, the range builder may return an loose range after parameters change.
// When we add the extra selection, it should meet two conditions:
// 1. The length of 'ds.pushedDownConds` should not be zero.
// 2. The result of function `MaybeOverOptimized4PlanCache(ds.pushedDownConds)` call needs to return true.
func (ds *DataSource) addSelection4PlanCache(task *rootTask, stats *property.StatsInfo, prop *property.PhysicalProperty) {
	if !expression.MaybeOverOptimized4PlanCache(ds.ctx, ds.pushedDownConds) || len(ds.pushedDownConds) == 0 {
		return
	}
	sel := PhysicalSelection{Conditions: ds.pushedDownConds}.Init(ds.ctx, stats, ds.blockOffset, prop)
	sel.SetChildren(task.p)
	task.p = sel
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty,
	candidate *candidatePath, opt *physicalOptimizeOp) (task task, err error) {
	if !candidate.path.IsSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return invalidTask, nil
		}
	} else if prop.TaskTp == property.CopDoubleReadTaskType {
		// If it's parent requires double read task, return max cost.
		return invalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	path := candidate.path
	is, cost, _ := ds.getOriginalPhysicalIndexScan(prop, path, candidate.isMatchProp, candidate.path.IsSingleScan)
	cop := &copTask{
		indexPlan:   is,
		tblColHists: ds.TblColHists,
		tblCols:     ds.TblCols,
		expectCnt:   uint64(prop.ExpectedCnt),
	}
	cop.partitionInfo = PartitionInfo{
		PruningConds:   pushDownNot(ds.ctx, ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	if !candidate.path.IsSingleScan {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         ds.Columns,
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			isPartition:     ds.isPartition,
			physicalTableID: ds.physicalTableID,
			tblCols:         ds.TblCols,
			tblColHists:     ds.TblColHists,
		}.Init(ds.ctx, is.blockOffset)
		ts.SetSchema(ds.schema.Clone())
		ts.SetCost(cost)
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.indexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.stats = &property.StatsInfo{StatsVersion: ds.tableStats.StatsVersion}
		cop.tablePlan = ts
	}
	cop.cst = cost
	task = cop
	if cop.tablePlan != nil && ds.tableInfo.IsCommonHandle {
		cop.commonHandleCols = ds.commonHandleCols
		commonHandle := ds.handleCols.(*CommonHandleCols)
		for _, col := range commonHandle.columns {
			if ds.schema.ColumnIndex(col) == -1 {
				ts := cop.tablePlan.(*PhysicalTableScan)
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
				cop.needExtraProj = true
			}
		}
	}
	if candidate.isMatchProp {
		if cop.tablePlan != nil && !ds.tableInfo.IsCommonHandle {
			col, isNew := cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.extraHandleCol = col
			cop.needExtraProj = cop.needExtraProj || isNew
		}
		cop.keepOrder = true
		// IndexScan on partition table can't keep order.
		if ds.tableInfo.GetPartitionInfo() != nil {
			return invalidTask, nil
		}
	}
	if cop.needExtraProj {
		cop.originSchema = ds.schema
	}
	// prop.IsSortItemEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of addPushedDownSelection.
	finalStats := ds.stats.ScaleByExpectCnt(prop.ExpectedCnt)
	is.addPushedDownSelection(cop, ds, path, finalStats)
	if prop.TaskTp == property.RootTaskType {
		task = task.convertToRootTask(ds.ctx)
		ds.addSelection4PlanCache(task.(*rootTask), finalStats, prop)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) getScanRowSize() float64 {
	idx := is.Index
	scanCols := make([]*expression.Column, 0, len(idx.Columns)+1)
	// If `initSchema` has already appended the handle column in schema, just use schema columns, otherwise, add extra handle column.
	if len(idx.Columns) == len(is.schema.Columns) {
		scanCols = append(scanCols, is.schema.Columns...)
		handleCol := is.pkIsHandleCol
		if handleCol != nil {
			scanCols = append(scanCols, handleCol)
		}
	} else {
		scanCols = is.schema.Columns
	}
	return is.tblColHists.GetIndexAvgRowSize(is.ctx, scanCols, is.Index.Unique)
}

// initSchema is used to set the schema of PhysicalIndexScan. Before calling this,
// make sure the following field of PhysicalIndexScan are initialized:
//   PhysicalIndexScan.Table         *model.TableInfo
//   PhysicalIndexScan.Index         *model.IndexInfo
//   PhysicalIndexScan.Index.Columns []*IndexColumn
//   PhysicalIndexScan.IdxCols       []*expression.Column
//   PhysicalIndexScan.Columns       []*model.ColumnInfo
func (is *PhysicalIndexScan) initSchema(idxExprCols []*expression.Column, isDoubleRead bool) {
	indexCols := make([]*expression.Column, len(is.IdxCols), len(is.Index.Columns)+1)
	copy(indexCols, is.IdxCols)

	for i := len(is.IdxCols); i < len(is.Index.Columns); i++ {
		if idxExprCols[i] != nil {
			indexCols = append(indexCols, idxExprCols[i])
		} else {
			// TODO: try to reuse the col generated when building the DataSource.
			indexCols = append(indexCols, &expression.Column{
				ID:       is.Table.Columns[is.Index.Columns[i].Offset].ID,
				RetType:  &is.Table.Columns[is.Index.Columns[i].Offset].FieldType,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			})
		}
	}
	is.NeedCommonHandle = is.Table.IsCommonHandle

	if is.NeedCommonHandle {
		for i := len(is.Index.Columns); i < len(idxExprCols); i++ {
			indexCols = append(indexCols, idxExprCols[i])
		}
	}
	setHandle := len(indexCols) > len(is.Index.Columns)
	if !setHandle {
		for i, col := range is.Columns {
			if (mysql.HasPriKeyFlag(col.GetFlag()) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
				indexCols = append(indexCols, is.dataSourceSchema.Columns[i])
				setHandle = true
				break
			}
		}
	}

	if isDoubleRead {
		// If it's double read case, the first index must return handle. So we should add extra handle column
		// if there isn't a handle column.
		if !setHandle {
			if !is.Table.IsCommonHandle {
				indexCols = append(indexCols, &expression.Column{
					RetType:  types.NewFieldType(mysql.TypeLonglong),
					ID:       model.ExtraHandleID,
					UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
				})
			}
		}
		// If index is global, we should add extra column for pid.
		if is.Index.Global {
			indexCols = append(indexCols, &expression.Column{
				RetType:  types.NewFieldType(mysql.TypeLonglong),
				ID:       model.ExtraPidColID,
				UniqueID: is.ctx.GetSessionVars().AllocPlanColumnID(),
			})
		}
	}

	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *copTask, p *DataSource, path *util.AccessPath, finalStats *property.StatsInfo) {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.IndexFilters, path.TableFilters
	tableConds, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(tableConds)

	var newRootConds []expression.Expression
	indexConds, newRootConds = expression.PushDownExprs(is.ctx.GetSessionVars().StmtCtx, indexConds, is.ctx.GetClient(), kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	tableConds, newRootConds = expression.PushDownExprs(is.ctx.GetSessionVars().StmtCtx, tableConds, is.ctx.GetClient(), kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	sessVars := is.ctx.GetSessionVars()
	if indexConds != nil {
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.stats.RowCount * selectivity
		stats := p.tableStats.ScaleByExpectCnt(count)
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.ctx, stats, is.blockOffset)
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if len(tableConds) > 0 {
		copTask.finishIndexPlan()
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		tableSel := PhysicalSelection{Conditions: tableConds}.Init(is.ctx, finalStats, is.blockOffset)
		if len(copTask.rootTaskConds) != 0 {
			selectivity, _, err := copTask.tblColHists.Selectivity(is.ctx, tableConds, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = SelectionFactor
			}
			tableSel.stats = copTask.plan().statsInfo().Scale(selectivity)
		}
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
	}
}

// SplitSelCondsWithVirtualColumn filter the select conditions which contain virtual column
func SplitSelCondsWithVirtualColumn(conds []expression.Expression) (withoutVirt []expression.Expression, withVirt []expression.Expression) {
	for i := range conds {
		if expression.ContainVirtualColumn(conds[i : i+1]) {
			withVirt = append(withVirt, conds[i])
		} else {
			withoutVirt = append(withoutVirt, conds[i])
		}
	}
	return withoutVirt, withVirt
}

func matchIndicesProp(idxCols []*expression.Column, colLens []int, propItems []property.SortItem) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.Col.EqualByExprAndID(nil, idxCols[i]) {
			return false
		}
	}
	return true
}

func (ds *DataSource) splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*expression.Column, idxColLens []int,
	table *model.TableInfo) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		if ds.isCoveringIndex(expression.ExtractColumns(cond), indexColumns, idxColLens, table) {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// getMostCorrCol4Handle checks if column in the condition is correlated enough with handle. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrCol4Handle(exprs []expression.Expression, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, exprs, nil)
	if len(cols) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		hist, ok := histColl.Columns[col.ID]
		if !ok {
			continue
		}
		curCorr := hist.Correlation
		if corrCol == nil || math.Abs(corr) < math.Abs(curCorr) {
			corrCol = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && math.Abs(corr) >= threshold {
		return corrCol, corr
	}
	return nil, corr
}

// getColumnRangeCounts estimates row count for each range respectively.
func getColumnRangeCounts(sctx sessionctx.Context, colID int64, ranges []*ranger.Range, histColl *statistics.HistColl, idxID int64) ([]float64, bool) {
	var err error
	var count float64
	rangeCounts := make([]float64, len(ranges))
	for i, ran := range ranges {
		if idxID >= 0 {
			idxHist := histColl.Indices[idxID]
			if idxHist == nil || idxHist.IsInvalid(false) {
				return nil, false
			}
			count, err = histColl.GetRowCountByIndexRanges(sctx, idxID, []*ranger.Range{ran})
		} else {
			colHist, ok := histColl.Columns[colID]
			if !ok || colHist.IsInvalid(sctx, false) {
				return nil, false
			}
			count, err = histColl.GetRowCountByColumnRanges(sctx, colID, []*ranger.Range{ran})
		}
		if err != nil {
			return nil, false
		}
		rangeCounts[i] = count
	}
	return rangeCounts, true
}

// convertRangeFromExpectedCnt builds new ranges used to estimate row count we need to scan in table scan before finding specified
// number of tuples which fall into input ranges.
func convertRangeFromExpectedCnt(ranges []*ranger.Range, rangeCounts []float64, expectedCnt float64, desc bool) ([]*ranger.Range, float64, bool) {
	var i int
	var count float64
	var convertedRanges []*ranger.Range
	if desc {
		for i = len(ranges) - 1; i >= 0; i-- {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i < 0 {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: ranges[i].HighVal, HighVal: []types.Datum{types.MaxValueDatum()}, LowExclude: !ranges[i].HighExclude, Collators: ranges[i].Collators}}
	} else {
		for i = 0; i < len(ranges); i++ {
			if count+rangeCounts[i] >= expectedCnt {
				break
			}
			count += rangeCounts[i]
		}
		if i == len(ranges) {
			return nil, 0, true
		}
		convertedRanges = []*ranger.Range{{LowVal: []types.Datum{{}}, HighVal: ranges[i].LowVal, HighExclude: !ranges[i].LowExclude, Collators: ranges[i].Collators}}
	}
	return convertedRanges, count, false
}

// crossEstimateTableRowCount estimates row count of table scan using histogram of another column which is in TableFilters
// and has high order correlation with handle column. For example, if the query is like:
// `select * from tbl where a = 1 order by pk limit 1`
// if order of column `a` is strictly correlated with column `pk`, the row count of table scan should be:
// `1 + row_count(a < 1 or a is null)`
func (ds *DataSource) crossEstimateTableRowCount(path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	if ds.statisticTable.Pseudo || len(path.TableFilters) == 0 || !ds.ctx.GetSessionVars().EnableCorrelationAdjustment {
		return 0, false, 0
	}
	col, corr := getMostCorrCol4Handle(path.TableFilters, ds.statisticTable, ds.ctx.GetSessionVars().CorrelationThreshold)
	return ds.crossEstimateRowCount(path, path.TableFilters, col, corr, expectedCnt, desc)
}

// crossEstimateRowCount is the common logic of crossEstimateTableRowCount and crossEstimateIndexRowCount.
func (ds *DataSource) crossEstimateRowCount(path *util.AccessPath, conds []expression.Expression, col *expression.Column, corr, expectedCnt float64, desc bool) (float64, bool, float64) {
	// If the scan is not full range scan, we cannot use histogram of other columns for estimation, because
	// the histogram reflects value distribution in the whole table level.
	if col == nil || len(path.AccessConds) > 0 {
		return 0, false, corr
	}
	colID := col.UniqueID
	if corr < 0 {
		desc = !desc
	}
	accessConds, remained := ranger.DetachCondsForColumn(ds.ctx, conds, col)
	if len(accessConds) == 0 {
		return 0, false, corr
	}
	ranges, err := ranger.BuildColumnRange(accessConds, ds.ctx, col.RetType, types.UnspecifiedLength)
	if len(ranges) == 0 || err != nil {
		return 0, err == nil, corr
	}
	idxID, idxExists := ds.stats.HistColl.ColID2IdxID[colID]
	if !idxExists {
		idxID = -1
	}
	rangeCounts, ok := getColumnRangeCounts(ds.ctx, colID, ranges, ds.tableStats.HistColl, idxID)
	if !ok {
		return 0, false, corr
	}
	convertedRanges, count, isFull := convertRangeFromExpectedCnt(ranges, rangeCounts, expectedCnt, desc)
	if isFull {
		return path.CountAfterAccess, true, 0
	}
	var rangeCount float64
	if idxExists {
		rangeCount, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(ds.ctx, idxID, convertedRanges)
	} else {
		rangeCount, err = ds.tableStats.HistColl.GetRowCountByColumnRanges(ds.ctx, colID, convertedRanges)
	}
	if err != nil {
		return 0, false, corr
	}
	scanCount := rangeCount + expectedCnt - count
	if len(remained) > 0 {
		scanCount = scanCount / SelectionFactor
	}
	scanCount = math.Min(scanCount, path.CountAfterAccess)
	return scanCount, true, 0
}

// crossEstimateIndexRowCount estimates row count of index scan using histogram of another column which is in TableFilters/IndexFilters
// and has high order correlation with the first index column. For example, if the query is like:
// `select * from tbl where a = 1 order by b limit 1`
// if order of column `a` is strictly correlated with column `b`, the row count of IndexScan(b) should be:
// `1 + row_count(a < 1 or a is null)`
func (ds *DataSource) crossEstimateIndexRowCount(path *util.AccessPath, expectedCnt float64, desc bool) (float64, bool, float64) {
	filtersLen := len(path.TableFilters) + len(path.IndexFilters)
	sessVars := ds.ctx.GetSessionVars()
	if ds.statisticTable.Pseudo || filtersLen == 0 || !sessVars.EnableExtendedStats || !ds.ctx.GetSessionVars().EnableCorrelationAdjustment {
		return 0, false, 0
	}
	col, corr := getMostCorrCol4Index(path, ds.statisticTable, sessVars.CorrelationThreshold)
	filters := make([]expression.Expression, 0, filtersLen)
	filters = append(filters, path.TableFilters...)
	filters = append(filters, path.IndexFilters...)
	return ds.crossEstimateRowCount(path, filters, col, corr, expectedCnt, desc)
}

// getMostCorrCol4Index checks if column in the condition is correlated enough with the first index column. If the condition
// contains multiple columns, return nil and get the max correlation, which would be used in the heuristic estimation.
func getMostCorrCol4Index(path *util.AccessPath, histColl *statistics.Table, threshold float64) (*expression.Column, float64) {
	if histColl.ExtendedStats == nil || len(histColl.ExtendedStats.Stats) == 0 {
		return nil, 0
	}
	var cols []*expression.Column
	cols = expression.ExtractColumnsFromExpressions(cols, path.TableFilters, nil)
	cols = expression.ExtractColumnsFromExpressions(cols, path.IndexFilters, nil)
	if len(cols) == 0 {
		return nil, 0
	}
	colSet := set.NewInt64Set()
	var corr float64
	var corrCol *expression.Column
	for _, col := range cols {
		if colSet.Exist(col.UniqueID) {
			continue
		}
		colSet.Insert(col.UniqueID)
		curCorr := float64(0)
		for _, item := range histColl.ExtendedStats.Stats {
			if (col.ID == item.ColIDs[0] && path.FullIdxCols[0].ID == item.ColIDs[1]) ||
				(col.ID == item.ColIDs[1] && path.FullIdxCols[0].ID == item.ColIDs[0]) {
				curCorr = item.ScalarVals
				break
			}
		}
		if corrCol == nil || math.Abs(corr) < math.Abs(curCorr) {
			corrCol = col
			corr = curCorr
		}
	}
	if len(colSet) == 1 && math.Abs(corr) >= threshold {
		return corrCol, corr
	}
	return nil, corr
}

// GetPhysicalScan returns PhysicalTableScan for the LogicalTableScan.
func (s *LogicalTableScan) GetPhysicalScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalTableScan {
	ds := s.Source
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(s.ctx, s.blockOffset)
	ts.stats = stats
	ts.SetSchema(schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	return ts
}

// GetPhysicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func (s *LogicalIndexScan) GetPhysicalIndexScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          s.Columns,
		Index:            s.Index,
		IdxCols:          s.IdxCols,
		IdxColLens:       s.IdxColLens,
		AccessCondition:  s.AccessConds,
		Ranges:           s.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
		tblColHists:      ds.TblColHists,
		pkIsHandleCol:    ds.getPKIsHandleCol(),
	}.Init(ds.ctx, ds.blockOffset)
	is.stats = stats
	is.initSchema(s.FullIdxCols, s.IsDoubleRead)
	return is
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath, opt *physicalOptimizeOp) (task task, err error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopDoubleReadTaskType {
		return invalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	ts, cost, _ := ds.getOriginalPhysicalTableScan(prop, candidate.path, candidate.isMatchProp)
	if ts.KeepOrder && ts.Desc && ts.StoreType == kv.TiFlash {
		return invalidTask, nil
	}
	if prop.TaskTp == property.MppTaskType {
		if ts.KeepOrder {
			return invalidTask, nil
		}
		if prop.MPPPartitionTp != property.AnyType || ts.isPartition {
			// If ts is a single partition, then this partition table is in static-only prune, then we should not choose mpp execution.
			ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because table `" + ds.tableInfo.Name.O + "`is a partition table which is not supported when `@@tidb_partition_prune_mode=static`.")
			return invalidTask, nil
		}
		for _, col := range ts.schema.Columns {
			if col.VirtualExpr != nil {
				ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because column `" + col.OrigName + "` is a virtual column which is not supported now.")
				return invalidTask, nil
			}
		}
		mppTask := &mppTask{
			p:      ts,
			cst:    cost,
			partTp: property.AnyType,
		}
		ts.PartitionInfo = PartitionInfo{
			PruningConds:   pushDownNot(ds.ctx, ds.allConds),
			PartitionNames: ds.partitionNames,
			Columns:        ds.TblCols,
			ColumnNames:    ds.names,
		}
		ts.cost = cost
		mppTask = ts.addPushedDownSelectionToMppTask(mppTask, ds.stats)
		return mppTask, nil
	}
	copTask := &copTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
		cst:               cost,
	}
	copTask.partitionInfo = PartitionInfo{
		PruningConds:   pushDownNot(ds.ctx, ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	ts.PartitionInfo = copTask.partitionInfo
	task = copTask
	if candidate.isMatchProp {
		copTask.keepOrder = true
		// TableScan on partition table can't keep order.
		if ds.tableInfo.GetPartitionInfo() != nil {
			return invalidTask, nil
		}
	}
	ts.cost = task.cost()
	ts.addPushedDownSelection(copTask, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt))
	if prop.IsFlashProp() && len(copTask.rootTaskConds) != 0 {
		return invalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.convertToRootTask(ds.ctx)
		ds.addSelection4PlanCache(task.(*rootTask), ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), prop)
	} else if _, ok := task.(*rootTask); ok {
		return invalidTask, nil
	}
	return task, nil
}

func (ds *DataSource) convertToSampleTable(prop *property.PhysicalProperty,
	candidate *candidatePath, opt *physicalOptimizeOp) (task task, err error) {
	if prop.TaskTp == property.CopDoubleReadTaskType {
		return invalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return invalidTask, nil
	}
	if candidate.isMatchProp {
		// TableSample on partition table can't keep order.
		if ds.tableInfo.GetPartitionInfo() != nil {
			return invalidTask, nil
		}
	}
	p := PhysicalTableSample{
		TableSampleInfo: ds.SampleInfo,
		TableInfo:       ds.table,
		Desc:            candidate.isMatchProp && prop.SortItems[0].Desc,
	}.Init(ds.ctx, ds.SelectBlockOffset())
	p.schema = ds.schema
	return &rootTask{
		p: p,
	}, nil
}

func (ds *DataSource) convertToPointGet(prop *property.PhysicalProperty, candidate *candidatePath, opt *physicalOptimizeOp) (task task) {
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CopDoubleReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return invalidTask
	}

	if tidbutil.IsMemDB(ds.DBName.L) {
		return invalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(1))
	pointGetPlan := PointGetPlan{
		ctx:              ds.ctx,
		AccessConditions: candidate.path.AccessConds,
		schema:           ds.schema.Clone(),
		dbName:           ds.DBName.L,
		TblInfo:          ds.TableInfo(),
		outputNames:      ds.OutputNames(),
		LockWaitTime:     ds.ctx.GetSessionVars().LockWaitTimeout,
		Columns:          ds.Columns,
	}.Init(ds.ctx, ds.tableStats.ScaleByExpectCnt(accessCnt), ds.blockOffset)
	var partitionInfo *model.PartitionDefinition
	if ds.isPartition {
		if pi := ds.tableInfo.GetPartitionInfo(); pi != nil {
			for i := range pi.Definitions {
				def := pi.Definitions[i]
				if def.ID == ds.physicalTableID {
					partitionInfo = &def
					break
				}
			}
		}
		if partitionInfo == nil {
			return invalidTask
		}
	}
	rTsk := &rootTask{p: pointGetPlan}
	var cost float64
	if candidate.path.IsIntHandlePath {
		pointGetPlan.Handle = kv.IntHandle(candidate.path.Ranges[0].LowVal[0].GetInt64())
		pointGetPlan.UnsignedHandle = mysql.HasUnsignedFlag(ds.handleCols.GetCol(0).RetType.GetFlag())
		pointGetPlan.PartitionInfo = partitionInfo
		pointGetPlan.accessCols = ds.TblCols
		cost = pointGetPlan.GetCost()
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.IdxCols = candidate.path.IdxCols
		pointGetPlan.IdxColLens = candidate.path.IdxColLens
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		pointGetPlan.PartitionInfo = partitionInfo
		if candidate.path.IsSingleScan {
			pointGetPlan.accessCols = candidate.path.IdxCols
		} else {
			pointGetPlan.accessCols = ds.TblCols
		}
		cost = pointGetPlan.GetCost()
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += pointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(pointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	rTsk.p.SetCost(cost)
	return rTsk
}

func (ds *DataSource) convertToBatchPointGet(prop *property.PhysicalProperty,
	candidate *candidatePath, hashPartColName *ast.ColumnName, opt *physicalOptimizeOp) (task task) {
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return invalidTask
	}
	if prop.TaskTp == property.CopDoubleReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return invalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(len(candidate.path.Ranges)))
	batchPointGetPlan := BatchPointGetPlan{
		ctx:              ds.ctx,
		AccessConditions: candidate.path.AccessConds,
		TblInfo:          ds.TableInfo(),
		KeepOrder:        !prop.IsSortItemEmpty(),
		Columns:          ds.Columns,
		SinglePart:       ds.isPartition,
		PartTblID:        ds.physicalTableID,
		PartitionExpr:    getPartitionExpr(ds.ctx, ds.TableInfo()),
	}.Init(ds.ctx, ds.tableStats.ScaleByExpectCnt(accessCnt), ds.schema.Clone(), ds.names, ds.blockOffset)
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.SortItems[0].Desc
	}
	rTsk := &rootTask{p: batchPointGetPlan}
	var cost float64
	if candidate.path.IsIntHandlePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, kv.IntHandle(ran.LowVal[0].GetInt64()))
		}
		batchPointGetPlan.accessCols = ds.TblCols
		cost = batchPointGetPlan.GetCost()
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	} else {
		batchPointGetPlan.IndexInfo = candidate.path.Index
		batchPointGetPlan.IdxCols = candidate.path.IdxCols
		batchPointGetPlan.IdxColLens = candidate.path.IdxColLens
		batchPointGetPlan.PartitionColPos = getHashPartitionColumnPos(candidate.path.Index, hashPartColName)
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.IndexValues = append(batchPointGetPlan.IndexValues, ran.LowVal)
		}
		if !prop.IsSortItemEmpty() {
			batchPointGetPlan.KeepOrder = true
			batchPointGetPlan.Desc = prop.SortItems[0].Desc
		}
		if candidate.path.IsSingleScan {
			batchPointGetPlan.accessCols = candidate.path.IdxCols
		} else {
			batchPointGetPlan.accessCols = ds.TblCols
		}
		cost = batchPointGetPlan.GetCost()
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sessVars := ds.ctx.GetSessionVars()
			cost += batchPointGetPlan.stats.RowCount * sessVars.CPUFactor
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.ctx, ds.stats.ScaleByExpectCnt(prop.ExpectedCnt), ds.blockOffset)
			sel.SetChildren(batchPointGetPlan)
			rTsk.p = sel
		}
	}

	rTsk.cst = cost
	rTsk.p.SetCost(cost)
	return rTsk
}

func (ts *PhysicalTableScan) addPushedDownSelectionToMppTask(mpp *mppTask, stats *property.StatsInfo) *mppTask {
	filterCondition, rootTaskConds := SplitSelCondsWithVirtualColumn(ts.filterCondition)
	var newRootConds []expression.Expression
	filterCondition, newRootConds = expression.PushDownExprs(ts.ctx.GetSessionVars().StmtCtx, filterCondition, ts.ctx.GetClient(), ts.StoreType)
	rootTaskConds = append(rootTaskConds, newRootConds...)
	if len(rootTaskConds) > 0 {
		return &mppTask{}
	}
	ts.filterCondition = filterCondition
	// Add filter condition to table plan now.
	sessVars := ts.ctx.GetSessionVars()
	if len(ts.filterCondition) > 0 {
		mpp.cst += mpp.count() * sessVars.CopCPUFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats, ts.blockOffset)
		sel.SetChildren(ts)
		sel.cost = mpp.cst
		mpp.p = sel
	}
	return mpp
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *copTask, stats *property.StatsInfo) {
	ts.filterCondition, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(ts.filterCondition)
	var newRootConds []expression.Expression
	ts.filterCondition, newRootConds = expression.PushDownExprs(ts.ctx.GetSessionVars().StmtCtx, ts.filterCondition, ts.ctx.GetClient(), ts.StoreType)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	// Add filter condition to table plan now.
	sessVars := ts.ctx.GetSessionVars()
	if len(ts.filterCondition) > 0 {
		copTask.cst += copTask.count() * sessVars.CopCPUFactor
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.ctx, stats, ts.blockOffset)
		if len(copTask.rootTaskConds) != 0 {
			selectivity, _, err := copTask.tblColHists.Selectivity(ts.ctx, ts.filterCondition, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = SelectionFactor
			}
			sel.stats = ts.stats.Scale(selectivity)
		}
		sel.SetChildren(ts)
		sel.cost = copTask.cst
		copTask.tablePlan = sel
	}
}

func (ts *PhysicalTableScan) getScanRowSize() float64 {
	if ts.StoreType == kv.TiKV {
		return ts.tblColHists.GetTableAvgRowSize(ts.ctx, ts.tblCols, ts.StoreType, true)
	}
	// If `ts.handleCol` is nil, then the schema of tableScan doesn't have handle column.
	// This logic can be ensured in column pruning.
	return ts.tblColHists.GetTableAvgRowSize(ts.ctx, ts.Schema().Columns, ts.StoreType, ts.HandleCols != nil)
}

func (ds *DataSource) getOriginalPhysicalTableScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool) (*PhysicalTableScan, float64, float64) {
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.isPartition,
		physicalTableID: ds.physicalTableID,
		Ranges:          path.Ranges,
		AccessCondition: path.AccessConds,
		StoreType:       path.StoreType,
		HandleCols:      ds.handleCols,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(ds.ctx, ds.blockOffset)
	ts.filterCondition = make([]expression.Expression, len(path.TableFilters))
	copy(ts.filterCondition, path.TableFilters)
	ts.SetSchema(ds.schema.Clone())
	if ts.Table.PKIsHandle {
		if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
			if ds.statisticTable.Columns[pkColInfo.ID] != nil {
				ts.Hist = &ds.statisticTable.Columns[pkColInfo.ID].Histogram
			}
		}
	}
	rowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.stats.RowCount {
		selectivity := ds.stats.RowCount / path.CountAfterAccess
		uniformEst := math.Min(path.CountAfterAccess, prop.ExpectedCnt/selectivity)

		corrEst, ok, corr := ds.crossEstimateTableRowCount(path, prop.ExpectedCnt, isMatchProp && prop.SortItems[0].Desc)
		if ok {
			// TODO: actually, before using this count as the estimated row count of table scan, we need additionally
			// check if count < row_count(first_region | last_region), and use the larger one since we build one copTask
			// for one region now, so even if it is `limit 1`, we have to scan at least one region in table scan.
			// Currently, we can use `tikvrpc.CmdDebugGetRegionProperties` interface as `getSampRegionsRowCount()` does
			// to get the row count in a region, but that result contains MVCC old version rows, so it is not that accurate.
			// Considering that when this scenario happens, the execution time is close between IndexScan and TableScan,
			// we do not add this check temporarily.

			// to reduce risks of correlation adjustment, use the maximum between uniformEst and corrEst
			rowCount = math.Max(uniformEst, corrEst)
		} else if abs := math.Abs(corr); abs < 1 {
			correlationFactor := math.Pow(1-abs, float64(ds.ctx.GetSessionVars().CorrelationExpFactor))
			rowCount = math.Min(path.CountAfterAccess, uniformEst/correlationFactor)
		}
	}
	// We need NDV of columns since it may be used in cost estimation of join. Precisely speaking,
	// we should track NDV of each histogram bucket, and sum up the NDV of buckets we actually need
	// to scan, but this would only help improve accuracy of NDV for one column, for other columns,
	// we still need to assume values are uniformly distributed. For simplicity, we use uniform-assumption
	// for all columns now, as we do in `deriveStatsByFilter`.
	ts.stats = ds.tableStats.ScaleByExpectCnt(rowCount)
	rowSize := ts.getScanRowSize()
	sessVars := ds.ctx.GetSessionVars()
	cost := rowCount * rowSize * sessVars.GetScanFactor(ds.tableInfo)
	if isMatchProp {
		ts.Desc = prop.SortItems[0].Desc
		if prop.SortItems[0].Desc && prop.ExpectedCnt >= smallScanThreshold {
			cost = rowCount * rowSize * sessVars.GetDescScanFactor(ds.tableInfo)
		}
		ts.KeepOrder = true
	}
	return ts, cost, rowCount
}

func (ds *DataSource) getOriginalPhysicalIndexScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool, isSingleScan bool) (*PhysicalIndexScan, float64, float64) {
	idx := path.Index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          ds.Columns,
		Index:            idx,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		AccessCondition:  path.AccessConds,
		Ranges:           path.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.isPartition,
		physicalTableID:  ds.physicalTableID,
		tblColHists:      ds.TblColHists,
		pkIsHandleCol:    ds.getPKIsHandleCol(),
	}.Init(ds.ctx, ds.blockOffset)
	statsTbl := ds.statisticTable
	if statsTbl.Indices[idx.ID] != nil {
		is.Hist = &statsTbl.Indices[idx.ID].Histogram
	}
	rowCount := path.CountAfterAccess
	is.initSchema(append(path.FullIdxCols, ds.commonHandleCols...), !isSingleScan)
	if (isMatchProp || prop.IsSortItemEmpty()) && prop.ExpectedCnt < ds.stats.RowCount {
		count, ok, corr := ds.crossEstimateIndexRowCount(path, prop.ExpectedCnt, isMatchProp && prop.SortItems[0].Desc)
		if ok {
			rowCount = count
		} else if abs := math.Abs(corr); abs < 1 {
			correlationFactor := math.Pow(1-abs, float64(ds.ctx.GetSessionVars().CorrelationExpFactor))
			selectivity := ds.stats.RowCount / rowCount
			rowCount = math.Min(prop.ExpectedCnt/selectivity/correlationFactor, rowCount)
		}
	}
	is.stats = ds.tableStats.ScaleByExpectCnt(rowCount)
	rowSize := is.getScanRowSize()
	sessVars := ds.ctx.GetSessionVars()
	cost := rowCount * rowSize * sessVars.GetScanFactor(ds.tableInfo)
	if isMatchProp {
		is.Desc = prop.SortItems[0].Desc
		if prop.SortItems[0].Desc && prop.ExpectedCnt >= smallScanThreshold {
			cost = rowCount * rowSize * sessVars.GetDescScanFactor(ds.tableInfo)
		}
		is.KeepOrder = true
	}
	is.cost = cost
	return is, cost, rowCount
}

func (p *LogicalCTE) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (t task, cntPlan int64, err error) {
	if !prop.IsSortItemEmpty() && !prop.CanAddEnforcer {
		return invalidTask, 1, nil
	}
	// The physical plan has been build when derive stats.
	pcte := PhysicalCTE{SeedPlan: p.cte.seedPartPhysicalPlan, RecurPlan: p.cte.recursivePartPhysicalPlan, CTE: p.cte, cteAsName: p.cteAsName}.Init(p.ctx, p.stats)
	pcte.SetSchema(p.schema)
	cst := p.cte.seedPartPhysicalPlan.Cost()
	if p.cte.recursivePartPhysicalPlan != nil {
		cst += p.cte.recursivePartPhysicalPlan.Cost()
	}
	t = &rootTask{pcte, cst, false}
	if prop.CanAddEnforcer {
		t = enforceProperty(prop, t, p.basePlan.ctx)
	}
	return t, 1, nil
}

func (p *LogicalCTETable) findBestTask(prop *property.PhysicalProperty, planCounter *PlanCounterTp, opt *physicalOptimizeOp) (t task, cntPlan int64, err error) {
	if !prop.IsSortItemEmpty() {
		return nil, 1, nil
	}

	pcteTable := PhysicalCTETable{IDForStorage: p.idForStorage}.Init(p.ctx, p.stats)
	pcteTable.SetSchema(p.schema)
	t = &rootTask{p: pcteTable}
	return t, 1, nil
}

func appendCandidate(lp LogicalPlan, task task, prop *property.PhysicalProperty, opt *physicalOptimizeOp) {
	if task == nil || task.invalid() {
		return
	}
	opt.appendCandidate(lp, task.plan(), prop)
}

// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from conds, the condition like
// 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
func pushDownNot(ctx sessionctx.Context, conds []expression.Expression) []expression.Expression {
	for i, cond := range conds {
		conds[i] = expression.PushDownNot(ctx, cond)
	}
	return conds
}
