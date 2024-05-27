// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// RebuildPlan4CachedPlan will rebuild this plan under current user parameters.
func RebuildPlan4CachedPlan(p base.Plan) (ok bool) {
	sc := p.SCtx().GetSessionVars().StmtCtx
	if !sc.UseCache() {
		return false // plan-cache is disabled for this query
	}

	sc.InPreparedPlanBuilding = true
	defer func() { sc.InPreparedPlanBuilding = false }()
	if err := rebuildRange(p); err != nil {
		sc.AppendWarning(errors.NewNoStackErrorf("skip plan-cache: plan rebuild failed, %s", err.Error()))
		return false // fail to rebuild ranges
	}
	if !sc.UseCache() {
		// in this case, the UseCache flag changes from `true` to `false`, then there must be some
		// over-optimized operations were triggered, return `false` for safety here.
		return false
	}
	return true
}

func updateRange(p base.PhysicalPlan, ranges ranger.Ranges, rangeInfo string) {
	switch x := p.(type) {
	case *PhysicalTableScan:
		x.Ranges = ranges
		x.rangeInfo = rangeInfo
	case *PhysicalIndexScan:
		x.Ranges = ranges
		x.rangeInfo = rangeInfo
	case *PhysicalTableReader:
		updateRange(x.TablePlans[0], ranges, rangeInfo)
	case *PhysicalIndexReader:
		updateRange(x.IndexPlans[0], ranges, rangeInfo)
	case *PhysicalIndexLookUpReader:
		updateRange(x.IndexPlans[0], ranges, rangeInfo)
	}
}

// rebuildRange doesn't set mem limit for building ranges. There are two reasons why we don't restrict range mem usage here.
//  1. The cached plan must be able to build complete ranges under mem limit when it is generated. Hence we can just build
//     ranges from x.AccessConditions. The only difference between the last ranges and new ranges is the change of parameter
//     values, which doesn't cause much change on the mem usage of complete ranges.
//  2. Different parameter values can change the mem usage of complete ranges. If we set range mem limit here, range fallback
//     may heppen and cause correctness problem. For example, a in (?, ?, ?) is the access condition. When the plan is firstly
//     generated, its complete ranges are ['a','a'], ['b','b'], ['c','c'], whose mem usage is under range mem limit 100B.
//     When the cached plan is hit, the complete ranges may become ['aaa','aaa'], ['bbb','bbb'], ['ccc','ccc'], whose mem
//     usage exceeds range mem limit 100B, and range fallback happens and tidb may fetch more rows than users expect.
func rebuildRange(p base.Plan) error {
	sctx := p.SCtx()
	sc := p.SCtx().GetSessionVars().StmtCtx
	var err error
	switch x := p.(type) {
	case *PhysicalIndexHashJoin:
		return rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexMergeJoin:
		return rebuildRange(&x.PhysicalIndexJoin)
	case *PhysicalIndexJoin:
		if err := x.Ranges.Rebuild(); err != nil {
			return err
		}
		if mutableRange, ok := x.Ranges.(*mutableIndexJoinRange); ok {
			helper := mutableRange.buildHelper
			rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.IdxCols, mutableRange.outerJoinKeys)
			innerPlan := x.Children()[x.InnerChildIdx]
			updateRange(innerPlan, x.Ranges.Range(), rangeInfo)
		}
		for _, child := range x.Children() {
			err = rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *PhysicalTableScan:
		err = buildRangeForTableScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalIndexScan:
		err = buildRangeForIndexScan(sctx, x)
		if err != nil {
			return err
		}
	case *PhysicalTableReader:
		err = rebuildRange(x.TablePlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexReader:
		err = rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PhysicalIndexLookUpReader:
		err = rebuildRange(x.IndexPlans[0])
		if err != nil {
			return err
		}
	case *PointGetPlan:
		if x.TblInfo.GetPartitionInfo() != nil {
			if fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix33031, false) {
				return errors.NewNoStackError("Fix33031 fix-control set and partitioned table in cached Point Get plan")
			}
		}
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx.GetRangerCtx(), x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != 1 || !isSafeRange(x.AccessConditions, ranges, false, nil) {
					return errors.New("rebuild to get an unsafe range")
				}
				for i := range x.IndexValues {
					x.IndexValues[i] = ranges.Ranges[0].LowVal[i]
				}
			} else {
				var pkCol *expression.Column
				var unsignedIntHandle bool
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
					if !x.TblInfo.IsCommonHandle {
						unsignedIntHandle = true
					}
				}
				if pkCol != nil {
					ranges, accessConds, remainingConds, err := ranger.BuildTableRange(x.AccessConditions, x.ctx.GetRangerCtx(), pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) != 1 || !isSafeRange(x.AccessConditions, &ranger.DetachRangeResult{
						Ranges:        ranges,
						AccessConds:   accessConds,
						RemainedConds: remainingConds,
					}, unsignedIntHandle, nil) {
						return errors.New("rebuild to get an unsafe range")
					}
					x.Handle = kv.IntHandle(ranges[0].LowVal[0].GetInt64())
				}
			}
		}
		if x.HandleConstant != nil {
			dVal, err := convertConstant2Datum(sctx, x.HandleConstant, x.handleFieldType)
			if err != nil {
				return err
			}
			iv, err := dVal.ToInt64(sc.TypeCtx())
			if err != nil {
				return err
			}
			x.Handle = kv.IntHandle(iv)
			return nil
		}
		for i, param := range x.IndexConstants {
			if param != nil {
				dVal, err := convertConstant2Datum(sctx, param, x.ColsFieldType[i])
				if err != nil {
					return err
				}
				x.IndexValues[i] = *dVal
			}
		}
		return nil
	case *BatchPointGetPlan:
		if x.TblInfo.GetPartitionInfo() != nil && fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix33031, false) {
			return errors.NewNoStackError("Fix33031 fix-control set and partitioned table in cached Batch Point Get plan")
		}
		// if access condition is not nil, which means it's a point get generated by cbo.
		if x.AccessConditions != nil {
			if x.IndexInfo != nil {
				ranges, err := ranger.DetachCondAndBuildRangeForIndex(x.ctx.GetRangerCtx(), x.AccessConditions, x.IdxCols, x.IdxColLens, 0)
				if err != nil {
					return err
				}
				if len(ranges.Ranges) != len(x.IndexValues) || !isSafeRange(x.AccessConditions, ranges, false, nil) {
					return errors.New("rebuild to get an unsafe range")
				}
				for i := range ranges.Ranges {
					copy(x.IndexValues[i], ranges.Ranges[i].LowVal)
				}
			} else {
				var pkCol *expression.Column
				var unsignedIntHandle bool
				if x.TblInfo.PKIsHandle {
					if pkColInfo := x.TblInfo.GetPkColInfo(); pkColInfo != nil {
						pkCol = expression.ColInfo2Col(x.schema.Columns, pkColInfo)
					}
					if !x.TblInfo.IsCommonHandle {
						unsignedIntHandle = true
					}
				}
				if pkCol != nil {
					ranges, accessConds, remainingConds, err := ranger.BuildTableRange(x.AccessConditions, x.ctx.GetRangerCtx(), pkCol.RetType, 0)
					if err != nil {
						return err
					}
					if len(ranges) != len(x.Handles) || !isSafeRange(x.AccessConditions, &ranger.DetachRangeResult{
						Ranges:        ranges,
						AccessConds:   accessConds,
						RemainedConds: remainingConds,
					}, unsignedIntHandle, nil) {
						return errors.New("rebuild to get an unsafe range")
					}
					for i := range ranges {
						x.Handles[i] = kv.IntHandle(ranges[i].LowVal[0].GetInt64())
					}
				}
			}
		}
		if len(x.HandleParams) > 0 {
			if len(x.HandleParams) != len(x.Handles) {
				return errors.New("rebuild to get an unsafe range, Handles length diff")
			}
			for i, param := range x.HandleParams {
				if param != nil {
					dVal, err := convertConstant2Datum(sctx, param, x.HandleType)
					if err != nil {
						return err
					}
					iv, err := dVal.ToInt64(sc.TypeCtx())
					if err != nil {
						return err
					}
					x.Handles[i] = kv.IntHandle(iv)
				}
			}
		}
		if len(x.IndexValueParams) > 0 {
			if len(x.IndexValueParams) != len(x.IndexValues) {
				return errors.New("rebuild to get an unsafe range, IndexValue length diff")
			}
			for i, params := range x.IndexValueParams {
				if len(params) < 1 {
					continue
				}
				for j, param := range params {
					if param != nil {
						dVal, err := convertConstant2Datum(sctx, param, x.IndexColTypes[j])
						if err != nil {
							return err
						}
						x.IndexValues[i][j] = *dVal
					}
				}
			}
		}
	case *PhysicalIndexMergeReader:
		indexMerge := p.(*PhysicalIndexMergeReader)
		for _, partialPlans := range indexMerge.PartialPlans {
			err = rebuildRange(partialPlans[0])
			if err != nil {
				return err
			}
		}
		// We don't need to handle the indexMerge.TablePlans, because the tablePlans
		// only can be (Selection) + TableRowIDScan. There have no range need to rebuild.
	case base.PhysicalPlan:
		for _, child := range x.Children() {
			err = rebuildRange(child)
			if err != nil {
				return err
			}
		}
	case *Insert:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	case *Update:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	case *Delete:
		if x.SelectPlan != nil {
			return rebuildRange(x.SelectPlan)
		}
	}
	return nil
}

func convertConstant2Datum(ctx base.PlanContext, con *expression.Constant, target *types.FieldType) (*types.Datum, error) {
	val, err := con.Eval(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	tc := ctx.GetSessionVars().StmtCtx.TypeCtx()
	dVal, err := val.ConvertTo(tc, target)
	if err != nil {
		return nil, err
	}
	// The converted result must be same as original datum.
	cmp, err := dVal.Compare(tc, &val, collate.GetCollator(target.GetCollate()))
	if err != nil || cmp != 0 {
		return nil, errors.New("Convert constant to datum is failed, because the constant has changed after the covert")
	}
	return &dVal, nil
}

func buildRangeForTableScan(sctx base.PlanContext, ts *PhysicalTableScan) (err error) {
	if ts.Table.IsCommonHandle {
		pk := tables.FindPrimaryIndex(ts.Table)
		pkCols := make([]*expression.Column, 0, len(pk.Columns))
		pkColsLen := make([]int, 0, len(pk.Columns))
		for _, colInfo := range pk.Columns {
			if pkCol := expression.ColInfo2Col(ts.schema.Columns, ts.Table.Columns[colInfo.Offset]); pkCol != nil {
				pkCols = append(pkCols, pkCol)
				// We need to consider the prefix index.
				// For example: when we have 'a varchar(50), index idx(a(10))'
				// So we will get 'colInfo.Length = 50' and 'pkCol.RetType.flen = 10'.
				// In 'hasPrefix' function from 'util/ranger/ranger.go' file,
				// we use 'columnLength == types.UnspecifiedLength' to check whether we have prefix index.
				if colInfo.Length != types.UnspecifiedLength && colInfo.Length == pkCol.RetType.GetFlen() {
					pkColsLen = append(pkColsLen, types.UnspecifiedLength)
				} else {
					pkColsLen = append(pkColsLen, colInfo.Length)
				}
			}
		}
		if len(pkCols) > 0 {
			res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), ts.AccessCondition, pkCols, pkColsLen, 0)
			if err != nil {
				return err
			}
			if !isSafeRange(ts.AccessCondition, res, false, ts.Ranges) {
				return errors.New("rebuild to get an unsafe range")
			}
			ts.Ranges = res.Ranges
		} else {
			if len(ts.AccessCondition) > 0 {
				return errors.New("fail to build ranges, cannot get the primary key column")
			}
			ts.Ranges = ranger.FullRange()
		}
	} else {
		var pkCol *expression.Column
		if ts.Table.PKIsHandle {
			if pkColInfo := ts.Table.GetPkColInfo(); pkColInfo != nil {
				pkCol = expression.ColInfo2Col(ts.schema.Columns, pkColInfo)
			}
		}
		if pkCol != nil {
			ranges, accessConds, remainingConds, err := ranger.BuildTableRange(ts.AccessCondition, sctx.GetRangerCtx(), pkCol.RetType, 0)
			if err != nil {
				return err
			}
			if !isSafeRange(ts.AccessCondition, &ranger.DetachRangeResult{
				Ranges:        ts.Ranges,
				AccessConds:   accessConds,
				RemainedConds: remainingConds,
			}, true, ts.Ranges) {
				return errors.New("rebuild to get an unsafe range")
			}
			ts.Ranges = ranges
		} else {
			if len(ts.AccessCondition) > 0 {
				return errors.New("fail to build ranges, cannot get the primary key column")
			}
			ts.Ranges = ranger.FullIntRange(false)
		}
	}
	return
}

func buildRangeForIndexScan(sctx base.PlanContext, is *PhysicalIndexScan) (err error) {
	if len(is.IdxCols) == 0 {
		if ranger.HasFullRange(is.Ranges, false) { // the original range is already a full-range.
			is.Ranges = ranger.FullRange()
			return
		}
		return errors.New("unexpected range for PhysicalIndexScan")
	}

	res, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), is.AccessCondition, is.IdxCols, is.IdxColLens, 0)
	if err != nil {
		return err
	}
	if !isSafeRange(is.AccessCondition, res, false, is.Ranges) {
		return errors.New("rebuild to get an unsafe range")
	}
	is.Ranges = res.Ranges
	return
}

// checkRebuiltRange checks whether the re-built range is safe.
// To re-use a cached plan, the planner needs to rebuild the access range, but as
// parameters change, some unsafe ranges may occur.
// For example, the first time the planner can build a range `(2, 5)` from `a>2 and a<(?)5`, but if the
// parameter changes to `(?)1`, then it'll get an unsafe range `(empty)`.
// To make plan-cache safer, let the planner abandon the cached plan if it gets an unsafe range here.
func isSafeRange(accessConds []expression.Expression, rebuiltResult *ranger.DetachRangeResult,
	unsignedIntHandle bool, originalRange ranger.Ranges) (safe bool) {
	if len(rebuiltResult.RemainedConds) > 0 || // the ranger generates some other extra conditions
		len(rebuiltResult.AccessConds) != len(accessConds) || // not all access conditions are used
		len(rebuiltResult.Ranges) == 0 { // get an empty range
		return false
	}

	if len(accessConds) > 0 && // if have accessConds, and
		ranger.HasFullRange(rebuiltResult.Ranges, unsignedIntHandle) && // get an full range, and
		originalRange != nil && !ranger.HasFullRange(originalRange, unsignedIntHandle) { // the original range is not a full range
		return false
	}

	return true
}
