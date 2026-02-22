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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// mutableIndexJoinRange is designed for IndexJoin plan in the Plan Cache.
// Before reusing a cached IndexJoin plan from the Plan Cache, we have to
// first update the ranges of the IndexJoin plan according to the current parameters.
// mutableIndexJoinRanges stores all necessary information to rebuild the ranges of an IndexJoin plan.
type mutableIndexJoinRange struct {
	ranges    ranger.Ranges
	rangeInfo string

	indexJoinInfo *indexJoinPathInfo // read-only
	path          *util.AccessPath   // read-only
}

func (mr *mutableIndexJoinRange) CloneForPlanCache() ranger.MutableRanges {
	cloned := new(mutableIndexJoinRange)
	if mr.ranges != nil {
		cloned.ranges = mr.ranges.CloneForPlanCache().(ranger.Ranges)
	}
	cloned.rangeInfo = mr.rangeInfo
	cloned.indexJoinInfo = mr.indexJoinInfo
	cloned.path = mr.path
	return cloned
}

func (mr *mutableIndexJoinRange) Range() ranger.Ranges {
	return mr.ranges
}

func (mr *mutableIndexJoinRange) Rebuild(sctx planctx.PlanContext) error {
	result, empty, err := indexJoinPathBuild(sctx, mr.path, mr.indexJoinInfo, true)
	if err != nil {
		return err
	}
	if empty { // empty ranges are dangerous for plan-cache, it's better to optimize the whole plan again in this case
		return errors.New("failed to rebuild range: empty range")
	}
	newRanges := result.chosenRanges.Range()
	if len(mr.ranges) != len(newRanges) || (len(mr.ranges) > 0 && mr.ranges[0].Width() != newRanges[0].Width()) {
		// some access conditions cannot be used to calculate the range after parameters change, return an error in this case for safety.
		return errors.New("failed to rebuild range: range width changed")
	}
	mr.rangeInfo = indexJoinPathRangeInfo(sctx, mr.indexJoinInfo.outerJoinKeys, result)
	mr.ranges = result.chosenRanges.Range()
	return nil
}

// indexJoinPathResult records necessary information if we build IndexJoin on this chosen access path (or index).
type indexJoinPathResult struct {
	chosenPath     *util.AccessPath        // the chosen access path (or index)
	candidate      *candidatePath          // the candidate representation of the chosen path, used for SkylinePruning
	chosenAccess   []expression.Expression // expressions used to access this index
	chosenRemained []expression.Expression // remaining expressions after accessing this index
	chosenRanges   ranger.MutableRanges    // the ranges used to access this index
	usedColsLen    int                     // the number of columns used on this index, `t1.a=t2.a and t1.b=t2.b` can use 2 columns of index t1(a, b, c)
	usedColsNDV    float64                 // the estimated NDV of the used columns on this index, the NDV of `t1(a, b)`
	idxOff2KeyOff  []int
	lastColManager *physicalop.ColWithCmpFuncManager
}

// indexJoinPathInfo records necessary information to build IndexJoin.
type indexJoinPathInfo struct {
	joinOtherConditions   []expression.Expression
	outerJoinKeys         []*expression.Column
	innerJoinKeys         []*expression.Column
	innerSchema           *expression.Schema
	innerPushedConditions []expression.Expression
	innerTableStats       *property.StatsInfo // the original table's stats
}

// indexJoinPathTmp records temporary information when building IndexJoin.
type indexJoinPathTmp struct {
	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	// Store the corresponding innerKeys offset of each column in the current path, reset by "resetContextForIndex()"
	curIdxOff2KeyOff []int
}

type indexJoinTmpRange struct {
	ranges            ranger.Ranges
	emptyRange        bool
	keyCntInRange     int
	eqAndInCntInRange int
	nextColInRange    bool
	extraColInRange   bool
	err               error
}

// indexJoinPathNewMutableRange creates a mutableIndexJoinRange.
// See more info in the comments of mutableIndexJoinRange.
func indexJoinPathNewMutableRange(
	sctx planctx.PlanContext,
	indexJoinInfo *indexJoinPathInfo,
	relatedExprs []expression.Expression,
	ranges []*ranger.Range,
	path *util.AccessPath) ranger.MutableRanges {
	// if the plan-cache is enabled and these ranges depend on some parameters, we have to rebuild these ranges after changing parameters
	if expression.MaybeOverOptimized4PlanCache(sctx.GetExprCtx(), relatedExprs...) {
		// assume that path, innerKeys and outerKeys will not be modified in the follow-up process
		return &mutableIndexJoinRange{
			ranges:        ranges,
			indexJoinInfo: indexJoinInfo,
			path:          path,
		}
	}
	return ranger.Ranges(ranges)
}

func indexJoinPathUpdateTmpRange(
	sctx planctx.PlanContext,
	buildTmp *indexJoinPathTmp,
	tempRangeRes *indexJoinTmpRange,
	accesses, remained []expression.Expression) (lastColPos int, newAccesses, newRemained []expression.Expression) {
	lastColPos = tempRangeRes.keyCntInRange + tempRangeRes.eqAndInCntInRange
	buildTmp.curPossibleUsedKeys = buildTmp.curPossibleUsedKeys[:tempRangeRes.keyCntInRange]
	for i := lastColPos; i < len(buildTmp.curIdxOff2KeyOff); i++ {
		buildTmp.curIdxOff2KeyOff[i] = -1
	}
	newAccesses = accesses[:tempRangeRes.eqAndInCntInRange]
	newRemained = ranger.AppendConditionsIfNotExist(sctx.GetExprCtx().GetEvalCtx(),
		remained, accesses[tempRangeRes.eqAndInCntInRange:])
	return
}

// indexJoinPathBuild tries to build an index join on this specified access path.
// The result is recorded in the *indexJoinPathResult, see more info in that structure.
func indexJoinPathBuild(sctx planctx.PlanContext,
	path *util.AccessPath,
	indexJoinInfo *indexJoinPathInfo,
	rebuildMode bool) (result *indexJoinPathResult, emptyRange bool, err error) {
	if len(path.IdxCols) == 0 {
		return nil, false, nil
	}
	accesses := make([]expression.Expression, 0, len(path.IdxCols))
	buildTmp := indexJoinPathTmpInit(sctx, indexJoinInfo, path.IdxCols, path.IdxColLens)
	notKeyEqAndIn, remained, rangeFilterCandidates, emptyRange := indexJoinPathFindUsefulEQIn(sctx, indexJoinInfo, buildTmp)
	if emptyRange {
		return nil, true, nil
	}
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = indexJoinPathRemoveUselessEQIn(buildTmp, path.IdxCols, notKeyEqAndIn)
	matchedKeyCnt := len(buildTmp.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(indexJoinInfo.innerJoinKeys) > 0 {
		return nil, false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = ranger.AppendConditionsIfNotExist(sctx.GetExprCtx().GetEvalCtx(), remained, remainedEqAndIn)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		return nil, false, nil
	}
	rangeMaxSize := sctx.GetSessionVars().RangeMaxSize
	if rebuildMode {
		// When rebuilding ranges for plan cache, we don't restrict range mem limit.
		rangeMaxSize = 0
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastColPos == len(path.IdxCols) {
		// If there's no join key matching index column, then choosing hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return nil, false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		tempRangeRes := indexJoinPathBuildTmpRange(sctx, buildTmp, matchedKeyCnt, notKeyEqAndIn, nil, false, rangeMaxSize)
		if tempRangeRes.err != nil || tempRangeRes.emptyRange || tempRangeRes.keyCntInRange <= 0 {
			return nil, tempRangeRes.emptyRange, tempRangeRes.err
		}
		lastColPos, accesses, remained = indexJoinPathUpdateTmpRange(sctx, buildTmp, tempRangeRes, accesses, remained)
		mutableRange := indexJoinPathNewMutableRange(sctx, indexJoinInfo, accesses, tempRangeRes.ranges, path)
		ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, lastColPos)
		return ret, false, nil
	}
	lastPossibleCol := path.IdxCols[lastColPos]
	lastColManager := &physicalop.ColWithCmpFuncManager{
		TargetCol:         lastPossibleCol,
		ColLength:         path.IdxColLens[lastColPos],
		AffectedColSchema: expression.NewSchema(),
	}
	lastColAccess := indexJoinPathBuildColManager(indexJoinInfo, lastPossibleCol, lastColManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastColAccess) == 0 {
		// If there's no join key matching index column, then choosing hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return nil, false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForColumn(sctx.GetRangerCtx(), rangeFilterCandidates, lastPossibleCol)
		var nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			var colRemained2 []expression.Expression
			nextColRange, colAccesses, colRemained2, err = ranger.BuildColumnRange(colAccesses, sctx.GetRangerCtx(), lastPossibleCol.RetType, path.IdxColLens[lastColPos], rangeMaxSize)
			if err != nil {
				return nil, false, err
			}
			if len(colRemained2) > 0 {
				colRemained = append(colRemained, colRemained2...)
				nextColRange = nil
			}
		}
		tempRangeRes := indexJoinPathBuildTmpRange(sctx, buildTmp, matchedKeyCnt, notKeyEqAndIn, nextColRange, false, rangeMaxSize)
		if tempRangeRes.err != nil || tempRangeRes.emptyRange || tempRangeRes.keyCntInRange <= 0 {
			return nil, tempRangeRes.emptyRange, tempRangeRes.err
		}
		lastColPos, accesses, remained = indexJoinPathUpdateTmpRange(sctx, buildTmp, tempRangeRes, accesses, remained)
		// update accesses and remained by colAccesses and colRemained.
		remained = append(remained, colRemained...)
		if tempRangeRes.nextColInRange {
			if path.IdxColLens[lastColPos] != types.UnspecifiedLength {
				remained = append(remained, colAccesses...)
			}
			accesses = append(accesses, colAccesses...)
			lastColPos = lastColPos + 1
		} else {
			remained = append(remained, colAccesses...)
		}
		mutableRange := indexJoinPathNewMutableRange(sctx, indexJoinInfo, accesses, tempRangeRes.ranges, path)
		ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, lastColPos)
		return ret, false, nil
	}
	tempRangeRes := indexJoinPathBuildTmpRange(sctx, buildTmp, matchedKeyCnt, notKeyEqAndIn, nil, true, rangeMaxSize)
	if tempRangeRes.err != nil || tempRangeRes.emptyRange {
		return nil, tempRangeRes.emptyRange, tempRangeRes.err
	}
	lastColPos, accesses, remained = indexJoinPathUpdateTmpRange(sctx, buildTmp, tempRangeRes, accesses, remained)

	remained = append(remained, rangeFilterCandidates...)
	if tempRangeRes.extraColInRange {
		accesses = append(accesses, lastColAccess...)
		lastColPos = lastColPos + 1
	} else {
		if tempRangeRes.keyCntInRange <= 0 {
			return nil, false, nil
		}
		lastColManager = nil
	}
	mutableRange := indexJoinPathNewMutableRange(sctx, indexJoinInfo, accesses, tempRangeRes.ranges, path)
	ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, lastColManager, lastColPos)
	return ret, false, nil
}

// indexJoinPathCompare compares these 2 index join paths and returns whether the current is better than the base.
func indexJoinPathCompare(ds *logicalop.DataSource, best, current *indexJoinPathResult) (curIsBetter bool) {
	// Notice that there may be the cases like `t1.a = t2.a and b > 2 and b < 1`, so ranges can be nil though the conditions are valid.
	// Obviously when the range is nil, we don't need index join.
	if current == nil || len(current.chosenRanges.Range()) == 0 {
		return false
	}
	if best == nil {
		return true
	}

	// reuse Skyline pruning to compare the index join paths.
	prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64} // default property without any requirement
	preferRange := ds.SCtx().GetSessionVars().GetAllowPreferRangeScan()

	cmpResult, _ := compareCandidates(ds.SCtx(), ds.StatisticTable, prop, current.candidate, best.candidate, preferRange)
	if cmpResult == 1 {
		return true
	} else if cmpResult == -1 {
		return false
	}
	// cmpResult == 0, go on to use NDV to compare.

	// We choose the index by the NDV of the used columns, the larger the better.
	// If NDVs are same, we choose index which uses more columns.
	// Note that these 2 heuristic rules are too simple to cover all cases,
	// since the NDV of outer join keys are not considered, and the detached access conditions
	// may contain expressions like `t1.a > t2.a`. It's pretty hard to evaluate the join selectivity
	// of these non-column-equal conditions, so I prefer to keep these heuristic rules simple at least for now.
	if current.usedColsNDV < best.usedColsNDV || (current.usedColsNDV == best.usedColsNDV && current.usedColsLen <= best.usedColsLen) {
		return false
	}
	return true
}

// indexJoinPathConstructResult constructs the index join path result.
func indexJoinPathConstructResult(
	sctx planctx.PlanContext,
	indexJoinInfo *indexJoinPathInfo,
	buildTmp *indexJoinPathTmp,
	ranges ranger.MutableRanges,
	path *util.AccessPath, accesses,
	remained []expression.Expression,
	lastColManager *physicalop.ColWithCmpFuncManager,
	usedColsLen int) *indexJoinPathResult {
	var innerNDV float64
	if stats := indexJoinInfo.innerTableStats; stats != nil && stats.StatsVersion != statistics.PseudoVersion {
		// NOTE: use the original table's stats (DataSource.TableStats) to estimate NDV instead of stats
		// AFTER APPLYING ALL FILTERS (DataSource.stats). Because here we'll use the NDV to measure how many
		// rows we need to scan in double-read, and these filters will be applied after the scanning, so we need to
		// ignore these filters to avoid underestimation. See #63869.
		innerNDV, _ = cardinality.EstimateColsNDVWithMatchedLen(
			sctx, path.IdxCols[:usedColsLen], indexJoinInfo.innerSchema, stats)
	}
	idxOff2KeyOff := make([]int, len(buildTmp.curIdxOff2KeyOff))
	copy(idxOff2KeyOff, buildTmp.curIdxOff2KeyOff)
	return &indexJoinPathResult{
		chosenPath:     path,
		candidate:      getIndexCandidateForIndexJoin(sctx, path, usedColsLen),
		usedColsLen:    len(ranges.Range()[0].LowVal),
		usedColsNDV:    innerNDV,
		chosenRanges:   ranges,
		chosenAccess:   accesses,
		chosenRemained: remained,
		idxOff2KeyOff:  idxOff2KeyOff,
		lastColManager: lastColManager,
	}
}

func indexJoinPathBuildTmpRange(
	sctx planctx.PlanContext,
	buildTmp *indexJoinPathTmp,
	matchedKeyCnt int,
	eqAndInFuncs []expression.Expression,
	nextColRange []*ranger.Range,
	haveExtraCol bool,
	rangeMaxSize int64) (res *indexJoinTmpRange) {
	res = &indexJoinTmpRange{}
	sc := sctx.GetSessionVars().StmtCtx
	defer func() {
		if sc.MemTracker != nil && res != nil && len(res.ranges) > 0 {
			sc.MemTracker.Consume(2 * types.EstimatedMemUsage(res.ranges[0].LowVal, len(res.ranges)))
		}
	}()
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	ranges := ranger.Ranges{&ranger.Range{}}
	for i, j := 0, 0; i+j < pointLength; {
		if buildTmp.curIdxOff2KeyOff[i+j] != -1 {
			// This position is occupied by join key.
			var fallback bool
			ranges, fallback = appendTailTemplateRange(ranges, rangeMaxSize)
			if fallback {
				sctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			i++
		} else {
			exprs := []expression.Expression{eqAndInFuncs[j]}
			oneColumnRan, _, remained, err := ranger.BuildColumnRange(exprs, sctx.GetRangerCtx(), buildTmp.curNotUsedIndexCols[j].RetType, buildTmp.curNotUsedColLens[j], rangeMaxSize)
			if err != nil {
				return &indexJoinTmpRange{err: err}
			}
			if len(oneColumnRan) == 0 {
				return &indexJoinTmpRange{emptyRange: true}
			}
			if sc.MemTracker != nil {
				sc.MemTracker.Consume(2 * types.EstimatedMemUsage(oneColumnRan[0].LowVal, len(oneColumnRan)))
			}
			if len(remained) > 0 {
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			var fallback bool
			ranges, fallback = ranger.AppendRanges2PointRanges(ranges, oneColumnRan, rangeMaxSize)
			if fallback {
				sctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
				res.ranges = ranges
				res.keyCntInRange = i
				res.eqAndInCntInRange = j
				return
			}
			j++
		}
	}
	if len(nextColRange) > 0 {
		var fallback bool
		ranges, fallback = ranger.AppendRanges2PointRanges(ranges, nextColRange, rangeMaxSize)
		if fallback {
			sctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
		}
		res.ranges = ranges
		res.keyCntInRange = matchedKeyCnt
		res.eqAndInCntInRange = len(eqAndInFuncs)
		res.nextColInRange = !fallback
		return
	}
	if haveExtraCol {
		var fallback bool
		ranges, fallback = appendTailTemplateRange(ranges, rangeMaxSize)
		if fallback {
			sctx.GetSessionVars().StmtCtx.RecordRangeFallback(rangeMaxSize)
		}
		res.ranges = ranges
		res.keyCntInRange = matchedKeyCnt
		res.eqAndInCntInRange = len(eqAndInFuncs)
		res.extraColInRange = !fallback
		return
	}
	res.ranges = ranges
	res.keyCntInRange = matchedKeyCnt
	res.eqAndInCntInRange = len(eqAndInFuncs)
	return
}

