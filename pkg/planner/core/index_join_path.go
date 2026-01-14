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
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
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
	eqUsedColsNDV  float64                 // the estimated NDV of the EQ used columns on this index, the NDV of `t1(a, b)`, a,b are in EQ constraint.
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
		ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, false, lastColPos)
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
		lastColIsRange := false
		if tempRangeRes.nextColInRange {
			if path.IdxColLens[lastColPos] != types.UnspecifiedLength {
				remained = append(remained, colAccesses...)
			}
			accesses = append(accesses, colAccesses...)
			lastColPos = lastColPos + 1
			lastColIsRange = true
		} else {
			remained = append(remained, colAccesses...)
		}
		mutableRange := indexJoinPathNewMutableRange(sctx, indexJoinInfo, accesses, tempRangeRes.ranges, path)
		ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, lastColIsRange, lastColPos)
		return ret, false, nil
	}
	tempRangeRes := indexJoinPathBuildTmpRange(sctx, buildTmp, matchedKeyCnt, notKeyEqAndIn, nil, true, rangeMaxSize)
	if tempRangeRes.err != nil || tempRangeRes.emptyRange {
		return nil, tempRangeRes.emptyRange, tempRangeRes.err
	}
	lastColPos, accesses, remained = indexJoinPathUpdateTmpRange(sctx, buildTmp, tempRangeRes, accesses, remained)

	remained = append(remained, rangeFilterCandidates...)
	lastColIsRange := false
	if tempRangeRes.extraColInRange {
		accesses = append(accesses, lastColAccess...)
		lastColPos = lastColPos + 1
		lastColIsRange = true
	} else {
		if tempRangeRes.keyCntInRange <= 0 {
			return nil, false, nil
		}
		lastColManager = nil
	}
	mutableRange := indexJoinPathNewMutableRange(sctx, indexJoinInfo, accesses, tempRangeRes.ranges, path)
	ret := indexJoinPathConstructResult(sctx, indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, lastColManager, lastColIsRange, lastColPos)
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
	// indexJoinCoverNumResult: in the index join case, if you can tell from index A is better than B by normal rules
	// like accessResult, eqOrInResult, then do it. But if two indexes is not comparable, we can take one more condition
	// into consideration here: if A's access col cover more index join keys, it has high potential to be better.
	//     for example: index(id, mem, scode, ctime) vs index(uname, scode, ntime)
	//                         ^   ^     ^                       ^     ^
	// static EQ predicates <--+---+     +-----------------------+-----+-----> dynamic join join EQ predicates.
	// since they two covers different index columns group, it's un-comparable and hard to see which one is better. Instead
	// of returning "no winner" and return cmpResult == 0 and letting ndv division outside to make the decision, we could
	// be a little more bias to index join keys here. (heuristic)
	indexJoinKeyCoverDiff := 0
	if current.idxOff2KeyOff != nil && best.idxOff2KeyOff != nil {
		curCover, bestCover := 0, 0
		for _, off := range current.idxOff2KeyOff {
			if off != -1 {
				curCover++
			}
		}
		for _, off := range best.idxOff2KeyOff {
			if off != -1 {
				bestCover++
			}
		}
		indexJoinKeyCoverDiff = curCover - bestCover
	}
	// 1: only care for the eq condition related group ndv, it's more accurate than range condition appended.
	// 2: when eqUsedColsNDV is in the same level, consider more about usedColsLen and indexJoinKeyCoverDiff.
	if !isNDVClose(current.eqUsedColsNDV, best.eqUsedColsNDV) {
		return current.eqUsedColsNDV > best.eqUsedColsNDV // if two NDV are NOT close, return the bigger, meaning the smaller of EQ rows. (the better)
	}
	if current.usedColsLen != best.usedColsLen { // if two NDV are quite close, and usedColsLen are different, return the longer one.
		return current.usedColsLen > best.usedColsLen
	}
	if indexJoinKeyCoverDiff != 0 { // if two NDV are quite close, and indexJoinKeyCoverDiff do exist. return the one covers more.
		return indexJoinKeyCoverDiff > 0
	}
	return current.eqUsedColsNDV > best.eqUsedColsNDV // if two NDV are quite close, no other things need to care about, return bigger NDV one.
}

/*
Simple NDV closeness rule:

	close if |a-b| < 200 OR |a-b|/max(a,b) < 0.5

Examples:
  - (1, 2)       → close
  - (100, 200)   → close
  - (100001, 100020) → close
  - (1000, 2000) → not close
  - (1e6, 2e6)   → not close
  - (100, 500)   → not close
  - (1000, 5000) → not close
*/
func isNDVClose(lhs, rhs float64) bool {
	if lhs == 0 || rhs == 0 {
		return lhs == rhs
	}
	diff := math.Abs(lhs - rhs)
	maxVal := math.Max(lhs, rhs)
	if diff < 200 {
		return true
	}
	return diff/maxVal < 0.5
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
	lastColIsRange bool,
	usedColsLen int) *indexJoinPathResult {
	var innerNDV float64
	if stats := indexJoinInfo.innerTableStats; stats != nil && stats.StatsVersion != statistics.PseudoVersion {
		// NOTE: use the original table's stats (DataSource.TableStats) to estimate NDV instead of stats
		// AFTER APPLYING ALL FILTERS (DataSource.stats). Because here we'll use the NDV to measure how many
		// rows we need to scan in double-read, and these filters will be applied after the scanning, so we need to
		// ignore these filters to avoid underestimation. See #63869.
		eqUsedColsLen := usedColsLen
		if lastColIsRange {
			eqUsedColsLen--
		}
		innerNDV, _ = cardinality.EstimateColsNDVWithMatchedLen(
			sctx, path.IdxCols[:eqUsedColsLen], indexJoinInfo.innerSchema, stats)
	}
	idxOff2KeyOff := make([]int, len(buildTmp.curIdxOff2KeyOff))
	copy(idxOff2KeyOff, buildTmp.curIdxOff2KeyOff)
	return &indexJoinPathResult{
		chosenPath:     path,
		candidate:      getIndexCandidateForIndexJoin(sctx, path, usedColsLen),
		usedColsLen:    len(ranges.Range()[0].LowVal),
		eqUsedColsNDV:  innerNDV,
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

func indexJoinIntPKRangeInfo(ectx expression.EvalContext, outerJoinKeys []*expression.Column) string {
	var buffer strings.Builder
	buffer.WriteString("[")
	for i, key := range outerJoinKeys {
		if i != 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(key.StringWithCtx(ectx, errors.RedactLogDisable))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// indexJoinPathRangeInfo generates the range information for the index join path.
func indexJoinPathRangeInfo(sctx planctx.PlanContext,
	outerJoinKeys []*expression.Column,
	indexJoinResult *indexJoinPathResult) string {
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range indexJoinResult.idxOff2KeyOff {
		if keyOff == -1 {
			continue
		}
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		fmt.Fprintf(buffer, "eq(%v, %v)", indexJoinResult.chosenPath.IdxCols[idxOff], outerJoinKeys[keyOff])
	}
	ectx := sctx.GetExprCtx().GetEvalCtx()
	// It is to build the range info which is used in explain. It is necessary to redact the range info.
	redact := ectx.GetTiDBRedactLog()
	for _, access := range indexJoinResult.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		fmt.Fprintf(buffer, "%v", access.StringWithCtx(ectx, redact))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// Reset the 'curIdxOff2KeyOff', 'curNotUsedIndexCols' and 'curNotUsedColLens' by innerKeys and idxCols
/*
For each idxCols,
  If column can be found in innerKeys
	save offset of innerKeys in 'curIdxOff2KeyOff'
  Else,
	save -1 in 'curIdxOff2KeyOff'
*/
// For example, innerKeys[t1.a, t1.sum_b, t1.c], idxCols [a, b, c]
// 'curIdxOff2KeyOff' = [0, -1, 2]
func indexJoinPathTmpInit(
	sctx planctx.PlanContext,
	indexJoinInfo *indexJoinPathInfo,
	idxCols []*expression.Column,
	colLens []int) *indexJoinPathTmp {
	tmpSchema := expression.NewSchema(indexJoinInfo.innerJoinKeys...)
	buildTmp := new(indexJoinPathTmp)
	buildTmp.curIdxOff2KeyOff = make([]int, len(idxCols))
	buildTmp.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	buildTmp.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		buildTmp.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if buildTmp.curIdxOff2KeyOff[i] >= 0 {
			// Don't use the join columns if their collations are unmatched and the new collation is enabled.
			if collate.NewCollationEnabled() && types.IsString(idxCol.RetType.GetType()) && types.IsString(indexJoinInfo.outerJoinKeys[buildTmp.curIdxOff2KeyOff[i]].RetType.GetType()) {
				et, err := expression.CheckAndDeriveCollationFromExprs(sctx.GetExprCtx(), "equal", types.ETInt, idxCol, indexJoinInfo.outerJoinKeys[buildTmp.curIdxOff2KeyOff[i]])
				if err != nil {
					logutil.BgLogger().Error("Unexpected error happened during constructing index join", zap.Stack("stack"))
				}
				if !collate.CompatibleCollate(idxCol.GetStaticType().GetCollate(), et.Collation) {
					buildTmp.curIdxOff2KeyOff[i] = -1
				}
			}
			continue
		}
		buildTmp.curNotUsedIndexCols = append(buildTmp.curNotUsedIndexCols, idxCol)
		buildTmp.curNotUsedColLens = append(buildTmp.curNotUsedColLens, colLens[i])
	}
	return buildTmp
}

// indexJoinPathFindUsefulEQIn analyzes the PushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// remainedEqOrIn is part of usefulEqOrInFilters, which needs to be evaluated again in selection.
// remainingRangeCandidates is the other conditions for future use.
func indexJoinPathFindUsefulEQIn(sctx planctx.PlanContext, indexJoinInfo *indexJoinPathInfo,
	buildTmp *indexJoinPathTmp) (usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates []expression.Expression, emptyRange bool) {
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _, emptyRange = ranger.ExtractEqAndInCondition(
		sctx.GetRangerCtx(),
		indexJoinInfo.innerPushedConditions,
		buildTmp.curNotUsedIndexCols,
		buildTmp.curNotUsedColLens,
	)
	return usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, emptyRange
}

// indexJoinPathBuildColManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func indexJoinPathBuildColManager(indexJoinInfo *indexJoinPathInfo,
	nextCol *expression.Column, cwc *physicalop.ColWithCmpFuncManager) []expression.Expression {
	var lastColAccesses []expression.Expression
loopOtherConds:
	for _, filter := range indexJoinInfo.joinOtherConditions {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			continue
		}
		var funcName string
		var anotherArg expression.Expression
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.EqualColumn(nextCol) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.EqualColumn(nextCol) {
			anotherArg = sf.GetArgs()[0]
			// The column manager always build expression in the form of col op arg1.
			// So we need use the symmetric one of the current function.
			funcName = symmetricOp[sf.FuncName.L]
		} else {
			continue
		}
		affectedCols := expression.ExtractColumns(anotherArg)
		if len(affectedCols) == 0 {
			continue
		}
		for _, col := range affectedCols {
			if indexJoinInfo.innerSchema.Contains(col) {
				continue loopOtherConds
			}
		}
		lastColAccesses = append(lastColAccesses, sf)
		cwc.AppendNewExpr(funcName, anotherArg, affectedCols)
	}
	return lastColAccesses
}

// indexJoinPathRemoveUselessEQIn removes the useless eq/in conditions. It's designed for the following case:
//
//	t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//	In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//	It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//	[0 -1 -1].
//	So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func indexJoinPathRemoveUselessEQIn(buildTmp *indexJoinPathTmp, idxCols []*expression.Column,
	notKeyEqAndIn []expression.Expression) (usefulEqAndIn, uselessOnes []expression.Expression) {
	buildTmp.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		if buildTmp.curIdxOff2KeyOff[idxColPos] != -1 {
			buildTmp.curPossibleUsedKeys = append(buildTmp.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		if notKeyColPos < len(notKeyEqAndIn) && buildTmp.curNotUsedIndexCols[notKeyColPos].EqualColumn(idxCols[idxColPos]) {
			notKeyColPos++
			continue
		}
		for i := idxColPos + 1; i < len(idxCols); i++ {
			buildTmp.curIdxOff2KeyOff[i] = -1
		}
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyColPos)
		remained = append(remained, notKeyEqAndIn[notKeyColPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyColPos]
		return notKeyEqAndIn, remained
	}
	return notKeyEqAndIn, nil
}

func getIndexJoinIntPKPathInfo(ds *logicalop.DataSource, innerJoinKeys, outerJoinKeys []*expression.Column,
	checkPathValid func(path *util.AccessPath) bool) (
	keyOff2IdxOff []int, newOuterJoinKeys []*expression.Column, ranges ranger.Ranges, chosenPath *util.AccessPath, ok bool) {
	pkMatched := false
	pkCol := ds.GetPKIsHandleCol()
	if pkCol == nil {
		return nil, nil, nil, nil, false
	}
	keyOff2IdxOff = make([]int, len(innerJoinKeys))
	newOuterJoinKeys = make([]*expression.Column, 0)
	for i, key := range innerJoinKeys {
		if !key.EqualColumn(pkCol) {
			keyOff2IdxOff[i] = -1
			continue
		}
		pkMatched = true
		keyOff2IdxOff[i] = 0
		// Add to newOuterJoinKeys only if conditions contain inner primary key. For issue #14822.
		newOuterJoinKeys = append(newOuterJoinKeys, outerJoinKeys[i])
	}
	if !pkMatched {
		return nil, nil, nil, nil, false
	}
	ranges = ranger.FullIntRange(mysql.HasUnsignedFlag(pkCol.RetType.GetFlag()))
	// compute the matchProp which require the int table path first.
	for _, path := range ds.PossibleAccessPaths {
		if checkPathValid(path) {
			chosenPath = path
			break
		}
	}
	return keyOff2IdxOff, newOuterJoinKeys, ranges, chosenPath, true
}

// getBestIndexJoinInnerTaskByProp tries to build the best inner child task from ds for index join by the given property.
func getBestIndexJoinInnerTaskByProp(ds *logicalop.DataSource, prop *property.PhysicalProperty) (base.Task, error) {
	// the below code is quite similar from the original logic
	// reason1: we need to leverage original indexPathInfo down related logic to build constant range for index plan.
	// reason2: the ranges from TS and IS couldn't be directly used to derive the stats' estimation, it's not real.
	// reason3: skyline pruning should not prune the possible index path which could feel the runtime EQ access conditions.
	//
	// here we build TableScan(TS) and IndexScan(IS) separately according to different index join prop is for we couldn't decide
	// which one as the copTask here is better, some more possible upper attached operator cost should be
	// considered, besides the row count, double reader cost for index lookup should also be considered as
	// a whole, so we leave the cost compare for index join itself just like what it was before.
	var innerCopTask base.Task
	if prop.IndexJoinProp.TableRangeScan {
		innerCopTask = buildDataSource2TableScanByIndexJoinProp(ds, prop)
	} else {
		innerCopTask = buildDataSource2IndexScanByIndexJoinProp(ds, prop)
	}
	if innerCopTask.Invalid() {
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		return innerCopTask.ConvertToRootTask(ds.SCtx()), nil
	}
	return innerCopTask, nil
}

// getBestIndexJoinPathResultByProp tries to iterate all possible access paths of the inner child and builds
// index join path for each access path based on push-down indexIndexProp. It returns the best index join path result and the mapping.
func getBestIndexJoinPathResultByProp(
	innerDS *logicalop.DataSource,
	indexJoinProp *property.IndexJoinRuntimeProp,
	checkPathValid func(path *util.AccessPath) bool) (*indexJoinPathResult, []int) {
	indexJoinInfo := &indexJoinPathInfo{
		joinOtherConditions:   indexJoinProp.OtherConditions, // other conditions is for complete last col non-eq range
		outerJoinKeys:         indexJoinProp.OuterJoinKeys,
		innerJoinKeys:         indexJoinProp.InnerJoinKeys,
		innerPushedConditions: innerDS.PushedDownConds,
		innerSchema:           innerDS.Schema(),
		innerTableStats:       innerDS.TableStats,
	}
	var bestResult *indexJoinPathResult
	for _, path := range innerDS.PossibleAccessPaths {
		if checkPathValid(path) {
			// here we still wrap indexJoinPathInfo to call index indexJoinPathBuild to get the chosen path result.
			result, emptyRange, err := indexJoinPathBuild(innerDS.SCtx(), path, indexJoinInfo, false)
			if emptyRange {
				return nil, nil
			}
			if err != nil {
				logutil.BgLogger().Warn("build index join failed", zap.Error(err))
				continue
			}
			if indexJoinPathCompare(innerDS, bestResult, result) {
				bestResult = result
			}
		}
	}
	if bestResult == nil || bestResult.chosenPath == nil {
		return nil, nil
	}
	keyOff2IdxOff := make([]int, len(indexJoinProp.InnerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	// reverse idxOff2KeyOff as keyOff2IdxOff, from the perspective of inner join key, we could easily get the offset of index col.
	for idxOff, keyOff := range bestResult.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	return bestResult, keyOff2IdxOff
}

// getBestIndexJoinPathResult tries to iterate all possible access paths of the inner child and builds
// index join path for each access path. It returns the best index join path result and the mapping.
func getBestIndexJoinPathResult(
	join *logicalop.LogicalJoin,
	innerChild *logicalop.DataSource,
	innerJoinKeys, outerJoinKeys []*expression.Column,
	checkPathValid func(path *util.AccessPath) bool) (*indexJoinPathResult, []int) {
	indexJoinInfo := &indexJoinPathInfo{
		joinOtherConditions:   join.OtherConditions,
		outerJoinKeys:         outerJoinKeys,
		innerJoinKeys:         innerJoinKeys,
		innerPushedConditions: innerChild.PushedDownConds,
		innerSchema:           innerChild.Schema(),
		innerTableStats:       innerChild.TableStats,
	}
	var bestResult *indexJoinPathResult
	for _, path := range innerChild.PossibleAccessPaths {
		if checkPathValid(path) {
			result, emptyRange, err := indexJoinPathBuild(join.SCtx(), path, indexJoinInfo, false)
			if emptyRange {
				return nil, nil
			}
			if err != nil {
				logutil.BgLogger().Warn("build index join failed", zap.Error(err))
				continue
			}
			if indexJoinPathCompare(innerChild, bestResult, result) {
				bestResult = result
			}
		}
	}
	if bestResult == nil || bestResult.chosenPath == nil {
		return nil, nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	// reverse idxOff2KeyOff as keyOff2IdxOff, from the perspective of inner join key, we could easily get the offset of index col.
	for idxOff, keyOff := range bestResult.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	return bestResult, keyOff2IdxOff
}

// appendTailTemplateRange appends empty datum for each range in originRanges.
// rangeMaxSize is the max memory limit for ranges. O indicates no memory limit.
// If the second return value is true, it means that the estimated memory after appending datums to originRanges exceeds
// rangeMaxSize and the function rejects appending datums to originRanges.
func appendTailTemplateRange(originRanges ranger.Ranges, rangeMaxSize int64) (ranger.Ranges, bool) {
	if rangeMaxSize > 0 && originRanges.MemUsage()+(types.EmptyDatumSize*2+16)*int64(len(originRanges)) > rangeMaxSize {
		return originRanges, true
	}
	for _, ran := range originRanges {
		ran.LowVal = append(ran.LowVal, types.Datum{})
		ran.HighVal = append(ran.HighVal, types.Datum{})
		ran.Collators = append(ran.Collators, nil)
	}
	return originRanges, false
}

var symmetricOp = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}
