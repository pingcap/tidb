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
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/size"
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
	chosenAccess   []expression.Expression // expressions used to access this index
	chosenRemained []expression.Expression // remaining expressions after accessing this index
	chosenRanges   ranger.MutableRanges    // the ranges used to access this index
	usedColsLen    int                     // the number of columns used on this index, `t1.a=t2.a and t1.b=t2.b` can use 2 columns of index t1(a, b, c)
	usedColsNDV    float64                 // the estimated NDV of the used columns on this index, the NDV of `t1(a, b)`
	idxOff2KeyOff  []int
	lastColManager *ColWithCmpFuncManager
}

// indexJoinPathInfo records necessary information to build IndexJoin.
type indexJoinPathInfo struct {
	joinOtherConditions   []expression.Expression
	outerJoinKeys         []*expression.Column
	innerJoinKeys         []*expression.Column
	innerSchema           *expression.Schema
	innerPushedConditions []expression.Expression
	innerStats            *property.StatsInfo
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
	if expression.MaybeOverOptimized4PlanCache(sctx.GetExprCtx(), relatedExprs) {
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
		ret := indexJoinPathConstructResult(indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, lastColPos)
		return ret, false, nil
	}
	lastPossibleCol := path.IdxCols[lastColPos]
	lastColManager := &ColWithCmpFuncManager{
		TargetCol:         lastPossibleCol,
		colLength:         path.IdxColLens[lastColPos],
		affectedColSchema: expression.NewSchema(),
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
		ret := indexJoinPathConstructResult(indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, nil, lastColPos)
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
	ret := indexJoinPathConstructResult(indexJoinInfo, buildTmp, mutableRange, path, accesses, remained, lastColManager, lastColPos)
	return ret, false, nil
}

// indexJoinPathCompare compares these 2 index join paths and returns whether the current is better than the base.
func indexJoinPathCompare(best, current *indexJoinPathResult) (curIsBetter bool) {
	// Notice that there may be the cases like `t1.a = t2.a and b > 2 and b < 1`, so ranges can be nil though the conditions are valid.
	// Obviously when the range is nil, we don't need index join.
	if current == nil || len(current.chosenRanges.Range()) == 0 {
		return false
	}
	if best == nil {
		return true
	}
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
	indexJoinInfo *indexJoinPathInfo,
	buildTmp *indexJoinPathTmp,
	ranges ranger.MutableRanges,
	path *util.AccessPath, accesses,
	remained []expression.Expression,
	lastColManager *ColWithCmpFuncManager,
	usedColsLen int) *indexJoinPathResult {
	var innerNDV float64
	if stats := indexJoinInfo.innerStats; stats != nil && stats.StatsVersion != statistics.PseudoVersion {
		innerNDV, _ = cardinality.EstimateColsNDVWithMatchedLen(path.IdxCols[:usedColsLen], indexJoinInfo.innerSchema, stats)
	}
	idxOff2KeyOff := make([]int, len(buildTmp.curIdxOff2KeyOff))
	copy(idxOff2KeyOff, buildTmp.curIdxOff2KeyOff)
	return &indexJoinPathResult{
		chosenPath:     path,
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
	nextCol *expression.Column, cwc *ColWithCmpFuncManager) []expression.Expression {
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
		cwc.appendNewExpr(funcName, anotherArg, affectedCols)
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
		innerStats:            innerChild.StatsInfo(),
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
			if indexJoinPathCompare(bestResult, result) {
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
	for idxOff, keyOff := range bestResult.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	return bestResult, keyOff2IdxOff
}

// ColWithCmpFuncManager is used in index join to handle the column with compare functions(>=, >, <, <=).
// It stores the compare functions and build ranges in execution phase.
type ColWithCmpFuncManager struct {
	TargetCol         *expression.Column
	colLength         int
	OpType            []string
	opArg             []expression.Expression
	TmpConstant       []*expression.Constant
	affectedColSchema *expression.Schema
	compareFuncs      []chunk.CompareFunc
}

// Copy clones the ColWithCmpFuncManager.
func (cwc *ColWithCmpFuncManager) Copy() *ColWithCmpFuncManager {
	if cwc == nil {
		return nil
	}
	cloned := new(ColWithCmpFuncManager)
	if cwc.TargetCol != nil {
		cloned.TargetCol = cwc.TargetCol.Clone().(*expression.Column)
	}
	cloned.colLength = cwc.colLength
	cloned.OpType = make([]string, len(cwc.OpType))
	copy(cloned.OpType, cwc.OpType)
	cloned.opArg = util.CloneExpressions(cwc.opArg)
	cloned.TmpConstant = util.CloneConstants(cwc.TmpConstant)
	cloned.affectedColSchema = cwc.affectedColSchema.Clone()
	cloned.compareFuncs = make([]chunk.CompareFunc, len(cwc.compareFuncs))
	copy(cloned.compareFuncs, cwc.compareFuncs)
	return cloned
}

func (cwc *ColWithCmpFuncManager) appendNewExpr(opName string, arg expression.Expression, affectedCols []*expression.Column) {
	cwc.OpType = append(cwc.OpType, opName)
	cwc.opArg = append(cwc.opArg, arg)
	cwc.TmpConstant = append(cwc.TmpConstant, &expression.Constant{RetType: cwc.TargetCol.RetType})
	for _, col := range affectedCols {
		if cwc.affectedColSchema.Contains(col) {
			continue
		}
		cwc.compareFuncs = append(cwc.compareFuncs, chunk.GetCompareFunc(col.RetType))
		cwc.affectedColSchema.Append(col)
	}
}

// CompareRow compares the rows for deduplicate.
func (cwc *ColWithCmpFuncManager) CompareRow(lhs, rhs chunk.Row) int {
	for i, col := range cwc.affectedColSchema.Columns {
		ret := cwc.compareFuncs[i](lhs, col.Index, rhs, col.Index)
		if ret != 0 {
			return ret
		}
	}
	return 0
}

// BuildRangesByRow will build range of the given row. It will eval each function's arg then call BuildRange.
func (cwc *ColWithCmpFuncManager) BuildRangesByRow(ctx *rangerctx.RangerContext, row chunk.Row) ([]*ranger.Range, error) {
	exprs := make([]expression.Expression, len(cwc.OpType))
	exprCtx := ctx.ExprCtx
	for i, opType := range cwc.OpType {
		constantArg, err := cwc.opArg[i].Eval(exprCtx.GetEvalCtx(), row)
		if err != nil {
			return nil, err
		}
		cwc.TmpConstant[i].Value = constantArg
		newExpr, err := expression.NewFunction(exprCtx, opType, types.NewFieldType(mysql.TypeTiny), cwc.TargetCol, cwc.TmpConstant[i])
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, newExpr) // nozero
	}
	// We already limit range mem usage when buildTemplateRange for inner table of IndexJoin in optimizer phase, so we
	// don't need and shouldn't limit range mem usage when we refill inner ranges during the execution phase.
	ranges, _, _, err := ranger.BuildColumnRange(exprs, ctx, cwc.TargetCol.RetType, cwc.colLength, 0)
	if err != nil {
		return nil, err
	}
	return ranges, nil
}

func (cwc *ColWithCmpFuncManager) resolveIndices(schema *expression.Schema) (err error) {
	for i := range cwc.opArg {
		cwc.opArg[i], err = cwc.opArg[i].ResolveIndices(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

// String implements Stringer interface.
func (cwc *ColWithCmpFuncManager) String() string {
	buffer := bytes.NewBufferString("")
	for i := range cwc.OpType {
		fmt.Fprintf(buffer, "%v(%v, %v)", cwc.OpType[i], cwc.TargetCol, cwc.opArg[i])
		if i < len(cwc.OpType)-1 {
			buffer.WriteString(" ")
		}
	}
	return buffer.String()
}

const emptyColWithCmpFuncManagerSize = int64(unsafe.Sizeof(ColWithCmpFuncManager{}))

// MemoryUsage return the memory usage of ColWithCmpFuncManager
func (cwc *ColWithCmpFuncManager) MemoryUsage() (sum int64) {
	if cwc == nil {
		return
	}

	sum = emptyColWithCmpFuncManagerSize + int64(cap(cwc.compareFuncs))*size.SizeOfFunc
	if cwc.TargetCol != nil {
		sum += cwc.TargetCol.MemoryUsage()
	}
	if cwc.affectedColSchema != nil {
		sum += cwc.affectedColSchema.MemoryUsage()
	}

	for _, str := range cwc.OpType {
		sum += int64(len(str))
	}
	for _, expr := range cwc.opArg {
		sum += expr.MemoryUsage()
	}
	for _, cst := range cwc.TmpConstant {
		sum += cst.MemoryUsage()
	}
	return
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
