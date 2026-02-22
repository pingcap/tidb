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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"go.uber.org/zap"
)

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

// indexJoinPathGetRangeInfoAndMaxOneRow computes the range information string and determines
// whether the index join can guarantee at most one row will be returned per probe.
// This happens when:
// 1. The chosen index is unique AND
// 2. All index columns are used for the join (usedColsLen == len(FullIdxCols)) AND
// 3. Either there are no access conditions, or the last access condition is an equality condition
//
// Parameters:
//   - sctx: the plan context
//   - outerJoinKeys: the outer join keys used to generate range info string
//   - indexJoinResult: the index join path result containing the chosen path and access conditions
//
// Returns:
//   - rangeInfo: a string representation of the range information for explain output
//   - maxOneRow: true if the index join guarantees at most one row per probe
func indexJoinPathGetRangeInfoAndMaxOneRow(
	sctx planctx.PlanContext,
	outerJoinKeys []*expression.Column,
	indexJoinResult *indexJoinPathResult) (rangeInfo string, maxOneRow bool) {
	rangeInfo = indexJoinPathRangeInfo(sctx, outerJoinKeys, indexJoinResult)
	maxOneRow = false
	if indexJoinResult.chosenPath.Index.Unique && indexJoinResult.usedColsLen == len(indexJoinResult.chosenPath.FullIdxCols) {
		l := len(indexJoinResult.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := indexJoinResult.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	return rangeInfo, maxOneRow
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
