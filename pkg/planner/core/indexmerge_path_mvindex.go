// Copyright 2022 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func buildPartialPathUp4MVIndex(
	partialPaths []*util.AccessPath,
	isIntersection bool,
	remainingFilters []expression.Expression,
	histColl *statistics.HistColl,
) *util.AccessPath {
	indexMergePath := &util.AccessPath{PartialIndexPaths: partialPaths, IndexMergeAccessMVIndex: true}
	indexMergePath.IndexMergeIsIntersection = isIntersection
	indexMergePath.TableFilters = remainingFilters
	indexMergePath.CountAfterAccess = float64(histColl.RealtimeCount) *
		cardinality.CalcTotalSelectivityForMVIdxPath(histColl, partialPaths, isIntersection)
	return indexMergePath
}

// buildPartialPaths4MVIndexWithPath builds partial paths by using these accessFilters upon this MVIndex.
// The accessFilters must be corresponding to these idxCols.
// OK indicates whether it builds successfully. These partial paths should be ignored if ok==false.
func buildPartialPaths4MVIndexWithPath(
	sctx planctx.PlanContext,
	accessFilters []expression.Expression,
	idxCols []*expression.Column,
	path *util.AccessPath,
	histColl *statistics.HistColl,
) (
	partialPaths []*util.AccessPath,
	isIntersection bool,
	ok bool,
	err error,
) {
	partialPaths, isIntersection, ok, err = buildPartialPaths4MVIndex(sctx, accessFilters, idxCols, path.Index, histColl)
	if ok && err == nil {
		if path.NoncacheableReason != "" {
			for _, p := range partialPaths {
				p.NoncacheableReason = path.NoncacheableReason
			}
		}
	}
	return
}

func buildPartialPaths4MVIndex(
	sctx planctx.PlanContext,
	accessFilters []expression.Expression,
	idxCols []*expression.Column,
	mvIndex *model.IndexInfo,
	histColl *statistics.HistColl,
) (
	partialPaths []*util.AccessPath,
	isIntersection bool,
	ok bool,
	err error,
) {
	evalCtx := sctx.GetExprCtx().GetEvalCtx()

	var virColID = -1
	for i := range idxCols {
		// index column may contain other virtual column.
		if idxCols[i].VirtualExpr != nil && idxCols[i].VirtualExpr.GetType(evalCtx).IsArray() {
			virColID = i
			break
		}
	}
	if virColID == -1 { // unexpected, no vir-col on this MVIndex
		return nil, false, false, nil
	}
	if len(accessFilters) <= virColID {
		// No filter related to the vir-col, cannot build a path for multi-valued index. Scanning on a multi-valued
		// index will only produce the rows whose corresponding array is not empty.
		return nil, false, false, nil
	}
	// If the condition is related with the array column, all following condition assumes that the array is not empty:
	// `member of`, `json_contains`, `json_overlaps` all return false when the array is empty, except that
	// `json_contains('[]', '[]')` is true. Therefore, using `json_contains(array, '[]')` is also not allowed here.
	//
	// Only when the condition implies that the array is not empty, it'd be safe to scan on multi-valued index without
	// worrying whether the row with empty array will be lost in the result.

	virCol := idxCols[virColID]
	jsonType := virCol.GetType(evalCtx).ArrayType()
	targetJSONPath, ok := unwrapJSONCast(virCol.VirtualExpr)
	if !ok {
		return nil, false, false, nil
	}

	// extract values related to this vir-col, for example, extract [1, 2] from `json_contains(j, '[1, 2]')`
	var virColVals []expression.Expression
	sf, ok := accessFilters[virColID].(*expression.ScalarFunction)
	if !ok {
		return nil, false, false, nil
	}
	switch sf.FuncName.L {
	case ast.JSONMemberOf: // (1 member of a->'$.zip')
		v, ok := unwrapJSONCast(sf.GetArgs()[0]) // cast(1 as json) --> 1
		if !ok {
			return nil, false, false, nil
		}
		virColVals = append(virColVals, v)
	case ast.JSONContains: // (json_contains(a->'$.zip', '[1, 2, 3]')
		isIntersection = true
		virColVals, ok = jsonArrayExpr2Exprs(
			sctx.GetExprCtx(),
			ast.JSONContains,
			sf.GetArgs()[1],
			jsonType,
			true,
		)
		if !ok || len(virColVals) == 0 {
			// json_contains(JSON, '[]') is TRUE. If the row has an empty array, it'll not exist on multi-valued index,
			// but the `json_contains(array, '[]')` is still true, so also don't try to scan on the index.
			return nil, false, false, nil
		}
	case ast.JSONOverlaps: // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		var jsonPathIdx int
		if sf.GetArgs()[0].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
			jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
		} else if sf.GetArgs()[1].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
			jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
		} else {
			return nil, false, false, nil
		}
		var ok bool
		virColVals, ok = jsonArrayExpr2Exprs(
			sctx.GetExprCtx(),
			ast.JSONOverlaps,
			sf.GetArgs()[1-jsonPathIdx],
			jsonType,
			true,
		)
		if !ok || len(virColVals) == 0 { // forbid empty array for safety
			return nil, false, false, nil
		}
	default:
		return nil, false, false, nil
	}

	for _, v := range virColVals {
		if !isSafeTypeConversion4MVIndexRange(v.GetType(evalCtx), virCol.GetType(evalCtx)) {
			return nil, false, false, nil
		}
	}

	for _, v := range virColVals {
		// rewrite json functions to EQ to calculate range, `(1 member of j)` -> `j=1`.
		eq, err := expression.NewFunction(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), virCol, v)
		if err != nil {
			return nil, false, false, err
		}
		newAccessFilters := make([]expression.Expression, len(accessFilters))
		copy(newAccessFilters, accessFilters)
		newAccessFilters[virColID] = eq

		partialPath, ok, err := buildPartialPath4MVIndex(sctx, newAccessFilters, idxCols, mvIndex, histColl)
		if !ok || err != nil {
			return nil, false, ok, err
		}
		partialPaths = append(partialPaths, partialPath)
	}
	return partialPaths, isIntersection, true, nil
}

// isSafeTypeConversion4MVIndexRange checks whether it is safe to convert valType to mvIndexType when building ranges for MVIndexes.
func isSafeTypeConversion4MVIndexRange(valType, mvIndexType *types.FieldType) (safe bool) {
	// for safety, forbid type conversion when building ranges for MVIndexes.
	// TODO: loose this restriction.
	// for example, converting '1' to 1 to access INT MVIndex may cause some wrong result.
	return valType.EvalType() == mvIndexType.EvalType()
}

// buildPartialPath4MVIndex builds a partial path on this MVIndex with these accessFilters.
func buildPartialPath4MVIndex(
	sctx planctx.PlanContext,
	accessFilters []expression.Expression,
	idxCols []*expression.Column,
	mvIndex *model.IndexInfo,
	histColl *statistics.HistColl,
) (*util.AccessPath, bool, error) {
	partialPath := &util.AccessPath{Index: mvIndex}
	partialPath.Ranges = ranger.FullRange()
	for i := range idxCols {
		length := mvIndex.Columns[i].Length
		// For full length prefix index, we consider it as non prefix index.
		// This behavior is the same as in IndexInfo2Cols(), which is used for non mv index.
		if length == idxCols[i].RetType.GetFlen() {
			length = types.UnspecifiedLength
		}
		partialPath.IdxCols = append(partialPath.IdxCols, idxCols[i])
		partialPath.IdxColLens = append(partialPath.IdxColLens, length)
		partialPath.FullIdxCols = append(partialPath.FullIdxCols, idxCols[i])
		partialPath.FullIdxColLens = append(partialPath.FullIdxColLens, length)
	}
	if err := detachCondAndBuildRangeForPath(sctx, partialPath, accessFilters, histColl); err != nil {
		return nil, false, err
	}
	if len(partialPath.AccessConds) != len(accessFilters) || len(partialPath.TableFilters) > 0 {
		// not all filters are used in this case.
		return nil, false, nil
	}
	return partialPath, true, nil
}

// PrepareIdxColsAndUnwrapArrayType collects columns for an index and returns them as []*expression.Column.
// If any column of them is an array type, we will use it's underlying FieldType in the returned Column.RetType.
// If checkOnly1ArrayTypeCol is true, we will check if this index contains only one array type column. If not, it will
// return (nil, false). This check works as a sanity check for an MV index.
// Though this function is introduced for MV index, it can also be used for normal index if you pass false to
// checkOnly1ArrayTypeCol.
// This function is exported for test.
func PrepareIdxColsAndUnwrapArrayType(
	tableInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	tblColsByID map[int64]*expression.Column,
	checkOnly1ArrayTypeCol bool,
) (idxCols []*expression.Column, ok bool) {
	colInfos := tableInfo.Cols()
	var virColNum = 0
	for i := range idxInfo.Columns {
		colOffset := idxInfo.Columns[i].Offset
		colMeta := colInfos[colOffset]
		col, found := tblColsByID[colMeta.ID]
		if !found { // unexpected, no vir-col on this MVIndex
			return nil, false
		}
		if col.GetStaticType().IsArray() {
			virColNum++
			col = col.Clone().(*expression.Column)
			col.RetType = col.GetStaticType().ArrayType() // use the underlying type directly: JSON-ARRAY(INT) --> INT
			col.RetType.SetCharset(charset.CharsetBin)
			col.RetType.SetCollate(charset.CollationBin)
		}
		idxCols = append(idxCols, col)
	}
	if checkOnly1ArrayTypeCol && virColNum != 1 { // assume only one vir-col in the MVIndex
		return nil, false
	}
	return idxCols, true
}

// collectFilters4MVIndex splits these filters into 2 parts where accessFilters can be used to access this index directly.
// For idx(x, cast(a as array), z), `x=1 and (2 member of a) and z=1 and x+z>0` is split to:
// accessFilters: `x=1 and (2 member of a) and z=1`, remaining: `x+z>0`.
func collectFilters4MVIndex(
	sctx planctx.PlanContext,
	filters []expression.Expression,
	idxCols []*expression.Column,
) (accessFilters, remainingFilters []expression.Expression, accessTp int) {
	accessTp = unspecifiedFilterTp
	usedAsAccess := make([]bool, len(filters))
	for _, col := range idxCols {
		found := false
		for i, f := range filters {
			if usedAsAccess[i] {
				continue
			}
			if ok, tp := checkAccessFilter4IdxCol(sctx, f, col); ok {
				accessFilters = append(accessFilters, f)
				usedAsAccess[i] = true
				found = true
				// access filter type on mv col overrides normal col for the return value of this function
				if accessTp == unspecifiedFilterTp || accessTp == eqOnNonMVColTp {
					accessTp = tp
				}
				break
			}
		}
		if !found {
			break
		}
	}
	for i := range usedAsAccess {
		if !usedAsAccess[i] {
			remainingFilters = append(remainingFilters, filters[i])
		}
	}
	return accessFilters, remainingFilters, accessTp
}

// CollectFilters4MVIndexMutations exported for unit test.
// For idx(x, cast(a as array), z), `x=1 and (2 member of a) and (1 member of a) and z=1 and x+z>0` is split to:
// accessFilters combination:
// 1: `x=1 and (2 member of a) and z=1`, remaining: `x+z>0`.
// 2: `x=1 and (1 member of a) and z=1`, remaining: `x+z>0`.
//
// Q: case like idx(x, cast(a as array), z), condition like: x=1 and x=2 and ( 2 member of a)? we can derive the x is invalid range?
// A: no way to here, it will derive an empty range in table path by all these conditions, and the heuristic rule will pick the table-dual table path directly.
//
// Theoretically For idx(x, cast(a as array), z), `x=1 and x=2 and (2 member of a) and (1 member of a) and z=1 and x+z>0` here should be split to:
// 1: `x=1 and x=2 and (2 member of a) and z=1`, remaining: `x+z>0`.
// 2: `x=1 and x=2 and (1 member of a) and z=1`, remaining: `x+z>0`.
// Note: x=1 and x=2 will derive an invalid range in ranger detach, for now because of heuristic rule above, we ignore this case here.
//
// just as the 3rd point as we said in generateANDIndexMerge4ComposedIndex
//
// 3: The predicate of mv index can not converge to a linear interval range at physical phase like EQ and
// GT in normal index. Among the predicates in mv index (member-of/contains/overlap), multi conditions
// about them should be built as self-independent index path, deriving the final intersection/union handles,
// which means a mv index path may be reused for multi related conditions. Here means whether (2 member of a)
// And (1 member of a) is valid composed range or empty range can't be told until runtime intersection/union.
//
// therefore, for multi condition about a single mv index virtual json col here: (2 member of a) and (1 member of a)
// we should build indexMerge above them, and each of them can access to the same mv index. That's why
// we should derive the mutations of virtual json col's access condition, output the accessFilter combination
// for each mutation of it.
//
// In the first case:
// the inputs will be:
//
//	filters:[x=1, (2 member of a), (1 member of a), z=1, x+z>0], idxCols: [x,a,z]
//
// the output will be:
//
//	accessFilters: [x=1, (2 member of a), z=1], remainingFilters: [x+z>0], mvColOffset: 1, mvFilterMutations[(2 member of a), (1 member of a)]
//
// the outer usage will be: accessFilter[mvColOffset] = each element of mvFilterMutations to get the mv access filters mutation combination.
func CollectFilters4MVIndexMutations(sctx base.PlanContext, filters []expression.Expression,
	idxCols []*expression.Column) (accessFilters, remainingFilters []expression.Expression, mvColOffset int, mvFilterMutations []expression.Expression) {
	usedAsAccess := make([]bool, len(filters))
	// accessFilters [x, a<json>, z]
	//                    |
	//                    +----> it may have several substitutions in mvFilterMutations if it's json col.
	mvFilterMutations = make([]expression.Expression, 0, 1)
	mvColOffset = -1
	for z, col := range idxCols {
		found := false
		for i, f := range filters {
			if usedAsAccess[i] {
				continue
			}
			if ok, _ := checkAccessFilter4IdxCol(sctx, f, col); ok {
				if col.VirtualExpr != nil && col.VirtualExpr.GetType(sctx.GetExprCtx().GetEvalCtx()).IsArray() {
					// assert jsonColOffset should always be the same.
					// if the filter is from virtual expression, it means it is about the mv json col.
					mvFilterMutations = append(mvFilterMutations, f)
					if mvColOffset == -1 {
						// means first encountering, recording offset pos, and append it as occupation of access filter.
						mvColOffset = z
						accessFilters = append(accessFilters, f)
					}
					// additional encountering, just map it as used access.
					usedAsAccess[i] = true
					found = true
					continue
				}
				accessFilters = append(accessFilters, f)
				usedAsAccess[i] = true
				found = true
				// shouldn't break once found here, because we want to collect all the mutation mv filters here.
			}
		}
		if !found {
			break
		}
	}
	for i := range usedAsAccess {
		if !usedAsAccess[i] {
			remainingFilters = append(remainingFilters, filters[i])
		}
	}
	return accessFilters, remainingFilters, mvColOffset, mvFilterMutations
}

// cleanAccessPathForMVIndexHint removes all other access path if there is a multi-valued index hint, and this hint
// has a valid path
func cleanAccessPathForMVIndexHint(ds *logicalop.DataSource) {
	forcedMultiValuedIndex := make(map[int64]struct{}, len(ds.PossibleAccessPaths))
	for _, p := range ds.PossibleAccessPaths {
		if !isMVIndexPath(p) || !p.Forced {
			continue
		}
		forcedMultiValuedIndex[p.Index.ID] = struct{}{}
	}
	// no multi-valued index specified, just return
	if len(forcedMultiValuedIndex) == 0 {
		return
	}

	validMVIndexPath := make([]*util.AccessPath, 0, len(ds.PossibleAccessPaths))
	for _, p := range ds.PossibleAccessPaths {
		if indexMergeContainSpecificIndex(p, forcedMultiValuedIndex) {
			validMVIndexPath = append(validMVIndexPath, p)
		}
	}
	if len(validMVIndexPath) > 0 {
		ds.PossibleAccessPaths = validMVIndexPath
	}
}

// indexMergeContainSpecificIndex checks whether the index merge path contains at least one index in the `indexSet`
func indexMergeContainSpecificIndex(path *util.AccessPath, indexSet map[int64]struct{}) bool {
	if path.PartialIndexPaths == nil {
		return false
	}
	for _, p := range path.PartialIndexPaths {
		// NOTE: currently, an index merge access path can only be "a single layer", it's impossible to meet this
		// condition. These codes are just left here for future change.
		if len(p.PartialIndexPaths) > 0 {
			contain := indexMergeContainSpecificIndex(p, indexSet)
			if contain {
				return true
			}
		}

		if p.Index != nil {
			if _, ok := indexSet[p.Index.ID]; ok {
				return true
			}
		}
	}

	return false
}

const (
	unspecifiedFilterTp int = iota
	eqOnNonMVColTp
	multiValuesOROnMVColTp
	multiValuesANDOnMVColTp
	singleValueOnMVColTp
)

// checkAccessFilter4IdxCol checks whether this filter can be used as an accessFilter to access the column of an index,
// and returns which type the access filter is, as defined above.
// Though this function is introduced for MV index, it can also be used for normal index
// If the return value ok is false, the type must be unspecifiedFilterTp.
func checkAccessFilter4IdxCol(
	sctx base.PlanContext,
	filter expression.Expression,
	idxCol *expression.Column,
) (
	ok bool,
	accessFilterTp int,
) {
	sf, ok := filter.(*expression.ScalarFunction)
	if !ok {
		return false, unspecifiedFilterTp
	}
	if idxCol.VirtualExpr != nil { // the virtual column on the MVIndex
		targetJSONPath, ok := unwrapJSONCast(idxCol.VirtualExpr)
		if !ok {
			return false, unspecifiedFilterTp
		}
		var virColVals []expression.Expression
		jsonType := idxCol.GetStaticType().ArrayType()
		var tp int
		switch sf.FuncName.L {
		case ast.JSONMemberOf: // (1 member of a)
			if !targetJSONPath.Equal(sctx.GetExprCtx().GetEvalCtx(), sf.GetArgs()[1]) {
				return false, unspecifiedFilterTp
			}
			v, ok := unwrapJSONCast(sf.GetArgs()[0]) // cast(1 as json) --> 1
			if !ok {
				return false, unspecifiedFilterTp
			}
			virColVals = append(virColVals, v)
			tp = singleValueOnMVColTp
		case ast.JSONContains: // json_contains(a, '1')
			if !targetJSONPath.Equal(sctx.GetExprCtx().GetEvalCtx(), sf.GetArgs()[0]) {
				return false, unspecifiedFilterTp
			}
			virColVals, ok = jsonArrayExpr2Exprs(
				sctx.GetExprCtx(),
				ast.JSONContains,
				sf.GetArgs()[1],
				jsonType,
				false,
			)
			if !ok || len(virColVals) == 0 {
				return false, unspecifiedFilterTp
			}
			tp = multiValuesANDOnMVColTp
		case ast.JSONOverlaps: // json_overlaps(a, '1') or json_overlaps('1', a)
			var jsonPathIdx int
			if sf.GetArgs()[0].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
				jsonPathIdx = 0 // (json_overlaps(a->'$.zip', '[1, 2, 3]')
			} else if sf.GetArgs()[1].Equal(sctx.GetExprCtx().GetEvalCtx(), targetJSONPath) {
				jsonPathIdx = 1 // (json_overlaps('[1, 2, 3]', a->'$.zip')
			} else {
				return false, unspecifiedFilterTp
			}
			var ok bool
			virColVals, ok = jsonArrayExpr2Exprs(
				sctx.GetExprCtx(),
				ast.JSONOverlaps,
				sf.GetArgs()[1-jsonPathIdx],
				jsonType,
				false,
			)
			if !ok || len(virColVals) == 0 { // forbid empty array for safety
				return false, unspecifiedFilterTp
			}
			tp = multiValuesOROnMVColTp
		default:
			return false, unspecifiedFilterTp
		}
		for _, v := range virColVals {
			if !isSafeTypeConversion4MVIndexRange(v.GetType(sctx.GetExprCtx().GetEvalCtx()), idxCol.GetStaticType()) {
				return false, unspecifiedFilterTp
			}
		}
		// If json_contains or json_overlaps only contains one value, like json_overlaps(a,'[1]') or
		// json_contains(a,'[1]'), we can just ignore the AND/OR semantic, and treat them like 1 member of (a).
		if (tp == multiValuesOROnMVColTp || tp == multiValuesANDOnMVColTp) && len(virColVals) == 1 {
			tp = singleValueOnMVColTp
		}
		return true, tp
	}

	// else: non virtual column
	if sf.FuncName.L != ast.EQ { // only support EQ now
		return false, unspecifiedFilterTp
	}
	args := sf.GetArgs()
	var argCol *expression.Column
	var argConst *expression.Constant
	if c, isCol := args[0].(*expression.Column); isCol {
		if con, isCon := args[1].(*expression.Constant); isCon {
			argCol, argConst = c, con
		}
	} else if c, isCol := args[1].(*expression.Column); isCol {
		if con, isCon := args[0].(*expression.Constant); isCon {
			argCol, argConst = c, con
		}
	}
	if argCol == nil || argConst == nil {
		return false, unspecifiedFilterTp
	}
	if argCol.Equal(sctx.GetExprCtx().GetEvalCtx(), idxCol) {
		return true, eqOnNonMVColTp
	}
	return false, unspecifiedFilterTp
}

// jsonArrayExpr2Exprs converts a JsonArray expression to expression list: cast('[1, 2, 3]' as JSON) --> []expr{1, 2, 3}
func jsonArrayExpr2Exprs(
	sctx expression.BuildContext,
	jsonFuncName string,
	jsonArrayExpr expression.Expression,
	targetType *types.FieldType,
	checkForSkipPlanCache bool,
) ([]expression.Expression, bool) {
	if checkForSkipPlanCache && expression.MaybeOverOptimized4PlanCache(sctx, jsonArrayExpr) {
		// skip plan cache and try to generate the best plan in this case.
		sctx.SetSkipPlanCache(jsonFuncName + " function with immutable parameters can affect index selection")
	}
	if !expression.IsImmutableFunc(jsonArrayExpr) || jsonArrayExpr.GetType(sctx.GetEvalCtx()).EvalType() != types.ETJson {
		return nil, false
	}

	jsonArray, isNull, err := jsonArrayExpr.EvalJSON(sctx.GetEvalCtx(), chunk.Row{})
	if isNull || err != nil {
		return nil, false
	}
	if jsonArray.TypeCode != types.JSONTypeCodeArray {
		single, ok := jsonValue2Expr(jsonArray, targetType) // '1' -> []expr{1}
		if ok {
			return []expression.Expression{single}, true
		}
		return nil, false
	}
	elemCnt := jsonArray.GetElemCount()
	exprs := make([]expression.Expression, 0, elemCnt)
	for i := range elemCnt { // '[1, 2, 3]' -> []expr{1, 2, 3}
		expr, ok := jsonValue2Expr(jsonArray.ArrayGetElem(i), targetType)
		if !ok {
			return nil, false
		}
		exprs = append(exprs, expr)
	}
	return exprs, true
}

func jsonValue2Expr(v types.BinaryJSON, targetType *types.FieldType) (expression.Expression, bool) {
	datum, err := expression.ConvertJSON2Tp(v, targetType)
	if err != nil {
		return nil, false
	}
	return &expression.Constant{
		Value:   types.NewDatum(datum),
		RetType: targetType,
	}, true
}

func unwrapJSONCast(expr expression.Expression) (expression.Expression, bool) {
	if expr == nil {
		return nil, false
	}
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}
	if sf == nil || sf.FuncName.L != ast.Cast || sf.GetStaticType().EvalType() != types.ETJson {
		return nil, false
	}
	return sf.GetArgs()[0], true
}

func isMVIndexPath(path *util.AccessPath) bool {
	return !path.IsTablePath() && path.Index != nil && path.Index.MVIndex
}
