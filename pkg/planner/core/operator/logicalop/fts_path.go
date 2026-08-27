// Copyright 2026 PingCAP, Inc.
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

package logicalop

import (
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

// FTSMatchWordDirtyTxnErrMsg is the single planner error text for full-text
// searches against transaction-local changes.
const FTSMatchWordDirtyTxnErrMsg = "FTS_MATCH_WORD() cannot be used in a transaction with uncommitted changes"

var ftsScoreFieldType = func() types.FieldType {
	tp := *types.NewFieldType(mysql.TypeFloat)
	tp.SetFlag(mysql.NotNullFlag)
	return tp
}()

// NewFTSScoreFieldType returns the logical full-text score column type.
func NewFTSScoreFieldType() *types.FieldType {
	return ftsScoreFieldType.Clone()
}

// resolveFTSWherePredicate consumes the one legal predicate owned by this
// DataSource. Unsupported, unpushed, or multiple bindings remain untouched for
// the final whole-plan rejection rule.
func (ds *DataSource) resolveFTSWherePredicate() error {
	ftsCondIndexes := collectFTSConditionIndexes(ds.PushedDownConds)
	// TODO: we only support single search function in this phase,
	// we will extend this in the next dev round.
	if ftsCondIndexes.Len() != 1 {
		return nil
	}
	matchedCondIndex, ok := ftsCondIndexes.Next(0)
	if !ok {
		return nil
	}
	searchExpr := unwrapFTSPredicate(ds.PushedDownConds[matchedCondIndex])
	if searchExpr == nil {
		return nil
	}
	newAllConds, newPushedDownConds, owned := prepareFTSConditionRemoval(ds, ftsCondIndexes)
	if !owned {
		return nil
	}
	if !hasPublicFTSIndex(ds.TableInfo) {
		return plannererrors.ErrWrongUsage.FastGen("full text search can only be used with a matching current TiFlash full-text index path")
	}
	if util.TableHasDirtyContent(ds.SCtx(), ds.TableInfo) {
		return plannererrors.ErrWrongUsage.FastGen(FTSMatchWordDirtyTxnErrMsg)
	}
	if len(ds.IndexMergeHints) > 0 {
		return plannererrors.ErrWrongUsage.FastGen("INDEX_MERGE hints are incompatible with full-text index access")
	}
	replica := ds.TableInfo.TiFlashReplica
	if replica == nil || !replica.Available {
		return plannererrors.ErrWrongUsage.FastGen("Full text search requires an available TiFlash replica")
	}
	if _, ok := ds.SCtx().GetSessionVars().GetIsolationReadEngines()[kv.TiFlash]; !ok {
		return plannererrors.ErrWrongUsage.FastGen("Full text search requires TiFlash in tidb_isolation_read_engines")
	}
	if ds.PreferStoreType&h.PreferTiKV != 0 && ds.PreferStoreType&h.PreferTiFlash == 0 {
		return plannererrors.ErrWrongUsage.FastGen("Full text search cannot use a TiKV-only storage preference")
	}
	selected, err := selectFTSIndex(ds, ftsCondIndexes)
	if err != nil {
		return plannererrors.ErrWrongUsage.FastGen("%s", err.Error())
	}
	if selected == nil || !selected.matchedCondIndexes.Equals(ftsCondIndexes) {
		return nil
	}
	if _, ok := selected.requiredRebuilt[matchedCondIndex]; !ok {
		return plannererrors.ErrWrongUsage.FastGen("failed to rebuild the full-text query expression")
	}
	removableCondIndexes := selected.removableCondIndexes
	rebuiltConds := selected.rebuiltConds
	if len(rebuiltConds) == 0 {
		return plannererrors.ErrWrongUsage.FastGen("failed to rebuild the full-text query expression")
	}
	if removableCondIndexes.Len() > ftsCondIndexes.Len() {
		var allOwned bool
		newAllConds, newPushedDownConds, allOwned = prepareFTSConditionRemoval(ds, removableCondIndexes)
		if !allOwned {
			// Keep all rebuilt conditions as safe FTS prefilters, but fall back to
			// removing only the explicit lossless FTS predicate. Exact rewrites
			// are still necessary conditions when their originals remain residual.
			newAllConds, newPushedDownConds, _ = prepareFTSConditionRemoval(ds, ftsCondIndexes)
		}
	}
	rebuiltExpr := expression.ComposeCNFCondition(ds.SCtx().GetExprCtx(), rebuiltConds...)
	if !containsPositiveFTSMatch(rebuiltExpr) {
		return plannererrors.ErrWrongUsage.FastGen("scored pure-negative FTS match expression is not supported")
	}
	if !expression.CanExprsPushDown(
		util.GetPushDownCtx(ds.SCtx()),
		[]expression.Expression{rebuiltExpr},
		kv.TiFlash,
	) {
		return plannererrors.ErrWrongUsage.FastGen("full-text query expression cannot be pushed down to TiFlash")
	}
	matchExpr, err := expression.FTSQueryExpressionToPB(
		ds.SCtx().GetExprCtx().GetEvalCtx(),
		ds.SCtx().GetClient(),
		rebuiltExpr,
	)
	if err != nil {
		return plannererrors.ErrWrongUsage.FastGen("%s", err.Error())
	}
	maxTopK := ^uint32(0)
	queryInfo := &tipb.FTSQueryInfo{
		QueryType: tipb.FTSQueryType_FTSQueryTypeWithScore,
		IndexId:   selected.index.ID,
		TopK:      &maxTopK,
		QueryFunc: ftsQueryFunc(rebuiltExpr),
		MatchExpr: []tipb.Expr{matchExpr},
	}

	scoreColumnInfo := &model.ColumnInfo{
		Name:      ast.NewCIStr("_FTS_SCORE"),
		ID:        model.VirtualColFTSScoreID,
		FieldType: *NewFTSScoreFieldType(),
	}
	scoreColumn := &expression.Column{
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.VirtualColFTSScoreID,
		RetType:  NewFTSScoreFieldType(),
		OrigName: "_FTS_SCORE",
	}

	hadForceKeepOrder := selected.path.ForceKeepOrder
	selected.path.ForceKeepOrder = false
	ds.PossibleAccessPaths = []*util.AccessPath{selected.path}
	ds.AllPossibleAccessPaths = ds.PossibleAccessPaths
	ds.AllConds = newAllConds
	ds.PushedDownConds = newPushedDownConds
	ds.FtsPushDown = queryInfo
	ds.Columns = append(ds.Columns, scoreColumnInfo)
	ds.Schema().Append(scoreColumn)
	if hadForceKeepOrder {
		ds.SCtx().GetSessionVars().StmtCtx.SetHintWarning("Optimizer Hint ORDER_INDEX is inapplicable for a full-text index because it does not provide natural order")
	}
	return nil
}

func hasPublicFTSIndex(table *model.TableInfo) bool {
	if table == nil {
		return false
	}
	for _, index := range table.Indices {
		if index != nil && index.IsPublic() && index.FullTextInfo != nil {
			return true
		}
	}
	return false
}

// containsPositiveFTSMatch mirrors the scored-query validation in the columnar
// engine. A match below NOT does not make that subtree positive.
func containsPositiveFTSMatch(expr expression.Expression) bool {
	scalar, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch scalar.FuncName.L {
	case ast.FTSMatchWord:
		return true
	case ast.LogicAnd, ast.LogicOr, ast.IsTruthWithNull:
		for _, child := range scalar.GetArgs() {
			if containsPositiveFTSMatch(child) {
				return true
			}
		}
	case ast.UnaryNot:
		return false
	}
	return false
}

func collectFTSIndexCoveredConditions(
	ds *DataSource,
	index *model.IndexInfo,
	requiredIndexes intset.FastIntSet,
	requiredRebuilt map[int]expression.Expression,
) (intset.FastIntSet, intset.FastIntSet, []expression.Expression) {
	removableIndexes := intset.NewFastIntSet()
	coveredIndexes := intset.NewFastIntSet()
	rebuiltConds := make([]expression.Expression, 0, len(ds.PushedDownConds))
	pushDownCtx := util.GetPushDownCtx(ds.SCtx())
	for i, condition := range ds.PushedDownConds {
		if requiredIndexes.Has(i) {
			rebuilt, ok := requiredRebuilt[i]
			if !ok {
				continue
			}
			removableIndexes.Insert(i)
			coveredIndexes.Insert(i)
			rebuiltConds = append(rebuiltConds, rebuilt)
			continue
		}

		candidate := unwrapFTSPredicate(condition)
		// Conditions containing FTS_MATCH_WORD cannot remain as TiDB-side
		// residual filters. They are accepted only through the lossless path.
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(
			ds.SCtx().GetExprCtx(), candidate, ds.TableInfo, index,
		)
		if ok && expression.CanExprsPushDown(pushDownCtx, []expression.Expression{rebuilt}, kv.TiFlash) {
			removableIndexes.Insert(i)
			coveredIndexes.Insert(i)
			rebuiltConds = append(rebuiltConds, rebuilt)
			continue
		}
		if expression.ContainsFullTextSearchFn(candidate) {
			continue
		}

		rebuilt, ok = buildColumnarFTSPrefilter(
			ds.SCtx().GetExprCtx(), candidate, ds.TableInfo, index,
		)
		if !ok || !expression.CanExprsPushDown(pushDownCtx, []expression.Expression{rebuilt}, kv.TiFlash) {
			continue
		}
		// A prefilter is only a necessary condition. Add it to the FTS query,
		// but deliberately leave the original condition as a residual.
		coveredIndexes.Insert(i)
		rebuiltConds = append(rebuiltConds, rebuilt)
	}
	return removableIndexes, coveredIndexes, rebuiltConds
}

func collectFTSConditionIndexes(conditions []expression.Expression) intset.FastIntSet {
	indexes := intset.NewFastIntSet()
	for i, condition := range conditions {
		if expression.ContainsFullTextSearchFn(condition) {
			indexes.Insert(i)
		}
	}
	return indexes
}

func resolveFTSQueryClause(index *model.IndexInfo, info *expression.FTSInfo) (*expression.QueryClause, error) {
	clause := *info.Clause
	clause.Fields = make([]expression.QueryField, 0, len(info.Columns))
	for i, column := range info.Columns {
		indexFields := index.FullTextInfo.IndexFieldsForColumn(column.ID)
		if len(indexFields) == 0 {
			return nil, errors.Errorf("full-text search index does not cover column %s", column.OrigName)
		}

		parserType := ast.FullTextParserTypeInvalid
		requestedParser := info.Clause.Fields[i].IndexField
		if requestedParser != "" {
			parserType = ast.GetFullTextParserTypeByName(requestedParser)
			if parserType == ast.FullTextParserTypeInvalid {
				return nil, errors.Errorf("unknown full-text parser type %q", requestedParser)
			}
			if !index.FullTextInfo.HasAnalyzerTypeForColumn(column.ID, parserType) {
				return nil, errors.Errorf(
					"full-text index %s does not provide parser %s for column %s",
					index.Name, requestedParser, column.OrigName,
				)
			}
		} else {
			// Keep parser-less queries compatible with legacy syntax by using
			// the first parser declared for this column.
			parserType = indexFields[0].IndexFieldOption.AnalyzerType
		}
		if parserType == ast.FullTextParserTypeExactV1 {
			return nil, errors.NewNoStackError(
				"FTS_MATCH_WORD does not support fields using the EXACT parser; use = or IN predicates instead",
			)
		}

		clause.Fields = append(clause.Fields, expression.QueryField{
			ColID:      column.ID,
			IndexField: string(parserType),
		})
	}
	return &clause, nil
}

func analyzeFTSMatchWord(match *expression.ScalarFunction) (*expression.FTSInfo, error) {
	query, ok := match.GetArgs()[0].(*expression.Constant)
	if !ok || query.RetType == nil || !query.RetType.EvalType().IsStringKind() ||
		!isStaticNonNullFTSConstant(query) {
		return nil, errors.NewNoStackError("invalid FTS_MATCH_WORD query argument")
	}
	info := expression.InterpretFullTextSearchExpr(match)
	if info == nil || info.Clause == nil || len(info.Columns) == 0 ||
		len(info.Columns) != len(info.Clause.Fields) {
		return nil, errors.NewNoStackError("invalid FTS_MATCH_WORD query expression")
	}
	for i := range info.Columns {
		parserName := info.Clause.Fields[i].IndexField
		if parserName != "" && ast.GetFullTextParserTypeByName(parserName) == ast.FullTextParserTypeInvalid {
			return nil, errors.Errorf("unknown full-text parser type %q", parserName)
		}
	}
	return info, nil
}

// ftsIndexCandidate contains the required FTS binding and every additional
// pushed-down condition that the same index can either evaluate losslessly or
// use as a safe prefilter. All condition sets refer to positions in
// DataSource.PushedDownConds; only removableCondIndexes may be removed from the
// residual filters.
type ftsIndexCandidate struct {
	index                *model.IndexInfo
	path                 *util.AccessPath
	matchedCondIndexes   intset.FastIntSet
	requiredRebuilt      map[int]expression.Expression
	removableCondIndexes intset.FastIntSet
	coveredCondIndexes   intset.FastIntSet
	rebuiltConds         []expression.Expression
}

// additionalCoveredConditionCount excludes the required FTS predicates,
// which every eligible candidate must already cover.
func (c *ftsIndexCandidate) additionalCoveredConditionCount() int {
	return c.coveredCondIndexes.Len() - c.matchedCondIndexes.Len()
}

func selectFTSIndex(
	ds *DataSource,
	ftsCondIndexes intset.FastIntSet,
) (*ftsIndexCandidate, error) {
	if ftsCondIndexes.IsEmpty() {
		return nil, nil
	}
	if ds == nil || ds.TableInfo == nil {
		return nil, errors.NewNoStackError("full-text search data source has no table metadata")
	}
	validCondIndexes := true
	ftsCondIndexes.ForEach(func(i int) {
		if i < 0 || i >= len(ds.PushedDownConds) {
			validCondIndexes = false
		}
	})
	if !validCondIndexes {
		return nil, errors.NewNoStackError("invalid full-text pushed-down condition index")
	}

	// Evaluate every current FTS path. A greater covered-condition count wins;
	// retaining the existing winner on ties makes metadata order deterministic.
	var bindingErr error
	var best *ftsIndexCandidate
	for _, index := range ds.TableInfo.Indices {
		path := findCurrentFTSPath(ds.PossibleAccessPaths, index.ID)
		if path == nil {
			continue
		}
		matchedCondIndexes := intset.NewFastIntSet()
		rebuiltExprs := make(map[int]expression.Expression, ftsCondIndexes.Len())
		fullyCovered := true
		ftsCondIndexes.ForEach(func(i int) {
			searchExpr := unwrapFTSPredicate(ds.PushedDownConds[i])
			rebuiltExpr, ok, err := checkExprCoveredByColumnarFTSAndRebuild(
				ds.SCtx().GetExprCtx(), searchExpr, ds.TableInfo, index,
			)
			if !ok {
				if bindingErr == nil && err != nil {
					bindingErr = err
				}
				fullyCovered = false
				return
			}
			matchedCondIndexes.Insert(i)
			rebuiltExprs[i] = rebuiltExpr
		})
		if !fullyCovered {
			continue
		}
		removableCondIndexes, coveredCondIndexes, rebuiltConds := collectFTSIndexCoveredConditions(
			ds, index, ftsCondIndexes, rebuiltExprs,
		)
		candidate := &ftsIndexCandidate{
			index:                index,
			path:                 path,
			matchedCondIndexes:   matchedCondIndexes,
			requiredRebuilt:      rebuiltExprs,
			removableCondIndexes: removableCondIndexes,
			coveredCondIndexes:   coveredCondIndexes,
			rebuiltConds:         rebuiltConds,
		}
		if best == nil || candidate.additionalCoveredConditionCount() > best.additionalCoveredConditionCount() {
			best = candidate
		}
	}
	if best != nil {
		return best, nil
	}
	// A matching metadata index may have been removed from the current access
	// paths by FORCE/IGNORE INDEX. Report that path mismatch instead of a
	// binding error from an unrelated remaining full-text index.
	for _, index := range ds.TableInfo.Indices {
		if index == nil || !index.IsPublic() || index.FullTextInfo == nil ||
			findCurrentFTSPath(ds.PossibleAccessPaths, index.ID) != nil {
			continue
		}
		fullyCovered := true
		ftsCondIndexes.ForEach(func(i int) {
			if !fullyCovered {
				return
			}
			searchExpr := unwrapFTSPredicate(ds.PushedDownConds[i])
			_, ok, _ := checkExprCoveredByColumnarFTSAndRebuild(
				ds.SCtx().GetExprCtx(), searchExpr, ds.TableInfo, index,
			)
			fullyCovered = ok
		})
		if fullyCovered {
			return nil, errors.NewNoStackError("full text search can only be used with a matching current TiFlash full-text index path")
		}
	}
	if bindingErr != nil {
		return nil, bindingErr
	}
	return nil, errors.NewNoStackError("full text search can only be used with a matching current TiFlash full-text index path")
}

func findCurrentFTSPath(paths []*util.AccessPath, indexID int64) *util.AccessPath {
	for _, path := range paths {
		if path != nil && path.Index != nil && path.Index.ID == indexID {
			return path
		}
	}
	return nil
}

func unwrapFTSPredicate(predicate expression.Expression) expression.Expression {
	wrapper, ok := predicate.(*expression.ScalarFunction)
	if !ok || wrapper.FuncName.L != ast.IsTruthWithNull || len(wrapper.GetArgs()) != 1 {
		return predicate
	}
	return wrapper.GetArgs()[0]
}

func ftsQueryFunc(expr expression.Expression) tipb.ScalarFuncSig {
	if scalar, ok := expr.(*expression.ScalarFunction); ok && scalar.FuncName.L == ast.FTSMatchWord {
		return tipb.ScalarFuncSig_FTSMatchWord
	}
	return tipb.ScalarFuncSig_FTSMatchExpression
}

func prepareFTSConditionRemoval(ds *DataSource, matchedCondIndexes intset.FastIntSet) (
	[]expression.Expression,
	[]expression.Expression,
	bool,
) {
	if ds == nil || matchedCondIndexes.IsEmpty() {
		return nil, nil, false
	}
	matchedExpressions := make([]expression.Expression, 0, matchedCondIndexes.Len())
	validIndexes := true
	matchedCondIndexes.ForEach(func(i int) {
		if i >= len(ds.PushedDownConds) {
			validIndexes = false
			return
		}
		matchedExpressions = append(matchedExpressions, ds.PushedDownConds[i])
	})
	if !validIndexes {
		return nil, nil, false
	}

	removeMatched := func(conditions []expression.Expression) ([]expression.Expression, []int, bool) {
		result := make([]expression.Expression, 0, len(conditions))
		counts := make([]int, len(matchedExpressions))
		hasExtraFTS := false
		for _, condition := range conditions {
			matched := false
			for i, matchedExpression := range matchedExpressions {
				if condition == matchedExpression {
					counts[i]++
					matched = true
					break
				}
			}
			if matched {
				continue
			}
			if expression.ContainsFullTextSearchFn(condition) {
				hasExtraFTS = true
			}
			result = append(result, condition)
		}
		return result, counts, hasExtraFTS
	}

	newAllConds, allCounts, allHasExtraFTS := removeMatched(ds.AllConds)
	newPushedDownConds, pushedCounts, pushedHasExtraFTS := removeMatched(ds.PushedDownConds)
	if allHasExtraFTS || pushedHasExtraFTS {
		return nil, nil, false
	}
	for i := range matchedExpressions {
		if allCounts[i] != 1 || pushedCounts[i] != 1 {
			return nil, nil, false
		}
	}
	return newAllConds, newPushedDownConds, true
}

// IsExactFTSPath reports whether path is the selected exact full-text path for
// the DataSource. Handle flags do not imply ordering for such a path.
func IsExactFTSPath(ds *DataSource, path *util.AccessPath) bool {
	return ds != nil && ds.FtsPushDown != nil && len(ds.PossibleAccessPaths) == 1 &&
		ds.PossibleAccessPaths[0] == path
}

// CheckExprCoveredByColumnarFTSAndRebuild checks whether expr can be evaluated
// entirely by index and returns a fresh expression tree whose columns are bound
// to the corresponding full-text index fields. It never mutates expr.
func CheckExprCoveredByColumnarFTSAndRebuild(
	ctx expression.BuildContext,
	expr expression.Expression,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool) {
	if ctx == nil || table == nil || index == nil || !index.IsPublic() || index.FullTextInfo == nil {
		return nil, false
	}
	rebuilt, ok, _ := checkExprCoveredByColumnarFTSAndRebuild(ctx, expr, table, index)
	return rebuilt, ok
}

// buildColumnarFTSPrefilter derives a necessary full-text condition from expr.
// It must only be used when expr contains no FTS_MATCH_WORD: the original expr
// remains as a TiDB-side residual filter to remove false positives.
func buildColumnarFTSPrefilter(
	ctx expression.BuildContext,
	expr expression.Expression,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool) {
	if ctx == nil || table == nil || index == nil || !index.IsPublic() || index.FullTextInfo == nil ||
		expression.ContainsFullTextSearchFn(expr) {
		return nil, false
	}
	return buildColumnarFTSPrefilterRecursive(ctx, expr, table, index)
}

// buildColumnarFTSPrefilterRecursive preserves only implications that remain
// necessary conditions: an AND may keep any supported conjunct, while an OR
// is usable only when every disjunct can be rebuilt. NOT is rejected because
// negating a superset would make the prefilter capable of dropping valid rows.
func buildColumnarFTSPrefilterRecursive(
	ctx expression.BuildContext,
	expr expression.Expression,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool) {
	if rebuilt, ok, _ := checkExprCoveredByColumnarFTSAndRebuild(ctx, expr, table, index); ok {
		return rebuilt, true
	}

	scalar, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}
	switch scalar.FuncName.L {
	case ast.LogicAnd:
		children := make([]expression.Expression, 0, len(scalar.GetArgs()))
		for _, child := range scalar.GetArgs() {
			if rebuilt, ok := buildColumnarFTSPrefilterRecursive(ctx, child, table, index); ok {
				children = append(children, rebuilt)
			}
		}
		if len(children) == 0 {
			return nil, false
		}
		return expression.ComposeCNFCondition(ctx, children...), true
	case ast.LogicOr:
		children := make([]expression.Expression, 0, len(scalar.GetArgs()))
		for _, child := range scalar.GetArgs() {
			rebuilt, ok := buildColumnarFTSPrefilterRecursive(ctx, child, table, index)
			if !ok {
				return nil, false
			}
			children = append(children, rebuilt)
		}
		return expression.ComposeDNFCondition(ctx, children...), len(children) > 0
	case ast.IsTruthWithNull:
		if len(scalar.GetArgs()) != 1 {
			return nil, false
		}
		return buildColumnarFTSPrefilterRecursive(ctx, scalar.GetArgs()[0], table, index)
	case ast.UnaryNot:
		return nil, false
	case ast.Like, ast.Ilike:
		return rebuildFTSLikePrefilter(ctx, scalar, table, index)
	default:
		return nil, false
	}
}

func checkExprCoveredByColumnarFTSAndRebuild(
	ctx expression.BuildContext,
	expr expression.Expression,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool, error) {
	e, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return nil, false, nil
	}
	switch e.FuncName.L {
	case ast.LogicAnd, ast.LogicOr:
		childExprs := make([]expression.Expression, 0, len(e.GetArgs()))
		for _, child := range e.GetArgs() {
			innerExpr, ok, err := checkExprCoveredByColumnarFTSAndRebuild(ctx, child, table, index)
			if !ok {
				return nil, false, err
			}
			childExprs = append(childExprs, innerExpr)
		}
		rebuilt, ok := rebuildFTSScalarFunction(ctx, e, childExprs...)
		return rebuilt, ok, nil
	case ast.UnaryNot, ast.IsTruthWithNull:
		child, ok, err := checkExprCoveredByColumnarFTSAndRebuild(ctx, e.GetArgs()[0], table, index)
		if !ok {
			return nil, false, err
		}
		if e.FuncName.L == ast.IsTruthWithNull {
			return child, true, nil
		}
		rebuilt, ok := rebuildFTSScalarFunction(ctx, e, child)
		return rebuilt, ok, nil
	case ast.EQ:
		column, constant, ok := extractFTSColumnConstant(e.GetArgs()[0], e.GetArgs()[1])
		if !ok {
			return nil, false, nil
		}
		_, comparisonCollation := e.CharsetAndCollation()
		rebuilt, ok := rebuildFTSExactComparison(ctx, e.RetType, column, constant, comparisonCollation, table, index)
		return rebuilt, ok, nil
	case ast.Like:
		_, collation := e.CharsetAndCollation()
		if collate.IsCICollation(collation) {
			return nil, false, nil
		}
		column, ok1 := e.GetArgs()[0].(*expression.Column)
		pattern, ok2 := e.GetArgs()[1].(*expression.Constant)
		escape, ok3 := e.GetArgs()[2].(*expression.Constant)
		if !ok1 || !ok2 || !ok3 {
			return nil, false, nil
		}
		field, literal, ok := matchFTSLikeIndexField(column, pattern, escape, table, index)
		if !ok {
			return nil, false, nil
		}
		rebuilt, ok := rebuildFTSIndexMatch(ctx, column, literal, literal, field)
		return rebuilt, ok, nil
	case ast.In:
		args := e.GetArgs()
		column, ok := args[0].(*expression.Column)
		if !ok {
			return nil, false, nil
		}
		equalities := make([]expression.Expression, 0, len(args)-1)
		_, comparisonCollation := e.CharsetAndCollation()
		for _, arg := range args[1:] {
			constant, ok := arg.(*expression.Constant)
			if !ok {
				return nil, false, nil
			}
			equality, ok := rebuildFTSExactComparison(ctx, e.RetType, column, constant, comparisonCollation, table, index)
			if !ok {
				return nil, false, nil
			}
			equalities = append(equalities, equality)
		}
		return expression.ComposeDNFCondition(ctx, equalities...), true, nil
	case ast.FTSMatchWord:
		rebuilt, err := rebuildFTSMatchWord(ctx, e, table, index)
		return rebuilt, err == nil, err
	default:
		return nil, false, nil
	}
}

func rebuildFTSScalarFunction(
	ctx expression.BuildContext,
	original *expression.ScalarFunction,
	args ...expression.Expression,
) (expression.Expression, bool) {
	if original.RetType == nil {
		return nil, false
	}
	rebuilt := expression.NewFunctionInternal(ctx, original.FuncName.L, original.RetType.Clone(), args...)
	if rebuilt == nil {
		return nil, false
	}
	return rebuilt, true
}

func extractFTSColumnConstant(lhs, rhs expression.Expression) (*expression.Column, *expression.Constant, bool) {
	if column, ok := lhs.(*expression.Column); ok {
		constant, ok := rhs.(*expression.Constant)
		return column, constant, ok
	}
	column, ok := rhs.(*expression.Column)
	if !ok {
		return nil, nil, false
	}
	constant, ok := lhs.(*expression.Constant)
	return column, constant, ok
}

func rebuildFTSExactComparison(
	ctx expression.BuildContext,
	retType *types.FieldType,
	column *expression.Column,
	constant *expression.Constant,
	comparisonCollation string,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool) {
	field, ok := findFTSIndexField(table, index, column.ID, ast.FullTextParserTypeExactV1)
	if !ok || retType == nil {
		return nil, false
	}
	constant, ok = materializeFTSConstant(constant)
	if !ok || !isStaticNonNullFTSConstant(constant) {
		return nil, false
	}
	queryValue, ok := ftsExactQueryValue(ctx, column, constant, comparisonCollation)
	if !ok {
		return nil, false
	}
	queryText, err := constant.Value.ToString()
	if err != nil {
		return nil, false
	}
	return rebuildFTSIndexMatch(ctx, column, queryText, queryValue, field)
}

// materializeFTSConstant freezes a parameter marker at the value already
// assigned for the current execution. FTS plans are not cached, so the planner
// rebuilds this expression after prepared parameters have been populated. Keep
// Value instead of evaluating the marker again because expression construction
// may already have refined it to the comparison or function argument type.
func materializeFTSConstant(constant *expression.Constant) (*expression.Constant, bool) {
	if constant == nil || constant.RetType == nil || constant.DeferredExpr != nil || constant.Value.IsNull() {
		return nil, false
	}
	if constant.ParamMarker == nil {
		return constant, true
	}
	materialized := *constant
	materialized.ParamMarker = nil
	return &materialized, true
}

func ftsExactQueryValue(
	ctx expression.BuildContext,
	column *expression.Column,
	constant *expression.Constant,
	comparisonCollation string,
) (any, bool) {
	if ctx == nil || column == nil || column.RetType == nil ||
		!isStaticNonNullFTSConstant(constant) || constant.RetType == nil {
		return nil, false
	}

	targetType := column.RetType
	targetTp := targetType.GetType()
	if targetTp == mysql.TypeEnum {
		return ftsEnumExactQueryValue(targetType, constant, comparisonCollation)
	}

	if types.IsString(targetTp) &&
		(!types.IsNonBinaryStr(targetType) || !types.IsStringKind(constant.Value.Kind())) {
		return nil, false
	}
	if targetTp == mysql.TypeBit {
		// SQL comparison coerces BIT/string operands numerically, whereas direct
		// conversion to BIT treats string bytes as a binary literal.
		switch constant.Value.Kind() {
		case types.KindInt64, types.KindUint64, types.KindBinaryLiteral, types.KindMysqlBit:
		default:
			return nil, false
		}
	}

	conversionType := targetType
	if targetTp == mysql.TypeNewDate {
		conversionType = targetType.Clone()
		conversionType.SetType(mysql.TypeDate)
	}
	converted, ok := expression.ConvertConstantLosslessly(
		ctx.GetEvalCtx(), constant, conversionType, collate.GetBinaryCollator(),
	)
	if !ok {
		return nil, false
	}
	return ftsExactQueryStorageValue(ctx.GetEvalCtx().Location(), targetType, converted)
}

func ftsExactQueryStorageValue(
	location *time.Location,
	targetType *types.FieldType,
	value types.Datum,
) (any, bool) {
	targetTp := targetType.GetType()
	switch targetTp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(targetType.GetFlag()) {
			return value.GetUint64(), true
		}
		return value.GetInt64(), true
	case mysql.TypeBit:
		queryValue, err := value.GetBinaryLiteral().ToInt(types.StrictContext.WithLocation(location))
		return queryValue, err == nil
	case mysql.TypeYear:
		queryValue := value.GetInt64()
		if queryValue < 0 {
			return nil, false
		}
		return uint64(queryValue), true
	case mysql.TypeFloat, mysql.TypeDouble:
		queryValue := value.GetFloat64()
		if math.IsNaN(queryValue) || math.IsInf(queryValue, 0) {
			return nil, false
		}
		return queryValue, true
	case mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		queryValue := value.GetMysqlTime()
		if targetTp == mysql.TypeTimestamp && location != time.UTC {
			if err := queryValue.ConvertTimeZone(location, time.UTC); err != nil {
				return nil, false
			}
		}
		packed, err := queryValue.ToPackedUint()
		return packed, err == nil
	case mysql.TypeDuration:
		return int64(value.GetMysqlDuration().Duration), true
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return value.GetString(), true
	default:
		return nil, false
	}
}

// ftsEnumExactQueryValue encodes an ENUM predicate as its storage ordinal.
// TiDB compares ENUM with strings by label, but switches ENUM to its numeric
// ordinal when the comparison expression marks it with EnumSetAsIntFlag.
func ftsEnumExactQueryValue(
	targetType *types.FieldType,
	constant *expression.Constant,
	comparisonCollation string,
) (any, bool) {
	if targetType == nil || constant == nil {
		return nil, false
	}

	if targetType.EvalType() == types.ETInt {
		var ordinal uint64
		switch constant.Value.Kind() {
		case types.KindInt64:
			value := constant.Value.GetInt64()
			if value < 0 {
				return nil, false
			}
			ordinal = uint64(value)
		case types.KindUint64:
			ordinal = constant.Value.GetUint64()
		case types.KindMysqlEnum:
			ordinal = constant.Value.GetMysqlEnum().Value
		default:
			return nil, false
		}
		if ordinal > uint64(len(targetType.GetElems())) {
			return nil, false
		}
		return ordinal, true
	}
	if targetType.EvalType() != types.ETString {
		return nil, false
	}

	var label string
	switch constant.Value.Kind() {
	case types.KindString, types.KindBytes:
		label = constant.Value.GetString()
	case types.KindMysqlEnum:
		label = constant.Value.GetMysqlEnum().Name
	default:
		return nil, false
	}

	if comparisonCollation == "" {
		comparisonCollation = targetType.GetCollate()
	}
	collator := collate.GetCollator(comparisonCollation)
	matchedOrdinal := uint64(0)
	matchCount := 0
	// ENUM's invalid/zero value has an empty string label and is physically
	// stored as ordinal 0, so it participates in string comparison as well.
	if collator.Compare("", label) == 0 {
		matchCount++
	}
	for i, elem := range targetType.GetElems() {
		if collator.Compare(elem, label) == 0 {
			matchedOrdinal = uint64(i) + 1
			matchCount++
		}
	}
	if matchCount != 1 {
		return nil, false
	}
	return matchedOrdinal, true
}

func rebuildFTSIndexMatch(
	ctx expression.BuildContext,
	column *expression.Column,
	queryText string,
	queryValue any,
	field ast.FullTextIndexFieldInfo,
) (expression.Expression, bool) {
	if column == nil {
		return nil, false
	}
	matchColumn := column.Clone().(*expression.Column)
	if matchColumn.RetType.EvalType() != types.ETString {
		matchColumn.RetType = types.NewFieldType(mysql.TypeVarchar)
	}
	rebuilt := expression.NewFunctionInternal(
		ctx,
		ast.FTSMatchWord,
		types.NewFieldType(mysql.TypeDouble),
		expression.NewStrConst(queryText),
		matchColumn,
	)
	scalar, ok := rebuilt.(*expression.ScalarFunction)
	if !ok {
		return nil, false
	}
	expression.SetFTSMatchWordOptions(scalar, &expression.FTSMatchWordOptions{
		Fields: []expression.FTSMatchWordFieldOption{{
			ParserName: string(field.IndexFieldOption.AnalyzerType),
		}},
		Query: queryValue,
	})
	return scalar, true
}

func isStaticNonNullFTSConstant(constant *expression.Constant) bool {
	return constant != nil && constant.DeferredExpr == nil && constant.ParamMarker == nil && !constant.Value.IsNull()
}

func findFTSIndexField(
	table *model.TableInfo,
	index *model.IndexInfo,
	columnID int64,
	analyzer ast.FullTextAnalyzerType,
) (ast.FullTextIndexFieldInfo, bool) {
	if !ftsIndexContainsColumn(table, index, columnID) {
		return ast.FullTextIndexFieldInfo{}, false
	}
	for _, field := range index.FullTextInfo.IndexFieldsForColumn(columnID) {
		if field.IndexFieldOption.AnalyzerType == analyzer {
			return field, true
		}
	}
	return ast.FullTextIndexFieldInfo{}, false
}

func ftsIndexContainsColumn(table *model.TableInfo, index *model.IndexInfo, columnID int64) bool {
	if table == nil || index == nil {
		return false
	}
	for _, indexColumn := range index.Columns {
		if indexColumn == nil || indexColumn.Offset < 0 || indexColumn.Offset >= len(table.Columns) || table.Columns[indexColumn.Offset] == nil {
			continue
		}
		if table.Columns[indexColumn.Offset].ID == columnID {
			return true
		}
	}
	return false
}

func matchFTSLikeIndexField(
	column *expression.Column,
	pattern *expression.Constant,
	escape *expression.Constant,
	table *model.TableInfo,
	index *model.IndexInfo,
) (ast.FullTextIndexFieldInfo, string, bool) {
	var ok bool
	pattern, ok = materializeFTSConstant(pattern)
	if !ok {
		return ast.FullTextIndexFieldInfo{}, "", false
	}
	if !isStaticNonNullFTSConstant(pattern) || pattern.RetType == nil || !pattern.RetType.EvalType().IsStringKind() ||
		!isStaticNonNullFTSConstant(escape) || escape.RetType == nil || escape.RetType.EvalType() != types.ETInt {
		return ast.FullTextIndexFieldInfo{}, "", false
	}
	escapeValue, err := escape.Value.ToInt64(types.StrictContext)
	if err != nil || escapeValue < 0 || escapeValue > 255 {
		return ast.FullTextIndexFieldInfo{}, "", false
	}
	patternText := pattern.Value.GetString()
	escapeByte := byte(escapeValue)

	if literal, ok := fulltext.PathHierarchyLikeLiteral(patternText, escapeByte); ok {
		if field, found := findFTSIndexField(table, index, column.ID, ast.FullTextParserTypePathHierarchyV1); found {
			delimiter := ast.DefaultFullTextPathHierarchyDelimiter
			if option := field.IndexFieldOption.PathHierarchy; option != nil && option.Delimiter != "" {
				delimiter = option.Delimiter
			}
			if strings.HasSuffix(literal, delimiter) {
				return field, literal, true
			}
		}
	}
	return ast.FullTextIndexFieldInfo{}, "", false
}

// rebuildFTSLikePrefilter builds a potentially lossy but false-negative-free
// FTS condition for LIKE or ILIKE. It uses a complete ancestor for a
// path-hierarchy index, or all sufficiently long mandatory literals for an
// ngram index, and rejects case-insensitive rewrites unless the parser applies
// compatible lower-casing. The original LIKE condition must remain residual.
func rebuildFTSLikePrefilter(
	ctx expression.BuildContext,
	condition *expression.ScalarFunction,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, bool) {
	if condition == nil || len(condition.GetArgs()) != 3 {
		return nil, false
	}
	column, ok1 := condition.GetArgs()[0].(*expression.Column)
	pattern, ok2 := condition.GetArgs()[1].(*expression.Constant)
	escape, ok3 := condition.GetArgs()[2].(*expression.Constant)
	if !ok1 || !ok2 || !ok3 {
		return nil, false
	}
	patternText, escapeByte, ok := staticFTSLikePattern(pattern, escape)
	if !ok {
		return nil, false
	}
	likeCaseInsensitive := false
	if condition.FuncName.L == ast.Like {
		_, collation := condition.CharsetAndCollation()
		likeCaseInsensitive = collate.IsCICollation(collation)
	} else {
		// ILIKE lowercases ASCII before compiling its wildcard pattern. Mirror
		// that transformation so alphabetic escape characters keep their exact
		// ILIKE meaning before mandatory literals are extracted.
		patternBytes := []byte(patternText)
		if stringutil.IsUpperASCII(escapeByte) || stringutil.IsLowerASCII(escapeByte) {
			escapeByte = stringutil.LowerOneStringExcludeEscapeChar(patternBytes, escapeByte)
		} else {
			stringutil.LowerOneString(patternBytes)
		}
		patternText = string(patternBytes)
	}

	// PATH_HIERARCHY has no lower-case option, so it cannot safely prefilter a
	// case-insensitive LIKE condition.
	if condition.FuncName.L == ast.Like && !likeCaseInsensitive {
		if field, found := findFTSIndexField(table, index, column.ID, ast.FullTextParserTypePathHierarchyV1); found {
			delimiter := ast.DefaultFullTextPathHierarchyDelimiter
			if option := field.IndexFieldOption.PathHierarchy; option != nil && option.Delimiter != "" {
				delimiter = option.Delimiter
			}
			if literal, ok := fulltext.PathHierarchyLikePrefilterLiteral(patternText, escapeByte, delimiter); ok {
				return rebuildFTSIndexMatch(ctx, column, literal, literal, field)
			}
		}
	}

	field, found := findFTSIndexField(table, index, column.ID, ast.FullTextParserTypeNgramV1)
	if !found {
		return nil, false
	}
	option := field.IndexFieldOption.Ngram
	minGram := ast.DefaultFullTextNgramMinGram
	granularity := ast.DefaultFullTextNgramGranularity
	lowerCase := ast.DefaultFullTextNgramLowerCase
	if option != nil {
		minGram = option.MinGram
		granularity = option.Granularity
		lowerCase = option.LowerCase
	}
	if (condition.FuncName.L == ast.Ilike || likeCaseInsensitive) && !lowerCase {
		return nil, false
	}
	literals, ok := fulltext.LikeMandatoryLiterals(patternText, escapeByte)
	if !ok {
		return nil, false
	}
	candidates, ok := selectNgramPrefilterLiterals(literals, minGram, granularity)
	if !ok {
		return nil, false
	}
	if granularity == ast.FullTextNgramGranularityWord {
		query := strings.Join(candidates, " ")
		return rebuildFTSIndexMatch(ctx, column, query, query, field)
	}
	matches := make([]expression.Expression, 0, len(candidates))
	for _, candidate := range candidates {
		match, ok := rebuildFTSIndexMatch(ctx, column, candidate, candidate, field)
		if !ok {
			return nil, false
		}
		matches = append(matches, match)
	}
	return expression.ComposeCNFCondition(ctx, matches...), true
}

// staticFTSLikePattern accepts only non-NULL constants whose values are fixed
// during planning. It also restricts the escape expression to the one-byte
// range expected by TiDB's LIKE pattern compiler.
func staticFTSLikePattern(pattern, escape *expression.Constant) (string, byte, bool) {
	if !isStaticNonNullFTSConstant(pattern) || pattern.RetType == nil || !pattern.RetType.EvalType().IsStringKind() ||
		!isStaticNonNullFTSConstant(escape) || escape.RetType == nil || escape.RetType.EvalType() != types.ETInt {
		return "", 0, false
	}
	escapeValue, err := escape.Value.ToInt64(types.StrictContext)
	if err != nil || escapeValue < 0 || escapeValue > 255 {
		return "", 0, false
	}
	return pattern.Value.GetString(), byte(escapeValue), true
}

// selectNgramPrefilterLiterals converts mandatory LIKE literal runs into terms
// compatible with the index granularity. Character ngrams keep each run
// intact; word ngrams split runs into Unicode letter-or-number sequences. It
// drops terms shorter than minGram and duplicates because every returned term
// is required by the generated prefilter.
func selectNgramPrefilterLiterals(
	literals []string,
	minGram uint64,
	granularity ast.FullTextNgramGranularity,
) ([]string, bool) {
	if minGram == 0 {
		return nil, false
	}
	candidates := make([]string, 0, len(literals))
	seen := make(map[string]struct{}, len(literals))
	consider := func(candidate string) {
		if utf8.RuneCountInString(candidate) < int(minGram) {
			return
		}
		if _, exists := seen[candidate]; exists {
			return
		}
		seen[candidate] = struct{}{}
		candidates = append(candidates, candidate)
	}
	for _, literal := range literals {
		switch granularity {
		case ast.FullTextNgramGranularityChar:
			consider(literal)
		case ast.FullTextNgramGranularityWord:
			var word strings.Builder
			flushWord := func() {
				consider(word.String())
				word.Reset()
			}
			for _, r := range literal {
				if unicode.IsLetter(r) || unicode.IsNumber(r) {
					word.WriteRune(r)
					continue
				}
				flushWord()
			}
			flushWord()
		default:
			return nil, false
		}
	}
	return candidates, len(candidates) > 0
}

func rebuildFTSMatchWord(
	ctx expression.BuildContext,
	original *expression.ScalarFunction,
	table *model.TableInfo,
	index *model.IndexInfo,
) (expression.Expression, error) {
	originalArgs := original.GetArgs()
	info, err := analyzeFTSMatchWord(original)
	if err != nil {
		return nil, err
	}
	query := originalArgs[0].(*expression.Constant)
	for _, column := range info.Columns {
		if !ftsIndexContainsColumn(table, index, column.ID) {
			return nil, errors.Errorf("full-text search index %s does not cover column %s", index.Name, column.OrigName)
		}
	}
	clause, err := resolveFTSQueryClause(index, info)
	if err != nil {
		return nil, err
	}
	args := make([]expression.Expression, 0, len(originalArgs))
	args = append(args, query.Clone())
	options := &expression.FTSMatchWordOptions{
		Fields: make([]expression.FTSMatchWordFieldOption, 0, len(info.Columns)),
	}
	for i, column := range info.Columns {
		parserType := ast.GetFullTextParserTypeByName(clause.Fields[i].IndexField)
		if parserType == ast.FullTextParserTypeInvalid {
			return nil, errors.Errorf("unknown full-text parser type %q", clause.Fields[i].IndexField)
		}
		args = append(args, column.Clone())
		options.Fields = append(options.Fields, expression.FTSMatchWordFieldOption{
			ParserName: string(parserType),
		})
	}
	rebuilt, ok := rebuildFTSScalarFunction(ctx, original, args...)
	if !ok {
		return nil, errors.NewNoStackError("failed to rebuild FTS_MATCH_WORD query expression")
	}
	expression.SetFTSMatchWordOptions(rebuilt.(*expression.ScalarFunction), options)
	return rebuilt, nil
}
