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

package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// generateFullTextIndexPaths offers an access path for each MATCH ... AGAINST
// conjunct that a FULLTEXT index built in TiKV can answer. The path is an
// IndexMerge path with one partial path: the index is read by TiDB's
// posting-list engine, which yields the handles of the matching rows, and the
// ordinary IndexMerge table lookup turns them into rows. The engine evaluates
// the whole boolean query, positions included, so the rows it yields are
// exactly the rows the MATCH accepts and the MATCH is consumed by the path
// like an access condition rather than re-evaluated on every row returned,
// which for a long document would tokenize it a second time. Rows changed by
// the current transaction are not read through the index at all; UnionScan
// re-evaluates the MATCH on them itself.
//
// Only a MATCH that is a top-level conjunct qualifies. Under a negation, or in
// one branch of an OR, the rows the index selects are exactly the ones that
// must not be dropped. The search string must be a constant the plan can bake
// in: a parameter would compile to a different set of terms per execution.
//
// When such a path exists it replaces every other path, as the columnar
// full-text path does. The alternative is to tokenize every document of the
// table to evaluate the MATCH, which the cost model does not see: it charges a
// filter one constant per row, and the index has no statistics from which to
// tell a rare term from a common one. Several MATCH conjuncts still compete on
// cost. USE INDEX and IGNORE INDEX hints are honoured, so a scan can be forced.
func generateFullTextIndexPaths(ds *logicalop.DataSource) error {
	if ds.TableInfo == nil || ds.PreferStoreType&h.PreferTiFlash != 0 {
		return nil
	}
	sessVars := ds.SCtx().GetSessionVars()
	var paths []*util.AccessPath
	for _, idx := range ds.TableInfo.Indices {
		if !idx.IsTiKVFullTextIndex() || idx.State != model.StatePublic || len(idx.Columns) != 1 {
			continue
		}
		if idx.Invisible && !sessVars.OptimizerUseInvisibleIndexes {
			continue
		}
		if !fullTextIndexAllowedByHints(ds, idx) {
			continue
		}
		colInfo := ds.TableInfo.Columns[idx.Columns[0].Offset]
		config := fulltext.AnalyzerConfigFromTiKVFullTextIndex(idx.TiKVFullText)
		for _, cond := range ds.AllConds {
			match, search, ok := fullTextMatchOnColumn(ds, cond, colInfo, config)
			if !ok {
				continue
			}
			path, err := buildFullTextIndexPath(ds, idx, match, search)
			if err != nil {
				return err
			}
			paths = append(paths, path)
		}
	}
	if len(paths) > 0 {
		ds.PossibleAccessPaths = paths
	}
	return nil
}

// fullTextIndexAllowedByHints reports whether the index hints on the data
// source let the index be used: it must be named by every USE INDEX or FORCE
// INDEX hint and by no IGNORE INDEX hint.
func fullTextIndexAllowedByHints(ds *logicalop.DataSource, idx *model.IndexInfo) bool {
	// Hints come in two forms: the SQL syntax on the table reference, and
	// comment-style hints, which name the table they apply to.
	hints := make([]*ast.IndexHint, 0, len(ds.AstIndexHints)+len(ds.IndexHints))
	hints = append(hints, ds.AstIndexHints...)
	tblName := ds.TableInfo.Name
	if ds.TableAsName != nil && ds.TableAsName.L != "" {
		tblName = *ds.TableAsName
	}
	for _, hint := range ds.IndexHints {
		if hint.Match(ds.DBName, tblName) {
			hints = append(hints, hint.IndexHint)
		}
	}
	for _, hint := range hints {
		if hint == nil || hint.HintScope != ast.HintForScan {
			continue
		}
		named := false
		for _, name := range hint.IndexNames {
			if name.L == idx.Name.L {
				named = true
				break
			}
		}
		switch hint.HintType {
		case ast.HintIgnore:
			if named {
				return false
			}
		case ast.HintUse, ast.HintForce:
			if !named {
				return false
			}
		}
	}
	return true
}

// fullTextMatchOnColumn reports whether cond is a locally evaluated
// MATCH(col) AGAINST('constant' IN BOOLEAN MODE) over the given column whose
// analyzer is the index's, and returns the search string.
func fullTextMatchOnColumn(ds *logicalop.DataSource, cond expression.Expression, colInfo *model.ColumnInfo, config fulltext.AnalyzerConfig) (*expression.ScalarFunction, string, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.FTSMysqlMatchAgainst {
		return nil, "", false
	}
	args := sf.GetArgs()
	if len(args) != 2 {
		return nil, "", false
	}
	col, ok := args[1].(*expression.Column)
	if !ok || col.ID != colInfo.ID {
		return nil, "", false
	}
	// Local evaluation is what makes the MATCH a boolean predicate this index
	// can serve; it also carries the analyzer the predicate compiled with,
	// which must be the index's own, or the terms looked up would not be the
	// terms stored.
	info, ok := expression.FTSMysqlMatchAgainstLocalEvalInfo(sf)
	if !ok || !info.AnalyzerConfig.Equal(config) {
		return nil, "", false
	}
	constant, ok := args[0].(*expression.Constant)
	if !ok || expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), constant) {
		return nil, "", false
	}
	value, err := constant.Eval(ds.SCtx().GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil || value.IsNull() || value.Kind() != types.KindString {
		return nil, "", false
	}
	search := value.GetString()
	if _, err := fulltext.CompileBooleanQuery(search, config); err != nil {
		return nil, "", false
	}
	return sf, search, true
}

func buildFullTextIndexPath(ds *logicalop.DataSource, idx *model.IndexInfo, match *expression.ScalarFunction, search string) (*util.AccessPath, error) {
	// The MATCH's own selectivity estimate is the best available guess for
	// how many rows the index will yield; the index has no statistics of its
	// own, since its entries are terms rather than column values.
	selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, []expression.Expression{match}, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	count := ds.TableStats.RowCount * selectivity
	partial := &util.AccessPath{
		Index:            idx,
		FullText:         &util.FullTextAccessInfo{Match: match, Search: search},
		CountAfterAccess: count,
		CountAfterIndex:  count,
		StoreType:        kv.TiKV,
	}
	partial.IdxCols, partial.IdxColLens, partial.FullIdxCols, partial.FullIdxColLens =
		util.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, idx)
	tableFilters := make([]expression.Expression, 0, len(ds.AllConds))
	for _, cond := range ds.AllConds {
		if cond != expression.Expression(match) {
			tableFilters = append(tableFilters, cond)
		}
	}
	return &util.AccessPath{
		PartialIndexPaths: []*util.AccessPath{partial},
		TableFilters:      tableFilters,
		CountAfterAccess:  count,
		StoreType:         kv.TiKV,
	}, nil
}
