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
	"github.com/pingcap/tidb/pkg/util/collate"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/ranger"
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
// An index with key columns ahead of the tokenized one holds each row's
// entries under the row's key-column values, so it answers a search only
// within one value of every key column: the conditions must pin each of them,
// as one equality per column, which ranger turns into a single point range.
// The equalities are then served by the index, like access conditions of an
// ordinary index scan, and dropped from the table filters. Without such a
// point the index cannot serve the query and the scan stays.
//
// Every entry of the index ends with the row's clustered handle, as the entry
// of any non-unique index does, so a term's postings are laid out in handle
// order and conditions on a leading prefix of the handle columns select a
// contiguous slice of each term's postings. The same conditions turn into
// ranges of an ordinary index scan, and here they narrow every exact term's
// posting scan to the matching handles: a tenant equality on a table whose
// primary key leads with the tenant confines a search on a FULLTEXT index
// without key columns to that tenant. A prefix search, standard `foo*` or an
// NGRAM fragment shorter than the gram size, reads every term with the prefix
// and is not narrowed, so with such a search the conditions also stay on the
// table side.
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
		textCol := idx.TiKVFullTextColumn()
		if textCol == nil || idx.State != model.StatePublic {
			continue
		}
		if idx.Invisible && !sessVars.OptimizerUseInvisibleIndexes {
			continue
		}
		if !fullTextIndexAllowedByHints(ds, idx) {
			continue
		}
		colInfo := ds.TableInfo.Columns[textCol.Offset]
		config := fulltext.AnalyzerConfigFromTiKVFullTextIndex(idx.TiKVFullText)
		for _, cond := range ds.AllConds {
			match, search, query, ok := fullTextMatchOnColumn(ds, cond, colInfo, config)
			if !ok {
				continue
			}
			path, err := buildFullTextIndexPath(ds, idx, match, search, query)
			if err != nil {
				return err
			}
			if path != nil {
				paths = append(paths, path)
			}
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
// analyzer is the index's, and returns the search string with the query it
// compiles to.
func fullTextMatchOnColumn(ds *logicalop.DataSource, cond expression.Expression, colInfo *model.ColumnInfo, config fulltext.AnalyzerConfig) (*expression.ScalarFunction, string, *fulltext.Query, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.FTSMysqlMatchAgainst {
		return nil, "", nil, false
	}
	args := sf.GetArgs()
	if len(args) != 2 {
		return nil, "", nil, false
	}
	col, ok := args[1].(*expression.Column)
	if !ok || col.ID != colInfo.ID {
		return nil, "", nil, false
	}
	// Local evaluation is what makes the MATCH a boolean predicate this index
	// can serve; it also carries the analyzer the predicate compiled with,
	// which must be the index's own, or the terms looked up would not be the
	// terms stored.
	info, ok := expression.FTSMysqlMatchAgainstLocalEvalInfo(sf)
	if !ok || !info.AnalyzerConfig.Equal(config) {
		return nil, "", nil, false
	}
	constant, ok := args[0].(*expression.Constant)
	if !ok || expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), constant) {
		return nil, "", nil, false
	}
	value, err := constant.Eval(ds.SCtx().GetExprCtx().GetEvalCtx(), chunk.Row{})
	if err != nil || value.IsNull() || value.Kind() != types.KindString {
		return nil, "", nil, false
	}
	search := value.GetString()
	query, err := fulltext.CompileBooleanQuery(search, config)
	if err != nil {
		return nil, "", nil, false
	}
	return sf, search, query, true
}

// buildFullTextIndexPath builds the IndexMerge path that answers match with
// idx, or returns nil when the index has key columns the conditions do not
// pin to one value. The path's ranges have the key columns as their leading
// dimensions and, when conditions on the clustered handle narrow the posting
// scans, the handle columns as the trailing ones; the term sits between them
// in the key and is implied.
func buildFullTextIndexPath(ds *logicalop.DataSource, idx *model.IndexInfo, match *expression.ScalarFunction, search string, query *fulltext.Query) (*util.AccessPath, error) {
	partial := &util.AccessPath{
		Index:     idx,
		FullText:  &util.FullTextAccessInfo{Match: match, Search: search},
		StoreType: kv.TiKV,
	}
	partial.IdxCols, partial.IdxColLens, partial.FullIdxCols, partial.FullIdxColLens =
		util.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, idx)
	tableFilters := make([]expression.Expression, 0, len(ds.AllConds))
	for _, cond := range ds.AllConds {
		if cond != expression.Expression(match) {
			tableFilters = append(tableFilters, cond)
		}
	}
	keyColumnCount := len(idx.Columns) - 1
	if keyColumnCount > 0 {
		if len(partial.IdxCols) < keyColumnCount {
			return nil, nil
		}
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.SCtx().GetRangerCtx(), tableFilters,
			partial.IdxCols[:keyColumnCount], partial.IdxColLens[:keyColumnCount], 0)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tc := ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()
		if len(res.Ranges) != 1 || len(res.Ranges[0].LowVal) != keyColumnCount || !res.Ranges[0].IsPointNullable(tc) {
			return nil, nil
		}
		partial.Ranges = res.Ranges
		partial.AccessConds = res.AccessConds
		partial.EqCondCount = res.EqCondCount
		partial.EqOrInCondCount = res.EqOrInCount
		partial.IsDNFCond = res.IsDNFCond
		tableFilters = res.RemainedConds
	}

	// Conditions on a leading prefix of the clustered handle columns, which
	// end every entry of the index, narrow each exact term's posting scan to
	// the handles they select. The columns and the cases in which the key
	// carries them are those of an ordinary non-unique index. A prefix search
	// cannot be narrowed, so its conditions remain table filters as well.
	if handleCols, handleLens := ds.HandleColsToAppend(partial, partial.IdxCols); len(handleCols) > 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.SCtx().GetRangerCtx(), tableFilters,
			handleCols, handleLens, ds.SCtx().GetSessionVars().RangeMaxSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(res.AccessConds) > 0 && len(res.Ranges) > 0 {
			partial.Ranges = fullTextHandleRanges(partial.Ranges, res.Ranges)
			partial.AccessConds = append(partial.AccessConds, res.AccessConds...)
			if !query.UsesPrefixPostings() {
				tableFilters = res.RemainedConds
			}
		}
	}

	// The MATCH's own selectivity estimate, with that of the conditions on
	// the key and handle columns, is the best available guess for how many
	// rows the index will yield; the index has no statistics of its own,
	// since its entries are terms rather than column values.
	accessConds := make([]expression.Expression, 0, 1+len(partial.AccessConds))
	accessConds = append(accessConds, match)
	accessConds = append(accessConds, partial.AccessConds...)
	selectivity, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, accessConds, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	count := ds.TableStats.RowCount * selectivity
	partial.CountAfterAccess = count
	partial.CountAfterIndex = count
	return &util.AccessPath{
		PartialIndexPaths: []*util.AccessPath{partial},
		TableFilters:      tableFilters,
		CountAfterAccess:  count,
		StoreType:         kv.TiKV,
	}, nil
}

// fullTextHandleRanges appends each range of the handle columns to the point
// the key columns are pinned to, which is keyRanges' single range, or returns
// the handle ranges themselves for an index without key columns. The handle
// ranges are sorted and disjoint, as ranger builds them, so the result is too.
func fullTextHandleRanges(keyRanges, handleRanges ranger.Ranges) ranger.Ranges {
	if len(keyRanges) == 0 {
		return handleRanges
	}
	point := keyRanges[0]
	ranges := make(ranger.Ranges, 0, len(handleRanges))
	for _, hr := range handleRanges {
		ran := &ranger.Range{
			LowVal:      make([]types.Datum, 0, len(point.LowVal)+len(hr.LowVal)),
			HighVal:     make([]types.Datum, 0, len(point.HighVal)+len(hr.HighVal)),
			Collators:   make([]collate.Collator, 0, len(point.Collators)+len(hr.Collators)),
			LowExclude:  hr.LowExclude,
			HighExclude: hr.HighExclude,
		}
		ran.LowVal = append(append(ran.LowVal, point.LowVal...), hr.LowVal...)
		ran.HighVal = append(append(ran.HighVal, point.HighVal...), hr.HighVal...)
		ran.Collators = append(append(ran.Collators, point.Collators...), hr.Collators...)
		ranges = append(ranges, ran)
	}
	return ranges
}
