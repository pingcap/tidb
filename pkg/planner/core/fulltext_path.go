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
	"slices"

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
// The path competes on cost with every other path. Reading the index costs
// the posting entries the search reads, since each posting list it opens is
// read in full: for every term, the rows containing it within the rows the
// key and handle conditions select, estimated with the ILIKE form of the term
// (see estimateFullTextPostingRows). Every other path evaluates the MATCH by
// analyzing each document it returns, which the Selection's cost charges by
// the document's size. A poorly filtering search can therefore lose to a scan,
// and a selective condition on another index can win over the search. USE
// INDEX and FORCE INDEX naming the index force the path; IGNORE INDEX forbids
// it.
func generateFullTextIndexPaths(ds *logicalop.DataSource) error {
	if ds.TableInfo == nil || ds.PreferStoreType&h.PreferTiFlash != 0 {
		return nil
	}
	sessVars := ds.SCtx().GetSessionVars()
	var paths []*util.AccessPath
	forced := false
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
				forced = forced || fullTextIndexNamedByHints(ds, idx)
			}
		}
	}
	if len(paths) == 0 {
		return nil
	}
	if !forced && pointPathSelectedByHeuristics(ds) {
		// A point lookup reads at most a row per point, which no posting scan
		// can beat; the heuristics that chose it pruned every other path, and
		// an ordinary IndexMerge path is not generated beside it either.
		return nil
	}
	if forced {
		// The hinted indexes compete among themselves. The other paths may be
		// there only because no hinted index offered an ordinary path, in
		// which case the hints were set aside; they are dropped unless a hint
		// names them.
		kept := ds.PossibleAccessPaths[:0]
		for _, path := range ds.PossibleAccessPaths {
			if accessPathNamedByHints(ds, path) {
				kept = append(kept, path)
			}
		}
		ds.PossibleAccessPaths = kept
	}
	ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, paths...)
	return nil
}

// fullTextIndexAllowedByHints reports whether the index hints on the data
// source let the index be used: it must be named by every USE INDEX or FORCE
// INDEX hint and by no IGNORE INDEX hint.
func fullTextIndexAllowedByHints(ds *logicalop.DataSource, idx *model.IndexInfo) bool {
	for _, hint := range scanIndexHints(ds) {
		named := indexHintNames(hint, idx)
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

// fullTextIndexNamedByHints reports whether a USE INDEX or FORCE INDEX hint
// on the data source names the index.
func fullTextIndexNamedByHints(ds *logicalop.DataSource, idx *model.IndexInfo) bool {
	for _, hint := range scanIndexHints(ds) {
		if (hint.HintType == ast.HintUse || hint.HintType == ast.HintForce) && indexHintNames(hint, idx) {
			return true
		}
	}
	return false
}

// accessPathNamedByHints reports whether a USE INDEX or FORCE INDEX hint on
// the data source names the index a path reads, or the primary key for a
// table path.
func accessPathNamedByHints(ds *logicalop.DataSource, path *util.AccessPath) bool {
	name := "primary"
	switch {
	case path.Index != nil && !path.IsTablePath():
		name = path.Index.Name.L
	case !path.IsTablePath():
		// An IndexMerge path names several indexes; none is hinted alone.
		return false
	}
	for _, hint := range scanIndexHints(ds) {
		if hint.HintType != ast.HintUse && hint.HintType != ast.HintForce {
			continue
		}
		for _, hinted := range hint.IndexNames {
			if hinted.L == name {
				return true
			}
		}
	}
	return false
}

func indexHintNames(hint *ast.IndexHint, idx *model.IndexInfo) bool {
	for _, name := range hint.IndexNames {
		if name.L == idx.Name.L {
			return true
		}
	}
	return false
}

// scanIndexHints returns the index hints for scanning that apply to the data
// source. Hints come in two forms: the SQL syntax on the table reference, and
// comment-style hints, which name the table they apply to.
func scanIndexHints(ds *logicalop.DataSource) []*ast.IndexHint {
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
	scan := hints[:0]
	for _, hint := range hints {
		if hint != nil && hint.HintScope == ast.HintForScan {
			scan = append(scan, hint)
		}
	}
	return scan
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
	keyConds := partial.AccessConds
	var handleConds []expression.Expression
	handleRanges := 0

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
			partial.AccessConds = append(slices.Clip(partial.AccessConds), res.AccessConds...)
			handleConds = res.AccessConds
			handleRanges = len(res.Ranges)
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
	partial.FullText.PostingRows, err = estimateFullTextPostingRows(ds, match, query, keyConds, handleConds)
	if err != nil {
		return nil, err
	}
	partial.FullText.PostingScans = countFullTextPostingScans(query, handleRanges)
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

// estimateFullTextPostingRows estimates the posting entries a search reads.
// Each posting list the search opens is read in full: an exact term's within
// the rows the key-column point and the handle ranges select, a prefix's
// within the key-column point alone, since handle ranges do not narrow it.
// The postings of a term are the rows containing it, which is what the ILIKE
// form of the term selects; its estimate comes from the column's statistics
// as for any string match, or is the default string-match selectivity when
// the statistics cannot evaluate it.
func estimateFullTextPostingRows(ds *logicalop.DataSource, match *expression.ScalarFunction, query *fulltext.Query,
	keyConds, handleConds []expression.Expression) (float64, error) {
	reads := query.PostingReads()
	if len(reads) == 0 {
		return 0, nil
	}
	selectivity := func(conds []expression.Expression) (float64, error) {
		if len(conds) == 0 {
			return 1, nil
		}
		sel, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, conds, nil)
		return sel, errors.Trace(err)
	}
	keySel, err := selectivity(keyConds)
	if err != nil {
		return 0, err
	}
	handleSel, err := selectivity(handleConds)
	if err != nil {
		return 0, err
	}
	column := match.GetArgs()[1]
	defaultSel := ds.SCtx().GetSessionVars().GetStrMatchDefaultSelectivity()
	rows := 0.0
	for _, read := range reads {
		// Without statistics every condition is given the generic
		// selectivity, which for a term would mean one in nearly every row;
		// the string-match default is the better guess.
		termSel := defaultSel
		if !ds.TableStats.HistColl.Pseudo {
			if like, err := expression.BuildFTSTermILikePredicate(ds.SCtx().GetExprCtx(), column, read.Term); err == nil {
				if termSel, err = selectivity([]expression.Expression{like}); err != nil {
					return 0, err
				}
			}
		}
		scope := ds.TableStats.RowCount * keySel
		if !read.Prefix {
			scope *= handleSel
		}
		rows += scope * termSel
	}
	return rows, nil
}

// countFullTextPostingScans counts the posting scans a search opens: an exact
// term is read within each handle range in turn, a prefix in one scan.
func countFullTextPostingScans(query *fulltext.Query, handleRanges int) float64 {
	scans := 0
	for _, read := range query.PostingReads() {
		if read.Prefix {
			scans++
		} else {
			scans += max(1, handleRanges)
		}
	}
	return float64(scans)
}

// pointPathSelectedByHeuristics reports whether derivePathStatsAndTryHeuristics
// chose a path that only reads points, or none at all, and pruned the others:
// an empty range, points of the handle or of a unique index, or a covering
// index preferred to such points.
func pointPathSelectedByHeuristics(ds *logicalop.DataSource) bool {
	tc := ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()
	for _, path := range ds.PossibleAccessPaths {
		if len(path.PartialIndexPaths) > 0 || path.StoreType == kv.TiFlash {
			continue
		}
		if len(path.Ranges) == 0 {
			return true
		}
		if !path.OnlyPointRange(tc) {
			continue
		}
		if path.IsTablePath() || path.Index.Unique || (len(ds.PossibleAccessPaths) == 1 && path.IsSingleScan) {
			return true
		}
	}
	return false
}

// isFullTextIndexPath reports whether path is the IndexMerge path that reads a
// FULLTEXT index built in TiKV.
func isFullTextIndexPath(path *util.AccessPath) bool {
	return len(path.PartialIndexPaths) == 1 && path.PartialIndexPaths[0].FullText != nil
}
