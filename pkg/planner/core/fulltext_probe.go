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
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

// A FULLTEXT index has no statistics of its own: its entries are terms, not
// column values, and the text column often has none either, since ANALYZE
// skips long text by default. The postings of an exact term are a contiguous
// key range, so the planner counts them instead, reading at most
// fullTextProbeLimit entries of each. A term with fewer postings is counted
// exactly, which is what matters most: whether a search is selective enough
// for the index, and how many rows it yields to the rest of the plan. A term
// with more is known to be common, and its estimate from the column's
// statistics, or the default, is kept, but never below the count read. A
// table of many partitions is counted in a few of them, and the count scaled.
const (
	// fullTextProbeLimit is the most postings counted for one term in one
	// physical table, about four kv_scan batches.
	fullTextProbeLimit = 1024
	// fullTextProbePartitions is the most partitions probed for one term;
	// the counts of a partitioned table are scaled from those probed.
	fullTextProbePartitions = 4
	// fullTextProbeTerms is the most terms probed in planning one statement.
	fullTextProbeTerms = 32
)

// fullTextPostingCount is the number of postings of a term, as probed.
type fullTextPostingCount struct {
	count float64
	// lowerBound is set when the count stopped at the probe limit in some
	// partition: the term has at least count postings. Otherwise count is
	// the number of postings, or its estimate from the partitions probed.
	lowerBound bool
}

// fullTextTermProbe sums what the partitions of a statically pruned table
// have counted for one term. Each partition is planned as its own data
// source; the first fullTextProbePartitions of them count the term, and the
// others take their mean.
type fullTextTermProbe struct {
	partitions int
	sum        float64
	lowerBound bool
}

// fullTextPostingCounts holds the terms one statement has probed.
type fullTextPostingCounts struct {
	mu sync.Mutex
	// counts is keyed by the physical table of the data source, the index
	// and the encoded key-column values and term.
	counts map[string]fullTextPostingCount
	// terms is keyed likewise, by the table in place of the partition.
	terms map[string]*fullTextTermProbe
	// probed is the number of distinct terms probed.
	probed int
	// snapshot reads at the statement's read timestamp, keys only.
	snapshot kv.Snapshot
	// stopped is set once a read failed or the statement was killed: the
	// statement probes no further.
	stopped bool
}

func fullTextPostingCountsOf(ds *logicalop.DataSource) *fullTextPostingCounts {
	return ds.SCtx().GetSessionVars().StmtCtx.GetOrStoreStmtCache(stmtctx.StmtFullTextPostingCountCacheKey,
		&fullTextPostingCounts{
			counts: make(map[string]fullTextPostingCount),
			terms:  make(map[string]*fullTextTermProbe),
		}).(*fullTextPostingCounts)
}

// probeSnapshot returns the snapshot the statement's probes read. It is the
// probes' own, at the timestamp the statement reads at, so that it can be
// read keys only: counting needs no posting's positions, and the option must
// not reach the transaction's snapshot, which the statement's readers share.
func (c *fullTextPostingCounts) probeSnapshot(pctx base.PlanContext) (kv.Snapshot, bool) {
	if c.snapshot != nil {
		return c.snapshot, true
	}
	sctx, err := AsSctx(pctx)
	if err != nil || sctx.GetStore() == nil || sessiontxn.GetTxnManager == nil {
		return nil, false
	}
	txnManager := sessiontxn.GetTxnManager(sctx)
	if txnManager == nil || txnManager.GetContextProvider() == nil {
		return nil, false
	}
	readTS, err := txnManager.GetStmtReadTS()
	if err != nil {
		return nil, false
	}
	sessVars := sctx.GetSessionVars()
	snapshot := sctx.GetStore().GetSnapshot(kv.Version{Ver: readTS})
	snapshot.SetOption(kv.KeyOnly, true)
	snapshot.SetOption(kv.TaskID, sessVars.StmtCtx.TaskID)
	snapshot.SetOption(kv.TiKVClientReadTimeout, sessVars.GetTiKVClientReadTimeout())
	snapshot.SetOption(kv.ResourceGroupName, sessVars.StmtCtx.ResourceGroupName)
	snapshot.SetOption(kv.ExplicitRequestSourceType, sessVars.ExplicitRequestSourceType)
	c.snapshot = snapshot
	return snapshot, true
}

// probeFullTextPostings counts the postings of term in idx under the encoded
// key-column values keyPrefix, across the physical tables ds reads. It
// returns false when the count is not known, and the estimate from the
// statistics stands:
//   - the rows of a temporary table, and those the transaction has written,
//     are not in TiKV, so a count there would miss them;
//   - the statement has probed its share of terms;
//   - a read failed, which must never fail planning, or the statement was
//     killed.
func probeFullTextPostings(ds *logicalop.DataSource, idx *model.IndexInfo, keyPrefix []byte, term string) (fullTextPostingCount, bool) {
	if ds.TableInfo.TempTableType != model.TempTableNone || tableHasDirtyContent(ds.SCtx(), ds.TableInfo) {
		return fullTextPostingCount{}, false
	}
	termValues, err := codec.EncodeKey(time.UTC, append([]byte(nil), keyPrefix...), types.NewBytesDatum([]byte(term)))
	if err != nil {
		return fullTextPostingCount{}, false
	}
	indexKey := string(codec.EncodeInt(nil, idx.ID)) + string(termValues)
	cacheKey := string(codec.EncodeInt(nil, ds.PhysicalTableID)) + indexKey
	counts := fullTextPostingCountsOf(ds)
	counts.mu.Lock()
	defer counts.mu.Unlock()
	if known, ok := counts.counts[cacheKey]; ok {
		return known, true
	}
	if counts.stopped {
		return fullTextPostingCount{}, false
	}
	// A partition planned by itself shares the term's probe with the other
	// partitions of its table.
	var shared *fullTextTermProbe
	if ds.PhysicalTableID != ds.TableInfo.ID {
		termKey := string(codec.EncodeInt(nil, ds.TableInfo.ID)) + indexKey
		shared = counts.terms[termKey]
		if shared == nil {
			if counts.probed >= fullTextProbeTerms {
				return fullTextPostingCount{}, false
			}
			counts.probed++
			shared = &fullTextTermProbe{}
			counts.terms[termKey] = shared
		}
		if shared.partitions >= fullTextProbePartitions {
			result := fullTextPostingCount{count: shared.sum / float64(shared.partitions), lowerBound: shared.lowerBound}
			counts.counts[cacheKey] = result
			return result, true
		}
	} else {
		if counts.probed >= fullTextProbeTerms {
			return fullTextPostingCount{}, false
		}
		counts.probed++
	}
	snapshot, ok := counts.probeSnapshot(ds.SCtx())
	if !ok {
		counts.stopped = true
		return fullTextPostingCount{}, false
	}
	killer := &ds.SCtx().GetSessionVars().SQLKiller
	physicalIDs, scale := fullTextProbePhysicalIDs(ds)
	var result fullTextPostingCount
	for _, physicalID := range physicalIDs {
		start := tablecodec.EncodeIndexSeekKey(physicalID, idx.ID, termValues)
		n, saturated, err := countKeys(snapshot, start, start.PrefixNext(), fullTextProbeLimit, killer)
		if err != nil {
			counts.stopped = true
			return fullTextPostingCount{}, false
		}
		result.count += float64(n)
		result.lowerBound = result.lowerBound || saturated
	}
	result.count *= scale
	if shared != nil {
		shared.partitions++
		shared.sum += result.count
		shared.lowerBound = shared.lowerBound || result.lowerBound
	}
	counts.counts[cacheKey] = result
	return result, true
}

// countKeys counts the keys in [start, end), stopping at limit, or with the
// killer's error once the statement is killed.
func countKeys(snapshot kv.Snapshot, start, end kv.Key, limit int, killer *sqlkiller.SQLKiller) (n int, saturated bool, err error) {
	if err := killer.HandleSignal(); err != nil {
		return 0, false, err
	}
	iter, err := snapshot.Iter(start, end)
	if err != nil {
		return 0, false, err
	}
	defer iter.Close()
	for iter.Valid() {
		if n == limit {
			return n, true, nil
		}
		n++
		if err := killer.HandleSignal(); err != nil {
			return 0, false, err
		}
		if err := iter.Next(); err != nil {
			return 0, false, err
		}
	}
	return n, false, nil
}

// fullTextProbePhysicalIDs returns the physical tables a probe reads, and the
// factor that scales their counts to every physical table ds reads: at most
// fullTextProbePartitions partitions, spread over the table's partitions.
func fullTextProbePhysicalIDs(ds *logicalop.DataSource) ([]int64, float64) {
	pi := ds.TableInfo.GetPartitionInfo()
	if pi == nil || ds.PhysicalTableID != ds.TableInfo.ID || len(pi.Definitions) == 0 {
		return []int64{ds.PhysicalTableID}, 1
	}
	defs := pi.Definitions
	if len(defs) <= fullTextProbePartitions {
		ids := make([]int64, 0, len(defs))
		for _, def := range defs {
			ids = append(ids, def.ID)
		}
		return ids, 1
	}
	ids := make([]int64, 0, fullTextProbePartitions)
	for i := range fullTextProbePartitions {
		ids = append(ids, defs[i*len(defs)/fullTextProbePartitions].ID)
	}
	return ids, float64(len(defs)) / float64(fullTextProbePartitions)
}

// fullTextTermPostings returns the estimate of the postings of one list a
// search reads, within scope rows: the probed count, unless the probe stopped
// at its limit, when it is the statistics-based estimate raised to the count
// read; and the statistics-based estimate when the term was not probed. Prefixes are never probed: their
// postings span every term under them.
func fullTextTermPostings(ds *logicalop.DataSource, idx *model.IndexInfo, keyPrefix []byte, read fulltext.PostingRead,
	probedScale float64, statsEstimate func() float64) float64 {
	// The statistics estimate evaluates the term against the column's whole
	// TopN and histogram, so it is computed only where it is used: a
	// count, the common case for a selective term, replaces it.
	if read.Prefix {
		return statsEstimate()
	}
	probed, ok := probeFullTextPostings(ds, idx, keyPrefix, read.Term)
	if !ok {
		return statsEstimate()
	}
	if probed.lowerBound {
		return max(statsEstimate(), probed.count*probedScale)
	}
	return probed.count * probedScale
}

// estimateFullTextMatchSelectivities estimates, from the postings of their
// terms, the selectivity of every MATCH on ds that a FULLTEXT index built in
// TiKV can answer, and records it for the statistics of ds and of every path
// built over it.
//
// An index without key columns holds a term's postings across the table in
// one range, so the MATCH's selectivity is its share of the table's rows. An
// index with key columns holds them once per key value: when the other
// conditions pin the key columns to a point, the postings under the point
// are counted, and the selectivity is the MATCH's share of the rows the
// statistics give the point, which the conditions on the key columns then
// scale back to the table. Without such a point the MATCH keeps the estimate
// from the column's statistics. An index without key columns is preferred:
// its count needs no estimate of the point.
func estimateFullTextMatchSelectivities(ds *logicalop.DataSource) {
	if ds.TableInfo == nil || ds.TableStats == nil || ds.TableStats.RowCount <= 0 {
		return
	}
	// The probe reads TiKV, which a statement kept from TiKV, by the
	// isolation read engines or a TiFlash hint, must not do even to plan.
	if !fullTextIndexReadable(ds) {
		return
	}
	for _, keyed := range []bool{false, true} {
		for _, idx := range ds.TableInfo.Indices {
			textCol := idx.QueryableTiKVFullTextColumn()
			if textCol == nil || (len(idx.Columns) > 1) != keyed {
				continue
			}
			colInfo := ds.TableInfo.Columns[textCol.Offset]
			config := fulltext.AnalyzerConfigFromTiKVFullTextIndex(idx.TiKVFullText)
			for _, cond := range ds.PushedDownConds {
				match, _, query, ok := fullTextMatchOnColumn(ds, cond, colInfo, config)
				if !ok || len(query.PostingReads()) == 0 || !query.RowsFromPostings() {
					continue
				}
				if keyed && cardinality.HasFullTextMatchSelectivity(ds.SCtx(), match) {
					continue
				}
				estimateFullTextMatchSelectivity(ds, idx, match, query)
			}
		}
	}
}

func estimateFullTextMatchSelectivity(ds *logicalop.DataSource, idx *model.IndexInfo, match *expression.ScalarFunction, query *fulltext.Query) {
	rows := ds.TableStats.RowCount
	var keyPrefix []byte
	if keyColumnCount := len(idx.Columns) - 1; keyColumnCount > 0 {
		idxCols, idxColLens, _, _ := util.IndexInfo2Cols(ds.Columns, ds.Schema().Columns, idx)
		if len(idxCols) < keyColumnCount {
			return
		}
		conds := make([]expression.Expression, 0, len(ds.PushedDownConds))
		for _, cond := range ds.PushedDownConds {
			if cond != expression.Expression(match) {
				conds = append(conds, cond)
			}
		}
		res, err := fullTextKeyPoint(ds, idxCols[:keyColumnCount], idxColLens[:keyColumnCount], conds)
		if err != nil || res == nil {
			return
		}
		keySel, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, res.AccessConds, nil)
		if err != nil {
			return
		}
		keyPrefix, err = codec.EncodeKey(ds.SCtx().GetSessionVars().StmtCtx.TimeZone(), nil, res.Ranges[0].LowVal[:keyColumnCount]...)
		if err != nil {
			return
		}
		rows *= keySel
		if rows <= 0 {
			return
		}
	}
	column := match.GetArgs()[1]
	// The selectivity is recorded only when every term was counted: it then
	// rests on the postings, as the statistics' estimate of the MATCH would
	// not.
	counted := true
	cost := query.EstimatePostingCost(fulltext.PostingCostModel{
		Postings: func(read fulltext.PostingRead) float64 {
			if !read.Prefix {
				if _, ok := probeFullTextPostings(ds, idx, keyPrefix, read.Term); !ok {
					counted = false
				}
			}
			return fullTextTermPostings(ds, idx, keyPrefix, read, 1, func() float64 {
				return rows * fullTextTermSelectivity(ds, column, read.Term)
			})
		},
		Rows:      rows,
		TermScans: 1,
	})
	if !counted {
		return
	}
	cardinality.SetFullTextMatchSelectivity(ds.SCtx(), match, min(1, cost.Rows/rows))
}

// fullTextTermSelectivity estimates the share of rows containing a term from
// the column's statistics, by the term's ILIKE form; without statistics, the
// string-match default.
func fullTextTermSelectivity(ds *logicalop.DataSource, column expression.Expression, term string) float64 {
	sel := ds.SCtx().GetSessionVars().GetStrMatchDefaultSelectivity()
	if ds.TableStats.HistColl.Pseudo {
		return sel
	}
	like, err := expression.BuildFTSTermILikePredicate(ds.SCtx().GetExprCtx(), column, term)
	if err != nil {
		return sel
	}
	if s, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, []expression.Expression{like}, nil); err == nil {
		return s
	}
	return sel
}
