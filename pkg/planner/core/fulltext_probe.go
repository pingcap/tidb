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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

// A FULLTEXT index has no statistics of its own: its entries are terms, not
// column values, and the text column often has none either, since ANALYZE
// skips long text by default. The postings of an exact term are a contiguous
// key range, so the planner counts them instead, reading at most
// fullTextProbeLimit entries of each. A term with fewer postings is counted
// exactly, which is what matters most: whether a search is selective enough
// for the index, and how many rows it yields to the rest of the plan. A term
// with more is known to be common, and its estimate from the column's
// statistics, or the default, is kept, but never below the count read.
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
	// exact is unset when the count stopped at the probe limit in some
	// partition, or was scaled from a sample of the partitions; count is
	// then a lower bound.
	exact bool
}

// fullTextPostingCounts holds the terms one statement has probed.
type fullTextPostingCounts struct {
	mu     sync.Mutex
	counts map[string]fullTextPostingCount
	probed int
}

func fullTextPostingCountsOf(ds *logicalop.DataSource) *fullTextPostingCounts {
	return ds.SCtx().GetSessionVars().StmtCtx.GetOrStoreStmtCache(stmtctx.StmtFullTextPostingCountCacheKey,
		&fullTextPostingCounts{counts: make(map[string]fullTextPostingCount)}).(*fullTextPostingCounts)
}

// probeFullTextPostings counts the postings of term in idx under the encoded
// key-column values keyPrefix, across the physical tables ds reads. It
// returns false when the count is not known: the statement has probed its
// share of terms, or the read failed, which must never fail planning.
func probeFullTextPostings(ds *logicalop.DataSource, idx *model.IndexInfo, keyPrefix []byte, term string) (fullTextPostingCount, bool) {
	termValues, err := codec.EncodeKey(time.UTC, append([]byte(nil), keyPrefix...), types.NewBytesDatum([]byte(term)))
	if err != nil {
		return fullTextPostingCount{}, false
	}
	physicalIDs, scale := fullTextProbePhysicalIDs(ds)
	cacheKey := string(codec.EncodeInt(codec.EncodeInt(nil, ds.PhysicalTableID), idx.ID)) + string(termValues)
	counts := fullTextPostingCountsOf(ds)
	counts.mu.Lock()
	defer counts.mu.Unlock()
	if known, ok := counts.counts[cacheKey]; ok {
		return known, true
	}
	if counts.probed >= fullTextProbeTerms {
		return fullTextPostingCount{}, false
	}
	counts.probed++
	txn, err := ds.SCtx().Txn(true)
	if err != nil || !txn.Valid() {
		return fullTextPostingCount{}, false
	}
	// The snapshot holds what the statement reads, without the changes the
	// transaction buffers, which are too few to matter to an estimate.
	snapshot := txn.GetSnapshot()
	result := fullTextPostingCount{exact: scale == 1}
	for _, physicalID := range physicalIDs {
		start := tablecodec.EncodeIndexSeekKey(physicalID, idx.ID, termValues)
		n, saturated, err := countKeys(snapshot, start, start.PrefixNext(), fullTextProbeLimit)
		if err != nil {
			return fullTextPostingCount{}, false
		}
		result.count += float64(n)
		result.exact = result.exact && !saturated
	}
	result.count *= scale
	counts.counts[cacheKey] = result
	return result, true
}

// countKeys counts the keys in [start, end), stopping at limit.
func countKeys(snapshot kv.Snapshot, start, end kv.Key, limit int) (n int, saturated bool, err error) {
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
// search reads, within scope rows: a probed count when the term was counted
// exactly, otherwise the statistics-based estimate, raised to the count read
// when the probe stopped at its limit. Prefixes are never probed: their
// postings span every term under them.
func fullTextTermPostings(ds *logicalop.DataSource, idx *model.IndexInfo, keyPrefix []byte, read fulltext.PostingRead,
	probedScale float64, statsEstimate func() float64) float64 {
	// The statistics estimate evaluates the term against the column's whole
	// TopN and histogram, so it is computed only where it is used: an exact
	// count, the common case for a selective term, replaces it.
	if read.Prefix {
		return statsEstimate()
	}
	probed, ok := probeFullTextPostings(ds, idx, keyPrefix, read.Term)
	if !ok {
		return statsEstimate()
	}
	if probed.exact {
		return probed.count * probedScale
	}
	return max(statsEstimate(), probed.count*probedScale)
}

// estimateFullTextMatchSelectivities estimates, from the postings of their
// terms, the selectivity of every MATCH on ds that a FULLTEXT index built in
// TiKV without key columns can answer, and records it for the statistics of
// ds and of every path built over it. An index with key columns stores each
// term once per key value, so a term's postings across the table are not one
// range; its MATCH keeps the estimate from the column's statistics.
func estimateFullTextMatchSelectivities(ds *logicalop.DataSource) {
	if ds.TableInfo == nil || ds.TableStats == nil || ds.TableStats.RowCount <= 0 {
		return
	}
	// The probe reads TiKV, which a statement isolated from TiKV must not do
	// even to plan. A hint for another engine is only a preference of where
	// the rows are read, and the count still tells how many there are.
	if !fullTextIndexReadable(ds) {
		return
	}
	rows := ds.TableStats.RowCount
	for _, idx := range ds.TableInfo.Indices {
		textCol := idx.QueryableTiKVFullTextColumn()
		if textCol == nil || len(idx.Columns) != 1 {
			continue
		}
		colInfo := ds.TableInfo.Columns[textCol.Offset]
		config := fulltext.AnalyzerConfigFromTiKVFullTextIndex(idx.TiKVFullText)
		for _, cond := range ds.PushedDownConds {
			match, _, query, ok := fullTextMatchOnColumn(ds, cond, colInfo, config)
			if !ok || len(query.PostingReads()) == 0 || !query.RowsFromPostings() {
				continue
			}
			column := match.GetArgs()[1]
			cost := query.EstimatePostingCost(fulltext.PostingCostModel{
				Postings: func(read fulltext.PostingRead) float64 {
					return fullTextTermPostings(ds, idx, nil, read, 1, func() float64 {
						return rows * fullTextTermSelectivity(ds, column, read.Term)
					})
				},
				Rows:      rows,
				TermScans: 1,
			})
			cardinality.SetFullTextMatchSelectivity(ds.SCtx(), match, min(1, cost.Rows/rows))
		}
	}
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
