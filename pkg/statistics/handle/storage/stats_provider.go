// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

const (
	statsQueryIDChunkSize             = 512
	defaultMaxPartitionStatsBatchLoad = 128
)

// OnDemandStatsOption configures an OnDemandStatsProvider.
type OnDemandStatsOption func(*OnDemandStatsProvider)

// WithSkipColumnDistributions skips loading per-column Histogram buckets, TopN,
// and CMSketch while still loading column metadata (NDV, null count, total
// column size, correlation, stats version) and full index statistics.
// Use this for InRestrictedSQL callers where ColumnStatsIsInvalid rejects
// per-column distributions during selectivity estimation.
func WithSkipColumnDistributions() OnDemandStatsOption {
	return func(p *OnDemandStatsProvider) {
		p.skipColumnDistributions = true
	}
}

// WithMaxPartitionBatchLoad sets the maximum number of analyzed partitions
// whose full distributions are loaded synchronously per partitioned table.
// Additional partitions beyond this budget fall back to pseudo statistics.
func WithMaxPartitionBatchLoad(limit int) OnDemandStatsOption {
	return func(p *OnDemandStatsProvider) {
		if limit > 0 {
			p.maxPartitionLoads = limit
		}
	}
}

// OnDemandStatsProvider loads physical table statistics on demand from
// mysql.stats_* system tables and memoizes them for the lifetime of a single
// SQL compilation or single-shot query execution.
//
// Missing statistics and storage load errors are permanently memoized as
// PseudoTable entries without invalidation or retry, so do not reuse one
// provider across long-lived or multi-query background loops that expect to
// observe newly analyzed statistics or recover from transient storage errors.
type OnDemandStatsProvider struct {
	ctx                     context.Context
	sessPool                util.DestroyableSessionPool
	skipColumnDistributions bool
	maxPartitionLoads       int

	mu    sync.Mutex
	cache map[int64]*statistics.Table
}

// NewOnDemandStatsProvider creates a scoped on-demand stats provider backed by
// sessPool and bound to ctx for cancellation propagation.
func NewOnDemandStatsProvider(
	ctx context.Context,
	sessPool util.DestroyableSessionPool,
	opts ...OnDemandStatsOption,
) *OnDemandStatsProvider {
	if ctx == nil {
		ctx = context.Background()
	}
	p := &OnDemandStatsProvider{
		ctx:               ctx,
		sessPool:          sessPool,
		maxPartitionLoads: defaultMaxPartitionStatsBatchLoad,
		cache:             make(map[int64]*statistics.Table),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// GetPhysicalTableStats returns cached statistics for physicalTableID, loading
// them on demand from mysql.stats_* on a miss and falling back to pseudo stats
// when unavailable, canceled, or on error. Missing and failed loads (except
// caller context cancellation) are memoized for the provider's lifetime.
func (p *OnDemandStatsProvider) GetPhysicalTableStats(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	// Never query mysql.stats_* for reserved/system tables to avoid recursive
	// stats lookups when loading mysql.stats_* itself.
	if metadef.IsReservedID(physicalTableID) {
		return newPseudoPhysicalTable(physicalTableID, tblInfo)
	}
	if p.ctx.Err() != nil {
		return newPseudoPhysicalTable(physicalTableID, tblInfo)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if tbl, ok := p.cache[physicalTableID]; ok {
		return tbl
	}
	if p.ctx.Err() != nil {
		return newPseudoPhysicalTable(physicalTableID, tblInfo)
	}

	targetIDs := p.collectUncachedTargetIDsLocked(physicalTableID, tblInfo)
	loaded, err := p.loadTablesFromStorage(physicalTableID, targetIDs, tblInfo)
	if err != nil {
		if p.ctx.Err() != nil {
			return newPseudoPhysicalTable(physicalTableID, tblInfo)
		}
		logutil.BgLogger().Warn("failed to load table stats on demand, fallback to pseudo",
			zap.Int64("physicalTableID", physicalTableID),
			zap.String("table", tblInfo.Name.O),
			zap.Error(err))
		for _, id := range targetIDs {
			p.cache[id] = newPseudoPhysicalTable(id, tblInfo)
		}
		return p.cache[physicalTableID]
	}

	for _, id := range targetIDs {
		if tbl := loaded[id]; tbl != nil {
			p.cache[id] = tbl
		} else {
			p.cache[id] = newPseudoPhysicalTable(id, tblInfo)
		}
	}
	return p.cache[physicalTableID]
}

func newPseudoPhysicalTable(physicalTableID int64, tblInfo *model.TableInfo) *statistics.Table {
	tbl := statistics.PseudoTable(tblInfo, false, true)
	tbl.PhysicalID = physicalTableID
	return tbl
}

func (p *OnDemandStatsProvider) collectUncachedTargetIDsLocked(physicalTableID int64, tblInfo *model.TableInfo) []int64 {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil || physicalTableID == tblInfo.ID || len(pi.Definitions) <= 1 {
		return []int64{physicalTableID}
	}
	targetIDs := make([]int64, 0, len(pi.Definitions)+1)
	targetIDs = append(targetIDs, physicalTableID)
	for _, def := range pi.Definitions {
		if def.ID == physicalTableID || metadef.IsReservedID(def.ID) {
			continue
		}
		if _, cached := p.cache[def.ID]; !cached {
			targetIDs = append(targetIDs, def.ID)
		}
	}
	return targetIDs
}

type histItemKey struct {
	tableID int64
	isIndex bool
	histID  int64
}

type pendingHistMeta struct {
	key         histItemKey
	distinct    int64
	histVer     uint64
	nullCount   int64
	totColSize  int64
	statsVer    int64
	correlation float64
	cmBytes     []byte
	colInfo     *model.ColumnInfo
	idxInfo     *model.IndexInfo
}

type rawTopNEntry struct {
	data  []byte
	count uint64
}

func (p *OnDemandStatsProvider) loadTablesFromStorage(
	primaryID int64,
	targetIDs []int64,
	tblInfo *model.TableInfo,
) (_ map[int64]*statistics.Table, err error) {
	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	res, err := p.sessPool.Get()
	if err != nil {
		return nil, err
	}
	auxSCtx := res.(sessionctx.Context)
	execCtx := kv.WithInternalSourceType(p.ctx, kv.InternalTxnStatsForegroundPriority)
	stop := context.AfterFunc(p.ctx, func() {
		auxSCtx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	})
	completed := false
	defer func() {
		stop()
		auxSCtx.RollbackTxn(statsutil.StatsCtx)
		if !completed || err != nil || p.ctx.Err() != nil {
			p.sessPool.Destroy(res)
			return
		}
		p.sessPool.Put(res)
	}()

	if err = p.ctx.Err(); err != nil {
		return nil, err
	}
	tracker := memory.NewTracker(memory.LabelForAnalyzeMemory, -1)
	tracker.AttachTo(auxSCtx.GetSessionVars().MemTracker)
	defer tracker.Detach()

	if _, _, err = statsutil.ExecRowsWithCtx(execCtx, auxSCtx, "BEGIN"); err != nil {
		return nil, err
	}

	tables, presentIDs, err := p.loadStatsMetaBatch(execCtx, auxSCtx, primaryID, targetIDs, tblInfo)
	if err != nil || len(presentIDs) == 0 {
		if err == nil {
			completed = true
		}
		return tables, err
	}

	// Cap synchronous partition distribution loading so static partition pruning on
	// tables with thousands of analyzed partitions has bounded latency and memory.
	if len(presentIDs) > p.maxPartitionLoads {
		logutil.BgLogger().Warn("on-demand partition stats load budget reached, falling back remaining partitions to pseudo",
			zap.String("table", tblInfo.Name.O),
			zap.Int("presentPartitions", len(presentIDs)),
			zap.Int("budget", p.maxPartitionLoads))
		for _, droppedID := range presentIDs[p.maxPartitionLoads:] {
			delete(tables, droppedID)
		}
		presentIDs = presentIDs[:p.maxPartitionLoads]
	}

	// Load histograms, TopN, and buckets in table-level batches while skipping
	// mysql.stats_fm_sketch (which is only used when merging partition stats
	// during ANALYZE). Restricted-SQL callers only bypass Domain's StatsSyncLoad
	// rules via adjustOptimizationFlags; non-restricted callers still need
	// eagerly loaded distributions marked NewStatsFullLoadStatus here.
	if err = p.loadTableDistributionsBatch(execCtx, auxSCtx, presentIDs, tables, tblInfo, tracker); err != nil {
		return nil, err
	}
	completed = true
	return tables, nil
}

func (p *OnDemandStatsProvider) loadStatsMetaBatch(
	ctx context.Context,
	sctx sessionctx.Context,
	primaryID int64,
	targetIDs []int64,
	tblInfo *model.TableInfo,
) (map[int64]*statistics.Table, []int64, error) {
	tables := make(map[int64]*statistics.Table)
	presentIDs := make([]int64, 0, min(len(targetIDs), p.maxPartitionLoads))

	for start := 0; start < len(targetIDs); start += statsQueryIDChunkSize {
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, nil, err
		}
		chunkIDs := targetIDs[start:min(start+statsQueryIDChunkSize, len(targetIDs))]
		sql := "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot from mysql.stats_meta where table_id in (" +
			strings.Join(slice.Int64sToStrings(chunkIDs), ",") + ")"
		rows, _, err := statsutil.ExecRowsWithCtx(ctx, sctx, sql)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			version := row.GetUint64(0)
			tblID := row.GetInt64(1)
			modifyCount := row.GetInt64(2)
			count := int64(row.GetUint64(3))
			snapshot := row.GetUint64(4)

			histColl := *statistics.NewHistColl(tblID, count, modifyCount, len(tblInfo.Columns), len(tblInfo.Indices))
			histColl.Pseudo = false
			tables[tblID] = &statistics.Table{
				HistColl:              histColl,
				Version:               version,
				ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(len(tblInfo.Columns), len(tblInfo.Indices)),
				// Seed LastAnalyzeVersion from stats_meta.snapshot so tables analyzed
				// under PREDICATE column mode without column/index histograms still
				// report IsAnalyzed() == true (matching initStatsMeta4Chunk / Cache.Update).
				LastAnalyzeVersion: snapshot,
			}
			presentIDs = append(presentIDs, tblID)
		}
	}

	// Keep primaryID at index 0 so a partition load cap never evicts the
	// partition explicitly requested by the caller.
	for i, id := range presentIDs {
		if id == primaryID && i > 0 {
			presentIDs[0], presentIDs[i] = presentIDs[i], presentIDs[0]
			break
		}
	}
	return tables, presentIDs, nil
}

func (p *OnDemandStatsProvider) loadTableDistributionsBatch(
	ctx context.Context,
	sctx sessionctx.Context,
	tableIDs []int64,
	tables map[int64]*statistics.Table,
	tblInfo *model.TableInfo,
	tracker *memory.Tracker,
) error {
	colInfoByID := make(map[int64]*model.ColumnInfo, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		colInfoByID[colInfo.ID] = colInfo
	}
	idxInfoByID := make(map[int64]*model.IndexInfo, len(tblInfo.Indices))
	for _, idxInfo := range tblInfo.Indices {
		idxInfoByID[idxInfo.ID] = idxInfo
	}

	for start := 0; start < len(tableIDs); start += statsQueryIDChunkSize {
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return err
		}
		chunkIDs := tableIDs[start:min(start+statsQueryIDChunkSize, len(tableIDs))]
		inClause := strings.Join(slice.Int64sToStrings(chunkIDs), ",")
		histSQL := "select HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, correlation, cm_sketch from mysql.stats_histograms where table_id in (" + inClause + ")"
		histRows, _, err := statsutil.ExecRowsWithCtx(ctx, sctx, histSQL)
		if err != nil {
			return err
		}
		if len(histRows) == 0 {
			continue
		}

		pendingOrder := make([]histItemKey, 0, len(histRows))
		pending := make(map[histItemKey]*pendingHistMeta, len(histRows))
		distTableIDSet := make(map[int64]struct{}, len(chunkIDs))
		distTableIDs := make([]int64, 0, len(chunkIDs))

		for _, row := range histRows {
			tblID := row.GetInt64(0)
			tbl := tables[tblID]
			if tbl == nil {
				continue
			}
			isIndex := row.GetInt64(1) > 0
			histID := row.GetInt64(2)
			distinct := row.GetInt64(3)
			histVer := row.GetUint64(4)
			nullCount := row.GetInt64(5)
			totColSize := row.GetInt64(6)
			statsVer := row.GetInt64(7)
			correlation := row.GetFloat64(8)

			if statsVer != statistics.Version0 {
				tbl.StatsVer = int(statsVer)
				tbl.LastAnalyzeVersion = max(tbl.LastAnalyzeVersion, histVer)
			}

			if isIndex {
				idxInfo := idxInfoByID[histID]
				if idxInfo == nil {
					continue
				}
				tbl.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, statsVer != statistics.Version0)
				var cmBytes []byte
				if statsVer <= statistics.Version1 && !row.IsNull(9) {
					raw := row.GetBytes(9)
					cmBytes = append([]byte(nil), raw...)
				}
				key := histItemKey{tableID: tblID, isIndex: true, histID: histID}
				pending[key] = &pendingHistMeta{
					key:       key,
					distinct:  distinct,
					histVer:   histVer,
					nullCount: nullCount,
					statsVer:  statsVer,
					cmBytes:   cmBytes,
					idxInfo:   idxInfo,
				}
				pendingOrder = append(pendingOrder, key)
				if _, seen := distTableIDSet[tblID]; !seen {
					distTableIDSet[tblID] = struct{}{}
					distTableIDs = append(distTableIDs, tblID)
				}
				continue
			}

			colInfo := colInfoByID[histID]
			if colInfo == nil {
				continue
			}
			tbl.ColAndIdxExistenceMap.InsertCol(histID, statistics.IsColumnAnalyzedOrSynthesized(statsVer, distinct, nullCount))
			if p.skipColumnDistributions {
				col := &statistics.Column{
					PhysicalID: tblID,
					Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
					Info:       colInfo,
					IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
					StatsVer:   statsVer,
				}
				col.Histogram.Correlation = correlation
				if col.StatsAvailable() {
					col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
				}
				tracker.Consume(col.MemoryUsage().TotalMemoryUsage())
				tbl.SetCol(col.ID, col)
				continue
			}

			var cmBytes []byte
			if statsVer <= statistics.Version1 && !row.IsNull(9) {
				raw := row.GetBytes(9)
				cmBytes = append([]byte(nil), raw...)
			}
			key := histItemKey{tableID: tblID, isIndex: false, histID: histID}
			pending[key] = &pendingHistMeta{
				key:         key,
				distinct:    distinct,
				histVer:     histVer,
				nullCount:   nullCount,
				totColSize:  totColSize,
				statsVer:    statsVer,
				correlation: correlation,
				cmBytes:     cmBytes,
				colInfo:     colInfo,
			}
			pendingOrder = append(pendingOrder, key)
			if _, seen := distTableIDSet[tblID]; !seen {
				distTableIDSet[tblID] = struct{}{}
				distTableIDs = append(distTableIDs, tblID)
			}
		}

		if len(pending) > 0 {
			if err := p.populateDistributionsForChunk(ctx, sctx, distTableIDs, pendingOrder, pending, tables, tblInfo, tracker); err != nil {
				return err
			}
		}
		for _, tblID := range chunkIDs {
			if tbl := tables[tblID]; tbl != nil {
				resetDroppedTableStatsVer(tbl)
			}
		}
	}
	return nil
}

func (p *OnDemandStatsProvider) populateDistributionsForChunk(
	ctx context.Context,
	sctx sessionctx.Context,
	distTableIDs []int64,
	pendingOrder []histItemKey,
	pending map[histItemKey]*pendingHistMeta,
	tables map[int64]*statistics.Table,
	tblInfo *model.TableInfo,
	tracker *memory.Tracker,
) error {
	if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
		return err
	}
	inClause := strings.Join(slice.Int64sToStrings(distTableIDs), ",")
	indexFilter := ""
	if p.skipColumnDistributions {
		indexFilter = " and is_index = 1"
	}

	topNSQL := "select HIGH_PRIORITY table_id, is_index, hist_id, value, count from mysql.stats_top_n where table_id in (" + inClause + ")" + indexFilter
	topNRows, _, err := statsutil.ExecRowsWithCtx(ctx, sctx, topNSQL)
	if err != nil {
		return err
	}
	topNByKey := make(map[histItemKey][]rawTopNEntry)
	for _, row := range topNRows {
		key := histItemKey{
			tableID: row.GetInt64(0),
			isIndex: row.GetInt64(1) > 0,
			histID:  row.GetInt64(2),
		}
		if _, ok := pending[key]; !ok {
			continue
		}
		val := row.GetBytes(3)
		copied := make([]byte, len(val))
		copy(copied, val)
		topNByKey[key] = append(topNByKey[key], rawTopNEntry{
			data:  copied,
			count: row.GetUint64(4),
		})
	}

	if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
		return err
	}
	bucketSQL := "select HIGH_PRIORITY table_id, is_index, hist_id, count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id in (" + inClause + ")" + indexFilter + " order by table_id, is_index, hist_id, bucket_id"
	bucketRows, bucketFields, err := statsutil.ExecRowsWithCtx(ctx, sctx, bucketSQL)
	if err != nil {
		return err
	}
	type rowSpan struct{ start, end int }
	bucketsByKey := make(map[histItemKey]rowSpan)
	for i := 0; i < len(bucketRows); {
		key := histItemKey{
			tableID: bucketRows[i].GetInt64(0),
			isIndex: bucketRows[i].GetInt64(1) > 0,
			histID:  bucketRows[i].GetInt64(2),
		}
		j := i + 1
		for j < len(bucketRows) {
			nextKey := histItemKey{
				tableID: bucketRows[j].GetInt64(0),
				isIndex: bucketRows[j].GetInt64(1) > 0,
				histID:  bucketRows[j].GetInt64(2),
			}
			if nextKey != key {
				break
			}
			j++
		}
		if _, ok := pending[key]; ok {
			bucketsByKey[key] = rowSpan{start: i, end: j}
		}
		i = j
	}

	var lowerFieldTp, upperFieldTp *types.FieldType
	if len(bucketFields) >= 7 {
		lowerFieldTp = &bucketFields[5].Column.FieldType
		upperFieldTp = &bucketFields[6].Column.FieldType
	}

	for _, key := range pendingOrder {
		meta := pending[key]
		tbl := tables[key.tableID]
		if meta == nil || tbl == nil {
			continue
		}
		var itemBuckets []chunk.Row
		if span, ok := bucketsByKey[key]; ok {
			itemBuckets = bucketRows[span.start:span.end]
		}
		hg, err := decodeHistogramFromBucketRows(meta, itemBuckets, lowerFieldTp, upperFieldTp)
		if err != nil {
			return errors.Trace(err)
		}
		var cms *statistics.CMSketch
		if meta.statsVer <= statistics.Version1 && len(meta.cmBytes) > 0 {
			cms, err = statistics.DecodeCMSketch(meta.cmBytes)
			if err != nil {
				return errors.Trace(err)
			}
		}
		var topN *statistics.TopN
		if entries := topNByKey[key]; len(entries) > 0 {
			topN = statistics.NewTopN(len(entries))
			for _, e := range entries {
				topN.AppendTopN(e.data, e.count)
			}
			topN.Sort()
		}

		if key.isIndex {
			idx := &statistics.Index{
				Histogram:  *hg,
				CMSketch:   cms,
				TopN:       topN,
				Info:       meta.idxInfo,
				StatsVer:   meta.statsVer,
				PhysicalID: key.tableID,
			}
			if meta.statsVer != statistics.Version0 {
				idx.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			tracker.Consume(idx.MemoryUsage().TotalMemoryUsage())
			tbl.SetIdx(key.histID, idx)
		} else {
			col := &statistics.Column{
				PhysicalID: key.tableID,
				Histogram:  *hg,
				Info:       meta.colInfo,
				CMSketch:   cms,
				TopN:       topN,
				IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(meta.colInfo.GetFlag()),
				StatsVer:   meta.statsVer,
			}
			if col.StatsAvailable() {
				col.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			tracker.Consume(col.MemoryUsage().TotalMemoryUsage())
			tbl.SetCol(col.ID, col)
		}
	}
	return nil
}

func decodeHistogramFromBucketRows(
	meta *pendingHistMeta,
	rows []chunk.Row,
	lowerFieldTp, upperFieldTp *types.FieldType,
) (*statistics.Histogram, error) {
	var tp *types.FieldType
	if meta.key.isIndex {
		tp = types.NewFieldType(mysql.TypeBlob)
	} else {
		tp = &meta.colInfo.FieldType
	}
	hg := statistics.NewHistogram(
		meta.key.histID,
		meta.distinct,
		meta.nullCount,
		meta.histVer,
		tp,
		len(rows),
		meta.totColSize,
	)
	hg.Correlation = meta.correlation
	boundTp := tp
	if !meta.key.isIndex && boundTp.EvalType() == types.ETString && boundTp.GetType() != mysql.TypeEnum && boundTp.GetType() != mysql.TypeSet {
		boundTp = types.NewFieldType(mysql.TypeBlob)
	}
	totalCount := int64(0)
	for i := range rows {
		count := rows[i].GetInt64(3)
		repeats := rows[i].GetInt64(4)
		var lowerBound, upperBound types.Datum
		if meta.key.isIndex {
			lowerBound = rows[i].GetDatum(5, lowerFieldTp)
			upperBound = rows[i].GetDatum(6, upperFieldTp)
		} else {
			var err error
			d := rows[i].GetDatum(5, lowerFieldTp)
			lowerBound, err = convertBoundFromBlob(statistics.UTCWithAllowInvalidDateCtx, d, boundTp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(6, upperFieldTp)
			upperBound, err = convertBoundFromBlob(statistics.UTCWithAllowInvalidDateCtx, d, boundTp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		totalCount += count
		hg.AppendBucketWithNDV(&lowerBound, &upperBound, totalCount, repeats, rows[i].GetInt64(7))
	}
	hg.PreCalculateScalar()
	return hg, nil
}

func resetDroppedTableStatsVer(tbl *statistics.Table) {
	if !statistics.IsAnalyzed(int64(tbl.StatsVer)) {
		return
	}
	hasStats := false
	allZero := true
	tbl.ForEachColumnImmutable(func(_ int64, c *statistics.Column) bool {
		hasStats = true
		if statistics.IsAnalyzed(c.StatsVer) {
			allZero = false
			return true
		}
		return false
	})
	tbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
		hasStats = true
		if statistics.IsAnalyzed(idx.StatsVer) {
			allZero = false
			return true
		}
		return false
	})
	if hasStats && allZero {
		tbl.StatsVer = statistics.Version0
	}
}
