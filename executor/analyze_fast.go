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

package executor

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	fastAnalyzeHistogramSample        = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "sample")
	fastAnalyzeHistogramAccessRegions = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "access_regions")
	fastAnalyzeHistogramScanKeys      = metrics.FastAnalyzeHistogram.WithLabelValues(metrics.LblGeneral, "scan_keys")
)

func analyzeFastExec(exec *AnalyzeFastExec) *statistics.AnalyzeResults {
	hists, cms, topNs, fms, err := exec.buildStats()
	if err != nil {
		return &statistics.AnalyzeResults{Err: err, Job: exec.job}
	}
	var results []*statistics.AnalyzeResult
	pkColCount := pkColsCount(exec.handleCols)
	if len(exec.idxsInfo) > 0 {
		idxResult := &statistics.AnalyzeResult{
			Hist:    hists[pkColCount+len(exec.colsInfo):],
			Cms:     cms[pkColCount+len(exec.colsInfo):],
			TopNs:   topNs[pkColCount+len(exec.colsInfo):],
			Fms:     fms[pkColCount+len(exec.colsInfo):],
			IsIndex: 1,
		}
		results = append(results, idxResult)
	}
	colResult := &statistics.AnalyzeResult{
		Hist:  hists[:pkColCount+len(exec.colsInfo)],
		Cms:   cms[:pkColCount+len(exec.colsInfo)],
		TopNs: topNs[:pkColCount+len(exec.colsInfo)],
		Fms:   fms[:pkColCount+len(exec.colsInfo)],
	}
	results = append(results, colResult)
	hist := hists[0]
	cnt := hist.NullCount
	if hist.Len() > 0 {
		cnt += hist.Buckets[hist.Len()-1].Count
	}
	if exec.rowCount != 0 {
		cnt = exec.rowCount
	}
	return &statistics.AnalyzeResults{
		TableID:  exec.tableID,
		Ars:      results,
		Job:      exec.job,
		StatsVer: statistics.Version1,
		Count:    cnt,
		Snapshot: exec.snapshot,
	}
}

// AnalyzeFastExec represents Fast Analyze executor.
type AnalyzeFastExec struct {
	baseAnalyzeExec
	handleCols  core.HandleCols
	colsInfo    []*model.ColumnInfo
	idxsInfo    []*model.IndexInfo
	tblInfo     *model.TableInfo
	cache       *tikv.RegionCache
	wg          *sync.WaitGroup
	rowCount    int64
	sampCursor  int32
	sampTasks   []*tikv.KeyLocation
	scanTasks   []*tikv.KeyLocation
	collectors  []*statistics.SampleCollector
	randSeed    int64
	estSampStep uint32
}

func (e *AnalyzeFastExec) calculateEstimateSampleStep() (err error) {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "select flag from mysql.stats_histograms where table_id = %?", e.tableID.GetStatisticsID())
	if err != nil {
		return
	}
	var historyRowCount uint64
	hasBeenAnalyzed := len(rows) != 0 && rows[0].GetInt64(0) == statistics.AnalyzeFlag
	if hasBeenAnalyzed {
		historyRowCount = uint64(domain.GetDomain(e.ctx).StatsHandle().GetPartitionStats(e.tblInfo, e.tableID.GetStatisticsID()).Count)
	} else {
		dbInfo, ok := domain.GetDomain(e.ctx).InfoSchema().SchemaByTable(e.tblInfo)
		if !ok {
			err = errors.Errorf("database not found for table '%s'", e.tblInfo.Name)
			return
		}
		var rollbackFn func() error
		rollbackFn, err = e.activateTxnForRowCount()
		if err != nil {
			return
		}
		defer func() {
			if rollbackFn != nil {
				err = rollbackFn()
			}
		}()
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "select count(*) from %n.%n", dbInfo.Name.L, e.tblInfo.Name.L)

		if e.tblInfo.ID != e.tableID.GetStatisticsID() {
			for _, definition := range e.tblInfo.Partition.Definitions {
				if definition.ID == e.tableID.GetStatisticsID() {
					sqlexec.MustFormatSQL(sql, " partition(%n)", definition.Name.L)
					break
				}
			}
		}
		var rs sqlexec.RecordSet
		rs, err = e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql.String())
		if err != nil {
			return
		}
		if rs == nil {
			err = errors.Trace(errors.Errorf("empty record set"))
			return
		}
		defer terror.Call(rs.Close)
		chk := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return
		}
		e.rowCount = chk.GetRow(0).GetInt64(0)
		historyRowCount = uint64(e.rowCount)
	}
	totalSampSize := e.opts[ast.AnalyzeOptNumSamples]
	e.estSampStep = uint32(historyRowCount / totalSampSize)
	return
}

func (e *AnalyzeFastExec) activateTxnForRowCount() (rollbackFn func() error, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	txn, err := e.ctx.Txn(true)
	if err != nil {
		if kv.ErrInvalidTxn.Equal(err) {
			_, err := e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "begin")
			if err != nil {
				return nil, errors.Trace(err)
			}
			rollbackFn = func() error {
				_, err := e.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "rollback")
				return err
			}
		} else {
			return nil, errors.Trace(err)
		}
	}
	txn.SetOption(kv.Priority, kv.PriorityLow)
	isoLevel := kv.RC
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		isoLevel = kv.SI
	}
	txn.SetOption(kv.IsolationLevel, isoLevel)
	txn.SetOption(kv.NotFillCache, true)
	return rollbackFn, nil
}

// buildSampTask build sample tasks.
func (e *AnalyzeFastExec) buildSampTask() (err error) {
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	store, _ := e.ctx.GetStore().(tikv.Storage)
	e.cache = store.GetRegionCache()
	accessRegionsCounter := 0
	pid := e.tableID.GetStatisticsID()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(pid)
	targetKey := startKey
	for {
		// Search for the region which contains the targetKey.
		loc, err := e.cache.LocateKey(bo, targetKey)
		if err != nil {
			return derr.ToTiDBErr(err)
		}
		if bytes.Compare(endKey, loc.StartKey) < 0 {
			break
		}
		accessRegionsCounter++

		// Set the next search key.
		targetKey = loc.EndKey

		// If the KV pairs in the region all belonging to the table, add it to the sample task.
		if bytes.Compare(startKey, loc.StartKey) <= 0 && len(loc.EndKey) != 0 && bytes.Compare(loc.EndKey, endKey) <= 0 {
			e.sampTasks = append(e.sampTasks, loc)
			continue
		}

		e.scanTasks = append(e.scanTasks, loc)
		if bytes.Compare(loc.StartKey, startKey) < 0 {
			loc.StartKey = startKey
		}
		if bytes.Compare(endKey, loc.EndKey) < 0 || len(loc.EndKey) == 0 {
			loc.EndKey = endKey
			break
		}
	}
	fastAnalyzeHistogramAccessRegions.Observe(float64(accessRegionsCounter))

	return nil
}

func (e *AnalyzeFastExec) decodeValues(handle kv.Handle, sValue []byte, wantCols map[int64]*types.FieldType) (values map[int64]types.Datum, err error) {
	loc := e.ctx.GetSessionVars().Location()
	values, err = tablecodec.DecodeRowToDatumMap(sValue, wantCols, loc)
	if err != nil || e.handleCols == nil {
		return values, err
	}
	wantCols = make(map[int64]*types.FieldType, e.handleCols.NumCols())
	handleColIDs := make([]int64, e.handleCols.NumCols())
	for i := 0; i < e.handleCols.NumCols(); i++ {
		c := e.handleCols.GetCol(i)
		handleColIDs[i] = c.ID
		wantCols[c.ID] = c.RetType
	}
	return tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, wantCols, loc, values)
}

func (e *AnalyzeFastExec) getValueByInfo(colInfo *model.ColumnInfo, values map[int64]types.Datum) (types.Datum, error) {
	val, ok := values[colInfo.ID]
	if !ok {
		return table.GetColOriginDefaultValue(e.ctx, colInfo)
	}
	return val, nil
}

func (e *AnalyzeFastExec) updateCollectorSamples(sValue []byte, sKey kv.Key, samplePos int32) (err error) {
	var handle kv.Handle
	handle, err = tablecodec.DecodeRowKey(sKey)
	if err != nil {
		return err
	}

	// Decode cols for analyze table
	wantCols := make(map[int64]*types.FieldType, len(e.colsInfo))
	for _, col := range e.colsInfo {
		wantCols[col.ID] = &col.FieldType
	}

	// Pre-build index->cols relationship and refill wantCols if not exists(analyze index)
	index2Cols := make([][]*model.ColumnInfo, len(e.idxsInfo))
	for i, idxInfo := range e.idxsInfo {
		for _, idxCol := range idxInfo.Columns {
			colInfo := e.tblInfo.Columns[idxCol.Offset]
			index2Cols[i] = append(index2Cols[i], colInfo)
			wantCols[colInfo.ID] = &colInfo.FieldType
		}
	}

	// Decode the cols value in order.
	var values map[int64]types.Datum
	values, err = e.decodeValues(handle, sValue, wantCols)
	if err != nil {
		return err
	}
	// Update the primary key collector.
	pkColsCount := pkColsCount(e.handleCols)
	for i := 0; i < pkColsCount; i++ {
		col := e.handleCols.GetCol(i)
		v, ok := values[col.ID]
		if !ok {
			return errors.Trace(errors.Errorf("Primary key column not found"))
		}
		if e.collectors[i].Samples[samplePos] == nil {
			e.collectors[i].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[i].Samples[samplePos].Handle = handle
		e.collectors[i].Samples[samplePos].Value = v
	}

	// Update the columns' collectors.
	for j, colInfo := range e.colsInfo {
		v, err := e.getValueByInfo(colInfo, values)
		if err != nil {
			return err
		}
		if e.collectors[pkColsCount+j].Samples[samplePos] == nil {
			e.collectors[pkColsCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[pkColsCount+j].Samples[samplePos].Handle = handle
		e.collectors[pkColsCount+j].Samples[samplePos].Value = v
	}
	// Update the indexes' collectors.
	for j, idxInfo := range e.idxsInfo {
		idxVals := make([]types.Datum, 0, len(idxInfo.Columns))
		cols := index2Cols[j]
		for _, colInfo := range cols {
			v, err := e.getValueByInfo(colInfo, values)
			if err != nil {
				return err
			}
			idxVals = append(idxVals, v)
		}
		var keyBytes []byte
		keyBytes, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, keyBytes, idxVals...)
		if err != nil {
			return err
		}
		if e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos] == nil {
			e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos] = &statistics.SampleItem{}
		}
		e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos].Handle = handle
		e.collectors[len(e.colsInfo)+pkColsCount+j].Samples[samplePos].Value = types.NewBytesDatum(keyBytes)
	}
	return nil
}

func (e *AnalyzeFastExec) handleBatchSeekResponse(kvMap map[string][]byte) (err error) {
	length := int32(len(kvMap))
	newCursor := atomic.AddInt32(&e.sampCursor, length)
	samplePos := newCursor - length
	for sKey, sValue := range kvMap {
		exceedNeededSampleCounts := uint64(samplePos) >= e.opts[ast.AnalyzeOptNumSamples]
		if exceedNeededSampleCounts {
			atomic.StoreInt32(&e.sampCursor, int32(e.opts[ast.AnalyzeOptNumSamples]))
			break
		}
		err = e.updateCollectorSamples(sValue, kv.Key(sKey), samplePos)
		if err != nil {
			return err
		}
		samplePos++
	}
	return nil
}

func (e *AnalyzeFastExec) handleScanIter(iter kv.Iterator) (scanKeysSize int, err error) {
	rander := rand.New(rand.NewSource(e.randSeed)) // #nosec G404
	sampleSize := int64(e.opts[ast.AnalyzeOptNumSamples])
	for ; iter.Valid() && err == nil; err = iter.Next() {
		// reservoir sampling
		scanKeysSize++
		randNum := rander.Int63n(int64(e.sampCursor) + int64(scanKeysSize))
		if randNum > sampleSize && e.sampCursor == int32(sampleSize) {
			continue
		}

		p := rander.Int31n(int32(sampleSize))
		if e.sampCursor < int32(sampleSize) {
			p = e.sampCursor
			e.sampCursor++
		}

		err = e.updateCollectorSamples(iter.Value(), iter.Key(), p)
		if err != nil {
			return
		}
	}
	return
}

func (e *AnalyzeFastExec) handleScanTasks(bo *tikv.Backoffer) (keysSize int, err error) {
	var snapshot kv.Snapshot
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		snapshot = e.ctx.GetStore().GetSnapshot(kv.NewVersion(e.snapshot))
		snapshot.SetOption(kv.IsolationLevel, kv.SI)
	} else {
		snapshot = e.ctx.GetStore().GetSnapshot(kv.MaxVersion)
	}
	if e.ctx.GetSessionVars().GetReplicaRead().IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, kv.ReplicaReadFollower)
	}
	setOptionForTopSQL(e.ctx.GetSessionVars().StmtCtx, snapshot)
	snapshot.SetOption(kv.ResourceGroupName, e.ctx.GetSessionVars().ResourceGroupName)
	for _, t := range e.scanTasks {
		iter, err := snapshot.Iter(kv.Key(t.StartKey), kv.Key(t.EndKey))
		if err != nil {
			return keysSize, err
		}
		size, err := e.handleScanIter(iter)
		keysSize += size
		if err != nil {
			return keysSize, err
		}
	}
	return keysSize, nil
}

func (e *AnalyzeFastExec) handleSampTasks(workID int, step uint32, err *error) {
	defer e.wg.Done()
	var snapshot kv.Snapshot
	if e.ctx.GetSessionVars().EnableAnalyzeSnapshot {
		snapshot = e.ctx.GetStore().GetSnapshot(kv.NewVersion(e.snapshot))
		snapshot.SetOption(kv.IsolationLevel, kv.SI)
	} else {
		snapshot = e.ctx.GetStore().GetSnapshot(kv.MaxVersion)
		snapshot.SetOption(kv.IsolationLevel, kv.RC)
	}
	snapshot.SetOption(kv.NotFillCache, true)
	snapshot.SetOption(kv.Priority, kv.PriorityLow)
	snapshot.SetOption(kv.ResourceGroupName, e.ctx.GetSessionVars().ResourceGroupName)
	setOptionForTopSQL(e.ctx.GetSessionVars().StmtCtx, snapshot)
	readReplicaType := e.ctx.GetSessionVars().GetReplicaRead()
	if readReplicaType.IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, readReplicaType)
	}

	rander := rand.New(rand.NewSource(e.randSeed)) // #nosec G404
	for i := workID; i < len(e.sampTasks); i += e.concurrency {
		task := e.sampTasks[i]
		// randomize the estimate step in range [step - 2 * sqrt(step), step]
		if step > 4 { // 2*sqrt(x) < x
			lower, upper := step-uint32(2*math.Sqrt(float64(step))), step
			step = uint32(rander.Intn(int(upper-lower))) + lower
		}
		snapshot.SetOption(kv.SampleStep, step)
		kvMap := make(map[string][]byte)
		var iter kv.Iterator
		iter, *err = snapshot.Iter(kv.Key(task.StartKey), kv.Key(task.EndKey))
		if *err != nil {
			return
		}
		for iter.Valid() {
			kvMap[string(iter.Key())] = iter.Value()
			*err = iter.Next()
			if *err != nil {
				return
			}
		}
		fastAnalyzeHistogramSample.Observe(float64(len(kvMap)))

		*err = e.handleBatchSeekResponse(kvMap)
		if *err != nil {
			return
		}
	}
}

func (e *AnalyzeFastExec) buildColumnStats(ID int64, collector *statistics.SampleCollector, tp *types.FieldType, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, *statistics.TopN, *statistics.FMSketch, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	data := make([][]byte, 0, len(collector.Samples))
	fmSketch := statistics.NewFMSketch(maxSketchSize)
	notNullSamples := make([]*statistics.SampleItem, 0, len(collector.Samples))
	for i, sample := range collector.Samples {
		sample.Ordinal = i
		if sample.Value.IsNull() {
			collector.NullCount++
			continue
		}
		notNullSamples = append(notNullSamples, sample)
		err := fmSketch.InsertValue(sc, sample.Value)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		valBytes, err := tablecodec.EncodeValue(sc, nil, sample.Value)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		data = append(data, valBytes)
	}
	// Build CMSketch.
	cmSketch, topN, ndv, scaleRatio := statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data, uint32(e.opts[ast.AnalyzeOptNumTopN]), uint64(rowCount))
	// Build Histogram.
	collector.Samples = notNullSamples
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), ID, collector, tp, rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, topN, fmSketch, err
}

func (e *AnalyzeFastExec) buildIndexStats(idxInfo *model.IndexInfo, collector *statistics.SampleCollector, rowCount int64) (*statistics.Histogram, *statistics.CMSketch, *statistics.TopN, error) {
	data := make([][][]byte, len(idxInfo.Columns))
	for _, sample := range collector.Samples {
		var preLen int
		remained := sample.Value.GetBytes()
		// We need to insert each prefix values into CM Sketch.
		for i := 0; i < len(idxInfo.Columns); i++ {
			var err error
			var value []byte
			value, remained, err = codec.CutOne(remained)
			if err != nil {
				return nil, nil, nil, err
			}
			preLen += len(value)
			data[i] = append(data[i], sample.Value.GetBytes()[:preLen])
		}
	}
	numTop := uint32(e.opts[ast.AnalyzeOptNumTopN])
	cmSketch, topN, ndv, scaleRatio := statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[0], numTop, uint64(rowCount))
	// Build CM Sketch for each prefix and merge them into one.
	for i := 1; i < len(idxInfo.Columns); i++ {
		var curCMSketch *statistics.CMSketch
		var curTopN *statistics.TopN
		// `ndv` should be the ndv of full index, so just rewrite it here.
		curCMSketch, curTopN, ndv, scaleRatio = statistics.NewCMSketchAndTopN(int32(e.opts[ast.AnalyzeOptCMSketchDepth]), int32(e.opts[ast.AnalyzeOptCMSketchWidth]), data[i], numTop, uint64(rowCount))
		err := cmSketch.MergeCMSketch(curCMSketch)
		if err != nil {
			return nil, nil, nil, err
		}
		statistics.MergeTopNAndUpdateCMSketch(topN, curTopN, cmSketch, numTop)
	}
	// Build Histogram.
	hist, err := statistics.BuildColumnHist(e.ctx, int64(e.opts[ast.AnalyzeOptNumBuckets]), idxInfo.ID, collector, types.NewFieldType(mysql.TypeBlob), rowCount, int64(ndv), collector.NullCount*int64(scaleRatio))
	return hist, cmSketch, topN, err
}

func (e *AnalyzeFastExec) runTasks() ([]*statistics.Histogram, []*statistics.CMSketch, []*statistics.TopN, []*statistics.FMSketch, error) {
	errs := make([]error, e.concurrency)
	pkColCount := pkColsCount(e.handleCols)
	// collect column samples and primary key samples and index samples.
	length := len(e.colsInfo) + pkColCount + len(e.idxsInfo)
	e.collectors = make([]*statistics.SampleCollector, length)
	for i := range e.collectors {
		e.collectors[i] = &statistics.SampleCollector{
			MaxSampleSize: int64(e.opts[ast.AnalyzeOptNumSamples]),
			Samples:       make([]*statistics.SampleItem, e.opts[ast.AnalyzeOptNumSamples]),
		}
	}

	e.wg.Add(e.concurrency)
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	for i := 0; i < e.concurrency; i++ {
		go e.handleSampTasks(i, e.estSampStep, &errs[i])
	}
	e.wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}

	scanKeysSize, err := e.handleScanTasks(bo)
	fastAnalyzeHistogramScanKeys.Observe(float64(scanKeysSize))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stats := domain.GetDomain(e.ctx).StatsHandle()
	var rowCount int64 = 0
	if stats.Lease() > 0 {
		if t := stats.GetPartitionStats(e.tblInfo, e.tableID.GetStatisticsID()); !t.Pseudo {
			rowCount = t.Count
		}
	}
	hists, cms, topNs, fms := make([]*statistics.Histogram, length), make([]*statistics.CMSketch, length), make([]*statistics.TopN, length), make([]*statistics.FMSketch, length)
	for i := 0; i < length; i++ {
		// Build collector properties.
		collector := e.collectors[i]
		collector.Samples = collector.Samples[:e.sampCursor]
		sort.Slice(collector.Samples, func(i, j int) bool {
			return collector.Samples[i].Handle.Compare(collector.Samples[j].Handle) < 0
		})
		collector.CalcTotalSize()
		// Adjust the row count in case the count of `tblStats` is not accurate and too small.
		rowCount = mathutil.Max(rowCount, int64(len(collector.Samples)))
		// Scale the total column size.
		if len(collector.Samples) > 0 {
			collector.TotalSize *= rowCount / int64(len(collector.Samples))
		}
		if i < pkColCount {
			pkCol := e.handleCols.GetCol(i)
			hists[i], cms[i], topNs[i], fms[i], err = e.buildColumnStats(pkCol.ID, e.collectors[i], pkCol.RetType, rowCount)
		} else if i < pkColCount+len(e.colsInfo) {
			hists[i], cms[i], topNs[i], fms[i], err = e.buildColumnStats(e.colsInfo[i-pkColCount].ID, e.collectors[i], &e.colsInfo[i-pkColCount].FieldType, rowCount)
		} else {
			hists[i], cms[i], topNs[i], err = e.buildIndexStats(e.idxsInfo[i-pkColCount-len(e.colsInfo)], e.collectors[i], rowCount)
		}
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	return hists, cms, topNs, fms, nil
}

func (e *AnalyzeFastExec) buildStats() (hists []*statistics.Histogram, cms []*statistics.CMSketch, topNs []*statistics.TopN, fms []*statistics.FMSketch, err error) {
	// To set rand seed, it's for unit test.
	// To ensure that random sequences are different in non-test environments, RandSeed must be set time.Now().
	if atomic.LoadInt64(&RandSeed) == 1 {
		atomic.StoreInt64(&e.randSeed, time.Now().UnixNano())
	} else {
		atomic.StoreInt64(&e.randSeed, RandSeed)
	}

	err = e.buildSampTask()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return e.runTasks()
}

// AnalyzeTestFastExec is for fast sample in unit test.
type AnalyzeTestFastExec struct {
	AnalyzeFastExec
	Ctx         sessionctx.Context
	TableID     statistics.AnalyzeTableID
	HandleCols  core.HandleCols
	ColsInfo    []*model.ColumnInfo
	IdxsInfo    []*model.IndexInfo
	Concurrency int
	Collectors  []*statistics.SampleCollector
	TblInfo     *model.TableInfo
	Opts        map[ast.AnalyzeOptionType]uint64
	Snapshot    uint64
}

// TestFastSample only test the fast sample in unit test.
func (e *AnalyzeTestFastExec) TestFastSample() error {
	e.ctx = e.Ctx
	e.handleCols = e.HandleCols
	e.colsInfo = e.ColsInfo
	e.idxsInfo = e.IdxsInfo
	e.concurrency = e.Concurrency
	e.tableID = e.TableID
	e.wg = &sync.WaitGroup{}
	e.job = &statistics.AnalyzeJob{}
	e.tblInfo = e.TblInfo
	e.opts = e.Opts
	e.snapshot = e.Snapshot
	_, _, _, _, err := e.buildStats()
	e.Collectors = e.collectors
	return err
}

func pkColsCount(handleCols core.HandleCols) int {
	if handleCols == nil {
		return 0
	}
	return handleCols.NumCols()
}
