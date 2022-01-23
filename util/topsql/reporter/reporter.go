// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/wangjohn/quickselect"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	dialTimeout               = 5 * time.Second
	reportTimeout             = 40 * time.Second
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
	// keyOthers is the key to store the aggregation of all records that is out of Top N.
	keyOthers = ""
)

var _ TopSQLReporter = &RemoteTopSQLReporter{}

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Collector
	RegisterSQL(sqlDigest []byte, normalizedSQL string, isInternal bool)
	RegisterPlan(planDigest []byte, normalizedPlan string)
	Close()
}

type cpuData struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}

// dataPoints represents the cumulative SQL plan CPU time in current minute window
// dataPoints do not guarantee the TimestampList is sorted by timestamp when there is a time jump backward.
type dataPoints struct {
	SQLDigest      []byte
	PlanDigest     []byte
	TimestampList  []uint64
	CPUTimeMsList  []uint32
	CPUTimeMsTotal uint64
}

func (d *dataPoints) isInvalid() bool {
	return len(d.TimestampList) != len(d.CPUTimeMsList)
}

func (d *dataPoints) Len() int {
	return len(d.TimestampList)
}

func (d *dataPoints) Less(i, j int) bool {
	// sort by timestamp
	return d.TimestampList[i] < d.TimestampList[j]
}
func (d *dataPoints) Swap(i, j int) {
	d.TimestampList[i], d.TimestampList[j] = d.TimestampList[j], d.TimestampList[i]
	d.CPUTimeMsList[i], d.CPUTimeMsList[j] = d.CPUTimeMsList[j], d.CPUTimeMsList[i]
}

type dataPointsOrderByCPUTime []*dataPoints

func (t dataPointsOrderByCPUTime) Len() int {
	return len(t)
}

func (t dataPointsOrderByCPUTime) Less(i, j int) bool {
	// We need find the kth largest value, so here should use >
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t dataPointsOrderByCPUTime) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type sqlCPUTimeRecordSlice []tracecpu.SQLCPUTimeRecord

func (t sqlCPUTimeRecordSlice) Len() int {
	return len(t)
}

func (t sqlCPUTimeRecordSlice) Less(i, j int) bool {
	// We need find the kth largest value, so here should use >
	return t[i].CPUTimeMs > t[j].CPUTimeMs
}
func (t sqlCPUTimeRecordSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type planBinaryDecodeFunc func(string) (string, error)

// RemoteTopSQLReporter implements a TopSQL reporter that sends data to a remote agent
// This should be called periodically to collect TopSQL resource usage metrics
type RemoteTopSQLReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	client ReportClient

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are SQLMeta.
	normalizedSQLMap atomic.Value // sync.Map
	sqlMapLength     atomic2.Int64

	// normalizedPlanMap is an map, whose keys are plan digest strings and values are normalized plans **in binary**.
	// The normalized plans in binary can be decoded to string using the `planBinaryDecoder`.
	normalizedPlanMap atomic.Value // sync.Map
	planMapLength     atomic2.Int64

	collectCPUDataChan      chan cpuData
	reportCollectedDataChan chan collectedData
}

// SQLMeta is the SQL meta which contains the normalized SQL string and a bool field which uses to distinguish internal SQL.
type SQLMeta struct {
	normalizedSQL string
	isInternal    bool
}

// NewRemoteTopSQLReporter creates a new TopSQL reporter
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(client ReportClient) *RemoteTopSQLReporter {
	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		ctx:                     ctx,
		cancel:                  cancel,
		client:                  client,
		collectCPUDataChan:      make(chan cpuData, 1),
		reportCollectedDataChan: make(chan collectedData, 1),
	}
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	go tsr.collectWorker()
	go tsr.reportWorker()

	return tsr
}

var (
	ignoreExceedSQLCounter              = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_sql")
	ignoreExceedPlanCounter             = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_plan")
	ignoreCollectChannelFullCounter     = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_channel_full")
	ignoreReportChannelFullCounter      = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_report_channel_full")
	reportAllDurationSuccHistogram      = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblOK)
	reportAllDurationFailedHistogram    = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblError)
	reportRecordDurationSuccHistogram   = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblOK)
	reportRecordDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblError)
	reportSQLDurationSuccHistogram      = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblOK)
	reportSQLDurationFailedHistogram    = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblError)
	reportPlanDurationSuccHistogram     = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblOK)
	reportPlanDurationFailedHistogram   = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblError)
	topSQLReportRecordCounterHistogram  = metrics.TopSQLReportDataHistogram.WithLabelValues("record")
	topSQLReportSQLCountHistogram       = metrics.TopSQLReportDataHistogram.WithLabelValues("sql")
	topSQLReportPlanCountHistogram      = metrics.TopSQLReportDataHistogram.WithLabelValues("plan")
)

// RegisterSQL registers a normalized SQL string to a SQL digest.
// This function is thread-safe and efficient.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string, isInternal bool) {
	if tsr.sqlMapLength.Load() >= variable.TopSQLVariable.MaxCollect.Load() {
		ignoreExceedSQLCounter.Inc()
		return
	}
	m := tsr.normalizedSQLMap.Load().(*sync.Map)
	key := string(sqlDigest)
	_, loaded := m.LoadOrStore(key, SQLMeta{
		normalizedSQL: normalizedSQL,
		isInternal:    isInternal,
	})
	if !loaded {
		tsr.sqlMapLength.Add(1)
	}
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedBinaryPlan string) {
	if tsr.planMapLength.Load() >= variable.TopSQLVariable.MaxCollect.Load() {
		ignoreExceedPlanCounter.Inc()
		return
	}
	m := tsr.normalizedPlanMap.Load().(*sync.Map)
	key := string(planDigest)
	_, loaded := m.LoadOrStore(key, normalizedBinaryPlan)
	if !loaded {
		tsr.planMapLength.Add(1)
	}
}

// Collect receives CPU time records for processing. WARN: It will drop the records if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) Collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	if len(records) == 0 {
		return
	}
	select {
	case tsr.collectCPUDataChan <- cpuData{
		timestamp: timestamp,
		records:   records,
	}:
	default:
		// ignore if chan blocked
		ignoreCollectChannelFullCounter.Inc()
	}
}

// Close uses to close and release the reporter resource.
func (tsr *RemoteTopSQLReporter) Close() {
	tsr.cancel()
	tsr.client.Close()
}

func addEvictedCPUTime(collectTarget map[string]*dataPoints, timestamp uint64, totalCPUTimeMs uint32) {
	if totalCPUTimeMs == 0 {
		return
	}
	others, ok := collectTarget[keyOthers]
	if !ok {
		others = &dataPoints{}
		collectTarget[keyOthers] = others
	}
	if len(others.TimestampList) == 0 {
		others.TimestampList = []uint64{timestamp}
		others.CPUTimeMsList = []uint32{totalCPUTimeMs}
	} else {
		others.TimestampList = append(others.TimestampList, timestamp)
		others.CPUTimeMsList = append(others.CPUTimeMsList, totalCPUTimeMs)
	}
	others.CPUTimeMsTotal += uint64(totalCPUTimeMs)
}

// addEvictedIntoSortedDataPoints adds the evict dataPoints into others.
// Attention, this function depend on others dataPoints is sorted, and this function will modify the evict dataPoints
// to make sure it is sorted by timestamp.
func addEvictedIntoSortedDataPoints(others *dataPoints, evict *dataPoints) *dataPoints {
	if others == nil {
		others = &dataPoints{}
	}
	if evict == nil || len(evict.TimestampList) == 0 {
		return others
	}
	if evict.isInvalid() {
		logutil.BgLogger().Warn("[top-sql] data points is invalid, it should never happen", zap.Any("self", others), zap.Any("evict", evict))
		return others
	}
	// Sort the dataPoints by timestamp to fix the affect of time jump backward.
	sort.Sort(evict)
	if len(others.TimestampList) == 0 {
		others.TimestampList = evict.TimestampList
		others.CPUTimeMsList = evict.CPUTimeMsList
		others.CPUTimeMsTotal = evict.CPUTimeMsTotal
		return others
	}
	length := len(others.TimestampList) + len(evict.TimestampList)
	timestampList := make([]uint64, 0, length)
	cpuTimeMsList := make([]uint32, 0, length)
	i := 0
	j := 0
	for i < len(others.TimestampList) && j < len(evict.TimestampList) {
		if others.TimestampList[i] == evict.TimestampList[j] {
			timestampList = append(timestampList, others.TimestampList[i])
			cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i]+evict.CPUTimeMsList[j])
			i++
			j++
		} else if others.TimestampList[i] < evict.TimestampList[j] {
			timestampList = append(timestampList, others.TimestampList[i])
			cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i])
			i++
		} else {
			timestampList = append(timestampList, evict.TimestampList[j])
			cpuTimeMsList = append(cpuTimeMsList, evict.CPUTimeMsList[j])
			j++
		}
	}
	if i < len(others.TimestampList) {
		timestampList = append(timestampList, others.TimestampList[i:]...)
		cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i:]...)
	}
	if j < len(evict.TimestampList) {
		timestampList = append(timestampList, evict.TimestampList[j:]...)
		cpuTimeMsList = append(cpuTimeMsList, evict.CPUTimeMsList[j:]...)
	}
	others.TimestampList = timestampList
	others.CPUTimeMsList = cpuTimeMsList
	others.CPUTimeMsTotal += evict.CPUTimeMsTotal
	return others
}

func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	collectedData := make(map[string]*dataPoints)
	currentReportInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
	for {
		select {
		case data := <-tsr.collectCPUDataChan:
			// On receiving data to collect: Write to local data array, and retain records with most CPU time.
			tsr.doCollect(collectedData, data.timestamp, data.records)
		case <-reportTicker.C:
			tsr.takeDataAndSendToReportChan(&collectedData)
			// Update `reportTicker` if report interval changed.
			if newInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load(); newInterval != currentReportInterval {
				currentReportInterval = newInterval
				reportTicker.Reset(time.Second * time.Duration(currentReportInterval))
			}
		case <-tsr.ctx.Done():
			return
		}
	}
}

func encodeKey(buf *bytes.Buffer, sqlDigest, planDigest []byte) string {
	buf.Reset()
	buf.Write(sqlDigest)
	buf.Write(planDigest)
	return buf.String()
}

func getTopNRecords(records []tracecpu.SQLCPUTimeRecord) (topN, shouldEvict []tracecpu.SQLCPUTimeRecord) {
	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(records) <= maxStmt {
		return records, nil
	}
	if err := quickselect.QuickSelect(sqlCPUTimeRecordSlice(records), maxStmt); err != nil {
		//	skip eviction
		return records, nil
	}
	return records[:maxStmt], records[maxStmt:]
}

func getTopNDataPoints(records []*dataPoints) (topN, shouldEvict []*dataPoints) {
	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(records) <= maxStmt {
		return records, nil
	}
	if err := quickselect.QuickSelect(dataPointsOrderByCPUTime(records), maxStmt); err != nil {
		//	skip eviction
		return records, nil
	}
	return records[:maxStmt], records[maxStmt:]
}

// doCollect collects top N records of each round into collectTarget, and evict the data that is not in top N.
// All the evicted record will be summary into the collectedData.others.
func (tsr *RemoteTopSQLReporter) doCollect(
	collectTarget map[string]*dataPoints, timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	defer util.Recover("top-sql", "doCollect", nil, false)

	// Get top N records of each round records.
	var evicted []tracecpu.SQLCPUTimeRecord
	records, evicted = getTopNRecords(records)

	keyBuf := bytes.NewBuffer(make([]byte, 0, 64))
	listCapacity := int(variable.TopSQLVariable.ReportIntervalSeconds.Load()/variable.TopSQLVariable.PrecisionSeconds.Load() + 1)
	if listCapacity < 1 {
		listCapacity = 1
	}
	// Collect the top N records to collectTarget for each round.
	for _, record := range records {
		key := encodeKey(keyBuf, record.SQLDigest, record.PlanDigest)
		entry, exist := collectTarget[key]
		if !exist {
			entry = &dataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: make([]uint32, 1, listCapacity),
				TimestampList: make([]uint64, 1, listCapacity),
			}
			entry.CPUTimeMsList[0] = record.CPUTimeMs
			entry.TimestampList[0] = timestamp
			collectTarget[key] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	if len(evicted) == 0 {
		return
	}
	// Clean up non Top N data and merge them as "others" (keyed by in the `keyOthers`) in `collectTarget`.
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)
	totalEvictedCPUTime := uint32(0)
	for _, evict := range evicted {
		totalEvictedCPUTime += evict.CPUTimeMs
		key := encodeKey(keyBuf, evict.SQLDigest, evict.PlanDigest)
		_, ok := collectTarget[key]
		if ok {
			continue
		}
		_, loaded := normalizedSQLMap.LoadAndDelete(string(evict.SQLDigest))
		if loaded {
			tsr.sqlMapLength.Add(-1)
		}
		_, loaded = normalizedPlanMap.LoadAndDelete(string(evict.PlanDigest))
		if loaded {
			tsr.planMapLength.Add(-1)
		}
	}
	addEvictedCPUTime(collectTarget, timestamp, totalEvictedCPUTime)
}

// takeDataAndSendToReportChan takes collected data and then send to the report channel for reporting.
func (tsr *RemoteTopSQLReporter) takeDataAndSendToReportChan(collectedDataPtr *map[string]*dataPoints) {
	data := collectedData{
		records:           *collectedDataPtr,
		normalizedSQLMap:  tsr.normalizedSQLMap.Load().(*sync.Map),
		normalizedPlanMap: tsr.normalizedPlanMap.Load().(*sync.Map),
	}

	// Reset data for next report.
	*collectedDataPtr = make(map[string]*dataPoints)
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})
	tsr.sqlMapLength.Store(0)
	tsr.planMapLength.Store(0)

	// Send to report channel. When channel is full, data will be dropped.
	select {
	case tsr.reportCollectedDataChan <- data:
	default:
		// ignore if chan blocked
		ignoreReportChannelFullCounter.Inc()
	}
}

type collectedData struct {
	records           map[string]*dataPoints
	normalizedSQLMap  *sync.Map
	normalizedPlanMap *sync.Map
}

// reportData contains data that reporter sends to the agent
type reportData struct {
	// collectedData contains the topN collected records and the `others` record which aggregation all records that is out of Top N.
	collectedData     []*dataPoints
	normalizedSQLMap  *sync.Map
	normalizedPlanMap *sync.Map
}

func (d *reportData) hasData() bool {
	if len(d.collectedData) > 0 {
		return true
	}
	cnt := 0
	d.normalizedSQLMap.Range(func(key, value interface{}) bool {
		cnt++
		return false
	})
	if cnt > 0 {
		return true
	}
	d.normalizedPlanMap.Range(func(key, value interface{}) bool {
		cnt++
		return false
	})
	return cnt > 0
}

// reportWorker sends data to the gRPC endpoint from the `reportCollectedDataChan` one by one.
func (tsr *RemoteTopSQLReporter) reportWorker() {
	defer util.Recover("top-sql", "reportWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportCollectedDataChan:
			// When `reportCollectedDataChan` receives something, there could be ongoing `RegisterSQL` and `RegisterPlan` running,
			// who writes to the data structure that `data` contains. So we wait for a little while to ensure that
			// these writes are finished.
			time.Sleep(time.Millisecond * 100)
			report := tsr.getReportData(data)
			tsr.doReport(report)
		case <-tsr.ctx.Done():
			return
		}
	}
}

// getReportData gets reportData from the collectedData.
// This function will calculate the topN collected records and the `others` record which aggregation all records that is out of Top N.
func (tsr *RemoteTopSQLReporter) getReportData(collected collectedData) reportData {
	// Fetch TopN dataPoints.
	others := collected.records[keyOthers]
	delete(collected.records, keyOthers)
	records := make([]*dataPoints, 0, len(collected.records))
	for _, v := range collected.records {
		records = append(records, v)
	}

	// Evict all records that is out of Top N.
	var evicted []*dataPoints
	records, evicted = getTopNDataPoints(records)
	if others != nil {
		// Sort the dataPoints by timestamp to fix the affect of time jump backward.
		sort.Sort(others)
	}
	for _, evict := range evicted {
		collected.normalizedSQLMap.LoadAndDelete(string(evict.SQLDigest))
		collected.normalizedPlanMap.LoadAndDelete(string(evict.PlanDigest))
		others = addEvictedIntoSortedDataPoints(others, evict)
	}

	// append others which summarize all evicted item's cpu-time.
	if others != nil && others.CPUTimeMsTotal > 0 {
		records = append(records, others)
	}

	return reportData{
		collectedData:     records,
		normalizedSQLMap:  collected.normalizedSQLMap,
		normalizedPlanMap: collected.normalizedPlanMap,
	}
}

func (tsr *RemoteTopSQLReporter) doReport(data reportData) {
	defer util.Recover("top-sql", "doReport", nil, false)

	if !data.hasData() {
		return
	}

	agentAddr := config.GetGlobalConfig().TopSQL.ReceiverAddress
	timeout := reportTimeout
	failpoint.Inject("resetTimeoutForTest", func(val failpoint.Value) {
		if val.(bool) {
			interval := time.Duration(variable.TopSQLVariable.ReportIntervalSeconds.Load()) * time.Second
			if interval < timeout {
				timeout = interval
			}
		}
	})
	ctx, cancel := context.WithTimeout(tsr.ctx, timeout)
	start := time.Now()
	err := tsr.client.Send(ctx, agentAddr, data)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] client failed to send data", zap.Error(err))
		reportAllDurationFailedHistogram.Observe(time.Since(start).Seconds())
	} else {
		reportAllDurationSuccHistogram.Observe(time.Since(start).Seconds())
	}
	cancel()
}
