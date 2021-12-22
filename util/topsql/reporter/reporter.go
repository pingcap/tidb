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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
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
	stmtstats.Collector
	RegisterSQL(sqlDigest []byte, normalizedSQL string, isInternal bool)
	RegisterPlan(planDigest []byte, normalizedPlan string)
	Close()
}

// DataSinkRegisterer is for registering DataSink
type DataSinkRegisterer interface {
	Register(dataSink DataSink) error
	Deregister(dataSink DataSink)
}

type cpuData struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}

// dataPoints represents the cumulative SQL plan CPU time in current minute window
// dataPoints do not guarantee the TimestampList is sorted by timestamp when there is a time jump backward.
type dataPoints struct {
	SQLDigest             []byte
	PlanDigest            []byte
	TimestampList         []uint64
	CPUTimeMsList         []uint32
	CPUTimeMsTotal        uint64
	StmtExecCountList     []uint64
	StmtKvExecCountList   []map[string]uint64
	StmtDurationSumNsList []uint64

	// tsIndex is used to quickly find the corresponding array index through timestamp.
	//
	// map: timestamp => index of TimestampList / CPUTimeMsList / StmtExecCountList / ...
	tsIndex map[uint64]int
}

func newDataPoints(sqlDigest, planDigest []byte) *dataPoints {
	listCap := int(variable.TopSQLVariable.ReportIntervalSeconds.Load()/variable.TopSQLVariable.PrecisionSeconds.Load() + 1)
	if listCap < 1 {
		listCap = 1
	}
	return &dataPoints{
		SQLDigest:             sqlDigest,
		PlanDigest:            planDigest,
		CPUTimeMsList:         make([]uint32, 0, listCap),
		TimestampList:         make([]uint64, 0, listCap),
		StmtExecCountList:     make([]uint64, 0, listCap),
		StmtKvExecCountList:   make([]map[string]uint64, 0, listCap),
		StmtDurationSumNsList: make([]uint64, 0, listCap),
		tsIndex:               make(map[uint64]int, listCap),
	}
}

func (d *dataPoints) rebuildTsIndex() {
	if len(d.TimestampList) == 0 {
		d.tsIndex = map[uint64]int{}
		return
	}
	d.tsIndex = make(map[uint64]int, len(d.TimestampList))
	for index, ts := range d.TimestampList {
		d.tsIndex[ts] = index
	}
}

func (d *dataPoints) appendCPUTime(timestamp uint64, cpuTimeMs uint32) {
	if index, ok := d.tsIndex[timestamp]; ok {
		// For the same timestamp, we have already called appendStmtStatsItem,
		// d.TimestampList already exists the corresponding timestamp. And it
		// can be determined that the corresponding index of d.StmtXxx has been
		// correctly assigned, and the corresponding index of d.CPUTimeMsList
		// has been set to 0, so we directly replace it.
		//
		// let timestamp = 10000, cpuTimeMs = 123
		//
		// Before:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [0]
		//     StmtExecCountList: [999]
		//   StmtKvExecCountList: [map{"9.9.9.9:9":999}]
		// StmtDurationSumNsList: [999]
		//
		// After:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [123]
		//     StmtExecCountList: [999]
		//   StmtKvExecCountList: [map{"9.9.9.9:9":999}]
		// StmtDurationSumNsList: [999]
		//
		d.CPUTimeMsList[index] = cpuTimeMs
	} else {
		// For this timestamp, we have not appended any data. So append it directly,
		// and set the data not related to CPUTimeList to 0.
		//
		// let timestamp = 10000, cpu_time = 123
		//
		// Before:
		//               tsIndex: []
		//         TimestampList: []
		//         CPUTimeMsList: []
		//     StmtExecCountList: []
		//   StmtKvExecCountList: []
		// StmtDurationSumNsList: []
		//
		// After:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [123]
		//     StmtExecCountList: [0]
		//   StmtKvExecCountList: [map{}]
		// StmtDurationSumNsList: [0]
		//
		d.tsIndex[timestamp] = len(d.TimestampList)
		d.TimestampList = append(d.TimestampList, timestamp)
		d.CPUTimeMsList = append(d.CPUTimeMsList, cpuTimeMs)
		d.StmtExecCountList = append(d.StmtExecCountList, 0)
		d.StmtKvExecCountList = append(d.StmtKvExecCountList, map[string]uint64{})
		d.StmtDurationSumNsList = append(d.StmtDurationSumNsList, 0)
	}
	d.CPUTimeMsTotal += uint64(cpuTimeMs)
}

func (d *dataPoints) appendStmtStatsItem(timestamp uint64, item *stmtstats.StatementStatsItem) {
	if index, ok := d.tsIndex[timestamp]; ok {
		// For the same timestamp, we have already called appendCPUTime,
		// d.TimestampList already exists the corresponding timestamp. And it
		// can be determined that the corresponding index of d.CPUTimeMsList has been
		// correctly assigned, and the corresponding index of d.StmtXxx
		// has been set to 0 or empty map, so we directly replace it.
		//
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456
		//
		// Before:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [999]
		//     StmtExecCountList: [0]
		//   StmtKvExecCountList: [map{}]
		// StmtDurationSumNsList: [0]
		//
		// After:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [999]
		//     StmtExecCountList: [123]
		//   StmtKvExecCountList: [map{"1.1.1.1:1": 123}]
		// StmtDurationSumNsList: [456]
		//
		d.StmtExecCountList[index] = item.ExecCount
		d.StmtKvExecCountList[index] = item.KvStatsItem.KvExecCount
		d.StmtDurationSumNsList[index] = 0 // TODO(mornyx): add duration
	} else {
		// For this timestamp, we have not appended any data. So append it directly,
		// the corresponding index of d.CPUTimeList is preset to 0.
		//
		// let timestamp = 10000, execCount = 123, kvExecCount = map{"1.1.1.1:1": 123}, durationSum = 456
		//
		// Before:
		//               tsIndex: []
		//         TimestampList: []
		//         CPUTimeMsList: []
		//     StmtExecCountList: []
		//   StmtKvExecCountList: []
		// StmtDurationSumNsList: []
		//
		// After:
		//               tsIndex: [10000 => 0]
		//         TimestampList: [10000]
		//         CPUTimeMsList: [0]
		//     StmtExecCountList: [123]
		//   StmtKvExecCountList: [map{"1.1.1.1:1": 123}]
		// StmtDurationSumNsList: [456]
		//
		d.tsIndex[timestamp] = len(d.TimestampList)
		d.TimestampList = append(d.TimestampList, timestamp)
		d.CPUTimeMsList = append(d.CPUTimeMsList, 0)
		d.StmtExecCountList = append(d.StmtExecCountList, item.ExecCount)
		d.StmtKvExecCountList = append(d.StmtKvExecCountList, item.KvStatsItem.KvExecCount)
		d.StmtDurationSumNsList = append(d.StmtDurationSumNsList, 0) // TODO(mornyx): add duration
	}
}

func (d *dataPoints) isInvalid() bool {
	return !(len(d.TimestampList) == len(d.CPUTimeMsList) &&
		len(d.TimestampList) == len(d.StmtExecCountList) &&
		len(d.TimestampList) == len(d.StmtKvExecCountList) &&
		len(d.TimestampList) == len(d.StmtDurationSumNsList))
}

func (d *dataPoints) Len() int {
	return len(d.TimestampList)
}

func (d *dataPoints) Less(i, j int) bool {
	// sort by timestamp
	return d.TimestampList[i] < d.TimestampList[j]
}
func (d *dataPoints) Swap(i, j int) {
	// before swap:
	//     TimestampList: [10000, 10001, 10002]
	//           tsIndex: [10000 => 0, 10001 => 1, 10002 => 2]
	//
	// let i = 0, j = 1
	// after swap tsIndex:
	//     TimestampList: [10000, 10001, 10002]
	//           tsIndex: [10000 => 1, 10001 => 0, 10002 => 2]
	//
	// after swap TimestampList:
	//     TimestampList: [10001, 10000, 10002]
	//           tsIndex: [10000 => 1, 10001 => 0, 10002 => 2]
	d.tsIndex[d.TimestampList[i]], d.tsIndex[d.TimestampList[j]] = d.tsIndex[d.TimestampList[j]], d.tsIndex[d.TimestampList[i]]
	d.TimestampList[i], d.TimestampList[j] = d.TimestampList[j], d.TimestampList[i]
	d.CPUTimeMsList[i], d.CPUTimeMsList[j] = d.CPUTimeMsList[j], d.CPUTimeMsList[i]
	d.StmtExecCountList[i], d.StmtExecCountList[j] = d.StmtExecCountList[j], d.StmtExecCountList[i]
	d.StmtKvExecCountList[i], d.StmtKvExecCountList[j] = d.StmtKvExecCountList[j], d.StmtKvExecCountList[i]
	d.StmtDurationSumNsList[i], d.StmtDurationSumNsList[j] = d.StmtDurationSumNsList[j], d.StmtDurationSumNsList[i]
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

	dataSinkMu sync.Mutex
	dataSinks  map[DataSink]struct{}

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are SQLMeta.
	normalizedSQLMap atomic.Value // sync.Map
	sqlMapLength     atomic2.Int64

	// normalizedPlanMap is an map, whose keys are plan digest strings and values are normalized plans **in binary**.
	// The normalized plans in binary can be decoded to string using the `planBinaryDecoder`.
	normalizedPlanMap atomic.Value // sync.Map
	planMapLength     atomic2.Int64

	collectCPUDataChan      chan cpuData
	collectStmtRecordsChan  chan []stmtstats.StatementStatsRecord
	reportCollectedDataChan chan collectedData

	// calling decodePlan this can take a while, so should not block critical paths
	decodePlan planBinaryDecodeFunc
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
func NewRemoteTopSQLReporter(decodePlan planBinaryDecodeFunc) *RemoteTopSQLReporter {
	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		ctx:    ctx,
		cancel: cancel,

		dataSinks: make(map[DataSink]struct{}, 10),

		collectCPUDataChan:      make(chan cpuData, 1),
		collectStmtRecordsChan:  make(chan []stmtstats.StatementStatsRecord, 1),
		reportCollectedDataChan: make(chan collectedData, 1),
		decodePlan:              decodePlan,
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
	ignoreCollectStmtChannelFullCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_stmt_channel_full")
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

var _ DataSinkRegisterer = &RemoteTopSQLReporter{}

// Register implements DataSinkRegisterer interface.
func (tsr *RemoteTopSQLReporter) Register(dataSink DataSink) error {
	tsr.dataSinkMu.Lock()
	defer tsr.dataSinkMu.Unlock()

	select {
	case <-tsr.ctx.Done():
		return errors.New("reporter is closed")
	default:
		if len(tsr.dataSinks) >= 10 {
			return errors.New("too many datasinks")
		}

		tsr.dataSinks[dataSink] = struct{}{}

		if len(tsr.dataSinks) > 0 {
			variable.TopSQLVariable.Enable.Store(true)
		}

		return nil
	}
}

// Deregister implements DataSinkRegisterer interface.
func (tsr *RemoteTopSQLReporter) Deregister(dataSink DataSink) {
	tsr.dataSinkMu.Lock()
	defer tsr.dataSinkMu.Unlock()

	select {
	case <-tsr.ctx.Done():
	default:
		delete(tsr.dataSinks, dataSink)

		if len(tsr.dataSinks) == 0 {
			variable.TopSQLVariable.Enable.Store(false)
		}
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

// CollectStmtStatsRecords receives stmtstats.StatementStatsRecord for processing.
// WARN: It will drop the records if the processing is not in time.
// This function is thread-safe and efficient.
//
// CollectStmtStatsRecords implements stmtstats.Collector.CollectStmtStatsRecords.
func (tsr *RemoteTopSQLReporter) CollectStmtStatsRecords(records []stmtstats.StatementStatsRecord) {
	if len(records) == 0 {
		return
	}
	select {
	case tsr.collectStmtRecordsChan <- records:
	default:
		// ignore if chan blocked
		ignoreCollectStmtChannelFullCounter.Inc()
	}
}

// Close uses to close and release the reporter resource.
func (tsr *RemoteTopSQLReporter) Close() {
	tsr.cancel()

	var m map[DataSink]struct{}
	tsr.dataSinkMu.Lock()
	m, tsr.dataSinks = tsr.dataSinks, make(map[DataSink]struct{})
	tsr.dataSinkMu.Unlock()

	for d := range m {
		d.OnReporterClosing()
	}
}

func addEvictedCPUTime(collectTarget map[string]*dataPoints, timestamp uint64, totalCPUTimeMs uint32) {
	if totalCPUTimeMs == 0 {
		return
	}
	others, ok := collectTarget[keyOthers]
	if !ok {
		others = &dataPoints{tsIndex: map[uint64]int{}}
		collectTarget[keyOthers] = others
	}
	others.appendCPUTime(timestamp, totalCPUTimeMs)
}

func addEvictedStmtStatsItem(collectTarget map[string]*dataPoints, timestamp uint64, item *stmtstats.StatementStatsItem) {
	others, ok := collectTarget[keyOthers]
	if !ok {
		others = &dataPoints{tsIndex: map[uint64]int{}}
		collectTarget[keyOthers] = others
	}
	others.appendStmtStatsItem(timestamp, item)
}

// addEvictedIntoSortedDataPoints adds evicted dataPoints into others.
// Attention, this function depend on others dataPoints is sorted, and this function will modify evicted dataPoints
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
		others.StmtExecCountList = evict.StmtExecCountList
		others.StmtKvExecCountList = evict.StmtKvExecCountList
		others.StmtDurationSumNsList = evict.StmtDurationSumNsList
		others.tsIndex = evict.tsIndex
		return others
	}
	length := len(others.TimestampList) + len(evict.TimestampList)
	timestampList := make([]uint64, 0, length)
	cpuTimeMsList := make([]uint32, 0, length)
	stmtExecCountList := make([]uint64, 0, length)
	stmtKvExecCountList := make([]map[string]uint64, 0, length)
	stmtDurationSumList := make([]uint64, 0, length)
	i := 0
	j := 0
	for i < len(others.TimestampList) && j < len(evict.TimestampList) {
		if others.TimestampList[i] == evict.TimestampList[j] {
			timestampList = append(timestampList, others.TimestampList[i])
			cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i]+evict.CPUTimeMsList[j])
			stmtExecCountList = append(stmtExecCountList, others.StmtExecCountList[i]+evict.StmtExecCountList[j])
			stmtKvExecCountList = append(stmtKvExecCountList, mergeKvExecCountMap(others.StmtKvExecCountList[i], evict.StmtKvExecCountList[j]))
			stmtDurationSumList = append(stmtDurationSumList, others.StmtDurationSumNsList[i]+evict.StmtDurationSumNsList[j])
			i++
			j++
		} else if others.TimestampList[i] < evict.TimestampList[j] {
			timestampList = append(timestampList, others.TimestampList[i])
			cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i])
			stmtExecCountList = append(stmtExecCountList, others.StmtExecCountList[i])
			stmtKvExecCountList = append(stmtKvExecCountList, others.StmtKvExecCountList[i])
			stmtDurationSumList = append(stmtDurationSumList, others.StmtDurationSumNsList[i])
			i++
		} else {
			timestampList = append(timestampList, evict.TimestampList[j])
			cpuTimeMsList = append(cpuTimeMsList, evict.CPUTimeMsList[j])
			stmtExecCountList = append(stmtExecCountList, evict.StmtExecCountList[j])
			stmtKvExecCountList = append(stmtKvExecCountList, evict.StmtKvExecCountList[j])
			stmtDurationSumList = append(stmtDurationSumList, evict.StmtDurationSumNsList[j])
			j++
		}
	}
	if i < len(others.TimestampList) {
		timestampList = append(timestampList, others.TimestampList[i:]...)
		cpuTimeMsList = append(cpuTimeMsList, others.CPUTimeMsList[i:]...)
		stmtExecCountList = append(stmtExecCountList, others.StmtExecCountList[i:]...)
		for _, l := range others.StmtKvExecCountList[i:] {
			stmtKvExecCountList = append(stmtKvExecCountList, l)
		}
		stmtDurationSumList = append(stmtDurationSumList, others.StmtDurationSumNsList[i:]...)
	}
	if j < len(evict.TimestampList) {
		timestampList = append(timestampList, evict.TimestampList[j:]...)
		cpuTimeMsList = append(cpuTimeMsList, evict.CPUTimeMsList[j:]...)
		stmtExecCountList = append(stmtExecCountList, evict.StmtExecCountList[j:]...)
		for _, l := range evict.StmtKvExecCountList[j:] {
			stmtKvExecCountList = append(stmtKvExecCountList, l)
		}
		stmtDurationSumList = append(stmtDurationSumList, evict.StmtDurationSumNsList[j:]...)
	}
	others.TimestampList = timestampList
	others.CPUTimeMsList = cpuTimeMsList
	others.StmtExecCountList = stmtExecCountList
	others.StmtKvExecCountList = stmtKvExecCountList
	others.StmtDurationSumNsList = stmtDurationSumList
	others.CPUTimeMsTotal += evict.CPUTimeMsTotal
	others.rebuildTsIndex()
	return others
}

func mergeKvExecCountMap(a, b map[string]uint64) map[string]uint64 {
	r := map[string]uint64{}
	for ka, va := range a {
		r[ka] = va
	}
	for kb, vb := range b {
		r[kb] += vb
	}
	return r
}

func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	collectedData := make(map[string]*dataPoints)
	evictedDigest := make(map[uint64]map[stmtstats.SQLPlanDigest]struct{})
	currentReportInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
	for {
		select {
		case data := <-tsr.collectCPUDataChan:
			// On receiving data to collect: Write to local data array, and retain records with most CPU time.
			tsr.doCollect(collectedData, evictedDigest, data.timestamp, data.records)
		case data := <-tsr.collectStmtRecordsChan:
			tsr.doCollectStmtRecords(collectedData, evictedDigest, data)
		case <-reportTicker.C:
			// We clean up evictedDigest before reporting, to avoid continuous accumulation.
			evictedDigest = make(map[uint64]map[stmtstats.SQLPlanDigest]struct{})
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
	collectTarget map[string]*dataPoints,
	evictedDigest map[uint64]map[stmtstats.SQLPlanDigest]struct{},
	timestamp uint64,
	records []tracecpu.SQLCPUTimeRecord) {
	defer util.Recover("top-sql", "doCollect", nil, false)

	// Get top N records of each round records.
	var evicted []tracecpu.SQLCPUTimeRecord
	records, evicted = getTopNRecords(records)

	keyBuf := bytes.NewBuffer(make([]byte, 0, 64))
	// Collect the top N records to collectTarget for each round.
	for _, record := range records {
		key := encodeKey(keyBuf, record.SQLDigest, record.PlanDigest)
		entry, exist := collectTarget[key]
		if !exist {
			collectTarget[key] = newDataPoints(record.SQLDigest, record.PlanDigest)
			entry = collectTarget[key]
		}
		entry.appendCPUTime(timestamp, record.CPUTimeMs)
	}

	if len(evicted) == 0 {
		return
	}
	// Merge non Top N data as "others" (keyed by in the `keyOthers`) in `collectTarget`.
	// SQL meta will not be evicted, since the evicted SQL can be appear on Other components (TiKV) TopN records.
	totalEvictedCPUTime := uint32(0)
	for _, evict := range evicted {
		totalEvictedCPUTime += evict.CPUTimeMs

		// Record which digests are evicted under each timestamp, and judge whether
		// the corresponding CPUTime is evicted when stmtstats is collected, if it
		// has been evicted, then we can ignore it directly.
		if _, ok := evictedDigest[timestamp]; !ok {
			evictedDigest[timestamp] = map[stmtstats.SQLPlanDigest]struct{}{}
		}
		evictedDigest[timestamp][stmtstats.SQLPlanDigest{
			SQLDigest:  stmtstats.BinaryDigest(evict.SQLDigest),
			PlanDigest: stmtstats.BinaryDigest(evict.PlanDigest),
		}] = struct{}{}
	}
	addEvictedCPUTime(collectTarget, timestamp, totalEvictedCPUTime)
}

func (tsr *RemoteTopSQLReporter) doCollectStmtRecords(
	collectTarget map[string]*dataPoints,
	evictedDigest map[uint64]map[stmtstats.SQLPlanDigest]struct{},
	records []stmtstats.StatementStatsRecord) {
	defer util.Recover("top-sql", "doCollectStmtRecords", nil, false)

	keyBuf := bytes.NewBuffer(make([]byte, 0, 64))
	for _, record := range records {
		timestamp := uint64(record.Timestamp)
		for digest, item := range record.Data {
			sqlDigest := []byte(digest.SQLDigest)
			planDigest := []byte(digest.PlanDigest)
			if digestSet, ok := evictedDigest[timestamp]; ok {
				if _, ok := digestSet[digest]; ok {
					// This record has been evicted due to low CPUTime.
					addEvictedStmtStatsItem(collectTarget, timestamp, item)
					continue
				}
			}
			key := encodeKey(keyBuf, sqlDigest, planDigest)
			entry, exist := collectTarget[key]
			if !exist {
				collectTarget[key] = newDataPoints(sqlDigest, planDigest)
				entry = collectTarget[key]
			}
			entry.appendStmtStatsItem(timestamp, item)
		}
	}
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

// ReportData contains data that reporter sends to the agent
type ReportData struct {
	// DataRecords contains the topN collected records and the `others` record which aggregation all records that is out of Top N.
	DataRecords []tipb.TopSQLRecord
	SQLMetas    []tipb.SQLMeta
	PlanMetas   []tipb.PlanMeta
}

func (d *ReportData) hasData() bool {
	return len(d.DataRecords) != 0 || len(d.SQLMetas) != 0 || len(d.PlanMetas) != 0
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

// getReportData gets ReportData from the collectedData.
// This function will calculate the topN collected records and the `others` record which aggregation all records that is out of Top N.
func (tsr *RemoteTopSQLReporter) getReportData(collected collectedData) *ReportData {
	records := getTopNFromCollected(collected)
	return tsr.buildReportData(records, collected.normalizedSQLMap, collected.normalizedPlanMap)
}

func getTopNFromCollected(collected collectedData) (records []*dataPoints) {
	// Fetch TopN dataPoints.
	others := collected.records[keyOthers]
	delete(collected.records, keyOthers)

	records = make([]*dataPoints, 0, len(collected.records))
	for _, v := range collected.records {
		if v.CPUTimeMsTotal > 0 {
			// For a certain timestamp, when StmtStats is collected, but CPUTime
			// has not been collected, we have not yet filled the evictedDigest data,
			// so it is possible that a piece of data is evicted in CPUTime, but still
			// exists in collectTarget due to StmtStats. So we are here to remove those
			// data that have no CPUTime at all.
			records = append(records, v)
		}
	}

	// Evict all records that is out of Top N.
	var evicted []*dataPoints
	records, evicted = getTopNDataPoints(records)
	if others != nil {
		// Sort the dataPoints by timestamp to fix the affect of time jump backward.
		sort.Sort(others)
	}
	for _, evict := range evicted {
		// SQL meta will not be evicted, since the evicted SQL can be appeared on Other components (TiKV) TopN records.
		others = addEvictedIntoSortedDataPoints(others, evict)
	}

	// append others which summarize all evicted item's cpu-time.
	if others != nil && others.CPUTimeMsTotal > 0 {
		records = append(records, others)
	}

	return
}

// buildReportData convert record data in dataPoints slice and meta data in sync.Map to ReportData.
//
// Attention, caller should guarantee no more reader or writer access `sqlMap` and `planMap`, because buildReportData
// will do heavy jobs in sync.Map.Range and it may block other readers and writers.
func (tsr *RemoteTopSQLReporter) buildReportData(records []*dataPoints, sqlMap *sync.Map, planMap *sync.Map) *ReportData {
	res := &ReportData{
		DataRecords: make([]tipb.TopSQLRecord, 0, len(records)),
		SQLMetas:    make([]tipb.SQLMeta, 0, len(records)),
		PlanMetas:   make([]tipb.PlanMeta, 0, len(records)),
	}

	for _, record := range records {
		recordListStmtKvExecCount := make([]*tipb.TopSQLStmtKvExecCount, len(record.StmtKvExecCountList))
		for n, l := range record.StmtKvExecCountList {
			recordListStmtKvExecCount[n] = &tipb.TopSQLStmtKvExecCount{ExecCount: l}
		}
		res.DataRecords = append(res.DataRecords, tipb.TopSQLRecord{
			RecordListTimestampSec:      record.TimestampList,
			RecordListCpuTimeMs:         record.CPUTimeMsList,
			RecordListStmtExecCount:     record.StmtExecCountList,
			RecordListStmtKvExecCount:   recordListStmtKvExecCount,
			RecordListStmtDurationSumNs: record.StmtDurationSumNsList,
			SqlDigest:                   record.SQLDigest,
			PlanDigest:                  record.PlanDigest,
		})
	}

	sqlMap.Range(func(key, value interface{}) bool {
		meta := value.(SQLMeta)
		res.SQLMetas = append(res.SQLMetas, tipb.SQLMeta{
			SqlDigest:     []byte(key.(string)),
			NormalizedSql: meta.normalizedSQL,
			IsInternalSql: meta.isInternal,
		})
		return true
	})

	planMap.Range(func(key, value interface{}) bool {
		planDecoded, errDecode := tsr.decodePlan(value.(string))
		if errDecode != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(errDecode))
			return true
		}
		res.PlanMetas = append(res.PlanMetas, tipb.PlanMeta{
			PlanDigest:     []byte(key.(string)),
			NormalizedPlan: planDecoded,
		})
		return true
	})

	return res
}

func (tsr *RemoteTopSQLReporter) doReport(data *ReportData) {
	defer util.Recover("top-sql", "doReport", nil, false)

	if !data.hasData() {
		return
	}

	timeout := reportTimeout
	failpoint.Inject("resetTimeoutForTest", func(val failpoint.Value) {
		if val.(bool) {
			interval := time.Duration(variable.TopSQLVariable.ReportIntervalSeconds.Load()) * time.Second
			if interval < timeout {
				timeout = interval
			}
		}
	})
	deadline := time.Now().Add(timeout)

	tsr.dataSinkMu.Lock()
	dataSinks := make([]DataSink, 0, len(tsr.dataSinks))
	for ds := range tsr.dataSinks {
		dataSinks = append(dataSinks, ds)
	}
	tsr.dataSinkMu.Unlock()

	for _, ds := range dataSinks {
		if err := ds.TrySend(data, deadline); err != nil {
			logutil.BgLogger().Warn("[top-sql] failed to send data to datasink", zap.Error(err))
		}
	}
}
