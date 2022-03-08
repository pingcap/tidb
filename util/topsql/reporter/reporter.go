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
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/collector"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"go.uber.org/zap"
)

const (
	reportTimeout         = 40 * time.Second
	collectChanBufferSize = 2
)

var nowFunc = time.Now

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	collector.Collector
	stmtstats.Collector

	// Start uses to start the reporter.
	Start()

	// RegisterSQL registers a normalizedSQL with SQLDigest.
	//
	// Note that the normalized SQL string can be of >1M long.
	// This function should be thread-safe, which means concurrently calling it
	// in several goroutines should be fine. It should also return immediately,
	// and do any CPU-intensive job asynchronously.
	RegisterSQL(sqlDigest []byte, normalizedSQL string, isInternal bool)

	// RegisterPlan like RegisterSQL, but for normalized plan strings.
	RegisterPlan(planDigest []byte, normalizedPlan string)

	// Close uses to close and release the reporter resource.
	Close()
}

var _ TopSQLReporter = &RemoteTopSQLReporter{}
var _ DataSinkRegisterer = &RemoteTopSQLReporter{}

// RemoteTopSQLReporter implements TopSQLReporter that sends data to a remote agent.
// This should be called periodically to collect TopSQL resource usage metrics.
type RemoteTopSQLReporter struct {
	DefaultDataSinkRegisterer

	ctx    context.Context
	cancel context.CancelFunc

	sqlCPUCollector         *collector.SQLCPUCollector
	collectCPUTimeChan      chan []collector.SQLCPUTimeRecord
	collectStmtStatsChan    chan stmtstats.StatementStatsMap
	reportCollectedDataChan chan collectedData

	collecting        *collecting
	normalizedSQLMap  *normalizedSQLMap
	normalizedPlanMap *normalizedPlanMap
	stmtStatsBuffer   map[uint64]stmtstats.StatementStatsMap // timestamp => stmtstats.StatementStatsMap

	// calling decodePlan this can take a while, so should not block critical paths.
	decodePlan planBinaryDecodeFunc
}

// NewRemoteTopSQLReporter creates a new RemoteTopSQLReporter.
//
// decodePlan is a decoding function which will be called asynchronously to decode the plan binary to string.
func NewRemoteTopSQLReporter(decodePlan planBinaryDecodeFunc) *RemoteTopSQLReporter {
	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		DefaultDataSinkRegisterer: NewDefaultDataSinkRegisterer(ctx),
		ctx:                       ctx,
		cancel:                    cancel,
		collectCPUTimeChan:        make(chan []collector.SQLCPUTimeRecord, collectChanBufferSize),
		collectStmtStatsChan:      make(chan stmtstats.StatementStatsMap, collectChanBufferSize),
		reportCollectedDataChan:   make(chan collectedData, 1),
		collecting:                newCollecting(),
		normalizedSQLMap:          newNormalizedSQLMap(),
		normalizedPlanMap:         newNormalizedPlanMap(),
		stmtStatsBuffer:           map[uint64]stmtstats.StatementStatsMap{},
		decodePlan:                decodePlan,
	}
	tsr.sqlCPUCollector = collector.NewSQLCPUCollector(tsr)
	return tsr
}

// Start implements the TopSQLReporter interface.
func (tsr *RemoteTopSQLReporter) Start() {
	tsr.sqlCPUCollector.Start()
	go tsr.collectWorker()
	go tsr.reportWorker()
}

// Collect implements tracecpu.Collector.
//
// WARN: It will drop the DataRecords if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) Collect(data []collector.SQLCPUTimeRecord) {
	if len(data) == 0 {
		return
	}
	select {
	case tsr.collectCPUTimeChan <- data:
	default:
		// ignore if chan blocked
		ignoreCollectChannelFullCounter.Inc()
	}
}

// CollectStmtStatsMap implements stmtstats.Collector.
//
// WARN: It will drop the DataRecords if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) CollectStmtStatsMap(data stmtstats.StatementStatsMap) {
	if len(data) == 0 {
		return
	}
	select {
	case tsr.collectStmtStatsChan <- data:
	default:
		// ignore if chan blocked
		ignoreCollectStmtChannelFullCounter.Inc()
	}
}

// RegisterSQL implements TopSQLReporter.
//
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string, isInternal bool) {
	tsr.normalizedSQLMap.register(sqlDigest, normalizedSQL, isInternal)
}

// RegisterPlan implements TopSQLReporter.
//
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedPlan string) {
	tsr.normalizedPlanMap.register(planDigest, normalizedPlan)
}

// Close implements TopSQLReporter.
func (tsr *RemoteTopSQLReporter) Close() {
	tsr.cancel()
	tsr.sqlCPUCollector.Stop()
	tsr.onReporterClosing()
}

// collectWorker consumes and collects data from tracecpu.Collector/stmtstats.Collector.
func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	currentReportInterval := topsqlstate.GlobalState.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
	defer reportTicker.Stop()
	for {
		select {
		case <-tsr.ctx.Done():
			return
		case data := <-tsr.collectCPUTimeChan:
			timestamp := uint64(nowFunc().Unix())
			tsr.processCPUTimeData(timestamp, data)
		case data := <-tsr.collectStmtStatsChan:
			timestamp := uint64(nowFunc().Unix())
			tsr.stmtStatsBuffer[timestamp] = data
		case <-reportTicker.C:
			tsr.processStmtStatsData()
			tsr.takeDataAndSendToReportChan()
			// Update `reportTicker` if report interval changed.
			if newInterval := topsqlstate.GlobalState.ReportIntervalSeconds.Load(); newInterval != currentReportInterval {
				currentReportInterval = newInterval
				reportTicker.Reset(time.Second * time.Duration(currentReportInterval))
			}
		}
	}
}

// processCPUTimeData collects top N cpuRecords of each round into tsr.collecting, and evict the
// data that is not in top N. All the evicted cpuRecords will be summary into the others.
func (tsr *RemoteTopSQLReporter) processCPUTimeData(timestamp uint64, data cpuRecords) {
	defer util.Recover("top-sql", "processCPUTimeData", nil, false)

	// Get top N cpuRecords of each round cpuRecords. Collect the top N to tsr.collecting
	// for each round. SQL meta will not be evicted, since the evicted SQL can be appeared
	// on other components (TiKV) TopN DataRecords.
	top, evicted := data.topN(int(topsqlstate.GlobalState.MaxStatementCount.Load()))
	for _, r := range top {
		tsr.collecting.getOrCreateRecord(r.SQLDigest, r.PlanDigest).appendCPUTime(timestamp, r.CPUTimeMs)
	}
	if len(evicted) == 0 {
		return
	}
	totalEvictedCPUTime := uint32(0)
	for _, e := range evicted {
		totalEvictedCPUTime += e.CPUTimeMs
		// Mark which digests are evicted under each timestamp.
		// We will determine whether the corresponding CPUTime has been evicted
		// when collecting stmtstats. If so, then we can ignore it directly.
		tsr.collecting.markAsEvicted(timestamp, e.SQLDigest, e.PlanDigest)
	}
	tsr.collecting.appendOthersCPUTime(timestamp, totalEvictedCPUTime)
}

// processStmtStatsData collects tsr.stmtStatsBuffer into tsr.collecting.
// All the evicted items will be summary into the others.
func (tsr *RemoteTopSQLReporter) processStmtStatsData() {
	defer util.Recover("top-sql", "processStmtStatsData", nil, false)

	for timestamp, data := range tsr.stmtStatsBuffer {
		for digest, item := range data {
			sqlDigest, planDigest := []byte(digest.SQLDigest), []byte(digest.PlanDigest)
			if tsr.collecting.hasEvicted(timestamp, sqlDigest, planDigest) {
				// This timestamp+sql+plan has been evicted due to low CPUTime.
				tsr.collecting.appendOthersStmtStatsItem(timestamp, *item)
				continue
			}
			tsr.collecting.getOrCreateRecord(sqlDigest, planDigest).appendStmtStatsItem(timestamp, *item)
		}
	}
	tsr.stmtStatsBuffer = map[uint64]stmtstats.StatementStatsMap{}
}

// takeDataAndSendToReportChan takes records data and then send to the report channel for reporting.
func (tsr *RemoteTopSQLReporter) takeDataAndSendToReportChan() {
	// Send to report channel. When channel is full, data will be dropped.
	select {
	case tsr.reportCollectedDataChan <- collectedData{
		collected:         tsr.collecting.take(),
		normalizedSQLMap:  tsr.normalizedSQLMap.take(),
		normalizedPlanMap: tsr.normalizedPlanMap.take(),
	}:
	default:
		// ignore if chan blocked
		ignoreReportChannelFullCounter.Inc()
	}
}

// reportWorker sends data to the gRPC endpoint from the `reportCollectedDataChan` one by one.
func (tsr *RemoteTopSQLReporter) reportWorker() {
	defer util.Recover("top-sql", "reportWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportCollectedDataChan:
			// When `reportCollectedDataChan` receives something, there could be ongoing
			// `RegisterSQL` and `RegisterPlan` running, who writes to the data structure
			// that `data` contains. So we wait for a little while to ensure that writes
			// are finished.
			time.Sleep(time.Millisecond * 100)
			rs := data.collected.getReportRecords()
			// Convert to protobuf data and do report.
			tsr.doReport(&ReportData{
				DataRecords: rs.toProto(),
				SQLMetas:    data.normalizedSQLMap.toProto(),
				PlanMetas:   data.normalizedPlanMap.toProto(tsr.decodePlan),
			})
		case <-tsr.ctx.Done():
			return
		}
	}
}

// doReport sends ReportData to DataSinks.
func (tsr *RemoteTopSQLReporter) doReport(data *ReportData) {
	defer util.Recover("top-sql", "doReport", nil, false)

	if !data.hasData() {
		return
	}
	timeout := reportTimeout
	if val, _err_ := failpoint.Eval(_curpkg_("resetTimeoutForTest")); _err_ == nil {
		if val.(bool) {
			interval := time.Duration(topsqlstate.GlobalState.ReportIntervalSeconds.Load()) * time.Second
			if interval < timeout {
				timeout = interval
			}
		}
	}
	_ = tsr.trySend(data, time.Now().Add(timeout))
}

// trySend sends ReportData to all internal registered DataSinks.
func (tsr *RemoteTopSQLReporter) trySend(data *ReportData, deadline time.Time) error {
	tsr.DefaultDataSinkRegisterer.Lock()
	dataSinks := make([]DataSink, 0, len(tsr.dataSinks))
	for ds := range tsr.dataSinks {
		dataSinks = append(dataSinks, ds)
	}
	tsr.DefaultDataSinkRegisterer.Unlock()
	for _, ds := range dataSinks {
		if err := ds.TrySend(data, deadline); err != nil {
			logutil.BgLogger().Warn("[top-sql] failed to send data to datasink", zap.Error(err))
		}
	}
	return nil
}

// onReporterClosing calls the OnReporterClosing method of all internally registered DataSinks.
func (tsr *RemoteTopSQLReporter) onReporterClosing() {
	var m map[DataSink]struct{}
	tsr.DefaultDataSinkRegisterer.Lock()
	m, tsr.dataSinks = tsr.dataSinks, make(map[DataSink]struct{})
	tsr.DefaultDataSinkRegisterer.Unlock()
	for d := range m {
		d.OnReporterClosing()
	}
}

// collectedData is used for transmission in the channel.
type collectedData struct {
	collected         *collecting
	normalizedSQLMap  *normalizedSQLMap
	normalizedPlanMap *normalizedPlanMap
}
