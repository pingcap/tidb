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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
)

const reportTimeout = 40 * time.Second

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Collector
	stmtstats.Collector

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

	collectCPUDataChan      chan cpuData
	collectStmtRecordsChan  chan []stmtstats.StatementStatsRecord
	reportCollectedDataChan chan collectedData

	collecting        *collecting
	normalizedSQLMap  *normalizedSQLMap
	normalizedPlanMap *normalizedPlanMap

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
		collectCPUDataChan:        make(chan cpuData, 1),
		collectStmtRecordsChan:    make(chan []stmtstats.StatementStatsRecord, 1),
		reportCollectedDataChan:   make(chan collectedData, 1),
		collecting:                newCollecting(),
		normalizedSQLMap:          newNormalizedSQLMap(),
		normalizedPlanMap:         newNormalizedPlanMap(),
		decodePlan:                decodePlan,
	}
	go tsr.collectWorker()
	go tsr.reportWorker()
	return tsr
}

// Collect implements tracecpu.Collector.
//
// WARN: It will drop the DataRecords if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) Collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	if len(records) == 0 {
		return
	}
	select {
	case tsr.collectCPUDataChan <- cpuData{timestamp: timestamp, records: records}:
	default:
		// ignore if chan blocked
		ignoreCollectChannelFullCounter.Inc()
	}
}

// CollectStmtStatsRecords implements stmtstats.Collector.
//
// WARN: It will drop the DataRecords if the processing is not in time.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) CollectStmtStatsRecords(rs []stmtstats.StatementStatsRecord) {
	if len(rs) == 0 {
		return
	}
	select {
	case tsr.collectStmtRecordsChan <- rs:
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
	tsr.OnReporterClosing()
}

// collectWorker consumes and collects data from tracecpu.Collector/stmtstats.Collector.
func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	currentReportInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
	for {
		select {
		case data := <-tsr.collectCPUDataChan:
			tsr.doCollect(data.timestamp, data.records)
		case rs := <-tsr.collectStmtRecordsChan:
			tsr.doCollectStmtRecords(rs)
		case <-reportTicker.C:
			tsr.takeDataAndSendToReportChan()
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

// doCollect collects top N cpuRecords of each round into tsr.collecting, and evict the
// data that is not in top N. All the evicted cpuRecords will be summary into the others.
func (tsr *RemoteTopSQLReporter) doCollect(timestamp uint64, rs cpuRecords) {
	defer util.Recover("top-sql", "doCollect", nil, false)

	// Get top N cpuRecords of each round cpuRecords. Collect the top N to tsr.collecting
	// for each round. SQL meta will not be evicted, since the evicted SQL can be appeared
	// on other components (TiKV) TopN DataRecords.
	var evicted cpuRecords
	rs, evicted = rs.topN(int(variable.TopSQLVariable.MaxStatementCount.Load()))
	for _, r := range rs {
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

// doCollectStmtRecords collects []stmtstats.StatementStatsRecord into tsr.collecting.
// All the evicted items will be summary into the others.
func (tsr *RemoteTopSQLReporter) doCollectStmtRecords(rs []stmtstats.StatementStatsRecord) {
	defer util.Recover("top-sql", "doCollectStmtRecords", nil, false)

	for _, r := range rs {
		timestamp := uint64(r.Timestamp)
		for digest, item := range r.Data {
			sqlDigest, planDigest := []byte(digest.SQLDigest), []byte(digest.PlanDigest)
			if tsr.collecting.hasEvicted(timestamp, sqlDigest, planDigest) {
				// This timestamp+sql+plan has been evicted due to low CPUTime.
				tsr.collecting.appendOthersStmtStatsItem(timestamp, *item)
				continue
			}
			tsr.collecting.getOrCreateRecord(sqlDigest, planDigest).appendStmtStatsItem(timestamp, *item)
		}
	}
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
			// Get top N records from records.
			rs := data.collected.topN(int(variable.TopSQLVariable.MaxStatementCount.Load()))
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
	failpoint.Inject("resetTimeoutForTest", func(val failpoint.Value) {
		if val.(bool) {
			interval := time.Duration(variable.TopSQLVariable.ReportIntervalSeconds.Load()) * time.Second
			if interval < timeout {
				timeout = interval
			}
		}
	})
	_ = tsr.TrySend(data, time.Now().Add(timeout))
}

// collectedData is used for transmission in the channel.
type collectedData struct {
	collected         *collecting
	normalizedSQLMap  *normalizedSQLMap
	normalizedPlanMap *normalizedPlanMap
}

// cpuData is used for transmission in the channel.
type cpuData struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}
