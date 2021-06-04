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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

const (
	collectCPUTimeChanLen     = 64
	dialTimeout               = 5 * time.Second
	reportTimeout             = 40 * time.Second
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

var _ TopSQLReporter = &RemoteTopSQLReporter{}

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Collector
	RegisterSQL(sqlDigest []byte, normalizedSQL string)
	RegisterPlan(planDigest []byte, normalizedPlan string)
	Close()
}

type cpuData struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}

// dataPoints represents the cumulative SQL plan CPU time in current minute window
type dataPoints struct {
	SQLDigest      []byte
	PlanDigest     []byte
	TimestampList  []uint64
	CPUTimeMsList  []uint32
	CPUTimeMsTotal uint64
}

// cpuTimeSort is used to sort TopSQL records by total CPU time
type cpuTimeSort struct {
	Key            string
	SQLDigest      []byte
	PlanDigest     []byte
	CPUTimeMsTotal uint64 // The sorting field
}

type cpuTimeSortSlice []cpuTimeSort

func (t cpuTimeSortSlice) Len() int {
	return len(t)
}

func (t cpuTimeSortSlice) Less(i, j int) bool {
	// We need find the kth largest value, so here should use >
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t cpuTimeSortSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type planBinaryDecodeFunc func(string) (string, error)

// RemoteTopSQLReporter implements a TopSQL reporter that sends data to a remote agent
// This should be called periodically to collect TopSQL resource usage metrics
type RemoteTopSQLReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	client ReportClient

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap atomic.Value // sync.Map

	// normalizedPlanMap is an map, whose keys are plan digest strings and values are normalized plans **in binary**.
	// The normalized plans in binary can be decoded to string using the `planBinaryDecoder`.
	normalizedPlanMap atomic.Value // sync.Map

	// calling this can take a while, so should not block critical paths
	planBinaryDecoder planBinaryDecodeFunc

	collectCPUDataChan chan cpuData
	reportDataChan     chan reportData
}

// NewRemoteTopSQLReporter creates a new TopSQL reporter
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(client ReportClient, planDecodeFn planBinaryDecodeFunc) *RemoteTopSQLReporter {

	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		ctx:                ctx,
		cancel:             cancel,
		client:             client,
		planBinaryDecoder:  planDecodeFn,
		collectCPUDataChan: make(chan cpuData, 1),
		reportDataChan:     make(chan reportData, 1),
	}
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	go tsr.collectWorker()
	go tsr.reportWorker()

	return tsr
}

// RegisterSQL registers a normalized SQL string to a SQL digest.
// This function is thread-safe and efficient.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string) {
	m := tsr.normalizedSQLMap.Load().(*sync.Map)
	key := string(sqlDigest)
	m.LoadOrStore(key, normalizedSQL)
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
// This function is thread-safe and efficient.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedBinaryPlan string) {
	m := tsr.normalizedPlanMap.Load().(*sync.Map)
	key := string(planDigest)
	m.LoadOrStore(key, normalizedBinaryPlan)
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
	}
}

func (tsr *RemoteTopSQLReporter) Close() {
	tsr.cancel()
	tsr.client.Close()
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

// doCollect uses a hashmap to store records in every second, and evict when necessary.
func (tsr *RemoteTopSQLReporter) doCollect(
	collectTarget map[string]*dataPoints, timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	defer util.Recover("top-sql", "doCollect", nil, false)

	keyBuf := bytes.NewBuffer(make([]byte, 0, 64))
	listCapacity := int(variable.TopSQLVariable.ReportIntervalSeconds.Load()/variable.TopSQLVariable.PrecisionSeconds.Load() + 1)
	if listCapacity < 1 {
		listCapacity = 1
	}
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

	// evict records according to `MaxStatementCount` variable.
	// TODO: Better to pass in the variable in the constructor, instead of referencing directly.
	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(collectTarget) <= maxStmt {
		return
	}

	// find the max CPUTimeMsTotal that should be evicted
	digestCPUTimeList := make([]cpuTimeSort, len(collectTarget))
	idx := 0
	for key, value := range collectTarget {
		digestCPUTimeList[idx] = cpuTimeSort{
			Key:            key,
			SQLDigest:      value.SQLDigest,
			PlanDigest:     value.PlanDigest,
			CPUTimeMsTotal: value.CPUTimeMsTotal,
		}
		idx++
	}

	// QuickSelect will only return error when the second parameter is out of range
	if err := quickselect.QuickSelect(cpuTimeSortSlice(digestCPUTimeList), maxStmt); err != nil {
		//	skip eviction
		return
	}

	itemsToEvict := digestCPUTimeList[maxStmt:]
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)
	for _, evict := range itemsToEvict {
		delete(collectTarget, evict.Key)
		normalizedSQLMap.Delete(string(evict.SQLDigest))
		normalizedPlanMap.Delete(string(evict.PlanDigest))
	}
}

// takeDataAndSendToReportChan takes out (resets) collected data. These data will be send to a report channel
// for reporting later.
func (tsr *RemoteTopSQLReporter) takeDataAndSendToReportChan(collectedDataPtr *map[string]*dataPoints) {
	data := reportData{
		collectedData:     *collectedDataPtr,
		normalizedSQLMap:  tsr.normalizedSQLMap.Load().(*sync.Map),
		normalizedPlanMap: tsr.normalizedPlanMap.Load().(*sync.Map),
	}

	// Reset data for next report.
	*collectedDataPtr = make(map[string]*dataPoints)
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	// Send to report channel. When channel is full, data will be dropped.
	select {
	case tsr.reportDataChan <- data:
	default:
	}
}

type reportData struct {
	collectedData     map[string]*dataPoints
	normalizedSQLMap  *sync.Map
	normalizedPlanMap *sync.Map
}

// prepareReportDataForSending prepares the data that need to reported.
func (tsr *RemoteTopSQLReporter) prepareReportDataForSending(data reportData) (sqlMetas []*tipb.SQLMeta, planMetas []*tipb.PlanMeta, records []*tipb.CPUTimeRecord) {
	sqlMetas = make([]*tipb.SQLMeta, 0, len(data.collectedData))
	data.normalizedSQLMap.Range(func(key, value interface{}) bool {
		sqlMetas = append(sqlMetas, &tipb.SQLMeta{
			SqlDigest:     []byte(key.(string)),
			NormalizedSql: value.(string),
		})
		return true
	})

	planMetas = make([]*tipb.PlanMeta, 0, len(data.collectedData))
	data.normalizedPlanMap.Range(func(key, value interface{}) bool {
		planDecoded, err := tsr.planBinaryDecoder(value.(string))
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(err))
			return true
		}
		planMetas = append(planMetas, &tipb.PlanMeta{
			PlanDigest:     []byte(key.(string)),
			NormalizedPlan: planDecoded,
		})
		return true
	})

	records = make([]*tipb.CPUTimeRecord, 0, len(data.collectedData))
	for _, value := range data.collectedData {
		records = append(records, &tipb.CPUTimeRecord{
			TimestampList: value.TimestampList,
			CpuTimeMsList: value.CPUTimeMsList,
			SqlDigest:     value.SQLDigest,
			PlanDigest:    value.PlanDigest,
		})
	}
	return sqlMetas, planMetas, records
}

// reportWorker sends data to the gRPC endpoint from the `reportDataChan` one by one.
func (tsr *RemoteTopSQLReporter) reportWorker() {
	defer util.Recover("top-sql", "reportWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportDataChan:
			// When `reportDataChan` receives something, there could be ongoing `RegisterSQL` and `RegisterPlan` running,
			// who writes to the data structure that `data` contains. So we wait for a little while to ensure that
			// these writes are finished.
			time.Sleep(time.Millisecond * 100)
			tsr.doReport(data)
		case <-tsr.ctx.Done():
			return
		}
	}
}

func (tsr *RemoteTopSQLReporter) doReport(data reportData) {
	defer util.Recover("top-sql", "doReport", nil, false)

	sqlMetas, planMetas, records := tsr.prepareReportDataForSending(data)
	agentAddr := variable.TopSQLVariable.AgentAddress.Load()

	ctx, cancel := context.WithTimeout(tsr.ctx, reportTimeout)
	err := tsr.client.Send(ctx, agentAddr, sqlMetas, planMetas, records)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] client failed to send data", zap.Error(err))
	}
	cancel()
}
