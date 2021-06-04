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
}

type topSQLCPUTimeInput struct {
	timestamp uint64
	records   []tracecpu.SQLCPUTimeRecord
}

// topSQLDataPoints represents the cumulative SQL plan CPU time in current minute window
type topSQLDataPoints struct {
	SQLDigest      []byte
	PlanDigest     []byte
	CPUTimeMsList  []uint32
	TimestampList  []uint64
	CPUTimeMsTotal uint64
}

// cpuTimeSort is used to sort TopSQL records by total CPU time
type cpuTimeSort struct {
	Key            string
	SQLDigest      []byte
	PlanDigest     []byte
	CPUTimeMsTotal uint64
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

	// topSQLMap maps `sqlDigest-planDigest` to TopSQLDataPoints
	topSQLMap map[string]*topSQLDataPoints

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap atomic.Value // sync.Map

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap atomic.Value // sync.Map

	// calling this can take a while, so should not block critical paths
	planBinaryDecoder planBinaryDecodeFunc

	collectCPUTimeChan chan *topSQLCPUTimeInput
	reportDataChan     chan reportData
}

// NewRemoteTopSQLReporter creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(client ReportClient, planDecoder planBinaryDecodeFunc) *RemoteTopSQLReporter {
	ctx, cancel := context.WithCancel(context.Background())
	tsr := &RemoteTopSQLReporter{
		ctx:                ctx,
		cancel:             cancel,
		client:             client,
		topSQLMap:          make(map[string]*topSQLDataPoints),
		planBinaryDecoder:  planDecoder,
		collectCPUTimeChan: make(chan *topSQLCPUTimeInput, collectCPUTimeChanLen),
		reportDataChan:     make(chan reportData, 1),
	}
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	go tsr.collectWorker()

	go tsr.reportWorker()

	return tsr
}

// RegisterSQL registers a normalized SQL string to a SQL digest.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string) {
	m := tsr.normalizedSQLMap.Load().(*sync.Map)
	key := string(sqlDigest)
	m.LoadOrStore(key, normalizedSQL)
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedPlan string) {
	m := tsr.normalizedPlanMap.Load().(*sync.Map)
	key := string(planDigest)
	m.LoadOrStore(key, normalizedPlan)
}

// Collect will drop the records when the collect channel is full
// TODO: test the dropping behavior
func (tsr *RemoteTopSQLReporter) Collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	select {
	case tsr.collectCPUTimeChan <- &topSQLCPUTimeInput{
		timestamp: timestamp,
		records:   records,
	}:
	default:
		// ignore if chan blocked
	}
}

func (tsr *RemoteTopSQLReporter) collectWorker() {
	defer util.Recover("top-sql", "collectWorker", nil, false)

	interval := variable.TopSQLVariable.ReportIntervalSeconds.Load()
	reportTicker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		select {
		case input := <-tsr.collectCPUTimeChan:
			tsr.collect(input.timestamp, input.records)
		case <-reportTicker.C:
			// Update report ticker if report interval changed.
			if newInterval := variable.TopSQLVariable.ReportIntervalSeconds.Load(); newInterval != interval {
				interval = newInterval
				reportTicker.Reset(time.Second * time.Duration(interval))
			}
			tsr.sendToReportChan()
		case <-tsr.ctx.Done():
			return
		}
	}
}

func encodeKey(buf *bytes.Buffer, sqlDigest, planDigest []byte) string {
	buf.Write(sqlDigest)
	buf.Write(planDigest)
	return buf.String()
}

// collect uses a hashmap to store records in every second, and evict when necessary.
func (tsr *RemoteTopSQLReporter) collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	// capacity uses to pre-allocate memory
	capacity := int(variable.TopSQLVariable.ReportIntervalSeconds.Load() / variable.TopSQLVariable.PrecisionSeconds.Load() / 2)
	if capacity < 1 {
		capacity = 1
	}
	for _, record := range records {
		buf.Reset()
		key := encodeKey(buf, record.SQLDigest, record.PlanDigest)
		entry, exist := tsr.topSQLMap[key]
		if !exist {
			entry = &topSQLDataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: make([]uint32, 1, capacity),
				TimestampList: make([]uint64, 1, capacity),
			}
			entry.CPUTimeMsList[0] = record.CPUTimeMs
			entry.TimestampList[0] = timestamp
			tsr.topSQLMap[key] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	maxStmt := int(variable.TopSQLVariable.MaxStatementCount.Load())
	if len(tsr.topSQLMap) <= maxStmt {
		return
	}

	// find the max CPUTimeMsTotal that should be evicted
	digestCPUTimeList := make([]cpuTimeSort, len(tsr.topSQLMap))
	idx := 0
	for key, value := range tsr.topSQLMap {
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

	shouldEvictList := digestCPUTimeList[maxStmt:]
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)
	for _, evict := range shouldEvictList {
		delete(tsr.topSQLMap, evict.Key)
		normalizedSQLMap.Delete(evict.SQLDigest)
		normalizedPlanMap.Delete(evict.PlanDigest)
	}
}

func (tsr *RemoteTopSQLReporter) sendToReportChan() {
	data := reportData{
		topSQL:            tsr.topSQLMap,
		normalizedSQLMap:  tsr.normalizedSQLMap.Load().(*sync.Map),
		normalizedPlanMap: tsr.normalizedPlanMap.Load().(*sync.Map),
	}

	// Reset data for next report.
	tsr.topSQLMap = make(map[string]*topSQLDataPoints)
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	// Send to report channel
	select {
	case tsr.reportDataChan <- data:
	default:
	}
}

type reportData struct {
	topSQL            map[string]*topSQLDataPoints
	normalizedSQLMap  *sync.Map
	normalizedPlanMap *sync.Map
}

// prepareReportData prepares the data that need to reported.
func (tsr *RemoteTopSQLReporter) prepareReportData(data reportData) (sqlMetas []*tipb.SQLMeta, planMetas []*tipb.PlanMeta, records []*tipb.CPUTimeRecord) {
	// wait latest register finish, use sleep instead of other method to avoid performance issue in hot code path.
	time.Sleep(time.Millisecond * 100)

	sqlMetas = make([]*tipb.SQLMeta, len(data.topSQL))
	idx := 0
	data.normalizedSQLMap.Range(func(key, value interface{}) bool {
		sqlMetas[idx] = &tipb.SQLMeta{
			SqlDigest:     []byte(key.(string)),
			NormalizedSql: value.(string),
		}
		idx++
		return true
	})

	planMetas = make([]*tipb.PlanMeta, len(data.topSQL))
	idx = 0
	data.normalizedPlanMap.Range(func(key, value interface{}) bool {
		planDecoded, err := tsr.planBinaryDecoder(value.(string))
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(err))
			return true
		}
		planMetas[idx] = &tipb.PlanMeta{
			PlanDigest:     []byte(key.(string)),
			NormalizedPlan: planDecoded,
		}
		idx++
		return true
	})

	idx = 0
	records = make([]*tipb.CPUTimeRecord, len(data.topSQL))
	for _, value := range data.topSQL {
		req := &tipb.CPUTimeRecord{
			TimestampList: value.TimestampList,
			CpuTimeMsList: value.CPUTimeMsList,
			SqlDigest:     value.SQLDigest,
			PlanDigest:    value.PlanDigest,
		}
		records[idx] = req
		idx++
	}
	return sqlMetas, planMetas, records
}

// sendToAgentWorker will send a snapshot to the gRPC endpoint every collect interval
func (tsr *RemoteTopSQLReporter) reportWorker() {
	defer util.Recover("top-sql", "reportWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportDataChan:
			tsr.report(data)
		case <-tsr.ctx.Done():
			return
		}
	}
}

func (tsr *RemoteTopSQLReporter) report(data reportData) {
	sqlMetas, planMetas, records := tsr.prepareReportData(data)
	addr := variable.TopSQLVariable.AgentAddress.Load()

	ctx, cancel := context.WithTimeout(tsr.ctx, reportTimeout)
	err := tsr.client.Send(ctx, addr, sqlMetas, planMetas, records)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] client failed to send data", zap.Error(err))
	}
	cancel()
}
