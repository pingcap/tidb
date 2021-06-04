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
	"math"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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

// ReportClient send data to the target server.
type ReportClient interface {
	Send(ctx context.Context, addr string, data []*tipb.CollectCPUTimeRequest) error
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

// cpuTimeSort is used to sort TopSQL records by tocal CPU time
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
	client ReportClient
	// topSQLMap maps `sqlDigest-planDigest` to TopSQLDataPoints
	topSQLMap map[string]*topSQLDataPoints

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap atomic.Value // sync.Map

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap atomic.Value // sync.Map

	reporterNormalizedSQLMap  map[string]string
	reporterNormalizedPlanMap map[string]string
	// calling this can take a while, so should not block critical paths
	planBinaryDecoder planBinaryDecodeFunc

	collectCPUTimeChan chan *topSQLCPUTimeInput
	reportDataChan     chan reportData

	quit chan struct{}
}

// NewRemoteTopSQLReporter creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(client ReportClient, planDecoder planBinaryDecodeFunc) *RemoteTopSQLReporter {
	tsr := &RemoteTopSQLReporter{
		client:                    client,
		topSQLMap:                 make(map[string]*topSQLDataPoints),
		reporterNormalizedSQLMap:  make(map[string]string),
		reporterNormalizedPlanMap: make(map[string]string),
		planBinaryDecoder:         planDecoder,
		collectCPUTimeChan:        make(chan *topSQLCPUTimeInput, collectCPUTimeChanLen),
		reportDataChan:            make(chan reportData, 1),
		quit:                      make(chan struct{}),
	}
	tsr.normalizedSQLMap.Store(&sync.Map{})
	tsr.normalizedPlanMap.Store(&sync.Map{})

	go tsr.collectWorker()

	go tsr.sendToAgentWorker()

	return tsr
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
			tsr.sendToReport()
		case <-tsr.quit:
			return
		}
	}
}

func encodeKey(sqlDigest, planDigest []byte) string {
	var buffer bytes.Buffer
	buffer.Write(sqlDigest)
	buffer.Write(planDigest)
	return buffer.String()
}

// collect uses a hashmap to store records in every second, and evict when necessary.
// This function can be run in parallel with snapshot, so we should protect the map operations with a mutex.
func (tsr *RemoteTopSQLReporter) collect(timestamp uint64, records []tracecpu.SQLCPUTimeRecord) {
	for _, record := range records {
		key := encodeKey(record.SQLDigest, record.PlanDigest)
		entry, exist := tsr.topSQLMap[key]
		if !exist {
			entry = &topSQLDataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
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
	{
		i := 0
		for key, value := range tsr.topSQLMap {
			data := cpuTimeSort{
				Key:            key,
				SQLDigest:      value.SQLDigest,
				PlanDigest:     value.PlanDigest,
				CPUTimeMsTotal: value.CPUTimeMsTotal,
			}
			digestCPUTimeList[i] = data
			i++
		}
	}

	// QuickSelect will only return error when the second parameter is out of range
	if err := quickselect.QuickSelect(cpuTimeSortSlice(digestCPUTimeList), maxStmt); err != nil {
		//	skip eviction
		return
	}

	// TODO: we can change to periodical eviction (every minute) to relax the CPU pressure
	shouldEvictList := digestCPUTimeList[maxStmt:]
	normalizedSQLMap := tsr.normalizedSQLMap.Load().(*sync.Map)
	normalizedPlanMap := tsr.normalizedPlanMap.Load().(*sync.Map)
	for _, evict := range shouldEvictList {
		delete(tsr.topSQLMap, evict.Key)
		normalizedSQLMap.Delete(evict.SQLDigest)
		normalizedPlanMap.Delete(evict.PlanDigest)
	}
}

func (tsr *RemoteTopSQLReporter) sendToReport() {
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

// RegisterSQL registers a normalized SQL string to a SQL digest.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest []byte, normalizedSQL string) {
	m := tsr.normalizedSQLMap.Load().(*sync.Map)
	key := string(sqlDigest)
	_, ok := m.Load(key)
	if ok {
		return
	}
	m.Store(key, normalizedSQL)
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest []byte, normalizedPlan string) {
	m := tsr.normalizedPlanMap.Load().(*sync.Map)
	key := string(planDigest)
	_, ok := m.Load(key)
	if ok {
		return
	}
	m.Store(key, normalizedPlan)
}

// prepareReportData will collect the current snapshot of data for transmission, and clear the records map.
// This could run in parallel with `Collect()`, so we should guard it by a mutex.
//
// NOTE: we could optimize this using a mutex protected pointer-swapping operation,
// which means we maintain 2 *struct which contains the maps, and snapshot() atomically
// swaps the pointer. After this, the writing is shifted to the new struct, and
// we can do the snapshot in the background.
func (tsr *RemoteTopSQLReporter) prepareReportData(data reportData) []*tipb.CollectCPUTimeRequest {
	// wait latest register finish, use sleep instead of other method to avoid performance issue in hot code path.
	time.Sleep(time.Millisecond * 100)
	data.normalizedSQLMap.Range(func(key, value interface{}) bool {
		keyStr := key.(string)
		_, ok := tsr.reporterNormalizedSQLMap[keyStr]
		if !ok {
			tsr.reporterNormalizedSQLMap[keyStr] = value.(string)
		}
		return true
	})
	data.normalizedPlanMap.Range(func(key, value interface{}) bool {
		keyStr := key.(string)
		_, ok := tsr.reporterNormalizedPlanMap[keyStr]
		if ok {
			return true
		}
		planDecoded, err := tsr.planBinaryDecoder(value.(string))
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] decode plan failed", zap.Error(err))
			return true
		}
		tsr.reporterNormalizedPlanMap[keyStr] = planDecoded
		return true
	})

	// TODO: evict redundant record in reporterNormalizedSQLMap and reporterNormalizedPlanMap.

	i := 0
	batch := make([]*tipb.CollectCPUTimeRequest, len(data.topSQL))
	for key, value := range data.topSQL {
		normalizedSQL := tsr.reporterNormalizedSQLMap[string(value.SQLDigest)]
		normalizedPlan := tsr.reporterNormalizedSQLMap[string(value.PlanDigest)]
		batch[i] = &tipb.CollectCPUTimeRequest{
			TimestampList:  value.TimestampList,
			CpuTimeMsList:  value.CPUTimeMsList,
			SqlDigest:      value.SQLDigest,
			NormalizedSql:  normalizedSQL,
			PlanDigest:     value.PlanDigest,
			NormalizedPlan: normalizedPlan,
		}
		delete(tsr.topSQLMap, key)
		i++
	}
	return batch
}

// sendToAgentWorker will send a snapshot to the gRPC endpoint every collect interval
func (tsr *RemoteTopSQLReporter) sendToAgentWorker() {
	defer util.Recover("top-sql", "sendToAgentWorker", nil, false)

	for {
		select {
		case data := <-tsr.reportDataChan:
			batch := tsr.prepareReportData(data)
			tsr.Report(batch)
		case <-tsr.quit:
			return
		}
	}
}

func (tsr *RemoteTopSQLReporter) Report(batch []*tipb.CollectCPUTimeRequest) {
	cli := tsr.client
	if cli == nil {
		return
	}
	addr := variable.TopSQLVariable.AgentAddress.Load()
	ctx, cancel := context.WithTimeout(context.TODO(), reportTimeout)
	err := cli.Send(ctx, addr, batch)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] client failed to send data", zap.Error(err))
	}
	cancel()
}

// ReportGRPCClient reports data to grpc servers.
type ReportGRPCClient struct {
	addr   string
	conn   *grpc.ClientConn
	client tipb.TopSQLAgentClient
}

// NewReportGRPCClient returns a new ReportGRPCClient
func NewReportGRPCClient() *ReportGRPCClient {
	return &ReportGRPCClient{}
}

// Send implements the ReportClient interface.
func (r *ReportGRPCClient) Send(ctx context.Context, addr string, batch []*tipb.CollectCPUTimeRequest) error {
	if addr == "" {
		return nil
	}
	err := r.initialize(addr)
	if err != nil {
		return err
	}
	// TODO: test timeout behavior
	stream, err := r.client.CollectCPUTime(ctx)
	if err != nil {
		return r.resetClientWhenSendError(err)
	}
	if err := r.sendBatch(stream, batch); err != nil {
		return r.resetClientWhenSendError(err)
	}
	return nil
}

// sendBatch sends a batch of TopSQL records streamingly.
// TODO: benchmark test with large amount of data (e.g. 5000), tune with grpc.WithWriteBufferSize()
func (tsr *ReportGRPCClient) sendBatch(stream tipb.TopSQLAgent_CollectCPUTimeClient, batch []*tipb.CollectCPUTimeRequest) error {
	for _, req := range batch {
		if err := stream.Send(req); err != nil {
			return err
		}
	}
	// response is Empty, drop it for now
	_, err := stream.CloseAndRecv()
	return err
}

func (r *ReportGRPCClient) initialize(addr string) (err error) {
	if r.addr == addr {
		return nil
	}
	r.conn, r.client, err = r.newAgentClient(addr)
	if err != nil {
		return err
	}
	r.addr = addr
	return nil
}

func (r *ReportGRPCClient) newAgentClient(addr string) (*grpc.ClientConn, tipb.TopSQLAgentClient, error) {
	dialCtx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	client := tipb.NewTopSQLAgentClient(conn)
	return conn, client, nil
}

func (r *ReportGRPCClient) resetClientWhenSendError(err error) error {
	if err == nil {
		return nil
	}
	r.addr = ""
	r.client = nil
	if r.conn != nil {
		err1 := r.conn.Close()
		if err1 != nil {
			logutil.BgLogger().Warn("[top-sql] close grpc conn failed", zap.Error(err1))
		}
	}
	return err
}
