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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
	collectCPUTimeChanLen     = 1
	planRegisterChanLen       = 1024
)

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Reporter
	RegisterSQL(sqlDigest []byte, normalizedSQL string)
	RegisterPlan(planDigest []byte, normalizedPlan string)
}

type topSQLCPUTimeInput struct {
	timestamp uint64
	records   []tracecpu.TopSQLCPUTimeRecord
}

type planBinaryDecodeFunc func(string) (string, error)

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

// We need find the kth largest value, so here should use >
func (t cpuTimeSortSlice) Less(i, j int) bool {
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t cpuTimeSortSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type planRegisterJob struct {
	planDigest     string
	normalizedPlan string
}

// RemoteTopSQLReporter implements a TopSQL reporter that sends data to a remote agent
// This should be called periodically to collect TopSQL resource usage metrics
type RemoteTopSQLReporter struct {
	mu sync.RWMutex
	// calling this can take a while, so should not block critical paths
	planBinaryDecoder planBinaryDecodeFunc

	// topSQLMap maps `sqlDigest-planDigest` to TopSQLDataPoints
	topSQLMap        map[string]*topSQLDataPoints
	MaxStatementsNum int

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap map[string]string

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap map[string]string

	collectCPUTimeChan chan *topSQLCPUTimeInput
	planRegisterChan   chan *planRegisterJob

	reportInterval   time.Duration
	reportTimeout    time.Duration
	agentGRPCAddress struct {
		mu      sync.Mutex
		address string
	}

	quit chan struct{}
}

// RemoteTopSQLReporterConfig is the config for RemoteTopSQLReporter
type RemoteTopSQLReporterConfig struct {
	// PlanBinaryDecoder is used to decode the plan binary before sending to agent
	PlanBinaryDecoder planBinaryDecodeFunc
	// MaxStatementsNum is the capacity of the TopSQL map, above which evition should happen
	MaxStatementsNum int
	// ReportInterval is the interval of sending TopSQL records to the agent
	ReportInterval time.Duration
	// ReportTimeout is the timeout of a single agent gRPC call
	ReportTimeout time.Duration
	// AgentGRPCAddress is the gRPC address of the agent server
	AgentGRPCAddress string
}

func encodeCacheKey(sqlDigest, planDigest []byte) []byte {
	var buffer bytes.Buffer
	buffer.Write(sqlDigest)
	buffer.Write(planDigest)
	return buffer.Bytes()
}

func newAgentClient(addr string) (*grpc.ClientConn, tipb.TopSQLAgentClient, error) {
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
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

// NewRemoteTopSQLReporter creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewRemoteTopSQLReporter(config *RemoteTopSQLReporterConfig) *RemoteTopSQLReporter {
	reportTimeout := config.ReportTimeout
	if reportTimeout > config.ReportInterval || reportTimeout <= 0 {
		reportTimeout = config.ReportInterval / 2
	}
	tsr := &RemoteTopSQLReporter{
		planBinaryDecoder:  config.PlanBinaryDecoder,
		MaxStatementsNum:   config.MaxStatementsNum,
		reportInterval:     config.ReportInterval,
		reportTimeout:      reportTimeout,
		topSQLMap:          make(map[string]*topSQLDataPoints),
		normalizedSQLMap:   make(map[string]string),
		normalizedPlanMap:  make(map[string]string),
		collectCPUTimeChan: make(chan *topSQLCPUTimeInput, collectCPUTimeChanLen),
		planRegisterChan:   make(chan *planRegisterJob, planRegisterChanLen),
		agentGRPCAddress: struct {
			mu      sync.Mutex
			address string
		}{
			address: config.AgentGRPCAddress,
		},
		quit: make(chan struct{}),
	}

	go tsr.collectWorker()

	go tsr.registerPlanWorker()

	go tsr.sendToAgentWorker()

	return tsr
}

// Collect will drop the records when the collect channel is full
// TODO: test the dropping behavior
func (tsr *RemoteTopSQLReporter) Collect(timestamp uint64, records []tracecpu.TopSQLCPUTimeRecord) {
	select {
	case tsr.collectCPUTimeChan <- &topSQLCPUTimeInput{
		timestamp: timestamp,
		records:   records,
	}:
	default:
	}
}

func (tsr *RemoteTopSQLReporter) collectWorker() {
	for {
		input := <-tsr.collectCPUTimeChan
		tsr.collect(input.timestamp, input.records)
	}
}

// collect uses a hashmap to store records in every second, and evict when necessary.
// This function can be run in parallel with snapshot, so we should protect the map operations with a mutex.
func (tsr *RemoteTopSQLReporter) collect(timestamp uint64, records []tracecpu.TopSQLCPUTimeRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		entry, exist := tsr.topSQLMap[string(encodedKey)]
		if !exist {
			entry = &topSQLDataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			tsr.topSQLMap[string(encodedKey)] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	if len(tsr.topSQLMap) <= tsr.MaxStatementsNum {
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
	if err := quickselect.QuickSelect(cpuTimeSortSlice(digestCPUTimeList), tsr.MaxStatementsNum); err != nil {
		//	skip eviction
		return

	}

	// TODO: we can change to periodical eviction (every minute) to relax the CPU pressure
	shouldEvictList := digestCPUTimeList[tsr.MaxStatementsNum:]
	for _, evict := range shouldEvictList {
		delete(tsr.topSQLMap, evict.Key)
		tsr.mu.Lock()
		delete(tsr.normalizedSQLMap, string(evict.SQLDigest))
		delete(tsr.normalizedPlanMap, string(evict.PlanDigest))
		tsr.mu.Unlock()
	}
}

// RegisterSQL registers a normalized SQL string to a SQL digest.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (tsr *RemoteTopSQLReporter) RegisterSQL(sqlDigest string, normalizedSQL string) {
	tsr.mu.RLock()
	_, exist := tsr.normalizedSQLMap[sqlDigest]
	tsr.mu.RUnlock()
	if !exist {
		tsr.mu.Lock()
		tsr.normalizedSQLMap[sqlDigest] = normalizedSQL
		tsr.mu.Unlock()
	}
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (tsr *RemoteTopSQLReporter) RegisterPlan(planDigest string, normalizedPlan string) {
	tsr.planRegisterChan <- &planRegisterJob{
		planDigest:     planDigest,
		normalizedPlan: normalizedPlan,
	}
}

// this should be the only place where the normalizedPlanMap is set
func (tsr *RemoteTopSQLReporter) registerPlanWorker() {
	// NOTE: if use multiple worker goroutine, we should make sure that access to the digest map is thread-safe
	for {
		job := <-tsr.planRegisterChan
		tsr.mu.RLock()
		_, exist := tsr.normalizedPlanMap[job.planDigest]
		tsr.mu.RUnlock()
		if exist {
			continue
		}
		planDecoded, err := tsr.planBinaryDecoder(job.normalizedPlan)
		if err != nil {
			log.Warn("decode plan failed: %v\n", zap.Error(err))
			continue
		}
		tsr.normalizedPlanMap[job.planDigest] = planDecoded
	}
}

// snapshot will collect the current snapshot of data for transmission
// This could run in parallel with `Collect()`, so we should guard it by a mutex.
//
// NOTE: we could optimize this using a mutex protected pointer-swapping operation,
// which means we maintain 2 *struct which contains the maps, and snapshot() atomically
// swaps the pointer. After this, the writing is shifted to the new struct, and
// we can do the snapshot in the background.
func (tsr *RemoteTopSQLReporter) snapshot() []*tipb.CollectCPUTimeRequest {
	tsr.mu.RLock()
	defer tsr.mu.RUnlock()
	total := len(tsr.topSQLMap)
	batch := make([]*tipb.CollectCPUTimeRequest, total)
	i := 0
	for _, value := range tsr.topSQLMap {
		normalizedSQL := tsr.normalizedSQLMap[string(value.SQLDigest)]
		normalizedPlan := tsr.normalizedPlanMap[string(value.PlanDigest)]
		batch[i] = &tipb.CollectCPUTimeRequest{
			TimestampList:  value.TimestampList,
			CpuTimeMsList:  value.CPUTimeMsList,
			SqlDigest:      value.SQLDigest,
			NormalizedSql:  normalizedSQL,
			PlanDigest:     value.PlanDigest,
			NormalizedPlan: normalizedPlan,
		}
		i++
	}
	return batch
}

// sendBatch sends a batch of TopSQL records streamingly.
// TODO: benchmark test with large amount of data (e.g. 5000), tune with grpc.WithWriteBufferSize()
func (tsr *RemoteTopSQLReporter) sendBatch(stream tipb.TopSQLAgent_CollectCPUTimeClient, batch []*tipb.CollectCPUTimeRequest) error {
	for _, req := range batch {
		if err := stream.Send(req); err != nil {
			log.Error("TopSQL: send stream request failed, %v", zap.Error(err))
			return err
		}
	}
	// response is Empty, drop it for now
	_, err := stream.CloseAndRecv()
	if err != nil {
		log.Error("TopSQL: receive stream response failed, %v", zap.Error(err))
		return err
	}
	return nil
}

// sendToAgentWorker will send a snapshot to the gRPC endpoint every collect interval
func (tsr *RemoteTopSQLReporter) sendToAgentWorker() {
	ticker := time.NewTicker(tsr.reportInterval)
	for {
		var ctx context.Context
		var cancel context.CancelFunc
		select {
		case <-ticker.C:
			batch := tsr.snapshot()
			// NOTE: Currently we are creating/destroying a TCP connection to the agent every time.
			// It's fine if we do this every minute, but need optimization if we need to do it more frequently, like every second.
			conn, client, err := newAgentClient(tsr.agentGRPCAddress.address)
			if err != nil {
				log.Error("TopSQL: failed to create agent client, %v", zap.Error(err))
				continue
			}
			ctx, cancel = context.WithTimeout(context.TODO(), tsr.reportTimeout)
			stream, err := client.CollectCPUTime(ctx)
			if err != nil {
				log.Error("TopSQL: failed to initialize gRPC call CollectCPUTime, %v", zap.Error(err))
				continue
			}
			if err := tsr.sendBatch(stream, batch); err != nil {
				continue
			}
			cancel()
			if err := conn.Close(); err != nil {
				log.Error("TopSQL: failed to close connection, %v", zap.Error(err))
				continue
			}
		case <-tsr.quit:
			ticker.Stop()
			break
		}
	}
}

// SetAgentGRPCAddress sets the agentGRPCAddress field
func (tsr *RemoteTopSQLReporter) SetAgentGRPCAddress(address string) {
	tsr.agentGRPCAddress.mu.Lock()
	defer tsr.agentGRPCAddress.mu.Unlock()
	tsr.agentGRPCAddress.address = address
}
