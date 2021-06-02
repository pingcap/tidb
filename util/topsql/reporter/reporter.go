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
	"fmt"
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

// TopSQLReporter collects Top SQL metrics.
type TopSQLReporter interface {
	tracecpu.Reporter
	RegisterSQL(sqlDigest []byte, normalizedSQL string)
	RegisterPlan(planDigest []byte, normalizedPlan string)
}

// TopSQLCPUTimeRecord represents a single record of how much cpu time a sql plan consumes in one second.
//
// PlanDigest can be empty, because:
// 1. some sql statements has no plan, like `COMMIT`
// 2. when a sql statement is being compiled, there's no plan yet
type TopSQLCPUTimeRecord struct {
	SQLDigest  []byte
	PlanDigest []byte
	CPUTimeMs  uint32
}

type topSQLCPUTimeInput struct {
	timestamp uint64
	records   []TopSQLCPUTimeRecord
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

// TopSQLReporterImpl is called periodically to collect TopSQL resource usage metrics
type TopSQLReporterImpl struct {
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

	collectInterval  time.Duration
	collectTimeout   time.Duration
	agentGRPCAddress string

	// current tidb-server instance ID
	instanceID string

	quit chan struct{}
}

// TopSQLReporterConfig is the config for TopSQLReporterImpl
type TopSQLReporterConfig struct {
	// PlanBinaryDecoder is used to decode the plan binary before sending to agent
	PlanBinaryDecoder planBinaryDecodeFunc
	// MaxStatementsNum is the capacity of the TopSQL map, above which evition should happen
	MaxStatementsNum int
	// CollectInterval is the interval of sending TopSQL records to the agent
	CollectInterval time.Duration
	// CollectTimeout is the timeout of a single agent gRPC call
	CollectTimeout time.Duration
	// AgentGRPCAddress is the gRPC address of the agent server
	AgentGRPCAddress string
	// InstanceID is the ID of this tidb server
	InstanceID string
}

func encodeCacheKey(sqlDigest, planDigest []byte) []byte {
	var buffer bytes.Buffer
	buffer.Write(sqlDigest)
	buffer.Write(planDigest)
	return buffer.Bytes()
}

const (
	GrpcInitialWindowSize     = 1 << 30
	GrpcInitialConnWindowSize = 1 << 30
)

func newAgentClient(addr string) (*grpc.ClientConn, tipb.TopSQLAgentClient, error) {
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		addr,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(GrpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(GrpcInitialConnWindowSize),
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

// NewTopSQLReporter creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// MaxStatementsNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewTopSQLReporter(config *TopSQLReporterConfig) *TopSQLReporterImpl {
	collectTimeout := config.CollectTimeout
	if collectTimeout > config.CollectInterval || collectTimeout <= 0 {
		collectTimeout = config.CollectInterval / 2
	}
	tsc := &TopSQLReporterImpl{
		planBinaryDecoder:  config.PlanBinaryDecoder,
		MaxStatementsNum:   config.MaxStatementsNum,
		collectInterval:    config.CollectInterval,
		collectTimeout:     collectTimeout,
		topSQLMap:          make(map[string]*topSQLDataPoints),
		normalizedSQLMap:   make(map[string]string),
		normalizedPlanMap:  make(map[string]string),
		collectCPUTimeChan: make(chan *topSQLCPUTimeInput, 1),
		planRegisterChan:   make(chan *planRegisterJob, 10),
		agentGRPCAddress:   config.AgentGRPCAddress,
		instanceID:         config.InstanceID,
		quit:               make(chan struct{}),
	}

	go tsc.collectWorker()

	go tsc.registerPlanWorker()

	go tsc.sendToAgentWorker()

	return tsc
}

// Collect will drop the records when the collect channel is full
func (tsc *TopSQLReporterImpl) Collect(timestamp uint64, records []TopSQLCPUTimeRecord) {
	select {
	case tsc.collectCPUTimeChan <- &topSQLCPUTimeInput{
		timestamp: timestamp,
		records:   records,
	}:
	default:
	}
}

func (tsc *TopSQLReporterImpl) collectWorker() {
	for {
		input := <-tsc.collectCPUTimeChan
		tsc.collect(input.timestamp, input.records)
	}
}

// collect uses a hashmap to store records in every second, and evict when necessary.
// This function can be run in parallel with snapshot, so we should protect the map operations with a mutex.
func (tsc *TopSQLReporterImpl) collect(timestamp uint64, records []TopSQLCPUTimeRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		entry, exist := tsc.topSQLMap[string(encodedKey)]
		if !exist {
			entry = &topSQLDataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			tsc.topSQLMap[string(encodedKey)] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	if len(tsc.topSQLMap) <= tsc.MaxStatementsNum {
		return
	}

	// find the max CPUTimeMsTotal that should be evicted
	digestCPUTimeList := make([]cpuTimeSort, len(tsc.topSQLMap))
	{
		i := 0
		for key, value := range tsc.topSQLMap {
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
	if err := quickselect.QuickSelect(cpuTimeSortSlice(digestCPUTimeList), tsc.MaxStatementsNum); err != nil {
		//	skip eviction
		return

	}

	// TODO: we can change to periodical eviction (every minute) to relax the CPU pressure
	shouldEvictList := digestCPUTimeList[tsc.MaxStatementsNum:]
	for _, evict := range shouldEvictList {
		delete(tsc.topSQLMap, evict.Key)
		tsc.mu.Lock()
		delete(tsc.normalizedSQLMap, string(evict.SQLDigest))
		delete(tsc.normalizedPlanMap, string(evict.PlanDigest))
		tsc.mu.Unlock()
	}
}

// RegisterSQL registers a normalized SQL string to a SQL digest.
//
// Note that the normalized SQL string can be of >1M long.
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (tsc *TopSQLReporterImpl) RegisterSQL(sqlDigest string, normalizedSQL string) {
	tsc.mu.RLock()
	_, exist := tsc.normalizedSQLMap[sqlDigest]
	tsc.mu.RUnlock()
	if !exist {
		tsc.mu.Lock()
		tsc.normalizedSQLMap[sqlDigest] = normalizedSQL
		tsc.mu.Unlock()
	}
}

// RegisterPlan is like RegisterSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (tsc *TopSQLReporterImpl) RegisterPlan(planDigest string, normalizedPlan string) {
	tsc.planRegisterChan <- &planRegisterJob{
		planDigest:     planDigest,
		normalizedPlan: normalizedPlan,
	}
}

// this should be the only place where the normalizedPlanMap is set
func (tsc *TopSQLReporterImpl) registerPlanWorker() {
	// NOTE: if use multiple worker goroutine, we should make sure that access to the digest map is thread-safe
	for {
		job := <-tsc.planRegisterChan
		tsc.mu.RLock()
		_, exist := tsc.normalizedPlanMap[job.planDigest]
		tsc.mu.RUnlock()
		if exist {
			continue
		}
		planDecoded, err := tsc.planBinaryDecoder(job.normalizedPlan)
		if err != nil {
			fmt.Printf("decode plan failed: %v\n", err)
			continue
		}
		tsc.normalizedPlanMap[job.planDigest] = planDecoded
	}
}

// snapshot will collect the current snapshot of data for transmission
// This could run in parallel with `Collect()`, so we should guard it by a mutex.
//
// NOTE: we could optimize this using a mutex protected pointer-swapping operation,
// which means we maintain 2 *struct which contains the maps, and snapshot() atomically
// swaps the pointer. After this, the writing is shifted to the new struct, and
// we can do the snapshot in the background.
func (tsc *TopSQLReporterImpl) snapshot() []*tipb.CollectCPUTimeRequest {
	tsc.mu.RLock()
	defer tsc.mu.RUnlock()
	total := len(tsc.topSQLMap)
	batch := make([]*tipb.CollectCPUTimeRequest, total)
	i := 0
	for _, value := range tsc.topSQLMap {
		normalizedSQL := tsc.normalizedSQLMap[string(value.SQLDigest)]
		normalizedPlan := tsc.normalizedPlanMap[string(value.PlanDigest)]
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
func (tsc *TopSQLReporterImpl) sendBatch(stream tipb.TopSQLAgent_CollectCPUTimeClient, batch []*tipb.CollectCPUTimeRequest) error {
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
func (tsc *TopSQLReporterImpl) sendToAgentWorker() {
	ticker := time.NewTicker(tsc.collectInterval)
	for {
		var ctx context.Context
		var cancel context.CancelFunc
		select {
		case <-ticker.C:
			batch := tsc.snapshot()
			// NOTE: Currently we are creating/destroying a TCP connection to the agent every time.
			// It's fine if we do this every minute, but need optimization if we need to do it more frequently, like every second.
			conn, client, err := newAgentClient(tsc.agentGRPCAddress)
			if err != nil {
				log.Error("TopSQL: failed to create agent client, %v", zap.Error(err))
				continue
			}
			ctx, cancel = context.WithTimeout(context.TODO(), tsc.collectTimeout)
			stream, err := client.CollectCPUTime(ctx)
			if err != nil {
				log.Error("TopSQL: failed to initialize gRPC call CollectCPUTime, %v", zap.Error(err))
				continue
			}
			if err := tsc.sendBatch(stream, batch); err != nil {
				continue
			}
			cancel()
			if err := conn.Close(); err != nil {
				log.Error("TopSQL: failed to close connection, %v", zap.Error(err))
				continue
			}
		case <-tsc.quit:
			ticker.Stop()
			break
		}
	}
}

// SetAgentGRPCAddress sets the agentGRPCAddress field
func (tsc *TopSQLReporterImpl) SetAgentGRPCAddress(address string) {
	tsc.agentGRPCAddress = address
}
