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

package tracecpu

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TopSQLRecord represents a single record of how much cpu time a sql plan consumes in one second.
//
// PlanDigest can be empty, because:
// 1. some sql statements has no plan, like `COMMIT`
// 2. when a sql statement is being compiled, there's no plan yet
type TopSQLRecord struct {
	SQLDigest  []byte
	PlanDigest []byte
	CPUTimeMs  uint32
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

// TopSQLCollector is called periodically to collect TopSQL resource usage metrics
type TopSQLCollector struct {
	// // calling this can take a while, so should not block critical paths
	// planBinaryDecoder planBinaryDecodeFunc
	mu sync.RWMutex

	// topSQLMap maps `sqlDigest-planDigest` to TopSQLDataPoints
	topSQLMap map[string]*topSQLDataPoints
	maxSQLNum int

	// normalizedSQLMap is an map, whose keys are SQL digest strings and values are normalized SQL strings
	normalizedSQLMap map[string]string

	// normalizedPlanMap is a plan version of normalizedSQLMap
	// this should only be set from the dedicated worker
	normalizedPlanMap map[string]string

	planRegisterChan chan *planRegisterJob

	// current tidb-server instance ID
	instanceID string

	agentGRPCAddress string

	quit chan struct{}
}

// TopSQLCollectorConfig is the config for TopSQLCollector
type TopSQLCollectorConfig struct {
	PlanBinaryDecoder   planBinaryDecodeFunc
	MaxStatementsNum           int
	CollectInterval time.Duration
	AgentGRPCAddress    string
	InstanceID          string
}

func encodeCacheKey(sqlDigest, planDigest []byte) []byte {
	var buffer bytes.Buffer
	buffer.Write(sqlDigest)
	buffer.Write(planDigest)
	return buffer.Bytes()
}

func newAgentClient(addr string, sendingTimeout time.Duration) (*grpc.ClientConn, tipb.TopSQLAgent_CollectCPUTimeClient, context.CancelFunc, error) {
	dialCtx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, nil, err
	}
	client := tipb.NewTopSQLAgentClient(conn)
	ctx, cancel := context.WithTimeout(context.TODO(), sendingTimeout)
	stream, err := client.CollectCPUTime(ctx)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	return conn, stream, cancel, nil
}

// NewTopSQLCollector creates a new TopSQL struct
//
// planBinaryDecoder is a decoding function which will be called asynchronously to decode the plan binary to string
// maxSQLNum is the maximum SQL and plan number, which will restrict the memory usage of the internal LFU cache
func NewTopSQLCollector(config *TopSQLCollectorConfig) *TopSQLCollector {
	normalizedSQLMap := make(map[string]string)
	normalizedPlanMap := make(map[string]string)
	planRegisterChan := make(chan *planRegisterJob, 10)
	topSQLMap := make(map[string]*topSQLDataPoints)

	ts := &TopSQLCollector{
		topSQLMap:         topSQLMap,
		maxSQLNum:         config.MaxSQLNum,
		normalizedSQLMap:  normalizedSQLMap,
		normalizedPlanMap: normalizedPlanMap,
		planRegisterChan:  planRegisterChan,
		agentGRPCAddress:  config.AgentGRPCAddress,
		instanceID:        config.InstanceID,
		quit:              make(chan struct{}),
	}

	go ts.registerNormalizedPlanWorker(config.PlanBinaryDecoder)

	go ts.sendToAgentWorker(config.SendToAgentInterval)

	return ts
}

// Collect uses a hashmap to store records in every minute, and evict every minute.
// This function can be run in parallel with snapshot, so we should protect the map operations with a mutex.
func (ts *TopSQLCollector) Collect(timestamp uint64, records []TopSQLRecord) {
	for _, record := range records {
		encodedKey := encodeCacheKey(record.SQLDigest, record.PlanDigest)
		entry, exist := ts.topSQLMap[string(encodedKey)]
		if !exist {
			entry = &topSQLDataPoints{
				SQLDigest:     record.SQLDigest,
				PlanDigest:    record.PlanDigest,
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			ts.topSQLMap[string(encodedKey)] = entry
		} else {
			entry.CPUTimeMsList = append(entry.CPUTimeMsList, record.CPUTimeMs)
			entry.TimestampList = append(entry.TimestampList, timestamp)
		}
		entry.CPUTimeMsTotal += uint64(record.CPUTimeMs)
	}

	if len(ts.topSQLMap) <= ts.maxSQLNum {
		return
	}

	// find the max CPUTimeMsTotal that should be evicted
	digestCPUTimeList := make([]cpuTimeSort, len(ts.topSQLMap))
	{
		i := 0
		for key, value := range ts.topSQLMap {
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
	if err := quickselect.QuickSelect(cpuTimeSortSlice(digestCPUTimeList), ts.maxSQLNum); err != nil {
		//	skip eviction
		return
	}
	shouldEvictList := digestCPUTimeList[ts.maxSQLNum:]
	for _, evict := range shouldEvictList {
		delete(ts.topSQLMap, evict.Key)
		ts.mu.Lock()
		delete(ts.normalizedSQLMap, string(evict.SQLDigest))
		delete(ts.normalizedPlanMap, string(evict.PlanDigest))
		ts.mu.Unlock()
	}
}

// RegisterNormalizedSQL registers a normalized sql string to a sql digest, while the former can be of >1M long.
// The in-memory space for registered normalized sql are limited by TopSQL.normalizedSQLCapacity.
//
// This function should be thread-safe, which means parallelly calling it in several goroutines should be fine.
// It should also return immediately, and do any CPU-intensive job asynchronously.
// TODO: benchmark test concurrent performance
func (ts *TopSQLCollector) RegisterNormalizedSQL(sqlDigest string, normalizedSQL string) {
	ts.mu.RLock()
	_, exist := ts.normalizedSQLMap[sqlDigest]
	ts.mu.RUnlock()
	if !exist {
		ts.mu.Lock()
		ts.normalizedSQLMap[sqlDigest] = normalizedSQL
		ts.mu.Unlock()
	}
}

// RegisterNormalizedPlan is like RegisterNormalizedSQL, but for normalized plan strings.
// TODO: benchmark test concurrent performance
func (ts *TopSQLCollector) RegisterNormalizedPlan(planDigest string, normalizedPlan string) {
	ts.planRegisterChan <- &planRegisterJob{
		planDigest:     planDigest,
		normalizedPlan: normalizedPlan,
	}
}

// this should be the only place where the normalizedPlanMap is set
func (ts *TopSQLCollector) registerNormalizedPlanWorker(planDecoder planBinaryDecodeFunc) {
	// NOTE: if use multiple worker goroutine, we should make sure that access to the digest map is thread-safe
	for {
		job := <-ts.planRegisterChan
		ts.mu.RLock()
		_, exist := ts.normalizedPlanMap[job.planDigest]
		ts.mu.RUnlock()
		if exist {
			continue
		}
		planDecoded, err := planDecoder(job.normalizedPlan)
		if err != nil {
			fmt.Printf("decode plan failed: %v\n", err)
			continue
		}
		ts.normalizedPlanMap[job.planDigest] = planDecoded
	}
}

// snapshot will collect the current snapshot of data for transmission
// This could run in parallel with `Collect()`, so we should guard it by a mutex.
//
// NOTE: we could optimize this using a mutex protected pointer-swapping operation,
// which means we maintain 2 *struct which contains the maps, and snapshot() atomically
// swaps the pointer. After this, the writing is shifted to the new struct, and
// we can do the snapshot in the background.
func (ts *TopSQLCollector) snapshot() []*tipb.CollectCPUTimeRequest {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	total := len(ts.topSQLMap)
	batch := make([]*tipb.CollectCPUTimeRequest, total)
	i := 0
	for _, value := range ts.topSQLMap {
		normalizedSQL := ts.normalizedSQLMap[string(value.SQLDigest)]
		normalizedPlan := ts.normalizedPlanMap[string(value.PlanDigest)]
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

func (ts *TopSQLCollector) sendBatch(stream tipb.TopSQLAgent_CollectCPUTimeClient, batch []*tipb.CollectCPUTimeRequest) error {
	for _, req := range batch {
		// TODO: look into the gRPC configuration for batching request, for potential better performance
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

// sendToAgentWorker will send a snapshot to the gRPC endpoint every interval
func (ts *TopSQLCollector) sendToAgentWorker(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			batch := ts.snapshot()
			sendingTimeout := interval - 10*time.Second
			if sendingTimeout < 0 {
				sendingTimeout = interval / 2
			}
			// NOTE: Currently we are creating/destroying a TCP connection to the agent every time.
			// It's fine if we do this every minute, but need optimization if we need to do it more frequently, like every second.
			conn, stream, _, err := newAgentClient(ts.agentGRPCAddress, sendingTimeout)
			if err != nil {
				log.Error("ERROR: failed to create agent client, %v\n", zap.Error(err))
				continue
			}
			if err := ts.sendBatch(stream, batch); err != nil {
				continue
			}
			if err := conn.Close(); err != nil {
				log.Error("ERROR: failed to close connection, %v\n", zap.Error(err))
				continue
			}
		case <-ts.quit:
			ticker.Stop()
			return
		}
	}
}
