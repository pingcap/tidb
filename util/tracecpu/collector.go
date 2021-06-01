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
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/util/logutil"
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
	SQLDigest  string
	PlanDigest string
	CPUTimeMs  uint32
}

type planBinaryDecodeFunc func(string) (string, error)

// TopSQLDataPoints represents the cumulative SQL plan CPU time in current minute window
type TopSQLDataPoints struct {
	CPUTimeMsList  []uint32
	TimestampList  []uint64
	CPUTimeMsTotal uint64
}

type digestAndCPUTime struct {
	Key            string
	CPUTimeMsTotal uint64
}
type digestAndCPUTimeSlice []digestAndCPUTime

func (t digestAndCPUTimeSlice) Len() int {
	return len(t)
}

// We need find the kth largest value, so here should use >
func (t digestAndCPUTimeSlice) Less(i, j int) bool {
	return t[i].CPUTimeMsTotal > t[j].CPUTimeMsTotal
}
func (t digestAndCPUTimeSlice) Swap(i, j int) {
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
	topSQLMap map[string]*TopSQLDataPoints
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
	MaxSQLNum           int
	SendToAgentInterval time.Duration
	AgentGRPCAddress    string
	InstanceID          string
}

func encodeCacheKey(sqlDigest, planDigest string) string {
	return sqlDigest + "-" + planDigest
}

func decodeCacheKey(key string) (string, string) {
	split := strings.Split(key, "-")
	sqlDigest := split[0]
	PlanDigest := split[1]
	return sqlDigest, PlanDigest
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
	topSQLMap := make(map[string]*TopSQLDataPoints)

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
		entry, exist := ts.topSQLMap[encodedKey]
		if !exist {
			entry = &TopSQLDataPoints{
				CPUTimeMsList: []uint32{record.CPUTimeMs},
				TimestampList: []uint64{timestamp},
			}
			ts.topSQLMap[encodedKey] = entry
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
	digestCPUTimeList := make([]digestAndCPUTime, len(ts.topSQLMap))
	{
		i := 0
		for key, value := range ts.topSQLMap {
			data := digestAndCPUTime{
				Key:            key,
				CPUTimeMsTotal: value.CPUTimeMsTotal,
			}
			digestCPUTimeList[i] = data
			i++
		}
	}
	// QuickSelect will only return error when the second parameter is out of range
	if err := quickselect.QuickSelect(digestAndCPUTimeSlice(digestCPUTimeList), ts.maxSQLNum); err != nil {
		//	skip eviction
		return
	}
	shouldEvictList := digestCPUTimeList[ts.maxSQLNum:]
	for _, evict := range shouldEvictList {
		delete(ts.topSQLMap, evict.Key)
		sqlDigest, planDigest := decodeCacheKey(evict.Key)
		ts.mu.Lock()
		delete(ts.normalizedSQLMap, sqlDigest)
		delete(ts.normalizedPlanMap, planDigest)
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
func (ts *TopSQLCollector) snapshot() []*tipb.CPUTimeRequestTiDB {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	total := len(ts.topSQLMap)
	batch := make([]*tipb.CPUTimeRequestTiDB, total)
	i := 0
	for key, value := range ts.topSQLMap {
		sqlDigest, planDigest := decodeCacheKey(key)
		normalizedSQL := ts.normalizedSQLMap[sqlDigest]
		normalizedPlan := ts.normalizedPlanMap[planDigest]
		batch[i] = &tipb.CPUTimeRequestTiDB{
			TimestampList:  value.TimestampList,
			CpuTimeMsList:  value.CPUTimeMsList,
			SqlDigest:      sqlDigest,
			NormalizedSql:  normalizedSQL,
			PlanDigest:     planDigest,
			NormalizedPlan: normalizedPlan,
		}
		i++
	}
	return batch
}

func (ts *TopSQLCollector) sendBatch(stream tipb.TopSQLAgent_CollectCPUTimeClient, batch []*tipb.CPUTimeRequestTiDB) error {
	for _, req := range batch {
		if err := stream.Send(req); err != nil {
			logutil.BgLogger().Error("TopSQL: send stream request failed, %v", zap.Error(err))
			return err
		}
	}
	// response is Empty, drop it for now
	_, err := stream.CloseAndRecv()
	if err != nil {
		logutil.BgLogger().Error("TopSQL: receive stream response failed, %v", zap.Error(err))
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
			conn, stream, _, err := newAgentClient(ts.agentGRPCAddress, sendingTimeout)
			if err != nil {
				log.Printf("ERROR: failed to create agent client, %v\n", err)
				continue
			}
			if err := ts.sendBatch(stream, batch); err != nil {
				continue
			}
			if err := conn.Close(); err != nil {
				log.Printf("ERROR: failed to close connection, %v\n", err)
				continue
			}
		case <-ts.quit:
			ticker.Stop()
			return
		}
	}
}
