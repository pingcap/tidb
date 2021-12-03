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

package execcount

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/atomic"
)

const (
	// execCounterManagerCollectDuration is the time period for execCounterManager
	// to collect data from all ExecCounter s.
	execCounterManagerCollectDuration = 3 * time.Second

	// execCounterManagerUploadDuration is the time period for execCounterManager
	// to report all aggregated data.
	execCounterManagerUploadDuration = 30 * time.Second
)

// ExecCountMap represents Map<SQLDigest, Map<Timestamp, Count>>.
// We put SQLDigest in front of the two-dimensional map, because SQLDigest
// is larger than Timestamp. This can reduce unnecessary memory usage.
type ExecCountMap map[string]map[int64]uint64

// Merge merges other into ExecCountMap.
// Values with the same SQL and same timestamp will be added.
func (m ExecCountMap) Merge(other ExecCountMap) {
	for newSQL, newTsCount := range other {
		tsCount, ok := m[newSQL]
		if !ok {
			m[newSQL] = newTsCount
			continue
		}
		for ts, count := range newTsCount {
			tsCount[ts] += count
		}
	}
}

type KvExecCountMap map[string]map[string]map[int64]uint64

func (m KvExecCountMap) Merge(other KvExecCountMap) {
	for newSQL, newAddrTsCount := range other {
		addrTsCount, ok := m[newSQL]
		if !ok {
			m[newSQL] = newAddrTsCount
			continue
		}
		ExecCountMap(addrTsCount).Merge(newAddrTsCount)
	}
}

// ExecCounter is a counter used locally in each session.
// We can count the number of SQL executions on ExecCounter, and it
// is expected that these statistics will eventually be collected
// and merged in the background.
type ExecCounter struct {
	mu          sync.Mutex
	execCount   ExecCountMap
	kvMu        sync.Mutex
	kvExecCount KvExecCountMap
	closed      *atomic.Bool
}

// CreateExecCounter try to create and register an ExecCounter.
// If we are in the initialization phase and have not yet called SetupTopSQL
// to initialize the top-sql, nothing will happen, and we will get nil. But
// this scene should never appear, because we always call SetupTopSQL before
// starting the server, at this moment we cannot receive connections and will
// not create a valid session. So this case will never happen: "This function
// returns nil, so this session will never count execution of SQL sent by peer
// client".
func CreateExecCounter() *ExecCounter {
	if globalExecCounterManager == nil {
		return nil
	}
	counter := &ExecCounter{
		execCount:   ExecCountMap{},
		kvExecCount: KvExecCountMap{},
		closed:      atomic.NewBool(false),
	}
	globalExecCounterManager.register(counter)
	return counter
}

// Count is used to count the number of executions of a certain SQL.
// You don't need to provide execution time. By default, the time when
// you call Count is considered the time when SQL is ready to execute.
// The parameter sql is a universal string, and ExecCounter does not care
// whether it is a normalized SQL or a stringified SQL digest.
// Count is thread-safe.
func (c *ExecCounter) Count(sql string, n uint64) {
	ts := time.Now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	tsCount, ok := c.execCount[sql]
	if !ok {
		c.execCount[sql] = map[int64]uint64{}
		tsCount = c.execCount[sql]
	}
	tsCount[ts] += n
}

func (c *ExecCounter) CountKv(sql, addr string, n uint64) {
	ts := time.Now().Unix()
	c.kvMu.Lock()
	defer c.kvMu.Unlock()
	addrTsCount, ok := c.kvExecCount[sql]
	if !ok {
		c.kvExecCount[sql] = map[string]map[int64]uint64{}
		addrTsCount = c.kvExecCount[sql]
	}
	tsCount, ok := addrTsCount[addr]
	if !ok {
		addrTsCount[addr] = map[int64]uint64{}
		tsCount = addrTsCount[addr]
	}
	tsCount[ts] += n
}

// Take removes all existing data from ExecCounter.
// Take is thread-safe.
func (c *ExecCounter) Take() ExecCountMap {
	c.mu.Lock()
	defer c.mu.Unlock()
	execCount := c.execCount
	c.execCount = ExecCountMap{}
	return execCount
}

func (c *ExecCounter) TakeKv() KvExecCountMap {
	c.kvMu.Lock()
	defer c.kvMu.Unlock()
	kvExecCount := c.kvExecCount
	c.kvExecCount = KvExecCountMap{}
	return kvExecCount
}

func (c *ExecCounter) RPCInterceptor(sql string) tikvrpc.Interceptor {
	if c == nil {
		return nil
	}
	return func(next tikvrpc.InterceptorFunc) tikvrpc.InterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			c.CountKv(sql, target, 1)
			return next(target, req)
		}
	}
}

// Close marks ExecCounter as "closed".
// The background goroutine will periodically detect whether each ExecCounter
// has been closed, and if so, it will be cleaned up.
func (c *ExecCounter) Close() {
	c.closed.Store(true)
}

// Closed returns whether the ExecCounter has been closed.
func (c *ExecCounter) Closed() bool {
	return c.closed.Load()
}

// execCounterManager is used to manage all ExecCounter s.
// It is responsible for collecting data from all ExecCounter s, aggregating
// them together, and regularly cleaning up the closed ExecCounter s.
type execCounterManager struct {
	counters     sync.Map // map[uint64]*ExecCounter
	curCounterID atomic.Uint64
	execCount    ExecCountMap
	kvExecCount  KvExecCountMap
	closeCh      chan struct{}
}

// newExecCountManager creates an empty execCounterManager.
func newExecCountManager() *execCounterManager {
	return &execCounterManager{
		execCount:   ExecCountMap{},
		kvExecCount: KvExecCountMap{},
		closeCh:     make(chan struct{}),
	}
}

// Run will block the current goroutine and execute the main task of execCounterManager.
func (m *execCounterManager) Run() {
	collectTicker := time.NewTicker(execCounterManagerCollectDuration)
	defer collectTicker.Stop()

	uploadTicker := time.NewTicker(execCounterManagerUploadDuration)
	defer uploadTicker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-collectTicker.C:
			m.collect()
		case <-uploadTicker.C:
			// TODO(mornyx): upload m.execCount & m.kvExecCount. Here is a bridge connecting
			//               the exec-count module with the existing top-sql cpu reporter.
			b, _ := json.MarshalIndent(m.kvExecCount, "", "  ")
			fmt.Println(string(b))
			fmt.Println("=====")
			m.execCount = ExecCountMap{}
			m.kvExecCount = KvExecCountMap{}
		}
	}
}

// collect data from all associated ExecCounter s.
// If an ExecCounter is closed, then remove it from the map.
func (m *execCounterManager) collect() {
	m.counters.Range(func(idRaw, counterRaw interface{}) bool {
		id := idRaw.(uint64)
		counter := counterRaw.(*ExecCounter)
		if counter.Closed() {
			m.counters.Delete(id)
		}
		execCount := counter.Take()
		kvExecCount := counter.TakeKv()
		m.execCount.Merge(execCount)
		m.kvExecCount.Merge(kvExecCount)
		return true
	})
}

// register binds ExecCounter to execCounterManager.
// register is thread-safe.
func (m *execCounterManager) register(counter *ExecCounter) {
	m.counters.Store(m.curCounterID.Add(1), counter)
}

// close ends the execution of the current execCounterManager.
func (m *execCounterManager) close() {
	m.closeCh <- struct{}{}
}

var globalExecCounterManager *execCounterManager

// SetupExecCounter is used to initialize the background goroutine of the exec-count module.
func SetupExecCounter() {
	globalExecCounterManager = newExecCountManager()
	go globalExecCounterManager.Run()
}

// CloseExecCounter is used to stop the background goroutine of the exec-count module.
func CloseExecCounter() {
	if globalExecCounterManager != nil {
		globalExecCounterManager.close()
	}
}
