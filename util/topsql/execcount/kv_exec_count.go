package execcount

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/atomic"
)

var globalKvExecCounterManager *kvExecCounterManager

func SetupKvExecCounter() {
	globalKvExecCounterManager = newKvExecCountManager()
	go globalKvExecCounterManager.Run()
}

func CloseKvExecCounter() {
	if globalKvExecCounterManager != nil {
		globalKvExecCounterManager.close()
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

type KvExecCounter struct {
	mu        sync.Mutex
	execCount KvExecCountMap
	closed    *atomic.Bool
}

func CreateKvExecCounter() *KvExecCounter {
	if globalExecCounterManager == nil {
		return nil
	}
	counter := &KvExecCounter{
		execCount: KvExecCountMap{},
		closed:    atomic.NewBool(false),
	}
	globalKvExecCounterManager.register(counter)
	return counter
}

func (c *KvExecCounter) Count(sql, addr string, n uint64) {
	ts := time.Now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	addrTsCount, ok := c.execCount[sql]
	if !ok {
		c.execCount[sql] = map[string]map[int64]uint64{}
		addrTsCount = c.execCount[sql]
	}
	tsCount, ok := addrTsCount[addr]
	if !ok {
		addrTsCount[addr] = map[int64]uint64{}
		tsCount = addrTsCount[addr]
	}
	tsCount[ts] += n
}

func (c *KvExecCounter) Take() KvExecCountMap {
	c.mu.Lock()
	defer c.mu.Unlock()
	execCount := c.execCount
	c.execCount = KvExecCountMap{}
	return execCount
}

func (c *KvExecCounter) RPCInterceptor(sql string) tikvrpc.Interceptor {
	if c == nil {
		return nil
	}
	return func(next tikvrpc.InterceptorFunc) tikvrpc.InterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			c.Count(sql, target, 1)
			return next(target, req)
		}
	}
}

func (c *KvExecCounter) Close() {
	c.closed.Store(true)
}

func (c *KvExecCounter) Closed() bool {
	return c.closed.Load()
}

type kvExecCounterManager struct {
	counters     sync.Map // map[uint64]*KvExecCounter
	curCounterID atomic.Uint64
	execCount    KvExecCountMap
	closeCh      chan struct{}
}

func newKvExecCountManager() *kvExecCounterManager {
	return &kvExecCounterManager{
		execCount: KvExecCountMap{},
		closeCh:   make(chan struct{}),
	}
}

func (m *kvExecCounterManager) Run() {
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
			// TODO(mornyx): upload m.execCount. Here is a bridge connecting the
			//               exec-count module with the existing top-sql cpu reporter.
			b, _ := json.MarshalIndent(m.execCount, "", "  ")
			fmt.Println(string(b))
			fmt.Println("=====")
			m.execCount = KvExecCountMap{}
		}
	}
}

func (m *kvExecCounterManager) collect() {
	m.counters.Range(func(idRaw, counterRaw interface{}) bool {
		id := idRaw.(uint64)
		counter := counterRaw.(*KvExecCounter)
		if counter.Closed() {
			m.counters.Delete(id)
		}
		execCount := counter.Take()
		m.execCount.Merge(execCount)
		return true
	})
}

func (m *kvExecCounterManager) register(counter *KvExecCounter) {
	m.counters.Store(counter, struct{}{})
}

func (m *kvExecCounterManager) close() {
	m.closeCh <- struct{}{}
}
