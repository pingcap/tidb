package topsql

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// ExecCountMap represents Map<Timestamp, Map<SQL, Count>>
type ExecCountMap map[int64]map[string]uint64

func (m ExecCountMap) Merge(other ExecCountMap) {
	for newTs, newSqlCount := range other {
		sqlCount, ok := m[newTs]
		if !ok {
			m[newTs] = newSqlCount
			continue
		}
		for sql, count := range newSqlCount {
			sqlCount[sql] += count
		}
	}
}

type ExecCounter struct {
	mu        sync.Mutex
	execCount ExecCountMap
	closed    *atomic.Bool
}

// CreateExecCounter try to create and register an ExecCounter.
//
// If we are in the initialization phase and have not yet called
// SetupTopSQL to initialize the top-sql, nothing will happen and
// we will get nil.
func CreateExecCounter() *ExecCounter {
	if globalExecCounterManager == nil {
		return nil
	}
	counter := &ExecCounter{
		execCount: ExecCountMap{},
		closed:    atomic.NewBool(false),
	}
	globalExecCounterManager.register(counter)
	return counter
}

func (c *ExecCounter) Count(sql string, n uint64) {
	ts := time.Now().Unix()
	c.mu.Lock()
	defer c.mu.Unlock()
	sqlCount, ok := c.execCount[ts]
	if !ok {
		c.execCount[ts] = map[string]uint64{}
		sqlCount = c.execCount[ts]
	}
	sqlCount[sql] += n
}

func (c *ExecCounter) Take() ExecCountMap {
	c.mu.Lock()
	defer c.mu.Unlock()
	execCount := c.execCount
	c.execCount = ExecCountMap{}
	return execCount
}

func (c *ExecCounter) Close() {
	c.closed.Store(true)
}

func (c *ExecCounter) Closed() bool {
	return c.closed.Load()
}

type execCounterManager struct {
	counters      sync.Map // map[uint64]*ExecCounter
	nextCounterID atomic.Uint64
	execCount     ExecCountMap
	closeCh       chan struct{}
}

func newExecCountManager() *execCounterManager {
	return &execCounterManager{
		execCount: ExecCountMap{},
		closeCh:   make(chan struct{}),
	}
}

func (m *execCounterManager) Run() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	tmpTicker := time.NewTicker(10 * time.Second)
	defer tmpTicker.Stop()

	for {
		select {
		case <-m.closeCh:
			return
		case <-ticker.C:
			m.collect()
		case <-tmpTicker.C:
			b, _ := json.MarshalIndent(m.execCount, "", "  ")
			fmt.Println(">>>>>", string(b))
			m.execCount = ExecCountMap{}
		}
	}
}

func (m *execCounterManager) collect() {
	m.counters.Range(func(id_, counter_ interface{}) bool {
		id := id_.(uint64)
		counter := counter_.(*ExecCounter)
		if counter.Closed() {
			m.counters.Delete(id)
		}
		execCount := counter.Take()
		m.execCount.Merge(execCount)
		return true
	})
}

func (m *execCounterManager) register(counter *ExecCounter) {
	m.counters.Store(m.nextCounterID.Add(1), counter)
}

func (m *execCounterManager) close() {
	m.closeCh <- struct{}{}
}
