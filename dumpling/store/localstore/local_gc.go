package localstore

import (
	"container/list"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
)

var _ kv.GC = (*localstoreGC)(nil)

type localstoreGC struct {
	mu         sync.Mutex
	recentKeys map[string]struct{}
	stopChan   chan struct{}
	ticker     *time.Ticker
}

func (gc *localstoreGC) OnSet(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreGC) OnGet(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreGC) Do(k kv.Key) {
	// TODO
	log.Debugf("gc key: %q", k)
}

func (gc *localstoreGC) Start() {
	go func() {
		for {
			select {
			case <-gc.stopChan:
				break
			case <-gc.ticker.C:
				l := list.New()
				gc.mu.Lock()
				for k, _ := range gc.recentKeys {
					// get recent keys list
					l.PushBack(k)
				}
				// clean recentKeys
				gc.recentKeys = map[string]struct{}{}
				gc.mu.Unlock()
				// Do GC
				for e := l.Front(); e != nil; e = e.Next() {
					gc.Do([]byte(e.Value.(string)))
				}
			}
		}
	}()
}

func (gc *localstoreGC) Stop() {
	gc.stopChan <- struct{}{}
	gc.ticker.Stop()
	close(gc.stopChan)
}

func newLocalGC() *localstoreGC {
	return &localstoreGC{
		recentKeys: map[string]struct{}{},
		stopChan:   make(chan struct{}),
		// TODO hard code
		ticker: time.NewTicker(time.Second * 1),
	}
}
