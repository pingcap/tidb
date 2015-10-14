package localstore

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var _ kv.GC = (*localstoreGC)(nil)

const (
	maxRetainVersions = 3
	gcBatchDeleteSize = 100
	gcInterval        = 1 * time.Second
)

type localstoreGC struct {
	mu         sync.Mutex
	recentKeys map[string]struct{}
	stopChan   chan struct{}
	delChan    chan kv.EncodedKey
	ticker     *time.Ticker
	store      *dbStore
	batch      engine.Batch
	cnt        int
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

func (gc *localstoreGC) getAllVersions(k kv.Key) ([]kv.EncodedKey, error) {
	startKey := MvccEncodeVersionKey(k, kv.MaxVersion)
	endKey := MvccEncodeVersionKey(k, kv.MinVersion)

	it, err := gc.store.db.Seek(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Release()

	var ret []kv.EncodedKey
	for it.Next() {
		if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 {
			ret = append(ret, kv.EncodedKey(it.Key()))
		}
	}
	return ret, nil
}

func (gc *localstoreGC) Do(k kv.Key) {
	keys, err := gc.getAllVersions(k)
	if err != nil {
		log.Error(err)
		return
	}
	if len(keys) > maxRetainVersions {
		// remove keys[maxRetainVersion:]
		for _, key := range keys[maxRetainVersions:] {
			log.Warn("GC Key:", key)
			gc.delChan <- key
		}
	}
}

func (gc *localstoreGC) Start() {
	// time trigger
	go func() {
		for {
			select {
			case <-gc.stopChan:
				log.Debug("stop gc")
				break
			case <-gc.ticker.C:
				log.Debug("gc trigger")
				gc.mu.Lock()
				m := gc.recentKeys
				gc.recentKeys = make(map[string]struct{})
				gc.mu.Unlock()
				// Do GC
				for k, _ := range m {
					gc.Do([]byte(k))
				}
			}
		}
	}()

	// delete worker
	go func() {
		cnt := 0
		batch := gc.store.db.NewBatch()
		for k := range gc.delChan {
			cnt++
			batch.Delete(k)
			if cnt == gcBatchDeleteSize {
				// batch delete
				err := gc.store.db.Commit(batch)
				if err != nil {
					// Ignore error
					log.Error(err)
				}
				log.Debug("batch gc delete done")
				batch = gc.store.newBatch()
				cnt = 0
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
	ret := &localstoreGC{
		recentKeys: make(map[string]struct{}),
		stopChan:   make(chan struct{}),
		delChan:    make(chan kv.EncodedKey),
		ticker:     time.NewTicker(gcInterval),
	}
	return ret
}
