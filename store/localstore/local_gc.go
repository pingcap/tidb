package localstore

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
	"github.com/pingcap/tidb/util/bytes"
)

var _ kv.GC = (*localstoreGC)(nil)

const (
	safeTimeInterval = 20 * time.Second
)

var localGCDefaultPolicy = kv.GCPolicy{
	MaxRetainVersions: 1,
	TriggerInterval:   1 * time.Second,
}

type localstoreGC struct {
	mu         sync.Mutex
	recentKeys map[string]struct{}
	stopChan   chan struct{}
	delChan    chan kv.EncodedKey
	ticker     *time.Ticker
	db         engine.DB
	policy     kv.GCPolicy
}

func (gc *localstoreGC) OnSet(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreGC) OnGet(k kv.Key) {
	// Do nothing now.
}

func (gc *localstoreGC) OnDelete(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreGC) getAllVersions(k kv.Key) ([]kv.EncodedKey, error) {
	startKey := MvccEncodeVersionKey(k, kv.MaxVersion)
	endKey := MvccEncodeVersionKey(k, kv.MinVersion)

	it, err := gc.db.Seek(startKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Release()

	var ret []kv.EncodedKey
	for it.Next() {
		if kv.EncodedKey(it.Key()).Cmp(endKey) < 0 {
			ret = append(ret, bytes.CloneBytes(kv.EncodedKey(it.Key())))
		}
	}
	return ret, nil
}

func (gc *localstoreGC) Compact(ctx interface{}, k kv.Key) error {
	keys, err := gc.getAllVersions(k)
	if err != nil {
		return err
	}
	if len(keys) > gc.policy.MaxRetainVersions {
		for _, key := range keys[gc.policy.MaxRetainVersions:] {
			gc.delChan <- bytes.CloneBytes(key)
		}
	}
	return nil
}

func (gc *localstoreGC) Start() {
	// time trigger
	go func() {
	L:
		for {
			select {
			case <-gc.stopChan:
				log.Debug("Stop GC")
				break L
			case <-gc.ticker.C:
				gc.mu.Lock()
				m := gc.recentKeys
				gc.recentKeys = make(map[string]struct{})
				gc.mu.Unlock()
				// Do GC
				if len(m) > 0 {
					log.Error("GC trigger")
					for k, _ := range m {
						err := gc.Compact(nil, []byte(k))
						if err != nil {
							log.Error(err)
						}
					}
				}
			}
		}
	}()

	go func() {
		cnt := 0
		batch := gc.db.NewBatch()
		for key := range gc.delChan {
			cnt++
			batch.Delete(key)
			if cnt == 500 {
				gc.db.Commit(batch)
				log.Error("GC batch delete")
				batch = gc.db.NewBatch()
				cnt = 0
			}
		}
	}()

	go func() {
		cnt := 0
		batch := gc.db.NewBatch()
		for key := range gc.delChan {
			cnt++
			batch.Delete(key)
			if cnt == 500 {
				gc.db.Commit(batch)
				log.Error("GC batch delete")
				batch = gc.db.NewBatch()
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

func newLocalGC(policy kv.GCPolicy, db engine.DB) *localstoreGC {
	ret := &localstoreGC{
		recentKeys: make(map[string]struct{}),
		stopChan:   make(chan struct{}),
		delChan:    make(chan kv.EncodedKey, 100),
		ticker:     time.NewTicker(policy.TriggerInterval),
		policy:     policy,
		db:         db,
	}
	return ret
}
