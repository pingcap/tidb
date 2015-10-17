// Copyright 2015 PingCAP, Inc.
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

var _ kv.Compactor = (*localstoreCompactor)(nil)

const (
	deleteWorkerCnt = 3
)

var localCompactorDefaultPolicy = kv.CompactorPolicy{
	SafePoint:       20 * 1000, // in ms
	TriggerInterval: 1 * time.Second,
	BatchDeleteCnt:  100,
}

type localstoreCompactor struct {
	mu         sync.Mutex
	recentKeys map[string]struct{}
	stopCh     chan struct{}
	delCh      chan kv.EncodedKey
	ticker     *time.Ticker
	db         engine.DB
	policy     kv.CompactorPolicy
}

func (gc *localstoreCompactor) OnSet(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreCompactor) OnGet(k kv.Key) {
	// Do nothing now.
}

func (gc *localstoreCompactor) OnDelete(k kv.Key) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.recentKeys[string(k)] = struct{}{}
}

func (gc *localstoreCompactor) getAllVersions(k kv.Key) ([]kv.EncodedKey, error) {
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

func (gc *localstoreCompactor) deleteWorker() {
	cnt := 0
	batch := gc.db.NewBatch()
L:
	for {
		select {
		case <-gc.stopCh:
			break L
		case key := <-gc.delCh:
			{
				cnt++
				batch.Delete(key)
				// Batch delete.
				if cnt == gc.policy.BatchDeleteCnt {
					err := gc.db.Commit(batch)
					if err != nil {
						log.Error(err)
					}
					batch = gc.db.NewBatch()
					cnt = 0
				}
			}
		}
	}
}

func (gc *localstoreCompactor) checkExpiredKeysWorker() {
L:
	for {
		select {
		case <-gc.stopCh:
			break L
		case <-gc.ticker.C:
			log.Info("GC trigger")
			gc.mu.Lock()
			m := gc.recentKeys
			gc.recentKeys = make(map[string]struct{})
			gc.mu.Unlock()
			// Do Compactor
			for k := range m {
				err := gc.Compact(nil, []byte(k))
				if err != nil {
					log.Error(err)
				}
			}
		}
	}
	log.Info("GC Stopped")
}

func (gc *localstoreCompactor) filterExpiredKeys(keys []kv.EncodedKey) []kv.EncodedKey {
	var ret []kv.EncodedKey
	first := true
	// keys are always in descending order.
	for _, k := range keys {
		_, ver, err := MvccDecode(k)
		if err != nil {
			// Should not happen.
			panic(err)
		}
		ts := localVersionToTimestamp(ver)
		currentTs := time.Now().UnixNano() / int64(time.Millisecond)
		// Check timeout keys.
		if currentTs-int64(ts) >= int64(gc.policy.SafePoint) {
			// Skip first version.
			if first {
				first = false
				continue
			}
			ret = append(ret, k)
		}
	}
	return ret
}

func (gc *localstoreCompactor) Compact(ctx interface{}, k kv.Key) error {
	keys, err := gc.getAllVersions(k)
	if err != nil {
		return errors.Trace(err)
	}
	for _, key := range gc.filterExpiredKeys(keys) {
		// Send timeout key to deleteWorker.
		log.Info("GC send key to deleteWorker", key)
		gc.delCh <- key
	}
	return nil
}

func (gc *localstoreCompactor) Start() {
	// Start workers.
	go gc.checkExpiredKeysWorker()
	for i := 0; i < deleteWorkerCnt; i++ {
		go gc.deleteWorker()
	}
}

func (gc *localstoreCompactor) Stop() {
	gc.ticker.Stop()
	close(gc.stopCh)
}

func newLocalCompactor(policy kv.CompactorPolicy, db engine.DB) *localstoreCompactor {
	return &localstoreCompactor{
		recentKeys: make(map[string]struct{}),
		stopCh:     make(chan struct{}),
		delCh:      make(chan kv.EncodedKey, 100),
		ticker:     time.NewTicker(policy.TriggerInterval),
		policy:     policy,
		db:         db,
	}
}
