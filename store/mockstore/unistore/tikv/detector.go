// Copyright 2019 PingCAP, Inc.
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
// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"container/list"
	"sync"
	"time"

	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"go.uber.org/zap"
)

// Detector detects deadlock.
type Detector struct {
	waitForMap       map[uint64]*txnList
	lock             sync.Mutex
	entryTTL         time.Duration
	totalSize        uint64
	lastActiveExpire time.Time
	urgentSize       uint64
	expireInterval   time.Duration
}

type txnList struct {
	//txns []txnKeyHashPair
	txns *list.List
}

type txnKeyHashPair struct {
	txn          uint64
	keyHash      uint64
	registerTime time.Time
	diagCtx      diagnosticContext
}

type diagnosticContext struct {
	key              []byte
	resourceGroupTag []byte
}

func (p *txnKeyHashPair) isExpired(ttl time.Duration, nowTime time.Time) bool {
	return p.registerTime.Add(ttl).Before(nowTime)
}

// NewDetector creates a new Detector.
func NewDetector(ttl time.Duration, urgentSize uint64, expireInterval time.Duration) *Detector {
	return &Detector{
		waitForMap:       map[uint64]*txnList{},
		entryTTL:         ttl,
		lastActiveExpire: time.Now(),
		urgentSize:       urgentSize,
		expireInterval:   expireInterval,
	}
}

// Detect detects deadlock for the sourceTxn on a locked key.
func (d *Detector) Detect(sourceTxn, waitForTxn, keyHash uint64, diagCtx diagnosticContext) *kverrors.ErrDeadlock {
	d.lock.Lock()
	nowTime := time.Now()
	d.activeExpire(nowTime)
	err := d.doDetect(nowTime, sourceTxn, waitForTxn)
	if err == nil {
		d.register(sourceTxn, waitForTxn, keyHash, diagCtx)
	} else {
		// Reverse the wait chain so that the order will be each one waiting for the next one, and append the current
		// entry that finally caused the deadlock.
		for i := 0; i < len(err.WaitChain)/2; i++ {
			j := len(err.WaitChain) - i - 1
			err.WaitChain[i], err.WaitChain[j] = err.WaitChain[j], err.WaitChain[i]
		}
		err.WaitChain = append(err.WaitChain, &deadlockpb.WaitForEntry{
			Txn:              sourceTxn,
			Key:              diagCtx.key,
			KeyHash:          keyHash,
			ResourceGroupTag: diagCtx.resourceGroupTag,
			WaitForTxn:       waitForTxn,
		})
	}
	d.lock.Unlock()
	return err
}

func (d *Detector) doDetect(nowTime time.Time, sourceTxn, waitForTxn uint64) *kverrors.ErrDeadlock {
	val := d.waitForMap[waitForTxn]
	if val == nil {
		return nil
	}
	var nextVal *list.Element
	for cur := val.txns.Front(); cur != nil; cur = nextVal {
		nextVal = cur.Next()
		keyHashPair := cur.Value.(*txnKeyHashPair)
		// check if this edge is expired
		if keyHashPair.isExpired(d.entryTTL, nowTime) {
			val.txns.Remove(cur)
			d.totalSize--
			continue
		}
		if keyHashPair.txn == sourceTxn {
			return &kverrors.ErrDeadlock{DeadlockKeyHash: keyHashPair.keyHash,
				WaitChain: []*deadlockpb.WaitForEntry{
					{
						Txn:              waitForTxn,
						Key:              keyHashPair.diagCtx.key,
						KeyHash:          keyHashPair.keyHash,
						ResourceGroupTag: keyHashPair.diagCtx.resourceGroupTag,
						WaitForTxn:       keyHashPair.txn,
					},
				},
			}
		}
		if err := d.doDetect(nowTime, sourceTxn, keyHashPair.txn); err != nil {
			err.WaitChain = append(err.WaitChain, &deadlockpb.WaitForEntry{
				Txn:              waitForTxn,
				Key:              keyHashPair.diagCtx.key,
				KeyHash:          keyHashPair.keyHash,
				ResourceGroupTag: keyHashPair.diagCtx.resourceGroupTag,
				WaitForTxn:       keyHashPair.txn,
			})
			return err
		}
	}
	if val.txns.Len() == 0 {
		delete(d.waitForMap, waitForTxn)
	}
	return nil
}

func (d *Detector) register(sourceTxn, waitForTxn, keyHash uint64, diagCtx diagnosticContext) {
	val := d.waitForMap[sourceTxn]
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash, registerTime: time.Now(), diagCtx: diagCtx}
	if val == nil {
		newList := &txnList{txns: list.New()}
		newList.txns.PushBack(&pair)
		d.waitForMap[sourceTxn] = newList
		d.totalSize++
		return
	}
	for cur := val.txns.Front(); cur != nil; cur = cur.Next() {
		valuePair := cur.Value.(*txnKeyHashPair)
		if valuePair.txn == waitForTxn && valuePair.keyHash == keyHash {
			return
		}
	}
	val.txns.PushBack(&pair)
	d.totalSize++
}

// CleanUp removes the wait for entry for the transaction.
func (d *Detector) CleanUp(txn uint64) {
	d.lock.Lock()
	if l, ok := d.waitForMap[txn]; ok {
		d.totalSize -= uint64(l.txns.Len())
	}
	delete(d.waitForMap, txn)
	d.lock.Unlock()
}

// CleanUpWaitFor removes a key in the wait for entry for the transaction.
func (d *Detector) CleanUpWaitFor(txn, waitForTxn, keyHash uint64) {
	d.lock.Lock()
	l := d.waitForMap[txn]
	if l != nil {
		var nextVal *list.Element
		for cur := l.txns.Front(); cur != nil; cur = nextVal {
			nextVal = cur.Next()
			valuePair := cur.Value.(*txnKeyHashPair)
			if valuePair.txn == waitForTxn && valuePair.keyHash == keyHash {
				l.txns.Remove(cur)
				d.totalSize--
				break
			}
		}
		if l.txns.Len() == 0 {
			delete(d.waitForMap, txn)
		}
	}
	d.lock.Unlock()

}

// activeExpire removes expired entries, should be called under d.lock protection
func (d *Detector) activeExpire(nowTime time.Time) {
	if nowTime.Sub(d.lastActiveExpire) > d.expireInterval &&
		d.totalSize >= d.urgentSize {
		log.Info("detector will do activeExpire", zap.Uint64("size", d.totalSize))
		for txn, l := range d.waitForMap {
			var nextVal *list.Element
			for cur := l.txns.Front(); cur != nil; cur = nextVal {
				nextVal = cur.Next()
				valuePair := cur.Value.(*txnKeyHashPair)
				if valuePair.isExpired(d.entryTTL, nowTime) {
					l.txns.Remove(cur)
					d.totalSize--
				}
			}
			if l.txns.Len() == 0 {
				delete(d.waitForMap, txn)
			}
		}
		d.lastActiveExpire = nowTime
		log.Info("detector activeExpire finished", zap.Uint64("size", d.totalSize))
	}
}
