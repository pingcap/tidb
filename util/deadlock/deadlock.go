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
// See the License for the specific language governing permissions and
// limitations under the License.

package deadlock

import (
	"fmt"
	"sync"
)

// Detector detects deadlock.
type Detector struct {
	waitForMap map[uint64]*txnList
	lock       sync.Mutex
}

type txnList struct {
	txns []txnKeyHashPair
}

type txnKeyHashPair struct {
	txn     uint64
	keyHash uint64
}

// NewDetector creates a new Detector.
func NewDetector() *Detector {
	return &Detector{
		waitForMap: map[uint64]*txnList{},
	}
}

// ErrDeadlock is returned when deadlock is detected.
type ErrDeadlock struct {
	KeyHash uint64
}

func (e *ErrDeadlock) Error() string {
	return fmt.Sprintf("deadlock(%d)", e.KeyHash)
}

// Detect detects deadlock for the sourceTxn on a locked key.
func (d *Detector) Detect(sourceTxn, waitForTxn, keyHash uint64) *ErrDeadlock {
	d.lock.Lock()
	err := d.doDetect(sourceTxn, waitForTxn)
	if err == nil {
		d.register(sourceTxn, waitForTxn, keyHash)
	}
	d.lock.Unlock()
	return err
}

func (d *Detector) doDetect(sourceTxn, waitForTxn uint64) *ErrDeadlock {
	list := d.waitForMap[waitForTxn]
	if list == nil {
		return nil
	}
	for _, nextTarget := range list.txns {
		if nextTarget.txn == sourceTxn {
			return &ErrDeadlock{KeyHash: nextTarget.keyHash}
		}
		if err := d.doDetect(sourceTxn, nextTarget.txn); err != nil {
			return err
		}
	}
	return nil
}

func (d *Detector) register(sourceTxn, waitForTxn, keyHash uint64) {
	list := d.waitForMap[sourceTxn]
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	if list == nil {
		d.waitForMap[sourceTxn] = &txnList{txns: []txnKeyHashPair{pair}}
		return
	}
	for _, tar := range list.txns {
		if tar.txn == waitForTxn && tar.keyHash == keyHash {
			return
		}
	}
	list.txns = append(list.txns, pair)
}

// CleanUp removes the wait for entry for the transaction.
func (d *Detector) CleanUp(txn uint64) {
	d.lock.Lock()
	delete(d.waitForMap, txn)
	d.lock.Unlock()
}

// CleanUpWaitFor removes a key in the wait for entry for the transaction.
func (d *Detector) CleanUpWaitFor(txn, waitForTxn, keyHash uint64) {
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	d.lock.Lock()
	l := d.waitForMap[txn]
	if l != nil {
		for i, tar := range l.txns {
			if tar == pair {
				l.txns = append(l.txns[:i], l.txns[i+1:]...)
				break
			}
		}
		if len(l.txns) == 0 {
			delete(d.waitForMap, txn)
		}
	}
	d.lock.Unlock()

}

// Expire removes entries with TS smaller than minTS.
func (d *Detector) Expire(minTS uint64) {
	d.lock.Lock()
	for ts := range d.waitForMap {
		if ts < minTS {
			delete(d.waitForMap, ts)
		}
	}
	d.lock.Unlock()
}
