// Copyright 2017 PingCAP, Inc.
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
package dlock

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/terror"
	"github.com/twinj/uuid"
)

const (
	leaseInterval time.Duration = 3 * time.Second
)

var (
	ErrAlreadyLocked = errors.New("already locked")
)

// DLock represents an distributed lock that can be locked and unlocked.
type DLock struct {
	name         string
	s            kv.Storage
	unlockSignal chan struct{}
	LockMeta
}

// LockMeta is the meta information that stored to the KV storage.
type LockMeta struct {
	ID          string    `json:"uuid"`
	LastUpdated time.Time `json:"last_updated"`
}

// New returns a DLock object.
func New(s kv.Storage, name string) *DLock {
	return &DLock{
		name:         name,
		s:            s,
		unlockSignal: make(chan struct{}),
		LockMeta: LockMeta{
			ID: uuid.NewV4().String(),
		},
	}
}

// TryLock tries to acquire the lock, returns error on fail.
func (l *DLock) TryLock() error {
	err := kv.RunInNewTxn(l.s, false, func(txn kv.Transaction) error {
		lockKey := l.getLockKey()
		val, err := txn.Get([]byte(lockKey))
		if err != nil {
			if kv.IsErrNotFound(err) {
				// if no such lock key, try to lock
				return l.writeLock(txn)
			}
			return errors.Trace(err)
		}

		// if there's a lock already, check if it's mine
		var meta LockMeta
		json.Unmarshal(val, &meta)

		// if another one has the lock, and not timeout yet
		if meta.ID != l.LockMeta.ID {
			if oracleTimeNow(txn).Sub(meta.LastUpdated) < 4*leaseInterval {
				return ErrAlreadyLocked
			}
		}

		return l.writeLock(txn)
	})

	if err == nil {
		go l.keepalive()
	}
	return err
}

// Lock tries to acquire the lock. it will block until get the lock, or some
// error happens.
func (l *DLock) Lock() error {
	try := 0
	for {
		err := l.TryLock()
		if err == nil {
			return nil
		}
		if terror.ErrorEqual(err, ErrAlreadyLocked) || kv.IsRetryableError(err) {
			// backoff, take a rest, at least 100ms
			sleepInterval := 100 + rand.Intn(500)
			time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
			try++
			if try > 10 {
				log.Warn("retry to fetch lock...", l.name)
			}
		} else {
			log.Error(err)
			return err
		}
	}
}

// Unlock unlocks the resource.
func (l *DLock) Unlock() {
	l.unlockSignal <- struct{}{}
}

func (l *DLock) getLockKey() []byte {
	lockKey := fmt.Sprintf("%s-LOCK", l.name)
	return []byte(lockKey)
}

func (l *DLock) writeLock(txn kv.Transaction) error {
	l.LockMeta.LastUpdated = oracleTimeNow(txn)
	meta, _ := json.Marshal(l.LockMeta)
	return txn.Set(l.getLockKey(), meta)
}

func (l *DLock) getLockMeta(txn kv.Transaction) (*LockMeta, error) {
	val, err := txn.Get(l.getLockKey())
	if err != nil {
		return nil, errors.Trace(err)
	}
	var meta LockMeta
	json.Unmarshal(val, &meta)
	return &meta, nil
}

func oracleTimeNow(txn kv.Transaction) time.Time {
	ts := txn.StartTS()
	physicalMS := oracle.ExtractPhysical(ts)
	return time.Unix(physicalMS/1e3, 0)
}

func (l *DLock) keepalive() error {
	ticker := time.NewTicker(leaseInterval)
	for {
		select {
		case <-ticker.C:
			// update lease
			err := kv.RunInNewTxn(l.s, false, func(txn kv.Transaction) error {
				meta, err := l.getLockMeta(txn)
				if err != nil {
					return errors.Trace(err)
				}

				if meta.ID != l.ID {
					log.Fatal(l.ID, meta, "someone else took the lock")
				}

				err = l.writeLock(txn)
				return errors.Trace(err)
			})
			if err != nil {
				return errors.Trace(err)
			}
		case <-l.unlockSignal:
			// unlock, delete lock key
			ticker.Stop()
			err := kv.RunInNewTxn(l.s, false, func(txn kv.Transaction) error {
				meta, err := l.getLockMeta(txn)
				if err != nil {
					return err
				}

				// we can only unlock our lock
				if meta.ID == l.ID {
					err = txn.Delete(l.getLockKey())
				}
				return err
			})
			return err
		}
	}
}
