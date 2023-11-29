// Copyright 2023 PingCAP, Inc.
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

package meta

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DataStore is an interface to create transaction.
type DataStore interface {
	Begin() (txn DataTxn, err error)
}

// DataTxn is an interface to operate data in transaction.
type DataTxn interface {
	Rollback()
	Commit() error

	StartTS() uint64
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) error
}

type lockContent struct {
	StartTS    uint64 `json:"start_ts"`
	InstanceID string `json:"instance_id"`
}

func newLockContent(startTS uint64, instanceID string) lockContent {
	return lockContent{
		StartTS:    startTS,
		InstanceID: instanceID,
	}
}

func (c lockContent) encode() ([]byte, error) {
	return json.Marshal(c)
}

func (c *lockContent) decode(data []byte) error {
	return json.Unmarshal(data, c)
}

// DistributedLock is a distributed lock implementation based on meta key transaction.
type DistributedLock struct {
	ctx        context.Context
	store      DataStore
	instanceID string

	lockName []byte
	backoff  time.Duration
	lease    time.Duration

	renewCtx    context.Context
	renewCancel context.CancelFunc
}

// NewDistributedLock creates a new distributed lock.
func NewDistributedLock(ctx context.Context, store DataStore, instanceID string, lockName string) *DistributedLock {
	ctx = logutil.WithCategory(ctx, "distributed-lock")
	return &DistributedLock{
		ctx:        ctx,
		store:      store,
		instanceID: instanceID,

		lockName: []byte(lockName),
		backoff:  3 * time.Second,
		lease:    30 * time.Second,
	}
}

// SetBackoff sets the backoff time between two lock operations.
func (d *DistributedLock) SetBackoff(backoff time.Duration) *DistributedLock {
	d.backoff = backoff
	return d
}

// SetLease sets the lease time of the lock.
func (d *DistributedLock) SetLease(lease time.Duration) *DistributedLock {
	d.lease = lease
	return d
}

func (d *DistributedLock) renewLockLoop() {
	ticker := time.NewTicker(d.lease / 3)
	defer ticker.Stop()

	for {
		select {
		case <-d.renewCtx.Done():
			return
		case <-ticker.C:
			err := d.renew()
			if err != nil {
				logutil.BgLogger().Warn("renew lock failed", zap.Error(err))
				return
			}
		}
	}
}

func (d *DistributedLock) renew() error {
	return d.getLockInTxnWithRetry(
		func(lc *lockContent, txn DataTxn) (storeErr, otherErr error) {
			if lc == nil || lc.InstanceID != d.instanceID {
				logutil.Logger(d.ctx).Warn("lock is unexpectedly removed", zap.String("instanceID", d.instanceID))
				return nil, nil
			}

			// Lock is exist, update it.
			lc.StartTS = txn.StartTS()
			data, err := lc.encode()
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = txn.Set(d.lockName, data)
			if err != nil {
				return errors.Trace(err), nil
			}
			return nil, nil
		})
}

var lockHeldErr error = errors.Errorf("lock is held")

// Lock locks the distributed lock.
func (d *DistributedLock) Lock() error {
	newLockCreated := false
	err := d.getLockInTxnWithRetry(
		func(lc *lockContent, txn DataTxn) (storeErr, otherErr error) {
			startTS := txn.StartTS()
			// Lock is not exist or expired, create or overwrite it.
			if lc == nil || d.lockExpired(lc, startTS) {
				lc := newLockContent(startTS, d.instanceID)
				data, err := lc.encode()
				if err != nil {
					return nil, errors.Trace(err)
				}
				err = txn.Set(d.lockName, data)
				if err != nil {
					return errors.Trace(err), nil
				}
				newLockCreated = true
				return nil, nil
			}
			// Lock is valid, wait for a while.
			return lockHeldErr, nil
		})
	if err != nil {
		return err
	}
	if !intest.InTest && newLockCreated {
		d.renewCtx, d.renewCancel = context.WithCancel(d.ctx)
		go d.renewLockLoop()
	}
	return nil
}

func (d *DistributedLock) lockExpired(lc *lockContent, currentStartTS uint64) bool {
	if intest.InTest {
		return false
	}
	lockStartTime := model.TSConvert2Time(lc.StartTS)
	currentTime := model.TSConvert2Time(currentStartTS)
	expired := currentTime.Sub(lockStartTime) > d.lease
	if expired {
		logutil.Logger(d.ctx).Warn("lock is expired",
			zap.Time("lockStartTime", lockStartTime),
			zap.Time("currentTime", currentTime),
			zap.Duration("lease", d.lease))
	}
	return expired
}

// Unlock unlocks the distributed lock.
func (d *DistributedLock) Unlock() error {
	defer func() {
		if d.renewCancel != nil {
			d.renewCancel()
		}
	}()
	return d.getLockInTxnWithRetry(
		func(lc *lockContent, txn DataTxn) (storeErr, otherErr error) {
			if lc == nil || lc.InstanceID != d.instanceID {
				logutil.Logger(d.ctx).Warn("lock is unexpectedly removed", zap.String("instanceID", d.instanceID))
				return nil, nil
			}

			// Lock is exist, remove it.
			err := txn.Del(d.lockName)
			if err != nil {
				return errors.Trace(err), nil
			}
			return nil, nil
		})
}

func (d *DistributedLock) getLockInTxnWithRetry(fn lockHandleFunc) error {
	for {
		if contextIsDone(d.ctx) {
			return d.ctx.Err()
		}

		var noRetry bool
		err := runInTxn(d.store, func(txn DataTxn) error {
			data, err := txn.Get(d.lockName)
			if err != nil {
				return errors.Trace(err)
			}

			lc, err := decodeLockContent(data)
			if err != nil {
				noRetry = true
				return errors.Trace(err)
			}

			storeErr, otherErr := fn(lc, txn)
			if otherErr != nil {
				noRetry = true
				return errors.Trace(otherErr)
			}
			return storeErr
		})

		if err != nil {
			if err == lockHeldErr {
				time.Sleep(d.backoff)
				continue
			}
			logutil.Logger(d.ctx).Warn("lock encounter error",
				zap.Error(err), zap.Bool("retry", !noRetry))
			if noRetry {
				return err
			}
			time.Sleep(d.backoff)
			continue
		}
		return nil
	}
}

type lockHandleFunc func(lc *lockContent, txn DataTxn) (storeErr, otherErr error)
type txnHandleFunc func(txn DataTxn) error

func contextIsDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func runInTxn(store DataStore, fn txnHandleFunc) error {
	txn, err := store.Begin()
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

func decodeLockContent(data []byte) (*lockContent, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var lc lockContent
	err := lc.decode(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &lc, nil
}
