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

package kv

import (
	"context"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// RunInNewTxn will run the f in a new transaction environment.
func RunInNewTxn(store Storage, retryable bool, f func(txn Transaction) error) error {
	var (
		err           error
		originalTxnTS uint64
		txn           Transaction
	)
	for i := uint(0); i < maxRetryCnt; i++ {
		txn, err = store.Begin()
		if err != nil {
			logutil.BgLogger().Error("RunInNewTxn", zap.Error(err))
			return err
		}

		// originalTxnTS is used to trace the original transaction when the function is retryable.
		if i == 0 {
			originalTxnTS = txn.StartTS()
		}

		err = f(txn)
		if err != nil {
			err1 := txn.Rollback()
			terror.Log(err1)
			if retryable && IsTxnRetryableError(err) {
				logutil.BgLogger().Warn("RunInNewTxn",
					zap.Uint64("retry txn", txn.StartTS()),
					zap.Uint64("original txn", originalTxnTS),
					zap.Error(err))
				continue
			}
			return err
		}

		err = txn.Commit(context.Background())
		if err == nil {
			break
		}
		if retryable && IsTxnRetryableError(err) {
			logutil.BgLogger().Warn("RunInNewTxn",
				zap.Uint64("retry txn", txn.StartTS()),
				zap.Uint64("original txn", originalTxnTS),
				zap.Error(err))
			BackOff(i)
			continue
		}
		return err
	}
	return err
}

var (
	// maxRetryCnt represents maximum retry times in RunInNewTxn.
	maxRetryCnt uint = 100
	// retryBackOffBase is the initial duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffBase = 1
	// retryBackOffCap is the max amount of duration, in microsecond, a failed transaction stays dormancy before it retries
	retryBackOffCap = 100
)

// BackOff Implements exponential backoff with full jitter.
// Returns real back off time in microsecond.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html.
func BackOff(attempts uint) int {
	upper := int(math.Min(float64(retryBackOffCap), float64(retryBackOffBase)*math.Pow(2.0, float64(attempts))))
	sleep := time.Duration(rand.Intn(upper)) * time.Millisecond
	time.Sleep(sleep)
	return int(sleep)
}

// mockCommitErrorEnable uses to enable `mockCommitError` and only mock error once.
var mockCommitErrorEnable = int64(0)

// MockCommitErrorEnable exports for gofail testing.
func MockCommitErrorEnable() {
	atomic.StoreInt64(&mockCommitErrorEnable, 1)
}

// MockCommitErrorDisable exports for gofail testing.
func MockCommitErrorDisable() {
	atomic.StoreInt64(&mockCommitErrorEnable, 0)
}

// IsMockCommitErrorEnable exports for gofail testing.
func IsMockCommitErrorEnable() bool {
	return atomic.LoadInt64(&mockCommitErrorEnable) == 1
}

// TxnInfo is used to keep track the info of a committed transaction (mainly for diagnosis and testing)
type TxnInfo struct {
	StartTS  uint64
	CommitTS uint64
}
