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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// RunInNewTxn will run the f in a new transaction environment.
func RunInNewTxn(ctx context.Context, store Storage, retryable bool, f func(ctx context.Context, txn Transaction) error) error {
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

		err = f(ctx, txn)
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

		if val, _err_ := failpoint.Eval(_curpkg_("mockCommitErrorInNewTxn")); _err_ == nil {
			if v := val.(string); len(v) > 0 {
				switch v {
				case "retry_once":
					if i == 0 {
						err = ErrTxnRetryable
					}
				case "no_retry":
					return errors.New("mock commit error")
				}
			}
		}

		if err == nil {
			err = txn.Commit(ctx)
			if err == nil {
				break
			}
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
	sleep := time.Duration(rand.Intn(upper)) * time.Millisecond // #nosec G404
	time.Sleep(sleep)
	return int(sleep)
}
