// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt          = int(3 * time.Second / newSessionRetryInterval)

	// NewSessionDefaultRetryCnt is the default retry times when create new session.
	NewSessionDefaultRetryCnt = 3
	// NewSessionRetryUnlimited is the unlimited retry times when create new session.
	NewSessionRetryUnlimited = math.MaxInt64
)

// NewSession creates a new etcd session.
func NewSession(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if err = contextDone(ctx, err); err != nil {
			return etcdSession, errors.Trace(err)
		}

		failpoint.Inject("closeClient", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.Close(); err != nil {
					failpoint.Return(etcdSession, errors.Trace(err))
				}
			}
		})

		failpoint.Inject("closeGrpc", func(val failpoint.Value) {
			if val.(bool) {
				if err := etcdCli.ActiveConnection().Close(); err != nil {
					failpoint.Return(etcdSession, errors.Trace(err))
				}
			}
		})

		startTime := time.Now()
		etcdSession, err = concurrency.NewSession(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		metrics.NewSessionHistogram.WithLabelValues(logPrefix, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			logutil.BgLogger().Warn("failed to new session to etcd", zap.String("ownerInfo", logPrefix), zap.Error(err))
		}

		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		terror.ErrorEqual(err, grpc.ErrClientConnClosing) {
		return errors.Trace(err)
	}

	return nil
}
