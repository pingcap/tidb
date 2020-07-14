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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"math"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionPrewrite struct{}

var _ twoPhaseCommitAction = actionPrewrite{}
var tiKVTxnRegionsNumHistogramPrewrite = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("prewrite"))

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionPrewrite) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramPrewrite
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchMutations, txnSize uint64) *tikvrpc.Request {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mutations[i] = &pb.Mutation{
			Op:    m.ops[i],
			Key:   m.keys[i],
			Value: m.values[i],
		}
	}
	var minCommitTS uint64
	if c.forUpdateTS > 0 {
		minCommitTS = c.forUpdateTS + 1
	} else {
		minCommitTS = c.startTS + 1
	}

	failpoint.Inject("mockZeroCommitTS", func(val failpoint.Value) {
		// Should be val.(uint64) but failpoint doesn't support that.
		if tmp, ok := val.(int); ok && uint64(tmp) == c.startTS {
			minCommitTS = 0
		}
	})

	req := &pb.PrewriteRequest{
		Mutations:         mutations,
		PrimaryLock:       c.primary(),
		StartVersion:      c.startTS,
		LockTtl:           c.lockTTL,
		IsPessimisticLock: m.isPessimisticLock,
		ForUpdateTs:       c.forUpdateTS,
		TxnSize:           txnSize,
		MinCommitTs:       minCommitTS,
	}

	if config.GetGlobalConfig().TiKVClient.EnableAsyncCommit {
		if batch.isPrimary {
			req.Secondaries = c.asyncSecondaries()
		}
		req.UseAsyncCommit = true
	}

	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
}

func (action actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	txnSize := uint64(c.regionTxnSize[batch.region.id])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve lock lite".
	if len(bo.errors) > 0 {
		txnSize = math.MaxUint64
	}

	req := c.buildPrewriteRequest(batch, txnSize)
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.prewriteMutations(bo, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*pb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			if bytes.Equal(c.primary(), batch.mutations.keys[0]) {
				// After writing the primary key, if the size of the transaction is large than 32M,
				// start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
				if int64(c.txnSize) > config.GetGlobalConfig().TiKVClient.TTLRefreshedTxnSize {
					c.run(c, nil)
				}
			}
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				existErrInfo := c.txn.us.GetKeyExistErrInfo(key)
				if existErrInfo == nil {
					return errors.Errorf("conn %d, existErr for key:%s should not be nil", c.connID, key)
				}
				return existErrInfo.Err()
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.BgLogger().Info("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}
		start := time.Now()
		msBeforeExpired, err := c.store.lockResolver.resolveLocksForWrite(bo, c.startTS, locks)
		if err != nil {
			return errors.Trace(err)
		}
		atomic.AddInt64(&c.getDetail().ResolveLockTime, int64(time.Since(start)))
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *twoPhaseCommitter) prewriteMutations(bo *Backoffer, mutations committerMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnMutations(bo, actionPrewrite{}, mutations)
}
