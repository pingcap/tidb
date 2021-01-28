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
	"encoding/hex"
	"math"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionPrewrite struct{}

var _ twoPhaseCommitAction = actionPrewrite{}
var tiKVTxnRegionsNumHistogramPrewrite = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("prewrite"))
var tikvOnePCTxnCounterFallback = metrics.TiKVOnePCTxnCounter.WithLabelValues("fallback")

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionPrewrite) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramPrewrite
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchMutations, txnSize uint64) *tikvrpc.Request {
	m := batch.mutations
	mutations := make([]*pb.Mutation, m.Len())
	isPessimisticLock := make([]bool, m.Len())
	for i := 0; i < m.Len(); i++ {
		mutations[i] = &pb.Mutation{
			Op:    m.GetOp(i),
			Key:   m.GetKey(i),
			Value: m.GetValue(i),
		}
		isPessimisticLock[i] = m.IsPessimisticLock(i)
	}
	c.mu.Lock()
	minCommitTS := c.minCommitTS
	c.mu.Unlock()
	if c.forUpdateTS > 0 && c.forUpdateTS >= minCommitTS {
		minCommitTS = c.forUpdateTS + 1
	} else if c.startTS >= minCommitTS {
		minCommitTS = c.startTS + 1
	}

	failpoint.Inject("mockZeroCommitTS", func(val failpoint.Value) {
		// Should be val.(uint64) but failpoint doesn't support that.
		if tmp, ok := val.(int); ok && uint64(tmp) == c.startTS {
			minCommitTS = 0
		}
	})

	ttl := c.lockTTL

	if c.connID > 0 {
		failpoint.Inject("twoPCShortLockTTL", func() {
			ttl = 1
			keys := make([]string, 0, len(mutations))
			for _, m := range mutations {
				keys = append(keys, hex.EncodeToString(m.Key))
			}
			logutil.BgLogger().Info("[failpoint] injected lock ttl = 1 on prewrite",
				zap.Uint64("txnStartTS", c.startTS), zap.Strings("keys", keys))
		})
	}

	req := &pb.PrewriteRequest{
		Mutations:         mutations,
		PrimaryLock:       c.primary(),
		StartVersion:      c.startTS,
		LockTtl:           ttl,
		IsPessimisticLock: isPessimisticLock,
		ForUpdateTs:       c.forUpdateTS,
		TxnSize:           txnSize,
		MinCommitTs:       minCommitTS,
		MaxCommitTs:       c.maxCommitTS,
	}

	failpoint.Inject("invalidMaxCommitTS", func() {
		if req.MaxCommitTs > 0 {
			req.MaxCommitTs = minCommitTS - 1
		}
	})

	if c.isAsyncCommit() {
		if batch.isPrimary {
			req.Secondaries = c.asyncSecondaries()
		}
		req.UseAsyncCommit = true
	}

	if c.isOnePC() {
		req.TryOnePc = true
	}

	return tikvrpc.NewRequest(tikvrpc.CmdPrewrite, req, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
}

func (action actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	// WARNING: This function only tries to send a single request to a single region, so it don't
	// need to unset the `useOnePC` flag when it fails. A special case is that when TiKV returns
	// regionErr, it's uncertain if the request will be splitted into multiple and sent to multiple
	// regions. It invokes `prewriteMutations` recursively here, and the number of batches will be
	// checked there.

	if c.connID > 0 {
		failpoint.Inject("prewritePrimaryFail", func() {
			if batch.isPrimary {
				logutil.Logger(bo.ctx).Info("[failpoint] injected error on prewriting primary batch",
					zap.Uint64("txnStartTS", c.startTS))
				failpoint.Return(errors.New("injected error on prewriting primary batch"))
			}
		})
	}

	txnSize := uint64(c.regionTxnSize[batch.region.id])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve lock lite".
	if len(bo.errors) > 0 {
		txnSize = math.MaxUint64
	}

	req := c.buildPrewriteRequest(batch, txnSize)
	for {
		sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
		resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

		// If we fail to receive response for async commit prewrite, it will be undetermined whether this
		// transaction has been successfully committed.
		// If prewrite has been cancelled, all ongoing prewrite RPCs will become errors, we needn't set undetermined
		// errors.
		if (c.isAsyncCommit() || c.isOnePC()) && sender.rpcError != nil && atomic.LoadUint32(&c.prewriteCancelled) == 0 {
			c.setUndeterminedErr(errors.Trace(sender.rpcError))
		}

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
			if batch.isPrimary {
				// After writing the primary key, if the size of the transaction is larger than 32M,
				// start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
				// In this case 1PC is not expected to be used, but still check it for safety.
				if int64(c.txnSize) > config.GetGlobalConfig().TiKVClient.TTLRefreshedTxnSize &&
					prewriteResp.OnePcCommitTs == 0 {
					c.run(c, nil)
				}
			}

			if c.isOnePC() {
				if prewriteResp.OnePcCommitTs == 0 {
					if prewriteResp.MinCommitTs != 0 {
						return errors.Trace(errors.New("MinCommitTs must be 0 when 1pc falls back to 2pc"))
					}
					logutil.Logger(bo.ctx).Warn("1pc failed and fallbacks to normal commit procedure",
						zap.Uint64("startTS", c.startTS))
					tikvOnePCTxnCounterFallback.Inc()
					c.setOnePC(false)
					c.setAsyncCommit(false)
				} else {
					// For 1PC, there's no racing to access to access `onePCCommmitTS` so it's safe
					// not to lock the mutex.
					if c.onePCCommitTS != 0 {
						logutil.Logger(bo.ctx).Fatal("one pc happened multiple times",
							zap.Uint64("startTS", c.startTS))
					}
					c.onePCCommitTS = prewriteResp.OnePcCommitTs
				}
				return nil
			} else if prewriteResp.OnePcCommitTs != 0 {
				logutil.Logger(bo.ctx).Fatal("tikv committed a non-1pc transaction with 1pc protocol",
					zap.Uint64("startTS", c.startTS))
			}
			if c.isAsyncCommit() {
				// 0 if the min_commit_ts is not ready or any other reason that async
				// commit cannot proceed. The client can then fallback to normal way to
				// continue committing the transaction if prewrite are all finished.
				if prewriteResp.MinCommitTs == 0 {
					if c.testingKnobs.noFallBack {
						return nil
					}
					logutil.Logger(bo.ctx).Warn("async commit cannot proceed since the returned minCommitTS is zero, "+
						"fallback to normal path", zap.Uint64("startTS", c.startTS))
					c.setAsyncCommit(false)
				} else {
					c.mu.Lock()
					if prewriteResp.MinCommitTs > c.minCommitTS {
						c.minCommitTS = prewriteResp.MinCommitTs
					}
					c.mu.Unlock()
				}
			}
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				return c.extractKeyExistsErr(key)
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

func (c *twoPhaseCommitter) prewriteMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	// `doActionOnMutations` will unset `useOnePC` if the mutations is splitted into multiple batches.
	return c.doActionOnMutations(bo, actionPrewrite{}, mutations)
}
