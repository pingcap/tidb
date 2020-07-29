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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionCommit struct {
	retry bool
}

var _ twoPhaseCommitAction = actionCommit{}

var tikvSecondaryLockCleanupFailureCounterCommit = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit")
var tiKVTxnRegionsNumHistogramCommit = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("commit"))

func (actionCommit) String() string {
	return "commit"
}

func (actionCommit) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramCommit
}

func (actionCommit) collectMutation(acc *CommitterMutations, m *mutation) {
	acc.keys = append(acc.keys, m.key)
}

func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch *batchMutations) (bool, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &pb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          batch.mutations.keys,
		CommitVersion: c.commitTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})

	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	if batch.isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	if err != nil {
		return false, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, errors.Trace(err)
	}
	if regionErr != nil {
		return true, errors.Trace(errors.New(regionErr.String()))
	}
	if resp.Resp == nil {
		return false, errors.Trace(ErrBodyMissing)
	}
	commitResp := resp.Resp.(*pb.CommitResponse)
	// Here we can make sure tikv has processed the commit primary key request. So
	// we can clean undetermined error.
	if batch.isPrimary {
		c.setUndeterminedErr(nil)
	}
	if keyErr := commitResp.GetError(); keyErr != nil {
		if rejected := keyErr.GetCommitTsExpired(); rejected != nil {
			logutil.Logger(bo.ctx).Info("2PC commitTS rejected by TiKV, retry with a newer commitTS",
				zap.Uint64("txnStartTS", c.startTS),
				zap.Stringer("info", logutil.Hex(rejected)))

			// Update commit ts and retry.
			commitTS, err := c.store.getTimestampWithRetry(bo)
			if err != nil {
				logutil.Logger(bo.ctx).Warn("2PC get commitTS failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return false, errors.Trace(err)
			}

			c.mu.Lock()
			c.commitTS = commitTS
			c.mu.Unlock()
			return true, nil
		}

		c.mu.RLock()
		defer c.mu.RUnlock()
		err = extractKeyErr(keyErr)
		if c.mu.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			hexBatchKeys := func(keys [][]byte) []string {
				var res []string
				for _, k := range keys {
					res = append(res, hex.EncodeToString(k))
				}
				return res
			}
			logutil.Logger(bo.ctx).Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS),
				zap.Uint64("commitTS", c.commitTS),
				zap.Strings("keys", hexBatchKeys(batch.mutations.keys)))
			return false, errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		logutil.Logger(bo.ctx).Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return false, nil
}

func (c *twoPhaseCommitter) commitMutations(bo *Backoffer, mutations mutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.commitMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	exec := c.newExecController(mutations, actionCommit{})
	return exec.run(bo)
}

func (c *twoPhaseCommitter) commitTxnMutations(bo *Backoffer) error {
	return c.commitMutations(bo, committerTxnMutations{c, false})
}
