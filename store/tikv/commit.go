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
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionCommit struct{ retry bool }

var _ twoPhaseCommitAction = actionCommit{}

func (actionCommit) String() string {
	return "commit"
}

func (actionCommit) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramCommit
}

func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	keys := batch.mutations.GetKeys()
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &pb.CommitRequest{
		StartVersion:  c.startTS,
		Keys:          keys,
		CommitVersion: c.commitTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})

	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	resp, err := sender.SendReq(bo, req, batch.region, ReadTimeoutShort)

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	if batch.isPrimary && sender.rpcError != nil {
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
		// re-split keys and commit again.
		err = c.doActionOnMutations(bo, actionCommit{retry: true}, batch.mutations)
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(tikverr.ErrBodyMissing)
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

			// Do not retry for a txn which has a too large MinCommitTs
			// 3600000 << 18 = 943718400000
			if rejected.MinCommitTs-rejected.AttemptedCommitTs > 943718400000 {
				err := errors.Errorf("2PC MinCommitTS is too large, we got MinCommitTS: %d, and AttemptedCommitTS: %d",
					rejected.MinCommitTs, rejected.AttemptedCommitTs)
				return errors.Trace(err)
			}

			// Update commit ts and retry.
			commitTS, err := c.store.getTimestampWithRetry(bo, c.txn.GetUnionStore().GetOption(kv.TxnScope).(string))
			if err != nil {
				logutil.Logger(bo.ctx).Warn("2PC get commitTS failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(err)
			}

			c.mu.Lock()
			c.commitTS = commitTS
			c.mu.Unlock()
			return c.commitMutations(bo, batch.mutations)
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
				zap.Strings("keys", hexBatchKeys(keys)))
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		logutil.Logger(bo.ctx).Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (c *twoPhaseCommitter) commitMutations(bo *Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.commitMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnMutations(bo, actionCommit{}, mutations)
}
