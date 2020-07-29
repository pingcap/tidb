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
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type actionCleanup struct {
}

var _ twoPhaseCommitAction = actionCleanup{}
var tiKVTxnRegionsNumHistogramCleanup = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("cleanup"))

func (actionCleanup) String() string {
	return "cleanup"
}

func (actionCleanup) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramCleanup
}

func (actionCleanup) collectMutation(acc *CommitterMutations, m *mutation) {
	acc.keys = append(acc.keys, m.key)
}

func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch *batchMutations) (bool, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, &pb.BatchRollbackRequest{
		Keys:         batch.mutations.keys,
		StartVersion: c.startTS,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
	resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
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
	if keyErr := resp.Resp.(*pb.BatchRollbackResponse).GetError(); keyErr != nil {
		err = errors.Errorf("conn %d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.BgLogger().Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return false, errors.Trace(err)
	}
	return false, nil
}

func (c *twoPhaseCommitter) cleanupMutations(bo *Backoffer, mutations mutations) error {
	exec := c.newExecController(mutations, actionCleanup{})
	return exec.run(bo)
}

func (c *twoPhaseCommitter) cleanupTxnMutations(bo *Backoffer) error {
	return c.cleanupMutations(bo, committerTxnMutations{c, false})
}
