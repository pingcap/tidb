// Copyright 2019 PingCAP, Inc.
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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type prewriteLoop struct {
	keysC       chan [][]byte
	batchC      chan []batchKeys
	futureC     chan *RetryableRespFuture
	resolveC    chan *resolveTask
	c           *twoPhaseCommitter
	bo          *Backoffer
	done        chan struct{}
	err         error
	resolveInit sync.Once
}

type resolveTask struct {
	keyErrors []*pb.KeyError
	batch     batchKeys
}

func (p *prewriteLoop) writeError(err error) {
	p.err = err
	close(p.done)
}

func (p *prewriteLoop) splitLoop() {
	for {
		select {
		case keys := <-p.keysC:
			if len(keys) == 0 {
				continue
			}
			groups, firstRegion, err := p.c.store.regionCache.GroupKeysByRegion(p.bo, keys)
			if err != nil {
				p.writeError(err)
				return
			}
			metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_prewrite").Observe(float64(len(groups)))

			var batches []batchKeys
			var sizeFunc = p.c.keyValueSize

			atomic.AddInt32(&p.c.getDetail().PrewriteRegionNum, int32(len(groups)))

			// Make sure the group that contains primary key goes first.
			batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
			delete(groups, firstRegion)
			for id, g := range groups {
				batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
			}
			p.batchC <- batches
		case <-p.done:
			return
		}
	}
}

func (p *prewriteLoop) sendLoop() {
	for {
		select {
		case bs := <-p.batchC:
			for _, batch := range bs {
				txnSize := uint64(math.MaxUint64)
				req := p.c.buildPrewriteRequest(batch, txnSize)
				sender := NewRegionRequestSender(p.c.store.regionCache, p.c.store.client)
				respFuture, err := sender.SendReqPipeline(p.bo, batch.region, req, readTimeoutShort)
				if err != nil {
					p.writeError(err)
					return
				}
				if respFuture == nil {
					// Get region-cache miss, need re-split keys.
					p.keysC <- batch.keys
					continue
				}
				respFuture.batch = batch
				p.futureC <- respFuture
			}
		case <-p.done:
			return
		}
	}
}

func (p *prewriteLoop) recvLoop() {
	for {
		select {
		case respFuture := <-p.futureC:
			resp, err := respFuture.WaitDo()
			if err != nil {
				needSleep, err := respFuture.HandleSendFail(err)
				if err != nil {
					p.writeError(err)
					return
				}
				go func() { // TODO: refine this with timer-heap
					select {
					case <-time.After(time.Duration(needSleep) * time.Millisecond):
						retryFuture, err := respFuture.RetrySend()
						if err != nil {
							p.writeError(err)
							return
						}
						if retryFuture == nil {
							p.keysC <- respFuture.batch.keys
							return
						}
						p.futureC <- retryFuture
					case <-p.bo.ctx.Done():
						p.writeError(p.bo.ctx.Err())
						return
					case <-p.done:
						return
					}
				}()
				break
			}

			regionErr, err := resp.GetRegionError()
			if err != nil {
				p.writeError(err)
				return
			}
			if regionErr != nil {
				retry, needSleep, err := respFuture.HandleRegionFail(regionErr)
				if err != nil {
					p.writeError(err)
					return
				}
				if retry {
					go func() { // TODO: refine this with timer-heap
						select {
						case <-time.After(time.Duration(needSleep) * time.Millisecond):
							retryFuture, err := respFuture.RetrySend()
							if err != nil {
								p.writeError(err)
								return
							}
							if retryFuture == nil {
								p.keysC <- respFuture.batch.keys
								return
							}
							p.futureC <- retryFuture
						case <-p.bo.ctx.Done():
							p.writeError(p.bo.ctx.Err())
							return
						case <-p.done:
							return
						}
					}()
				}
			}

			if resp.Resp == nil {
				p.writeError(errors.Trace(ErrBodyMissing))
				return
			}
			prewriteResp := resp.Resp.(*pb.PrewriteResponse)
			keyErrs := prewriteResp.GetErrors()
			if len(keyErrs) > 0 {
				p.resolveInit.Do(func() {
					go util.WithRecovery(p.resolveLoop, nil)
				})
				p.resolveC <- &resolveTask{
					keyErrors: keyErrs,
					batch:     respFuture.batch,
				}
				break
			}
			close(p.done)
			return
		case <-p.done:
			return
		}
	}
}

func (p *prewriteLoop) resolveLoop() {
	for {
		select {
		case task := <-p.resolveC:
			var locks []*Lock
			for _, keyErr := range task.keyErrors {
				// Check already exists error
				if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
					key := alreadyExist.GetKey()
					existErrInfo := p.c.txn.us.GetKeyExistErrInfo(key)
					if existErrInfo == nil {
						p.writeError(errors.Errorf("conn %d, existErr for key:%s should not be nil", p.c.connID, key))
						return
					}
					p.writeError(existErrInfo.Err())
					return
				}

				// Extract lock from key error
				lock, err1 := extractLockFromKeyErr(keyErr)
				if err1 != nil {
					p.writeError(err1)
					return
				}
				logutil.BgLogger().Debug("prewrite encounters lock",
					zap.Uint64("conn", p.c.connID),
					zap.Stringer("lock", lock))
				locks = append(locks, lock)
			}

			start := time.Now()
			msBeforeExpired, err := p.c.store.lockResolver.ResolveLocks(p.bo, locks)
			if err != nil {
				p.writeError(err)
				return
			}
			atomic.AddInt64(&p.c.getDetail().ResolveLockTime, int64(time.Since(start)))
			if msBeforeExpired > 0 {
				var needSleep int
				needSleep, err = p.bo.BackoffWithMaxSleepAsync(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
				if err != nil {
					p.writeError(err)
					return
				}
				go func() { // TODO: refine this with timer-heap
					select {
					case <-time.After(time.Duration(needSleep) * time.Millisecond):
						p.batchC <- []batchKeys{task.batch}
						return
					case <-p.bo.ctx.Done():
						p.writeError(p.bo.ctx.Err())
						return
					case <-p.done:
						return
					}
				}()
			}
		case <-p.done:
			return
		}
	}
}

func (c *twoPhaseCommitter) prewriteBatches(bo *Backoffer, batches [][]byte) error {
	loops := &prewriteLoop{
		c:       c,
		bo:      bo,
		keysC:   make(chan [][]byte),
		batchC:  make(chan []batchKeys),
		futureC: make(chan *RetryableRespFuture, len(batches)),
		done:    make(chan struct{}),
	}
	go util.WithRecovery(loops.splitLoop, nil)
	go util.WithRecovery(loops.sendLoop, nil)
	go util.WithRecovery(loops.recvLoop, nil)

	loops.keysC <- batches

	select {
	case <-loops.done:
		return loops.err
	case <-bo.ctx.Done():
		return bo.ctx.Err()
	}
}
