// Copyright 2017 PingCAP, Inc.
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
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const splitBatchRegionLimit = 16

func equalRegionStartKey(key, regionStartKey []byte) bool {
	return bytes.Equal(key, regionStartKey)
}

func (s *tikvStore) splitBatchRegionsReq(bo *Backoffer, keys [][]byte, scatter bool, tableID *int64) (*tikvrpc.Response, error) {
	// equalRegionStartKey is used to filter split keys.
	// If the split key is equal to the start key of the region, then the key has been split, we need to skip the split key.
	groups, _, err := s.regionCache.GroupKeysByRegion(bo, keys, equalRegionStartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var batches []batch
	for regionID, groupKeys := range groups {
		batches = appendKeyBatches(batches, regionID, groupKeys, splitBatchRegionLimit)
	}

	if len(batches) == 0 {
		return nil, nil
	}
	// The first time it enters this function.
	if bo.totalSleep == 0 {
		logutil.BgLogger().Info("split batch regions request",
			zap.Int("split key count", len(keys)),
			zap.Int("batch count", len(batches)),
			zap.Uint64("first batch, region ID", batches[0].regionID.id),
			zap.Stringer("first split key", kv.Key(batches[0].keys[0])))
	}
	if len(batches) == 1 {
		resp := s.batchSendSingleRegion(bo, batches[0], scatter, tableID)
		return resp.resp, errors.Trace(resp.err)
	}
	ch := make(chan singleBatchResp, len(batches))
	for _, batch1 := range batches {
		go func(b batch) {
			backoffer, cancel := bo.Fork()
			defer cancel()

			util.WithRecovery(func() {
				select {
				case ch <- s.batchSendSingleRegion(backoffer, b, scatter, tableID):
				case <-bo.ctx.Done():
					ch <- singleBatchResp{err: bo.ctx.Err()}
				}
			}, func(r interface{}) {
				if r != nil {
					ch <- singleBatchResp{err: errors.Errorf("%v", r)}
				}
			})
		}(batch1)
	}

	srResp := &kvrpcpb.SplitRegionResponse{Regions: make([]*metapb.Region, 0, len(keys)*2)}
	for i := 0; i < len(batches); i++ {
		batchResp := <-ch
		if batchResp.err != nil {
			logutil.BgLogger().Info("batch split regions failed", zap.Error(batchResp.err))
			if err == nil {
				err = batchResp.err
			}
		}

		// If the split succeeds and the scatter fails, we also need to add the region IDs.
		if batchResp.resp != nil {
			spResp := batchResp.resp.Resp.(*kvrpcpb.SplitRegionResponse)
			regions := spResp.GetRegions()
			srResp.Regions = append(srResp.Regions, regions...)
		}
	}
	return &tikvrpc.Response{Resp: srResp}, errors.Trace(err)
}

func (s *tikvStore) batchSendSingleRegion(bo *Backoffer, batch batch, scatter bool, tableID *int64) singleBatchResp {
	failpoint.Inject("MockSplitRegionTimeout", func(val failpoint.Value) {
		if val.(bool) {
			if _, ok := bo.ctx.Deadline(); ok {
				<-bo.ctx.Done()
			}
		}
	})

	req := tikvrpc.NewRequest(tikvrpc.CmdSplitRegion, &kvrpcpb.SplitRegionRequest{
		SplitKeys: batch.keys,
	}, kvrpcpb.Context{
		Priority: kvrpcpb.CommandPri_Normal,
	})

	sender := NewRegionRequestSender(s.regionCache, s.client)
	resp, err := sender.SendReq(bo, req, batch.regionID, readTimeoutShort)

	batchResp := singleBatchResp{resp: resp}
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		batchResp.err = errors.Trace(err)
		return batchResp
	}
	if regionErr != nil {
		err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			batchResp.err = errors.Trace(err)
			return batchResp
		}
		resp, err = s.splitBatchRegionsReq(bo, batch.keys, scatter, tableID)
		batchResp.resp = resp
		batchResp.err = err
		return batchResp
	}

	spResp := resp.Resp.(*kvrpcpb.SplitRegionResponse)
	regions := spResp.GetRegions()
	if len(regions) > 0 {
		// Divide a region into n, one of them may not need to be scattered,
		// so n-1 needs to be scattered to other stores.
		spResp.Regions = regions[:len(regions)-1]
	}
	logutil.BgLogger().Info("batch split regions complete",
		zap.Uint64("batch region ID", batch.regionID.id),
		zap.Stringer("first at", kv.Key(batch.keys[0])),
		zap.Stringer("first new region left", stringutil.MemoizeStr(func() string {
			if len(spResp.Regions) == 0 {
				return ""
			}
			return logutil.Hex(spResp.Regions[0]).String()
		})),
		zap.Int("new region count", len(spResp.Regions)))

	if !scatter {
		return batchResp
	}

	for i, r := range spResp.Regions {
		if err = s.scatterRegion(bo, r.Id, tableID); err == nil {
			logutil.BgLogger().Info("batch split regions, scatter region complete",
				zap.Uint64("batch region ID", batch.regionID.id),
				zap.Stringer("at", kv.Key(batch.keys[i])),
				zap.Stringer("new region left", logutil.Hex(r)))
			continue
		}

		logutil.BgLogger().Info("batch split regions, scatter region failed",
			zap.Uint64("batch region ID", batch.regionID.id),
			zap.Stringer("at", kv.Key(batch.keys[i])),
			zap.Stringer("new region left", logutil.Hex(r)),
			zap.Error(err))
		if batchResp.err == nil {
			batchResp.err = err
		}
		if ErrPDServerTimeout.Equal(err) {
			break
		}
	}
	return batchResp
}

// SplitRegions splits regions by splitKeys.
func (s *tikvStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error) {
	bo := NewBackofferWithVars(ctx, int(math.Min(float64(len(splitKeys))*splitRegionBackoff, maxSplitRegionsBackoff)), nil)
	resp, err := s.splitBatchRegionsReq(bo, splitKeys, scatter, tableID)
	regionIDs = make([]uint64, 0, len(splitKeys))
	if resp != nil && resp.Resp != nil {
		spResp := resp.Resp.(*kvrpcpb.SplitRegionResponse)
		for _, r := range spResp.Regions {
			regionIDs = append(regionIDs, r.Id)
		}
		logutil.BgLogger().Info("split regions complete", zap.Int("region count", len(regionIDs)), zap.Uint64s("region IDs", regionIDs))
	}
	return regionIDs, errors.Trace(err)
}

func (s *tikvStore) scatterRegion(bo *Backoffer, regionID uint64, tableID *int64) error {
	logutil.BgLogger().Info("start scatter region",
		zap.Uint64("regionID", regionID))
	for {
		opts := make([]pd.ScatterRegionOption, 0, 1)
		if tableID != nil {
			opts = append(opts, pd.WithGroup(fmt.Sprintf("%v", *tableID)))
		}
		err := s.pdClient.ScatterRegionWithOption(bo.ctx, regionID, opts...)

		failpoint.Inject("MockScatterRegionTimeout", func(val failpoint.Value) {
			if val.(bool) {
				err = ErrPDServerTimeout
			}
		})

		if err == nil {
			break
		}
		err = bo.Backoff(BoPDRPC, errors.New(err.Error()))
		if err != nil {
			return errors.Trace(err)
		}
	}
	logutil.BgLogger().Debug("scatter region complete",
		zap.Uint64("regionID", regionID))
	return nil
}

func (s *tikvStore) preSplitRegion(ctx context.Context, group groupedMutations) bool {
	splitKeys := make([][]byte, 0, 4)

	preSplitSizeThresholdVal := atomic.LoadUint32(&preSplitSizeThreshold)
	regionSize := 0
	keysLength := group.mutations.len()
	valsLength := len(group.mutations.values)
	// The value length maybe zero for pessimistic lock keys
	for i := 0; i < keysLength; i++ {
		regionSize = regionSize + len(group.mutations.keys[i])
		if i < valsLength {
			regionSize = regionSize + len(group.mutations.values[i])
		}
		// The second condition is used for testing.
		if regionSize >= int(preSplitSizeThresholdVal) {
			regionSize = 0
			splitKeys = append(splitKeys, group.mutations.keys[i])
		}
	}
	if len(splitKeys) == 0 {
		return false
	}

	regionIDs, err := s.SplitRegions(ctx, splitKeys, true, nil)
	if err != nil {
		logutil.BgLogger().Warn("2PC split regions failed", zap.Uint64("regionID", group.region.id),
			zap.Int("keys count", keysLength), zap.Int("values count", valsLength), zap.Error(err))
		return false
	}

	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(ctx, regionID, 0)
		if err != nil {
			logutil.BgLogger().Warn("2PC wait scatter region failed", zap.Uint64("regionID", regionID), zap.Error(err))
		}
	}
	// Invalidate the old region cache information.
	s.regionCache.InvalidateCachedRegion(group.region)
	return true
}

// WaitScatterRegionFinish implements SplittableStore interface.
// backOff is the back off time of the wait scatter region.(Milliseconds)
// if backOff <= 0, the default wait scatter back off time will be used.
func (s *tikvStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error {
	if backOff <= 0 {
		backOff = waitScatterRegionFinishBackoff
	}
	logutil.BgLogger().Info("wait scatter region",
		zap.Uint64("regionID", regionID), zap.Int("backoff(ms)", backOff))

	bo := NewBackofferWithVars(ctx, backOff, nil)
	logFreq := 0
	for {
		resp, err := s.pdClient.GetOperator(ctx, regionID)
		if err == nil && resp != nil {
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				logutil.BgLogger().Info("wait scatter region finished",
					zap.Uint64("regionID", regionID))
				return nil
			}
			if resp.GetHeader().GetError() != nil {
				err = errors.AddStack(&PDError{
					Err: resp.Header.Error,
				})
				logutil.BgLogger().Warn("wait scatter region error",
					zap.Uint64("regionID", regionID), zap.Error(err))
				return err
			}
			if logFreq%10 == 0 {
				logutil.BgLogger().Info("wait scatter region",
					zap.Uint64("regionID", regionID),
					zap.String("reverse", string(resp.Desc)),
					zap.String("status", pdpb.OperatorStatus_name[int32(resp.Status)]))
			}
			logFreq++
		}
		if err != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(err.Error()))
		} else {
			err = bo.Backoff(BoRegionMiss, errors.New("wait scatter region timeout"))
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// CheckRegionInScattering uses to check whether scatter region finished.
func (s *tikvStore) CheckRegionInScattering(regionID uint64) (bool, error) {
	bo := NewBackofferWithVars(context.Background(), locateRegionMaxBackoff, nil)
	for {
		resp, err := s.pdClient.GetOperator(context.Background(), regionID)
		if err == nil && resp != nil {
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				return false, nil
			}
		}
		if err != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(err.Error()))
		} else {
			return true, nil
		}
		if err != nil {
			return true, errors.Trace(err)
		}
	}
}
