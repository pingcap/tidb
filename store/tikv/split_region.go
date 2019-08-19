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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitRegion splits the region contains splitKey into 2 regions: [start,
// splitKey) and [splitKey, end).
func (s *tikvStore) SplitRegion(splitKey kv.Key, scatter bool) (regionID uint64, err error) {
	logutil.BgLogger().Info("start split region",
		zap.Stringer("at", splitKey))
	bo := NewBackoffer(context.Background(), splitRegionBackoff)
	sender := NewRegionRequestSender(s.regionCache, s.client)
	req := tikvrpc.NewRequest(tikvrpc.CmdSplitRegion, &kvrpcpb.SplitRegionRequest{
		SplitKey: splitKey,
	}, kvrpcpb.Context{
		Priority: kvrpcpb.CommandPri_Normal,
	})
	for {
		loc, err := s.regionCache.LocateKey(bo, splitKey)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if bytes.Equal(splitKey, loc.StartKey) {
			logutil.BgLogger().Info("skip split region",
				zap.Stringer("at", splitKey))
			return 0, nil
		}
		res, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return 0, errors.Trace(err)
		}
		regionErr, err := res.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}
		splitRegion := res.Resp.(*kvrpcpb.SplitRegionResponse)
		logutil.BgLogger().Info("split region complete",
			zap.Stringer("at", splitKey),
			zap.Stringer("new region left", logutil.Hex(splitRegion.GetLeft())),
			zap.Stringer("new region right", logutil.Hex(splitRegion.GetRight())))
		left := splitRegion.GetLeft()
		if left == nil {
			return 0, nil
		}
		if scatter {
			err = s.scatterRegion(left.Id)
			if err != nil {
				return 0, errors.Trace(err)
			}
		}
		return left.Id, nil
	}
}

func (s *tikvStore) scatterRegion(regionID uint64) error {
	logutil.BgLogger().Info("start scatter region",
		zap.Uint64("regionID", regionID))
	bo := NewBackoffer(context.Background(), scatterRegionBackoff)
	for {
		err := s.pdClient.ScatterRegion(context.Background(), regionID)
		if err != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(err.Error()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		break
	}
	logutil.BgLogger().Info("scatter region complete",
		zap.Uint64("regionID", regionID))
	return nil
}

// WaitScatterRegionFinish implements SplitableStore interface.
// backOff is the back off time of the wait scatter region.(Milliseconds)
// if backOff <= 0, the default wait scatter back off time will be used.
func (s *tikvStore) WaitScatterRegionFinish(regionID uint64, backOff int) error {
	logutil.BgLogger().Info("wait scatter region",
		zap.Uint64("regionID", regionID))
	if backOff <= 0 {
		backOff = waitScatterRegionFinishBackoff
	}
	bo := NewBackoffer(context.Background(), backOff)
	logFreq := 0
	for {
		resp, err := s.pdClient.GetOperator(context.Background(), regionID)
		if err == nil && resp != nil {
			if !bytes.Equal(resp.Desc, []byte("scatter-region")) || resp.Status != pdpb.OperatorStatus_RUNNING {
				logutil.BgLogger().Info("wait scatter region finished",
					zap.Uint64("regionID", regionID))
				return nil
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
	bo := NewBackoffer(context.Background(), locateRegionMaxBackoff)
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
