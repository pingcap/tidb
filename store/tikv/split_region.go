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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"go.uber.org/zap"
)

// SplitRegion splits the region contains splitKey into 2 regions: [start,
// splitKey) and [splitKey, end).
func (s *tikvStore) SplitRegion(splitKey kv.Key) error {
	log.Info("start split_region",
		zap.Binary("at", splitKey))
	bo := NewBackoffer(context.Background(), splitRegionBackoff)
	sender := NewRegionRequestSender(s.regionCache, s.client)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdSplitRegion,
		SplitRegion: &kvrpcpb.SplitRegionRequest{
			SplitKey: splitKey,
		},
	}
	req.Context.Priority = kvrpcpb.CommandPri_Normal
	for {
		loc, err := s.regionCache.LocateKey(bo, splitKey)
		if err != nil {
			return errors.Trace(err)
		}
		if bytes.Equal(splitKey, loc.StartKey) {
			log.Info("skip split_region region",
				zap.Binary("at", splitKey))
			return nil
		}
		res, err := sender.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := res.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		log.Info("split_region complete",
			zap.Binary("at", splitKey),
			zap.String("new region left", res.SplitRegion.GetLeft().String()),
			zap.String("new region right", res.SplitRegion.GetRight().String()))
		return nil
	}
}
