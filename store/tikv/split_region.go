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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitRegion splits the region contains splitKey into 2 regions: [start,
// splitKey) and [splitKey, end).
func (s *tikvStore) SplitRegion(splitKey kv.Key) error {
	logutil.Logger(context.Background()).Info("start split region",
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
			logutil.Logger(context.Background()).Info("skip split region",
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
		logutil.Logger(context.Background()).Info("split region complete",
			zap.Binary("at", splitKey),
			zap.Stringer("new region left", res.SplitRegion.GetLeft()),
			zap.Stringer("new region right", res.SplitRegion.GetRight()))
		return nil
	}
}
