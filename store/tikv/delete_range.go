// Copyright 2018 PingCAP, Inc.
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

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type DeleteRangeTask struct {
	regions  int
	canceled bool
	store    Storage
	ctx      context.Context
	bo       *Backoffer
	startKey []byte
	endKey   []byte
}

func NewDeleteRangeTask(store Storage, ctx context.Context, bo *Backoffer, startKey []byte, endKey []byte) DeleteRangeTask {
	return DeleteRangeTask{
		regions:  0,
		canceled: false,
		store:    store,
		ctx:      ctx,
		bo:       bo,
		startKey: startKey,
		endKey:   endKey,
	}
}

func (t DeleteRangeTask) Execute() error {
	startKey, rangeEndKey := t.startKey, t.endKey
	for {
		select {
		case <-t.ctx.Done():
			t.canceled = true
			return nil
		default:
		}

		loc, err := t.store.GetRegionCache().LocateKey(t.bo, startKey)
		if err != nil {
			return errors.Trace(err)
		}

		endKey := loc.EndKey
		if loc.Contains(rangeEndKey) {
			endKey = rangeEndKey
		}

		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDeleteRange,
			DeleteRange: &kvrpcpb.DeleteRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			},
		}

		resp, err := t.store.SendReq(t.bo, req, loc.Region, ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = t.bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		deleteRangeResp := resp.DeleteRange
		if deleteRangeResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		if err := deleteRangeResp.GetError(); err != "" {
			return errors.Errorf("unexpected delete range err: %v", err)
		}
		t.regions++
		if bytes.Equal(endKey, rangeEndKey) {
			break
		}
		startKey = endKey
	}

	return nil
}

func (t DeleteRangeTask) Regions() int {
	return t.regions
}

func (t DeleteRangeTask) IsCanceled() bool {
	return t.canceled
}
