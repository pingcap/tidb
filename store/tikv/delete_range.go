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
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// DeleteRangeTask is used to delete all keys in a range. After
// performing DeleteRange, it keeps how many ranges it affects and
// if the task was canceled or not.
type DeleteRangeTask struct {
	completedRegions int
	canceled         bool
	store            Storage
	ctx              context.Context
	startKey         []byte
	endKey           []byte
	notifyOnly       bool
}

// NewDeleteRangeTask creates a DeleteRangeTask. Deleting will be performed when `Execute` method is invoked.
// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to TiKV.
func NewDeleteRangeTask(ctx context.Context, store Storage, startKey []byte, endKey []byte) *DeleteRangeTask {
	return &DeleteRangeTask{
		completedRegions: 0,
		canceled:         false,
		store:            store,
		ctx:              ctx,
		startKey:         startKey,
		endKey:           endKey,
		notifyOnly:       false,
	}
}

// NewNotifyDeleteRangeTask creates a task that sends delete range requests to all regions in the range, but with the
// flag `notifyOnly` set. TiKV will not actually delete the range after receiving request, but it will be replicated via
// raft. This is used to notify the involved regions before sending UnsafeDestroyRange requests.
func NewNotifyDeleteRangeTask(ctx context.Context, store Storage, startKey []byte, endKey []byte) *DeleteRangeTask {
	task := NewDeleteRangeTask(ctx, store, startKey, endKey)
	task.notifyOnly = true
	return task
}

// Execute performs the delete range operation.
func (t *DeleteRangeTask) Execute() error {
	startKey, rangeEndKey := t.startKey, t.endKey
	for {
		select {
		case <-t.ctx.Done():
			t.canceled = true
			return errors.Trace(t.ctx.Err())
		default:
		}

		if bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		bo := NewBackoffer(t.ctx, deleteRangeOneRegionMaxBackoff)
		loc, err := t.store.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return errors.Trace(err)
		}

		// Delete to the end of the region, except if it's the last region overlapping the range
		endKey := loc.EndKey
		// If it is the last region
		if loc.Contains(rangeEndKey) {
			endKey = rangeEndKey
		}

		req := &tikvrpc.Request{
			Type: tikvrpc.CmdDeleteRange,
			DeleteRange: &kvrpcpb.DeleteRangeRequest{
				StartKey:   startKey,
				EndKey:     endKey,
				NotifyOnly: t.notifyOnly,
			},
		}

		resp, err := t.store.SendReq(bo, req, loc.Region, ReadTimeoutMedium)
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
			continue
		}
		deleteRangeResp := resp.DeleteRange
		if deleteRangeResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		if err := deleteRangeResp.GetError(); err != "" {
			return errors.Errorf("unexpected delete range err: %v", err)
		}
		t.completedRegions++
		startKey = endKey
	}

	return nil
}

// CompletedRegions returns the number of regions that are affected by this delete range task
func (t *DeleteRangeTask) CompletedRegions() int {
	return t.completedRegions
}

// IsCanceled returns true if the delete range operation was canceled on the half way
func (t *DeleteRangeTask) IsCanceled() bool {
	return t.canceled
}
