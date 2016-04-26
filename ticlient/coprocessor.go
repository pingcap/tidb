// Copyright 2016 PingCAP, Inc.
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

package ticlient

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tipb/go-tipb"
)

// CopClient is coprocessor client.
type CopClient struct {
	store *tikvStore
}

// SupportRequestType checks whether reqType is supported.
func (c *CopClient) SupportRequestType(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect:
		return supportExpr(tipb.ExprType(subType))
	case kv.ReqTypeIndex:
		return subType == kv.ReqSubTypeBasic
	}
	return false
}

func supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_ColumnRef,
		tipb.ExprType_And, tipb.ExprType_Or,
		tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList,
		tipb.ExprType_Like, tipb.ExprType_Not:
		return true
	default:
		return false
	}
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(req *kv.Request) kv.Response {
	tasks, err := buildCopTasks(c.store.regionCache, req.KeyRanges)
	if err != nil {
		return copErrorResponse{err}
	}
	index := 0
	if req.Desc {
		index = len(tasks) - 1
	}
	return &copIterator{
		store: c.store,
		req:   req,
		tasks: tasks,
		index: index,
	}
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region *Region
	ranges []kv.KeyRange
}

func (t *copTask) pbRanges() []*coprocessor.KeyRange {
	var ranges []*coprocessor.KeyRange
	for _, r := range t.ranges {
		ranges = append(ranges, &coprocessor.KeyRange{
			Start: r.StartKey,
			End:   r.EndKey,
		})
	}
	return ranges
}

func buildCopTasks(cache *RegionCache, ranges []kv.KeyRange) ([]*copTask, error) {
	var tasks []*copTask
	for _, r := range ranges {
		var err error
		if tasks, err = appendTask(tasks, cache, r); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tasks, nil
}

func appendTask(tasks []*copTask, cache *RegionCache, r kv.KeyRange) ([]*copTask, error) {
	var last *copTask
	if len(tasks) > 0 {
		last = tasks[len(tasks)-1]
	}
	// Ensure `r` (or part of `r`) is inside `last`, create a task if need.
	if last == nil || !last.region.Contains(r.StartKey) {
		region, err := cache.GetRegion(r.StartKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		last = &copTask{
			region: region,
		}
		tasks = append(tasks, last)
	}
	if last.region.Contains(r.EndKey) || bytes.Equal(last.region.EndKey(), r.EndKey) {
		// The whole range is inside last task.
		last.ranges = append(last.ranges, r)
	} else {
		// Part of r is not in the range of last task.
		last.ranges = append(last.ranges, kv.KeyRange{
			StartKey: r.StartKey,
			EndKey:   last.region.EndKey(),
		})
		remain := kv.KeyRange{
			StartKey: last.region.EndKey(),
			EndKey:   r.EndKey,
		}
		return appendTask(tasks, cache, remain)
	}
	return tasks, nil
}

type copIterator struct {
	store *tikvStore
	req   *kv.Request
	tasks []*copTask
	index int // index indicates the next task to be executed
}

func (it *copIterator) Next() (io.ReadCloser, error) {
	// TODO: Support `Concurrent` instruction in `kv.Request`.

	var backoffErr error
	for backoff := rpcBackoff(); backoffErr == nil; backoffErr = backoff() {
		if it.index < 0 || it.index >= len(it.tasks) {
			return nil, nil
		}
		task := it.tasks[it.index]

		client, err := it.store.getClient(task.region.GetAddress())
		if err != nil {
			return nil, errors.Trace(err)
		}
		req := &coprocessor.Request{
			Context: task.region.GetContext(),
			Tp:      proto.Int64(it.req.Tp),
			Data:    it.req.Data,
			Ranges:  task.pbRanges(),
		}
		resp, err := client.SendCopReq(req)
		if err != nil {
			it.store.regionCache.NextPeer(task.region.GetID())
			err = it.rebuildCurrentTask()
			if err != nil {
				return nil, errors.Trace(err)
			}
			log.Warnf("send coprocessor request error: %v, try next peer later", err)
			continue
		}
		if e := resp.GetRegionError(); e != nil {
			if notLeader := e.GetNotLeader(); notLeader != nil {
				it.store.regionCache.UpdateLeader(notLeader.GetRegionId(), notLeader.GetLeader().GetId())
			} else {
				it.store.regionCache.DropRegion(task.region.GetID())
			}
			err = it.rebuildCurrentTask()
			if err != nil {
				return nil, errors.Trace(err)
			}
			log.Warnf("coprocessor region error: %v, retry later", e)
			continue
		}
		if e := resp.GetLocked(); e != nil {
			lock := newLock(it.store, e.GetPrimaryLock(), e.GetLockVersion(), e.GetKey(), e.GetLockVersion())
			_, lockErr := lock.cleanup()
			if lockErr == nil || terror.ErrorEqual(lockErr, errInnerRetryable) {
				continue
			}
			log.Warnf("cleanup lock error: %v", lockErr)
			return nil, errors.Trace(lockErr)
		}
		if e := resp.GetOtherError(); e != "" {
			err = errors.Errorf("other error: %s", e)
			log.Warnf("coprocessor err: %v", err)
			return nil, errors.Trace(err)
		}

		if it.req.Desc {
			it.index--
		} else {
			it.index++
		}

		return ioutil.NopCloser(bytes.NewBuffer(resp.Data)), nil
	}

	return nil, errors.Trace(backoffErr)
}

func (it *copIterator) rebuildCurrentTask() error {
	task := it.tasks[it.index]
	newTasks, err := buildCopTasks(it.store.regionCache, task.ranges)
	if err != nil {
		return errors.Trace(err)
	}
	if it.req.Desc {
		it.tasks = append(it.tasks[:it.index], newTasks...)
		it.index = len(it.tasks) - 1
	} else {
		it.tasks = append(newTasks, it.tasks[it.index+1:]...)
		it.index = 0
	}
	return nil
}

func (it *copIterator) Close() error {
	return nil
}

// copErrorResponse returns error when calling Next()
type copErrorResponse struct{ error }

func (it copErrorResponse) Next() (io.ReadCloser, error) {
	return nil, it.error
}

func (it copErrorResponse) Close() error {
	return nil
}
