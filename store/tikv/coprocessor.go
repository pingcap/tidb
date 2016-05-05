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

package tikv

import (
	"bytes"
	"io"
	"io/ioutil"
	"sync"

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
	it := &copIterator{
		store:       c.store,
		req:         req,
		tasks:       tasks,
		concurrency: req.Concurrency,
	}
	if it.concurrency < 1 {
		it.concurrency = 1
	}
	if it.req.KeepOrder {
		it.respChan = make(chan *coprocessor.Response, it.concurrency)
	}
	it.errChan = make(chan error, 1)
	it.run()
	return it
}

const (
	taskNew int = iota
	taskRunning
	taskDone
	taskFetched
)

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region *Region
	ranges []kv.KeyRange

	status   int
	idx      int // Index of task in the tasks slice.
	respChan chan *coprocessor.Response
}

func (t *copTask) pbRanges() []*coprocessor.KeyRange {
	ranges := make([]*coprocessor.KeyRange, 0, len(t.ranges))
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
			idx:    len(tasks),
			region: region,
			status: taskNew,
		}
		last.respChan = make(chan *coprocessor.Response)
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

	mu          sync.RWMutex
	reqSent     int
	respGot     int
	concurrency int
	respChan    chan *coprocessor.Response
	errChan     chan error
	finished    bool
}

func (it *copIterator) work() {
	for {
		if it.finished {
			break
		}
		it.mu.Lock()
		// Get task
		var task *copTask
		if it.req.Desc {
			for i := len(it.tasks) - 1; i >= 0; i-- {
				if it.tasks[i].status == taskNew {
					task = it.tasks[i]
					break
				}
			}
		} else {
			for _, t := range it.tasks {
				if t.status == taskNew {
					task = t
					break
				}
			}
		}
		it.mu.Unlock()
		if task == nil {
			break
		}
		task.status = taskRunning
		resp, err := it.handleTask(task)
		if err != nil {
			it.errChan <- err
			break
		}
		if !it.req.KeepOrder {
			it.respChan <- resp
		} else {
			task.respChan <- resp
		}
	}
}

func (it *copIterator) run() {
	for i := 0; i < it.concurrency; i++ {
		go it.work()
	}
}

// Return next coprocessor result.
func (it *copIterator) Next() (io.ReadCloser, error) {
	// TODO: Support `Concurrent` instruction in `kv.Request`.
	if it.finished {
		return nil, nil
	}
	var (
		resp *coprocessor.Response
		err  error
	)
	if !it.req.KeepOrder {
		// Get next fetched resp from chan
		select {
		case resp = <-it.respChan:
		case err = <-it.errChan:
		}
	} else {
		var task *copTask
		it.mu.Lock()
		for _, t := range it.tasks {
			if t.status != taskDone {
				task = t
				break
			}
		}
		it.mu.Unlock()
		if task == nil {
			it.Close()
			return nil, nil
		}
		select {
		case resp = <-task.respChan:
		case err = <-it.errChan:
		}
	}
	if err != nil {
		it.Close()
		return nil, err
	}
	it.respGot++
	// TODO: Check this
	if it.respGot == len(it.tasks) {
		it.Close()
	}
	return ioutil.NopCloser(bytes.NewBuffer(resp.Data)), nil
}

// Handle single task.
func (it *copIterator) handleTask(task *copTask) (*coprocessor.Response, error) {
	var backoffErr error
	for backoff := rpcBackoff(); backoffErr == nil; backoffErr = backoff() {
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
			it.store.regionCache.NextStore(task.region.GetID())
			err = it.rebuildCurrentTask(task)
			if err != nil {
				return nil, errors.Trace(err)
			}
			log.Warnf("send coprocessor request error: %v, try next store later", err)
			continue
		}
		if e := resp.GetRegionError(); e != nil {
			if notLeader := e.GetNotLeader(); notLeader != nil {
				it.store.regionCache.UpdateLeader(notLeader.GetRegionId(), notLeader.GetLeaderStoreId())
			} else {
				it.store.regionCache.DropRegion(task.region.GetID())
			}
			err = it.rebuildCurrentTask(task)
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
		task.status = taskDone
		return resp, nil
	}
	return nil, errors.Trace(backoffErr)
}

func (it *copIterator) rebuildCurrentTask(task *copTask) error {
	newTasks, err := buildCopTasks(it.store.regionCache, task.ranges)
	if err != nil {
		return errors.Trace(err)
	}
	if len(newTasks) == 0 {
		// TODO: check this
		return nil
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	var t *copTask
	if it.req.Desc {
		t = newTasks[len(newTasks)-1]
	} else {
		t = newTasks[0]
	}
	task.region = t.region
	task.ranges = t.ranges
	close(t.respChan)
	// We should keep the original channel, because someone may be waiting on the channel.
	t.respChan = task.respChan
	if it.req.Desc {
		newTasks[len(newTasks)-1] = task
	} else {
		newTasks[0] = task
	}

	//fmt.Printf("tasks: %d, newTasks: %d, idx: %d\n", len(it.tasks), len(newTasks), task.idx)
	it.tasks = append(it.tasks[:task.idx], append(newTasks, it.tasks[task.idx+1:]...)...)
	// Update index
	for i := task.idx; i < len(it.tasks); i++ {
		it.tasks[i].idx = i
	}
	return nil
}

func (it *copIterator) Close() error {
	it.finished = true
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
