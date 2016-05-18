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
		switch subType {
		case kv.ReqSubTypeDesc, kv.ReqSubTypeBasic:
			return true
		}
	}
	return false
}

func supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_ColumnRef,
		tipb.ExprType_And, tipb.ExprType_Or,
		tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList,
		tipb.ExprType_Like, tipb.ExprType_Not:
		return true
	case kv.ReqSubTypeDesc:
		return true
	default:
		return false
	}
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(req *kv.Request) kv.Response {
	tasks, err := buildCopTasks(c.store.regionCache, req.KeyRanges, req.Desc)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &copIterator{
		store:       c.store,
		req:         req,
		tasks:       tasks,
		concurrency: req.Concurrency,
	}
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}
	if !it.req.KeepOrder {
		it.respChan = make(chan *coprocessor.Response, it.concurrency)
	}
	it.errChan = make(chan error, 1)
	if len(it.tasks) == 0 {
		it.Close()
	}
	it.run()
	return it
}

const (
	taskNew int = iota
	taskRunning
	taskDone
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

func buildCopTasks(cache *RegionCache, ranges []kv.KeyRange, desc bool) ([]*copTask, error) {
	var tasks []*copTask
	for _, r := range ranges {
		var err error
		if tasks, err = appendTask(tasks, cache, r); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if desc {
		reverseTasks(tasks)
	}
	return tasks, nil
}

func reverseTasks(tasks []*copTask) {
	for i := 0; i < len(tasks)/2; i++ {
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
		tasks[i].idx, tasks[j].idx = tasks[j].idx, tasks[i].idx
	}
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
		last.respChan = make(chan *coprocessor.Response, 1)
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
	respGot     int
	concurrency int
	respChan    chan *coprocessor.Response
	errChan     chan error
	finished    bool
}

// Pick the next new copTask and send request to tikv-server.
func (it *copIterator) work() {
	for {
		it.mu.Lock()
		if it.finished {
			it.mu.Lock()
			break
		}
		// Find the next task to send.
		var task *copTask
		for _, t := range it.tasks {
			if t.status == taskNew {
				task = t
				break
			}
		}
		if task == nil {
			it.mu.Unlock()
			break
		}
		task.status = taskRunning
		it.mu.Unlock()
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
	// Start it.concurrency number of workers to handle cop requests.
	for i := 0; i < it.concurrency; i++ {
		go it.work()
	}
}

// Return next coprocessor result.
func (it *copIterator) Next() (io.ReadCloser, error) {
	if it.finished {
		return nil, nil
	}
	var (
		resp *coprocessor.Response
		err  error
	)
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.
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
		it.mu.Lock()
		task.status = taskDone
		it.mu.Unlock()
	}
	if err != nil {
		it.Close()
		return nil, err
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	it.respGot++
	if it.respGot == len(it.tasks) {
		it.Close()
	}
	return ioutil.NopCloser(bytes.NewBuffer(resp.Data)), nil
}

// Handle single copTask.
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
			it.store.regionCache.NextPeer(task.region.VerID())
			err1 := it.rebuildCurrentTask(task)
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			log.Warnf("send coprocessor request error: %v, try next peer later", err)
			continue
		}
		if e := resp.GetRegionError(); e != nil {
			if notLeader := e.GetNotLeader(); notLeader != nil {
				it.store.regionCache.UpdateLeader(task.region.VerID(), notLeader.GetLeader().GetId())
			} else {
				it.store.regionCache.DropRegion(task.region.VerID())
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
		return resp, nil
	}
	return nil, errors.Trace(backoffErr)
}

// Rebuild current task. It may be split into multiple tasks (in region split scenario).
func (it *copIterator) rebuildCurrentTask(task *copTask) error {
	newTasks, err := buildCopTasks(it.store.regionCache, task.ranges, it.req.Desc)
	if err != nil {
		return errors.Trace(err)
	}
	if len(newTasks) == 0 {
		// TODO: check this, this should never happend.
		return nil
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	// We should put the original task back to the original place in the task list.
	// So the tasks can be handled in order.
	t := newTasks[0]
	task.region = t.region
	task.ranges = t.ranges
	close(t.respChan)
	newTasks[0] = task
	it.tasks = append(it.tasks[:task.idx], append(newTasks, it.tasks[task.idx+1:]...)...)
	// Update index.
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
