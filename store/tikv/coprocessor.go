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
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/goroutine_pool"
	tipb "github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var copIteratorGP = gp.New(time.Minute)

// CopClient is coprocessor client.
type CopClient struct {
	store *tikvStore
}

// IsRequestTypeSupported checks whether reqType is supported.
func (c *CopClient) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			return true
		default:
			return c.supportExpr(tipb.ExprType(subType))
		}
	case kv.ReqTypeDAG:
		return c.supportExpr(tipb.ExprType(subType))
	case kv.ReqTypeAnalyze:
		return true
	}
	return false
}

func (c *CopClient) supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlTime, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_ColumnRef:
		return true
	// logic operators.
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Not:
		return true
	// compare operators.
	case tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList, tipb.ExprType_IsNull,
		tipb.ExprType_Like:
		return true
	// arithmetic operators.
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus, tipb.ExprType_Mul:
		return true
	// control functions
	case tipb.ExprType_Case, tipb.ExprType_If, tipb.ExprType_IfNull, tipb.ExprType_Coalesce:
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Max, tipb.ExprType_Min, tipb.ExprType_Sum, tipb.ExprType_Avg,
		tipb.ExprType_Agg_BitXor, tipb.ExprType_Agg_BitAnd, tipb.ExprType_Agg_BitOr:
		return true
	// json functions.
	case tipb.ExprType_JsonType, tipb.ExprType_JsonExtract, tipb.ExprType_JsonUnquote,
		tipb.ExprType_JsonObject, tipb.ExprType_JsonArray, tipb.ExprType_JsonMerge,
		tipb.ExprType_JsonSet, tipb.ExprType_JsonInsert, tipb.ExprType_JsonReplace, tipb.ExprType_JsonRemove:
		return true
	// date functions.
	case tipb.ExprType_DateFormat:
		return true
	case kv.ReqSubTypeDesc:
		return true
	case kv.ReqSubTypeSignature:
		return true
	default:
		return false
	}
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request) kv.Response {
	metrics.TiKVCoprocessorCounter.WithLabelValues("send").Inc()

	bo := NewBackoffer(ctx, copBuildTaskMaxBackoff)
	tasks, err := buildCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, req.Desc, req.Streaming)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &copIterator{
		store:       c.store,
		req:         req,
		concurrency: req.Concurrency,
		finished:    make(chan struct{}),
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}
	if !it.req.KeepOrder {
		it.respChan = make(chan copResponse, it.concurrency)
	}
	it.open(ctx)
	return it
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region RegionVerID
	ranges *copRanges

	respChan  chan copResponse
	storeAddr string
	cmdType   tikvrpc.CmdType
}

func (r *copTask) String() string {
	return fmt.Sprintf("region(%d %d %d) ranges(%d) store(%s)",
		r.region.id, r.region.confVer, r.region.ver, r.ranges.len(), r.storeAddr)
}

// copRanges is like []kv.KeyRange, but may has extra elements at head/tail.
// It's for avoiding alloc big slice during build copTask.
type copRanges struct {
	first *kv.KeyRange
	mid   []kv.KeyRange
	last  *kv.KeyRange
}

func (r *copRanges) String() string {
	var s string
	r.do(func(ran *kv.KeyRange) {
		s += fmt.Sprintf("[%q, %q]", ran.StartKey, ran.EndKey)
	})
	return s
}

func (r *copRanges) len() int {
	var l int
	if r.first != nil {
		l++
	}
	l += len(r.mid)
	if r.last != nil {
		l++
	}
	return l
}

func (r *copRanges) at(i int) kv.KeyRange {
	if r.first != nil {
		if i == 0 {
			return *r.first
		}
		i--
	}
	if i < len(r.mid) {
		return r.mid[i]
	}
	return *r.last
}

func (r *copRanges) slice(from, to int) *copRanges {
	var ran copRanges
	if r.first != nil {
		if from == 0 && to > 0 {
			ran.first = r.first
		}
		if from > 0 {
			from--
		}
		if to > 0 {
			to--
		}
	}
	if to <= len(r.mid) {
		ran.mid = r.mid[from:to]
	} else {
		if from <= len(r.mid) {
			ran.mid = r.mid[from:]
		}
		if from < to {
			ran.last = r.last
		}
	}
	return &ran
}

func (r *copRanges) do(f func(ran *kv.KeyRange)) {
	if r.first != nil {
		f(r.first)
	}
	for _, ran := range r.mid {
		f(&ran)
	}
	if r.last != nil {
		f(r.last)
	}
}

func (r *copRanges) toPBRanges() []*coprocessor.KeyRange {
	ranges := make([]*coprocessor.KeyRange, 0, r.len())
	r.do(func(ran *kv.KeyRange) {
		ranges = append(ranges, &coprocessor.KeyRange{
			Start: ran.StartKey,
			End:   ran.EndKey,
		})
	})
	return ranges
}

// Split ranges into (left, right) by key.
func (r *copRanges) split(key []byte) (*copRanges, *copRanges) {
	n := sort.Search(r.len(), func(i int) bool {
		cur := r.at(i)
		return len(cur.EndKey) == 0 || bytes.Compare(cur.EndKey, key) > 0
	})
	// If a range p contains the key, it will split to 2 parts.
	if n < r.len() {
		p := r.at(n)
		if bytes.Compare(key, p.StartKey) > 0 {
			left := r.slice(0, n)
			left.last = &kv.KeyRange{StartKey: p.StartKey, EndKey: key}
			right := r.slice(n+1, r.len())
			right.first = &kv.KeyRange{StartKey: key, EndKey: p.EndKey}
			return left, right
		}
	}
	return r.slice(0, n), r.slice(n, r.len())
}

func buildCopTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, desc bool, streaming bool) ([]*copTask, error) {
	metrics.TiKVCoprocessorCounter.WithLabelValues("build_task").Inc()

	start := time.Now()
	rangesLen := ranges.len()
	cmdType := tikvrpc.CmdCop
	if streaming {
		cmdType = tikvrpc.CmdCopStream
	}

	var tasks []*copTask
	appendTask := func(region RegionVerID, ranges *copRanges) {
		tasks = append(tasks, &copTask{
			region:   region,
			ranges:   ranges,
			respChan: make(chan copResponse, 1),
			cmdType:  cmdType,
		})
	}

	err := splitRanges(bo, cache, ranges, appendTask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if desc {
		reverseTasks(tasks)
	}
	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		log.Warnf("buildCopTasks takes too much time (%v), range len %v, task len %v", elapsed, rangesLen, len(tasks))
	}
	metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("coprocessor").Observe(float64(len(tasks)))
	return tasks, nil
}

func splitRanges(bo *Backoffer, cache *RegionCache, ranges *copRanges, fn func(region RegionVerID, ranges *copRanges)) error {
	for ranges.len() > 0 {
		loc, err := cache.LocateKey(bo, ranges.at(0).StartKey)
		if err != nil {
			return errors.Trace(err)
		}

		// Iterate to the first range that is not complete in the region.
		var i int
		for ; i < ranges.len(); i++ {
			r := ranges.at(i)
			if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
				break
			}
		}
		// All rest ranges belong to the same region.
		if i == ranges.len() {
			fn(loc.Region, ranges)
			break
		}

		r := ranges.at(i)
		if loc.Contains(r.StartKey) {
			// Part of r is not in the region. We need to split it.
			taskRanges := ranges.slice(0, i)
			taskRanges.last = &kv.KeyRange{
				StartKey: r.StartKey,
				EndKey:   loc.EndKey,
			}
			fn(loc.Region, taskRanges)

			ranges = ranges.slice(i+1, ranges.len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.slice(0, i)
			fn(loc.Region, taskRanges)
			ranges = ranges.slice(i, ranges.len())
		}
	}

	return nil
}

// SplitRegionRanges get the split ranges from pd region.
func SplitRegionRanges(bo *Backoffer, cache *RegionCache, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	ranges := copRanges{mid: keyRanges}

	var ret []kv.KeyRange
	appendRange := func(region RegionVerID, ranges *copRanges) {
		for i := 0; i < ranges.len(); i++ {
			ret = append(ret, ranges.at(i))
		}
	}

	err := splitRanges(bo, cache, &ranges, appendRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}

func reverseTasks(tasks []*copTask) {
	for i := 0; i < len(tasks)/2; i++ {
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type copIterator struct {
	store       *tikvStore
	req         *kv.Request
	concurrency int
	finished    chan struct{}
	// There are two cases we need to close the `finished` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*copTask
	curr  int

	// Otherwise, results are stored in respChan.
	respChan chan copResponse
	wg       sync.WaitGroup
}

// copIteratorWorker receive tasks from copIteratorTaskSender and handle tasks, sent the copResponse to respChan.
type copIteratorWorker struct {
	taskCh   <-chan *copTask
	wg       *sync.WaitGroup
	store    *tikvStore
	req      *kv.Request
	respChan chan<- copResponse
	finished <-chan struct{}
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh   chan<- *copTask
	wg       *sync.WaitGroup
	tasks    []*copTask
	finished <-chan struct{}
	respChan chan<- copResponse
}

type copResponse struct {
	*coprocessor.Response
	startKey kv.Key
	err      error
}

const minLogCopTaskTime = 300 * time.Millisecond

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	span, ctx1 := opentracing.StartSpanFromContext(ctx, "copIteratorWorker.run")
	defer span.Finish()

	defer worker.wg.Done()
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}

		bo := NewBackoffer(ctx1, copNextMaxBackoff)
		worker.handleTask(bo, task, respCh)
		if bo.totalSleep > 0 {
			metrics.TiKVBackoffHistogram.Observe(float64(bo.totalSleep) / 1000)
		}
		close(task.respChan)
		select {
		case <-worker.finished:
			return
		default:
		}
	}
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context) {
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency)
	// Start it.concurrency number of workers to handle cop requests.
	for i := 0; i < it.concurrency; i++ {
		worker := &copIteratorWorker{
			taskCh:   taskCh,
			wg:       &it.wg,
			store:    it.store,
			req:      it.req,
			respChan: it.respChan,
			finished: it.finished,
		}
		copIteratorGP.Go(func() {
			worker.run(ctx)
		})
	}
	taskSender := &copIteratorTaskSender{
		taskCh: taskCh,
		wg:     &it.wg,
		tasks:  it.tasks,
	}
	taskSender.respChan = it.respChan
	copIteratorGP.Go(taskSender.run)
}

func (sender *copIteratorTaskSender) run() {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		exit := sender.sendToTaskCh(t)
		if exit {
			break
		}
	}
	close(sender.taskCh)

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		close(sender.respChan)
	}
}

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan copResponse) (resp copResponse, ok bool, exit bool) {
	select {
	case resp, ok = <-respCh:
	case <-it.finished:
		exit = true
	case <-ctx.Done():
		// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
		if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
			close(it.finished)
		}
		exit = true
	}
	return
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask) (exit bool) {
	select {
	case sender.taskCh <- t:
	case <-sender.finished:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp copResponse, respCh chan<- copResponse) (exit bool) {
	select {
	case respCh <- resp:
	case <-worker.finished:
		exit = true
	}
	return
}

// copResultSubset implements the kv.ResultSubset interface.
type copResultSubset struct {
	data     []byte
	startKey kv.Key
}

// GetData implements the kv.ResultSubset GetData interface.
func (rs *copResultSubset) GetData() []byte {
	return rs.data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *copResultSubset) GetStartKey() kv.Key {
	return rs.startKey
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	metrics.TiKVCoprocessorCounter.WithLabelValues("next").Inc()

	var (
		resp   copResponse
		ok     bool
		closed bool
	)
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.
	if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			return nil, nil
		}
	} else {
		for {
			if it.curr >= len(it.tasks) {
				// Resp will be nil if iterator is finished.
				return nil, nil
			}
			task := it.tasks[it.curr]
			resp, ok, closed = it.recvFromRespCh(ctx, task.respChan)
			if closed {
				// Close() is already called, so Next() is invalid.
				return nil, nil
			}
			if ok {
				break
			}
			// Switch to next task.
			it.tasks[it.curr] = nil
			it.curr++
		}
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := it.store.CheckVisibility(it.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.Data == nil {
		return &copResultSubset{}, nil
	}
	return &copResultSubset{data: resp.Data, startKey: resp.startKey}, nil
}

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(bo *Backoffer, task *copTask, respCh chan<- copResponse) {
	remainTasks := []*copTask{task}
	for len(remainTasks) > 0 {
		tasks, err := worker.handleTaskOnce(bo, remainTasks[0], respCh)
		if err != nil {
			resp := copResponse{err: errors.Trace(err)}
			worker.sendToRespCh(resp, respCh)
			return
		}
		if len(tasks) > 0 {
			remainTasks = append(tasks, remainTasks[1:]...)
		} else {
			remainTasks = remainTasks[1:]
		}
	}
}

// handleTaskOnce handles single copTask, successful results are send to channel.
// If error happened, returns error. If region split or meet lock, returns the remain tasks.
func (worker *copIteratorWorker) handleTaskOnce(bo *Backoffer, task *copTask, ch chan<- copResponse) ([]*copTask, error) {

	// gofail: var handleTaskOnceError bool
	// if handleTaskOnceError {
	// 	return nil, errors.New("mock handleTaskOnce error")
	// }

	sender := NewRegionRequestSender(worker.store.regionCache, worker.store.client)
	req := &tikvrpc.Request{
		Type: task.cmdType,
		Cop: &coprocessor.Request{
			Tp:     worker.req.Tp,
			Data:   worker.req.Data,
			Ranges: task.ranges.toPBRanges(),
		},
		Context: kvrpcpb.Context{
			IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
			Priority:       kvPriorityToCommandPri(worker.req.Priority),
			NotFillCache:   worker.req.NotFillCache,
		},
	}
	startTime := time.Now()
	resp, err := sender.SendReq(bo, req, task.region, ReadTimeoutMedium)
	if err != nil {
		return nil, errors.Trace(err)
	}
	costTime := time.Since(startTime)
	if costTime > minLogCopTaskTime {
		log.Infof("[TIME_COP_TASK] %s%s %s", costTime, bo, task)
	}
	metrics.TiKVCoprocessorCounter.WithLabelValues("handle_task").Inc()
	metrics.TiKVCoprocessorHistogram.Observe(costTime.Seconds())
	// Set task.storeAddr field so its task.String() method have the store address information.
	task.storeAddr = sender.storeAddr

	if task.cmdType == tikvrpc.CmdCopStream {
		return worker.handleCopStreamResult(bo, resp.CopStream, task, ch)
	}

	// Handles the response for non-streaming copTask.
	return worker.handleCopResponse(bo, resp.Cop, task, ch, nil)
}

func (worker *copIteratorWorker) handleCopStreamResult(bo *Backoffer, stream *tikvrpc.CopStreamResponse, task *copTask, ch chan<- copResponse) ([]*copTask, error) {
	defer stream.Close()
	var resp *coprocessor.Response
	var lastRange *coprocessor.KeyRange
	resp = stream.Response
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return nil, nil
	}
	for {
		remainedTasks, err := worker.handleCopResponse(bo, resp, task, ch, lastRange)
		if err != nil || len(remainedTasks) != 0 {
			return remainedTasks, errors.Trace(err)
		}
		resp, err = stream.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil, nil
			}

			if err1 := bo.Backoff(boTiKVRPC, errors.Errorf("recv stream response error: %v, task: %s", err, task)); err1 != nil {
				return nil, errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			log.Info("stream recv timeout:", err)
			return worker.buildCopTasksFromRemain(bo, lastRange, task)
		}
		lastRange = resp.Range
	}
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling streaming coprocessor response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, resp *coprocessor.Response, task *copTask, ch chan<- copResponse, lastRange *coprocessor.KeyRange) ([]*copTask, error) {
	if regionErr := resp.GetRegionError(); regionErr != nil {
		if err := bo.Backoff(BoRegionMiss, errors.New(regionErr.String())); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		metrics.TiKVCoprocessorCounter.WithLabelValues("rebuild_task").Inc()
		return buildCopTasks(bo, worker.store.regionCache, task.ranges, worker.req.Desc, worker.req.Streaming)
	}
	if lockErr := resp.GetLocked(); lockErr != nil {
		log.Debugf("coprocessor encounters lock: %v", lockErr)
		ok, err1 := worker.store.lockResolver.ResolveLocks(bo, []*Lock{NewLock(lockErr)})
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if !ok {
			if err := bo.Backoff(boTxnLockFast, errors.New(lockErr.String())); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return worker.buildCopTasksFromRemain(bo, lastRange, task)
	}
	if otherErr := resp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)
		log.Warnf("coprocessor err: %v", err)
		return nil, errors.Trace(err)
	}
	var startKey kv.Key
	// When the request is using streaming API, the `Range` is not nil.
	if resp.Range != nil {
		startKey = resp.Range.Start
	} else {
		startKey = task.ranges.at(0).StartKey
	}
	worker.sendToRespCh(copResponse{resp, startKey, nil}, ch)
	return nil, nil
}

func (worker *copIteratorWorker) buildCopTasksFromRemain(bo *Backoffer, lastRange *coprocessor.KeyRange, task *copTask) ([]*copTask, error) {
	remainedRanges := task.ranges
	if worker.req.Streaming && lastRange != nil {
		remainedRanges = worker.calculateRemain(task.ranges, lastRange, worker.req.Desc)
	}
	return buildCopTasks(bo, worker.store.regionCache, remainedRanges, worker.req.Desc, worker.req.Streaming)
}

// calculateRemain splits the input ranges into two, and take one of them according to desc flag.
// It's used in streaming API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the remain ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the remain ranges should be [r1 --> r2) [r3 --> s2)
func (worker *copIteratorWorker) calculateRemain(ranges *copRanges, split *coprocessor.KeyRange, desc bool) *copRanges {
	if desc {
		left, _ := ranges.split(split.End)
		return left
	}
	_, right := ranges.split(split.Start)
	return right
}

func (it *copIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finished)
	}
	it.wg.Wait()
	return nil
}

// copErrorResponse returns error when calling Next()
type copErrorResponse struct{ error }

func (it copErrorResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	return nil, it.error
}

func (it copErrorResponse) Close() error {
	return nil
}
