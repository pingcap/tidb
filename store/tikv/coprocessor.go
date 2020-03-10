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
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var tikvTxnRegionsNumHistogramWithCoprocessor = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues("coprocessor")

// CopClient is coprocessor client.
type CopClient struct {
	kv.RequestTypeSupportedChecker
	store           *tikvStore
	replicaReadSeed uint32
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request, vars *kv.Variables) kv.Response {
	if req.StoreType == kv.TiFlash {
		logutil.BgLogger().Info("send batch requests")
		return c.SendBatch(ctx, req, vars)
	}
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackoffer(ctx, copBuildTaskMaxBackoff).WithVars(vars)
	tasks, err := buildCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, req)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &copIterator{
		store:           c.store,
		req:             req,
		concurrency:     req.Concurrency,
		finishCh:        make(chan struct{}),
		vars:            vars,
		memTracker:      req.MemTracker,
		replicaReadSeed: c.replicaReadSeed,
	}
	it.minCommitTSPushed.data = make(map[uint64]struct{}, 5)
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}
	if it.req.KeepOrder {
		it.sendRate = newRateLimit(2 * it.concurrency)
	} else {
		it.respChan = make(chan *copResponse, it.concurrency)
	}
	it.open(ctx)
	return it
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region RegionVerID
	ranges *copRanges

	respChan  chan *copResponse
	storeAddr string
	cmdType   tikvrpc.CmdType
	storeType kv.StoreType
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

// split ranges into (left, right) by key.
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

// rangesPerTask limits the length of the ranges slice sent in one copTask.
const rangesPerTask = 25000

func buildCopTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, req *kv.Request) ([]*copTask, error) {
	start := time.Now()
	cmdType := tikvrpc.CmdCop
	if req.Streaming {
		cmdType = tikvrpc.CmdCopStream
	}

	if req.StoreType == kv.TiDB {
		return buildTiDBMemCopTasks(ranges, req)
	}

	rangesLen := ranges.len()
	var tasks []*copTask
	appendTask := func(regionWithRangeInfo *KeyLocation, ranges *copRanges) {
		// TiKV will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := ranges.len()
		for i := 0; i < rLen; {
			nextI := mathutil.Min(i+rangesPerTask, rLen)
			tasks = append(tasks, &copTask{
				region: regionWithRangeInfo.Region,
				ranges: ranges.slice(i, nextI),
				// Channel buffer is 2 for handling region split.
				// In a common case, two region split tasks will not be blocked.
				respChan:  make(chan *copResponse, 2),
				cmdType:   cmdType,
				storeType: req.StoreType,
			})
			i = nextI
		}
	}

	err := splitRanges(bo, cache, ranges, appendTask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if req.Desc {
		reverseTasks(tasks)
	}
	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildCopTasks takes too much time",
			zap.Duration("elapsed", elapsed),
			zap.Int("range len", rangesLen),
			zap.Int("task len", len(tasks)))
	}
	tikvTxnRegionsNumHistogramWithCoprocessor.Observe(float64(len(tasks)))
	return tasks, nil
}

func buildTiDBMemCopTasks(ranges *copRanges, req *kv.Request) ([]*copTask, error) {
	servers, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, err
	}
	cmdType := tikvrpc.CmdCop
	if req.Streaming {
		cmdType = tikvrpc.CmdCopStream
	}
	tasks := make([]*copTask, 0, len(servers))
	for _, ser := range servers {
		addr := ser.IP + ":" + strconv.FormatUint(uint64(ser.StatusPort), 10)
		tasks = append(tasks, &copTask{
			ranges:    ranges,
			respChan:  make(chan *copResponse, 2),
			cmdType:   cmdType,
			storeType: req.StoreType,
			storeAddr: addr,
		})
	}
	return tasks, nil
}

func splitRanges(bo *Backoffer, cache *RegionCache, ranges *copRanges, fn func(regionWithRangeInfo *KeyLocation, ranges *copRanges)) error {
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
			fn(loc, ranges)
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
			fn(loc, taskRanges)

			ranges = ranges.slice(i+1, ranges.len())
			ranges.first = &kv.KeyRange{
				StartKey: loc.EndKey,
				EndKey:   r.EndKey,
			}
		} else {
			// rs[i] is not in the region.
			taskRanges := ranges.slice(0, i)
			fn(loc, taskRanges)
			ranges = ranges.slice(i, ranges.len())
		}
	}

	return nil
}

// SplitRegionRanges get the split ranges from pd region.
func SplitRegionRanges(bo *Backoffer, cache *RegionCache, keyRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	ranges := copRanges{mid: keyRanges}

	var ret []kv.KeyRange
	appendRange := func(regionWithRangeInfo *KeyLocation, ranges *copRanges) {
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
	finishCh    chan struct{}

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*copTask
	curr  int
	// sendRate controls the sending rate of copIteratorTaskSender, if keepOrder,
	// to prevent all tasks being done (aka. all of the responses are buffered)
	sendRate *rateLimit

	// Otherwise, results are stored in respChan.
	respChan chan *copResponse

	vars *kv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32

	minCommitTSPushed
}

// copIteratorWorker receives tasks from copIteratorTaskSender, handles tasks and sends the copResponse to respChan.
type copIteratorWorker struct {
	taskCh   <-chan *copTask
	wg       *sync.WaitGroup
	store    *tikvStore
	req      *kv.Request
	respChan chan<- *copResponse
	finishCh <-chan struct{}
	vars     *kv.Variables
	clientHelper

	memTracker *memory.Tracker

	replicaReadSeed uint32
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh   chan<- *copTask
	wg       *sync.WaitGroup
	tasks    []*copTask
	finishCh <-chan struct{}
	respChan chan<- *copResponse
	sendRate *rateLimit
}

type copResponse struct {
	pbResp   *coprocessor.Response
	detail   *execdetails.ExecDetails
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

const (
	sizeofExecDetails   = int(unsafe.Sizeof(execdetails.ExecDetails{}))
	sizeofCommitDetails = int(unsafe.Sizeof(execdetails.CommitDetails{}))
)

// GetData implements the kv.ResultSubset GetData interface.
func (rs *copResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *copResponse) GetStartKey() kv.Key {
	return rs.startKey
}

func (rs *copResponse) GetExecDetails() *execdetails.ExecDetails {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *copResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
		if rs.detail.CommitDetail != nil {
			rs.respSize += int64(sizeofCommitDetails)
		}
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *copResponse) RespTime() time.Duration {
	return rs.respTime
}

const minLogCopTaskTime = 300 * time.Millisecond

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	defer worker.wg.Done()
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}

		bo := NewBackoffer(ctx, copNextMaxBackoff).WithVars(worker.vars)
		worker.handleTask(bo, task, respCh)
		close(task.respChan)
		select {
		case <-worker.finishCh:
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
			finishCh: it.finishCh,
			vars:     it.vars,
			clientHelper: clientHelper{
				LockResolver:      it.store.lockResolver,
				RegionCache:       it.store.regionCache,
				minCommitTSPushed: &it.minCommitTSPushed,
				Client:            it.store.client,
			},

			memTracker: it.memTracker,

			replicaReadSeed: it.replicaReadSeed,
		}
		go worker.run(ctx)
	}
	taskSender := &copIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
		sendRate: it.sendRate,
	}
	taskSender.respChan = it.respChan
	go taskSender.run()
}

func (sender *copIteratorTaskSender) run() {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		// If keepOrder, we must control the sending rate to prevent all tasks
		// being done (aka. all of the responses are buffered) by copIteratorWorker.
		// We keep the number of inflight tasks within the number of concurrency * 2.
		// It sends one more task if a task has been finished in copIterator.Next.
		if sender.sendRate != nil {
			exit := sender.sendRate.getToken(sender.finishCh)
			if exit {
				break
			}
		}
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

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan *copResponse) (resp *copResponse, ok bool, exit bool) {
	select {
	case resp, ok = <-respCh:
		if it.memTracker != nil && resp != nil {
			it.memTracker.Consume(-int64(resp.MemSize()))
		}
	case <-it.finishCh:
		exit = true
	case <-ctx.Done():
		// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
		if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
			close(it.finishCh)
		}
		exit = true
	}
	return
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask) (exit bool) {
	select {
	case sender.taskCh <- t:
	case <-sender.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp *copResponse, respCh chan<- *copResponse, checkOOM bool) (exit bool) {
	if worker.memTracker != nil && checkOOM {
		worker.memTracker.Consume(int64(resp.MemSize()))
	}
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *copResponse
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
				// Resp will be nil if iterator is finishCh.
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
			it.sendRate.putToken()
		}
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := it.store.CheckVisibility(it.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(bo *Backoffer, task *copTask, respCh chan<- *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("copIteratorWork meet panic",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
			resp := &copResponse{err: errors.Errorf("%v", r)}
			// if panic has happened, set checkOOM to false to avoid another panic.
			worker.sendToRespCh(resp, respCh, false)
		}
	}()
	remainTasks := []*copTask{task}
	for len(remainTasks) > 0 {
		tasks, err := worker.handleTaskOnce(bo, remainTasks[0], respCh)
		if err != nil {
			resp := &copResponse{err: errors.Trace(err)}
			worker.sendToRespCh(resp, respCh, true)
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
func (worker *copIteratorWorker) handleTaskOnce(bo *Backoffer, task *copTask, ch chan<- *copResponse) ([]*copTask, error) {
	failpoint.Inject("handleTaskOnceError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock handleTaskOnce error"))
		}
	})

	copReq := coprocessor.Request{
		Tp:        worker.req.Tp,
		StartTs:   worker.req.StartTs,
		Data:      worker.req.Data,
		Ranges:    task.ranges.toPBRanges(),
		SchemaVer: worker.req.SchemaVar,
	}

	var cacheKey []byte = nil
	var cacheValue *coprCacheValue = nil

	// If there are many ranges, it is very likely to be a TableLookupRequest. They are not worth to cache since
	// computing is not the main cost. Ignore such requests directly to avoid slowly building the cache key.
	if task.cmdType == tikvrpc.CmdCop && worker.store.coprCache != nil && worker.req.Cacheable && len(copReq.Ranges) < 10 {
		cKey, err := coprCacheBuildKey(&copReq)
		if err == nil {
			cacheKey = cKey
			cValue := worker.store.coprCache.Get(cKey)
			if cValue != nil && cValue.RegionID == task.region.id && cValue.TimeStamp <= worker.req.StartTs {
				// Append cache version to the request to skip Coprocessor computation if possible
				// when request result is cached
				copReq.IsCacheEnabled = true
				copReq.CacheIfMatchVersion = cValue.RegionDataVersion
				cacheValue = cValue
			}
		} else {
			logutil.BgLogger().Warn("Failed to build copr cache key", zap.Error(err))
		}
	}

	req := tikvrpc.NewReplicaReadRequest(task.cmdType, &copReq, worker.req.ReplicaRead, worker.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: pbIsolationLevel(worker.req.IsolationLevel),
		Priority:       kvPriorityToCommandPri(worker.req.Priority),
		NotFillCache:   worker.req.NotFillCache,
		HandleTime:     true,
		ScanDetail:     true,
	})
	req.StoreTp = task.storeType
	startTime := time.Now()
	resp, rpcCtx, storeAddr, err := worker.SendReqCtx(bo, req, task.region, ReadTimeoutMedium, task.storeType, task.storeAddr)
	if err != nil {
		if task.storeType == kv.TiDB {
			err = worker.handleTiDBSendReqErr(err, task, ch)
			return nil, err
		}
		return nil, errors.Trace(err)
	}
	// Set task.storeAddr field so its task.String() method have the store address information.
	task.storeAddr = storeAddr
	costTime := time.Since(startTime)
	if costTime > minLogCopTaskTime {
		worker.logTimeCopTask(costTime, task, bo, resp)
	}
	metrics.TiKVCoprocessorHistogram.Observe(costTime.Seconds())

	if task.cmdType == tikvrpc.CmdCopStream {
		return worker.handleCopStreamResult(bo, rpcCtx, resp.Resp.(*tikvrpc.CopStreamResponse), task, ch, costTime)
	}

	// Handles the response for non-streaming copTask.
	return worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: resp.Resp.(*coprocessor.Response)}, cacheKey, cacheValue, task, ch, nil, costTime)
}

type minCommitTSPushed struct {
	data map[uint64]struct{}
	sync.RWMutex
}

func (m *minCommitTSPushed) Update(from []uint64) {
	m.Lock()
	for _, v := range from {
		m.data[v] = struct{}{}
	}
	m.Unlock()
}

func (m *minCommitTSPushed) Get() []uint64 {
	m.RLock()
	defer m.RUnlock()
	if len(m.data) == 0 {
		return nil
	}

	ret := make([]uint64, 0, len(m.data))
	for k := range m.data {
		ret = append(ret, k)
	}
	return ret
}

// clientHelper wraps LockResolver and RegionRequestSender.
// It's introduced to support the new lock resolving pattern in the large transaction.
// In the large transaction protocol, sending requests and resolving locks are
// context-dependent. For example, when a send request meets a secondary lock, we'll
// call ResolveLock, and if the lock belongs to a large transaction, we may retry
// the request. If there is no context information about the resolved locks, we'll
// meet the secondary lock again and run into a deadloop.
type clientHelper struct {
	*LockResolver
	*RegionCache
	*minCommitTSPushed
	Client
}

// ResolveLocks wraps the ResolveLocks function and store the resolved result.
func (ch *clientHelper) ResolveLocks(bo *Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	msBeforeTxnExpired, resolvedLocks, err := ch.LockResolver.ResolveLocks(bo, callerStartTS, locks)
	if err != nil {
		return msBeforeTxnExpired, err
	}
	if len(resolvedLocks) > 0 {
		ch.minCommitTSPushed.Update(resolvedLocks)
		return 0, nil
	}
	return msBeforeTxnExpired, nil
}

// SendReqCtx wraps the SendReqCtx function and use the resolved lock result in the kvrpcpb.Context.
func (ch *clientHelper) SendReqCtx(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration, sType kv.StoreType, directStoreAddr string) (*tikvrpc.Response, *RPCContext, string, error) {
	sender := NewRegionRequestSender(ch.RegionCache, ch.Client)
	if len(directStoreAddr) > 0 {
		sender.storeAddr = directStoreAddr
	}
	req.Context.ResolvedLocks = ch.minCommitTSPushed.Get()
	resp, ctx, err := sender.SendReqCtx(bo, req, regionID, timeout, sType)
	return resp, ctx, sender.storeAddr, err
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
	minLogKVWaitTime    = 200
)

func (worker *copIteratorWorker) logTimeCopTask(costTime time.Duration, task *copTask, bo *Backoffer, resp *tikvrpc.Response) {
	logStr := fmt.Sprintf("[TIME_COP_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.id, task.storeAddr)
	if bo.totalSleep > minLogBackoffTime {
		backoffTypes := strings.Replace(fmt.Sprintf("%v", bo.types), " ", ",", -1)
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.totalSleep, backoffTypes)
	}
	var detail *kvrpcpb.ExecDetails
	if resp.Resp != nil {
		switch r := resp.Resp.(type) {
		case *coprocessor.Response:
			detail = r.ExecDetails
		case *tikvrpc.CopStreamResponse:
			// streaming request returns io.EOF, so the first CopStreamResponse.Response maybe nil.
			if r.Response != nil {
				detail = r.Response.ExecDetails
			}
		default:
			panic("unreachable")
		}
	}

	if detail != nil && detail.HandleTime != nil {
		processMs := detail.HandleTime.ProcessMs
		waitMs := detail.HandleTime.WaitMs
		if processMs > minLogKVProcessTime {
			logStr += fmt.Sprintf(" kv_process_ms:%d", processMs)
			if detail.ScanDetail != nil {
				logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
				logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
				logStr = appendScanDetail(logStr, "lock", detail.ScanDetail.Lock)
			}
		}
		if waitMs > minLogKVWaitTime {
			logStr += fmt.Sprintf(" kv_wait_ms:%d", waitMs)
			if processMs <= minLogKVProcessTime {
				logStr = strings.Replace(logStr, "TIME_COP_PROCESS", "TIME_COP_WAIT", 1)
			}
		}
	}
	logutil.Logger(bo.ctx).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	if scanInfo != nil {
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	return logStr
}

func (worker *copIteratorWorker) handleCopStreamResult(bo *Backoffer, rpcCtx *RPCContext, stream *tikvrpc.CopStreamResponse, task *copTask, ch chan<- *copResponse, costTime time.Duration) ([]*copTask, error) {
	defer stream.Close()
	var resp *coprocessor.Response
	var lastRange *coprocessor.KeyRange
	resp = stream.Response
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return nil, nil
	}
	for {
		remainedTasks, err := worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: resp}, nil, nil, task, ch, lastRange, costTime)
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
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return worker.buildCopTasksFromRemain(bo, lastRange, task)
		}
		if resp.Range != nil {
			lastRange = resp.Range
		}
	}
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling streaming coprocessor response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, rpcCtx *RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, ch chan<- *copResponse, lastRange *coprocessor.KeyRange, costTime time.Duration) ([]*copTask, error) {
	if regionErr := resp.pbResp.GetRegionError(); regionErr != nil {
		if rpcCtx != nil && task.storeType == kv.TiDB {
			resp.err = errors.Errorf("error: %v", regionErr)
			worker.sendToRespCh(resp, ch, true)
			return nil, nil
		}
		errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
			task.region.id, task.region.ver, task.storeType.Name(), task.storeAddr, regionErr.String())
		if err := bo.Backoff(BoRegionMiss, errors.New(errStr)); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		return buildCopTasks(bo, worker.store.regionCache, task.ranges, worker.req)
	}
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		logutil.BgLogger().Debug("coprocessor encounters",
			zap.Stringer("lock", lockErr))
		msBeforeExpired, err1 := worker.ResolveLocks(bo, worker.req.StartTs, []*Lock{NewLock(lockErr)})
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if msBeforeExpired > 0 {
			if err := bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
				return nil, errors.Trace(err)
			}
		}
		return worker.buildCopTasksFromRemain(bo, lastRange, task)
	}
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.id),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	// When the request is using streaming API, the `Range` is not nil.
	if resp.pbResp.Range != nil {
		resp.startKey = resp.pbResp.Range.Start
	} else if task.ranges != nil && task.ranges.len() > 0 {
		resp.startKey = task.ranges.at(0).StartKey
	}
	if resp.detail == nil {
		resp.detail = new(execdetails.ExecDetails)
	}
	resp.detail.BackoffTime = time.Duration(bo.totalSleep) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(bo.backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(bo.backoffTimes))
	for backoff := range bo.backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = bo.backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.backoffSleepMS[backoff]) * time.Millisecond
	}
	if rpcCtx != nil {
		resp.detail.CalleeAddress = rpcCtx.Addr
	}
	resp.respTime = costTime
	if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		if handleTime := pbDetails.HandleTime; handleTime != nil {
			resp.detail.WaitTime = time.Duration(handleTime.WaitMs) * time.Millisecond
			resp.detail.ProcessTime = time.Duration(handleTime.ProcessMs) * time.Millisecond
		}
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			if scanDetail.Write != nil {
				resp.detail.TotalKeys += scanDetail.Write.Total
				resp.detail.ProcessedKeys += scanDetail.Write.Processed
			}
		}
	}
	if resp.pbResp.IsCacheHit {
		if cacheValue == nil {
			return nil, errors.New("Internal error: received illegal TiKV response")
		}
		// Cache hit and is valid: use cached data as response data and we don't update the cache.
		data := make([]byte, len(cacheValue.Data))
		copy(data, cacheValue.Data)
		resp.pbResp.Data = data
	} else {
		// Cache not hit or cache hit but not valid: update the cache if the response can be cached.
		if cacheKey != nil && resp.pbResp.CacheLastVersion > 0 {
			if worker.store.coprCache.CheckAdmission(resp.pbResp.Data.Size(), resp.detail.ProcessTime) {
				data := make([]byte, len(resp.pbResp.Data))
				copy(data, resp.pbResp.Data)

				newCacheValue := coprCacheValue{
					Data:              data,
					TimeStamp:         worker.req.StartTs,
					RegionID:          task.region.id,
					RegionDataVersion: resp.pbResp.CacheLastVersion,
				}
				worker.store.coprCache.Set(cacheKey, &newCacheValue)
			}
		}
	}
	worker.sendToRespCh(resp, ch, true)
	return nil, nil
}

func (worker *copIteratorWorker) handleTiDBSendReqErr(err error, task *copTask, ch chan<- *copResponse) error {
	errCode := errno.ErrUnknown
	errMsg := err.Error()
	if terror.ErrorEqual(err, ErrTiKVServerTimeout) {
		errCode = errno.ErrTiKVServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	selResp := tipb.SelectResponse{
		Warnings: []*tipb.Error{
			{
				Code: int32(errCode),
				Msg:  errMsg,
			},
		},
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return errors.Trace(err)
	}
	resp := &copResponse{
		pbResp: &coprocessor.Response{
			Data: data,
		},
		detail: &execdetails.ExecDetails{},
	}
	worker.sendToRespCh(resp, ch, true)
	return nil
}

func (worker *copIteratorWorker) buildCopTasksFromRemain(bo *Backoffer, lastRange *coprocessor.KeyRange, task *copTask) ([]*copTask, error) {
	remainedRanges := task.ranges
	if worker.req.Streaming && lastRange != nil {
		remainedRanges = worker.calculateRemain(task.ranges, lastRange, worker.req.Desc)
	}
	return buildCopTasks(bo, worker.store.regionCache, remainedRanges, worker.req)
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
		close(it.finishCh)
	}
	it.wg.Wait()
	return nil
}

type rateLimit struct {
	token chan struct{}
}

func newRateLimit(n int) *rateLimit {
	return &rateLimit{
		token: make(chan struct{}, n),
	}
}

func (r *rateLimit) getToken(done <-chan struct{}) (exit bool) {
	select {
	case <-done:
		return true
	case r.token <- struct{}{}:
		return false
	}
}

func (r *rateLimit) putToken() {
	select {
	case <-r.token:
	default:
		panic("put a redundant token")
	}
}

// copErrorResponse returns error when calling Next()
type copErrorResponse struct{ error }

func (it copErrorResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	return nil, it.error
}

func (it copErrorResponse) Close() error {
	return nil
}
