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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	tidbmetrics "github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/driver/backoff"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/driver/options"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/paging"
	"github.com/pingcap/tidb/util/trxevents"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

var coprCacheHistogramEvict = tidbmetrics.DistSQLCoprCacheHistogram.WithLabelValues("evict")

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	copBuildTaskMaxBackoff = 5000
	copNextMaxBackoff      = 20000
)

// CopClient is coprocessor client.
type CopClient struct {
	kv.RequestTypeSupportedChecker
	store           *Store
	replicaReadSeed uint32
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request, variables interface{}, option *kv.ClientSendOption) kv.Response {
	eventCb := option.EventCb
	enabledRateLimitAction := option.EnabledRateLimitAction
	sessionMemTracker := option.SessionMemTracker
	vars, ok := variables.(*tikv.Variables)
	if !ok {
		return copErrorResponse{errors.Errorf("unsupported variables:%+v", variables)}
	}
	if req.StoreType == kv.TiFlash && req.BatchCop {
		logutil.BgLogger().Debug("send batch requests")
		return c.sendBatch(ctx, req, vars, option)
	}
	if req.Streaming && req.Paging {
		return copErrorResponse{errors.New("streaming and paging are both on")}
	}
	ctx = context.WithValue(ctx, tikv.TxnStartKey(), req.StartTs)
	bo := backoff.NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)
	ranges := NewKeyRanges(req.KeyRanges)
	tasks, err := buildCopTasks(bo, c.store.GetRegionCache(), ranges, req, eventCb)
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
		rpcCancel:       tikv.NewRPCanceller(),
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}

	if it.req.KeepOrder {
		it.sendRate = util.NewRateLimit(2 * it.concurrency)
		it.respChan = nil
	} else {
		capacity := it.concurrency
		if enabledRateLimitAction {
			// The count of cached response in memory is controlled by the capacity of the it.sendRate, not capacity of the respChan.
			// As the worker will send finCopResponse after each task being handled, we make the capacity of the respCh equals to
			// 2*it.concurrency to avoid deadlock in the unit test caused by the `MustExec` or `Exec`
			capacity = it.concurrency * 2
		}
		// in streaming or paging request, a request will be returned in multi batches,
		// enlarge the channel size to avoid the request blocked by buffer full.
		if req.Streaming || req.Paging {
			if capacity < 2048 {
				capacity = 2048
			}
		}
		it.respChan = make(chan *copResponse, capacity)
		it.sendRate = util.NewRateLimit(it.concurrency)
	}
	it.actionOnExceed = newRateLimitAction(uint(it.sendRate.GetCapacity()))
	if sessionMemTracker != nil && enabledRateLimitAction {
		sessionMemTracker.FallbackOldAndSetNewAction(it.actionOnExceed)
	}

	if !it.req.Streaming {
		ctx = context.WithValue(ctx, tikv.RPCCancellerCtxKey{}, it.rpcCancel)
	}
	it.open(ctx, enabledRateLimitAction, option.EnableCollectExecutionInfo)
	return it
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	region     tikv.RegionVerID
	bucketsVer uint64
	ranges     *KeyRanges

	respChan  chan *copResponse
	storeAddr string
	cmdType   tikvrpc.CmdType
	storeType kv.StoreType

	eventCb    trxevents.EventCallback
	paging     bool
	pagingSize uint64

	partitionIndex int64 // used by balanceBatchCopTask in PartitionTableScan
}

func (r *copTask) String() string {
	return fmt.Sprintf("region(%d %d %d) ranges(%d) store(%s)",
		r.region.GetID(), r.region.GetConfVer(), r.region.GetVer(), r.ranges.Len(), r.storeAddr)
}

// rangesPerTask limits the length of the ranges slice sent in one copTask.
const rangesPerTask = 25000

func buildCopTasks(bo *Backoffer, cache *RegionCache, ranges *KeyRanges, req *kv.Request, eventCb trxevents.EventCallback) ([]*copTask, error) {
	start := time.Now()
	cmdType := tikvrpc.CmdCop
	if req.Streaming {
		cmdType = tikvrpc.CmdCopStream
	}

	if req.StoreType == kv.TiDB {
		return buildTiDBMemCopTasks(ranges, req)
	}

	rangesLen := ranges.Len()

	// TODO(youjiali1995): is there any request type that needn't be splitted by buckets?
	locs, err := cache.SplitKeyRangesByBuckets(bo, ranges)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Channel buffer is 2 for handling region split.
	// In a common case, two region split tasks will not be blocked.
	chanSize := 2
	// in streaming or paging request, a request will be returned in multi batches,
	// enlarge the channel size to avoid the request blocked by buffer full.
	if req.Streaming || req.Paging {
		chanSize = 128
	}

	var tasks []*copTask
	for _, loc := range locs {
		// TiKV will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := loc.Ranges.Len()
		for i := 0; i < rLen; {
			nextI := mathutil.Min(i+rangesPerTask, rLen)
			// If this is a paging request, we set the paging size to minPagingSize,
			// the size will grow every round.
			pagingSize := uint64(0)
			if req.Paging {
				pagingSize = paging.MinPagingSize
			}
			tasks = append(tasks, &copTask{
				region:     loc.Location.Region,
				bucketsVer: loc.getBucketVersion(),
				ranges:     loc.Ranges.Slice(i, nextI),
				respChan:   make(chan *copResponse, chanSize),
				cmdType:    cmdType,
				storeType:  req.StoreType,
				eventCb:    eventCb,
				paging:     req.Paging,
				pagingSize: pagingSize,
			})
			i = nextI
		}
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
	metrics.TxnRegionsNumHistogramWithCoprocessor.Observe(float64(len(tasks)))
	return tasks, nil
}

func buildTiDBMemCopTasks(ranges *KeyRanges, req *kv.Request) ([]*copTask, error) {
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
		if req.TiDBServerID > 0 && req.TiDBServerID != ser.ServerIDGetter() {
			continue
		}

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

func reverseTasks(tasks []*copTask) {
	for i := 0; i < len(tasks)/2; i++ {
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

type copIterator struct {
	store       *Store
	req         *kv.Request
	concurrency int
	finishCh    chan struct{}

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*copTask
	// curr indicates the curr id of the finished copTask
	curr int

	// sendRate controls the sending rate of copIteratorTaskSender
	sendRate *util.RateLimit

	// Otherwise, results are stored in respChan.
	respChan chan *copResponse

	vars *tikv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	rpcCancel *tikv.RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to make sure the channel is not closed twice.
	closed uint32

	resolvedLocks  util.TSSet
	committedLocks util.TSSet

	actionOnExceed *rateLimitAction
}

// copIteratorWorker receives tasks from copIteratorTaskSender, handles tasks and sends the copResponse to respChan.
type copIteratorWorker struct {
	taskCh   <-chan *copTask
	wg       *sync.WaitGroup
	store    *Store
	req      *kv.Request
	respChan chan<- *copResponse
	finishCh <-chan struct{}
	vars     *tikv.Variables
	kvclient *txnsnapshot.ClientHelper

	memTracker *memory.Tracker

	replicaReadSeed uint32

	enableCollectExecutionInfo bool
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh   chan<- *copTask
	wg       *sync.WaitGroup
	tasks    []*copTask
	finishCh <-chan struct{}
	respChan chan<- *copResponse
	sendRate *util.RateLimit
}

type copResponse struct {
	pbResp   *coprocessor.Response
	detail   *CopRuntimeStats
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

const sizeofExecDetails = int(unsafe.Sizeof(execdetails.ExecDetails{}))

// GetData implements the kv.ResultSubset GetData interface.
func (rs *copResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *copResponse) GetStartKey() kv.Key {
	return rs.startKey
}

func (rs *copResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *copResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}
	if rs == finCopResp {
		return 0
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
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

// When the worker finished `handleTask`, we need to notify the copIterator that there is one task finished.
// For the non-keep-order case, we send a finCopResp into the respCh after `handleTask`. When copIterator recv
// finCopResp from the respCh, it will be aware that there is one task finished.
var finCopResp *copResponse

func init() {
	finCopResp = &copResponse{}
}

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	defer func() {
		failpoint.Inject("ticase-4169", func(val failpoint.Value) {
			if val.(bool) {
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
			}
		})
		worker.wg.Done()
	}()
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}
		worker.handleTask(ctx, task, respCh)
		if worker.respChan != nil {
			// When a task is finished by the worker, send a finCopResp into channel to notify the copIterator that
			// there is a task finished.
			worker.sendToRespCh(finCopResp, worker.respChan, false)
		}
		close(task.respChan)
		if worker.finished() {
			return
		}
	}
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context, enabledRateLimitAction, enableCollectExecutionInfo bool) {
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency)
	// Start it.concurrency number of workers to handle cop requests.
	for i := 0; i < it.concurrency; i++ {
		worker := &copIteratorWorker{
			taskCh:                     taskCh,
			wg:                         &it.wg,
			store:                      it.store,
			req:                        it.req,
			respChan:                   it.respChan,
			finishCh:                   it.finishCh,
			vars:                       it.vars,
			kvclient:                   txnsnapshot.NewClientHelper(it.store.store, &it.resolvedLocks, &it.committedLocks, false),
			memTracker:                 it.memTracker,
			replicaReadSeed:            it.replicaReadSeed,
			enableCollectExecutionInfo: enableCollectExecutionInfo,
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
	it.actionOnExceed.setEnabled(enabledRateLimitAction)
	failpoint.Inject("ticase-4171", func(val failpoint.Value) {
		if val.(bool) {
			it.memTracker.Consume(10 * MockResponseSizeForTest)
			it.memTracker.Consume(10 * MockResponseSizeForTest)
		}
	})
	go taskSender.run()
}

func (sender *copIteratorTaskSender) run() {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		// we control the sending rate to prevent all tasks
		// being done (aka. all of the responses are buffered) by copIteratorWorker.
		// We keep the number of inflight tasks within the number of 2 * concurrency when Keep Order is true.
		// If KeepOrder is false, the number equals the concurrency.
		// It sends one more task if a task has been finished in copIterator.Next.
		exit := sender.sendRate.GetToken(sender.finishCh)
		if exit {
			break
		}
		exit = sender.sendToTaskCh(t)
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
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-respCh:
			if it.memTracker != nil && resp != nil {
				consumed := resp.MemSize()
				failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
					if val.(bool) {
						if resp != finCopResp {
							consumed = MockResponseSizeForTest
						}
					}
				})
				it.memTracker.Consume(-consumed)
			}
			return
		case <-it.finishCh:
			exit = true
			return
		case <-ticker.C:
			if atomic.LoadUint32(it.vars.Killed) == 1 {
				resp = &copResponse{err: derr.ErrQueryInterrupted}
				ok = true
				return
			}
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
				close(it.finishCh)
			}
			exit = true
			return
		}
	}
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
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		worker.memTracker.Consume(consumed)
	}
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

// MockResponseSizeForTest mock the response size
const MockResponseSizeForTest = 100 * 1024 * 1024

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *copResponse
		ok     bool
		closed bool
	)
	defer func() {
		if resp == nil {
			failpoint.Inject("ticase-4170", func(val failpoint.Value) {
				if val.(bool) {
					it.memTracker.Consume(10 * MockResponseSizeForTest)
					it.memTracker.Consume(10 * MockResponseSizeForTest)
				}
			})
		}
	}()
	// wait unit at least 5 copResponse received.
	failpoint.Inject("testRateLimitActionMockWaitMax", func(val failpoint.Value) {
		if val.(bool) {
			// we only need to trigger oom at least once.
			if len(it.tasks) > 9 {
				for it.memTracker.MaxConsumed() < 5*MockResponseSizeForTest {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	})
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.
	if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			it.actionOnExceed.close()
			return nil, nil
		}
		if resp == finCopResp {
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
			return it.Next(ctx)
		}
	} else {
		for {
			if it.curr >= len(it.tasks) {
				// Resp will be nil if iterator is finishCh.
				it.actionOnExceed.close()
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
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
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
	return resp, nil
}

// Associate each region with an independent backoffer. In this way, when multiple regions are
// unavailable, TiDB can execute very quickly without blocking
func chooseBackoffer(ctx context.Context, backoffermap map[uint64]*Backoffer, task *copTask, worker *copIteratorWorker) *Backoffer {
	bo, ok := backoffermap[task.region.GetID()]
	if ok {
		return bo
	}
	newbo := backoff.NewBackofferWithVars(ctx, copNextMaxBackoff, worker.vars)
	backoffermap[task.region.GetID()] = newbo
	return newbo
}

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(ctx context.Context, task *copTask, respCh chan<- *copResponse) {
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
	backoffermap := make(map[uint64]*Backoffer)
	for len(remainTasks) > 0 {
		curTask := remainTasks[0]
		bo := chooseBackoffer(ctx, backoffermap, curTask, worker)
		tasks, err := worker.handleTaskOnce(bo, curTask, respCh)
		if err != nil {
			resp := &copResponse{err: errors.Trace(err)}
			worker.sendToRespCh(resp, respCh, true)
			return
		}
		if worker.finished() {
			break
		}
		if len(tasks) > 0 {
			remainTasks = append(tasks, remainTasks[1:]...)
		} else {
			remainTasks = remainTasks[1:]
		}
	}
	if worker.store.coprCache != nil && worker.store.coprCache.cache.Metrics != nil {
		coprCacheHistogramEvict.Observe(float64(worker.store.coprCache.cache.Metrics.KeysEvicted()))
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
		Tp:         worker.req.Tp,
		StartTs:    worker.req.StartTs,
		Data:       worker.req.Data,
		Ranges:     task.ranges.ToPBRanges(),
		SchemaVer:  worker.req.SchemaVar,
		PagingSize: task.pagingSize,
	}

	var cacheKey []byte
	var cacheValue *coprCacheValue

	// TODO: cache paging copr
	// If there are many ranges, it is very likely to be a TableLookupRequest. They are not worth to cache since
	// computing is not the main cost. Ignore such requests directly to avoid slowly building the cache key.
	if task.cmdType == tikvrpc.CmdCop && !task.paging && worker.store.coprCache != nil && worker.req.Cacheable && worker.store.coprCache.CheckRequestAdmission(len(copReq.Ranges)) {
		cKey, err := coprCacheBuildKey(&copReq)
		if err == nil {
			cacheKey = cKey
			cValue := worker.store.coprCache.Get(cKey)
			copReq.IsCacheEnabled = true
			if cValue != nil && cValue.RegionID == task.region.GetID() && cValue.TimeStamp <= worker.req.StartTs {
				// Append cache version to the request to skip Coprocessor computation if possible
				// when request result is cached
				copReq.CacheIfMatchVersion = cValue.RegionDataVersion
				cacheValue = cValue
			} else {
				copReq.CacheIfMatchVersion = 0
			}
		} else {
			logutil.BgLogger().Warn("Failed to build copr cache key", zap.Error(err))
		}
	}

	req := tikvrpc.NewReplicaReadRequest(task.cmdType, &copReq, options.GetTiKVReplicaReadType(worker.req.ReplicaRead), &worker.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: isolationLevelToPB(worker.req.IsolationLevel),
		Priority:       priorityToPB(worker.req.Priority),
		NotFillCache:   worker.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         worker.req.TaskID,
	})
	if worker.req.ResourceGroupTagger != nil {
		worker.req.ResourceGroupTagger(req)
	}
	req.StoreTp = getEndPointType(task.storeType)
	startTime := time.Now()
	if worker.kvclient.Stats == nil {
		worker.kvclient.Stats = make(map[tikvrpc.CmdType]*tikv.RPCRuntimeStats)
	}
	req.ReadReplicaScope = worker.req.ReadReplicaScope
	if worker.req.IsStaleness {
		req.EnableStaleRead()
	}
	staleRead := req.GetStaleRead()
	ops := make([]tikv.StoreSelectorOption, 0, 2)
	if len(worker.req.MatchStoreLabels) > 0 {
		ops = append(ops, tikv.WithMatchLabels(worker.req.MatchStoreLabels))
	}
	resp, rpcCtx, storeAddr, err := worker.kvclient.SendReqCtx(bo.TiKVBackoffer(), req, task.region, tikv.ReadTimeoutMedium, getEndPointType(task.storeType), task.storeAddr, ops...)
	err = derr.ToTiDBErr(err)
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
	storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
	metrics.TiKVCoprocessorHistogram.WithLabelValues(storeID, strconv.FormatBool(staleRead)).Observe(costTime.Seconds())

	if task.cmdType == tikvrpc.CmdCopStream {
		return worker.handleCopStreamResult(bo, rpcCtx, resp.Resp.(*tikvrpc.CopStreamResponse), task, ch, costTime)
	}

	if worker.req.Paging {
		return worker.handleCopPagingResult(bo, rpcCtx, &copResponse{pbResp: resp.Resp.(*coprocessor.Response)}, task, ch, costTime)
	}

	// Handles the response for non-streaming copTask.
	return worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: resp.Resp.(*coprocessor.Response)}, cacheKey, cacheValue, task, ch, nil, costTime)
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
)

func (worker *copIteratorWorker) logTimeCopTask(costTime time.Duration, task *copTask, bo *Backoffer, resp *tikvrpc.Response) {
	logStr := fmt.Sprintf("[TIME_COP_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.GetID(), task.storeAddr)
	if bo.GetTotalSleep() > minLogBackoffTime {
		backoffTypes := strings.Replace(fmt.Sprintf("%v", bo.TiKVBackoffer().GetTypes()), " ", ",", -1)
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.GetTotalSleep(), backoffTypes)
	}
	var detailV2 *kvrpcpb.ExecDetailsV2
	var detail *kvrpcpb.ExecDetails
	if resp.Resp != nil {
		switch r := resp.Resp.(type) {
		case *coprocessor.Response:
			detailV2 = r.ExecDetailsV2
			detail = r.ExecDetails
		case *tikvrpc.CopStreamResponse:
			// streaming request returns io.EOF, so the first CopStreamResponse.Response maybe nil.
			if r.Response != nil {
				detailV2 = r.Response.ExecDetailsV2
				detail = r.Response.ExecDetails
			}
		default:
			panic("unreachable")
		}
	}

	var timeDetail *kvrpcpb.TimeDetail
	if detailV2 != nil && detailV2.TimeDetail != nil {
		timeDetail = detailV2.TimeDetail
	} else if detail != nil && detail.TimeDetail != nil {
		timeDetail = detail.TimeDetail
	}
	if timeDetail != nil {
		logStr += fmt.Sprintf(" kv_process_ms:%d", timeDetail.ProcessWallTimeMs)
		logStr += fmt.Sprintf(" kv_wait_ms:%d", timeDetail.WaitWallTimeMs)
		logStr += fmt.Sprintf(" kv_read_ms:%d", timeDetail.KvReadWallTimeMs)
		if timeDetail.ProcessWallTimeMs <= minLogKVProcessTime {
			logStr = strings.Replace(logStr, "TIME_COP_PROCESS", "TIME_COP_WAIT", 1)
		}
	}

	if detailV2 != nil && detailV2.ScanDetailV2 != nil {
		logStr += fmt.Sprintf(" processed_versions:%d", detailV2.ScanDetailV2.ProcessedVersions)
		logStr += fmt.Sprintf(" total_versions:%d", detailV2.ScanDetailV2.TotalVersions)
		logStr += fmt.Sprintf(" rocksdb_delete_skipped_count:%d", detailV2.ScanDetailV2.RocksdbDeleteSkippedCount)
		logStr += fmt.Sprintf(" rocksdb_key_skipped_count:%d", detailV2.ScanDetailV2.RocksdbKeySkippedCount)
		logStr += fmt.Sprintf(" rocksdb_cache_hit_count:%d", detailV2.ScanDetailV2.RocksdbBlockCacheHitCount)
		logStr += fmt.Sprintf(" rocksdb_read_count:%d", detailV2.ScanDetailV2.RocksdbBlockReadCount)
		logStr += fmt.Sprintf(" rocksdb_read_byte:%d", detailV2.ScanDetailV2.RocksdbBlockReadByte)
	} else if detail != nil && detail.ScanDetail != nil {
		logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
		logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
		logStr = appendScanDetail(logStr, "lock", detail.ScanDetail.Lock)
	}
	logutil.Logger(bo.GetCtx()).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	if scanInfo != nil {
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	return logStr
}

func (worker *copIteratorWorker) handleCopStreamResult(bo *Backoffer, rpcCtx *tikv.RPCContext, stream *tikvrpc.CopStreamResponse, task *copTask, ch chan<- *copResponse, costTime time.Duration) ([]*copTask, error) {
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

			err1 := errors.Errorf("recv stream response error: %v, task: %s", err, task)
			if task.storeType == kv.TiFlash {
				err1 = bo.Backoff(tikv.BoTiFlashRPC(), err1)
			} else {
				err1 = bo.Backoff(tikv.BoTiKVRPC(), err1)
			}

			if err1 != nil {
				return nil, errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			task.ranges = worker.calculateRemain(task.ranges, lastRange, worker.req.Desc)
			return []*copTask{task}, nil
		}
		if resp.Range != nil {
			lastRange = resp.Range
		}
	}
}

func (worker *copIteratorWorker) handleCopPagingResult(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, task *copTask, ch chan<- *copResponse, costTime time.Duration) ([]*copTask, error) {
	remainedTasks, err := worker.handleCopResponse(bo, rpcCtx, resp, nil, nil, task, ch, nil, costTime)
	if err != nil || len(remainedTasks) != 0 {
		// If there is region error or lock error, keep the paging size and retry.
		for _, remainedTask := range remainedTasks {
			remainedTask.pagingSize = task.pagingSize
		}
		return remainedTasks, errors.Trace(err)
	}
	pagingRange := resp.pbResp.Range
	// only paging requests need to calculate the next ranges
	if pagingRange == nil {
		return nil, errors.New("lastRange in paging should not be nil")
	}
	// calculate next ranges and grow the paging size
	task.ranges = worker.calculateRemain(task.ranges, pagingRange, worker.req.Desc)
	if task.ranges.Len() == 0 {
		return nil, nil
	}
	task.pagingSize = paging.GrowPagingSize(task.pagingSize)
	return []*copTask{task}, nil
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling streaming coprocessor response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, ch chan<- *copResponse, lastRange *coprocessor.KeyRange, costTime time.Duration) ([]*copTask, error) {
	if ver := resp.pbResp.GetLatestBucketsVersion(); task.bucketsVer < ver {
		worker.store.GetRegionCache().UpdateBucketsIfNeeded(task.region, ver)
	}
	if regionErr := resp.pbResp.GetRegionError(); regionErr != nil {
		if rpcCtx != nil && task.storeType == kv.TiDB {
			resp.err = errors.Errorf("error: %v", regionErr)
			worker.sendToRespCh(resp, ch, true)
			return nil, nil
		}
		errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
			task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, regionErr.String())
		if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		return buildCopTasks(bo, worker.store.GetRegionCache(), task.ranges, worker.req, task.eventCb)
	}
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		// Be care that we didn't redact the SQL statement because the log is DEBUG level.
		if task.eventCb != nil {
			task.eventCb(trxevents.WrapCopMeetLock(&trxevents.CopMeetLock{
				LockInfo: lockErr,
			}))
		} else {
			logutil.Logger(bo.GetCtx()).Debug("coprocessor encounters lock",
				zap.Stringer("lock", lockErr))
		}
		msBeforeExpired, err1 := worker.kvclient.ResolveLocks(bo.TiKVBackoffer(), worker.req.StartTs, []*txnlock.Lock{txnlock.NewLock(lockErr)})
		err1 = derr.ToTiDBErr(err1)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if msBeforeExpired > 0 {
			if err := bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
				return nil, errors.Trace(err)
			}
		}
		if worker.req.Streaming {
			task.ranges = worker.calculateRetry(task.ranges, lastRange, worker.req.Desc)
		}
		return []*copTask{task}, nil
	}
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)
		logutil.Logger(bo.GetCtx()).Warn("other error",
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.GetID()),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		if strings.Contains(err.Error(), "write conflict") {
			return nil, kv.ErrWriteConflict
		}
		return nil, errors.Trace(err)
	}
	// When the request is using streaming API, the `Range` is not nil.
	if resp.pbResp.Range != nil {
		resp.startKey = resp.pbResp.Range.Start
	} else if task.ranges != nil && task.ranges.Len() > 0 {
		resp.startKey = task.ranges.At(0).StartKey
	}
	worker.handleCollectExecutionInfo(bo, rpcCtx, resp)
	resp.respTime = costTime
	if resp.pbResp.IsCacheHit {
		if cacheValue == nil {
			return nil, errors.New("Internal error: received illegal TiKV response")
		}
		// Cache hit and is valid: use cached data as response data and we don't update the cache.
		data := make([]byte, len(cacheValue.Data))
		copy(data, cacheValue.Data)
		resp.pbResp.Data = data
		resp.detail.CoprCacheHit = true
	} else {
		// Cache not hit or cache hit but not valid: update the cache if the response can be cached.
		if cacheKey != nil && resp.pbResp.CanBeCached && resp.pbResp.CacheLastVersion > 0 {
			if worker.store.coprCache.CheckResponseAdmission(resp.pbResp.Data.Size(), resp.detail.TimeDetail.ProcessTime) {
				data := make([]byte, len(resp.pbResp.Data))
				copy(data, resp.pbResp.Data)

				newCacheValue := coprCacheValue{
					Data:              data,
					TimeStamp:         worker.req.StartTs,
					RegionID:          task.region.GetID(),
					RegionDataVersion: resp.pbResp.CacheLastVersion,
				}
				worker.store.coprCache.Set(cacheKey, &newCacheValue)
			}
		}
	}
	worker.sendToRespCh(resp, ch, true)
	return nil, nil
}

func (worker *copIteratorWorker) handleCollectExecutionInfo(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse) {
	defer func() {
		worker.kvclient.Stats = nil
	}()
	if !worker.enableCollectExecutionInfo {
		return
	}
	failpoint.Inject("disable-collect-execution", func(val failpoint.Value) {
		if val.(bool) {
			panic("shouldn't reachable")
		}
	})
	if resp.detail == nil {
		resp.detail = new(CopRuntimeStats)
	}
	resp.detail.Stats = worker.kvclient.Stats
	backoffTimes := bo.GetBackoffTimes()
	resp.detail.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(backoffTimes))
	for backoff := range backoffTimes {
		resp.detail.BackoffTimes[backoff] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
	}
	if rpcCtx != nil {
		resp.detail.CalleeAddress = rpcCtx.Addr
	}
	sd := &util.ScanDetail{}
	td := util.TimeDetail{}
	if pbDetails := resp.pbResp.ExecDetailsV2; pbDetails != nil {
		// Take values in `ExecDetailsV2` first.
		if timeDetail := pbDetails.TimeDetail; timeDetail != nil {
			td.MergeFromTimeDetail(timeDetail)
		}
		if scanDetailV2 := pbDetails.ScanDetailV2; scanDetailV2 != nil {
			sd.MergeFromScanDetailV2(scanDetailV2)
		}
	} else if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		if timeDetail := pbDetails.TimeDetail; timeDetail != nil {
			td.MergeFromTimeDetail(timeDetail)
		}
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			if scanDetail.Write != nil {
				sd.ProcessedKeys = scanDetail.Write.Processed
				sd.TotalKeys = scanDetail.Write.Total
			}
		}
	}
	resp.detail.ScanDetail = sd
	resp.detail.TimeDetail = td
}

// CopRuntimeStats contains execution detail information.
type CopRuntimeStats struct {
	execdetails.ExecDetails
	tikv.RegionRequestRuntimeStats

	CoprCacheHit bool
}

func (worker *copIteratorWorker) handleTiDBSendReqErr(err error, task *copTask, ch chan<- *copResponse) error {
	errCode := errno.ErrUnknown
	errMsg := err.Error()
	if terror.ErrorEqual(err, derr.ErrTiKVServerTimeout) {
		errCode = errno.ErrTiKVServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	if terror.ErrorEqual(err, derr.ErrTiFlashServerTimeout) {
		errCode = errno.ErrTiFlashServerTimeout
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
		detail: &CopRuntimeStats{},
	}
	worker.sendToRespCh(resp, ch, true)
	return nil
}

// calculateRetry splits the input ranges into two, and take one of them according to desc flag.
// It's used in streaming API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the retry ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the retry ranges should be [r1 --> r2) [r3 --> s2)
func (worker *copIteratorWorker) calculateRetry(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.End)
		return left
	}
	_, right := ranges.Split(split.Start)
	return right
}

// calculateRemain calculates the remain ranges to be processed, it's used in streaming and paging API.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s2 is consumed, so the remained ranges should be [s2 --> r4)
// In reverse scan order, all data after s1 is consumed, so the remained ranges should be [r1 --> s1)
func (worker *copIteratorWorker) calculateRemain(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.Start)
		return left
	}
	_, right := ranges.Split(split.End)
	return right
}

// finished checks the flags and finished channel, it tells whether the worker is finished.
func (worker *copIteratorWorker) finished() bool {
	if worker.vars != nil && worker.vars.Killed != nil && atomic.LoadUint32(worker.vars.Killed) == 1 {
		return true
	}
	select {
	case <-worker.finishCh:
		return true
	default:
		return false
	}
}

func (it *copIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finishCh)
	}
	it.rpcCancel.CancelAll()
	it.actionOnExceed.close()
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

// rateLimitAction an OOM Action which is used to control the token if OOM triggered. The token number should be
// set on initial. Each time the Action is triggered, one token would be destroyed. If the count of the token is less
// than 2, the action would be delegated to the fallback action.
type rateLimitAction struct {
	memory.BaseOOMAction
	// enabled indicates whether the rateLimitAction is permitted to Action. 1 means permitted, 0 denied.
	enabled uint32
	// totalTokenNum indicates the total token at initial
	totalTokenNum uint
	cond          struct {
		sync.Mutex
		// exceeded indicates whether have encountered OOM situation.
		exceeded bool
		// remainingTokenNum indicates the count of tokens which still exists
		remainingTokenNum uint
		once              sync.Once
		// triggerCountForTest indicates the total count of the rateLimitAction's Action being executed
		triggerCountForTest uint
	}
}

func newRateLimitAction(totalTokenNumber uint) *rateLimitAction {
	return &rateLimitAction{
		totalTokenNum: totalTokenNumber,
		cond: struct {
			sync.Mutex
			exceeded            bool
			remainingTokenNum   uint
			once                sync.Once
			triggerCountForTest uint
		}{
			Mutex:             sync.Mutex{},
			exceeded:          false,
			remainingTokenNum: totalTokenNumber,
			once:              sync.Once{},
		},
	}
}

// Action implements ActionOnExceed.Action
func (e *rateLimitAction) Action(t *memory.Tracker) {
	if !e.isEnabled() {
		if fallback := e.GetFallback(); fallback != nil {
			fallback.Action(t)
		}
		return
	}
	e.conditionLock()
	defer e.conditionUnlock()
	e.cond.once.Do(func() {
		if e.cond.remainingTokenNum < 2 {
			e.setEnabled(false)
			logutil.BgLogger().Info("memory exceeds quota, rateLimitAction delegate to fallback action",
				zap.Uint("total token count", e.totalTokenNum))
			if fallback := e.GetFallback(); fallback != nil {
				fallback.Action(t)
			}
			return
		}
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if e.cond.triggerCountForTest+e.cond.remainingTokenNum != e.totalTokenNum {
					panic("triggerCount + remainingTokenNum not equal to totalTokenNum")
				}
			}
		})
		logutil.BgLogger().Info("memory exceeds quota, destroy one token now.",
			zap.Int64("consumed", t.BytesConsumed()),
			zap.Int64("quota", t.GetBytesLimit()),
			zap.Uint("total token count", e.totalTokenNum),
			zap.Uint("remaining token count", e.cond.remainingTokenNum))
		e.cond.exceeded = true
		e.cond.triggerCountForTest++
	})
}

// SetLogHook implements ActionOnExceed.SetLogHook
func (e *rateLimitAction) SetLogHook(hook func(uint64)) {

}

// GetPriority get the priority of the Action.
func (e *rateLimitAction) GetPriority() int64 {
	return memory.DefRateLimitPriority
}

// destroyTokenIfNeeded will check the `exceed` flag after copWorker finished one task.
// If the exceed flag is true and there is no token been destroyed before, one token will be destroyed,
// or the token would be return back.
func (e *rateLimitAction) destroyTokenIfNeeded(returnToken func()) {
	if !e.isEnabled() {
		returnToken()
		return
	}
	e.conditionLock()
	defer e.conditionUnlock()
	if !e.cond.exceeded {
		returnToken()
		return
	}
	// If actionOnExceed has been triggered and there is no token have been destroyed before,
	// destroy one token.
	e.cond.remainingTokenNum = e.cond.remainingTokenNum - 1
	e.cond.exceeded = false
	e.cond.once = sync.Once{}
}

func (e *rateLimitAction) conditionLock() {
	e.cond.Lock()
}

func (e *rateLimitAction) conditionUnlock() {
	e.cond.Unlock()
}

func (e *rateLimitAction) close() {
	if !e.isEnabled() {
		return
	}
	e.setEnabled(false)
	e.conditionLock()
	defer e.conditionUnlock()
	e.cond.exceeded = false
	e.SetFinished()
}

func (e *rateLimitAction) setEnabled(enabled bool) {
	newValue := uint32(0)
	if enabled {
		newValue = uint32(1)
	}
	atomic.StoreUint32(&e.enabled, newValue)
}

func (e *rateLimitAction) isEnabled() bool {
	return atomic.LoadUint32(&e.enabled) > 0
}

// priorityToPB converts priority type to wire type.
func priorityToPB(pri int) kvrpcpb.CommandPri {
	switch pri {
	case kv.PriorityLow:
		return kvrpcpb.CommandPri_Low
	case kv.PriorityHigh:
		return kvrpcpb.CommandPri_High
	default:
		return kvrpcpb.CommandPri_Normal
	}
}

func isolationLevelToPB(level kv.IsoLevel) kvrpcpb.IsolationLevel {
	switch level {
	case kv.RC:
		return kvrpcpb.IsolationLevel_RC
	case kv.SI:
		return kvrpcpb.IsolationLevel_SI
	case kv.RCCheckTS:
		return kvrpcpb.IsolationLevel_RCCheckTS
	default:
		return kvrpcpb.IsolationLevel_SI
	}
}
