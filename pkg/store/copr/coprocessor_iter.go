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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
)

// CopInfo is used to expose functions of copIterator.
type CopInfo interface {
	// GetConcurrency returns the concurrency and small task concurrency.
	GetConcurrency() (int, int)
	// GetStoreBatchInfo returns the batched and fallback num.
	GetStoreBatchInfo() (uint64, uint64)
	// GetBuildTaskElapsed returns the duration of building task.
	GetBuildTaskElapsed() time.Duration
}

type copIterator struct {
	store                *Store
	req                  *kv.Request
	concurrency          int
	smallTaskConcurrency int
	// liteWorker uses to send cop request without start new goroutine, it is only work when tasks count is 1, and used to improve the performance of small cop query.
	liteWorker *liteCopIteratorWorker
	finishCh   chan struct{}

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
	pagingTaskIdx  uint32

	buildTaskElapsed        time.Duration
	storeBatchedNum         atomic.Uint64
	storeBatchedFallbackNum atomic.Uint64

	runawayChecker resourcegroup.RunawayChecker
	stats          *copIteratorRuntimeStats
}

type liteCopIteratorWorker struct {
	// ctx contains some info(such as rpc interceptor(WithSQLKvExecCounterInterceptor)), it is used for handle cop task later.
	ctx              context.Context
	worker           *copIteratorWorker
	batchCopRespList []*copResponse
	tryCopLiteWorker *atomic2.Uint32
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

	pagingTaskIdx *uint32

	storeBatchedNum         *atomic.Uint64
	storeBatchedFallbackNum *atomic.Uint64
	stats                   *copIteratorRuntimeStats
}

// copIteratorTaskSender sends tasks to taskCh then wait for the workers to exit.
type copIteratorTaskSender struct {
	taskCh      chan<- *copTask
	smallTaskCh chan<- *copTask
	wg          *sync.WaitGroup
	tasks       []*copTask
	finishCh    <-chan struct{}
	respChan    chan<- *copResponse
	sendRate    *util.RateLimit
}

type copResponse struct {
	pbResp   *coprocessor.Response
	detail   *CopRuntimeStats
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

type copTaskResult struct {
	resp          *copResponse
	batchRespList []*copResponse
	remains       []*copTask
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
