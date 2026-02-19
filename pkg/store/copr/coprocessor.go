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
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	copBuildTaskMaxBackoff = 5000
	CopNextMaxBackoff      = 20000
	CopSmallTaskRow        = 32 // 32 is the initial batch size of TiKV
	smallTaskSigma         = 0.5
	smallConcPerCore       = 20
)

var liteWorkerFallbackHook atomic.Pointer[func()]

// CopClient is coprocessor client.
type CopClient struct {
	kv.RequestTypeSupportedChecker
	store           *Store
	replicaReadSeed uint32
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request, variables any, option *kv.ClientSendOption) kv.Response {
	vars, ok := variables.(*tikv.Variables)
	if !ok {
		return copErrorResponse{errors.Errorf("unsupported variables:%+v", variables)}
	}
	if req.StoreType == kv.TiFlash && req.BatchCop {
		logutil.BgLogger().Debug("send batch requests")
		return c.sendBatch(ctx, req, vars, option)
	}
	ctx = context.WithValue(ctx, tikv.TxnStartKey(), req.StartTs)
	ctx = context.WithValue(ctx, util.RequestSourceKey, req.RequestSource)
	ctx = interceptor.WithRPCInterceptor(ctx, interceptor.GetRPCInterceptorFromCtx(ctx))
	it, errRes := c.BuildCopIterator(ctx, req, vars, option)
	if errRes != nil {
		return errRes
	}
	ctx = context.WithValue(ctx, tikv.RPCCancellerCtxKey{}, it.rpcCancel)
	if ctx.Value(util.RUDetailsCtxKey) == nil {
		ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	}
	it.open(ctx, option.TryCopLiteWorker)
	return it
}

// BuildCopIterator builds the iterator without calling `open`.
func (c *CopClient) BuildCopIterator(ctx context.Context, req *kv.Request, vars *tikv.Variables, option *kv.ClientSendOption) (*copIterator, kv.Response) {
	eventCb := option.EventCb
	failpoint.Inject("DisablePaging", func(_ failpoint.Value) {
		req.Paging.Enable = false
	})
	if req.StoreType == kv.TiDB {
		// coprocessor on TiDB doesn't support paging
		req.Paging.Enable = false
	}
	if req.Tp != kv.ReqTypeDAG {
		// coprocessor request but type is not DAG
		req.Paging.Enable = false
	}
	failpoint.Inject("checkKeyRangeSortedForPaging", func(_ failpoint.Value) {
		if req.Paging.Enable {
			if !req.KeyRanges.IsFullySorted() {
				logutil.BgLogger().Fatal("distsql request key range not sorted!")
			}
		}
	})
	if !checkStoreBatchCopr(req) {
		req.StoreBatchSize = 0
	}

	boCtx := ctx
	if req.MaxExecutionTime > 0 {
		// If the request has a MaxExecutionTime, we need to set the deadline of the context.
		var cancel context.CancelFunc
		boCtx, cancel = context.WithTimeout(boCtx, time.Duration(req.MaxExecutionTime)*time.Millisecond)
		defer func() {
			cancel()
		}()
	}
	bo := backoff.NewBackofferWithVars(boCtx, copBuildTaskMaxBackoff, vars)
	var (
		tasks []*copTask
		err   error
	)
	tryRowHint := optRowHint(req)
	elapsed := time.Duration(0)
	buildOpt := &buildCopTaskOpt{
		req:      req,
		cache:    c.store.GetRegionCache(),
		eventCb:  eventCb,
		respChan: req.KeepOrder,
		elapsed:  &elapsed,
	}
	buildTaskFunc := func(ranges []kv.KeyRange, hints []int) error {
		keyRanges := NewKeyRanges(ranges)
		if tryRowHint {
			buildOpt.rowHints = hints
		}
		tasksFromRanges, err := buildCopTasks(bo, keyRanges, buildOpt)
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			tasks = tasksFromRanges
			return nil
		}
		tasks = append(tasks, tasksFromRanges...)
		return nil
	}
	// Here we build the task by partition, not directly by region.
	// This is because it's possible that TiDB merge multiple small partition into one region which break some assumption.
	// Keep it split by partition would be more safe.
	err = req.KeyRanges.ForEachPartitionWithErr(buildTaskFunc)
	// only batch store requests in first build.
	req.StoreBatchSize = 0
	reqType := "null"
	if req.ClosestReplicaReadAdjuster != nil {
		reqType = "miss"
		if req.ClosestReplicaReadAdjuster(req, len(tasks)) {
			reqType = "hit"
		}
	}
	tidbmetrics.DistSQLCoprClosestReadCounter.WithLabelValues(reqType).Inc()
	if err != nil {
		return nil, copErrorResponse{err}
	}
	it := &copIterator{
		store:            c.store,
		req:              req,
		concurrency:      req.Concurrency,
		finishCh:         make(chan struct{}),
		vars:             vars,
		memTracker:       req.MemTracker,
		replicaReadSeed:  c.replicaReadSeed,
		rpcCancel:        tikv.NewRPCanceller(),
		buildTaskElapsed: *buildOpt.elapsed,
		runawayChecker:   req.RunawayChecker,
	}
	// Pipelined-dml can flush locks when it is still reading.
	// The coprocessor of the txn should not be blocked by itself.
	// It should be the only case where a coprocessor can read locks of the same ts.
	//
	// But when start_ts is not obtained from PD,
	// the start_ts could conflict with another pipelined-txn's start_ts.
	// in which case the locks of same ts cannot be ignored.
	// We rely on the assumption: start_ts is not from PD => this is a stale read.
	if !req.IsStaleness {
		it.resolvedLocks.Put(req.StartTs)
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if tryRowHint {
		var smallTasks int
		smallTasks, it.smallTaskConcurrency = smallTaskConcurrency(tasks, c.store.numcpu)
		if len(tasks)-smallTasks < it.concurrency {
			it.concurrency = len(tasks) - smallTasks
		}
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}

	// issue56916 is about the cooldown of the runaway checker may block the SQL execution.
	failpoint.Inject("issue56916", func(_ failpoint.Value) {
		it.concurrency = 1
		it.smallTaskConcurrency = 0
	})

	// if the request is triggered cool down by the runaway checker, we need to adjust the concurrency, let the sql run slowly.
	if req.RunawayChecker != nil && req.RunawayChecker.CheckAction() == rmpb.RunawayAction_CoolDown {
		it.concurrency = 1
		it.smallTaskConcurrency = 0
	}

	if it.req.KeepOrder {
		if it.smallTaskConcurrency > 20 {
			it.smallTaskConcurrency = 20
		}
		it.sendRate = util.NewRateLimit(2 * (it.concurrency + it.smallTaskConcurrency))
		it.respChan = nil
	} else {
		it.respChan = make(chan *copResponse)
		it.sendRate = util.NewRateLimit(it.concurrency + it.smallTaskConcurrency)
	}
	if option.EnableCollectExecutionInfo {
		it.stats = &copIteratorRuntimeStats{}
	}
	it.actionOnExceed = newRateLimitAction(uint(it.sendRate.GetCapacity()))
	if option.SessionMemTracker != nil && option.EnabledRateLimitAction {
		option.SessionMemTracker.FallbackOldAndSetNewAction(it.actionOnExceed)
	}
	it.actionOnExceed.setEnabled(option.EnabledRateLimitAction)
	return it, nil
}

// copTask contains a related Region and KeyRange for a kv.Request.
type copTask struct {
	taskID     uint64
	region     tikv.RegionVerID
	bucketsVer uint64
	ranges     *KeyRanges
	// buildLocStartKey/buildLocEndKey are the location boundaries used when building
	// this task. They are only set in skip-buckets rebuild path for diagnostics.
	buildLocStartKey []byte
	buildLocEndKey   []byte

	respChan  chan *copResponse
	storeAddr string
	cmdType   tikvrpc.CmdType
	storeType kv.StoreType

	eventCb       trxevents.EventCallback
	paging        bool
	pagingSize    uint64
	pagingTaskIdx uint32

	partitionIndex int64 // used by balanceBatchCopTask in PartitionTableScan
	requestSource  util.RequestSource
	RowCountHint   int // used for extra concurrency of small tasks, -1 for unknown row count
	batchTaskList  map[uint64]*batchedCopTask

	// when this task is batched and the leader's wait duration exceeds the load-based threshold,
	// we set this field to the target replica store ID and redirect the request to the replica.
	redirect2Replica *uint64
	busyThreshold    time.Duration
	meetLockFallback bool

	// timeout value for one kv readonly request
	tikvClientReadTimeout uint64
	// firstReadType is used to indicate the type of first read when retrying.
	firstReadType string
	// skipBuckets indicates this task was built without bucket-level splitting.
	// If "Request range exceeds bound" still occurs on such a task, we apply
	// bounded self-healing retries; after budget is exhausted, fail fast.
	skipBuckets bool
	// exceedsBoundRetry tracks how many "Request range exceeds bound" retries have
	// been attempted for this task.
	exceedsBoundRetry int
}

type batchedCopTask struct {
	task                  *copTask
	region                coprocessor.RegionInfo
	storeID               uint64
	peer                  *metapb.Peer
	loadBasedReplicaRetry bool
}

func (r *copTask) String() string {
	return fmt.Sprintf("region(%d %d %d) ranges(%d) store(%s)",
		r.region.GetID(), r.region.GetConfVer(), r.region.GetVer(), r.ranges.Len(), r.storeAddr)
}

func (r *copTask) ToPBBatchTasks() []*coprocessor.StoreBatchTask {
	if len(r.batchTaskList) == 0 {
		return nil
	}
	pbTasks := make([]*coprocessor.StoreBatchTask, 0, len(r.batchTaskList))
	for _, task := range r.batchTaskList {
		storeBatchTask := &coprocessor.StoreBatchTask{
			RegionId:    task.region.GetRegionId(),
			RegionEpoch: task.region.GetRegionEpoch(),
			Peer:        task.peer,
			Ranges:      task.region.GetRanges(),
			TaskId:      task.task.taskID,
		}
		pbTasks = append(pbTasks, storeBatchTask)
	}
	return pbTasks
}

// rangesPerTask limits the length of the ranges slice sent in one copTask.
const (
	rangesPerTask          = 25000
	maxExceedsBoundRetries = 3
)

type buildCopTaskOpt struct {
	req      *kv.Request
	cache    *RegionCache
	eventCb  trxevents.EventCallback
	respChan bool
	rowHints []int
	elapsed  *time.Duration
	// ignoreTiKVClientReadTimeout is used to ignore tikv_client_read_timeout configuration, use default timeout instead.
	ignoreTiKVClientReadTimeout bool
	// skipBuckets skips bucket-level splitting, only splitting by region.
	// Used when retrying after bucket-related errors.
	skipBuckets bool
	// exceedsBoundRetry propagates bounded retry attempts to generated tasks.
	exceedsBoundRetry int
}

const (
	rangeIssueDuplicate    = "duplicate"
	rangeIssueOverlap      = "overlap"
	rangeIssueContain      = "contain"
	rangeIssueOutOfOrder   = "outOfOrder"
	rangeIssueInvalidBound = "invalidBounds"
	rangeIssueInfiniteTail = "infiniteTail"
)

type rangeIssueStats struct {
	Duplicate    int `json:"duplicate,omitempty"`
	Overlap      int `json:"overlap,omitempty"`
	Contain      int `json:"contain,omitempty"`
	OutOfOrder   int `json:"outOfOrder,omitempty"`
	InvalidBound int `json:"invalidBounds,omitempty"`
	InfiniteTail int `json:"infiniteTail,omitempty"`
}

func (s *rangeIssueStats) add(category string) {
	switch category {
	case rangeIssueDuplicate:
		s.Duplicate++
	case rangeIssueOverlap:
		s.Overlap++
	case rangeIssueContain:
		s.Contain++
	case rangeIssueOutOfOrder:
		s.OutOfOrder++
	case rangeIssueInvalidBound:
		s.InvalidBound++
	case rangeIssueInfiniteTail:
		s.InfiniteTail++
	}
}

func (s rangeIssueStats) empty() bool {
	return s == (rangeIssueStats{})
}

func compareRangeEnd(a, b []byte) int {
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	return bytes.Compare(a, b)
}

func rangeContains(a, b kv.KeyRange) bool {
	if bytes.Compare(a.StartKey, b.StartKey) > 0 {
		return false
	}
	return compareRangeEnd(a.EndKey, b.EndKey) >= 0
}

func rangesOverlap(a, b kv.KeyRange) bool {
	if len(a.EndKey) > 0 && bytes.Compare(a.EndKey, b.StartKey) <= 0 {
		return false
	}
	if len(b.EndKey) > 0 && bytes.Compare(b.EndKey, a.StartKey) <= 0 {
		return false
	}
	return true
}

func classifyRangePair(prev, curr kv.KeyRange) string {
	switch {
	case bytes.Equal(prev.StartKey, curr.StartKey) && bytes.Equal(prev.EndKey, curr.EndKey):
		return rangeIssueDuplicate
	case rangeContains(prev, curr):
		return rangeIssueContain
	case rangeContains(curr, prev):
		return rangeIssueContain
	case rangesOverlap(prev, curr):
		return rangeIssueOverlap
	default:
		return rangeIssueOutOfOrder
	}
}

func ensureMonotonicKeyRanges(ctx context.Context, ranges *KeyRanges) bool {
	if ranges == nil || ranges.Len() == 0 {
		return false
	}
	total := ranges.Len()
	prev := ranges.At(0)
	var stats rangeIssueStats
	validateRange := func(idx int, r kv.KeyRange) bool {
		if len(r.EndKey) > 0 && bytes.Compare(r.StartKey, r.EndKey) > 0 {
			logutil.Logger(ctx).Error("invalid key range start > end",
				zap.String("start", redact.Key(r.StartKey)),
				zap.String("end", redact.Key(r.EndKey)))
			stats.add(rangeIssueInvalidBound)
			return false
		}
		return true
	}
	valid := validateRange(0, prev)
	sorted := true
	for i := 1; i < total; i++ {
		curr := ranges.At(i)
		valid = validateRange(i, curr) && valid
		switch {
		case len(prev.EndKey) == 0:
			sorted = false
			stats.add(rangeIssueInfiniteTail)
		case bytes.Compare(prev.EndKey, curr.StartKey) > 0:
			sorted = false
			stats.add(classifyRangePair(prev, curr))
		}
		prev = curr
	}
	if sorted && valid {
		return false
	}
	flat := ranges.ToRanges()
	fields := []zap.Field{
		zap.Int("rangeCount", ranges.Len()),
		formatRanges(NewKeyRanges(flat)),
		zap.Stack("stack"),
	}
	if !stats.empty() {
		fields = append(fields, zap.Any("rangeIssues", stats))
	}
	logutil.Logger(ctx).Error("key ranges not monotonic, reorder before BatchLocateKeyRanges", fields...)
	sortedRanges := append([]kv.KeyRange(nil), flat...)
	slices.SortFunc(sortedRanges, func(a, b kv.KeyRange) int {
		if cmp := bytes.Compare(a.StartKey, b.StartKey); cmp != 0 {
			return cmp
		}
		return bytes.Compare(a.EndKey, b.EndKey)
	})
	ranges.Reset(sortedRanges)
	return true
}

func buildCopTasks(bo *Backoffer, ranges *KeyRanges, opt *buildCopTaskOpt) ([]*copTask, error) {
	req, cache, eventCb, hints := opt.req, opt.cache, opt.eventCb, opt.rowHints
	start := time.Now()
	ctx := bo.GetCtx()
	defer tracing.StartRegion(ctx, "copr.buildCopTasks").End()
	if traceevent.IsEnabled(traceevent.KvRequest) {
		traceevent.TraceEvent(ctx, traceevent.KvRequest, "copr.build_ranges",
			zap.Uint64("connID", req.ConnID),
			zap.String("connAlias", req.ConnAlias),
			zap.Int("rangeCount", ranges.Len()),
			formatRanges(ranges))
	}
	reordered := ensureMonotonicKeyRanges(ctx, ranges)
	cmdType := tikvrpc.CmdCop
	if req.StoreType == kv.TiDB {
		return buildTiDBMemCopTasks(ranges, req)
	}
	rangesLen := ranges.Len()
	// something went wrong, disable hints to avoid out of range index.
	if len(hints) != rangesLen {
		hints = nil
	} else if reordered {
		hints = nil
	}

	rangesPerTaskLimit := rangesPerTask
	failpoint.Inject("setRangesPerTask", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			rangesPerTaskLimit = v
		}
	})

	// TODO(youjiali1995): is there any request type that needn't be split by buckets?
	var locs []*LocationKeyRanges
	var err error
	if opt.skipBuckets {
		// Skip bucket splitting - only split by region.
		// Used when retrying after "Request range exceeds bound" error,
		// which may be caused by stale bucket metadata.
		locs, err = cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, false)
	} else {
		locs, err = cache.SplitKeyRangesByBuckets(bo, ranges)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if opt.skipBuckets {
		kvRanges := make([]tikv.KeyRange, 0, ranges.Len())
		for i := range ranges.Len() {
			kvRanges = append(kvRanges, tikv.KeyRange{
				StartKey: ranges.At(i).StartKey,
				EndKey:   ranges.At(i).EndKey,
			})
		}
		tikvLocs := make([]*tikv.KeyLocation, 0, len(locs))
		for _, l := range locs {
			if l == nil {
				continue
			}
			tikvLocs = append(tikvLocs, l.Location)
		}
		coverageValid := validateLocationCoverage(ctx, kvRanges, tikvLocs)
		if !coverageValid {
			logutil.Logger(ctx).Error("skip-buckets SplitKeyRangesByLocations produced invalid location coverage",
				zap.Int("locationCount", len(locs)),
				zap.Int("rangeCount", ranges.Len()),
				zap.Any("rangeIssues", rangeIssuesForKeyRanges(ranges)),
				formatRanges(ranges),
				zap.Stack("stack"),
			)
		}

		for locIdx, loc := range locs {
			if loc == nil || loc.Location == nil || loc.Ranges == nil {
				continue
			}
			badIdx, badRange, badReason := firstOutOfBoundKeyRangeInLocation(loc.Ranges, loc.Location.StartKey, loc.Location.EndKey)
			if badIdx < 0 {
				continue
			}
			fields := []zap.Field{
				zap.Int("locationIndex", locIdx),
				zap.Int("locationCount", len(locs)),
				zap.Int("locationRangeCount", loc.Ranges.Len()),
				zap.Any("locationRangeIssues", rangeIssuesForKeyRanges(loc.Ranges)),
				zap.Int("outOfBoundRangeIndex", badIdx),
				zap.String("outOfBoundReason", badReason),
				keyField("outOfBoundRangeStartKey", badRange.StartKey),
				keyField("outOfBoundRangeEndKey", badRange.EndKey),
				zap.Uint64("regionID", loc.Location.Region.GetID()),
				zap.Uint64("regionVer", loc.Location.Region.GetVer()),
				zap.Uint64("regionConfVer", loc.Location.Region.GetConfVer()),
				keyField("locationStart", loc.Location.StartKey),
				keyField("locationEnd", loc.Location.EndKey),
			}
			cacheLoc := cache.RegionCache.TryLocateKey(badRange.StartKey)
			if cacheLoc == nil {
				fields = append(fields, zap.Bool("cacheLocateByBadRangeStartMissing", true))
			} else {
				fields = append(fields, formatKeyLocation("cacheLocateByBadRangeStart", cacheLoc))
			}
			pdLoc, pdErr := cache.RegionCache.LocateRegionByIDFromPD(bo.TiKVBackoffer(), loc.Location.Region.GetID())
			if pdErr != nil {
				fields = append(fields, zap.Error(pdErr))
			} else {
				fields = append(fields,
					zap.Uint64("pdRegionVer", pdLoc.Region.GetVer()),
					zap.Uint64("pdRegionConfVer", pdLoc.Region.GetConfVer()),
					keyField("pdRegionStartKey", pdLoc.StartKey),
					keyField("pdRegionEndKey", pdLoc.EndKey),
					zap.Bool("pdEpochChanged", pdLoc.Region.GetVer() != loc.Location.Region.GetVer() || pdLoc.Region.GetConfVer() != loc.Location.Region.GetConfVer()),
					zap.Bool("pdBoundaryChanged", !bytes.Equal(pdLoc.StartKey, loc.Location.StartKey) || !bytes.Equal(pdLoc.EndKey, loc.Location.EndKey)),
				)
			}
			fields = append(fields,
				formatLocation(loc.Location),
				formatRanges(loc.Ranges),
				zap.Stack("stack"),
			)
			logutil.Logger(ctx).Error("skip-buckets SplitKeyRangesByLocations produced out-of-bound ranges before sending", fields...)
		}
	}
	// Channel buffer is 2 for handling region split.
	// In a common case, two region split tasks will not be blocked.
	chanSize := 2
	// in paging request, a request will be returned in multi batches,
	// enlarge the channel size to avoid the request blocked by buffer full.
	if req.Paging.Enable {
		chanSize = 18
	}

	var builder taskBuilder
	if req.StoreBatchSize > 0 && hints != nil {
		builder = newBatchTaskBuilder(bo, req, cache, req.ReplicaRead)
	} else {
		builder = newLegacyTaskBuilder(len(locs))
	}
	origRangeIdx := 0
	for _, loc := range locs {
		// TiKV will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := loc.Ranges.Len()
		// If this is a paging request, we set the paging size to minPagingSize,
		// the size will grow every round.
		pagingSize := uint64(0)
		if req.Paging.Enable {
			pagingSize = req.Paging.MinPagingSize
		}
		for i := 0; i < rLen; {
			nextI := min(i+rangesPerTaskLimit, rLen)
			hint := -1
			// calculate the row count hint
			if hints != nil {
				startKey, endKey := loc.Ranges.RefAt(i).StartKey, loc.Ranges.RefAt(nextI-1).EndKey
				// move to the previous range if startKey of current range is lower than endKey of previous location.
				// In the following example, task1 will move origRangeIdx to region(i, z).
				// When counting the row hint for task2, we need to move origRangeIdx back to region(a, h).
				// |<-      region(a, h)    ->| |<-   region(i, z)   ->|
				// |<- task1 ->| |<- task2 ->| ...
				if origRangeIdx > 0 && ranges.RefAt(origRangeIdx-1).EndKey.Cmp(startKey) > 0 {
					origRangeIdx--
				}
				hint = 0
				for nextOrigRangeIdx := origRangeIdx; nextOrigRangeIdx < ranges.Len(); nextOrigRangeIdx++ {
					rangeStart := ranges.RefAt(nextOrigRangeIdx).StartKey
					if rangeStart.Cmp(endKey) > 0 {
						origRangeIdx = nextOrigRangeIdx
						break
					}
					hint += hints[nextOrigRangeIdx]
				}
			}
			task := &copTask{
				region:            loc.Location.Region,
				bucketsVer:        loc.getBucketVersion(),
				ranges:            loc.Ranges.Slice(i, nextI),
				cmdType:           cmdType,
				storeType:         req.StoreType,
				eventCb:           eventCb,
				paging:            req.Paging.Enable,
				pagingSize:        pagingSize,
				requestSource:     req.RequestSource,
				RowCountHint:      hint,
				busyThreshold:     req.StoreBusyThreshold,
				skipBuckets:       opt.skipBuckets,
				exceedsBoundRetry: opt.exceedsBoundRetry,
			}
			if opt.skipBuckets {
				task.buildLocStartKey = append([]byte(nil), loc.Location.StartKey...)
				task.buildLocEndKey = append([]byte(nil), loc.Location.EndKey...)
			}
			if !opt.ignoreTiKVClientReadTimeout {
				task.tikvClientReadTimeout = req.TiKVClientReadTimeout
			}
			// only keep-order need chan inside task.
			// tasks by region error will reuse the channel of parent task.
			if req.KeepOrder && opt.respChan {
				task.respChan = make(chan *copResponse, chanSize)
			}
			if err = builder.handle(task); err != nil {
				return nil, err
			}
			i = nextI
			if req.Paging.Enable {
				if req.LimitSize != 0 && req.LimitSize < pagingSize {
					// disable paging for small limit.
					task.paging = false
					task.pagingSize = 0
				} else {
					pagingSize = paging.GrowPagingSize(pagingSize, req.Paging.MaxPagingSize)
				}
			}
		}
	}

	if req.Desc {
		builder.reverse()
	}
	tasks := builder.build()
	elapsed := time.Since(start)
	if elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildCopTasks takes too much time",
			zap.Duration("elapsed", elapsed),
			zap.Int("range len", rangesLen),
			zap.Int("task len", len(tasks)))
	}
	if opt.elapsed != nil {
		*opt.elapsed = *opt.elapsed + elapsed
	}
	metrics.TxnRegionsNumHistogramWithCoprocessor.Observe(float64(builder.regionNum()))
	return tasks, nil
}

type taskBuilder interface {
	handle(*copTask) error
	reverse()
	build() []*copTask
	regionNum() int
}

type legacyTaskBuilder struct {
	tasks []*copTask
}

func newLegacyTaskBuilder(hint int) *legacyTaskBuilder {
	return &legacyTaskBuilder{
		tasks: make([]*copTask, 0, hint),
	}
}

func (b *legacyTaskBuilder) handle(task *copTask) error {
	b.tasks = append(b.tasks, task)
	return nil
}

func (b *legacyTaskBuilder) regionNum() int {
	return len(b.tasks)
}

func (b *legacyTaskBuilder) reverse() {
	reverseTasks(b.tasks)
}

func (b *legacyTaskBuilder) build() []*copTask {
	return b.tasks
}

type storeReplicaKey struct {
	storeID     uint64
	replicaRead bool
}

type batchStoreTaskBuilder struct {
	bo          *Backoffer
	req         *kv.Request
	cache       *RegionCache
	taskID      uint64
	limit       int
	store2Idx   map[storeReplicaKey]int
	tasks       []*copTask
	replicaRead kv.ReplicaReadType
}

func newBatchTaskBuilder(bo *Backoffer, req *kv.Request, cache *RegionCache, replicaRead kv.ReplicaReadType) *batchStoreTaskBuilder {
	return &batchStoreTaskBuilder{
		bo:          bo,
		req:         req,
		cache:       cache,
		taskID:      0,
		limit:       req.StoreBatchSize,
		store2Idx:   make(map[storeReplicaKey]int, 16),
		tasks:       make([]*copTask, 0, 16),
		replicaRead: replicaRead,
	}
}

func (b *batchStoreTaskBuilder) handle(task *copTask) (err error) {
	b.taskID++
	task.taskID = b.taskID
	handled := false
	defer func() {
		if !handled && err == nil {
			// fallback to non-batch way. It's mainly caused by region miss.
			b.tasks = append(b.tasks, task)
		}
	}()
	// only batch small tasks for memory control.
	if b.limit <= 0 || !isSmallTask(task) {
		return nil
	}
	batchedTask, err := b.cache.BuildBatchTask(b.bo, b.req, task, b.replicaRead)
	if err != nil {
		return err
	}
	if batchedTask == nil {
		return nil
	}
	key := storeReplicaKey{
		storeID:     batchedTask.storeID,
		replicaRead: batchedTask.loadBasedReplicaRetry,
	}
	if idx, ok := b.store2Idx[key]; !ok || len(b.tasks[idx].batchTaskList) >= b.limit {
		if batchedTask.loadBasedReplicaRetry {
			// If the task is dispatched to leader because all followers are busy,
			// task.redirect2Replica != nil means the busy threshold shouldn't take effect again.
			batchedTask.task.redirect2Replica = &batchedTask.storeID
		}
		b.tasks = append(b.tasks, batchedTask.task)
		b.store2Idx[key] = len(b.tasks) - 1
	} else {
		if b.tasks[idx].batchTaskList == nil {
			b.tasks[idx].batchTaskList = make(map[uint64]*batchedCopTask, b.limit)
			// disable paging for batched task.
			b.tasks[idx].paging = false
			b.tasks[idx].pagingSize = 0
			// The task and it's batched can be served only in the store we chose.
			// If the task is redirected to other replica, the batched task may not meet region-miss or store-not-match error.
			// So disable busy threshold for the task which carries batched tasks.
			b.tasks[idx].busyThreshold = 0
		}
		if task.RowCountHint > 0 {
			b.tasks[idx].RowCountHint += task.RowCountHint
		}
		batchedTask.task.paging = false
		batchedTask.task.pagingSize = 0
		batchedTask.task.busyThreshold = 0
		b.tasks[idx].batchTaskList[task.taskID] = batchedTask
	}
	handled = true
	return nil
}

func (b *batchStoreTaskBuilder) regionNum() int {
	// we allocate b.taskID for each region task, so the final b.taskID is equal to the related region number.
	return int(b.taskID)
}

func (b *batchStoreTaskBuilder) reverse() {
	reverseTasks(b.tasks)
}

func (b *batchStoreTaskBuilder) build() []*copTask {
	return b.tasks
}

func buildTiDBMemCopTasks(ranges *KeyRanges, req *kv.Request) ([]*copTask, error) {
	servers, err := infosync.GetAllServerInfo(context.Background())
	if err != nil {
		return nil, err
	}
	cmdType := tikvrpc.CmdCop
	tasks := make([]*copTask, 0, len(servers))
	for _, ser := range servers {
		if req.TiDBServerID > 0 && req.TiDBServerID != ser.ServerIDGetter() {
			continue
		}
		// skip some nodes, such as BR created when backup/restore
		if ser.IP == config.UnavailableIP {
			continue
		}

		addr := net.JoinHostPort(ser.IP, strconv.FormatUint(uint64(ser.StatusPort), 10))
		tasks = append(tasks, &copTask{
			ranges:       ranges,
			respChan:     make(chan *copResponse, 2),
			cmdType:      cmdType,
			storeType:    req.StoreType,
			storeAddr:    addr,
			RowCountHint: -1,
		})
	}
	return tasks, nil
}

func reverseTasks(tasks []*copTask) {
	for i := range len(tasks) / 2 {
		j := len(tasks) - i - 1
		tasks[i], tasks[j] = tasks[j], tasks[i]
	}
}

func isSmallTask(task *copTask) bool {
	// strictly, only RowCountHint == -1 stands for unknown task rows,
	// but when RowCountHint == 0, it may be caused by initialized value,
	// to avoid the future bugs, let the tasks with RowCountHint == 0 be non-small tasks.
	return task.RowCountHint > 0 &&
		(len(task.batchTaskList) == 0 && task.RowCountHint <= CopSmallTaskRow) ||
		(len(task.batchTaskList) > 0 && task.RowCountHint <= 2*CopSmallTaskRow)
}

// smallTaskConcurrency counts the small tasks of tasks,
// then returns the task count and extra concurrency for small tasks.
func smallTaskConcurrency(tasks []*copTask, numcpu int) (int, int) {
	res := 0
	for _, task := range tasks {
		if isSmallTask(task) {
			res++
		}
	}
	if res == 0 {
		return 0, 0
	}
	// Calculate the extra concurrency for small tasks
	// extra concurrency = tasks / (1 + sigma * sqrt(log(tasks ^ 2)))
	extraConc := int(float64(res) / (1 + smallTaskSigma*math.Sqrt(2*math.Log(float64(res)))))
	if numcpu <= 0 {
		numcpu = 1
	}
	smallTaskConcurrencyLimit := smallConcPerCore * numcpu
	if extraConc > smallTaskConcurrencyLimit {
		extraConc = smallTaskConcurrencyLimit
	}
	return res, extraConc
}

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

// SetLiteWorkerFallbackHookForTest installs a hook invoked when the lite worker falls back to the concurrent worker.
// Production code should not rely on this hook.
func SetLiteWorkerFallbackHookForTest(hook func()) {
	if hook != nil {
		liteWorkerFallbackHook.Store(&hook)
		return
	}
	liteWorkerFallbackHook.Store(nil)
}

func triggerLiteWorkerFallbackHook() {
	if hookPtr := liteWorkerFallbackHook.Load(); hookPtr != nil && *hookPtr != nil {
		(*hookPtr)()
	}
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
	// 16KB ballast helps grow the stack to the requirement of copIteratorWorker.
	// This reduces the `morestack` call during the execution of `handleTask`, thus improvement the efficiency of TiDB.
	// TODO: remove ballast after global pool is applied.
	ballast := make([]byte, 16*size.KB)
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}
		worker.handleTask(ctx, task, respCh)
		if worker.respChan != nil {
			// When a task is finished by the worker, send a finCopResp into channel to notify the copIterator that
			// there is a task finished.
			worker.sendToRespCh(finCopResp, worker.respChan)
		}
		if task.respChan != nil {
			close(task.respChan)
		}
		if worker.finished() {
			return
		}
	}
	runtime.KeepAlive(ballast)
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context, tryCopLiteWorker *atomic2.Uint32) {
	if len(it.tasks) == 1 && tryCopLiteWorker != nil && tryCopLiteWorker.CompareAndSwap(0, 1) {
		// For a query, only one `copIterator` can use `liteWorker`, otherwise it will affect the performance of multiple cop iterators executed concurrently,
		// see more detail in TestQueryWithConcurrentSmallCop.
		it.liteWorker = &liteCopIteratorWorker{
			ctx:              ctx,
			worker:           newCopIteratorWorker(it, nil),
			tryCopLiteWorker: tryCopLiteWorker,
		}
		return
	}
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency + it.smallTaskConcurrency)
	var smallTaskCh chan *copTask
	if it.smallTaskConcurrency > 0 {
		smallTaskCh = make(chan *copTask, 1)
	}
	// Start it.concurrency number of workers to handle cop requests.
	for i := range it.concurrency + it.smallTaskConcurrency {
		ch := taskCh
		if i >= it.concurrency && smallTaskCh != nil {
			ch = smallTaskCh
		}
		worker := newCopIteratorWorker(it, ch)
		go worker.run(ctx)
	}
	taskSender := &copIteratorTaskSender{
		taskCh:      taskCh,
		smallTaskCh: smallTaskCh,
		wg:          &it.wg,
		tasks:       it.tasks,
		finishCh:    it.finishCh,
		sendRate:    it.sendRate,
	}
	taskSender.respChan = it.respChan
	failpoint.Inject("ticase-4171", func(val failpoint.Value) {
		if val.(bool) {
			it.memTracker.Consume(10 * MockResponseSizeForTest)
			it.memTracker.Consume(10 * MockResponseSizeForTest)
		}
	})
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

func newCopIteratorWorker(it *copIterator, taskCh <-chan *copTask) *copIteratorWorker {
	return &copIteratorWorker{
		taskCh:                  taskCh,
		wg:                      &it.wg,
		store:                   it.store,
		req:                     it.req,
		respChan:                it.respChan,
		finishCh:                it.finishCh,
		vars:                    it.vars,
		kvclient:                txnsnapshot.NewClientHelper(it.store.store, &it.resolvedLocks, &it.committedLocks, false),
		memTracker:              it.memTracker,
		replicaReadSeed:         it.replicaReadSeed,
		pagingTaskIdx:           &it.pagingTaskIdx,
		storeBatchedNum:         &it.storeBatchedNum,
		storeBatchedFallbackNum: &it.storeBatchedFallbackNum,
		stats:                   it.stats,
	}
}

func (sender *copIteratorTaskSender) run(connID uint64, checker resourcegroup.RunawayChecker) {
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
		var sendTo chan<- *copTask
		if isSmallTask(t) && sender.smallTaskCh != nil {
			sendTo = sender.smallTaskCh
		} else {
			sendTo = sender.taskCh
		}
		exit = sender.sendToTaskCh(t, sendTo)
		if exit {
			break
		}
		if connID > 0 {
			failpoint.Inject("pauseCopIterTaskSender", func() {})
		}
	}
	close(sender.taskCh)
	if sender.smallTaskCh != nil {
		close(sender.smallTaskCh)
	}

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		close(sender.respChan)
	}
	if checker != nil {
		// runaway checker need to focus on the all processed keys of all tasks at a time.
		checker.ResetTotalProcessedKeys()
	}
}

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan *copResponse) (resp *copResponse, ok bool, exit bool) {
	for {
		select {
		case resp, ok = <-respCh:
			memTrackerConsumeResp(it.memTracker, resp)
			return
		case <-it.finishCh:
			exit = true
			return
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

func memTrackerConsumeResp(memTracker *memory.Tracker, resp *copResponse) {
	if memTracker != nil && resp != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		memTracker.Consume(-consumed)
	}
}

// GetConcurrency returns the concurrency and small task concurrency.
func (it *copIterator) GetConcurrency() (int, int) {
	return it.concurrency, it.smallTaskConcurrency
}

// GetStoreBatchInfo returns the batched and fallback num.
func (it *copIterator) GetStoreBatchInfo() (uint64, uint64) {
	return it.storeBatchedNum.Load(), it.storeBatchedFallbackNum.Load()
}

// GetBuildTaskElapsed returns the duration of building task.
func (it *copIterator) GetBuildTaskElapsed() time.Duration {
	return it.buildTaskElapsed
}

// GetSendRate returns the rate-limit object.
func (it *copIterator) GetSendRate() *util.RateLimit {
	return it.sendRate
}

// GetTasks returns the built tasks.
func (it *copIterator) GetTasks() []*copTask {
	return it.tasks
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask, sendTo chan<- *copTask) (exit bool) {
	select {
	case sendTo <- t:
	case <-sender.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp *copResponse, respCh chan<- *copResponse) (exit bool) {
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) checkRespOOM(resp *copResponse) {
	if worker.memTracker != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		failpoint.Inject("ConsumeRandomPanic", nil)
		worker.memTracker.Consume(consumed)
	}
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

	failpoint.InjectCall("CtxCancelBeforeReceive", ctx)
	if it.liteWorker != nil {
		resp = it.liteWorker.liteSendReq(ctx, it)
		// after lite handle 1 task, reset tryCopLiteWorker to 0 to make future request can reuse copLiteWorker.
		it.liteWorker.tryCopLiteWorker.CompareAndSwap(1, 0)
		if resp == nil {
			it.actionOnExceed.close()
			return nil, nil
		}
		if len(it.tasks) > 0 && len(it.liteWorker.batchCopRespList) == 0 && resp.err == nil {
			// if there are remain tasks to be processed, we need to run worker concurrently to avoid blocking.
			// see more detail in https://github.com/pingcap/tidb/issues/58658 and TestDMLWithLiteCopWorker.
			it.liteWorker.runWorkerConcurrently(it)
			it.liteWorker = nil
		}
		it.actionOnExceed.destroyTokenIfNeeded(func() {})
		memTrackerConsumeResp(it.memTracker, resp)
	} else if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			it.actionOnExceed.close()
			return nil, errors.Trace(ctx.Err())
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
				// Close() is called or context cancelled/timeout, so Next() is invalid.
				return nil, errors.Trace(ctx.Err())
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

func (w *liteCopIteratorWorker) liteSendReq(ctx context.Context, it *copIterator) (resp *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Warn("copIteratorWork meet panic",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			resp = &copResponse{err: util2.GetRecoverError(r)}
		}
	}()

	worker := w.worker
	if len(w.batchCopRespList) > 0 {
		resp = w.batchCopRespList[0]
		w.batchCopRespList = w.batchCopRespList[1:]
		return resp
	}
	backoffermap := make(map[uint64]*Backoffer)
	cancelFuncs := make([]context.CancelFunc, 0)
	defer func() {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}()
	for len(it.tasks) > 0 {
		curTask := it.tasks[0]
		bo, cancel := chooseBackoffer(w.ctx, backoffermap, curTask, worker)
		if cancel != nil {
			cancelFuncs = append(cancelFuncs, cancel)
		}
		result, err := worker.handleTaskOnce(bo, curTask)
		if err != nil {
			resp = &copResponse{err: errors.Trace(err)}
			worker.checkRespOOM(resp)
			return resp
		}

		if result != nil && len(result.remains) > 0 {
			it.tasks = append(result.remains, it.tasks[1:]...)
		} else {
			it.tasks = it.tasks[1:]
		}
		if result != nil {
			if result.resp != nil {
				w.batchCopRespList = result.batchRespList
				return result.resp
			}
			if len(result.batchRespList) > 0 {
				resp = result.batchRespList[0]
				w.batchCopRespList = result.batchRespList[1:]
				return resp
			}
		}
	}
	return nil
}

func (w *liteCopIteratorWorker) runWorkerConcurrently(it *copIterator) {
	triggerLiteWorkerFallbackHook()
	taskCh := make(chan *copTask, 1)
	worker := w.worker
	worker.taskCh = taskCh
	it.wg.Add(1)
	go worker.run(w.ctx)

	if it.respChan == nil {
		// If it.respChan is nil, we will read the response from task.respChan,
		// but task.respChan maybe nil when rebuilding cop task, so we need to create respChan for the task.
		for i := range it.tasks {
			if it.tasks[i].respChan == nil {
				it.tasks[i].respChan = make(chan *copResponse, 2)
			}
		}
	}

	taskSender := &copIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
		sendRate: it.sendRate,
		respChan: it.respChan,
	}
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

// HasUnconsumedCopRuntimeStats indicate whether has unconsumed CopRuntimeStats.
type HasUnconsumedCopRuntimeStats interface {
	// CollectUnconsumedCopRuntimeStats returns unconsumed CopRuntimeStats.
	CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats
}

func (it *copIterator) CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats {
	if it == nil || it.stats == nil {
		return nil
	}
	it.stats.Lock()
	stats := make([]*CopRuntimeStats, 0, len(it.stats.stats))
	stats = append(stats, it.stats.stats...)
	it.stats.Unlock()
	return stats
}

// Associate each region with an independent backoffer. In this way, when multiple regions are
// unavailable, TiDB can execute very quickly without blocking, if the returned CancelFunc is not nil,
// the caller must call it to avoid context leak.
func chooseBackoffer(ctx context.Context, backoffermap map[uint64]*Backoffer, task *copTask, worker *copIteratorWorker) (*Backoffer, context.CancelFunc) {
	bo, ok := backoffermap[task.region.GetID()]
	if ok {
		return bo, nil
	}
	boMaxSleep := CopNextMaxBackoff
	failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
		if value.(bool) {
			boMaxSleep = 2
		}
	})
	var cancel context.CancelFunc
	boCtx := ctx
	if worker.req.MaxExecutionTime > 0 {
		boCtx, cancel = context.WithTimeout(boCtx, time.Duration(worker.req.MaxExecutionTime)*time.Millisecond)
	}
	newbo := backoff.NewBackofferWithVars(boCtx, boMaxSleep, worker.vars)
	backoffermap[task.region.GetID()] = newbo
	return newbo, cancel
}



// CopRuntimeStats contains execution detail information.
type CopRuntimeStats struct {
	execdetails.CopExecDetails
	ReqStats *tikv.RegionRequestRuntimeStats

	CoprCacheHit bool
}

type copIteratorRuntimeStats struct {
	sync.Mutex
	stats []*CopRuntimeStats
}

func (worker *copIteratorWorker) handleTiDBSendReqErr(err error, task *copTask) (*copTaskResult, error) {
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
		return nil, errors.Trace(err)
	}
	resp := &copResponse{
		pbResp: &coprocessor.Response{
			Data: data,
		},
		detail: &CopRuntimeStats{},
	}
	worker.checkRespOOM(resp)
	return &copTaskResult{resp: resp}, nil
}

// calculateRetry splits the input ranges into two, and take one of them according to desc flag.
// It's used in paging API, to calculate which range is consumed and what needs to be retry.
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

// calculateRemain calculates the remain ranges to be processed, it's used in paging API.
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
	if worker.vars != nil && worker.vars.Killed != nil {
		killed := atomic.LoadUint32(worker.vars.Killed)
		if killed != 0 {
			logutil.BgLogger().Info(
				"a killed signal is received in copIteratorWorker",
				zap.Uint32("signal", killed),
			)
			return true
		}
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

// BuildKeyRanges is used for test, quickly build key ranges from paired keys.
func BuildKeyRanges(keys ...string) []kv.KeyRange {
	var ranges []kv.KeyRange
	for i := 0; i < len(keys); i += 2 {
		ranges = append(ranges, kv.KeyRange{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
		})
	}
	return ranges
}

func optRowHint(req *kv.Request) bool {
	opt := true
	if req.StoreType == kv.TiDB {
		return false
	}
	if req.RequestSource.RequestSourceInternal || req.Tp != kv.ReqTypeDAG {
		// disable extra concurrency for internal tasks.
		return false
	}
	failpoint.Inject("disableFixedRowCountHint", func(_ failpoint.Value) {
		opt = false
	})
	return opt
}

func checkStoreBatchCopr(req *kv.Request) bool {
	if req.Tp != kv.ReqTypeDAG || req.StoreType != kv.TiKV {
		return false
	}
	// TODO: support keep-order batch
	if req.ReplicaRead != kv.ReplicaReadLeader || req.KeepOrder {
		// Disable batch copr for follower read
		return false
	}
	// Disable batch copr when paging is enabled.
	if req.Paging.Enable {
		return false
	}
	// Disable it for internal requests to avoid regression.
	if req.RequestSource.RequestSourceInternal {
		return false
	}
	return true
}
