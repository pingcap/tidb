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
	"slices"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tidb/pkg/util/trxevents"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

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
