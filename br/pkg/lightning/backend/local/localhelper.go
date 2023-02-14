// Copyright 2020 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"database/sql"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	retrySplitMaxWaitTime = 4 * time.Second
)

var (
	// the max keys count in a batch to split one region
	maxBatchSplitKeys = 4096
	// the max total key size in a split region batch.
	// our threshold should be smaller than TiKV's raft max entry size(default is 8MB).
	maxBatchSplitSize = 6 * units.MiB
	// the base exponential backoff time
	// the variable is only changed in unit test for running test faster.
	splitRegionBaseBackOffTime = time.Second
	// the max retry times to split regions.
	splitRetryTimes = 8
)

// SplitAndScatterRegionInBatches splits&scatter regions in batches.
// Too many split&scatter requests may put a lot of pressure on TiKV and PD.
func (local *local) SplitAndScatterRegionInBatches(
	ctx context.Context,
	ranges []Range,
	tableInfo *checkpoints.TidbTableInfo,
	needSplit bool,
	regionSplitSize int64,
	batchCnt int,
) error {
	for i := 0; i < len(ranges); i += batchCnt {
		batch := ranges[i:]
		if len(batch) > batchCnt {
			batch = batch[:batchCnt]
		}
		if err := local.SplitAndScatterRegionByRanges(ctx, batch, tableInfo, needSplit, regionSplitSize); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SplitAndScatterRegionByRanges include region split & scatter operation just like br.
// we can simply call br function, but we need to change some function signature of br
// When the ranges total size is small, we can skip the split to avoid generate empty regions.
// TODO: remove this file and use br internal functions
func (local *local) SplitAndScatterRegionByRanges(
	ctx context.Context,
	ranges []Range,
	tableInfo *checkpoints.TidbTableInfo,
	needSplit bool,
	regionSplitSize int64,
) error {
	if len(ranges) == 0 {
		return nil
	}

	db, err := local.g.GetDB()
	if err != nil {
		return errors.Trace(err)
	}

	minKey := codec.EncodeBytes([]byte{}, ranges[0].start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].end)

	scatterRegions := make([]*split.RegionInfo, 0)
	var retryKeys [][]byte
	waitTime := splitRegionBaseBackOffTime
	skippedKeys := 0
	for i := 0; i < splitRetryTimes; i++ {
		log.FromContext(ctx).Info("split and scatter region",
			logutil.Key("minKey", minKey),
			logutil.Key("maxKey", maxKey),
			zap.Int("retry", i),
		)
		err = nil
		if i > 0 {
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			waitTime *= 2
			if waitTime > retrySplitMaxWaitTime {
				waitTime = retrySplitMaxWaitTime
			}
		}
		var regions []*split.RegionInfo
		regions, err = split.PaginateScanRegion(ctx, local.splitCli, minKey, maxKey, 128)
		log.FromContext(ctx).Info("paginate scan regions", zap.Int("count", len(regions)),
			logutil.Key("start", minKey), logutil.Key("end", maxKey))
		if err != nil {
			log.FromContext(ctx).Warn("paginate scan region failed", logutil.Key("minKey", minKey), logutil.Key("maxKey", maxKey),
				log.ShortError(err), zap.Int("retry", i))
			continue
		}

		log.FromContext(ctx).Info("paginate scan region finished", logutil.Key("minKey", minKey), logutil.Key("maxKey", maxKey),
			zap.Int("regions", len(regions)))

		if !needSplit {
			scatterRegions = append(scatterRegions, regions...)
			break
		}

		needSplitRanges := make([]Range, 0, len(ranges))
		startKey := make([]byte, 0)
		endKey := make([]byte, 0)
		for _, r := range ranges {
			startKey = codec.EncodeBytes(startKey, r.start)
			endKey = codec.EncodeBytes(endKey, r.end)
			idx := sort.Search(len(regions), func(i int) bool {
				return beforeEnd(startKey, regions[i].Region.EndKey)
			})
			if idx < 0 || idx >= len(regions) {
				log.FromContext(ctx).Error("target region not found", logutil.Key("start_key", startKey),
					logutil.RegionBy("first_region", regions[0].Region),
					logutil.RegionBy("last_region", regions[len(regions)-1].Region))
				return errors.New("target region not found")
			}
			if bytes.Compare(startKey, regions[idx].Region.StartKey) > 0 || bytes.Compare(endKey, regions[idx].Region.EndKey) < 0 {
				needSplitRanges = append(needSplitRanges, r)
			}
		}
		ranges = needSplitRanges
		if len(ranges) == 0 {
			log.FromContext(ctx).Info("no ranges need to be split, skipped.")
			return nil
		}

		var tableRegionStats map[uint64]int64
		if tableInfo != nil {
			tableRegionStats, err = fetchTableRegionSizeStats(ctx, db, tableInfo.ID)
			if err != nil {
				log.FromContext(ctx).Warn("fetch table region size statistics failed",
					zap.String("table", tableInfo.Name), zap.Error(err))
				tableRegionStats, err = make(map[uint64]int64), nil
			}
		}

		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}

		var splitKeyMap map[uint64][][]byte
		if len(retryKeys) > 0 {
			firstKeyEnc := codec.EncodeBytes([]byte{}, retryKeys[0])
			lastKeyEnc := codec.EncodeBytes([]byte{}, retryKeys[len(retryKeys)-1])
			if bytes.Compare(firstKeyEnc, regions[0].Region.StartKey) < 0 || !beforeEnd(lastKeyEnc, regions[len(regions)-1].Region.EndKey) {
				log.FromContext(ctx).Warn("no valid key for split region",
					logutil.Key("firstKey", firstKeyEnc), logutil.Key("lastKey", lastKeyEnc),
					logutil.Key("firstRegionStart", regions[0].Region.StartKey),
					logutil.Key("lastRegionEnd", regions[len(regions)-1].Region.EndKey))
				return errors.New("check split keys failed")
			}
			splitKeyMap = getSplitKeys(retryKeys, regions, log.FromContext(ctx))
			retryKeys = retryKeys[:0]
		} else {
			splitKeyMap = getSplitKeysByRanges(ranges, regions, log.FromContext(ctx))
		}

		type splitInfo struct {
			region *split.RegionInfo
			keys   [][]byte
		}

		var syncLock sync.Mutex
		// TODO, make this size configurable
		size := mathutil.Min(len(splitKeyMap), runtime.GOMAXPROCS(0))
		ch := make(chan *splitInfo, size)
		eg, splitCtx := errgroup.WithContext(ctx)

		for splitWorker := 0; splitWorker < size; splitWorker++ {
			eg.Go(func() error {
				for sp := range ch {
					var newRegions []*split.RegionInfo
					var err1 error
					region := sp.region
					keys := sp.keys
					slices.SortFunc(keys, func(i, j []byte) bool {
						return bytes.Compare(i, j) < 0
					})
					splitRegion := region
					startIdx := 0
					endIdx := 0
					batchKeySize := 0
					for endIdx <= len(keys) {
						if endIdx == len(keys) || batchKeySize+len(keys[endIdx]) > maxBatchSplitSize || endIdx-startIdx >= maxBatchSplitKeys {
							splitRegionStart := codec.EncodeBytes([]byte{}, keys[startIdx])
							splitRegionEnd := codec.EncodeBytes([]byte{}, keys[endIdx-1])
							if bytes.Compare(splitRegionStart, splitRegion.Region.StartKey) < 0 || !beforeEnd(splitRegionEnd, splitRegion.Region.EndKey) {
								log.FromContext(ctx).Fatal("no valid key in region",
									logutil.Key("startKey", splitRegionStart), logutil.Key("endKey", splitRegionEnd),
									logutil.Key("regionStart", splitRegion.Region.StartKey), logutil.Key("regionEnd", splitRegion.Region.EndKey),
									logutil.Region(splitRegion.Region), logutil.Leader(splitRegion.Leader))
							}
							splitRegion, newRegions, err1 = local.BatchSplitRegions(splitCtx, splitRegion, keys[startIdx:endIdx])
							if err1 != nil {
								if strings.Contains(err1.Error(), "no valid key") {
									for _, key := range keys {
										log.FromContext(ctx).Warn("no valid key",
											logutil.Key("startKey", region.Region.StartKey),
											logutil.Key("endKey", region.Region.EndKey),
											logutil.Key("key", codec.EncodeBytes([]byte{}, key)))
									}
									return err1
								} else if common.IsContextCanceledError(err1) {
									// do not retry on context.Canceled error
									return err1
								}
								log.FromContext(ctx).Warn("split regions", log.ShortError(err1), zap.Int("retry time", i),
									zap.Uint64("region_id", region.Region.Id))

								syncLock.Lock()
								retryKeys = append(retryKeys, keys[startIdx:]...)
								// set global error so if we exceed retry limit, the function will return this error
								err = multierr.Append(err, err1)
								syncLock.Unlock()
								break
							}
							log.FromContext(ctx).Info("batch split region", zap.Uint64("region_id", splitRegion.Region.Id),
								zap.Int("keys", endIdx-startIdx), zap.Binary("firstKey", keys[startIdx]),
								zap.Binary("end", keys[endIdx-1]))
							slices.SortFunc(newRegions, func(i, j *split.RegionInfo) bool {
								return bytes.Compare(i.Region.StartKey, j.Region.StartKey) < 0
							})
							syncLock.Lock()
							scatterRegions = append(scatterRegions, newRegions...)
							syncLock.Unlock()
							// the region with the max start key is the region need to be further split.
							if bytes.Compare(splitRegion.Region.StartKey, newRegions[len(newRegions)-1].Region.StartKey) < 0 {
								splitRegion = newRegions[len(newRegions)-1]
							}

							batchKeySize = 0
							startIdx = endIdx
						}
						if endIdx < len(keys) {
							batchKeySize += len(keys[endIdx])
						}
						endIdx++
					}
				}
				return nil
			})
		}
	sendLoop:
		for regionID, keys := range splitKeyMap {
			// if region not in tableRegionStats, that means this region is newly split, so
			// we can skip split it again.
			regionSize, ok := tableRegionStats[regionID]
			if !ok {
				log.FromContext(ctx).Warn("region stats not found", zap.Uint64("region", regionID))
			}
			if len(keys) == 1 && regionSize < regionSplitSize {
				skippedKeys++
			}
			select {
			case ch <- &splitInfo{region: regionMap[regionID], keys: keys}:
			case <-ctx.Done():
				// outer context is canceled, can directly return
				close(ch)
				return ctx.Err()
			case <-splitCtx.Done():
				// met critical error, stop process
				break sendLoop
			}
		}
		close(ch)
		if splitError := eg.Wait(); splitError != nil {
			retryKeys = retryKeys[:0]
			err = splitError
			continue
		}

		if len(retryKeys) == 0 {
			break
		}
		slices.SortFunc(retryKeys, func(i, j []byte) bool {
			return bytes.Compare(i, j) < 0
		})
		minKey = codec.EncodeBytes([]byte{}, retryKeys[0])
		maxKey = codec.EncodeBytes([]byte{}, nextKey(retryKeys[len(retryKeys)-1]))
	}
	if err != nil {
		return errors.Trace(err)
	}

	startTime := time.Now()
	scatterCount, err := local.waitForScatterRegions(ctx, scatterRegions)
	if scatterCount == len(scatterRegions) {
		log.FromContext(ctx).Info("waiting for scattering regions done",
			zap.Int("skipped_keys", skippedKeys),
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.FromContext(ctx).Info("waiting for scattering regions timeout",
			zap.Int("skipped_keys", skippedKeys),
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)),
			zap.Error(err))
	}
	return nil
}

func fetchTableRegionSizeStats(ctx context.Context, db *sql.DB, tableID int64) (map[uint64]int64, error) {
	if db == nil {
		return nil, errors.Errorf("db is nil")
	}
	exec := &common.SQLWithRetry{
		DB:     db,
		Logger: log.FromContext(ctx),
	}

	stats := make(map[uint64]int64)
	err := exec.Transact(ctx, "fetch region approximate sizes", func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, "SELECT REGION_ID, APPROXIMATE_SIZE FROM information_schema.TIKV_REGION_STATUS WHERE TABLE_ID = ?", tableID)
		if err != nil {
			return errors.Trace(err)
		}
		//nolint: errcheck
		defer rows.Close()
		var (
			regionID uint64
			size     int64
		)
		for rows.Next() {
			if err = rows.Scan(&regionID, &size); err != nil {
				return errors.Trace(err)
			}
			stats[regionID] = size * units.MiB
		}
		return rows.Err()
	})
	return stats, errors.Trace(err)
}

func (local *local) BatchSplitRegions(
	ctx context.Context,
	region *split.RegionInfo,
	keys [][]byte,
) (*split.RegionInfo, []*split.RegionInfo, error) {
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		failpoint.Return(nil, nil, errors.New("retryable error"))
	})
	region, newRegions, err := local.splitCli.BatchSplitRegionsWithOrigin(ctx, region, keys)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "batch split regions failed")
	}
	var failedErr error
	retryRegions := make([]*split.RegionInfo, 0)
	scatterRegions := newRegions
	waitTime := splitRegionBaseBackOffTime
	for i := 0; i < maxRetryTimes; i++ {
		for _, region := range scatterRegions {
			// Wait for a while until the regions successfully splits.
			local.waitForSplit(ctx, region.Region.Id)
			if err = local.splitCli.ScatterRegion(ctx, region); err != nil {
				failedErr = err
				retryRegions = append(retryRegions, region)
			}
		}
		if len(retryRegions) == 0 {
			break
		}
		// the scatter operation likely fails because region replicate not finish yet
		// pack them to one log to avoid printing a lot warn logs.
		log.FromContext(ctx).Warn("scatter region failed", zap.Int("regionCount", len(newRegions)),
			zap.Int("failedCount", len(retryRegions)), zap.Error(failedErr), zap.Int("retry", i))
		scatterRegions = retryRegions
		retryRegions = make([]*split.RegionInfo, 0)
		select {
		case <-time.After(waitTime):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		waitTime *= 2
	}

	return region, newRegions, nil
}

func (local *local) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := local.splitCli.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func (local *local) waitForSplit(ctx context.Context, regionID uint64) {
	for i := 0; i < split.SplitCheckMaxRetryTimes; i++ {
		ok, err := local.hasRegion(ctx, regionID)
		if err != nil {
			log.FromContext(ctx).Info("wait for split failed", log.ShortError(err))
			return
		}
		if ok {
			break
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func (local *local) waitForScatterRegions(ctx context.Context, regions []*split.RegionInfo) (scatterCount int, _ error) {
	subCtx, cancel := context.WithTimeout(ctx, split.ScatterWaitUpperInterval)
	defer cancel()

	for len(regions) > 0 {
		var retryRegions []*split.RegionInfo
		for _, region := range regions {
			scattered, err := local.checkRegionScatteredOrReScatter(subCtx, region)
			if scattered {
				scatterCount++
				continue
			}
			if err != nil {
				if !common.IsRetryableError(err) {
					log.FromContext(ctx).Warn("wait for scatter region encountered non-retryable error", logutil.Region(region.Region), zap.Error(err))
					return scatterCount, err
				}
				log.FromContext(ctx).Warn("wait for scatter region encountered error, will retry again", logutil.Region(region.Region), zap.Error(err))
			}
			retryRegions = append(retryRegions, region)
		}
		regions = retryRegions
		select {
		case <-time.After(time.Second):
		case <-subCtx.Done():
			return
		}
	}
	return scatterCount, nil
}

func (local *local) checkRegionScatteredOrReScatter(ctx context.Context, regionInfo *split.RegionInfo) (bool, error) {
	resp, err := local.splitCli.GetOperator(ctx, regionInfo.Region.GetId())
	if err != nil {
		return false, err
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Errorf(
			"failed to get region operator, error type: %s, error message: %s",
			respErr.GetType().String(), respErr.GetMessage())
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished.
	if string(resp.GetDesc()) != "scatter-region" {
		return true, nil
	}
	switch resp.GetStatus() {
	case pdpb.OperatorStatus_RUNNING:
		return false, nil
	case pdpb.OperatorStatus_SUCCESS:
		return true, nil
	default:
		log.FromContext(ctx).Debug("scatter-region operator status is abnormal, will scatter region again",
			logutil.Region(regionInfo.Region), zap.Stringer("status", resp.GetStatus()))
		return false, local.splitCli.ScatterRegion(ctx, regionInfo)
	}
}

func getSplitKeysByRanges(ranges []Range, regions []*split.RegionInfo, logger log.Logger) map[uint64][][]byte {
	checkKeys := make([][]byte, 0)
	var lastEnd []byte
	for _, rg := range ranges {
		if !bytes.Equal(lastEnd, rg.start) {
			checkKeys = append(checkKeys, rg.start)
		}
		checkKeys = append(checkKeys, rg.end)
		lastEnd = rg.end
	}
	return getSplitKeys(checkKeys, regions, logger)
}

func getSplitKeys(checkKeys [][]byte, regions []*split.RegionInfo, logger log.Logger) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	for _, key := range checkKeys {
		if region := needSplit(key, regions, logger); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			logger.Debug("get key for split region",
				zap.Binary("key", key),
				zap.Binary("startKey", region.Region.StartKey),
				zap.Binary("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// needSplit checks whether a key is necessary to split, if true returns the split region
func needSplit(key []byte, regions []*split.RegionInfo, logger log.Logger) *split.RegionInfo {
	// If splitKey is the max key.
	if len(key) == 0 {
		return nil
	}
	splitKey := codec.EncodeBytes([]byte{}, key)

	idx := sort.Search(len(regions), func(i int) bool {
		return beforeEnd(splitKey, regions[i].Region.EndKey)
	})
	if idx < len(regions) {
		// If splitKey is in a region
		if bytes.Compare(splitKey, regions[idx].Region.GetStartKey()) > 0 && beforeEnd(splitKey, regions[idx].Region.GetEndKey()) {
			logger.Debug("need split",
				zap.Binary("splitKey", key),
				zap.Binary("encodedKey", splitKey),
				zap.Binary("region start", regions[idx].Region.GetStartKey()),
				zap.Binary("region end", regions[idx].Region.GetEndKey()),
			)
			return regions[idx]
		}
	}
	return nil
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func insideRegion(region *metapb.Region, metas []*sst.SSTMeta) bool {
	inside := true
	for _, meta := range metas {
		rg := meta.GetRange()
		inside = inside && (keyInsideRegion(region, rg.GetStart()) && keyInsideRegion(region, rg.GetEnd()))
	}
	return inside
}

func keyInsideRegion(region *metapb.Region, key []byte) bool {
	return bytes.Compare(key, region.GetStartKey()) >= 0 && (beforeEnd(key, region.GetEndKey()))
}

func intersectRange(region *metapb.Region, rg Range) Range {
	var startKey, endKey []byte
	if len(region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.StartKey, []byte{})
	}
	if bytes.Compare(startKey, rg.start) < 0 {
		startKey = rg.start
	}
	if len(region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.EndKey, []byte{})
	}
	if beforeEnd(rg.end, endKey) {
		endKey = rg.end
	}

	return Range{start: startKey, end: endKey}
}

type StoreWriteLimiter interface {
	WaitN(ctx context.Context, storeID uint64, n int) error
	Limit() int
}

type storeWriteLimiter struct {
	rwm      sync.RWMutex
	limiters map[uint64]*rate.Limiter
	limit    int
	burst    int
}

func newStoreWriteLimiter(limit int) *storeWriteLimiter {
	var burst int
	// Allow burst of at most 20% of the limit.
	if limit <= math.MaxInt-limit/5 {
		burst = limit + limit/5
	} else {
		// If overflowed, set burst to math.MaxInt.
		burst = math.MaxInt
	}
	return &storeWriteLimiter{
		limiters: make(map[uint64]*rate.Limiter),
		limit:    limit,
		burst:    burst,
	}
}

func (s *storeWriteLimiter) WaitN(ctx context.Context, storeID uint64, n int) error {
	limiter := s.getLimiter(storeID)
	// The original WaitN doesn't allow n > burst,
	// so we call WaitN with burst multiple times.
	for n > limiter.Burst() {
		if err := limiter.WaitN(ctx, limiter.Burst()); err != nil {
			return err
		}
		n -= limiter.Burst()
	}
	return limiter.WaitN(ctx, n)
}

func (s *storeWriteLimiter) Limit() int {
	return s.limit
}

func (s *storeWriteLimiter) getLimiter(storeID uint64) *rate.Limiter {
	s.rwm.RLock()
	limiter, ok := s.limiters[storeID]
	s.rwm.RUnlock()
	if ok {
		return limiter
	}
	s.rwm.Lock()
	defer s.rwm.Unlock()
	limiter, ok = s.limiters[storeID]
	if !ok {
		limiter = rate.NewLimiter(rate.Limit(s.limit), s.burst)
		s.limiters[storeID] = limiter
	}
	return limiter
}

type noopStoreWriteLimiter struct{}

func (noopStoreWriteLimiter) WaitN(ctx context.Context, storeID uint64, n int) error {
	return nil
}

func (noopStoreWriteLimiter) Limit() int {
	return math.MaxInt
}
