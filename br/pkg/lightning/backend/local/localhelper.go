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
	"slices"
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
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	retrySplitMaxWaitTime = 4 * time.Second
)

var (
	// the max total key size in a split region batch.
	// our threshold should be smaller than TiKV's raft max entry size(default is 8MB).
	maxBatchSplitSize = 6 * units.MiB
	// the base exponential backoff time
	// the variable is only changed in unit test for running test faster.
	splitRegionBaseBackOffTime = time.Second
	// the max retry times to split regions.
	splitRetryTimes = 8
)

// TableRegionSizeGetter get table region size.
type TableRegionSizeGetter interface {
	GetTableRegionSize(ctx context.Context, tableID int64) (map[uint64]int64, error)
}

// TableRegionSizeGetterImpl implements TableRegionSizeGetter.
type TableRegionSizeGetterImpl struct {
	DB *sql.DB
}

var _ TableRegionSizeGetter = &TableRegionSizeGetterImpl{}

// GetTableRegionSize implements TableRegionSizeGetter.
func (g *TableRegionSizeGetterImpl) GetTableRegionSize(ctx context.Context, tableID int64) (map[uint64]int64, error) {
	if g.DB == nil {
		return nil, errors.Errorf("db is nil")
	}
	exec := &common.SQLWithRetry{
		DB:     g.DB,
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

// SplitAndScatterRegionInBatches splits&scatter regions in batches.
// Too many split&scatter requests may put a lot of pressure on TiKV and PD.
func (local *Backend) SplitAndScatterRegionInBatches(
	ctx context.Context,
	ranges []Range,
	needSplit bool,
	batchCnt int,
) error {
	for i := 0; i < len(ranges); i += batchCnt {
		batch := ranges[i:]
		if len(batch) > batchCnt {
			batch = batch[:batchCnt]
		}
		if err := local.SplitAndScatterRegionByRanges(ctx, batch, needSplit); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SplitAndScatterRegionByRanges include region split & scatter operation just like br.
// we can simply call br function, but we need to change some function signature of br
// When the ranges total size is small, we can skip the split to avoid generate empty regions.
// TODO: remove this file and use br internal functions
func (local *Backend) SplitAndScatterRegionByRanges(
	ctx context.Context,
	ranges []Range,
	needSplit bool,
) (err error) {
	if len(ranges) == 0 {
		return nil
	}

	if m, ok := metric.FromContext(ctx); ok {
		begin := time.Now()
		defer func() {
			if err == nil {
				m.SSTSecondsHistogram.WithLabelValues(metric.SSTProcessSplit).Observe(time.Since(begin).Seconds())
			}
		}()
	}

	minKey := codec.EncodeBytes([]byte{}, ranges[0].start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].end)

	scatterRegions := make([]*split.RegionInfo, 0)
	var retryKeys [][]byte
	waitTime := splitRegionBaseBackOffTime
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
		size := mathutil.Min(len(splitKeyMap), local.RegionSplitConcurrency)
		ch := make(chan *splitInfo, size)
		eg, splitCtx := errgroup.WithContext(ctx)

		for splitWorker := 0; splitWorker < size; splitWorker++ {
			eg.Go(func() error {
				for sp := range ch {
					var newRegions []*split.RegionInfo
					var err1 error
					region := sp.region
					keys := sp.keys
					slices.SortFunc(keys, bytes.Compare)
					splitRegion := region
					startIdx := 0
					endIdx := 0
					batchKeySize := 0
					for endIdx <= len(keys) {
						if endIdx == len(keys) ||
							batchKeySize+len(keys[endIdx]) > maxBatchSplitSize ||
							endIdx-startIdx >= local.RegionSplitBatchSize {
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
							slices.SortFunc(newRegions, func(i, j *split.RegionInfo) int {
								return bytes.Compare(i.Region.StartKey, j.Region.StartKey)
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
		slices.SortFunc(retryKeys, bytes.Compare)
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
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.FromContext(ctx).Info("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)),
			zap.Error(err))
	}
	return nil
}

// BatchSplitRegions will split regions by the given split keys and tries to
// scatter new regions. If split/scatter fails because new region is not ready,
// this function will not return error.
func (local *Backend) BatchSplitRegions(
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
	scatterRegions := newRegions
	backoffer := split.NewWaitRegionOnlineBackoffer().(*split.WaitRegionOnlineBackoffer)
	_ = utils.WithRetry(ctx, func() error {
		retryRegions := make([]*split.RegionInfo, 0)
		for _, region := range scatterRegions {
			// Wait for a while until the regions successfully splits.
			ok, err2 := local.hasRegion(ctx, region.Region.Id)
			if !ok || err2 != nil {
				failedErr = err2
				if failedErr == nil {
					failedErr = errors.Errorf("region %d not found", region.Region.Id)
				}
				retryRegions = append(retryRegions, region)
				continue
			}
			if err = local.splitCli.ScatterRegion(ctx, region); err != nil {
				failedErr = err
				retryRegions = append(retryRegions, region)
			}
		}
		if len(retryRegions) == 0 {
			return nil
		}
		// if the number of becomes smaller, we can infer TiKV side really
		// made some progress so don't increase the retry times.
		if len(retryRegions) < len(scatterRegions) {
			backoffer.Stat.ReduceRetry()
		}
		// the scatter operation likely fails because region replicate not finish yet
		// pack them to one log to avoid printing a lot warn logs.
		log.FromContext(ctx).Warn("scatter region failed", zap.Int("regionCount", len(newRegions)),
			zap.Int("failedCount", len(retryRegions)), zap.Error(failedErr))
		scatterRegions = retryRegions
		// although it's not PDBatchScanRegion, WaitRegionOnlineBackoffer will only
		// check this error class so we simply reuse it. Will refine WaitRegionOnlineBackoffer
		// later
		failedErr = errors.Annotatef(berrors.ErrPDBatchScanRegion, "scatter region failed")
		return failedErr
	}, backoffer)

	// TODO: there's still change that we may skip scatter if the retry is timeout.
	return region, newRegions, ctx.Err()
}

func (local *Backend) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := local.splitCli.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func (local *Backend) waitForScatterRegions(ctx context.Context, regions []*split.RegionInfo) (scatterCount int, _ error) {
	var (
		retErr    error
		backoffer = split.NewWaitRegionOnlineBackoffer().(*split.WaitRegionOnlineBackoffer)
	)
	// WithRetry will return multierr which is hard to use, so we use `retErr`
	// to save the error needed to return.
	_ = utils.WithRetry(ctx, func() error {
		var retryRegions []*split.RegionInfo
		for _, region := range regions {
			scattered, err := local.checkRegionScatteredOrReScatter(ctx, region)
			if scattered {
				scatterCount++
				continue
			}
			if err != nil {
				if !common.IsRetryableError(err) {
					log.FromContext(ctx).Warn("wait for scatter region encountered non-retryable error", logutil.Region(region.Region), zap.Error(err))
					retErr = err
					// return nil to stop retry, the error is saved in `retErr`
					return nil
				}
				log.FromContext(ctx).Warn("wait for scatter region encountered error, will retry again", logutil.Region(region.Region), zap.Error(err))
			}
			retryRegions = append(retryRegions, region)
		}
		if len(retryRegions) == 0 {
			regions = retryRegions
			return nil
		}
		if len(retryRegions) < len(regions) {
			backoffer.Stat.ReduceRetry()
		}

		regions = retryRegions
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "wait for scatter region failed")
	}, backoffer)

	if len(regions) > 0 && retErr == nil {
		retErr = errors.Errorf("wait for scatter region timeout, print the first unfinished region %v",
			regions[0].Region.String())
	}
	return scatterCount, retErr
}

func (local *Backend) checkRegionScatteredOrReScatter(ctx context.Context, regionInfo *split.RegionInfo) (bool, error) {
	resp, err := local.splitCli.GetOperator(ctx, regionInfo.Region.GetId())
	if err != nil {
		return false, err
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		// TODO: why this is OK?
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

// StoreWriteLimiter is used to limit the write rate of a store.
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

func (noopStoreWriteLimiter) WaitN(_ context.Context, _ uint64, _ int) error {
	return nil
}

func (noopStoreWriteLimiter) Limit() int {
	return math.MaxInt
}

// compaction threshold
const (
	CompactionLowerThreshold = 512 * units.MiB
	CompactionUpperThreshold = 32 * units.GiB
)

// EstimateCompactionThreshold estimate SST files compression threshold by total row file size
// with a higher compression threshold, the compression time increases, but the iteration time decreases.
// Try to limit the total SST files number under 500. But size compress 32GB SST files cost about 20min,
// we set the upper bound to 32GB to avoid too long compression time.
// factor is the non-clustered(1 for data engine and number of non-clustered index count for index engine).
func EstimateCompactionThreshold(files []mydump.FileInfo, cp *checkpoints.TableCheckpoint, factor int64) int64 {
	totalRawFileSize := int64(0)
	var lastFile string
	fileSizeMap := make(map[string]int64, len(files))
	for _, file := range files {
		fileSizeMap[file.FileMeta.Path] = file.FileMeta.RealSize
	}

	for _, engineCp := range cp.Engines {
		for _, chunk := range engineCp.Chunks {
			if chunk.FileMeta.Path == lastFile {
				continue
			}
			size, ok := fileSizeMap[chunk.FileMeta.Path]
			if !ok {
				size = chunk.FileMeta.FileSize
			}
			if chunk.FileMeta.Type == mydump.SourceTypeParquet {
				// parquet file is compressed, thus estimates with a factor of 2
				size *= 2
			}
			totalRawFileSize += size
			lastFile = chunk.FileMeta.Path
		}
	}
	totalRawFileSize *= factor

	return EstimateCompactionThreshold2(totalRawFileSize)
}

// EstimateCompactionThreshold2 estimate SST files compression threshold by total row file size
// see EstimateCompactionThreshold for more details.
func EstimateCompactionThreshold2(totalRawFileSize int64) int64 {
	// try restrict the total file number within 512
	threshold := totalRawFileSize / 512
	threshold = utils.NextPowerOfTwo(threshold)
	if threshold < CompactionLowerThreshold {
		// too may small SST files will cause inaccuracy of region range estimation,
		threshold = CompactionLowerThreshold
	} else if threshold > CompactionUpperThreshold {
		threshold = CompactionUpperThreshold
	}

	return threshold
}
