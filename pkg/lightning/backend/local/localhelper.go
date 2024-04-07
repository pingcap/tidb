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
	"math"
	"slices"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	retrySplitMaxWaitTime = 4 * time.Second
)

var (

	// the base exponential backoff time
	// the variable is only changed in unit test for running test faster.
	splitRegionBaseBackOffTime = time.Second
	// the max retry times to split regions.
	splitRetryTimes = 8
)

// SplitAndScatterRegionInBatches splits&scatter regions in batches.
// Too many split&scatter requests may put a lot of pressure on TiKV and PD.
func (local *Backend) SplitAndScatterRegionInBatches(
	ctx context.Context,
	ranges []common.Range,
	batchCnt int,
) error {
	for i := 0; i < len(ranges); i += batchCnt {
		batch := ranges[i:]
		if len(batch) > batchCnt {
			batch = batch[:batchCnt]
		}
		if err := local.SplitAndScatterRegionByRanges(ctx, batch); err != nil {
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
	ranges []common.Range,
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

	minKey := codec.EncodeBytes([]byte{}, ranges[0].Start)
	maxKey := codec.EncodeBytes([]byte{}, ranges[len(ranges)-1].End)

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

		if len(ranges) == 0 {
			log.FromContext(ctx).Info("no ranges need to be split, skipped.")
			return nil
		}

		var splitKeyMap map[*split.RegionInfo][][]byte
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
			splitKeyMap = split.GetSplitKeysOfRegions(retryKeys, regions, false)
			retryKeys = retryKeys[:0]
		} else {
			splitKeyMap = getSplitKeysByRanges(ranges, regions)
		}

		type splitInfo struct {
			region *split.RegionInfo
			keys   [][]byte
		}

		var syncLock sync.Mutex
		size := min(len(splitKeyMap), local.RegionSplitConcurrency)
		ch := make(chan *splitInfo, size)
		eg, splitCtx := errgroup.WithContext(ctx)

		for splitWorker := 0; splitWorker < size; splitWorker++ {
			eg.Go(func() error {
				for sp := range ch {
					region := sp.region
					keys := sp.keys
					newRegions, err2 := local.splitCli.SplitWaitAndScatter(splitCtx, region, keys)
					if err2 != nil {
						if common.IsContextCanceledError(err2) {
							return err2
						}
						log.FromContext(ctx).Warn("split regions",
							log.ShortError(err2),
							zap.Int("retry time", i),
							zap.Uint64("region_id", region.Region.Id))
						// TODO(lance6716): now the retryKeys can not be shrank to the middle of the keys
						// and it will retry all keys of the region. Will fix it in future PR.
						syncLock.Lock()
						retryKeys = append(retryKeys, keys...)
						// set global error so if we exceed retry limit, the function will return this error
						err = multierr.Append(err, err2)
						syncLock.Unlock()
						continue
					}
					syncLock.Lock()
					scatterRegions = append(scatterRegions, newRegions...)
					syncLock.Unlock()
				}
				return nil
			})
		}
	sendLoop:
		for region, keys := range splitKeyMap {
			select {
			case ch <- &splitInfo{region: region, keys: keys}:
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
	unScatteredCount, err := local.splitCli.WaitRegionsScattered(ctx, scatterRegions)
	if unScatteredCount == 0 {
		log.FromContext(ctx).Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.FromContext(ctx).Info("waiting for scattering regions timeout",
			zap.Int("unScatteredCount", unScatteredCount),
			zap.Int("allRegionCount", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)),
			zap.Error(err))
	}
	return nil
}

func (local *Backend) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := local.splitCli.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func getSplitKeysByRanges(ranges []common.Range, regions []*split.RegionInfo) map[*split.RegionInfo][][]byte {
	checkKeys := make([][]byte, 0)
	var lastEnd []byte
	for _, rg := range ranges {
		if !bytes.Equal(lastEnd, rg.Start) {
			checkKeys = append(checkKeys, rg.Start)
		}
		checkKeys = append(checkKeys, rg.End)
		lastEnd = rg.End
	}
	return split.GetSplitKeysOfRegions(checkKeys, regions, false)
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

func intersectRange(region *metapb.Region, rg common.Range) common.Range {
	var startKey, endKey []byte
	if len(region.StartKey) > 0 {
		_, startKey, _ = codec.DecodeBytes(region.StartKey, []byte{})
	}
	if bytes.Compare(startKey, rg.Start) < 0 {
		startKey = rg.Start
	}
	if len(region.EndKey) > 0 {
		_, endKey, _ = codec.DecodeBytes(region.EndKey, []byte{})
	}
	if beforeEnd(rg.End, endKey) {
		endKey = rg.End
	}

	return common.Range{Start: startKey, End: endKey}
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
	threshold = mathutil.NextPowerOfTwo(threshold)
	if threshold < CompactionLowerThreshold {
		// too may small SST files will cause inaccuracy of region range estimation,
		threshold = CompactionLowerThreshold
	} else if threshold > CompactionUpperThreshold {
		threshold = CompactionUpperThreshold
	}

	return threshold
}
