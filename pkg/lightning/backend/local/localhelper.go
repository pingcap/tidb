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
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (

	// the base exponential backoff time
	// the variable is only changed in unit test for running test faster.
	splitRegionBaseBackOffTime = time.Second
)

// splitAndScatterRegionInBatches splits&scatter regions in batches.
// Too many split&scatter requests may put a lot of pressure on TiKV and PD.
func (local *Backend) splitAndScatterRegionInBatches(
	ctx context.Context,
	splitKeys [][]byte,
	batchCnt int,
) error {
	for i := 0; i < len(splitKeys); i += batchCnt {
		batch := splitKeys[i:]
		if len(batch) > batchCnt {
			batch = batch[:batchCnt]
		}
		if err := local.splitAndScatterRegionByRanges(ctx, batch); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (local *Backend) splitAndScatterRegionByRanges(
	ctx context.Context,
	splitKeys [][]byte,
) (err error) {
	if len(splitKeys) == 0 {
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

	scatterRegions, err := local.splitCli.SplitKeysAndScatter(ctx, splitKeys)
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

func largerStartKey(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

// StoreWriteLimiter is used to limit the write rate of a store.
type StoreWriteLimiter interface {
	WaitN(ctx context.Context, storeID uint64, n int) error
	Limit() int
	UpdateLimit(limit int)
}

type storeWriteLimiter struct {
	rwm      sync.RWMutex
	limiters map[uint64]*rate.Limiter
	// limit and burst can only be non-negative, 0 means no rate limiting.
	limit atomic.Int64
	burst atomic.Int64
}

func newStoreWriteLimiter(limit int) *storeWriteLimiter {
	l, b := calculateLimitAndBurst(limit)
	s := &storeWriteLimiter{
		limiters: make(map[uint64]*rate.Limiter),
	}
	s.limit.Store(l)
	s.burst.Store(b)
	return s
}

func calculateLimitAndBurst(writeLimit int) (limit int64, burst int64) {
	if writeLimit <= 0 {
		return 0, 0
	}
	// Allow burst of at most 20% of the writeLimit.
	if writeLimit <= math.MaxInt-writeLimit/5 {
		burst = int64(writeLimit) + int64(writeLimit)/5
	} else {
		// If overflowed, set burst to math.MaxInt.
		burst = math.MaxInt
	}
	return int64(writeLimit), burst
}

func (s *storeWriteLimiter) WaitN(ctx context.Context, storeID uint64, n int) error {
	limiter := s.getLimiter(storeID)
	if limiter == nil {
		return nil
	}
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
	return int(s.limit.Load())
}

func (s *storeWriteLimiter) getLimiter(storeID uint64) *rate.Limiter {
	if s.limit.Load() == 0 {
		return nil
	}
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
		limiter = rate.NewLimiter(rate.Limit(s.limit.Load()), int(s.burst.Load()))
		s.limiters[storeID] = limiter
	}
	return limiter
}

func (s *storeWriteLimiter) UpdateLimit(newLimit int) {
	limit, burst := calculateLimitAndBurst(newLimit)
	if s.limit.Load() == limit {
		return
	}

	s.limit.Store(limit)
	s.burst.Store(burst)
	// Update all existing limiters with the new limit and burst values.
	s.rwm.Lock()
	defer s.rwm.Unlock()
	if s.limit.Load() == 0 {
		s.limiters = make(map[uint64]*rate.Limiter)
		return
	}
	for _, limiter := range s.limiters {
		limiter.SetLimit(rate.Limit(s.limit.Load()))
		limiter.SetBurst(int(s.burst.Load()))
	}
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
