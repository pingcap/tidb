// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type retryTimeKey struct{}

var retryTimes = new(retryTimeKey)

type Granularity string

const (
	FineGrained   Granularity = "fine-grained"
	CoarseGrained Granularity = "coarse-grained"
)

type SplitContext struct {
	isRawKv     bool
	needScatter bool
	waitScatter bool
	storeCount  int
	onSplit     OnSplitFunc
}

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client split.SplitClient
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client split.SplitClient) *RegionSplitter {
	return &RegionSplitter{
		client: client,
	}
}

// OnSplitFunc is called before split a range.
type OnSplitFunc func(key [][]byte)

// ExecuteSplit executes regions split and make sure new splitted regions are balance.
// It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) ExecuteSplit(
	ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	storeCount int,
	granularity string,
	isRawKv bool,
	onSplit OnSplitFunc,
) error {
	if len(ranges) == 0 {
		log.Info("skip split regions, no range")
		return nil
	}

	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("RegionSplitter.Split", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	// Sort the range for getting the min and max key of the ranges
	// TODO: this sort may not needed if we sort tables after creatation outside.
	sortedRanges, errSplit := SortRanges(ranges, rewriteRules)
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	sortedKeys := make([][]byte, 0, len(sortedRanges))
	totalRangeSize := uint64(0)
	for _, r := range sortedRanges {
		sortedKeys = append(sortedKeys, r.EndKey)
		totalRangeSize += r.Size
	}
	sctx := SplitContext{
		isRawKv:     isRawKv,
		needScatter: true,
		waitScatter: false,
		onSplit:     onSplit,
		storeCount:  storeCount,
	}
	if granularity == string(CoarseGrained) {
		return rs.executeSplitByRanges(ctx, sctx, sortedRanges)
	}
	return rs.executeSplitByKeys(ctx, sctx, sortedKeys)
}

func (rs *RegionSplitter) executeSplitByRanges(
	ctx context.Context,
	splitContext SplitContext,
	sortedRanges []rtree.Range,
) error {
	startTime := time.Now()
	minKey := codec.EncodeBytesExt(nil, sortedRanges[0].StartKey, splitContext.isRawKv)
	maxKey := codec.EncodeBytesExt(nil, sortedRanges[len(sortedRanges)-1].EndKey, splitContext.isRawKv)

	err := utils.WithRetry(ctx, func() error {
		regions, err := split.PaginateScanRegion(ctx, rs.client, minKey, maxKey, split.ScanRegionPaginationLimit)
		if err != nil {
			return err
		}
		lastSortedIndex := 0
		sortedIndex := 0
		splitRangeMap := make(map[uint64][]rtree.Range)
		regionMap := make(map[uint64]*split.RegionInfo)
	loop:
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
			// collect all sortedKeys belong to this region
			if len(region.Region.GetEndKey()) == 0 {
				splitRangeMap[region.Region.GetId()] = sortedRanges[lastSortedIndex:]
				break
			}
			for {
				encodeKey := codec.EncodeBytesExt(nil, sortedRanges[sortedIndex].StartKey, splitContext.isRawKv)
				if bytes.Compare(encodeKey, region.Region.GetEndKey()) >= 0 {
					//                  start    end
					// range:            |--------|
					// region: |---------|
					// pick up this range due to region end key is exclusive.
					splitRangeMap[region.Region.GetId()] = sortedRanges[lastSortedIndex:sortedIndex]
					lastSortedIndex = sortedIndex
					// reach the region end key and break for next region
					break
				}
				sortedIndex += 1
				if sortedIndex >= len(sortedRanges) {
					splitRangeMap[region.Region.GetId()] = sortedRanges[lastSortedIndex:]
					// has reach the region files' end
					break loop
				}
			}
		}

		workerPool := utils.NewWorkerPool(uint(splitContext.storeCount), "split ranges")
		eg, ectx := errgroup.WithContext(ctx)
		for rID, rgs := range splitRangeMap {
			region := regionMap[rID]
			ranges := rgs
			sctx := splitContext
			sctx.waitScatter = true
			workerPool.ApplyOnErrorGroup(eg, func() error {
				var newRegions []*split.RegionInfo
				rangeSize := uint64(0)
				allKeys := make([][]byte, 0, len(ranges))
				if len(ranges) <= 1 {
					// we may have splitted in last restore run.
					return nil
				}
				for _, rg := range ranges {
					rangeSize += rg.Size
					allKeys = append(allKeys, rg.EndKey)
				}
				expectSplitSize := rangeSize / uint64(sctx.storeCount)
				size := uint64(0)
				keys := make([][]byte, 0, sctx.storeCount)
				for _, rg := range ranges {
					if size >= expectSplitSize {
						// collect enough ranges, choose this one
						keys = append(keys, rg.EndKey)
						log.Info("choose the split key", zap.Uint64("split size", size), logutil.Key("key", rg.EndKey))
						size = 0
					}
					size += rg.Size
				}
				keys = keys[:sctx.storeCount-1]
				log.Info("get split ranges for region",
					zap.Int("keys", len(keys)),
					zap.Uint64("expect split size", expectSplitSize),
					zap.Uint64("total range size", rangeSize),
					zap.Bool("need scatter", sctx.needScatter),
					zap.Bool("wait scatter", sctx.waitScatter),
					logutil.Keys(keys),
					logutil.Region(region.Region))
				newRegions, err := rs.splitAndScatterRegions(ectx, sctx, region, keys)
				if err != nil {
					return err
				}
				if len(newRegions) != len(keys) {
					log.Warn("split key count and new region count mismatch",
						zap.Int("new region count", len(newRegions)),
						zap.Int("split key count", len(keys)))
				}
				sctx.onSplit(keys)
				sctx.needScatter = false
				return rs.executeSplitByKeys(ectx, sctx, allKeys)
			})
		}
		return eg.Wait()
	}, newSplitBackoffer())
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("finish splitting and scattering regions by ranges",
		zap.Duration("take", time.Since(startTime)))
	return nil
}

// executeSplitByKeys will split regions by **sorted** keys with following steps.
// 1. locate regions with correspond keys.
// 2. split these regions with correspond keys.
// 3. make sure new splitted regions are balanced.
func (rs *RegionSplitter) executeSplitByKeys(
	ctx context.Context,
	splitContext SplitContext,
	sortedKeys [][]byte,
) error {
	var mutex sync.Mutex
	startTime := time.Now()
	minKey := codec.EncodeBytesExt(nil, sortedKeys[0], splitContext.isRawKv)
	maxKey := codec.EncodeBytesExt(nil, sortedKeys[len(sortedKeys)-1], splitContext.isRawKv)
	scatterRegions := make([]*split.RegionInfo, 0)
	regionsMap := make(map[uint64]*split.RegionInfo)

	err := utils.WithRetry(ctx, func() error {
		maps.Clear(regionsMap)
		regions, err := split.PaginateScanRegion(ctx, rs.client, minKey, maxKey, split.ScanRegionPaginationLimit)
		if err != nil {
			return err
		}
		splitKeyMap := getSplitKeys(splitContext, sortedKeys, regions)
		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		workerPool := utils.NewWorkerPool(8, "split keys")
		eg, ectx := errgroup.WithContext(ctx)
		for regionID, splitKeys := range splitKeyMap {
			region := regionMap[regionID]
			keys := splitKeys
			sctx := splitContext
			workerPool.ApplyOnErrorGroup(eg, func() error {
				log.Info("get split keys for split regions",
					logutil.Region(region.Region), logutil.Keys(keys),
					zap.Bool("need scatter", sctx.needScatter))
				newRegions, err := rs.splitAndScatterRegions(ectx, sctx, region, keys)
				if err != nil {
					return err
				}
				if len(newRegions) != len(keys) {
					log.Warn("split key count and new region count mismatch",
						zap.Int("new region count", len(newRegions)),
						zap.Int("split key count", len(keys)))
				}
				if sctx.needScatter {
					log.Info("scattered regions", zap.Int("count", len(newRegions)))
					mutex.Lock()
					for _, r := range newRegions {
						regionsMap[r.Region.Id] = r
					}
					mutex.Unlock()
				}
				sctx.onSplit(keys)
				return nil
			})
		}
		err = eg.Wait()
		if err != nil {
			return err
		}
		for _, r := range regionsMap {
			// merge all scatter regions
			scatterRegions = append(scatterRegions, r)
		}
		return nil
	}, newSplitBackoffer())
	if err != nil {
		return errors.Trace(err)
	}
	if len(scatterRegions) > 0 {
		log.Info("finish splitting and scattering regions. and starts to wait", zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
		rs.waitRegionsScattered(ctx, scatterRegions, split.ScatterWaitUpperInterval)
	} else {
		log.Info("finish splitting regions.", zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

func (rs *RegionSplitter) splitAndScatterRegions(
	ctx context.Context, splitContext SplitContext, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	if len(keys) < 1 {
		return []*split.RegionInfo{regionInfo}, nil
	}

	newRegions, err := rs.splitRegionsSync(ctx, regionInfo, keys)
	if err != nil {
		if strings.Contains(err.Error(), "no valid key") {
			for _, key := range keys {
				// Region start/end keys are encoded. split_region RPC
				// requires raw keys (without encoding).
				log.Error("split regions no valid key",
					logutil.Key("startKey", regionInfo.Region.StartKey),
					logutil.Key("endKey", regionInfo.Region.EndKey),
					logutil.Key("key", codec.EncodeBytesExt(nil, key, splitContext.isRawKv)))
			}
		}
		return nil, errors.Trace(err)
	}
	if splitContext.needScatter {
		// To make region leader balanced. need scatter origin one too
		if splitContext.waitScatter {
			rs.ScatterRegionsSync(ctx, append(newRegions, regionInfo))
		} else {
			rs.ScatterRegionsAsync(ctx, append(newRegions, regionInfo))
		}
	}
	return newRegions, nil
}

// splitRegionsSync perform batchSplit on a region by keys
// and then check the batch split success or not.
func (rs *RegionSplitter) splitRegionsSync(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	if len(keys) == 0 {
		return []*split.RegionInfo{regionInfo}, nil
	}
	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rs.waitRegionsSplitted(ctx, newRegions)
	return newRegions, nil
}

// ScatterRegionsAsync scatter the regions.
// for same reason just log and ignore error.
// See the comments of function waitRegionScattered.
func (rs *RegionSplitter) ScatterRegionsAsync(ctx context.Context, newRegions []*split.RegionInfo) {
	log.Info("start to scatter regions", zap.Int("regions", len(newRegions)))
	// the retry is for the temporary network errors during sending request.
	err := utils.WithRetry(ctx, func() error {
		err := rs.client.ScatterRegions(ctx, newRegions)
		if isUnsupportedError(err) {
			log.Warn("batch scatter isn't supported, rollback to old method", logutil.ShortError(err))
			rs.ScatterRegionsSequentially(
				ctx, newRegions,
				// backoff about 6s, or we give up scattering this region.
				&split.ExponentialBackoffer{
					Attempts:    7,
					BaseBackoff: 100 * time.Millisecond,
				})
			return nil
		}
		return err
	}, &split.ExponentialBackoffer{Attempts: 3, BaseBackoff: 500 * time.Millisecond})
	if err != nil {
		log.Warn("failed to scatter regions", logutil.ShortError(err))
	}
}

// ScatterRegionsSync scatter the regions and wait these region scattered from PD.
// for same reason just log and ignore error.
// See the comments of function waitRegionScattered.
func (rs *RegionSplitter) ScatterRegionsSync(ctx context.Context, newRegions []*split.RegionInfo) {
	rs.ScatterRegionsAsync(ctx, newRegions)
	rs.waitRegionsScattered(ctx, newRegions, split.ScatterWaitUpperInterval)
}

// waitRegionsSplitted check multiple regions have finished the split.
func (rs *RegionSplitter) waitRegionsSplitted(ctx context.Context, splitRegions []*split.RegionInfo) {
	// Wait for a while until the regions successfully split.
	for _, region := range splitRegions {
		rs.waitRegionSplitted(ctx, region.Region.Id)
	}
}

// waitRegionSplitted check single region has finished the split.
func (rs *RegionSplitter) waitRegionSplitted(ctx context.Context, regionID uint64) {
	state := utils.InitialRetryState(
		split.SplitCheckMaxRetryTimes,
		split.SplitCheckInterval,
		split.SplitMaxCheckInterval,
	)
	err := utils.WithRetry(ctx, func() error { //nolint: errcheck
		ok, err := rs.hasHealthyRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Uint64("regionID", regionID), zap.Error(err))
			return err
		}
		if ok {
			return nil
		}
		return errors.Annotate(berrors.ErrPDSplitFailed, "wait region splitted failed")
	}, &state)
	if err != nil {
		log.Warn("failed to split regions", logutil.ShortError(err))
	}
}

// waitRegionsScattered try to wait mutilple regions scatterd in 3 minutes.
// this could timeout, but if many regions scatterd the restore could continue
// so we don't wait long time here.
func (rs *RegionSplitter) waitRegionsScattered(ctx context.Context, scatterRegions []*split.RegionInfo, timeout time.Duration) {
	log.Info("start to wait for scattering regions", zap.Int("regions", len(scatterRegions)))
	startTime := time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		rs.waitRegionScattered(ctx, region)
		if time.Since(startTime) > timeout {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
}

// waitRegionsScattered try to wait single region scatterd
// because we may not get the accurate result of scatter region.
// even we got error here the scatter could also succeed.
// so add a warn log and ignore error does make sense here.
func (rs *RegionSplitter) waitRegionScattered(ctx context.Context, regionInfo *split.RegionInfo) {
	state := utils.InitialRetryState(split.ScatterWaitMaxRetryTimes, split.ScatterWaitInterval, split.ScatterMaxWaitInterval)
	retryCount := 0
	err := utils.WithRetry(ctx, func() error {
		ctx1 := context.WithValue(ctx, retryTimes, retryCount)
		ok, _, err := rs.isScatterRegionFinished(ctx1, regionInfo.Region.Id)
		if err != nil {
			log.Warn("scatter region failed: do not have the region",
				logutil.Region(regionInfo.Region))
			return err
		}
		if ok {
			return nil
		}
		retryCount++
		return errors.Annotatef(berrors.ErrPDUnknownScatterResult, "try wait region scatter")
	}, &state)
	if err != nil {
		log.Warn("wait scatter region meet error", logutil.Region(regionInfo.Region), logutil.ShortError(err))
	}
}

// ScatterRegionsSequentially scatter the region with some backoffer.
// This function is for testing the retry mechanism.
// For a real cluster, directly use ScatterRegions would be fine.
func (rs *RegionSplitter) ScatterRegionsSequentially(ctx context.Context, newRegions []*split.RegionInfo, backoffer utils.Backoffer) {
	newRegionSet := make(map[uint64]*split.RegionInfo, len(newRegions))
	for _, newRegion := range newRegions {
		newRegionSet[newRegion.Region.Id] = newRegion
	}

	if err := utils.WithRetry(ctx, func() error {
		log.Info("trying to scatter regions...", zap.Int("remain", len(newRegionSet)))
		var errs error
		for _, region := range newRegionSet {
			err := rs.client.ScatterRegion(ctx, region)
			if err == nil {
				// it is safe according to the Go language spec.
				delete(newRegionSet, region.Region.Id)
			} else if !split.PdErrorCanRetry(err) {
				log.Warn("scatter meet error cannot be retried, skipping",
					logutil.ShortError(err),
					logutil.Region(region.Region),
				)
				delete(newRegionSet, region.Region.Id)
			}
			errs = multierr.Append(errs, err)
		}
		return errs
	}, backoffer); err != nil {
		log.Warn("Some regions haven't been scattered because errors.",
			zap.Int("count", len(newRegionSet)),
			// if all region are failed to scatter, the short error might also be verbose...
			logutil.ShortError(err),
			logutil.AbbreviatedArray("failed-regions", newRegionSet, func(i interface{}) []string {
				m := i.(map[uint64]*split.RegionInfo)
				result := make([]string, 0, len(m))
				for id := range m {
					result = append(result, strconv.Itoa(int(id)))
				}
				return result
			}),
		)
	}
}

// hasHealthyRegion is used to check whether region splitted success
func (rs *RegionSplitter) hasHealthyRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := rs.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// the region hasn't get ready.
	if regionInfo == nil {
		return false, nil
	}

	// check whether the region is healthy and report.
	// TODO: the log may be too verbose. we should use Prometheus metrics once it get ready for BR.
	for _, peer := range regionInfo.PendingPeers {
		log.Debug("unhealthy region detected", logutil.Peer(peer), zap.String("type", "pending"))
	}
	for _, peer := range regionInfo.DownPeers {
		log.Debug("unhealthy region detected", logutil.Peer(peer), zap.String("type", "down"))
	}
	// we ignore down peers for they are (normally) hard to be fixed in reasonable time.
	// (or once there is a peer down, we may get stuck at waiting region get ready.)
	return len(regionInfo.PendingPeers) == 0, nil
}

// isScatterRegionFinished check the latest successful operator and return the follow status:
//
//	return (finished, needRescatter, error)
//
// if the latest operator is not `scatter-operator`, or its status is SUCCESS, it's likely that the
// scatter region operator is finished.
//
// if the latest operator is `scatter-operator` and its status is TIMEOUT or CANCEL, the needRescatter
// is true and the function caller needs to scatter this region again.
func (rs *RegionSplitter) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, bool, error) {
	resp, err := rs.client.GetOperator(ctx, regionID)
	if err != nil {
		if common.IsRetryableError(err) {
			// retry in the next cycle
			return false, false, nil
		}
		return false, false, errors.Trace(err)
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, false, nil
		}
		return false, false, errors.Annotatef(berrors.ErrPDInvalidResponse, "get operator error: %s", respErr.GetType())
	}
	retryTimes := ctx.Value(retryTimes).(int)
	if retryTimes > 3 {
		log.Info("get operator", zap.Uint64("regionID", regionID), zap.Stringer("resp", resp))
	}
	// that 'scatter-operator' has finished
	if string(resp.GetDesc()) != "scatter-region" {
		return true, false, nil
	}
	switch resp.GetStatus() {
	case pdpb.OperatorStatus_SUCCESS:
		return true, false, nil
	case pdpb.OperatorStatus_RUNNING:
		return false, false, nil
	default:
		return false, true, nil
	}
}

func (rs *RegionSplitter) WaitForScatterRegionsTimeout(ctx context.Context, regionInfos []*split.RegionInfo, timeout time.Duration) int {
	var (
		startTime   = time.Now()
		interval    = split.ScatterWaitInterval
		leftRegions = mapRegionInfoSlice(regionInfos)
		retryCnt    = 0

		reScatterRegions = make([]*split.RegionInfo, 0, len(regionInfos))
	)
	for {
		ctx1 := context.WithValue(ctx, retryTimes, retryCnt)
		reScatterRegions = reScatterRegions[:0]
		for regionID, regionInfo := range leftRegions {
			ok, rescatter, err := rs.isScatterRegionFinished(ctx1, regionID)
			if err != nil {
				log.Warn("scatter region failed: do not have the region",
					logutil.Region(regionInfo.Region), zap.Error(err))
				delete(leftRegions, regionID)
				continue
			}
			if ok {
				delete(leftRegions, regionID)
				continue
			}
			if rescatter {
				reScatterRegions = append(reScatterRegions, regionInfo)
			}
			// RUNNING_STATUS, just wait and check it in the next loop
		}

		if len(leftRegions) == 0 {
			return 0
		}

		if len(reScatterRegions) > 0 {
			rs.ScatterRegionsAsync(ctx1, reScatterRegions)
		}

		if time.Since(startTime) > timeout {
			break
		}
		retryCnt += 1
		interval = 2 * interval
		if interval > split.ScatterMaxWaitInterval {
			interval = split.ScatterMaxWaitInterval
		}
		time.Sleep(interval)
	}

	return len(leftRegions)
}

func mapRegionInfoSlice(regionInfos []*split.RegionInfo) map[uint64]*split.RegionInfo {
	regionInfoMap := make(map[uint64]*split.RegionInfo)
	for _, info := range regionInfos {
		regionID := info.Region.GetId()
		regionInfoMap[regionID] = info
	}
	return regionInfoMap
}

// getSplitKeys checks if the regions should be split by the end key of
// the ranges, groups the split keys by region id.
func getSplitKeys(splitContext SplitContext, keys [][]byte, regions []*split.RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	for _, key := range keys {
		if region := NeedSplit(key, regions, splitContext.isRawKv); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
			log.Debug("get key for split region",
				logutil.Key("key", key),
				logutil.Key("startKey", region.Region.StartKey),
				logutil.Key("endKey", region.Region.EndKey))
		}
	}
	return splitKeyMap
}

// NeedSplit checks whether a key is necessary to split, if true returns the split region.
func NeedSplit(splitKey []byte, regions []*split.RegionInfo, isRawKv bool) *split.RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	splitKey = codec.EncodeBytesExt(nil, splitKey, isRawKv)
	for _, region := range regions {
		// If splitKey is the boundary of the region
		if bytes.Equal(splitKey, region.Region.GetStartKey()) {
			return nil
		}
		// If splitKey is in a region
		if region.ContainsInterior(splitKey) {
			return region
		}
	}
	return nil
}

func replacePrefix(s []byte, rewriteRules *RewriteRules) ([]byte, *sst.RewriteRule) {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}

	return s, nil
}

// isUnsupportedError checks whether we should fallback to ScatterRegion API when meeting the error.
func isUnsupportedError(err error) bool {
	s, ok := status.FromError(errors.Cause(err))
	if !ok {
		// Not a gRPC error. Something other went wrong.
		return false
	}
	// In two conditions, we fallback to ScatterRegion:
	// (1) If the RPC endpoint returns UNIMPLEMENTED. (This is just for making test cases not be so magic.)
	// (2) If the Message is "region 0 not found":
	//     In fact, PD reuses the gRPC endpoint `ScatterRegion` for the batch version of scattering.
	//     When the request contains the field `regionIDs`, it would use the batch version,
	//     Otherwise, it uses the old version and scatter the region with `regionID` in the request.
	//     When facing 4.x, BR(which uses v5.x PD clients and call `ScatterRegions`!) would set `regionIDs`
	//     which would be ignored by protocol buffers, and leave the `regionID` be zero.
	//     Then the older version of PD would try to search the region with ID 0.
	//     (Then it consistently fails, and returns "region 0 not found".)
	return s.Code() == codes.Unimplemented ||
		strings.Contains(s.Message(), "region 0 not found")
}

type splitBackoffer struct {
	state utils.RetryState
}

func newSplitBackoffer() *splitBackoffer {
	return &splitBackoffer{
		state: utils.InitialRetryState(split.SplitRetryTimes, split.SplitRetryInterval, split.SplitMaxRetryInterval),
	}
}

func (bo *splitBackoffer) NextBackoff(err error) time.Duration {
	switch {
	case berrors.ErrPDBatchScanRegion.Equal(err):
		log.Warn("inconsistent region info get.", logutil.ShortError(err))
		return time.Second
	case strings.Contains(err.Error(), "no valid key"):
		bo.state.GiveUp()
		return 0
	case berrors.ErrRestoreInvalidRange.Equal(err):
		bo.state.GiveUp()
		return 0
	}
	return bo.state.ExponentialBackoff()
}

func (bo *splitBackoffer) Attempt() int {
	return bo.state.Attempt()
}
