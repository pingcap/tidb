// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

// Split executes a region split. It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) Split(
	ctx context.Context,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
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

	startTime := time.Now()
	// Sort the range for getting the min and max key of the ranges
	sortedRanges, errSplit := SortRanges(ranges, rewriteRules)
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	minKey := codec.EncodeBytesExt(nil, sortedRanges[0].StartKey, isRawKv)
	maxKey := codec.EncodeBytesExt(nil, sortedRanges[len(sortedRanges)-1].EndKey, isRawKv)
	interval := split.SplitRetryInterval
	scatterRegions := make([]*split.RegionInfo, 0)
SplitRegions:
	for i := 0; i < split.SplitRetryTimes; i++ {
		regions, errScan := split.PaginateScanRegion(ctx, rs.client, minKey, maxKey, split.ScanRegionPaginationLimit)
		if errScan != nil {
			if berrors.ErrPDBatchScanRegion.Equal(errScan) {
				log.Warn("inconsistent region info get.", logutil.ShortError(errScan))
				time.Sleep(time.Second)
				continue SplitRegions
			}
			return errors.Trace(errScan)
		}
		splitKeyMap := getSplitKeys(rewriteRules, sortedRanges, regions, isRawKv)
		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		for regionID, keys := range splitKeyMap {
			log.Info("get split keys for region", zap.Int("len", len(keys)), zap.Uint64("region", regionID))
			var newRegions []*split.RegionInfo
			region := regionMap[regionID]
			log.Info("split regions",
				logutil.Region(region.Region), logutil.Keys(keys), rtree.ZapRanges(ranges))
			newRegions, errSplit = rs.splitAndScatterRegions(ctx, region, keys)
			if errSplit != nil {
				if strings.Contains(errSplit.Error(), "no valid key") {
					for _, key := range keys {
						// Region start/end keys are encoded. split_region RPC
						// requires raw keys (without encoding).
						log.Error("split regions no valid key",
							logutil.Key("startKey", region.Region.StartKey),
							logutil.Key("endKey", region.Region.EndKey),
							logutil.Key("key", codec.EncodeBytesExt(nil, key, isRawKv)),
							rtree.ZapRanges(ranges))
					}
					return errors.Trace(errSplit)
				}
				interval = 2 * interval
				if interval > split.SplitMaxRetryInterval {
					interval = split.SplitMaxRetryInterval
				}
				time.Sleep(interval)
				log.Warn("split regions failed, retry",
					zap.Error(errSplit),
					logutil.Region(region.Region),
					logutil.Leader(region.Leader),
					logutil.Keys(keys), rtree.ZapRanges(ranges))
				continue SplitRegions
			}
			log.Info("scattered regions", zap.Int("count", len(newRegions)))
			if len(newRegions) != len(keys) {
				log.Warn("split key count and new region count mismatch",
					zap.Int("new region count", len(newRegions)),
					zap.Int("split key count", len(keys)))
			}
			scatterRegions = append(scatterRegions, newRegions...)
			onSplit(keys)
		}
		break
	}
	if errSplit != nil {
		return errors.Trace(errSplit)
	}
	log.Info("start to wait for scattering regions",
		zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	startTime = time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		rs.waitForScatterRegion(ctx, region)
		if time.Since(startTime) > split.ScatterWaitUpperInterval {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

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

func (rs *RegionSplitter) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, error) {
	resp, err := rs.client.GetOperator(ctx, regionID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Annotatef(berrors.ErrPDInvalidResponse, "get operator error: %s", respErr.GetType())
	}
	retryTimes := ctx.Value(retryTimes).(int)
	if retryTimes > 3 {
		log.Info("get operator", zap.Uint64("regionID", regionID), zap.Stringer("resp", resp))
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func (rs *RegionSplitter) waitForSplit(ctx context.Context, regionID uint64) {
	interval := split.SplitCheckInterval
	for i := 0; i < split.SplitCheckMaxRetryTimes; i++ {
		ok, err := rs.hasHealthyRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Error(err))
			return
		}
		if ok {
			break
		}
		interval = 2 * interval
		if interval > split.SplitMaxCheckInterval {
			interval = split.SplitMaxCheckInterval
		}
		time.Sleep(interval)
	}
}

type retryTimeKey struct{}

var retryTimes = new(retryTimeKey)

func (rs *RegionSplitter) waitForScatterRegion(ctx context.Context, regionInfo *split.RegionInfo) {
	interval := split.ScatterWaitInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < split.ScatterWaitMaxRetryTimes; i++ {
		ctx1 := context.WithValue(ctx, retryTimes, i)
		ok, err := rs.isScatterRegionFinished(ctx1, regionID)
		if err != nil {
			log.Warn("scatter region failed: do not have the region",
				logutil.Region(regionInfo.Region))
			return
		}
		if ok {
			break
		}
		interval = 2 * interval
		if interval > split.ScatterMaxWaitInterval {
			interval = split.ScatterMaxWaitInterval
		}
		time.Sleep(interval)
	}
}

func (rs *RegionSplitter) splitAndScatterRegions(
	ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte,
) ([]*split.RegionInfo, error) {
	if len(keys) == 0 {
		return []*split.RegionInfo{regionInfo}, nil
	}

	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// There would be some regions be scattered twice, e.g.:
	// |--1-|--2-+----|-3--|
	//      |    +(t1)|
	//      +(t1_r4)  |
	//                +(t2_r42)
	// When spliting at `t1_r4`, we would scatter region 1, 2.
	// When spliting at `t2_r42`, we would scatter region 2, 3.
	// Because we don't split at t1 anymore.
	// The trick here is a pinky promise: never scatter regions you haven't imported any data.
	// In this scenario, it is the last region after spliting (applying to >= 5.0).
	if bytes.Equal(newRegions[len(newRegions)-1].Region.StartKey, keys[len(keys)-1]) {
		newRegions = newRegions[:len(newRegions)-1]
	}
	rs.ScatterRegions(ctx, newRegions)
	return newRegions, nil
}

// ScatterRegionsWithBackoffer scatter the region with some backoffer.
// This function is for testing the retry mechanism.
// For a real cluster, directly use ScatterRegions would be fine.
func (rs *RegionSplitter) ScatterRegionsWithBackoffer(ctx context.Context, newRegions []*split.RegionInfo, backoffer utils.Backoffer) {
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

// ScatterRegions scatter the regions.
func (rs *RegionSplitter) ScatterRegions(ctx context.Context, newRegions []*split.RegionInfo) {
	for _, region := range newRegions {
		// Wait for a while until the regions successfully split.
		rs.waitForSplit(ctx, region.Region.Id)
	}

	// the retry is for the temporary network errors during sending request.
	err := utils.WithRetry(ctx, func() error {
		err := rs.client.ScatterRegions(ctx, newRegions)
		if isUnsupportedError(err) {
			log.Warn("batch scatter isn't supported, rollback to old method", logutil.ShortError(err))
			rs.ScatterRegionsWithBackoffer(
				ctx, newRegions,
				// backoff about 6s, or we give up scattering this region.
				&split.ExponentialBackoffer{
					Attempts:    7,
					BaseBackoff: 100 * time.Millisecond,
				})
			return nil
		}
		if err != nil {
			log.Warn("scatter region meet error", logutil.ShortError(err))
		}
		return err
	}, &split.ExponentialBackoffer{Attempts: 3, BaseBackoff: 500 * time.Millisecond})

	if err != nil {
		log.Warn("failed to batch scatter region", logutil.ShortError(err))
	}
}

// getSplitKeys checks if the regions should be split by the end key of
// the ranges, groups the split keys by region id.
func getSplitKeys(rewriteRules *RewriteRules, ranges []rtree.Range, regions []*split.RegionInfo, isRawKv bool) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	checkKeys := make([][]byte, 0)
	for _, rg := range ranges {
		checkKeys = append(checkKeys, rg.EndKey)
	}
	for _, key := range checkKeys {
		if region := NeedSplit(key, regions, isRawKv); region != nil {
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
