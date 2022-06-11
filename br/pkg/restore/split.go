// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"encoding/hex"
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
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Constants for split retry machinery.
const (
	SplitRetryTimes       = 32
	SplitRetryInterval    = 50 * time.Millisecond
	SplitMaxRetryInterval = time.Second

	SplitCheckMaxRetryTimes = 64
	SplitCheckInterval      = 8 * time.Millisecond
	SplitMaxCheckInterval   = time.Second

	ScatterWaitMaxRetryTimes = 64
	ScatterWaitInterval      = 50 * time.Millisecond
	ScatterMaxWaitInterval   = time.Second
	ScatterWaitUpperInterval = 180 * time.Second

	ScanRegionPaginationLimit = 128

	RejectStoreCheckRetryTimes  = 64
	RejectStoreCheckInterval    = 100 * time.Millisecond
	RejectStoreMaxCheckInterval = 2 * time.Second
)

var (
	ScanRegionAttemptTimes = 30
)

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client SplitClient
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client SplitClient) *RegionSplitter {
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
	minKey := codec.EncodeBytes(nil, sortedRanges[0].StartKey)
	maxKey := codec.EncodeBytes(nil, sortedRanges[len(sortedRanges)-1].EndKey)
	interval := SplitRetryInterval
	scatterRegions := make([]*RegionInfo, 0)
SplitRegions:
	for i := 0; i < SplitRetryTimes; i++ {
		regions, errScan := PaginateScanRegion(ctx, rs.client, minKey, maxKey, ScanRegionPaginationLimit)
		if errScan != nil {
			if berrors.ErrPDBatchScanRegion.Equal(errScan) {
				log.Warn("inconsistent region info get.", logutil.ShortError(errScan))
				time.Sleep(time.Second)
				continue SplitRegions
			}
			return errors.Trace(errScan)
		}
		splitKeyMap := getSplitKeys(rewriteRules, sortedRanges, regions, isRawKv)
		regionMap := make(map[uint64]*RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		for regionID, keys := range splitKeyMap {
			log.Info("get split keys for region", zap.Int("len", len(keys)), zap.Uint64("region", regionID))
			var newRegions []*RegionInfo
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
							logutil.Key("key", codec.EncodeBytes(nil, key)),
							rtree.ZapRanges(ranges))
					}
					return errors.Trace(errSplit)
				}
				interval = 2 * interval
				if interval > SplitMaxRetryInterval {
					interval = SplitMaxRetryInterval
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
		if time.Since(startTime) > ScatterWaitUpperInterval {
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
	interval := SplitCheckInterval
	for i := 0; i < SplitCheckMaxRetryTimes; i++ {
		ok, err := rs.hasHealthyRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Error(err))
			return
		}
		if ok {
			break
		}
		interval = 2 * interval
		if interval > SplitMaxCheckInterval {
			interval = SplitMaxCheckInterval
		}
		time.Sleep(interval)
	}
}

type retryTimeKey struct{}

var retryTimes = new(retryTimeKey)

func (rs *RegionSplitter) waitForScatterRegion(ctx context.Context, regionInfo *RegionInfo) {
	interval := ScatterWaitInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
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
		if interval > ScatterMaxWaitInterval {
			interval = ScatterMaxWaitInterval
		}
		time.Sleep(interval)
	}
}

func (rs *RegionSplitter) splitAndScatterRegions(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) ([]*RegionInfo, error) {
	if len(keys) == 0 {
		return []*RegionInfo{regionInfo}, nil
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
func (rs *RegionSplitter) ScatterRegionsWithBackoffer(ctx context.Context, newRegions []*RegionInfo, backoffer utils.Backoffer) {
	newRegionSet := make(map[uint64]*RegionInfo, len(newRegions))
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
			} else if !pdErrorCanRetry(err) {
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
				m := i.(map[uint64]*RegionInfo)
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
func (rs *RegionSplitter) ScatterRegions(ctx context.Context, newRegions []*RegionInfo) {
	for _, region := range newRegions {
		// Wait for a while until the regions successfully split.
		rs.waitForSplit(ctx, region.Region.Id)
	}

	err := utils.WithRetry(ctx, func() error {
		err := rs.client.ScatterRegions(ctx, newRegions)
		if isUnsupportedError(err) {
			log.Warn("batch scatter isn't supported, rollback to old method", logutil.ShortError(err))
			rs.ScatterRegionsWithBackoffer(
				ctx, newRegions,
				// backoff about 6s, or we give up scattering this region.
				&exponentialBackoffer{
					attempt:     7,
					baseBackoff: 100 * time.Millisecond,
				})
			return nil
		}
		return err
		// the retry is for the temporary network errors during sending request.
	}, &exponentialBackoffer{attempt: 3, baseBackoff: 500 * time.Millisecond})

	if err != nil {
		log.Warn("failed to batch scatter region", logutil.ShortError(err))
	}
}

func CheckRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "scan region return empty result, startKey: %s, endKey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "first region's startKey > startKey, startKey: %s, regionStartKey: %s",
			redact.Key(startKey), redact.Key(regions[0].Region.StartKey))
	} else if len(regions[len(regions)-1].Region.EndKey) != 0 && bytes.Compare(regions[len(regions)-1].Region.EndKey, endKey) < 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "last region's endKey < endKey, endKey: %s, regionEndKey: %s",
			redact.Key(endKey), redact.Key(regions[len(regions)-1].Region.EndKey))
	}

	cur := regions[0]
	for _, r := range regions[1:] {
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion, "region endKey not equal to next region startKey, endKey: %s, startKey: %s",
				redact.Key(cur.Region.EndKey), redact.Key(r.Region.StartKey))
		}
		cur = r
	}

	return nil
}

// PaginateScanRegion scan regions with a limit pagination and
// return all regions at once.
// It reduces max gRPC message size.
func PaginateScanRegion(
	ctx context.Context, client SplitClient, startKey, endKey []byte, limit int,
) ([]*RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.Annotatef(berrors.ErrRestoreInvalidRange, "startKey > endKey, startKey: %s, endkey: %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	var regions []*RegionInfo
	var err error
	// we don't need to return multierr. since there only 3 times retry.
	// in most case 3 times retry have the same error. so we just return the last error.
	// actually we'd better remove all multierr in br/lightning.
	// because it's not easy to check multierr equals normal error.
	// see https://github.com/pingcap/tidb/issues/33419.
	_ = utils.WithRetry(ctx, func() error {
		regions = []*RegionInfo{}
		scanStartKey := startKey
		for {
			var batch []*RegionInfo
			batch, err = client.ScanRegions(ctx, scanStartKey, endKey, limit)
			if err != nil {
				return errors.Trace(err)
			}
			regions = append(regions, batch...)
			if len(batch) < limit {
				// No more region
				break
			}
			scanStartKey = batch[len(batch)-1].Region.GetEndKey()
			if len(scanStartKey) == 0 ||
				(len(endKey) > 0 && bytes.Compare(scanStartKey, endKey) >= 0) {
				// All key space have scanned
				break
			}
		}
		if err = CheckRegionConsistency(startKey, endKey, regions); err != nil {
			log.Warn("failed to scan region, retrying", logutil.ShortError(err))
			return err
		}
		return nil
	}, newScanRegionBackoffer())

	return regions, err
}

type scanRegionBackoffer struct {
	attempt int
}

func newScanRegionBackoffer() utils.Backoffer {
	return &scanRegionBackoffer{
		attempt: ScanRegionAttemptTimes,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *scanRegionBackoffer) NextBackoff(err error) time.Duration {
	if berrors.ErrPDBatchScanRegion.Equal(err) {
		// 500ms * 30 could be enough for splitting remain regions in the hole.
		b.attempt--
		return 500 * time.Millisecond
	}
	b.attempt = 0
	return 0
}

// Attempt returns the remain attempt times
func (b *scanRegionBackoffer) Attempt() int {
	return b.attempt
}

// getSplitKeys checks if the regions should be split by the end key of
// the ranges, groups the split keys by region id.
func getSplitKeys(rewriteRules *RewriteRules, ranges []rtree.Range, regions []*RegionInfo, isRawKv bool) map[uint64][][]byte {
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
func NeedSplit(splitKey []byte, regions []*RegionInfo, isRawKv bool) *RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	if !isRawKv {
		splitKey = codec.EncodeBytes(nil, splitKey)
	}
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
