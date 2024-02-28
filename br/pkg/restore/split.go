// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/multierr"
	"go.uber.org/zap"
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

const (
	splitRegionKeysConcurrency   = 8
	splitRegionRangesConcurrency = 32
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
	if len(sortedRanges) == 0 {
		log.Info("skip split regions after sorted, no range")
		return nil
	}
	sortedKeys := make([][]byte, 0, len(sortedRanges))
	totalRangeSize := uint64(0)
	for _, r := range sortedRanges {
		sortedKeys = append(sortedKeys, r.EndKey)
		totalRangeSize += r.Size
	}
	// need use first range's start key to scan region
	// and the range size must be greater than 0 here
	scanStartKey := sortedRanges[0].StartKey
	sctx := SplitContext{
		isRawKv:     isRawKv,
		needScatter: true,
		waitScatter: false,
		onSplit:     onSplit,
		storeCount:  storeCount,
	}
	return rs.executeSplitByRanges(ctx, sctx, scanStartKey, sortedKeys, granularity)
}

func (rs *RegionSplitter) executeSplitByRanges(
	ctx context.Context,
	splitContext SplitContext,
	scanStartKey []byte,
	sortedKeys [][]byte,
	granularity string,
) error {
	startTime := time.Now()
	splitContext.needScatter = true
	// Choose the rough region split keys,
	// each splited region contains 128 regions to be splitted.
	const regionIndexStep = 128
	const maxSplitKeysOnce = 10240
	curRegionIndex := regionIndexStep
	for {
		roughSortedSplitKeys := make([][]byte, 0, maxSplitKeysOnce)
		for i := 0; i < maxSplitKeysOnce && curRegionIndex < len(sortedKeys); i += 1 {
			roughSortedSplitKeys = append(roughSortedSplitKeys, sortedKeys[curRegionIndex])
			curRegionIndex += regionIndexStep
		}
		if len(roughSortedSplitKeys) == 0 {
			break
		}
		if err := rs.executeSplitByKeys(ctx, splitContext, scanStartKey, roughSortedSplitKeys); err != nil {
			return errors.Trace(err)
		}
		if curRegionIndex >= len(sortedKeys) {
			break
		}
	}
	log.Info("finish spliting regions roughly", zap.Duration("take", time.Since(startTime)))

	// Then send split requests to each TiKV.
	if err := rs.executeSplitByKeys(ctx, splitContext, scanStartKey, sortedKeys); err != nil {
		return errors.Trace(err)
	}

	log.Info("finish spliting and scattering regions", zap.Duration("take", time.Since(startTime)))
	return nil
}

// executeSplitByKeys will split regions by **sorted** keys with following steps.
// 1. locate regions with correspond keys.
// 2. split these regions with correspond keys.
// 3. make sure new splitted regions are balanced.
func (rs *RegionSplitter) executeSplitByKeys(
	ctx context.Context,
	splitContext SplitContext,
	scanStartKey []byte,
	sortedKeys [][]byte,
) error {
	var mutex sync.Mutex
	startTime := time.Now()
	minKey := codec.EncodeBytesExt(nil, scanStartKey, splitContext.isRawKv)
	maxKey := codec.EncodeBytesExt(nil, sortedKeys[len(sortedKeys)-1], splitContext.isRawKv)
	scatterRegions := make([]*split.RegionInfo, 0)
	regionsMap := make(map[uint64]*split.RegionInfo)

	err := utils.WithRetry(ctx, func() error {
		clear(regionsMap)
		regions, err := split.PaginateScanRegion(ctx, rs.client, minKey, maxKey, split.ScanRegionPaginationLimit)
		if err != nil {
			return err
		}
		splitKeyMap := getSplitSortedKeysFromSortedRegions(splitContext, sortedKeys, regions)
		regionMap := make(map[uint64]*split.RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		workerPool := utils.NewWorkerPool(uint(splitContext.storeCount), "split keys")
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
	// it takes 30 minutes to scatter regions when each TiKV has 400k regions
	leftCnt := rs.WaitForScatterRegionsTimeout(ctx, scatterRegions, 30*time.Minute)
	if leftCnt == 0 {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("not scattered Count", leftCnt),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
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
			logutil.AbbreviatedArray("failed-regions", newRegionSet, func(i any) []string {
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
	if retryTimes > 10 {
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

// TestGetSplitSortedKeysFromSortedRegionsTest is used only in unit test
var TestGetSplitSortedKeysFromSortedRegionsTest = getSplitSortedKeysFromSortedRegions

// getSplitSortedKeysFromSortedRegions checks if the sorted regions should be split by the end key of
// the sorted ranges, and groups the split keys by region id.
//
// ASSERT: sortedRegions[0].StartKey <= sortedKeys[0]
func getSplitSortedKeysFromSortedRegions(splitContext SplitContext, sortedKeys [][]byte, sortedRegions []*split.RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	curKeyIndex := 0
	for _, region := range sortedRegions {
		for ; curKeyIndex < len(sortedKeys); curKeyIndex += 1 {
			if len(sortedKeys[curKeyIndex]) == 0 {
				continue
			}
			splitKey := codec.EncodeBytesExt(nil, sortedKeys[curKeyIndex], splitContext.isRawKv)
			// If splitKey is the boundary of the region
			if bytes.Equal(splitKey, region.Region.GetStartKey()) {
				continue
			}
			// If splitKey is not in a region
			if !region.ContainsInterior(splitKey) {
				break
			}
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, sortedKeys[curKeyIndex])
		}
	}
	return splitKeyMap
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

type rewriteSplitter struct {
	rewriteKey []byte
	tableID    int64
	rule       *RewriteRules
	splitter   *split.SplitHelper
}

type splitHelperIterator struct {
	tableSplitters []*rewriteSplitter
}

func (iter *splitHelperIterator) Traverse(fn func(v split.Valued, endKey []byte, rule *RewriteRules) bool) {
	for _, entry := range iter.tableSplitters {
		endKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(entry.tableID+1))
		rule := entry.rule
		entry.splitter.Traverse(func(v split.Valued) bool {
			return fn(v, endKey, rule)
		})
	}
}

func NewSplitHelperIteratorForTest(helper *split.SplitHelper, tableID int64, rule *RewriteRules) *splitHelperIterator {
	return &splitHelperIterator{
		tableSplitters: []*rewriteSplitter{
			{
				tableID:  tableID,
				rule:     rule,
				splitter: helper,
			},
		},
	}
}

type LogSplitHelper struct {
	tableSplitter map[int64]*split.SplitHelper
	rules         map[int64]*RewriteRules
	client        split.SplitClient
	pool          *utils.WorkerPool
	eg            *errgroup.Group
	regionsCh     chan []*split.RegionInfo

	splitThreSholdSize uint64
	splitThreSholdKeys int64
}

func NewLogSplitHelper(rules map[int64]*RewriteRules, client split.SplitClient, splitSize uint64, splitKeys int64) *LogSplitHelper {
	return &LogSplitHelper{
		tableSplitter: make(map[int64]*split.SplitHelper),
		rules:         rules,
		client:        client,
		pool:          utils.NewWorkerPool(128, "split region"),
		eg:            nil,

		splitThreSholdSize: splitSize,
		splitThreSholdKeys: splitKeys,
	}
}

func (helper *LogSplitHelper) iterator() *splitHelperIterator {
	tableSplitters := make([]*rewriteSplitter, 0, len(helper.tableSplitter))
	for tableID, splitter := range helper.tableSplitter {
		delete(helper.tableSplitter, tableID)
		rewriteRule, exists := helper.rules[tableID]
		if !exists {
			log.Info("skip splitting due to no table id matched", zap.Int64("tableID", tableID))
			continue
		}
		newTableID := GetRewriteTableID(tableID, rewriteRule)
		if newTableID == 0 {
			log.Warn("failed to get the rewrite table id", zap.Int64("tableID", tableID))
			continue
		}
		tableSplitters = append(tableSplitters, &rewriteSplitter{
			rewriteKey: codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(newTableID)),
			tableID:    newTableID,
			rule:       rewriteRule,
			splitter:   splitter,
		})
	}
	sort.Slice(tableSplitters, func(i, j int) bool {
		return bytes.Compare(tableSplitters[i].rewriteKey, tableSplitters[j].rewriteKey) < 0
	})
	return &splitHelperIterator{
		tableSplitters: tableSplitters,
	}
}

const splitFileThreshold = 1024 * 1024 // 1 MB

func (helper *LogSplitHelper) skipFile(file *backuppb.DataFileInfo) bool {
	_, exist := helper.rules[file.TableId]
	return file.Length < splitFileThreshold || file.IsMeta || !exist
}

func (helper *LogSplitHelper) Merge(file *backuppb.DataFileInfo) {
	if helper.skipFile(file) {
		return
	}
	splitHelper, exist := helper.tableSplitter[file.TableId]
	if !exist {
		splitHelper = split.NewSplitHelper()
		helper.tableSplitter[file.TableId] = splitHelper
	}

	splitHelper.Merge(split.Valued{
		Key: split.Span{
			StartKey: file.StartKey,
			EndKey:   file.EndKey,
		},
		Value: split.Value{
			Size:   file.Length,
			Number: file.NumberOfEntries,
		},
	})
}

type splitFunc = func(context.Context, *RegionSplitter, uint64, int64, *split.RegionInfo, []split.Valued) error

func (helper *LogSplitHelper) splitRegionByPoints(
	ctx context.Context,
	regionSplitter *RegionSplitter,
	initialLength uint64,
	initialNumber int64,
	region *split.RegionInfo,
	valueds []split.Valued,
) error {
	var (
		splitPoints [][]byte = make([][]byte, 0)
		lastKey     []byte   = region.Region.StartKey
		length      uint64   = initialLength
		number      int64    = initialNumber
	)
	for _, v := range valueds {
		// decode will discard ts behind the key, which results in the same key for consecutive ranges
		if !bytes.Equal(lastKey, v.GetStartKey()) && (v.Value.Size+length > helper.splitThreSholdSize || v.Value.Number+number > helper.splitThreSholdKeys) {
			_, rawKey, _ := codec.DecodeBytes(v.GetStartKey(), nil)
			splitPoints = append(splitPoints, rawKey)
			length = 0
			number = 0
		}
		lastKey = v.GetStartKey()
		length += v.Value.Size
		number += v.Value.Number
	}

	if len(splitPoints) == 0 {
		return nil
	}

	sctx := SplitContext{
		storeCount: 0,
	}

	helper.pool.ApplyOnErrorGroup(helper.eg, func() error {
		newRegions, errSplit := regionSplitter.splitAndScatterRegions(ctx, sctx, region, splitPoints)
		if errSplit != nil {
			log.Warn("failed to split the scaned region", zap.Error(errSplit))
			_, startKey, _ := codec.DecodeBytes(region.Region.StartKey, nil)
			ranges := make([]rtree.Range, 0, len(splitPoints))
			for _, point := range splitPoints {
				ranges = append(ranges, rtree.Range{StartKey: startKey, EndKey: point})
				startKey = point
			}

			return regionSplitter.ExecuteSplit(ctx, ranges, nil, 3, "", false, func([][]byte) {})
		}
		select {
		case <-ctx.Done():
			return nil
		case helper.regionsCh <- newRegions:
		}
		log.Info("split the region", zap.Uint64("region-id", region.Region.Id), zap.Int("split-point-number", len(splitPoints)))
		return nil
	})
	return nil
}

// GetRewriteTableID gets rewrite table id by the rewrite rule and original table id
func GetRewriteTableID(tableID int64, rewriteRules *RewriteRules) int64 {
	tableKey := tablecodec.GenTableRecordPrefix(tableID)
	rule := matchOldPrefix(tableKey, rewriteRules)
	if rule == nil {
		return 0
	}

	return tablecodec.DecodeTableID(rule.GetNewKeyPrefix())
}

// SplitPoint selects ranges overlapped with each region, and calls `splitF` to split the region
func SplitPoint(
	ctx context.Context,
	iter *splitHelperIterator,
	client split.SplitClient,
	splitF splitFunc,
) (err error) {
	// common status
	var (
		regionSplitter *RegionSplitter = NewRegionSplitter(client)
	)
	// region traverse status
	var (
		// the region buffer of each scan
		regions     []*split.RegionInfo = nil
		regionIndex int                 = 0
	)
	// region split status
	var (
		// range span   +----------------+------+---+-------------+
		// region span    +------------------------------------+
		//                +initial length+          +end valued+
		// regionValueds is the ranges array overlapped with `regionInfo`
		regionValueds []split.Valued = nil
		// regionInfo is the region to be split
		regionInfo *split.RegionInfo = nil
		// intialLength is the length of the part of the first range overlapped with the region
		initialLength uint64 = 0
		initialNumber int64  = 0
	)
	// range status
	var (
		// regionOverCount is the number of regions overlapped with the range
		regionOverCount uint64 = 0
	)

	iter.Traverse(func(v split.Valued, endKey []byte, rule *RewriteRules) bool {
		if v.Value.Number == 0 || v.Value.Size == 0 {
			return true
		}
		var (
			vStartKey []byte
			vEndKey   []byte
		)
		// use `vStartKey` and `vEndKey` to compare with region's key
		vStartKey, vEndKey, err = GetRewriteEncodedKeys(v, rule)
		if err != nil {
			return false
		}
		// traverse to the first region overlapped with the range
		for ; regionIndex < len(regions); regionIndex++ {
			if bytes.Compare(vStartKey, regions[regionIndex].Region.EndKey) < 0 {
				break
			}
		}
		// cannot find any regions overlapped with the range
		// need to scan regions again
		if regionIndex == len(regions) {
			regions = nil
		}
		regionOverCount = 0
		for {
			if regionIndex >= len(regions) {
				var startKey []byte
				if len(regions) > 0 {
					// has traversed over the region buffer, should scan from the last region's end-key of the region buffer
					startKey = regions[len(regions)-1].Region.EndKey
				} else {
					// scan from the range's start-key
					startKey = vStartKey
				}
				// scan at most 64 regions into the region buffer
				regions, err = split.ScanRegionsWithRetry(ctx, client, startKey, endKey, 64)
				if err != nil {
					return false
				}
				regionIndex = 0
			}

			region := regions[regionIndex]
			// this region must be overlapped with the range
			regionOverCount++
			// the region is the last one overlapped with the range,
			// should split the last recorded region,
			// and then record this region as the region to be split
			if bytes.Compare(vEndKey, region.Region.EndKey) < 0 {
				endLength := v.Value.Size / regionOverCount
				endNumber := v.Value.Number / int64(regionOverCount)
				if len(regionValueds) > 0 && regionInfo != region {
					// add a part of the range as the end part
					if bytes.Compare(vStartKey, regionInfo.Region.EndKey) < 0 {
						regionValueds = append(regionValueds, split.NewValued(vStartKey, regionInfo.Region.EndKey, split.Value{Size: endLength, Number: endNumber}))
					}
					// try to split the region
					err = splitF(ctx, regionSplitter, initialLength, initialNumber, regionInfo, regionValueds)
					if err != nil {
						return false
					}
					regionValueds = make([]split.Valued, 0)
				}
				if regionOverCount == 1 {
					// the region completely contains the range
					regionValueds = append(regionValueds, split.Valued{
						Key: split.Span{
							StartKey: vStartKey,
							EndKey:   vEndKey,
						},
						Value: v.Value,
					})
				} else {
					// the region is overlapped with the last part of the range
					initialLength = endLength
					initialNumber = endNumber
				}
				regionInfo = region
				// try the next range
				return true
			}

			// try the next region
			regionIndex++
		}
	})

	if err != nil {
		return errors.Trace(err)
	}
	if len(regionValueds) > 0 {
		// try to split the region
		err = splitF(ctx, regionSplitter, initialLength, initialNumber, regionInfo, regionValueds)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (helper *LogSplitHelper) Split(ctx context.Context) error {
	var ectx context.Context
	var wg sync.WaitGroup
	helper.eg, ectx = errgroup.WithContext(ctx)
	helper.regionsCh = make(chan []*split.RegionInfo, 1024)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scatterRegions := make([]*split.RegionInfo, 0)
	receiveNewRegions:
		for {
			select {
			case <-ectx.Done():
				return
			case newRegions, ok := <-helper.regionsCh:
				if !ok {
					break receiveNewRegions
				}

				scatterRegions = append(scatterRegions, newRegions...)
			}
		}

		regionSplitter := NewRegionSplitter(helper.client)
		// It is too expensive to stop recovery and wait for a small number of regions
		// to complete scatter, so the maximum waiting time is reduced to 1 minute.
		_ = regionSplitter.WaitForScatterRegionsTimeout(ctx, scatterRegions, time.Minute)
	}()

	iter := helper.iterator()
	if err := SplitPoint(ectx, iter, helper.client, helper.splitRegionByPoints); err != nil {
		return errors.Trace(err)
	}

	// wait for completion of splitting regions
	if err := helper.eg.Wait(); err != nil {
		return errors.Trace(err)
	}

	// wait for completion of scattering regions
	close(helper.regionsCh)
	wg.Wait()

	return nil
}

type LogFilesIterWithSplitHelper struct {
	iter   LogIter
	helper *LogSplitHelper
	buffer []*LogDataFileInfo
	next   int
}

const SplitFilesBufferSize = 4096

func NewLogFilesIterWithSplitHelper(iter LogIter, rules map[int64]*RewriteRules, client split.SplitClient, splitSize uint64, splitKeys int64) LogIter {
	return &LogFilesIterWithSplitHelper{
		iter:   iter,
		helper: NewLogSplitHelper(rules, client, splitSize, splitKeys),
		buffer: nil,
		next:   0,
	}
}

func (splitIter *LogFilesIterWithSplitHelper) TryNext(ctx context.Context) iter.IterResult[*LogDataFileInfo] {
	if splitIter.next >= len(splitIter.buffer) {
		splitIter.buffer = make([]*LogDataFileInfo, 0, SplitFilesBufferSize)
		for r := splitIter.iter.TryNext(ctx); !r.Finished; r = splitIter.iter.TryNext(ctx) {
			if r.Err != nil {
				return r
			}
			f := r.Item
			splitIter.helper.Merge(f.DataFileInfo)
			splitIter.buffer = append(splitIter.buffer, f)
			if len(splitIter.buffer) >= SplitFilesBufferSize {
				break
			}
		}
		splitIter.next = 0
		if len(splitIter.buffer) == 0 {
			return iter.Done[*LogDataFileInfo]()
		}
		log.Info("start to split the regions")
		startTime := time.Now()
		if err := splitIter.helper.Split(ctx); err != nil {
			return iter.Throw[*LogDataFileInfo](errors.Trace(err))
		}
		log.Info("end to split the regions", zap.Duration("takes", time.Since(startTime)))
	}

	res := iter.Emit(splitIter.buffer[splitIter.next])
	splitIter.next += 1
	return res
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
