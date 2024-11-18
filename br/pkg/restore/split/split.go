// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"context"
	"encoding/hex"
	goerrors "errors"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	WaitRegionOnlineAttemptTimes = config.DefaultRegionCheckBackoffLimit
	SplitRetryTimes              = 150
)

// Constants for split retry machinery.
const (
	SplitRetryInterval    = 50 * time.Millisecond
	SplitMaxRetryInterval = 4 * time.Second

	// it takes 30 minutes to scatter regions when each TiKV has 400k regions
	ScatterWaitUpperInterval = 30 * time.Minute

	ScanRegionPaginationLimit = 128
)

type rewriteSplitter struct {
	rewriteKey []byte
	tableID    int64
	rule       *restoreutils.RewriteRules
	splitter   *SplitHelper
}

type splitHelperIterator struct {
	tableSplitters []*rewriteSplitter
}

func (iter *splitHelperIterator) Traverse(fn func(v Valued, endKey []byte, rule *restoreutils.RewriteRules) bool) {
	for _, entry := range iter.tableSplitters {
		endKey := codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(entry.tableID+1))
		rule := entry.rule
		entry.splitter.Traverse(func(v Valued) bool {
			return fn(v, endKey, rule)
		})
	}
}

type LogSplitHelper struct {
	tableSplitter map[int64]*SplitHelper
	rules         map[int64]*restoreutils.RewriteRules
	client        SplitClient
	pool          *util.WorkerPool
	eg            *errgroup.Group
	regionsCh     chan []*RegionInfo

	splitThresholdSize uint64
	splitThresholdKeys int64
}

func NewLogSplitHelper(rules map[int64]*restoreutils.RewriteRules, client SplitClient, splitSize uint64, splitKeys int64) *LogSplitHelper {
	return &LogSplitHelper{
		tableSplitter: make(map[int64]*SplitHelper),
		rules:         rules,
		client:        client,
		pool:          util.NewWorkerPool(128, "split region"),
		eg:            nil,

		splitThresholdSize: splitSize,
		splitThresholdKeys: splitKeys,
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
		newTableID := restoreutils.GetRewriteTableID(tableID, rewriteRule)
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
		splitHelper = NewSplitHelper()
		helper.tableSplitter[file.TableId] = splitHelper
	}

	splitHelper.Merge(Valued{
		Key: Span{
			StartKey: file.StartKey,
			EndKey:   file.EndKey,
		},
		Value: Value{
			Size:   file.Length,
			Number: file.NumberOfEntries,
		},
	})
}

type splitFunc = func(context.Context, *RegionSplitter, uint64, int64, *RegionInfo, []Valued) error

func (helper *LogSplitHelper) splitRegionByPoints(
	ctx context.Context,
	regionSplitter *RegionSplitter,
	initialLength uint64,
	initialNumber int64,
	region *RegionInfo,
	valueds []Valued,
) error {
	var (
		splitPoints [][]byte = make([][]byte, 0)
		lastKey     []byte   = region.Region.StartKey
		length      uint64   = initialLength
		number      int64    = initialNumber
	)
	for _, v := range valueds {
		// decode will discard ts behind the key, which results in the same key for consecutive ranges
		if !bytes.Equal(lastKey, v.GetStartKey()) && (v.Value.Size+length > helper.splitThresholdSize || v.Value.Number+number > helper.splitThresholdKeys) {
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

	helper.pool.ApplyOnErrorGroup(helper.eg, func() error {
		newRegions, errSplit := regionSplitter.ExecuteOneRegion(ctx, region, splitPoints)
		if errSplit != nil {
			log.Warn("failed to split the scaned region", zap.Error(errSplit))
			sort.Slice(splitPoints, func(i, j int) bool {
				return bytes.Compare(splitPoints[i], splitPoints[j]) < 0
			})
			return regionSplitter.ExecuteSortedKeys(ctx, splitPoints)
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

// SplitPoint selects ranges overlapped with each region, and calls `splitF` to split the region
func SplitPoint(
	ctx context.Context,
	iter *splitHelperIterator,
	client SplitClient,
	splitF splitFunc,
) (err error) {
	// common status
	var (
		regionSplitter *RegionSplitter = NewRegionSplitter(client)
	)
	// region traverse status
	var (
		// the region buffer of each scan
		regions     []*RegionInfo = nil
		regionIndex int           = 0
	)
	// region split status
	var (
		// range span   +----------------+------+---+-------------+
		// region span    +------------------------------------+
		//                +initial length+          +end valued+
		// regionValueds is the ranges array overlapped with `regionInfo`
		regionValueds []Valued = nil
		// regionInfo is the region to be split
		regionInfo *RegionInfo = nil
		// intialLength is the length of the part of the first range overlapped with the region
		initialLength uint64 = 0
		initialNumber int64  = 0
	)
	// range status
	var (
		// regionOverCount is the number of regions overlapped with the range
		regionOverCount uint64 = 0
	)

	iter.Traverse(func(v Valued, endKey []byte, rule *restoreutils.RewriteRules) bool {
		if v.Value.Number == 0 || v.Value.Size == 0 {
			return true
		}
		var (
			vStartKey []byte
			vEndKey   []byte
		)
		// use `vStartKey` and `vEndKey` to compare with region's key
		vStartKey, vEndKey, err = restoreutils.GetRewriteEncodedKeys(v, rule)
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
				regions, err = ScanRegionsWithRetry(ctx, client, startKey, endKey, 64)
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
						regionValueds = append(regionValueds, NewValued(vStartKey, regionInfo.Region.EndKey, Value{Size: endLength, Number: endNumber}))
					}
					// try to split the region
					err = splitF(ctx, regionSplitter, initialLength, initialNumber, regionInfo, regionValueds)
					if err != nil {
						return false
					}
					regionValueds = make([]Valued, 0)
				}
				if regionOverCount == 1 {
					// the region completely contains the range
					regionValueds = append(regionValueds, Valued{
						Key: Span{
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
	helper.regionsCh = make(chan []*RegionInfo, 1024)
	wg.Add(1)
	go func() {
		defer wg.Done()
		scatterRegions := make([]*RegionInfo, 0)
	receiveNewRegions:
		for {
			select {
			case <-ctx.Done():
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

// ExecuteOneRegion expose the function `SplitWaitAndScatter` of split client.
func (rs *RegionSplitter) ExecuteOneRegion(ctx context.Context, region *RegionInfo, keys [][]byte) ([]*RegionInfo, error) {
	return rs.client.SplitWaitAndScatter(ctx, region, keys)
}

// ExecuteSortedKeys executes regions split and make sure new splitted regions are balance.
// It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) ExecuteSortedKeys(
	ctx context.Context,
	sortedSplitKeys [][]byte,
) error {
	if len(sortedSplitKeys) == 0 {
		log.Info("skip split regions, no split keys")
		return nil
	}

	log.Info("execute split sorted keys", zap.Int("keys count", len(sortedSplitKeys)))
	return rs.executeSplitByRanges(ctx, sortedSplitKeys)
}

func (rs *RegionSplitter) executeSplitByRanges(
	ctx context.Context,
	sortedKeys [][]byte,
) error {
	startTime := time.Now()
	// Choose the rough region split keys,
	// each splited region contains 128 regions to be splitted.
	const regionIndexStep = 128

	roughSortedSplitKeys := make([][]byte, 0, len(sortedKeys)/regionIndexStep+1)
	for curRegionIndex := regionIndexStep; curRegionIndex < len(sortedKeys); curRegionIndex += regionIndexStep {
		roughSortedSplitKeys = append(roughSortedSplitKeys, sortedKeys[curRegionIndex])
	}
	if len(roughSortedSplitKeys) > 0 {
		if err := rs.executeSplitByKeys(ctx, roughSortedSplitKeys); err != nil {
			return errors.Trace(err)
		}
	}
	log.Info("finish spliting regions roughly", zap.Duration("take", time.Since(startTime)))

	// Then send split requests to each TiKV.
	if err := rs.executeSplitByKeys(ctx, sortedKeys); err != nil {
		return errors.Trace(err)
	}

	log.Info("finish spliting and scattering regions", zap.Duration("take", time.Since(startTime)))
	return nil
}

// executeSplitByKeys will split regions by **sorted** keys with following steps.
// 1. locate regions with correspond keys.
// 2. split these regions with correspond keys.
// 3. make sure new split regions are balanced.
func (rs *RegionSplitter) executeSplitByKeys(
	ctx context.Context,
	sortedKeys [][]byte,
) error {
	startTime := time.Now()
	scatterRegions, err := rs.client.SplitKeysAndScatter(ctx, sortedKeys)
	if err != nil {
		return errors.Trace(err)
	}
	if len(scatterRegions) > 0 {
		log.Info("finish splitting and scattering regions. and starts to wait", zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
		rs.waitRegionsScattered(ctx, scatterRegions, ScatterWaitUpperInterval)
	} else {
		log.Info("finish splitting regions.", zap.Duration("take", time.Since(startTime)))
	}
	return nil
}

// waitRegionsScattered try to wait mutilple regions scatterd in 3 minutes.
// this could timeout, but if many regions scatterd the restore could continue
// so we don't wait long time here.
func (rs *RegionSplitter) waitRegionsScattered(ctx context.Context, scatterRegions []*RegionInfo, timeout time.Duration) {
	log.Info("start to wait for scattering regions", zap.Int("regions", len(scatterRegions)))
	startTime := time.Now()
	leftCnt := rs.WaitForScatterRegionsTimeout(ctx, scatterRegions, timeout)
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

func (rs *RegionSplitter) WaitForScatterRegionsTimeout(ctx context.Context, regionInfos []*RegionInfo, timeout time.Duration) int {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	leftRegions, _ := rs.client.WaitRegionsScattered(ctx2, regionInfos)
	return leftRegions
}

func checkRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion, "scan region return empty result, startKey: %s, endKey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"first region %d's startKey(%s) > startKey(%s), region epoch: %s",
			regions[0].Region.Id,
			redact.Key(regions[0].Region.StartKey), redact.Key(startKey),
			regions[0].Region.RegionEpoch.String())
	} else if len(regions[len(regions)-1].Region.EndKey) != 0 &&
		bytes.Compare(regions[len(regions)-1].Region.EndKey, endKey) < 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"last region %d's endKey(%s) < endKey(%s), region epoch: %s",
			regions[len(regions)-1].Region.Id,
			redact.Key(regions[len(regions)-1].Region.EndKey), redact.Key(endKey),
			regions[len(regions)-1].Region.RegionEpoch.String())
	}

	cur := regions[0]
	if cur.Leader == nil {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"region %d's leader is nil", cur.Region.Id)
	}
	if cur.Leader.StoreId == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"region %d's leader's store id is 0", cur.Region.Id)
	}
	for _, r := range regions[1:] {
		if r.Leader == nil {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion,
				"region %d's leader is nil", r.Region.Id)
		}
		if r.Leader.StoreId == 0 {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion,
				"region %d's leader's store id is 0", r.Region.Id)
		}
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion,
				"region %d's endKey not equal to next region %d's startKey, endKey: %s, startKey: %s, region epoch: %s %s",
				cur.Region.Id, r.Region.Id,
				redact.Key(cur.Region.EndKey), redact.Key(r.Region.StartKey),
				cur.Region.RegionEpoch.String(), r.Region.RegionEpoch.String())
		}
		cur = r
	}

	return nil
}

// PaginateScanRegion scan regions with a limit pagination and return all regions
// at once. The returned regions are continuous and cover the key range. If not,
// or meet errors, it will retry internally.
func PaginateScanRegion(
	ctx context.Context, client SplitClient, startKey, endKey []byte, limit int,
) ([]*RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.Annotatef(berrors.ErrInvalidRange, "startKey > endKey, startKey: %s, endkey: %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	var (
		lastRegions []*RegionInfo
		err         error
		backoffer   = NewWaitRegionOnlineBackoffer()
	)
	_ = utils.WithRetry(ctx, func() error {
		regions := make([]*RegionInfo, 0, 16)
		scanStartKey := startKey
		for {
			var batch []*RegionInfo
			batch, err = client.ScanRegions(ctx, scanStartKey, endKey, limit)
			if err != nil {
				err = errors.Annotatef(berrors.ErrPDBatchScanRegion.Wrap(err), "scan regions from start-key:%s, err: %s",
					redact.Key(scanStartKey), err.Error())
				return err
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
		// if the number of regions changed, we can infer TiKV side really
		// made some progress so don't increase the retry times.
		if len(regions) != len(lastRegions) {
			backoffer.Stat.ReduceRetry()
		}
		lastRegions = regions

		if err = checkRegionConsistency(startKey, endKey, regions); err != nil {
			log.Warn("failed to scan region, retrying",
				logutil.ShortError(err),
				zap.Int("regionLength", len(regions)))
			return err
		}
		return nil
	}, backoffer)

	return lastRegions, err
}

// checkPartRegionConsistency only checks the continuity of regions and the first region consistency.
func checkPartRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"scan region return empty result, startKey: %s, endKey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(berrors.ErrPDBatchScanRegion,
			"first region's startKey > startKey, startKey: %s, regionStartKey: %s",
			redact.Key(startKey), redact.Key(regions[0].Region.StartKey))
	}

	cur := regions[0]
	for _, r := range regions[1:] {
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(berrors.ErrPDBatchScanRegion,
				"region endKey not equal to next region startKey, endKey: %s, startKey: %s",
				redact.Key(cur.Region.EndKey), redact.Key(r.Region.StartKey))
		}
		cur = r
	}

	return nil
}

func ScanRegionsWithRetry(
	ctx context.Context, client SplitClient, startKey, endKey []byte, limit int,
) ([]*RegionInfo, error) {
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) > 0 {
		return nil, errors.Annotatef(berrors.ErrInvalidRange, "startKey > endKey, startKey: %s, endkey: %s",
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
		regions, err = client.ScanRegions(ctx, startKey, endKey, limit)
		if err != nil {
			err = errors.Annotatef(berrors.ErrPDBatchScanRegion, "scan regions from start-key:%s, err: %s",
				redact.Key(startKey), err.Error())
			return err
		}

		if err = checkPartRegionConsistency(startKey, endKey, regions); err != nil {
			log.Warn("failed to scan region, retrying", logutil.ShortError(err))
			return err
		}

		return nil
	}, NewWaitRegionOnlineBackoffer())

	return regions, err
}

type WaitRegionOnlineBackoffer struct {
	Stat utils.RetryState
}

// NewWaitRegionOnlineBackoffer create a backoff to wait region online.
func NewWaitRegionOnlineBackoffer() *WaitRegionOnlineBackoffer {
	return &WaitRegionOnlineBackoffer{
		Stat: utils.InitialRetryState(
			WaitRegionOnlineAttemptTimes,
			time.Millisecond*10,
			time.Second*2,
		),
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *WaitRegionOnlineBackoffer) NextBackoff(err error) time.Duration {
	// TODO(lance6716): why we only backoff when the error is ErrPDBatchScanRegion?
	var perr *errors.Error
	if goerrors.As(err, &perr) && berrors.ErrPDBatchScanRegion.ID() == perr.ID() {
		// it needs more time to wait splitting the regions that contains data in PITR.
		// 2s * 150
		delayTime := b.Stat.ExponentialBackoff()
		failpoint.Inject("hint-scan-region-backoff", func(val failpoint.Value) {
			if val.(bool) {
				delayTime = time.Microsecond
			}
		})
		return delayTime
	}
	b.Stat.GiveUp()
	return 0
}

// Attempt returns the remain attempt times
func (b *WaitRegionOnlineBackoffer) Attempt() int {
	return b.Stat.Attempt()
}

// BackoffMayNotCountBackoffer is a backoffer but it may not increase the retry
// counter. It should be used with ErrBackoff or ErrBackoffAndDontCount.
type BackoffMayNotCountBackoffer struct {
	state utils.RetryState
}

var (
	ErrBackoff             = errors.New("found backoff error")
	ErrBackoffAndDontCount = errors.New("found backoff error but don't count")
)

// NewBackoffMayNotCountBackoffer creates a new backoffer that may backoff or retry.
//
// TODO: currently it has the same usage as NewWaitRegionOnlineBackoffer so we
// don't expose its inner settings.
func NewBackoffMayNotCountBackoffer() *BackoffMayNotCountBackoffer {
	return &BackoffMayNotCountBackoffer{
		state: utils.InitialRetryState(
			WaitRegionOnlineAttemptTimes,
			time.Millisecond*10,
			time.Second*2,
		),
	}
}

// NextBackoff implements utils.Backoffer. For BackoffMayNotCountBackoffer, only
// ErrBackoff and ErrBackoffAndDontCount is meaningful.
func (b *BackoffMayNotCountBackoffer) NextBackoff(err error) time.Duration {
	if errors.ErrorEqual(err, ErrBackoff) {
		return b.state.ExponentialBackoff()
	}
	if errors.ErrorEqual(err, ErrBackoffAndDontCount) {
		delay := b.state.ExponentialBackoff()
		b.state.ReduceRetry()
		return delay
	}
	b.state.GiveUp()
	return 0
}

// Attempt implements utils.Backoffer.
func (b *BackoffMayNotCountBackoffer) Attempt() int {
	return b.state.Attempt()
}

// getSplitKeysOfRegions checks every input key is necessary to split region on
// it. Returns a map from region to split keys belongs to it.
//
// The key will be skipped if it's the region boundary.
//
// prerequisite:
// - sortedKeys are sorted in ascending order.
// - sortedRegions are continuous and sorted in ascending order by start key.
// - sortedRegions can cover all keys in sortedKeys.
// PaginateScanRegion should satisfy the above prerequisites.
func getSplitKeysOfRegions(
	sortedKeys [][]byte,
	sortedRegions []*RegionInfo,
	isRawKV bool,
) map[*RegionInfo][][]byte {
	splitKeyMap := make(map[*RegionInfo][][]byte, len(sortedRegions))
	curKeyIndex := 0
	splitKey := codec.EncodeBytesExt(nil, sortedKeys[curKeyIndex], isRawKV)

	for _, region := range sortedRegions {
		for {
			if len(sortedKeys[curKeyIndex]) == 0 {
				// should not happen?
				goto nextKey
			}
			// If splitKey is the boundary of the region, don't need to split on it.
			if bytes.Equal(splitKey, region.Region.GetStartKey()) {
				goto nextKey
			}
			// If splitKey is not in this region, we should move to the next region.
			if !region.ContainsInterior(splitKey) {
				break
			}

			splitKeyMap[region] = append(splitKeyMap[region], sortedKeys[curKeyIndex])

		nextKey:
			curKeyIndex++
			if curKeyIndex >= len(sortedKeys) {
				return splitKeyMap
			}
			splitKey = codec.EncodeBytesExt(nil, sortedKeys[curKeyIndex], isRawKV)
		}
	}
	lastKey := sortedKeys[len(sortedKeys)-1]
	endOfLastRegion := sortedRegions[len(sortedRegions)-1].Region.GetEndKey()
	if !bytes.Equal(lastKey, endOfLastRegion) {
		log.Error("in getSplitKeysOfRegions, regions don't cover all keys",
			zap.String("firstKey", hex.EncodeToString(sortedKeys[0])),
			zap.String("lastKey", hex.EncodeToString(lastKey)),
			zap.String("firstRegionStartKey", hex.EncodeToString(sortedRegions[0].Region.GetStartKey())),
			zap.String("lastRegionEndKey", hex.EncodeToString(endOfLastRegion)),
		)
	}
	return splitKeyMap
}
