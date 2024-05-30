// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"context"
	"encoding/hex"
	goerrors "errors"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
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

// CheckPartRegionConsistency only checks the continuity of regions and the first region consistency.
func CheckPartRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
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

		if err = CheckPartRegionConsistency(startKey, endKey, regions); err != nil {
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
