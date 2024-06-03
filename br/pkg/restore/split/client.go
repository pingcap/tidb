// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package split

import (
	"bytes"
	"context"
	"crypto/tls"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	brlog "github.com/pingcap/tidb/pkg/lightning/log"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/intest"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	splitRegionMaxRetryTime = 4
)

var (
	// the max total key size in a split region batch.
	// our threshold should be smaller than TiKV's raft max entry size(default is 8MB).
	maxBatchSplitSize = 6 * units.MiB
)

// SplitClient is an external client used by RegionSplitter.
type SplitClient interface {
	// GetStore gets a store by a store id.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetRegion gets a region which includes a specified key.
	GetRegion(ctx context.Context, key []byte) (*RegionInfo, error)
	// GetRegionByID gets a region by a region id.
	GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)
	// SplitKeysAndScatter splits the related regions of the keys and scatters the
	// new regions. It returns the new regions that need to be called with
	// WaitRegionsScattered.
	SplitKeysAndScatter(ctx context.Context, sortedSplitKeys [][]byte) ([]*RegionInfo, error)

	// SplitWaitAndScatter splits a region from a batch of keys, waits for the split
	// is finished, and scatters the new regions. It will return the original region,
	// new regions and error. The input keys should not be encoded.
	//
	// The split step has a few retry times. If it meets error, the error is returned
	// directly.
	//
	// The split waiting step has a backoff retry logic, if split has made progress,
	// it will not increase the retry counter. Otherwise, it will retry for about 1h.
	// If the retry is timeout, it will log a warning and continue.
	//
	// The scatter step has a few retry times. If it meets error, it will log a
	// warning and continue.
	// TODO(lance6716): remove this function in interface after BR uses SplitKeysAndScatter.
	SplitWaitAndScatter(ctx context.Context, region *RegionInfo, keys [][]byte) ([]*RegionInfo, error)
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// ScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error)
	// GetPlacementRule loads a placement rule from PD.
	GetPlacementRule(ctx context.Context, groupID, ruleID string) (*pdhttp.Rule, error)
	// SetPlacementRule insert or update a placement rule to PD.
	SetPlacementRule(ctx context.Context, rule *pdhttp.Rule) error
	// DeletePlacementRule removes a placement rule from PD.
	DeletePlacementRule(ctx context.Context, groupID, ruleID string) error
	// SetStoresLabel add or update specified label of stores. If labelValue
	// is empty, it clears the label.
	SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error
	// WaitRegionsScattered waits for an already started scatter region action to
	// finish. Internally it will backoff and retry at the maximum internal of 2
	// seconds. If the scatter makes progress during the retry, it will not decrease
	// the retry counter. If there's always no progress, it will retry for about 1h.
	// Caller can set the context timeout to control the max waiting time.
	//
	// The first return value is always the number of regions that are not finished
	// scattering no matter what the error is.
	WaitRegionsScattered(ctx context.Context, regionInfos []*RegionInfo) (notFinished int, err error)
}

// pdClient is a wrapper of pd client, can be used by RegionSplitter.
type pdClient struct {
	mu         sync.Mutex
	client     pd.Client
	httpCli    pdhttp.Client
	tlsConf    *tls.Config
	storeCache map[uint64]*metapb.Store

	// FIXME when config changed during the lifetime of pdClient,
	// 	this may mislead the scatter.
	needScatterVal  bool
	needScatterInit sync.Once

	isRawKv          bool
	onSplit          func(key [][]byte)
	splitConcurrency int
	splitBatchKeyCnt int
}

type ClientOptionalParameter func(*pdClient)

// WithRawKV sets the client to use raw kv mode.
func WithRawKV() ClientOptionalParameter {
	return func(c *pdClient) {
		c.isRawKv = true
	}
}

// WithOnSplit sets a callback function to be called after each split.
func WithOnSplit(onSplit func(key [][]byte)) ClientOptionalParameter {
	return func(c *pdClient) {
		c.onSplit = onSplit
	}
}

// NewClient creates a SplitClient.
//
// splitBatchKeyCnt controls how many keys are sent to TiKV in a batch in split
// region API. splitConcurrency controls how many regions are split concurrently.
func NewClient(
	client pd.Client,
	httpCli pdhttp.Client,
	tlsConf *tls.Config,
	splitBatchKeyCnt int,
	splitConcurrency int,
	opts ...ClientOptionalParameter,
) SplitClient {
	cli := &pdClient{
		client:           client,
		httpCli:          httpCli,
		tlsConf:          tlsConf,
		storeCache:       make(map[uint64]*metapb.Store),
		splitBatchKeyCnt: splitBatchKeyCnt,
		splitConcurrency: splitConcurrency,
	}
	for _, opt := range opts {
		opt(cli)
	}
	return cli
}

func (c *pdClient) needScatter(ctx context.Context) bool {
	c.needScatterInit.Do(func() {
		var err error
		c.needScatterVal, err = c.checkNeedScatter(ctx)
		if err != nil {
			log.Warn(
				"failed to check whether need to scatter, use permissive strategy: always scatter",
				logutil.ShortError(err))
			c.needScatterVal = true
		}
		if !c.needScatterVal {
			log.Info("skipping scatter because the replica number isn't less than store count.")
		}
	})
	return c.needScatterVal
}

func (c *pdClient) scatterRegions(ctx context.Context, newRegions []*RegionInfo) error {
	log.Info("scatter regions", zap.Int("regions", len(newRegions)))
	// the retry is for the temporary network errors during sending request.
	return utils.WithRetry(ctx, func() error {
		err := c.tryScatterRegions(ctx, newRegions)
		if isUnsupportedError(err) {
			log.Warn("batch scatter isn't supported, rollback to old method", logutil.ShortError(err))
			c.scatterRegionsSequentially(
				ctx, newRegions,
				// backoff about 6s, or we give up scattering this region.
				&ExponentialBackoffer{
					Attempts:    7,
					BaseBackoff: 100 * time.Millisecond,
				})
			return nil
		}
		return err
	}, &ExponentialBackoffer{Attempts: 3, BaseBackoff: 500 * time.Millisecond})
}

func (c *pdClient) tryScatterRegions(ctx context.Context, regionInfo []*RegionInfo) error {
	regionsID := make([]uint64, 0, len(regionInfo))
	for _, v := range regionInfo {
		regionsID = append(regionsID, v.Region.Id)
		log.Debug("scattering regions", logutil.Key("start", v.Region.StartKey),
			logutil.Key("end", v.Region.EndKey),
			zap.Uint64("id", v.Region.Id))
	}
	resp, err := c.client.ScatterRegions(ctx, regionsID, pd.WithSkipStoreLimit())
	if err != nil {
		return err
	}
	if pbErr := resp.GetHeader().GetError(); pbErr.GetType() != pdpb.ErrorType_OK {
		return errors.Annotatef(berrors.ErrPDInvalidResponse,
			"pd returns error during batch scattering: %s", pbErr)
	}
	return nil
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.storeCache[storeID]
	if ok {
		return store, nil
	}
	store, err := c.client.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.storeCache[storeID] = store
	return store, nil
}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	region, err := c.client.GetRegion(ctx, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if region == nil {
		return nil, nil
	}
	return &RegionInfo{
		Region: region.Meta,
		Leader: region.Leader,
	}, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	region, err := c.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if region == nil {
		return nil, nil
	}
	return &RegionInfo{
		Region:       region.Meta,
		Leader:       region.Leader,
		PendingPeers: region.PendingPeers,
		DownPeers:    region.DownPeers,
	}, nil
}

func splitRegionWithFailpoint(
	ctx context.Context,
	regionInfo *RegionInfo,
	peer *metapb.Peer,
	client tikvpb.TikvClient,
	keys [][]byte,
	isRawKv bool,
) (*kvrpcpb.SplitRegionResponse, error) {
	failpoint.Inject("not-leader-error", func(injectNewLeader failpoint.Value) {
		log.Debug("failpoint not-leader-error injected.")
		resp := &kvrpcpb.SplitRegionResponse{
			RegionError: &errorpb.Error{
				NotLeader: &errorpb.NotLeader{
					RegionId: regionInfo.Region.Id,
				},
			},
		}
		if injectNewLeader.(bool) {
			resp.RegionError.NotLeader.Leader = regionInfo.Leader
		}
		failpoint.Return(resp, nil)
	})
	failpoint.Inject("somewhat-retryable-error", func() {
		log.Debug("failpoint somewhat-retryable-error injected.")
		failpoint.Return(&kvrpcpb.SplitRegionResponse{
			RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			},
		}, nil)
	})
	return client.SplitRegion(ctx, &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
		},
		SplitKeys: keys,
		IsRawKv:   isRawKv,
	})
}

func (c *pdClient) sendSplitRegionRequest(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) (*kvrpcpb.SplitRegionResponse, error) {
	var splitErrors error
	for i := 0; i < splitRegionMaxRetryTime; i++ {
		retry, result, err := sendSplitRegionRequest(ctx, c, regionInfo, keys, &splitErrors, i)
		if retry {
			continue
		}
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		if result != nil {
			return result, nil
		}
		return nil, errors.Trace(splitErrors)
	}
	return nil, errors.Trace(splitErrors)
}

func sendSplitRegionRequest(
	ctx context.Context,
	c *pdClient,
	regionInfo *RegionInfo,
	keys [][]byte,
	splitErrors *error,
	retry int,
) (bool, *kvrpcpb.SplitRegionResponse, error) {
	if intest.InTest {
		mockCli, ok := c.client.(*MockPDClientForSplit)
		if ok {
			return mockCli.SplitRegion(regionInfo, keys, c.isRawKv)
		}
	}
	var peer *metapb.Peer
	// scanRegions may return empty Leader in https://github.com/tikv/pd/blob/v4.0.8/server/grpc_service.go#L524
	// so wee also need check Leader.Id != 0
	if regionInfo.Leader != nil && regionInfo.Leader.Id != 0 {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return false, nil,
				errors.Annotatef(berrors.ErrRestoreNoPeer, "region[%d] doesn't have any peer",
					regionInfo.Region.GetId())
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := c.GetStore(ctx, storeID)
	if err != nil {
		return false, nil, err
	}
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if c.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConf))
	}
	conn, err := grpc.Dial(store.GetAddress(), opt,
		config.DefaultGrpcKeepaliveParams)
	if err != nil {
		return false, nil, err
	}
	defer conn.Close()
	client := tikvpb.NewTikvClient(conn)
	resp, err := splitRegionWithFailpoint(ctx, regionInfo, peer, client, keys, c.isRawKv)
	if err != nil {
		return false, nil, err
	}
	if resp.RegionError != nil {
		log.Warn("fail to split region",
			logutil.Region(regionInfo.Region),
			logutil.Keys(keys),
			zap.Stringer("regionErr", resp.RegionError))
		*splitErrors = multierr.Append(*splitErrors,
			errors.Annotatef(berrors.ErrRestoreSplitFailed, "split region failed: err=%v", resp.RegionError))
		if nl := resp.RegionError.NotLeader; nl != nil {
			if leader := nl.GetLeader(); leader != nil {
				regionInfo.Leader = leader
			} else {
				newRegionInfo, findLeaderErr := c.GetRegionByID(ctx, nl.RegionId)
				if findLeaderErr != nil {
					return false, nil, findLeaderErr
				}
				if !CheckRegionEpoch(newRegionInfo, regionInfo) {
					return false, nil, berrors.ErrKVEpochNotMatch
				}
				log.Info("find new leader", zap.Uint64("new leader", newRegionInfo.Leader.Id))
				regionInfo = newRegionInfo
			}
			log.Info("split region meet not leader error, retrying",
				zap.Int("retry times", retry),
				zap.Uint64("regionID", regionInfo.Region.Id),
				zap.Any("new leader", regionInfo.Leader),
			)
			return true, nil, nil
		}
		// TODO: we don't handle RegionNotMatch and RegionNotFound here,
		// because I think we don't have enough information to retry.
		// But maybe we can handle them here by some information the error itself provides.
		if resp.RegionError.ServerIsBusy != nil ||
			resp.RegionError.StaleCommand != nil {
			log.Warn("a error occurs on split region",
				zap.Int("retry times", retry),
				zap.Uint64("regionID", regionInfo.Region.Id),
				zap.String("error", resp.RegionError.Message),
				zap.Any("error verbose", resp.RegionError),
			)
			return true, nil, nil
		}
		return false, nil, nil
	}
	return false, resp, nil
}

// batchSplitRegionsWithOrigin calls the batch split region API and groups the
// returned regions into two groups: the region with the same ID as the origin,
// and the other regions. The former does not need to be scattered while the
// latter need to be scattered.
//
// Depending on the TiKV configuration right-derive-when-split, the origin region
// can be the first return region or the last return region.
func (c *pdClient) batchSplitRegionsWithOrigin(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) (*RegionInfo, []*RegionInfo, error) {
	resp, err := c.sendSplitRegionRequest(ctx, regionInfo, keys)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	regions := resp.GetRegions()
	newRegionInfos := make([]*RegionInfo, 0, len(regions))
	var originRegion *RegionInfo
	for _, region := range regions {
		var leader *metapb.Peer

		// Assume the leaders will be at the same store.
		if regionInfo.Leader != nil {
			for _, p := range region.GetPeers() {
				if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
					leader = p
					break
				}
			}
		}
		// original region
		if region.GetId() == regionInfo.Region.GetId() {
			originRegion = &RegionInfo{
				Region: region,
				Leader: leader,
			}
			continue
		}
		newRegionInfos = append(newRegionInfos, &RegionInfo{
			Region: region,
			Leader: leader,
		})
	}
	return originRegion, newRegionInfos, nil
}

func (c *pdClient) waitRegionsSplit(ctx context.Context, newRegions []*RegionInfo) error {
	backoffer := NewBackoffMayNotCountBackoffer()
	needRecheck := make([]*RegionInfo, 0, len(newRegions))
	return utils.WithRetryReturnLastErr(ctx, func() error {
		needRecheck = needRecheck[:0]

		for _, r := range newRegions {
			regionID := r.Region.GetId()

			ok, err := c.hasHealthyRegion(ctx, regionID)
			if !ok || err != nil {
				if err != nil {
					brlog.FromContext(ctx).Warn(
						"wait for split failed",
						zap.Uint64("regionID", regionID),
						zap.Error(err),
					)
				}
				needRecheck = append(needRecheck, r)
			}
		}

		if len(needRecheck) == 0 {
			return nil
		}

		backoffErr := ErrBackoff
		// if made progress in this round, don't increase the retryCnt
		if len(needRecheck) < len(newRegions) {
			backoffErr = ErrBackoffAndDontCount
		}
		newRegions = slices.Clone(needRecheck)

		return errors.Annotatef(
			backoffErr,
			"WaitRegionsSplit not finished, needRecheck: %d, the first unfinished region: %s",
			len(needRecheck), needRecheck[0].Region.String(),
		)
	}, backoffer)
}

func (c *pdClient) hasHealthyRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := c.GetRegionByID(ctx, regionID)
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

func (c *pdClient) SplitKeysAndScatter(ctx context.Context, sortedSplitKeys [][]byte) ([]*RegionInfo, error) {
	if len(sortedSplitKeys) == 0 {
		return nil, nil
	}
	// we need to find the regions that contain the split keys. However, the scan
	// region API accepts a key range [start, end) where end key is exclusive, and if
	// sortedSplitKeys length is 1, scan region may return empty result. So we
	// increase the end key a bit. If the end key is on the region boundaries, it
	// will be skipped by getSplitKeysOfRegions.
	scanStart := codec.EncodeBytesExt(nil, sortedSplitKeys[0], c.isRawKv)
	lastKey := kv.Key(sortedSplitKeys[len(sortedSplitKeys)-1])
	if len(lastKey) > 0 {
		lastKey = lastKey.Next()
	}
	scanEnd := codec.EncodeBytesExt(nil, lastKey, c.isRawKv)

	// mu protects ret, retrySplitKeys, lastSplitErr
	mu := sync.Mutex{}
	ret := make([]*RegionInfo, 0, len(sortedSplitKeys)+1)
	retrySplitKeys := make([][]byte, 0, len(sortedSplitKeys))
	var lastSplitErr error

	err := utils.WithRetryReturnLastErr(ctx, func() error {
		ret = ret[:0]

		if len(retrySplitKeys) > 0 {
			scanStart = codec.EncodeBytesExt(nil, retrySplitKeys[0], c.isRawKv)
			lastKey2 := kv.Key(retrySplitKeys[len(retrySplitKeys)-1])
			scanEnd = codec.EncodeBytesExt(nil, lastKey2.Next(), c.isRawKv)
		}
		regions, err := PaginateScanRegion(ctx, c, scanStart, scanEnd, ScanRegionPaginationLimit)
		if err != nil {
			return err
		}
		log.Info("paginate scan regions",
			zap.Int("count", len(regions)),
			logutil.Key("start", scanStart),
			logutil.Key("end", scanEnd))

		allSplitKeys := sortedSplitKeys
		if len(retrySplitKeys) > 0 {
			allSplitKeys = retrySplitKeys
			retrySplitKeys = retrySplitKeys[:0]
		}
		splitKeyMap := getSplitKeysOfRegions(allSplitKeys, regions, c.isRawKv)
		workerPool := tidbutil.NewWorkerPool(uint(c.splitConcurrency), "split keys")
		eg, eCtx := errgroup.WithContext(ctx)
		for region, splitKeys := range splitKeyMap {
			region := region
			splitKeys := splitKeys
			workerPool.ApplyOnErrorGroup(eg, func() error {
				// TODO(lance6716): add error handling to retry from scan or retry from split
				newRegions, err2 := c.SplitWaitAndScatter(eCtx, region, splitKeys)
				if err2 != nil {
					if common.IsContextCanceledError(err2) {
						return err2
					}
					log.Warn("split and scatter region meet error, will retry",
						zap.Uint64("region_id", region.Region.Id),
						zap.Error(err2))
					mu.Lock()
					retrySplitKeys = append(retrySplitKeys, splitKeys...)
					lastSplitErr = err2
					mu.Unlock()
					return nil
				}

				if len(newRegions) != len(splitKeys) {
					log.Warn("split key count and new region count mismatch",
						zap.Int("new region count", len(newRegions)),
						zap.Int("split key count", len(splitKeys)))
				}
				mu.Lock()
				ret = append(ret, newRegions...)
				mu.Unlock()
				return nil
			})
		}
		if err2 := eg.Wait(); err2 != nil {
			return err2
		}
		if len(retrySplitKeys) == 0 {
			return nil
		}
		slices.SortFunc(retrySplitKeys, bytes.Compare)
		return lastSplitErr
	}, newSplitBackoffer())
	return ret, errors.Trace(err)
}

type splitBackoffer struct {
	state utils.RetryState
}

func newSplitBackoffer() *splitBackoffer {
	return &splitBackoffer{
		state: utils.InitialRetryState(SplitRetryTimes, SplitRetryInterval, SplitMaxRetryInterval),
	}
}

func (bo *splitBackoffer) NextBackoff(err error) time.Duration {
	if berrors.ErrInvalidRange.Equal(err) {
		bo.state.GiveUp()
		return 0
	}
	return bo.state.ExponentialBackoff()
}

func (bo *splitBackoffer) Attempt() int {
	return bo.state.Attempt()
}

func (c *pdClient) SplitWaitAndScatter(ctx context.Context, region *RegionInfo, keys [][]byte) ([]*RegionInfo, error) {
	failpoint.Inject("failToSplit", func(_ failpoint.Value) {
		failpoint.Return(nil, errors.New("retryable error"))
	})
	if len(keys) == 0 {
		return []*RegionInfo{region}, nil
	}

	var (
		start, end = 0, 0
		batchSize  = 0
		newRegions = make([]*RegionInfo, 0, len(keys))
	)

	for end <= len(keys) {
		if end == len(keys) ||
			batchSize+len(keys[end]) > maxBatchSplitSize ||
			end-start >= c.splitBatchKeyCnt {
			// split, wait and scatter for this batch
			originRegion, newRegionsOfBatch, err := c.batchSplitRegionsWithOrigin(ctx, region, keys[start:end])
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = c.waitRegionsSplit(ctx, newRegionsOfBatch)
			if err != nil {
				brlog.FromContext(ctx).Warn(
					"wait regions split failed, will continue anyway",
					zap.Error(err),
				)
			}
			if err = ctx.Err(); err != nil {
				return nil, errors.Trace(err)
			}
			err = c.scatterRegions(ctx, newRegionsOfBatch)
			if err != nil {
				brlog.FromContext(ctx).Warn(
					"scatter regions failed, will continue anyway",
					zap.Error(err),
				)
			}
			if c.onSplit != nil {
				c.onSplit(keys[start:end])
			}

			// the region with the max start key is the region need to be further split,
			// depending on the origin region is the first region or last region, we need to
			// compare the origin region and the last one of new regions.
			lastNewRegion := newRegionsOfBatch[len(newRegionsOfBatch)-1]
			if bytes.Compare(originRegion.Region.StartKey, lastNewRegion.Region.StartKey) < 0 {
				region = lastNewRegion
			} else {
				region = originRegion
			}
			newRegions = append(newRegions, newRegionsOfBatch...)
			batchSize = 0
			start = end
		}

		if end < len(keys) {
			batchSize += len(keys[end])
		}
		end++
	}

	return newRegions, errors.Trace(ctx.Err())
}

func (c *pdClient) getStoreCount(ctx context.Context) (int, error) {
	stores, err := util.GetAllTiKVStores(ctx, c.client, util.SkipTiFlash)
	if err != nil {
		return 0, err
	}
	return len(stores), err
}

func (c *pdClient) getMaxReplica(ctx context.Context) (int, error) {
	resp, err := c.httpCli.GetReplicateConfig(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	key := "max-replicas"
	val, ok := resp[key]
	if !ok {
		return 0, errors.Errorf("key %s not found in response %v", key, resp)
	}
	return int(val.(float64)), nil
}

func (c *pdClient) checkNeedScatter(ctx context.Context) (bool, error) {
	storeCount, err := c.getStoreCount(ctx)
	if err != nil {
		return false, err
	}
	maxReplica, err := c.getMaxReplica(ctx)
	if err != nil {
		return false, err
	}
	log.Info("checking whether need to scatter", zap.Int("store", storeCount), zap.Int("max-replica", maxReplica))
	// Skipping scatter may lead to leader unbalanced,
	// currently, we skip scatter only when:
	//   1. max-replica > store-count (Probably a misconfigured or playground cluster.)
	//   2. store-count == 1 (No meaning for scattering.)
	// We can still omit scatter when `max-replica == store-count`, if we create a BalanceLeader operator here,
	//   however, there isn't evidence for transform leader is much faster than scattering empty regions.
	return storeCount >= maxReplica && storeCount > 1, nil
}

func (c *pdClient) scatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	if !c.needScatter(ctx) {
		return nil
	}
	return c.client.ScatterRegion(ctx, regionInfo.Region.GetId())
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return c.client.GetOperator(ctx, regionID)
}

func (c *pdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	failpoint.Inject("no-leader-error", func(_ failpoint.Value) {
		logutil.CL(ctx).Debug("failpoint no-leader-error injected.")
		failpoint.Return(nil, status.Error(codes.Unavailable, "not leader"))
	})

	regions, err := c.client.ScanRegions(ctx, key, endKey, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionInfos := make([]*RegionInfo, 0, len(regions))
	for _, region := range regions {
		regionInfos = append(regionInfos, &RegionInfo{
			Region: region.Meta,
			Leader: region.Leader,
		})
	}
	return regionInfos, nil
}

func (c *pdClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (*pdhttp.Rule, error) {
	resp, err := c.httpCli.GetPlacementRule(ctx, groupID, ruleID)
	return resp, errors.Trace(err)
}

func (c *pdClient) SetPlacementRule(ctx context.Context, rule *pdhttp.Rule) error {
	return c.httpCli.SetPlacementRule(ctx, rule)
}

func (c *pdClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return c.httpCli.DeletePlacementRule(ctx, groupID, ruleID)
}

func (c *pdClient) SetStoresLabel(
	ctx context.Context, stores []uint64, labelKey, labelValue string,
) error {
	m := map[string]string{labelKey: labelValue}
	for _, id := range stores {
		err := c.httpCli.SetStoreLabels(ctx, int64(id), m)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *pdClient) scatterRegionsSequentially(ctx context.Context, newRegions []*RegionInfo, backoffer utils.Backoffer) {
	newRegionSet := make(map[uint64]*RegionInfo, len(newRegions))
	for _, newRegion := range newRegions {
		newRegionSet[newRegion.Region.Id] = newRegion
	}

	if err := utils.WithRetry(ctx, func() error {
		log.Info("trying to scatter regions...", zap.Int("remain", len(newRegionSet)))
		var errs error
		for _, region := range newRegionSet {
			err := c.scatterRegion(ctx, region)
			if err == nil {
				// it is safe according to the Go language spec.
				delete(newRegionSet, region.Region.Id)
			} else if !PdErrorCanRetry(err) {
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

func (c *pdClient) isScatterRegionFinished(
	ctx context.Context,
	regionID uint64,
) (scatterDone bool, needRescatter bool, scatterErr error) {
	resp, err := c.GetOperator(ctx, regionID)
	if err != nil {
		if common.IsRetryableError(err) {
			// retry in the next cycle
			return false, false, nil
		}
		return false, false, errors.Trace(err)
	}
	return isScatterRegionFinished(resp)
}

func (c *pdClient) WaitRegionsScattered(ctx context.Context, regions []*RegionInfo) (int, error) {
	var (
		backoffer     = NewBackoffMayNotCountBackoffer()
		retryCnt      = -1
		needRescatter = make([]*RegionInfo, 0, len(regions))
		needRecheck   = make([]*RegionInfo, 0, len(regions))
	)

	err := utils.WithRetryReturnLastErr(ctx, func() error {
		retryCnt++
		loggedInThisRound := false
		needRecheck = needRecheck[:0]
		needRescatter = needRescatter[:0]

		for i, region := range regions {
			regionID := region.Region.GetId()

			if retryCnt > 10 && !loggedInThisRound {
				loggedInThisRound = true
				resp, err := c.GetOperator(ctx, regionID)
				brlog.FromContext(ctx).Info(
					"retried many times to wait for scattering regions, checking operator",
					zap.Int("retryCnt", retryCnt),
					zap.Uint64("firstRegionID", regionID),
					zap.Stringer("response", resp),
					zap.Error(err),
				)
			}

			ok, rescatter, err := c.isScatterRegionFinished(ctx, regionID)
			if err != nil {
				if !common.IsRetryableError(err) {
					brlog.FromContext(ctx).Warn(
						"wait for scatter region encountered non-retryable error",
						logutil.Region(region.Region),
						zap.Error(err),
					)
					needRecheck = append(needRecheck, regions[i:]...)
					return err
				}
				// if meet retryable error, recheck this region in next round
				brlog.FromContext(ctx).Warn(
					"wait for scatter region encountered error, will retry again",
					logutil.Region(region.Region),
					zap.Error(err),
				)
				needRecheck = append(needRecheck, region)
				continue
			}

			if ok {
				continue
			}
			// not finished scattered, check again in next round
			needRecheck = append(needRecheck, region)

			if rescatter {
				needRescatter = append(needRescatter, region)
			}
		}

		if len(needRecheck) == 0 {
			return nil
		}

		backoffErr := ErrBackoff
		// if made progress in this round, don't increase the retryCnt
		if len(needRecheck) < len(regions) {
			backoffErr = ErrBackoffAndDontCount
		}

		regions = slices.Clone(needRecheck)

		if len(needRescatter) > 0 {
			scatterErr := c.scatterRegions(ctx, needRescatter)
			if scatterErr != nil {
				if !common.IsRetryableError(scatterErr) {
					return scatterErr
				}

				return errors.Annotate(backoffErr, scatterErr.Error())
			}
		}
		return errors.Annotatef(
			backoffErr,
			"scatter region not finished, retryCnt: %d, needRecheck: %d, needRescatter: %d, the first unfinished region: %s",
			retryCnt, len(needRecheck), len(needRescatter), needRecheck[0].Region.String(),
		)
	}, backoffer)

	return len(needRecheck), err
}

// isScatterRegionFinished checks whether the scatter region operator is
// finished.
func isScatterRegionFinished(resp *pdpb.GetOperatorResponse) (
	scatterDone bool,
	needRescatter bool,
	scatterErr error,
) {
	// Heartbeat may not be sent to PD
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, false, nil
		}
		return false, false, errors.Annotatef(
			berrors.ErrPDInvalidResponse,
			"get operator error: %s, error message: %s",
			respErr.GetType(),
			respErr.GetMessage(),
		)
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

// CheckRegionEpoch check region epoch.
func CheckRegionEpoch(_new, _old *RegionInfo) bool {
	return _new.Region.GetId() == _old.Region.GetId() &&
		_new.Region.GetRegionEpoch().GetVersion() == _old.Region.GetRegionEpoch().GetVersion() &&
		_new.Region.GetRegionEpoch().GetConfVer() == _old.Region.GetRegionEpoch().GetConfVer()
}

// ExponentialBackoffer trivially retry any errors it meets.
// It's useful when the caller has handled the errors but
// only want to a more semantic backoff implementation.
type ExponentialBackoffer struct {
	Attempts    int
	BaseBackoff time.Duration
}

func (b *ExponentialBackoffer) exponentialBackoff() time.Duration {
	bo := b.BaseBackoff
	b.Attempts--
	if b.Attempts == 0 {
		return 0
	}
	b.BaseBackoff *= 2
	return bo
}

// PdErrorCanRetry when pd error retry.
func PdErrorCanRetry(err error) bool {
	// There are 3 type of reason that PD would reject a `scatter` request:
	// (1) region %d has no leader
	// (2) region %d is hot
	// (3) region %d is not fully replicated
	//
	// (2) shouldn't happen in a recently splitted region.
	// (1) and (3) might happen, and should be retried.
	grpcErr := status.Convert(err)
	if grpcErr == nil {
		return false
	}
	return strings.Contains(grpcErr.Message(), "is not fully replicated") ||
		strings.Contains(grpcErr.Message(), "has no leader")
}

// NextBackoff returns a duration to wait before retrying again.
func (b *ExponentialBackoffer) NextBackoff(error) time.Duration {
	// trivially exponential back off, because we have handled the error at upper level.
	return b.exponentialBackoff()
}

// Attempt returns the remain attempt times
func (b *ExponentialBackoffer) Attempt() int {
	return b.Attempts
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
