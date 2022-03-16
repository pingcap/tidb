// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/store/pdtypes"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	splitRegionMaxRetryTime = 4
)

// SplitClient is an external client used by RegionSplitter.
type SplitClient interface {
	// GetStore gets a store by a store id.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetRegion gets a region which includes a specified key.
	GetRegion(ctx context.Context, key []byte) (*RegionInfo, error)
	// GetRegionByID gets a region by a region id.
	GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)
	// SplitRegion splits a region from a key, if key is not included in the region, it will return nil.
	// note: the key should not be encoded
	SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error)
	// BatchSplitRegions splits a region from a batch of keys.
	// note: the keys should not be encoded
	BatchSplitRegions(ctx context.Context, regionInfo *RegionInfo, keys [][]byte) ([]*RegionInfo, error)
	// BatchSplitRegionsWithOrigin splits a region from a batch of keys and return the original region and split new regions
	BatchSplitRegionsWithOrigin(ctx context.Context, regionInfo *RegionInfo, keys [][]byte) (*RegionInfo, []*RegionInfo, error)
	// ScatterRegion scatters a specified region.
	ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error
	// ScatterRegions scatters regions in a batch.
	ScatterRegions(ctx context.Context, regionInfo []*RegionInfo) error
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// ScanRegions gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error)
	// GetPlacementRule loads a placement rule from PD.
	GetPlacementRule(ctx context.Context, groupID, ruleID string) (pdtypes.Rule, error)
	// SetPlacementRule insert or update a placement rule to PD.
	SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error
	// DeletePlacementRule removes a placement rule from PD.
	DeletePlacementRule(ctx context.Context, groupID, ruleID string) error
	// SetStoresLabel add or update specified label of stores. If labelValue
	// is empty, it clears the label.
	SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error
}

// pdClient is a wrapper of pd client, can be used by RegionSplitter.
type pdClient struct {
	mu         sync.Mutex
	client     pd.Client
	tlsConf    *tls.Config
	storeCache map[uint64]*metapb.Store

	// FIXME when config changed during the lifetime of pdClient,
	// 	this may mislead the scatter.
	needScatterVal  bool
	needScatterInit sync.Once

	isRawKv bool
}

// NewSplitClient returns a client used by RegionSplitter.
func NewSplitClient(client pd.Client, tlsConf *tls.Config, isRawKv bool) SplitClient {
	cli := &pdClient{
		client:     client,
		tlsConf:    tlsConf,
		storeCache: make(map[uint64]*metapb.Store),
		isRawKv:    isRawKv,
	}
	return cli
}

func (c *pdClient) needScatter(ctx context.Context) bool {
	c.needScatterInit.Do(func() {
		var err error
		c.needScatterVal, err = c.checkNeedScatter(ctx)
		if err != nil {
			log.Warn("failed to check whether need to scatter, use permissive strategy: always scatter", logutil.ShortError(err))
			c.needScatterVal = true
		}
		if !c.needScatterVal {
			log.Info("skipping scatter because the replica number isn't less than store count.")
		}
	})
	return c.needScatterVal
}

// ScatterRegions scatters regions in a batch.
func (c *pdClient) ScatterRegions(ctx context.Context, regionInfo []*RegionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	regionsID := make([]uint64, 0, len(regionInfo))
	for _, v := range regionInfo {
		regionsID = append(regionsID, v.Region.Id)
	}
	resp, err := c.client.ScatterRegions(ctx, regionsID)
	if err != nil {
		return err
	}
	if pbErr := resp.GetHeader().GetError(); pbErr.GetType() != pdpb.ErrorType_OK {
		return errors.Annotatef(berrors.ErrPDInvalidResponse, "pd returns error during batch scattering: %s", pbErr)
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

func (c *pdClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return nil, errors.Annotate(berrors.ErrRestoreNoPeer, "region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := c.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	resp, err := client.SplitRegion(ctx, &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
		},
		SplitKey: key,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.RegionError != nil {
		log.Error("fail to split region",
			logutil.Region(regionInfo.Region),
			logutil.Key("key", key),
			zap.Stringer("regionErr", resp.RegionError))
		return nil, errors.Annotatef(berrors.ErrRestoreSplitFailed, "err=%v", resp.RegionError)
	}

	// BUG: Left is deprecated, it may be nil even if split is succeed!
	// Assume the new region is the left one.
	newRegion := resp.GetLeft()
	if newRegion == nil {
		regions := resp.GetRegions()
		for _, r := range regions {
			if bytes.Equal(r.GetStartKey(), regionInfo.Region.GetStartKey()) {
				newRegion = r
				break
			}
		}
	}
	if newRegion == nil {
		return nil, errors.Annotate(berrors.ErrRestoreSplitFailed, "new region is nil")
	}
	var leader *metapb.Peer
	// Assume the leaders will be at the same store.
	if regionInfo.Leader != nil {
		for _, p := range newRegion.GetPeers() {
			if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
				leader = p
				break
			}
		}
	}
	return &RegionInfo{
		Region: newRegion,
		Leader: leader,
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
		var peer *metapb.Peer
		// scanRegions may return empty Leader in https://github.com/tikv/pd/blob/v4.0.8/server/grpc_service.go#L524
		// so wee also need check Leader.Id != 0
		if regionInfo.Leader != nil && regionInfo.Leader.Id != 0 {
			peer = regionInfo.Leader
		} else {
			if len(regionInfo.Region.Peers) == 0 {
				return nil, multierr.Append(splitErrors,
					errors.Annotatef(berrors.ErrRestoreNoPeer, "region[%d] doesn't have any peer", regionInfo.Region.GetId()))
			}
			peer = regionInfo.Region.Peers[0]
		}
		storeID := peer.GetStoreId()
		store, err := c.GetStore(ctx, storeID)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		opt := grpc.WithInsecure()
		if c.tlsConf != nil {
			opt = grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConf))
		}
		conn, err := grpc.Dial(store.GetAddress(), opt)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		defer conn.Close()
		client := tikvpb.NewTikvClient(conn)
		resp, err := splitRegionWithFailpoint(ctx, regionInfo, peer, client, keys, c.isRawKv)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		if resp.RegionError != nil {
			log.Warn("fail to split region",
				logutil.Region(regionInfo.Region),
				zap.Stringer("regionErr", resp.RegionError))
			splitErrors = multierr.Append(splitErrors,
				errors.Annotatef(berrors.ErrRestoreSplitFailed, "split region failed: err=%v", resp.RegionError))
			if nl := resp.RegionError.NotLeader; nl != nil {
				if leader := nl.GetLeader(); leader != nil {
					regionInfo.Leader = leader
				} else {
					newRegionInfo, findLeaderErr := c.GetRegionByID(ctx, nl.RegionId)
					if findLeaderErr != nil {
						return nil, multierr.Append(splitErrors, findLeaderErr)
					}
					if !checkRegionEpoch(newRegionInfo, regionInfo) {
						return nil, multierr.Append(splitErrors, berrors.ErrKVEpochNotMatch)
					}
					log.Info("find new leader", zap.Uint64("new leader", newRegionInfo.Leader.Id))
					regionInfo = newRegionInfo
				}
				log.Info("split region meet not leader error, retrying",
					zap.Int("retry times", i),
					zap.Uint64("regionID", regionInfo.Region.Id),
					zap.Any("new leader", regionInfo.Leader),
				)
				continue
			}
			// TODO: we don't handle RegionNotMatch and RegionNotFound here,
			// because I think we don't have enough information to retry.
			// But maybe we can handle them here by some information the error itself provides.
			if resp.RegionError.ServerIsBusy != nil ||
				resp.RegionError.StaleCommand != nil {
				log.Warn("a error occurs on split region",
					zap.Int("retry times", i),
					zap.Uint64("regionID", regionInfo.Region.Id),
					zap.String("error", resp.RegionError.Message),
					zap.Any("error verbose", resp.RegionError),
				)
				continue
			}
			return nil, errors.Trace(splitErrors)
		}
		return resp, nil
	}
	return nil, errors.Trace(splitErrors)
}

func (c *pdClient) BatchSplitRegionsWithOrigin(
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

func (c *pdClient) BatchSplitRegions(
	ctx context.Context, regionInfo *RegionInfo, keys [][]byte,
) ([]*RegionInfo, error) {
	_, newRegions, err := c.BatchSplitRegionsWithOrigin(ctx, regionInfo, keys)
	return newRegions, err
}

func (c *pdClient) getStoreCount(ctx context.Context) (int, error) {
	stores, err := conn.GetAllTiKVStores(ctx, c.client, conn.SkipTiFlash)
	if err != nil {
		return 0, err
	}
	return len(stores), err
}

func (c *pdClient) getMaxReplica(ctx context.Context) (int, error) {
	api := c.getPDAPIAddr()
	configAPI := api + "/pd/api/v1/config/replicate"
	req, err := http.NewRequestWithContext(ctx, "GET", configAPI, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer func() {
		if err = res.Body.Close(); err != nil {
			log.Error("Response fail to close", zap.Error(err))
		}
	}()
	var conf pdtypes.ReplicationConfig
	if err := json.NewDecoder(res.Body).Decode(&conf); err != nil {
		return 0, errors.Trace(err)
	}
	return int(conf.MaxReplicas), nil
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

func (c *pdClient) ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	if !c.needScatter(ctx) {
		return nil
	}
	return c.client.ScatterRegion(ctx, regionInfo.Region.GetId())
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return c.client.GetOperator(ctx, regionID)
}

func (c *pdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
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

func (c *pdClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (pdtypes.Rule, error) {
	var rule pdtypes.Rule
	addr := c.getPDAPIAddr()
	if addr == "" {
		return rule, errors.Annotate(berrors.ErrRestoreSplitFailed, "failed to add stores labels: no leader")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", addr+path.Join("/pd/api/v1/config/rule", groupID, ruleID), nil)
	if err != nil {
		return rule, errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return rule, errors.Trace(err)
	}
	defer func() {
		if err = res.Body.Close(); err != nil {
			log.Error("Response fail to close", zap.Error(err))
		}
	}()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return rule, errors.Trace(err)
	}
	err = json.Unmarshal(b, &rule)
	if err != nil {
		return rule, errors.Trace(err)
	}
	return rule, nil
}

func (c *pdClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.Annotate(berrors.ErrPDLeaderNotFound, "failed to add stores labels")
	}
	m, _ := json.Marshal(rule)
	req, err := http.NewRequestWithContext(ctx, "POST", addr+path.Join("/pd/api/v1/config/rule"), bytes.NewReader(m))
	if err != nil {
		return errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(res.Body.Close())
}

func (c *pdClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.Annotate(berrors.ErrPDLeaderNotFound, "failed to add stores labels")
	}
	req, err := http.NewRequestWithContext(ctx, "DELETE", addr+path.Join("/pd/api/v1/config/rule", groupID, ruleID), nil)
	if err != nil {
		return errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(res.Body.Close())
}

func (c *pdClient) SetStoresLabel(
	ctx context.Context, stores []uint64, labelKey, labelValue string,
) error {
	b := []byte(fmt.Sprintf(`{"%s": "%s"}`, labelKey, labelValue))
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.Annotate(berrors.ErrPDLeaderNotFound, "failed to add stores labels")
	}
	httpCli := httputil.NewClient(c.tlsConf)
	for _, id := range stores {
		req, err := http.NewRequestWithContext(
			ctx, "POST",
			addr+path.Join("/pd/api/v1/store", strconv.FormatUint(id, 10), "label"),
			bytes.NewReader(b),
		)
		if err != nil {
			return errors.Trace(err)
		}
		res, err := httpCli.Do(req)
		if err != nil {
			return errors.Trace(err)
		}
		err = res.Body.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *pdClient) getPDAPIAddr() string {
	addr := c.client.GetLeaderAddr()
	if addr != "" && !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return strings.TrimRight(addr, "/")
}

func checkRegionEpoch(_new, _old *RegionInfo) bool {
	return _new.Region.GetId() == _old.Region.GetId() &&
		_new.Region.GetRegionEpoch().GetVersion() == _old.Region.GetRegionEpoch().GetVersion() &&
		_new.Region.GetRegionEpoch().GetConfVer() == _old.Region.GetRegionEpoch().GetConfVer()
}

// exponentialBackoffer trivially retry any errors it meets.
// It's useful when the caller has handled the errors but
// only want to a more semantic backoff implementation.
type exponentialBackoffer struct {
	attempt     int
	baseBackoff time.Duration
}

func (b *exponentialBackoffer) exponentialBackoff() time.Duration {
	bo := b.baseBackoff
	b.attempt--
	if b.attempt == 0 {
		return 0
	}
	b.baseBackoff *= 2
	return bo
}

func pdErrorCanRetry(err error) bool {
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
func (b *exponentialBackoffer) NextBackoff(error) time.Duration {
	// trivially exponential back off, because we have handled the error at upper level.
	return b.exponentialBackoff()
}

// Attempt returns the remain attempt times
func (b *exponentialBackoffer) Attempt() int {
	return b.attempt
}
