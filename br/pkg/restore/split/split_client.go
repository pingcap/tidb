package split

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
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
	"github.com/pingcap/tidb/br/pkg/conn/util"
	errors2 "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/utils/utildb"
	"github.com/pingcap/tidb/store/pdtypes"
	pd "github.com/tikv/pd/client"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// ScanRegion gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error)
	// GetPlacementRule loads a placement rule from PD.
	GetPlacementRule(ctx context.Context, groupID, ruleID string) (pdtypes.Rule, error)
	// SetPlacementRule insert or update a placement rule to PD.
	SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error
	// DeletePlacementRule removes a placement rule from PD.
	DeletePlacementRule(ctx context.Context, groupID, ruleID string) error
	// SetStoreLabel add or update specified label of stores. If labelValue
	// is empty, it clears the label.
	SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error
}

func checkRegionConsistency(startKey, endKey []byte, regions []*RegionInfo) error {
	// current pd can't guarantee the consistency of returned regions
	if len(regions) == 0 {
		return errors.Annotatef(errors2.ErrPDBatchScanRegion, "scan region return empty result, startKey: %s, endkey: %s",
			redact.Key(startKey), redact.Key(endKey))
	}

	if bytes.Compare(regions[0].Region.StartKey, startKey) > 0 {
		return errors.Annotatef(errors2.ErrPDBatchScanRegion, "first region's startKey > startKey, startKey: %s, regionStartKey: %s",
			redact.Key(startKey), redact.Key(regions[0].Region.StartKey))
	} else if len(regions[len(regions)-1].Region.EndKey) != 0 && bytes.Compare(regions[len(regions)-1].Region.EndKey, endKey) < 0 {
		return errors.Annotatef(errors2.ErrPDBatchScanRegion, "last region's endKey < startKey, startKey: %s, regionStartKey: %s",
			redact.Key(endKey), redact.Key(regions[len(regions)-1].Region.EndKey))
	}

	cur := regions[0]
	for _, r := range regions[1:] {
		if !bytes.Equal(cur.Region.EndKey, r.Region.StartKey) {
			return errors.Annotatef(errors2.ErrPDBatchScanRegion, "region endKey not equal to next region startKey, endKey: %s, startKey: %s",
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
	if len(endKey) != 0 && bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.Annotatef(errors2.ErrRestoreInvalidRange, "startKey >= endKey, startKey: %s, endkey: %s",
			hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	}

	var regions []*RegionInfo
	err := utildb.WithRetry(ctx, func() error {
		regions = []*RegionInfo{}
		scanStartKey := startKey
		for {
			batch, err := client.ScanRegions(ctx, scanStartKey, endKey, limit)
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
		if err := checkRegionConsistency(startKey, endKey, regions); err != nil {
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

func newScanRegionBackoffer() utildb.Backoffer {
	return &scanRegionBackoffer{
		attempt: 3,
	}
}

// NextBackoff returns a duration to wait before retrying again
func (b *scanRegionBackoffer) NextBackoff(err error) time.Duration {
	if errors2.ErrPDBatchScanRegion.Equal(err) {
		// 500ms * 3 could be enough for splitting remain regions in the hole.
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

const (
	splitRegionMaxRetryTime = 4
)

// pdClient is a wrapper of pd client, can be used by RegionSplitter.
type pdClient struct {
	mu         sync.Mutex
	client     pd.Client
	tlsConf    *tls.Config
	storeCache map[uint64]*metapb.Store
	isRawKv    bool
	// FIXME when config changed during the lifetime of pdClient,
	// 	this may mislead the scatter.
	needScatterVal  bool
	needScatterInit sync.Once
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
		Region: region.Meta,
		Leader: region.Leader,
	}, nil
}

func (c *pdClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		if len(regionInfo.Region.Peers) == 0 {
			return nil, errors.Annotate(errors2.ErrRestoreNoPeer, "region does not have peer")
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
		return nil, errors.Annotatef(errors2.ErrRestoreSplitFailed, "err=%v", resp.RegionError)
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
		return nil, errors.Annotate(errors2.ErrRestoreSplitFailed, "new region is nil")
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
					errors.Annotatef(errors2.ErrRestoreNoPeer, "region[%d] doesn't have any peer", regionInfo.Region.GetId()))
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
		resp, err := splitRegionWithFailpoint(ctx, regionInfo, peer, client, keys)
		if err != nil {
			return nil, multierr.Append(splitErrors, err)
		}
		if resp.RegionError != nil {
			log.Warn("fail to split region",
				logutil.Region(regionInfo.Region),
				zap.Stringer("regionErr", resp.RegionError))
			splitErrors = multierr.Append(splitErrors,
				errors.Annotatef(errors2.ErrRestoreSplitFailed, "split region failed: err=%v", resp.RegionError))
			if nl := resp.RegionError.NotLeader; nl != nil {
				if leader := nl.GetLeader(); leader != nil {
					regionInfo.Leader = leader
				} else {
					newRegionInfo, findLeaderErr := c.GetRegionByID(ctx, nl.RegionId)
					if findLeaderErr != nil {
						return nil, multierr.Append(splitErrors, findLeaderErr)
					}
					if !CheckRegionEpoch(newRegionInfo, regionInfo) {
						return nil, multierr.Append(splitErrors, errors2.ErrKVEpochNotMatch)
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
	stores, err := util.GetAllTiKVStores(ctx, c.client, util.SkipTiFlash)
	if err != nil {
		return 0, err
	}
	return len(stores), err
}

func (c *pdClient) getMaxReplica(ctx context.Context) (int, error) {
	api := c.getPDAPIAddr()
	configAPI := api + "/pd/api/v1/config"
	req, err := http.NewRequestWithContext(ctx, "GET", configAPI, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return 0, errors.Trace(err)
	}
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
		return rule, errors.Annotate(errors2.ErrRestoreSplitFailed, "failed to add stores labels: no leader")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", addr+path.Join("/pd/api/v1/config/rule", groupID, ruleID), nil)
	if err != nil {
		return rule, errors.Trace(err)
	}
	res, err := httputil.NewClient(c.tlsConf).Do(req)
	if err != nil {
		return rule, errors.Trace(err)
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return rule, errors.Trace(err)
	}
	res.Body.Close()
	err = json.Unmarshal(b, &rule)
	if err != nil {
		return rule, errors.Trace(err)
	}
	return rule, nil
}

func (c *pdClient) SetPlacementRule(ctx context.Context, rule pdtypes.Rule) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.Annotate(errors2.ErrPDLeaderNotFound, "failed to add stores labels")
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
		return errors.Annotate(errors2.ErrPDLeaderNotFound, "failed to add stores labels")
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
		return errors.Annotate(errors2.ErrPDLeaderNotFound, "failed to add stores labels")
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

func CheckRegionEpoch(newInfo, oldInfo *RegionInfo) bool {
	return newInfo.Region.GetId() == oldInfo.Region.GetId() &&
		newInfo.Region.GetRegionEpoch().GetVersion() == oldInfo.Region.GetRegionEpoch().GetVersion() &&
		newInfo.Region.GetRegionEpoch().GetConfVer() == oldInfo.Region.GetRegionEpoch().GetConfVer()
}
