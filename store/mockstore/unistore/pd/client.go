// Copyright 2019-present PingCAP, Inc.
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

package pd

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)
	IsBootstrapped(ctx context.Context) (bool, error)
	PutStore(ctx context.Context, store *metapb.Store) error
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetRegion(ctx context.Context, key []byte) (*pd.Region, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error)
	ReportRegion(*pdpb.RegionHeartbeatRequest)
	AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error)
	AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error)
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error
	GetGCSafePoint(ctx context.Context) (uint64, error)
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	GetTS(ctx context.Context) (int64, int64, error)
	SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse))
	Close()
}

const (
	pdTimeout     = time.Second
	retryInterval = time.Second
	maxRetryCount = 10
)

type client struct {
	urls      []string
	clusterID uint64
	tag       string

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}
	checkLeaderCh chan struct{}

	receiveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse
	regionCh                 chan *pdpb.RegionHeartbeatRequest
	pendingRequest           *pdpb.RegionHeartbeatRequest

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	heartbeatHandler atomic.Value
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string, tag string) (Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	urls := make([]string, 0, len(pdAddrs))
	for _, addr := range pdAddrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	log.Info("[pd] client created", zap.String("tag", tag), zap.Strings("endpoints", urls))

	c := &client{
		urls:                     urls,
		receiveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		checkLeaderCh:            make(chan struct{}, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
		regionCh:                 make(chan *pdpb.RegionHeartbeatRequest, 64),
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	var (
		err     error
		members *pdpb.GetMembersResponse
	)
	for i := 0; i < maxRetryCount; i++ {
		if members, err = c.updateLeader(); err == nil {
			break
		}
		time.Sleep(retryInterval)
	}
	if err != nil {
		return nil, err
	}

	c.clusterID = members.GetHeader().GetClusterId()
	log.Info("[pd] init cluster id", zap.String("tag", tag), zap.Uint64("id", c.clusterID))
	c.wg.Add(2)
	go c.checkLeaderLoop()
	go c.heartbeatStreamLoop()

	return c, nil
}

func (c *client) schedulerUpdateLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) checkLeaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		if _, err := c.updateLeader(); err != nil {
			log.Error("[pd] failed updateLeader", zap.Error(err))
		}
	}
}

func (c *client) updateLeader() (*pdpb.GetMembersResponse, error) {
	for _, u := range c.urls {
		ctx, cancel := context.WithTimeout(c.ctx, pdTimeout)
		members, err := c.getMembers(ctx, u)
		cancel()
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			select {
			case <-c.ctx.Done():
				return nil, err
			default:
				continue
			}
		}

		c.updateURLs(members.GetMembers(), members.GetLeader())
		return members, c.switchLeader(members.GetLeader().GetClientUrls())
	}
	return nil, errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) updateURLs(members []*pdpb.Member, leader *pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		if m.GetMemberId() == leader.GetMemberId() {
			continue
		}
		urls = append(urls, m.GetClientUrls()...)
	}
	c.urls = append(urls, leader.GetClientUrls()...)
}

func (c *client) switchLeader(addrs []string) error {
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Info("[pd] switch leader", zap.String("new leader", addr), zap.String("old leader", oldLeader))
	if _, err := c.getOrCreateConn(addr); err != nil {
		return err
	}

	c.connMu.Lock()
	c.connMu.leader = addr
	c.connMu.Unlock()
	return nil
}

func (c *client) getMembers(ctx context.Context, url string) (*pdpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateConn(url)
	if err != nil {
		return nil, err
	}
	return pdpb.NewPDClient(cc).GetMembers(ctx, new(pdpb.GetMembersRequest))
}

func (c *client) getOrCreateConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.Dial(u.Host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		err = cc.Close()
		return old, err
	}
	c.connMu.clientConns[addr] = cc
	return cc, nil
}

func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *client) doRequest(ctx context.Context, f func(context.Context, pdpb.PDClient) error) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		ctx1, cancel := context.WithTimeout(ctx, pdTimeout)
		err = f(ctx1, c.leaderClient())
		cancel()
		if err == nil {
			return nil
		}
		log.Error("do request failed", zap.Error(err))

		c.schedulerUpdateLeader()
		select {
		case <-time.After(retryInterval):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("failed too many times")
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		ctx, cancel := context.WithCancel(c.ctx)
		c.connMu.RLock()
		stream, err := c.leaderClient().RegionHeartbeat(ctx)
		c.connMu.RUnlock()
		if err != nil {
			cancel()
			c.schedulerUpdateLeader()
			time.Sleep(retryInterval)
			continue
		}

		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)

		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.receiveRegionHeartbeat(stream, errCh, wg)
		select {
		case err := <-errCh:
			log.Warn("[pd] heartbeat stream failed", zap.String("tag", c.tag), zap.Error(err))
			cancel()
			c.schedulerUpdateLeader()
			time.Sleep(retryInterval)
			wg.Wait()
		case <-c.ctx.Done():
			log.Info("cancel heartbeat stream loop")
			cancel()
			return
		}
	}
}

func (c *client) receiveRegionHeartbeat(stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}

		if h := c.heartbeatHandler.Load(); h != nil {
			h.(func(*pdpb.RegionHeartbeatResponse))(resp)
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		request, ok := c.getNextHeartbeatRequest(ctx)
		if !ok {
			return
		}

		request.Header = c.requestHeader()
		err := stream.Send(request)
		if err != nil {
			c.pendingRequest = request
			errCh <- err
			return
		}
	}
}

func (c *client) getNextHeartbeatRequest(ctx context.Context) (*pdpb.RegionHeartbeatRequest, bool) {
	if c.pendingRequest != nil {
		req := c.pendingRequest
		c.pendingRequest = nil
		return req, true
	}

	select {
	case <-ctx.Done():
		return nil, false
	case request, ok := <-c.regionCh:
		if !ok {
			return nil, false
		}
		return request, true
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()
	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		err := cc.Close()
		if err != nil {
			log.Error("[pd] close failed", zap.Error(err))
		}
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	var resp *pdpb.AllocIDResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AllocID(ctx, &pdpb.AllocIDRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (resp *pdpb.BootstrapResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.Bootstrap(ctx, &pdpb.BootstrapRequest{
			Header: c.requestHeader(),
			Store:  store,
			Region: region,
		})
		return err1
	})
	return resp, err
}

func (c *client) IsBootstrapped(ctx context.Context) (bool, error) {
	var resp *pdpb.IsBootstrappedResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{Header: c.requestHeader()})
		return err1
	})
	if err != nil {
		return false, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return false, errors.New(herr.String())
	}
	return resp.Bootstrapped, nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	var resp *pdpb.PutStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.PutStore(ctx, &pdpb.PutStoreRequest{
			Header: c.requestHeader(),
			Store:  store,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	var resp *pdpb.GetStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetStore(ctx, &pdpb.GetStoreRequest{
			Header:  c.requestHeader(),
			StoreId: storeID,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Store, nil
}

func (c *client) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	var resp *pdpb.GetAllStoresResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{
			Header:                 c.requestHeader(),
			ExcludeTombstoneStores: true,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Stores, nil
}

func (c *client) GetClusterConfig(ctx context.Context) (*metapb.Cluster, error) {
	var resp *pdpb.GetClusterConfigResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetClusterConfig(ctx, &pdpb.GetClusterConfigRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Cluster, nil
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	var resp *pdpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	r := &pd.Region{
		Meta:         resp.Region,
		Leader:       resp.Leader,
		PendingPeers: resp.PendingPeers,
	}
	for _, s := range resp.DownPeers {
		r.DownPeers = append(r.DownPeers, s.Peer)
	}
	return r, nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	var resp *pdpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
			Header:   c.requestHeader(),
			RegionId: regionID,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	r := &pd.Region{
		Meta:         resp.Region,
		Leader:       resp.Leader,
		PendingPeers: resp.PendingPeers,
	}
	for _, s := range resp.DownPeers {
		r.DownPeers = append(r.DownPeers, s.Peer)
	}
	return r, nil
}

func (c *client) AskSplit(ctx context.Context, region *metapb.Region) (resp *pdpb.AskSplitResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AskSplit(ctx, &pdpb.AskSplitRequest{
			Header: c.requestHeader(),
			Region: region,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (resp *pdpb.AskBatchSplitResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
			Header:     c.requestHeader(),
			Region:     region,
			SplitCount: uint32(count),
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	var resp *pdpb.ReportBatchSplitResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.ReportBatchSplit(ctx, &pdpb.ReportBatchSplitRequest{
			Header:  c.requestHeader(),
			Regions: regions,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetGCSafePoint(ctx context.Context) (uint64, error) {
	var resp *pdpb.GetGCSafePointResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return 0, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return 0, errors.New(herr.String())
	}
	return resp.SafePoint, nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	var resp *pdpb.StoreHeartbeatResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
			Header: c.requestHeader(),
			Stats:  stats,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetTS(ctx context.Context) (int64, int64, error) {
	var resp *pdpb.TsoResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		tsoClient, err := client.Tso(ctx)
		if err != nil {
			return err
		}
		err = tsoClient.Send(&pdpb.TsoRequest{Header: c.requestHeader(), Count: 1})
		if err != nil {
			return err
		}
		resp, err = tsoClient.Recv()
		if err != nil {
			return err
		}
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return 0, 0, errors.New(herr.String())
	}
	return resp.Timestamp.Physical, resp.Timestamp.Logical, nil
}

func (c *client) ReportRegion(request *pdpb.RegionHeartbeatRequest) {
	c.regionCh <- request
}

func (c *client) SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*pdpb.RegionHeartbeatResponse) {}
	}
	c.heartbeatHandler.Store(h)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}
