// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetClusterID gets the cluster ID from PD.
	GetClusterID(ctx context.Context) uint64
	// GetTS gets a timestamp from PD.
	GetTS(ctx context.Context) (int64, int64, error)
	// GetTSAsync gets a timestamp from PD, without block the caller.
	GetTSAsync(ctx context.Context) TSFuture
	// GetRegion gets a region and its leader Peer from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	// Also it may return nil if PD finds no Region for the key temporarily,
	// client should retry later.
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)
	// GetRegionByID gets a region and its leader Peer from PD by id.
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// Close closes the client.
	Close()
}

type tsoRequest struct {
	start    time.Time
	ctx      context.Context
	done     chan error
	physical int64
	logical  int64
}

const (
	pdTimeout             = 3 * time.Second
	updateLeaderTimeout   = time.Second // Use a shorter timeout to recover faster from network isolation.
	maxMergeTSORequests   = 10000
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[pd] closing")
	// errTSOLength is returned when the number of response timestamps is inconsistent with request.
	errTSOLength = errors.New("[pd] tso length in rpc response is incorrect")
)

type client struct {
	urls        []string
	clusterID   uint64
	tsoRequests chan *tsoRequest

	connMu struct {
		sync.RWMutex
		clientConns map[string]*grpc.ClientConn
		leader      string
	}

	tsDeadlineCh  chan deadline
	checkLeaderCh chan struct{}

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string) (Client, error) {
	log.Infof("[pd] create pd client with endpoints %v", pdAddrs)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		urls:          addrsToUrls(pdAddrs),
		tsoRequests:   make(chan *tsoRequest, maxMergeTSORequests),
		tsDeadlineCh:  make(chan deadline, 1),
		checkLeaderCh: make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
	}
	c.connMu.clientConns = make(map[string]*grpc.ClientConn)

	if err := c.initClusterID(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := c.updateLeader(); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[pd] init cluster id %v", c.clusterID)

	c.wg.Add(3)
	go c.tsLoop()
	go c.tsCancelLoop()
	go c.leaderLoop()

	return c, nil
}

func (c *client) updateURLs(members []*pdpb.Member) {
	urls := make([]string, 0, len(members))
	for _, m := range members {
		urls = append(urls, m.GetClientUrls()...)
	}
	c.urls = urls
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		for _, u := range c.urls {
			members, err := c.getMembers(ctx, u)
			if err != nil || members.GetHeader() == nil {
				log.Errorf("[pd] failed to get cluster id: %v", err)
				continue
			}
			c.clusterID = members.GetHeader().GetClusterId()
			return nil
		}

		time.Sleep(time.Second)
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) updateLeader() error {
	for _, u := range c.urls {
		ctx, cancel := context.WithTimeout(c.ctx, updateLeaderTimeout)
		members, err := c.getMembers(ctx, u)
		cancel()
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			continue
		}
		c.updateURLs(members.GetMembers())
		if err = c.switchLeader(members.GetLeader().GetClientUrls()); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("failed to get leader from %v", c.urls)
}

func (c *client) getMembers(ctx context.Context, url string) (*pdpb.GetMembersResponse, error) {
	cc, err := c.getOrCreateGRPCConn(url)
	if err != nil {
		return nil, errors.Trace(err)
	}
	members, err := pdpb.NewPDClient(cc).GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func (c *client) switchLeader(addrs []string) error {
	// FIXME: How to safely compare leader urls? For now, only allows one client url.
	addr := addrs[0]

	c.connMu.RLock()
	oldLeader := c.connMu.leader
	c.connMu.RUnlock()

	if addr == oldLeader {
		return nil
	}

	log.Infof("[pd] leader switches to: %v, previous: %v", addr, oldLeader)
	if _, err := c.getOrCreateGRPCConn(addr); err != nil {
		return errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	c.connMu.leader = addr
	return nil
}

func (c *client) getOrCreateGRPCConn(addr string) (*grpc.ClientConn, error) {
	c.connMu.RLock()
	conn, ok := c.connMu.clientConns[addr]
	c.connMu.RUnlock()
	if ok {
		return conn, nil
	}

	cc, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure()) // TODO: Support HTTPS.
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()
	if old, ok := c.connMu.clientConns[addr]; ok {
		cc.Close()
		return old, nil
	}

	c.connMu.clientConns[addr] = cc
	return cc, nil
}

func (c *client) leaderLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case <-c.checkLeaderCh:
		case <-time.After(time.Minute):
		case <-ctx.Done():
			return
		}

		if err := c.updateLeader(); err != nil {
			log.Errorf("[pd] failed updateLeader: %v", err)
		}
	}
}

type deadline struct {
	timer  <-chan time.Time
	done   chan struct{}
	cancel context.CancelFunc
}

func (c *client) tsCancelLoop() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	for {
		select {
		case d := <-c.tsDeadlineCh:
			select {
			case <-d.timer:
				log.Error("tso request is canceled due to timeout")
				d.cancel()
			case <-d.done:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) tsLoop() {
	defer c.wg.Done()

	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	var requests []*tsoRequest
	var stream pdpb.PD_TsoClient
	var cancel context.CancelFunc

	for {
		var err error

		if stream == nil {
			var ctx context.Context
			ctx, cancel = context.WithCancel(c.ctx)
			stream, err = c.leaderClient().Tso(ctx)
			if err != nil {
				log.Errorf("[pd] create tso stream error: %v", err)
				c.scheduleCheckLeader()
				cancel()
				c.revokeTSORequest(err)
				select {
				case <-time.After(time.Second):
				case <-loopCtx.Done():
					return
				}
				continue
			}
		}

		select {
		case first := <-c.tsoRequests:
			requests = append(requests, first)
			pending := len(c.tsoRequests)
			for i := 0; i < pending; i++ {
				requests = append(requests, <-c.tsoRequests)
			}
			done := make(chan struct{})
			dl := deadline{
				timer:  time.After(pdTimeout),
				done:   done,
				cancel: cancel,
			}
			select {
			case c.tsDeadlineCh <- dl:
			case <-loopCtx.Done():
				return
			}
			err = c.processTSORequests(stream, requests)
			close(done)
			requests = requests[:0]
		case <-loopCtx.Done():
			return
		}

		if err != nil {
			log.Errorf("[pd] getTS error: %v", err)
			c.scheduleCheckLeader()
			cancel()
			stream, cancel = nil, nil
		}
	}
}

func (c *client) processTSORequests(stream pdpb.PD_TsoClient, requests []*tsoRequest) error {
	start := time.Now()
	req := &pdpb.TsoRequest{
		Header: c.requestHeader(),
		Count:  uint32(len(requests)),
	}
	if err := stream.Send(req); err != nil {
		c.finishTSORequest(requests, 0, 0, err)
		return errors.Trace(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		c.finishTSORequest(requests, 0, 0, errors.Trace(err))
		return errors.Trace(err)
	}
	requestDuration.WithLabelValues("tso").Observe(time.Since(start).Seconds())
	if err == nil && resp.GetCount() != uint32(len(requests)) {
		err = errTSOLength
	}
	if err != nil {
		c.finishTSORequest(requests, 0, 0, errors.Trace(err))
		return errors.Trace(err)
	}

	physical, logical := resp.GetTimestamp().GetPhysical(), resp.GetTimestamp().GetLogical()
	// Server returns the highest ts.
	logical -= int64(resp.GetCount() - 1)
	c.finishTSORequest(requests, physical, logical, nil)
	return nil
}

func (c *client) finishTSORequest(requests []*tsoRequest, physical, firstLogical int64, err error) {
	for i := 0; i < len(requests); i++ {
		requests[i].physical, requests[i].logical = physical, firstLogical+int64(i)
		requests[i].done <- err
	}
}

func (c *client) revokeTSORequest(err error) {
	n := len(c.tsoRequests)
	for i := 0; i < n; i++ {
		req := <-c.tsoRequests
		req.done <- errors.Trace(err)
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	c.revokeTSORequest(errClosing)

	c.connMu.Lock()
	defer c.connMu.Unlock()
	for _, cc := range c.connMu.clientConns {
		if err := cc.Close(); err != nil {
			log.Errorf("[pd] failed close grpc clientConn: %v", err)
		}
	}
}

func (c *client) leaderClient() pdpb.PDClient {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	return pdpb.NewPDClient(c.connMu.clientConns[c.connMu.leader])
}

func (c *client) scheduleCheckLeader() {
	select {
	case c.checkLeaderCh <- struct{}{}:
	default:
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

var tsoReqPool = sync.Pool{
	New: func() interface{} {
		return &tsoRequest{
			done: make(chan error, 1),
		}
	},
}

func (c *client) GetTSAsync(ctx context.Context) TSFuture {
	req := tsoReqPool.Get().(*tsoRequest)
	req.start = time.Now()
	req.ctx = ctx
	req.physical = 0
	req.logical = 0
	c.tsoRequests <- req

	return req
}

// TSFuture is a future which promises to return a TSO.
type TSFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

func (req *tsoRequest) Wait() (int64, int64, error) {
	select {
	case err := <-req.done:
		defer tsoReqPool.Put(req)
		if err != nil {
			cmdFailedDuration.WithLabelValues("tso").Observe(time.Since(req.start).Seconds())
			return 0, 0, errors.Trace(err)
		}
		physical, logical := req.physical, req.logical
		cmdDuration.WithLabelValues("tso").Observe(time.Since(req.start).Seconds())
		return physical, logical, err
	case <-req.ctx.Done():
		return 0, 0, errors.Trace(req.ctx.Err())
	}
}

func (c *client) GetTS(ctx context.Context) (int64, int64, error) {
	resp := c.GetTSAsync(ctx)
	return resp.Wait()
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	requestDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_region").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	})
	requestDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_region_byid").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, nil, errors.Trace(err)
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds()) }()
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.leaderClient().GetStore(ctx, &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	})
	requestDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
	cancel()

	if err != nil {
		cmdFailedDuration.WithLabelValues("get_store").Observe(time.Since(start).Seconds())
		c.scheduleCheckLeader()
		return nil, errors.Trace(err)
	}
	store := resp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}

func addrsToUrls(addrs []string) []string {
	// Add default schema "http://" to addrs.
	urls := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if strings.Contains(addr, "://") {
			urls = append(urls, addr)
		} else {
			urls = append(urls, "http://"+addr)
		}
	}
	return urls
}
