package ddl

import (
	"bytes"
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
)

type logReplClientPool struct {
	dialOptions []grpc.DialOption

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]logreplicationpb.LogReplicationClient

	dialGroup singleflight.Group
}

func newLogReplClientPool(dialOptions []grpc.DialOption) *logReplClientPool {
	return &logReplClientPool{
		dialOptions: dialOptions,
		conns:       make(map[string]*grpc.ClientConn),
		clients:     make(map[string]logreplicationpb.LogReplicationClient),
	}
}

func (p *logReplClientPool) Get(ctx context.Context, addr string) (logreplicationpb.LogReplicationClient, error) {
	p.mu.Lock()
	if cli := p.clients[addr]; cli != nil {
		p.mu.Unlock()
		return cli, nil
	}
	p.mu.Unlock()

	v, err, _ := p.dialGroup.Do(addr, func() (any, error) {
		p.mu.Lock()
		if cli := p.clients[addr]; cli != nil {
			p.mu.Unlock()
			return cli, nil
		}
		p.mu.Unlock()

		conn, err := grpc.DialContext(ctx, addr, p.dialOptions...)
		if err != nil {
			return nil, err
		}
		cli := logreplicationpb.NewLogReplicationClient(conn)

		p.mu.Lock()
		if existing := p.clients[addr]; existing != nil {
			p.mu.Unlock()
			_ = conn.Close()
			return existing, nil
		}
		p.conns[addr] = conn
		p.clients[addr] = cli
		p.mu.Unlock()
		return cli, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(logreplicationpb.LogReplicationClient), nil
}

func (p *logReplClientPool) Delete(addr string) {
	p.mu.Lock()
	conn := p.conns[addr]
	delete(p.conns, addr)
	delete(p.clients, addr)
	p.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (p *logReplClientPool) Close() {
	p.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]logreplicationpb.LogReplicationClient)
	p.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

type logReplStateScanner struct {
	pdCli         pd.Client
	clientPool    *logReplClientPool
	storeAddrByID map[uint64]string
}

func newLogReplStateScanner(ctx context.Context, pdAddrs []string) (*logReplStateScanner, error) {
	pdCli, err := pd.NewClientWithContext(ctx, pdAddrs, pkdbPDSecurityOption())
	if err != nil {
		return nil, err
	}
	dialOptions, err := pkdbDebugDialOptions()
	if err != nil {
		pdCli.Close()
		return nil, err
	}
	return &logReplStateScanner{
		pdCli:         pdCli,
		clientPool:    newLogReplClientPool(dialOptions),
		storeAddrByID: make(map[uint64]string),
	}, nil
}

func (s *logReplStateScanner) Close() {
	s.clientPool.Close()
	s.pdCli.Close()
}

func (s *logReplStateScanner) ScanLogReplStates(
	ctx context.Context,
	startKey, endKey []byte,
	limit int,
) ([]*logreplicationpb.LogReplicationState, error) {
	if limit <= 0 {
		return nil, nil
	}

	var states []*logreplicationpb.LogReplicationState
	scanKey := bytes.Clone(startKey)
	for len(states) < limit {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		region, err := s.nextRegion(ctx, scanKey, endKey)
		if err != nil {
			logutil.BgLogger().Warn(
				"log replication state scanner failed to scan target regions, retrying",
				zap.Error(err),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}
		if region == nil {
			return states, nil
		}

		storeAddr, err := pkdbResolveStoreAddr(ctx, s.storeAddrByID, region.Leader.GetStoreId(), s.pdCli)
		if err != nil {
			logutil.BgLogger().Warn(
				"log replication state scanner failed to resolve target store, retrying",
				zap.Error(err),
				zap.Uint64("storeID", region.Leader.GetStoreId()),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}

		cli, err := s.clientPool.Get(ctx, storeAddr)
		if err != nil {
			logutil.BgLogger().Warn(
				"log replication state scanner failed to connect target store, retrying",
				zap.Error(err),
				zap.String("storeAddr", storeAddr),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}

		batch, err := s.scanRegionStates(ctx, cli, region, scanKey, endKey, limit-len(states))
		if err != nil {
			s.clientPool.Delete(storeAddr)
			logutil.BgLogger().Warn(
				"log replication state scanner failed to scan target store, retrying",
				zap.Error(err),
				zap.Uint64("regionID", region.Meta.GetId()),
				zap.String("storeAddr", storeAddr),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}
		states = append(states, batch...)
		if len(states) >= limit {
			break
		}

		nextKey := region.Meta.GetEndKey()
		if len(nextKey) == 0 || len(endKey) > 0 && bytes.Compare(nextKey, endKey) >= 0 {
			break
		}
		if bytes.Compare(nextKey, scanKey) <= 0 {
			return nil, errors.New("target region scan returned non-progressing end key")
		}
		scanKey = bytes.Clone(nextKey)
	}
	return states, nil
}

func (s *logReplStateScanner) nextRegion(ctx context.Context, startKey, endKey []byte) (*pd.Region, error) {
	regions, err := s.pdCli.BatchScanRegions(
		ctx,
		[]pd.KeyRange{{StartKey: startKey, EndKey: endKey}},
		1,
	)
	if err != nil {
		return nil, err
	}
	if len(regions) == 0 {
		return nil, nil
	}
	region := regions[0]
	if region == nil || region.Meta == nil {
		return nil, errors.New("target region scan returned nil region meta")
	}
	if region.Leader == nil || region.Leader.GetStoreId() == 0 {
		return nil, errors.Errorf("target region %d has no leader store", region.Meta.GetId())
	}
	if region.Meta.GetEndKey() != nil && len(region.Meta.GetEndKey()) > 0 &&
		bytes.Compare(region.Meta.GetEndKey(), startKey) <= 0 {
		return nil, errors.Errorf("target region %d ends before scan start key", region.Meta.GetId())
	}
	return region, nil
}

func (s *logReplStateScanner) scanRegionStates(
	ctx context.Context,
	cli logreplicationpb.LogReplicationClient,
	region *pd.Region,
	startKey, endKey []byte,
	limit int,
) ([]*logreplicationpb.LogReplicationState, error) {
	scanEndKey := region.Meta.GetEndKey()
	if len(endKey) > 0 && (len(scanEndKey) == 0 || bytes.Compare(endKey, scanEndKey) < 0) {
		scanEndKey = endKey
	}

	req := &logreplicationpb.ScanLogReplStateRequest{
		Context: &kvrpcpb.Context{
			RegionId:    region.Meta.GetId(),
			RegionEpoch: region.Meta.GetRegionEpoch(),
			Peer:        region.Leader,
		},
		StartKey: startKey,
		EndKey:   scanEndKey,
		Limit:    uint32(limit),
	}
	resp, err := cli.ScanLogReplState(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.GetError() != "" {
		return nil, errors.New(resp.GetError())
	}
	if resp.GetRegionError() != nil {
		return nil, errors.Errorf("target region %d returned region error: %s", region.Meta.GetId(), resp.GetRegionError())
	}
	return resp.GetStates(), nil
}
