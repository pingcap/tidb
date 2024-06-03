// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"crypto/tls"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout     = 30 * time.Second
	resetRetryTimes = 3
)

// Pool is a lazy pool of gRPC channels.
// When `Get` called, it lazily allocates new connection if connection not full.
// If it's full, then it will return allocated channels round-robin.
type Pool struct {
	mu sync.Mutex

	conns   []*grpc.ClientConn
	next    int
	cap     int
	newConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func (p *Pool) takeConns() (conns []*grpc.ClientConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns, conns = nil, p.conns
	p.next = 0
	return conns
}

// Close closes the conn pool.
func (p *Pool) Close() {
	for _, c := range p.takeConns() {
		if err := c.Close(); err != nil {
			log.Warn("failed to close clientConn", zap.String("target", c.Target()), zap.Error(err))
		}
	}
}

// Get tries to get an existing connection from the pool, or make a new one if the pool not full.
func (p *Pool) Get(ctx context.Context) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.conns) < p.cap {
		c, err := p.newConn(ctx)
		if err != nil {
			return nil, err
		}
		p.conns = append(p.conns, c)
		return c, nil
	}

	conn := p.conns[p.next]
	p.next = (p.next + 1) % p.cap
	return conn, nil
}

// NewConnPool creates a new Pool by the specified conn factory function and capacity.
func NewConnPool(capacity int, newConn func(ctx context.Context) (*grpc.ClientConn, error)) *Pool {
	return &Pool{
		cap:     capacity,
		conns:   make([]*grpc.ClientConn, 0, capacity),
		newConn: newConn,

		mu: sync.Mutex{},
	}
}

type StoreManager struct {
	pdClient pd.Client
	grpcClis struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
	keepalive keepalive.ClientParameters
	tlsConf   *tls.Config
}

func (mgr *StoreManager) GetKeepalive() keepalive.ClientParameters {
	return mgr.keepalive
}

// NewStoreManager create a new manager for gRPC connections to stores.
func NewStoreManager(pdCli pd.Client, kl keepalive.ClientParameters, tlsConf *tls.Config) *StoreManager {
	return &StoreManager{
		pdClient: pdCli,
		grpcClis: struct {
			mu   sync.Mutex
			clis map[uint64]*grpc.ClientConn
		}{clis: make(map[uint64]*grpc.ClientConn)},
		keepalive: kl,
		tlsConf:   tlsConf,
	}
}

func (mgr *StoreManager) PDClient() pd.Client {
	return mgr.pdClient
}

func (mgr *StoreManager) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	failpoint.Inject("hint-get-backup-client", func(v failpoint.Value) {
		log.Info("failpoint hint-get-backup-client injected, "+
			"process will notify the shell.", zap.Uint64("store", storeID))
		if sigFile, ok := v.(string); ok {
			file, err := os.Create(sigFile)
			if err != nil {
				log.Warn("failed to create file for notifying, skipping notify", zap.Error(err))
			}
			if file != nil {
				file.Close()
			}
		}
		time.Sleep(3 * time.Second)
	})
	store, err := mgr.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if mgr.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(mgr.tlsConf))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	log.Info("StoreManager: dialing to store.", zap.String("address", addr), zap.Uint64("store-id", storeID))
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(mgr.keepalive),
	)
	cancel()
	if err != nil {
		return nil, berrors.ErrFailedToConnect.Wrap(err).GenWithStack("failed to make connection to store %d", storeID)
	}
	return conn, nil
}

func (mgr *StoreManager) RemoveConn(ctx context.Context, storeID uint64) error {
	if ctx.Err() != nil {
		return errors.Trace(ctx.Err())
	}

	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		err := conn.Close()
		if err != nil {
			log.Warn("close backup connection failed, ignore it", zap.Uint64("storeID", storeID))
		}
		delete(mgr.grpcClis.clis, storeID)
		return nil
	}
	return nil
}

func (mgr *StoreManager) TryWithConn(ctx context.Context, storeID uint64, f func(*grpc.ClientConn) error) error {
	if ctx.Err() != nil {
		return errors.Trace(ctx.Err())
	}

	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		return f(conn)
	}

	conn, err := mgr.getGrpcConnLocked(ctx, storeID)
	if err != nil {
		return errors.Trace(err)
	}
	// Cache the conn.
	mgr.grpcClis.clis[storeID] = conn
	return f(conn)
}

func (mgr *StoreManager) WithConn(ctx context.Context, storeID uint64, f func(*grpc.ClientConn)) error {
	return mgr.TryWithConn(ctx, storeID, func(cc *grpc.ClientConn) error { f(cc); return nil })
}

// ResetBackupClient reset the connection for backup client.
func (mgr *StoreManager) ResetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	var (
		conn *grpc.ClientConn
		err  error
	)
	err = mgr.RemoveConn(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	for retry := 0; retry < resetRetryTimes; retry++ {
		conn, err = mgr.getGrpcConnLocked(ctx, storeID)
		if err != nil {
			log.Warn("failed to reset grpc connection, retry it",
				zap.Int("retry time", retry), logutil.ShortError(err))
			time.Sleep(time.Duration(retry+3) * time.Second)
			continue
		}
		mgr.grpcClis.clis[storeID] = conn
		break
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return backuppb.NewBackupClient(conn), nil
}

// Close closes all client in Mgr.
func (mgr *StoreManager) Close() {
	if mgr == nil {
		return
	}
	mgr.grpcClis.mu.Lock()
	for _, cli := range mgr.grpcClis.clis {
		err := cli.Close()
		if err != nil {
			log.Error("fail to close Mgr", zap.Error(err))
		}
	}
	mgr.grpcClis.mu.Unlock()
}

func (mgr *StoreManager) TLSConfig() *tls.Config {
	if mgr == nil {
		return nil
	}
	return mgr.tlsConf
}
