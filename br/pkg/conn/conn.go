// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMergeRegionSizeBytes is the default region split size, 96MB.
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	DefaultMergeRegionSizeBytes uint64 = 96 * units.MiB

	// DefaultMergeRegionKeyCount is the default region key count, 960000.
	DefaultMergeRegionKeyCount uint64 = 960000

	dialTimeout = 30 * time.Second

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

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	*pdutil.PdController
	tlsConf   *tls.Config
	dom       *domain.Domain
	storage   kv.Storage   // Used to access SQL related interfaces.
	tikvStore tikv.Storage // Used to access TiKV specific interfaces.
	grpcClis  struct {
		mu   sync.Mutex
		clis map[uint64]*grpc.ClientConn
	}
	keepalive   keepalive.ClientParameters
	ownsStorage bool
}

// StoreBehavior is the action to do in GetAllTiKVStores when a non-TiKV
// store (e.g. TiFlash store) is found.
type StoreBehavior uint8

const (
	// ErrorOnTiFlash causes GetAllTiKVStores to return error when the store is
	// found to be a TiFlash node.
	ErrorOnTiFlash StoreBehavior = 0
	// SkipTiFlash causes GetAllTiKVStores to skip the store when it is found to
	// be a TiFlash node.
	SkipTiFlash StoreBehavior = 1
	// TiFlashOnly caused GetAllTiKVStores to skip the store which is not a
	// TiFlash node.
	TiFlashOnly StoreBehavior = 2
)

// GetAllTiKVStores returns all TiKV stores registered to the PD client. The
// stores must not be a tombstone and must never contain a label `engine=tiflash`.
func GetAllTiKVStores(
	ctx context.Context,
	pdClient pd.Client,
	storeBehavior StoreBehavior,
) ([]*metapb.Store, error) {
	// get all live stores.
	stores, err := pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// filter out all stores which are TiFlash.
	j := 0
	for _, store := range stores {
		isTiFlash := false
		if version.IsTiFlash(store) {
			if storeBehavior == SkipTiFlash {
				continue
			} else if storeBehavior == ErrorOnTiFlash {
				return nil, errors.Annotatef(berrors.ErrPDInvalidResponse,
					"cannot restore to a cluster with active TiFlash stores (store %d at %s)", store.Id, store.Address)
			}
			isTiFlash = true
		}
		if !isTiFlash && storeBehavior == TiFlashOnly {
			continue
		}
		stores[j] = store
		j++
	}
	return stores[:j], nil
}

func GetAllTiKVStoresWithRetry(ctx context.Context,
	pdClient pd.Client,
	storeBehavior StoreBehavior,
) ([]*metapb.Store, error) {
	stores := make([]*metapb.Store, 0)
	var err error

	errRetry := utils.WithRetry(
		ctx,
		func() error {
			stores, err = GetAllTiKVStores(ctx, pdClient, storeBehavior)
			failpoint.Inject("hint-GetAllTiKVStores-error", func(val failpoint.Value) {
				if val.(bool) {
					logutil.CL(ctx).Debug("failpoint hint-GetAllTiKVStores-error injected.")
					err = status.Error(codes.Unknown, "Retryable error")
				}
			})

			failpoint.Inject("hint-GetAllTiKVStores-cancel", func(val failpoint.Value) {
				if val.(bool) {
					logutil.CL(ctx).Debug("failpoint hint-GetAllTiKVStores-cancel injected.")
					err = status.Error(codes.Canceled, "Cancel Retry")
				}
			})

			return errors.Trace(err)
		},
		utils.NewPDReqBackoffer(),
	)

	return stores, errors.Trace(errRetry)
}

func checkStoresAlive(ctx context.Context,
	pdclient pd.Client,
	storeBehavior StoreBehavior) error {
	// Check live tikv.
	stores, err := GetAllTiKVStores(ctx, pdclient, storeBehavior)
	if err != nil {
		log.Error("fail to get store", zap.Error(err))
		return errors.Trace(err)
	}

	liveStoreCount := 0
	for _, s := range stores {
		if s.GetState() != metapb.StoreState_Up {
			continue
		}
		liveStoreCount++
	}
	log.Info("checked alive KV stores", zap.Int("aliveStores", liveStoreCount), zap.Int("totalStores", len(stores)))
	return nil
}

// NewMgr creates a new Mgr.
//
// Domain is optional for Backup, set `needDomain` to false to disable
// initializing Domain.
func NewMgr(
	ctx context.Context,
	g glue.Glue,
	pdAddrs string,
	tlsConf *tls.Config,
	securityOption pd.SecurityOption,
	keepalive keepalive.ClientParameters,
	storeBehavior StoreBehavior,
	checkRequirements bool,
	needDomain bool,
) (*Mgr, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("conn.NewMgr", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	log.Info("new mgr", zap.String("pdAddrs", pdAddrs))

	controller, err := pdutil.NewPdController(ctx, pdAddrs, tlsConf, securityOption)
	if err != nil {
		log.Error("fail to create pd controller", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if checkRequirements {
		err = version.CheckClusterVersion(ctx, controller.GetPDClient(), version.CheckVersionForBR)
		if err != nil {
			return nil, errors.Annotate(err, "running BR in incompatible version of cluster, "+
				"if you believe it's OK, use --check-requirements=false to skip.")
		}
	}

	err = checkStoresAlive(ctx, controller.GetPDClient(), storeBehavior)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Disable GC because TiDB enables GC already.
	storage, err := g.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddrs), securityOption)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tikvStorage, ok := storage.(tikv.Storage)
	if !ok {
		return nil, berrors.ErrKVNotTiKV
	}

	var dom *domain.Domain
	if needDomain {
		dom, err = g.GetDomain(storage)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	mgr := &Mgr{
		PdController: controller,
		storage:      storage,
		tikvStore:    tikvStorage,
		dom:          dom,
		tlsConf:      tlsConf,
		ownsStorage:  g.OwnsStorage(),
		grpcClis: struct {
			mu   sync.Mutex
			clis map[uint64]*grpc.ClientConn
		}{clis: make(map[uint64]*grpc.ClientConn)},
		keepalive: keepalive,
	}
	return mgr, nil
}

func (mgr *Mgr) getGrpcConnLocked(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
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
	store, err := mgr.GetPDClient().GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithInsecure()
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

// GetBackupClient get or create a backup client.
func (mgr *Mgr) GetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	if ctx.Err() != nil {
		return nil, errors.Trace(ctx.Err())
	}

	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		return backuppb.NewBackupClient(conn), nil
	}

	conn, err := mgr.getGrpcConnLocked(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Cache the conn.
	mgr.grpcClis.clis[storeID] = conn
	return backuppb.NewBackupClient(conn), nil
}

// ResetBackupClient reset the connection for backup client.
func (mgr *Mgr) ResetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	if ctx.Err() != nil {
		return nil, errors.Trace(ctx.Err())
	}

	mgr.grpcClis.mu.Lock()
	defer mgr.grpcClis.mu.Unlock()

	if conn, ok := mgr.grpcClis.clis[storeID]; ok {
		// Find a cached backup client.
		log.Info("Reset backup client", zap.Uint64("storeID", storeID))
		err := conn.Close()
		if err != nil {
			log.Warn("close backup connection failed, ignore it", zap.Uint64("storeID", storeID))
		}
		delete(mgr.grpcClis.clis, storeID)
	}
	var (
		conn *grpc.ClientConn
		err  error
	)
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

// GetStorage returns a kv storage.
func (mgr *Mgr) GetStorage() kv.Storage {
	return mgr.storage
}

// GetTLSConfig returns the tls config.
func (mgr *Mgr) GetTLSConfig() *tls.Config {
	return mgr.tlsConf
}

// GetLockResolver gets the LockResolver.
func (mgr *Mgr) GetLockResolver() *txnlock.LockResolver {
	return mgr.tikvStore.GetLockResolver()
}

// GetDomain returns a tikv storage.
func (mgr *Mgr) GetDomain() *domain.Domain {
	return mgr.dom
}

// Close closes all client in Mgr.
func (mgr *Mgr) Close() {
	mgr.grpcClis.mu.Lock()
	for _, cli := range mgr.grpcClis.clis {
		err := cli.Close()
		if err != nil {
			log.Error("fail to close Mgr", zap.Error(err))
		}
	}
	mgr.grpcClis.mu.Unlock()

	// Gracefully shutdown domain so it does not affect other TiDB DDL.
	// Must close domain before closing storage, otherwise it gets stuck forever.
	if mgr.ownsStorage {
		if mgr.dom != nil {
			mgr.dom.Close()
		}
		tikv.StoreShuttingDown(1)
		mgr.storage.Close()
	}

	mgr.PdController.Close()
}

// GetTS gets current ts from pd.
func (mgr *Mgr) GetTS(ctx context.Context) (uint64, error) {
	p, l, err := mgr.GetPDClient().GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return oracle.ComposeTS(p, l), nil
}

// GetMergeRegionSizeAndCount returns the tikv config `coprocessor.region-split-size` and `coprocessor.region-split-key`.
func (mgr *Mgr) GetMergeRegionSizeAndCount(ctx context.Context, client *http.Client) (uint64, uint64, error) {
	regionSplitSize := DefaultMergeRegionSizeBytes
	regionSplitKeys := DefaultMergeRegionKeyCount
	type coprocessor struct {
		RegionSplitKeys uint64 `json:"region-split-keys"`
		RegionSplitSize string `json:"region-split-size"`
	}

	type config struct {
		Cop coprocessor `json:"coprocessor"`
	}
	err := mgr.GetConfigFromTiKV(ctx, client, func(resp *http.Response) error {
		c := &config{}
		e := json.NewDecoder(resp.Body).Decode(c)
		if e != nil {
			return e
		}
		rs, e := units.RAMInBytes(c.Cop.RegionSplitSize)
		if e != nil {
			return e
		}
		urs := uint64(rs)
		if regionSplitSize == DefaultMergeRegionSizeBytes || urs < regionSplitSize {
			regionSplitSize = urs
			regionSplitKeys = c.Cop.RegionSplitKeys
		}
		return nil
	})
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	return regionSplitSize, regionSplitKeys, nil
}

// GetConfigFromTiKV get configs from all alive tikv stores.
func (mgr *Mgr) GetConfigFromTiKV(ctx context.Context, cli *http.Client, fn func(*http.Response) error) error {
	allStores, err := GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}

	httpPrefix := "http://"
	if mgr.GetTLSConfig() != nil {
		httpPrefix = "https://"
	}

	for _, store := range allStores {
		if store.State != metapb.StoreState_Up {
			continue
		}
		// we need make sure every available store support backup-stream otherwise we might lose data.
		// so check every store's config
		addr, err := handleTiKVAddress(store, httpPrefix)
		if err != nil {
			return err
		}
		configAddr := fmt.Sprintf("%s/config", addr.String())

		err = utils.WithRetry(ctx, func() error {
			resp, e := cli.Get(configAddr)
			if e != nil {
				return e
			}
			err = fn(resp)
			if err != nil {
				return err
			}
			_ = resp.Body.Close()
			return nil
		}, utils.NewPDReqBackoffer())
		if err != nil {
			// if one store failed, break and return error
			return err
		}
	}
	return nil
}

func handleTiKVAddress(store *metapb.Store, httpPrefix string) (*url.URL, error) {
	statusAddr := store.GetStatusAddress()
	nodeAddr := store.GetAddress()
	if !strings.HasPrefix(statusAddr, "http") {
		statusAddr = httpPrefix + statusAddr
	}
	if !strings.HasPrefix(nodeAddr, "http") {
		nodeAddr = httpPrefix + nodeAddr
	}

	statusUrl, err := url.Parse(statusAddr)
	if err != nil {
		return nil, err
	}
	nodeUrl, err := url.Parse(nodeAddr)
	if err != nil {
		return nil, err
	}

	// we try status address as default
	addr := statusUrl
	// but in sometimes we may not get the correct status address from PD.
	if statusUrl.Hostname() != nodeUrl.Hostname() {
		// if not matched, we use the address as default, but change the port
		addr.Host = nodeUrl.Hostname() + ":" + statusUrl.Port()
		log.Warn("store address and status address mismatch the host, we will use the store address as hostname",
			zap.Uint64("store", store.Id),
			zap.String("status address", statusAddr),
			zap.String("node address", nodeAddr),
			zap.Any("request address", statusUrl),
		)
	}
	return addr, nil
}
