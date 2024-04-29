// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	logBackupServiceID    = "log-backup-coordinator"
	logBackupSafePointTTL = 24 * time.Hour
)

// Env is the interface required by the advancer.
type Env interface {
	// The region scanner provides the region information.
	TiKVClusterMeta
	// LogBackupService connects to the TiKV, so we can collect the region checkpoints.
	LogBackupService
	// StreamMeta connects to the metadata service (normally PD).
	StreamMeta
	// GCLockResolver try to resolve locks when region checkpoint stopped.
	tikv.RegionLockResolver
}

// PDRegionScanner is a simple wrapper over PD
// to adapt the requirement of `RegionScan`.
type PDRegionScanner struct {
	pd.Client
}

// Updates the service GC safe point for the cluster.
// Returns the minimal service GC safe point across all services.
// If the arguments is `0`, this would remove the service safe point.
func (c PDRegionScanner) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	minimalSafePoint, err := c.UpdateServiceGCSafePoint(
		ctx, logBackupServiceID, int64(logBackupSafePointTTL.Seconds()), at)
	if err != nil {
		return 0, errors.Annotate(err, "failed to block gc until")
	}
	if minimalSafePoint > at {
		return 0, errors.Errorf("minimal safe point %d is greater than the target %d", minimalSafePoint, at)
	}
	return at, nil
}

func (c PDRegionScanner) UnblockGC(ctx context.Context) error {
	// set ttl to 0, means remove the safe point.
	_, err := c.UpdateServiceGCSafePoint(ctx, logBackupServiceID, 0, math.MaxUint64)
	return err
}

// TODO: It should be able to synchoronize the current TS with the PD.
func (c PDRegionScanner) FetchCurrentTS(ctx context.Context) (uint64, error) {
	return oracle.ComposeTS(time.Now().UnixMilli(), 0), nil
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (c PDRegionScanner) RegionScan(ctx context.Context, key, endKey []byte, limit int) ([]RegionWithLeader, error) {
	rs, err := c.Client.ScanRegions(ctx, key, endKey, limit)
	if err != nil {
		return nil, err
	}
	rls := make([]RegionWithLeader, 0, len(rs))
	for _, r := range rs {
		rls = append(rls, RegionWithLeader{
			Region: r.Meta,
			Leader: r.Leader,
		})
	}
	return rls, nil
}

func (c PDRegionScanner) Stores(ctx context.Context) ([]Store, error) {
	res, err := c.Client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	r := make([]Store, 0, len(res))
	for _, re := range res {
		if !engine.IsTiFlash(re) {
			r = append(r, Store{
				BootAt: uint64(re.StartTimestamp),
				ID:     re.GetId(),
			})
		}
	}
	return r, nil
}

// clusterEnv is the environment for running in the real cluster.
type clusterEnv struct {
	clis *utils.StoreManager
	*AdvancerExt
	PDRegionScanner
	*AdvancerLockResolver
}

var _ Env = &clusterEnv{}

// GetLogBackupClient gets the log backup client.
func (t clusterEnv) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	var cli logbackup.LogBackupClient
	err := t.clis.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = logbackup.NewLogBackupClient(cc)
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// ClearCache clears the log backup client connection cache.
func (t clusterEnv) ClearCache(ctx context.Context, storeID uint64) error {
	return t.clis.RemoveConn(ctx, storeID)
}

// CliEnv creates the Env for CLI usage.
func CliEnv(cli *utils.StoreManager, tikvStore tikv.Storage, etcdCli *clientv3.Client) Env {
	return clusterEnv{
		clis:                 cli,
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{cli.PDClient()},
		AdvancerLockResolver: newAdvancerLockResolver(tikvStore),
	}
}

// TiDBEnv creates the Env by TiDB config.
func TiDBEnv(tikvStore tikv.Storage, pdCli pd.Client, etcdCli *clientv3.Client, conf *config.Config) (Env, error) {
	tconf, err := conf.GetTiKVConfig().Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	return clusterEnv{
		clis: utils.NewStoreManager(pdCli, keepalive.ClientParameters{
			Time:    time.Duration(conf.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(conf.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
		}, tconf),
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{Client: pdCli},
		AdvancerLockResolver: newAdvancerLockResolver(tikvStore),
	}, nil
}

type LogBackupService interface {
	// GetLogBackupClient gets the log backup client.
	GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error)
	// Disable log backup client connection cache.
	ClearCache(ctx context.Context, storeID uint64) error
}

// StreamMeta connects to the metadata service (normally PD).
// It provides the global checkpoint information.
type StreamMeta interface {
	// Begin begins listen the task event change.
	Begin(ctx context.Context, ch chan<- TaskEvent) error
	// UploadV3GlobalCheckpointForTask uploads the global checkpoint to the meta store.
	UploadV3GlobalCheckpointForTask(ctx context.Context, taskName string, checkpoint uint64) error
	// ClearV3GlobalCheckpointForTask clears the global checkpoint to the meta store.
	ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error
	PauseTask(ctx context.Context, taskName string) error
}

var _ tikv.RegionLockResolver = &AdvancerLockResolver{}

type AdvancerLockResolver struct {
	*tikv.BaseRegionLockResolver
}

func newAdvancerLockResolver(store tikv.Storage) *AdvancerLockResolver {
	return &AdvancerLockResolver{
		BaseRegionLockResolver: tikv.NewRegionLockResolver("log backup advancer", store),
	}
}

// ResolveLocksInOneRegion tries to resolve expired locks with this method.
// It will check status of the txn. Resolve the lock if txn is expired, Or do nothing.
func (l *AdvancerLockResolver) ResolveLocksInOneRegion(
	bo *tikv.Backoffer, locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
	_, err := l.GetStore().GetLockResolver().ResolveLocks(bo, 0, locks)
	if err != nil {
		return nil, err
	}
	return loc, nil
}

// If we don't implement GetStore here, it won't complie.
func (l *AdvancerLockResolver) GetStore() tikv.Storage {
	return l.BaseRegionLockResolver.GetStore()
}
