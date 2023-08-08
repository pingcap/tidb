// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
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
	gcutil.GCLockResolver
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
	return c.UpdateServiceGCSafePoint(ctx, logBackupServiceID, int64(logBackupSafePointTTL.Seconds()), at)
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

// CliEnv creates the Env for CLI usage.
func CliEnv(cli *utils.StoreManager, tikvStore tikv.Storage, etcdCli *clientv3.Client) Env {
	return clusterEnv{
		clis:                 cli,
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{cli.PDClient()},
		AdvancerLockResolver: &AdvancerLockResolver{TiKvStore: tikvStore},
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
		AdvancerLockResolver: &AdvancerLockResolver{TiKvStore: tikvStore},
	}, nil
}

type LogBackupService interface {
	// GetLogBackupClient gets the log backup client.
	GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error)
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
}

type AdvancerLockResolver struct {
	TiKvStore tikv.Storage
}

func (w *AdvancerLockResolver) LocateKey(bo *tikv.Backoffer, key []byte) (*tikv.KeyLocation, error) {
	return w.TiKvStore.GetRegionCache().LocateKey(bo, key)
}

// ResolveLocks tries to resolve expired locks with batch method.
// Travesal the given locks and check that:
// 1. If the ts of lock is equal with or smaller than forceResolveLocksTS(acually equals safepoint),
// it will rollback the txn, no matter the lock is expired of not.
// 2. If the ts of lock is larger than forceResolveLocksTS, it will check status of the txn.
// Resolve the lock if txn is expired, Or do nothing.
func (w *AdvancerLockResolver) ResolveLocks(bo *tikv.Backoffer, locks []*txnlock.Lock, loc tikv.RegionVerID, safePoint uint64) (bool, error) {
	if len(locks) == 0 {
		return true, nil
	}

	forceResolveLocks := make([]*txnlock.Lock, 0, len(locks))
	tryResolveLocks := make([]*txnlock.Lock, 0, len(locks))
	for _, l := range locks {
		if l.TxnID <= safePoint {
			forceResolveLocks = append(forceResolveLocks, l)
		} else {
			tryResolveLocks = append(tryResolveLocks, l)
		}
	}

	ok, err := w.TiKvStore.GetLockResolver().BatchResolveLocks(bo, forceResolveLocks, loc)
	if err != nil || !ok {
		return ok, err
	}
	_, err = w.TiKvStore.GetLockResolver().ResolveLocks(bo, 0, tryResolveLocks)
	return err == nil, errors.Trace(err)
}

func (w *AdvancerLockResolver) ScanLocks(key []byte, regionID uint64) []*txnlock.Lock {
	return nil
}

func (w *AdvancerLockResolver) SendReq(bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	return w.TiKvStore.SendReq(bo, req, regionID, timeout)
}
