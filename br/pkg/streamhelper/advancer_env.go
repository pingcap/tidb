// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	connutil "github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/configtypes"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/httputil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	logBackupServiceID    = "log-backup-coordinator"
	logBackupSafePointTTL = 24 * time.Hour

	// dialTimeOut is the timeout for establishing the connection to a TiKV.
	// when TiKV was disconnected, the request may not be positive rejected but get stuck,
	// which may make the remaining task in a tick fail.
	dialTimeOut = 8 * time.Second
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
	// LogBackupFlushIntervalGetter fetches TiKV log-backup.max-flush-interval for resolving locks.
	LogBackupFlushIntervalGetter
}

// LogBackupFlushIntervalGetter fetches TiKV log-backup.max-flush-interval.
type LogBackupFlushIntervalGetter interface {
	GetLogBackupFlushInterval(ctx context.Context) (time.Duration, error)
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

func (c PDRegionScanner) FetchCurrentTS(ctx context.Context) (uint64, error) {
	return connutil.GetCurrentTsFromPDWithRetry(ctx, c.Client)
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (c PDRegionScanner) RegionScan(ctx context.Context, key, endKey []byte, limit int) ([]RegionWithLeader, error) {
	//nolint:staticcheck
	rs, err := c.Client.ScanRegions(ctx, key, endKey, limit, opt.WithAllowFollowerHandle())
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
	res, err := c.Client.GetAllStores(ctx, opt.WithExcludeTombstone())
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

	fetchTiKVConfigs func(context.Context, func([]byte) error) error
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

func (t clusterEnv) GetLogBackupFlushInterval(ctx context.Context) (time.Duration, error) {
	return GetLogBackupFlushIntervalFromTiKVConfig(ctx, t.fetchTiKVConfigs)
}

// CliEnv creates the Env for CLI usage.
func CliEnv(cli *utils.StoreManager, tikvStore tikv.Storage, etcdCli *clientv3.Client) Env {
	cli.ResetPDClientCallerComponent(caller.Pitr)
	configHTTPPrefix := "http://"
	if cli.TLSConfig() != nil {
		configHTTPPrefix = "https://"
	}
	configHTTPClient := httputil.NewClient(cli.TLSConfig())
	return clusterEnv{
		clis:                 cli,
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{cli.PDClient()},
		AdvancerLockResolver: newAdvancerLockResolver(tikvStore),
		fetchTiKVConfigs: func(ctx context.Context, collect func([]byte) error) error {
			stores, err := connutil.GetAllTiKVStoresWithRetry(ctx, cli.PDClient(), connutil.SkipTiFlash)
			if err != nil {
				return errors.Trace(err)
			}
			return connutil.GetConfigBytesFromTiKVStores(ctx, stores, configHTTPClient, configHTTPPrefix, collect)
		},
	}
}

// TiDBEnv creates the Env by TiDB config.
func TiDBEnv(tikvStore tikv.Storage, pdCli pd.Client, etcdCli *clientv3.Client, conf *config.Config) (Env, error) {
	tconf, err := conf.GetTiKVConfig().Security.ToTLSConfig()
	if err != nil {
		return nil, err
	}
	pitrPDClient := pdCli.WithCallerComponent(caller.Pitr)
	configHTTPClient := tidbutil.InternalHTTPClient()
	configHTTPPrefix := tidbutil.InternalHTTPSchema() + "://"
	env := clusterEnv{
		clis: utils.NewStoreManager(pitrPDClient, keepalive.ClientParameters{
			Time:    time.Duration(conf.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(conf.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
		}, tconf),
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{Client: pitrPDClient},
		AdvancerLockResolver: newAdvancerLockResolver(tikvStore),
		fetchTiKVConfigs: func(ctx context.Context, collect func([]byte) error) error {
			stores, err := connutil.GetAllTiKVStores(ctx, pitrPDClient, connutil.SkipTiFlash)
			if err != nil {
				return errors.Trace(err)
			}
			return connutil.GetConfigBytesFromTiKVStores(ctx, stores, configHTTPClient, configHTTPPrefix, collect)
		},
	}

	env.clis.DialTimeout = dialTimeOut
	return env, nil
}

func GetLogBackupFlushIntervalFromTiKVConfig(
	ctx context.Context,
	fetchTiKVConfigs func(context.Context, func([]byte) error) error,
) (time.Duration, error) {
	var maxFlushInterval time.Duration
	var minFlushInterval time.Duration
	storeCount := 0
	err := fetchTiKVConfigs(ctx, func(resp []byte) error {
		flushInterval, err := parseLogBackupFlushIntervalFromConfig(resp)
		if err != nil {
			log.Warn("failed to parse log-backup.max-flush-interval from TiKV config", zap.Error(err))
			return err
		}
		if storeCount == 0 || flushInterval < minFlushInterval {
			minFlushInterval = flushInterval
		}
		if flushInterval > maxFlushInterval {
			maxFlushInterval = flushInterval
		}
		storeCount++
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	if storeCount == 0 {
		return 0, errors.New("no TiKV config found for log-backup.max-flush-interval")
	}
	if minFlushInterval != maxFlushInterval {
		log.Warn("TiKV log-backup.max-flush-interval is not consistent; use the max value for resolve lock",
			zap.Int("stores", storeCount),
			zap.Duration("min-flush-interval", minFlushInterval),
			zap.Duration("max-flush-interval", maxFlushInterval))
	}
	return maxFlushInterval, nil
}

func parseLogBackupFlushIntervalFromConfig(resp []byte) (time.Duration, error) {
	type logbackup struct {
		MaxFlushInterval *configtypes.Duration `json:"max-flush-interval"`
	}

	type config struct {
		LogBackup logbackup `json:"log-backup"`
	}
	var c config
	e := json.Unmarshal(resp, &c)
	if e != nil {
		return 0, e
	}
	if c.LogBackup.MaxFlushInterval == nil {
		return 0, errors.New("log-backup.max-flush-interval is not found in TiKV config")
	}
	if c.LogBackup.MaxFlushInterval.Duration <= 0 {
		return 0, errors.Errorf("invalid log-backup.max-flush-interval %s", c.LogBackup.MaxFlushInterval)
	}
	return c.LogBackup.MaxFlushInterval.Duration, nil
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
	// GetGlobalCheckpointForTask gets the global checkpoint from the meta store.
	GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error)
	// ClearV3GlobalCheckpointForTask clears the global checkpoint to the meta store.
	ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error
	PauseTask(ctx context.Context, taskName string, opts ...PauseTaskOption) error
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
