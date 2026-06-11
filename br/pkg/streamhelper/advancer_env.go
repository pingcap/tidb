// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
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
	//nolint:staticcheck
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
	env := clusterEnv{
		clis: utils.NewStoreManager(pdCli, keepalive.ClientParameters{
			Time:    time.Duration(conf.TiKVClient.GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(conf.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
		}, tconf),
		AdvancerExt:          &AdvancerExt{MetaDataClient: *NewMetaDataClient(etcdCli)},
		PDRegionScanner:      PDRegionScanner{Client: pdCli},
		AdvancerLockResolver: newAdvancerLockResolver(tikvStore),
	}

	env.clis.DialTimeout = dialTimeOut
	return env, nil
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

func appendKeyLocationFields(fields []zap.Field, prefix string, loc *tikv.KeyLocation) []zap.Field {
	if loc == nil {
		return append(fields, zap.Bool(prefix+"-location-exists", false))
	}
	return append(fields,
		zap.Bool(prefix+"-location-exists", true),
		zap.Uint64(prefix+"-region-id", loc.Region.GetID()),
		zap.Uint64(prefix+"-region-conf-ver", loc.Region.GetConfVer()),
		zap.Uint64(prefix+"-region-ver", loc.Region.GetVer()),
		logutil.Key(prefix+"-location-start-key", loc.StartKey),
		logutil.Key(prefix+"-location-end-key", loc.EndKey),
	)
}

func appendLockFields(fields []zap.Field, locks []*txnlock.Lock) []zap.Field {
	fields = append(fields, zap.Int("locks", len(locks)))
	if len(locks) == 0 {
		return fields
	}

	minTxnID, maxTxnID := locks[0].TxnID, locks[0].TxnID
	minTTL, maxTTL := locks[0].TTL, locks[0].TTL
	lockSamples := make([]string, 0, min(len(locks), resolveLockTargetSampleLimit))
	for i, lock := range locks {
		if lock.TxnID < minTxnID {
			minTxnID = lock.TxnID
		}
		if lock.TxnID > maxTxnID {
			maxTxnID = lock.TxnID
		}
		if lock.TTL < minTTL {
			minTTL = lock.TTL
		}
		if lock.TTL > maxTTL {
			maxTTL = lock.TTL
		}
		if i < resolveLockTargetSampleLimit {
			lockSamples = append(lockSamples, lock.String())
		}
	}
	fields = append(fields,
		logutil.Key("first-lock-key", locks[0].Key),
		logutil.Key("last-lock-key", locks[len(locks)-1].Key),
		zap.Uint64("min-lock-ttl", minTTL),
		zap.Uint64("max-lock-ttl", maxTTL),
		zap.Strings("lock-sample", lockSamples),
	)
	fields = appendTSFields(fields, "min-lock-txn-id", minTxnID)
	fields = appendTSFields(fields, "max-lock-txn-id", maxTxnID)
	if len(locks) > resolveLockTargetSampleLimit {
		fields = append(fields, zap.Int("lock-sample-omitted", len(locks)-resolveLockTargetSampleLimit))
	}
	return fields
}

// ScanLocksInOneRegion scans locks and emits advancer-specific diagnostics.
func (l *AdvancerLockResolver) ScanLocksInOneRegion(
	bo *tikv.Backoffer, key []byte, maxVersion uint64, scanLimit uint32) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	start := time.Now()
	fields := []zap.Field{
		zap.String("category", "log backup advancer"),
		zap.String("identifier", l.Identifier()),
		logutil.Key("scan-start-key", key),
		zap.Uint32("scan-limit", scanLimit),
	}
	fields = appendTSFields(fields, "scan-max-version", maxVersion)
	locks, loc, err := l.BaseRegionLockResolver.ScanLocksInOneRegion(bo, key, maxVersion, scanLimit)
	fields = appendKeyLocationFields(fields, "scan", loc)
	fields = appendLockFields(fields, locks)
	fields = append(fields, zap.Stringer("take", time.Since(start)))
	if err != nil {
		log.Warn("advancer resolve lock scan failed", append(fields, logutil.ShortError(err))...)
		return nil, loc, err
	}
	if len(locks) == 0 {
		log.Info("advancer resolve lock scan found no locks", fields...)
	} else {
		log.Info("advancer resolve lock scan found locks", fields...)
	}
	return locks, loc, nil
}

// ResolveLocksInOneRegion tries to resolve expired locks with this method.
// It will check status of the txn. Resolve the lock if txn is expired, Or do nothing.
func (l *AdvancerLockResolver) ResolveLocksInOneRegion(
	bo *tikv.Backoffer, locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
	start := time.Now()
	fields := []zap.Field{
		zap.String("category", "log backup advancer"),
		zap.String("identifier", l.Identifier()),
	}
	fields = appendKeyLocationFields(fields, "resolve", loc)
	fields = appendLockFields(fields, locks)
	log.Info("advancer resolve lock batch started", fields...)
	_, err := l.GetStore().GetLockResolver().ResolveLocks(bo, 0, locks)
	if err != nil {
		fields = append(fields, zap.Stringer("take", time.Since(start)))
		log.Warn("advancer resolve lock batch failed", append(fields, logutil.ShortError(err))...)
		return nil, err
	}
	fields = append(fields, zap.Stringer("take", time.Since(start)))
	log.Info("advancer resolve lock batch finished", fields...)
	return loc, nil
}

// If we don't implement GetStore here, it won't complie.
func (l *AdvancerLockResolver) GetStore() tikv.Storage {
	return l.BaseRegionLockResolver.GetStore()
}
