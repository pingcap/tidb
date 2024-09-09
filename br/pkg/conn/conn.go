// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package conn

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/docker/go-units"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	kvconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	// DefaultMergeRegionSizeBytes is the default region split size, 96MB.
	// See https://github.com/tikv/tikv/blob/v4.0.8/components/raftstore/src/coprocessor/config.rs#L35-L38
	DefaultMergeRegionSizeBytes uint64 = 96 * units.MiB

	// DefaultMergeRegionKeyCount is the default region key count, 960000.
	DefaultMergeRegionKeyCount uint64 = 960000

	// DefaultImportNumGoroutines is the default number of threads for import.
	// use 128 as default value, which is 8 times of the default value of tidb.
	// we think is proper for IO-bound cases.
	DefaultImportNumGoroutines uint = 128
)

type VersionCheckerType int

const (
	// default version checker
	NormalVersionChecker VersionCheckerType = iota
	// version checker for PiTR
	StreamVersionChecker
)

// Mgr manages connections to a TiDB cluster.
type Mgr struct {
	*pdutil.PdController
	dom         *domain.Domain
	storage     kv.Storage   // Used to access SQL related interfaces.
	tikvStore   tikv.Storage // Used to access TiKV specific interfaces.
	ownsStorage bool

	*utils.StoreManager
}

func GetAllTiKVStoresWithRetry(ctx context.Context,
	pdClient util.StoreMeta,
	storeBehavior util.StoreBehavior,
) ([]*metapb.Store, error) {
	stores := make([]*metapb.Store, 0)
	var err error

	errRetry := utils.WithRetry(
		ctx,
		func() error {
			stores, err = util.GetAllTiKVStores(ctx, pdClient, storeBehavior)
			failpoint.Inject("hint-GetAllTiKVStores-error", func(val failpoint.Value) {
				logutil.CL(ctx).Debug("failpoint hint-GetAllTiKVStores-error injected.")
				if val.(bool) {
					err = status.Error(codes.Unknown, "Retryable error")
					failpoint.Return(err)
				}
			})

			failpoint.Inject("hint-GetAllTiKVStores-grpc-cancel", func(val failpoint.Value) {
				logutil.CL(ctx).Debug("failpoint hint-GetAllTiKVStores-grpc-cancel injected.")
				if val.(bool) {
					err = status.Error(codes.Canceled, "Cancel Retry")
					failpoint.Return(err)
				}
			})

			failpoint.Inject("hint-GetAllTiKVStores-ctx-cancel", func(val failpoint.Value) {
				logutil.CL(ctx).Debug("failpoint hint-GetAllTiKVStores-ctx-cancel injected.")
				if val.(bool) {
					err = context.Canceled
					failpoint.Return(err)
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
	storeBehavior util.StoreBehavior) error {
	// Check live tikv.
	stores, err := util.GetAllTiKVStores(ctx, pdclient, storeBehavior)
	if err != nil {
		log.Error("failed to get store", zap.Error(err))
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
	pdAddrs []string,
	tlsConf *tls.Config,
	securityOption pd.SecurityOption,
	keepalive keepalive.ClientParameters,
	storeBehavior util.StoreBehavior,
	checkRequirements bool,
	needDomain bool,
	versionCheckerType VersionCheckerType,
) (*Mgr, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("conn.NewMgr", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	log.Info("new mgr", zap.Strings("pdAddrs", pdAddrs))

	controller, err := pdutil.NewPdController(ctx, pdAddrs, tlsConf, securityOption)
	if err != nil {
		log.Error("failed to create pd controller", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if checkRequirements {
		var checker version.VerChecker
		switch versionCheckerType {
		case NormalVersionChecker:
			checker = version.CheckVersionForBR
		case StreamVersionChecker:
			checker = version.CheckVersionForBRPiTR
		default:
			return nil, errors.Errorf("unknown command type, comman code is %d", versionCheckerType)
		}
		err = version.CheckClusterVersion(ctx, controller.GetPDClient(), checker)
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
	path := fmt.Sprintf(
		"tikv://%s?disableGC=true&keyspaceName=%s",
		strings.Join(pdAddrs, ","), config.GetGlobalKeyspaceName(),
	)
	storage, err := g.Open(path, securityOption)
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
		// we must check tidb(tikv version) any time after concurrent ddl feature implemented in v6.2.
		// we will keep this check until 7.0, which allow the breaking changes.
		// NOTE: must call it after domain created!
		// FIXME: remove this check in v7.0
		err = version.CheckClusterVersion(ctx, controller.GetPDClient(), version.CheckVersionForDDL)
		if err != nil {
			return nil, errors.Annotate(err, "unable to check cluster version for ddl")
		}
	}

	mgr := &Mgr{
		PdController: controller,
		storage:      storage,
		tikvStore:    tikvStorage,
		dom:          dom,
		ownsStorage:  g.OwnsStorage(),
		StoreManager: utils.NewStoreManager(controller.GetPDClient(), keepalive, tlsConf),
	}
	return mgr, nil
}

// GetBackupClient get or create a backup client.
func (mgr *Mgr) GetBackupClient(ctx context.Context, storeID uint64) (backuppb.BackupClient, error) {
	var cli backuppb.BackupClient
	if err := mgr.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = backuppb.NewBackupClient(cc)
	}); err != nil {
		return nil, err
	}
	return cli, nil
}

func (mgr *Mgr) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	var cli logbackup.LogBackupClient
	if err := mgr.WithConn(ctx, storeID, func(cc *grpc.ClientConn) {
		cli = logbackup.NewLogBackupClient(cc)
	}); err != nil {
		return nil, err
	}
	return cli, nil
}

// GetStorage returns a kv storage.
func (mgr *Mgr) GetStorage() kv.Storage {
	return mgr.storage
}

// GetTLSConfig returns the tls config.
func (mgr *Mgr) GetTLSConfig() *tls.Config {
	return mgr.StoreManager.TLSConfig()
}

// GetStore gets the tikvStore.
func (mgr *Mgr) GetStore() tikv.Storage {
	return mgr.tikvStore
}

// GetLockResolver gets the LockResolver.
func (mgr *Mgr) GetLockResolver() *txnlock.LockResolver {
	return mgr.tikvStore.GetLockResolver()
}

// GetDomain returns a tikv storage.
func (mgr *Mgr) GetDomain() *domain.Domain {
	return mgr.dom
}

func (mgr *Mgr) Close() {
	if mgr.StoreManager != nil {
		mgr.StoreManager.Close()
	}
	// Gracefully shutdown domain so it does not affect other TiDB DDL.
	// Must close domain before closing storage, otherwise it gets stuck forever.
	if mgr.ownsStorage {
		if mgr.dom != nil {
			mgr.dom.Close()
		}
		tikv.StoreShuttingDown(1)
		_ = mgr.storage.Close()
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

// ProcessTiKVConfigs handle the tikv config for region split size, region split keys, and import goroutines in place.
// It retrieves the config from all alive tikv stores and returns the minimum values.
// If retrieving the config fails, it returns the default config values.
func (mgr *Mgr) ProcessTiKVConfigs(ctx context.Context, cfg *kvconfig.KVConfig, client *http.Client) {
	mergeRegionSize := cfg.MergeRegionSize
	mergeRegionKeyCount := cfg.MergeRegionKeyCount
	importGoroutines := cfg.ImportGoroutines

	if mergeRegionSize.Modified && mergeRegionKeyCount.Modified && importGoroutines.Modified {
		log.Info("no need to retrieve the config from tikv if user has set the config")
		return
	}

	err := mgr.GetConfigFromTiKV(ctx, client, func(resp *http.Response) error {
		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if !mergeRegionSize.Modified || !mergeRegionKeyCount.Modified {
			size, keys, e := kvconfig.ParseMergeRegionSizeFromConfig(respBytes)
			if e != nil {
				log.Warn("Failed to parse region split size and keys from config", logutil.ShortError(e))
				return e
			}
			if mergeRegionKeyCount.Value == DefaultMergeRegionKeyCount || keys < mergeRegionKeyCount.Value {
				mergeRegionSize.Value = size
				mergeRegionKeyCount.Value = keys
			}
		}
		if !importGoroutines.Modified {
			threads, e := kvconfig.ParseImportThreadsFromConfig(respBytes)
			if e != nil {
				log.Warn("Failed to parse import num-threads from config", logutil.ShortError(e))
				return e
			}
			// We use 8 times the default value because it's an IO-bound case.
			if importGoroutines.Value == DefaultImportNumGoroutines || (threads > 0 && threads*8 < importGoroutines.Value) {
				importGoroutines.Value = threads * 8
			}
		}
		// replace the value
		cfg.MergeRegionSize = mergeRegionSize
		cfg.MergeRegionKeyCount = mergeRegionKeyCount
		cfg.ImportGoroutines = importGoroutines
		return nil
	})

	if err != nil {
		log.Warn("Failed to get config from TiKV; using default", logutil.ShortError(err))
	}
}

// IsLogBackupEnabled is used for br to check whether tikv has enabled log backup.
func (mgr *Mgr) IsLogBackupEnabled(ctx context.Context, client *http.Client) (bool, error) {
	logbackupEnable := true
	err := mgr.GetConfigFromTiKV(ctx, client, func(resp *http.Response) error {
		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		enable, err := kvconfig.ParseLogBackupEnableFromConfig(respBytes)
		if err != nil {
			log.Warn("Failed to parse log-backup enable from config", logutil.ShortError(err))
			return err
		}
		logbackupEnable = logbackupEnable && enable
		return nil
	})
	return logbackupEnable, errors.Trace(err)
}

// GetConfigFromTiKV get configs from all alive tikv stores.
func (mgr *Mgr) GetConfigFromTiKV(ctx context.Context, cli *http.Client, fn func(*http.Response) error) error {
	allStores, err := GetAllTiKVStoresWithRetry(ctx, mgr.GetPDClient(), util.SkipTiFlash)
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
			defer resp.Body.Close()
			err = fn(resp)
			if err != nil {
				return err
			}
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
		addr.Host = net.JoinHostPort(nodeUrl.Hostname(), statusUrl.Port())
		log.Warn("store address and status address mismatch the host, we will use the store address as hostname",
			zap.Uint64("store", store.Id),
			zap.String("status address", statusAddr),
			zap.String("node address", nodeAddr),
			zap.Any("request address", statusUrl),
		)
	}
	return addr, nil
}
