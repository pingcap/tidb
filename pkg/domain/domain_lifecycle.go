// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/domain/crossks"
	"github.com/pingcap/tidb/pkg/domain/globalconfigsync"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/perfschema"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, schemaLease time.Duration, statsLease time.Duration, dumpFileGcLease time.Duration, factory pools.Factory) *Domain {
	return NewDomainWithEtcdClient(store, schemaLease, statsLease, dumpFileGcLease, factory, nil, nil, nil)
}

// NewDomainWithEtcdClient creates a new domain with etcd client. Should not create multiple domains for the same store.
func NewDomainWithEtcdClient(
	store kv.Storage,
	schemaLease time.Duration,
	statsLease time.Duration,
	dumpFileGcLease time.Duration,
	factory pools.Factory,
	crossKSSessFactoryGetter func(targetKS string, validator validatorapi.Validator) pools.Factory,
	etcdClient *clientv3.Client,
	schemaFilter issyncer.Filter,
) *Domain {
	intest.Assert(schemaLease > 0, "schema lease should be a positive duration")
	do := &Domain{
		store:             store,
		exit:              make(chan struct{}),
		sysSessionPool:    createInternelSessionPool(systemSessionPoolSize, factory),
		dxfSessionPool:    createInternelSessionPool(dxfSessionPoolSize, factory),
		statsLease:        statsLease,
		schemaLease:       schemaLease,
		slowQuery:         newTopNSlowQueries(config.GetGlobalConfig().InMemSlowQueryTopNNum, time.Hour*24*7, config.GetGlobalConfig().InMemSlowQueryRecentNum),
		dumpFileGcChecker: &dumpFileGcChecker{gcLease: dumpFileGcLease, paths: []string{replayer.GetPlanReplayerDirName(), GetOptimizerTraceDirName(), GetExtractTaskDirName()}},

		crossKSSessFactoryGetter: crossKSSessFactoryGetter,
	}

	do.advancedSysSessionPool = syssession.NewAdvancedSessionPool(systemSessionPoolSize, func() (syssession.SessionContext, error) {
		r, err := factory()
		if err != nil {
			return nil, err
		}
		sctx, ok := r.(syssession.SessionContext)
		intest.Assert(ok, "type: %T should be cast to syssession.SessionContext", r)
		if !ok {
			return nil, errors.Errorf("type: %T cannot be cast to syssession.SessionContext", r)
		}
		return sctx, nil
	})
	do.infoCache = infoschema.NewCache(do.store, int(vardef.SchemaVersionCacheLimit.Load()))
	do.stopAutoAnalyze.Store(false)
	do.wg = util.NewWaitGroupEnhancedWrapper("domain", do.exit, config.GetGlobalConfig().TiDBEnableExitCheck)
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
	do.memoryUsageAlarmHandle = memoryusagealarm.NewMemoryUsageAlarmHandle(do.exit,
		&memoryusagealarm.TiDBConfigProvider{})
	do.serverMemoryLimitHandle = servermemorylimit.NewServerMemoryLimitHandle(do.exit)
	do.sysProcesses = SysProcesses{mu: &sync.RWMutex{}, procMap: make(map[uint64]sysproctrack.TrackProc)}
	do.expiredTimeStamp4PC.expiredTimeStamp = types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp)
	do.etcdClient = etcdClient
	do.isSyncer = issyncer.New(
		do.store,
		do.infoCache,
		do.schemaLease,
		do.sysSessionPool,
		isvalidator.New(do.schemaLease),
		schemaFilter,
	)
	do.initDomainSysVars()

	do.crossKSSessMgr = crossks.NewManager(do.store)
	return do
}

func createInternelSessionPool(capacity int, factory pools.Factory) util.DestroyableSessionPool {
	return util.NewSessionPool(
		capacity, factory,
		func(r pools.Resource) {
			_, ok := r.(sessionctx.Context)
			intest.Assert(ok)
			infosync.StoreInternalSession(r)
		},
		func(r pools.Resource) {
			sctx, ok := r.(sessionctx.Context)
			intest.Assert(ok)
			intest.AssertFunc(func() bool {
				txn, _ := sctx.Txn(false)
				return txn == nil || !txn.Valid()
			})
			infosync.DeleteInternalSession(r)
		},
		func(r pools.Resource) {
			intest.Assert(r != nil)
			infosync.DeleteInternalSession(r)
		},
	)
}

const serverIDForStandalone = 1 // serverID for standalone deployment.

// Init initializes a domain. after return, session can be used to do DMLs but not
// DDLs which can be used after domain Start.
func (do *Domain) Init(
	sysExecutorFactory func(*Domain) (pools.Resource, error),
	ddlInjector func(ddl.DDL, ddl.Executor, *infoschema.InfoCache) *schematracker.Checker,
) error {
	do.sysExecutorFactory = sysExecutorFactory
	perfschema.Init()
	etcdStore, addrs, err := kvstore.GetEtcdAddrs(do.store)
	if err != nil {
		return errors.Trace(err)
	}
	if len(addrs) > 0 {
		cli, err2 := kvstore.NewEtcdCliWithAddrs(addrs, etcdStore)
		if err2 != nil {
			return errors.Trace(err2)
		}
		etcd.SetEtcdCliByNamespace(cli, keyspace.MakeKeyspaceEtcdNamespace(do.store.GetCodec()))

		do.etcdClient = cli

		do.autoidClient = autoid.NewClientDiscover(cli)

		unprefixedEtcdCli, err2 := kvstore.NewEtcdCliWithAddrs(addrs, etcdStore)
		if err2 != nil {
			return errors.Trace(err2)
		}
		do.unprefixedEtcdCli = unprefixedEtcdCli
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	do.ctx = ctx
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancelFunc)
	do.cancelFns.mu.Unlock()

	ddlNotifierStore := notifier.OpenTableStore("mysql", metadef.NotifierTableName)
	do.ddlNotifier = notifier.NewDDLNotifier(
		do.sysSessionPool,
		ddlNotifierStore,
		time.Second,
	)
	// TODO(lance6716): find a more representative place for subscriber
	failpoint.InjectCall("afterDDLNotifierCreated", do.ddlNotifier)

	d := do.ddl
	eBak := do.ddlExecutor
	do.ddl, do.ddlExecutor = ddl.NewDDL(
		ctx,
		ddl.WithEtcdClient(do.etcdClient),
		ddl.WithStore(do.store),
		ddl.WithAutoIDClient(do.autoidClient),
		ddl.WithInfoCache(do.infoCache),
		ddl.WithLease(do.schemaLease),
		ddl.WithSchemaLoader(do.isSyncer),
		ddl.WithEventPublishStore(ddlNotifierStore),
	)

	failpoint.Inject("MockReplaceDDL", func(val failpoint.Value) {
		if val.(bool) {
			do.ddl = d
			do.ddlExecutor = eBak
		}
	})
	var checker *schematracker.Checker
	if ddlInjector != nil {
		checker = ddlInjector(do.ddl, do.ddlExecutor, do.infoCache)
		checker.CreateTestDB(nil)
		do.ddl = checker
		do.ddlExecutor = checker
	}

	// step 1: prepare the info/schema syncer which domain reload needed.
	pdCli, pdHTTPCli := do.GetPDClient(), do.GetPDHTTPClient()
	skipRegisterToDashboard := config.GetGlobalConfig().SkipRegisterToDashboard
	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.ddl.GetID(), do.ServerID,
		do.etcdClient, do.unprefixedEtcdCli, pdCli, pdHTTPCli,
		do.Store().GetCodec(), skipRegisterToDashboard, do.infoCache)
	if err != nil {
		return err
	}
	do.globalCfgSyncer = globalconfigsync.NewGlobalConfigSyncer(pdCli)
	schemaVerSyncer := do.ddl.SchemaSyncer()
	err = schemaVerSyncer.Init(ctx)
	if err != nil {
		return err
	}
	schemaVerSyncer.SetServerInfoSyncer(do.info.ServerInfoSyncer())

	// step 2: initialize the global kill, which depends on `globalInfoSyncer`.`
	if config.GetGlobalConfig().EnableGlobalKill {
		do.connIDAllocator = globalconn.NewGlobalAllocator(do.ServerID, config.GetGlobalConfig().Enable32BitsConnectionID)

		if do.etcdClient != nil {
			err := do.acquireServerID(ctx)
			if err != nil {
				logutil.BgLogger().Error("acquire serverID failed", zap.Error(err))
				do.isLostConnectionToPD.Store(1) // will retry in `do.serverIDKeeper`
			} else {
				if err := do.info.ServerInfoSyncer().StoreServerInfo(context.Background()); err != nil {
					return errors.Trace(err)
				}
				do.isLostConnectionToPD.Store(0)
			}
		} else {
			// set serverID for standalone deployment to enable 'KILL'.
			atomic.StoreUint64(&do.serverID, serverIDForStandalone)
		}
	} else {
		do.connIDAllocator = globalconn.NewSimpleAllocator()
	}

	// should put `initResourceGroupsController` after fetching server ID
	err = do.initResourceGroupsController(ctx, pdCli, do.ServerID())
	if err != nil {
		return err
	}

	do.isSyncer.InitRequiredFields(
		func() sessmgr.InfoSchemaCoordinator {
			if do.info == nil {
				return nil
			}
			return do.info.GetSessionManager()
		},
		schemaVerSyncer,
		do.autoidClient,
		func() (pools.Resource, error) {
			return do.sysExecutorFactory(do)
		},
	)
	// step 3: domain reload the infoSchema.
	if err = do.isSyncer.Reload(); err != nil {
		return err
	}
	if checker != nil {
		checker.InitFromIS(do.InfoSchema())
	}
	return nil
}

// Start starts the domain. After start, DDLs can be executed using session, see
// Init also.
func (do *Domain) Start(startMode ddl.StartMode) error {
	gCfg := config.GetGlobalConfig()
	if gCfg.EnableGlobalKill && do.etcdClient != nil {
		do.wg.Add(1)
		go do.serverIDKeeper()
	}

	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	sysFac := func() (pools.Resource, error) {
		return do.sysExecutorFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 512, 512, resourceIdleTimeout)

	// start the ddl after the domain reload, avoiding some internal sql running before infoSchema construction.
	err := do.ddl.Start(startMode, sysCtxPool)
	if err != nil {
		return err
	}
	do.minJobIDRefresher = do.ddl.GetMinJobIDRefresher()

	do.isSyncer.SetMinJobIDRefresher(do.minJobIDRefresher)
	// Local store needs to get the change information for every DDL state in each session.
	do.wg.Run(func() {
		do.isSyncer.SyncLoop(do.ctx)
	}, "loadSchemaInLoop")
	do.wg.Run(func() {
		do.isSyncer.MDLCheckLoop(do.ctx)
	}, "mdlCheckLoop")
	do.wg.Run(do.topNSlowQueryLoop, "topNSlowQueryLoop")
	do.wg.Run(func() {
		do.info.ServerInfoSyncer().ServerInfoSyncLoop(do.store, do.exit)
	}, "infoSyncerKeeper")
	do.wg.Run(do.globalConfigSyncerKeeper, "globalConfigSyncerKeeper")
	do.wg.Run(do.runawayManager.RunawayRecordFlushLoop, "runawayRecordFlushLoop")
	do.wg.Run(do.runawayManager.RunawayWatchSyncLoop, "runawayWatchSyncLoop")
	do.wg.Run(do.requestUnitsWriterLoop, "requestUnitsWriterLoop")
	skipRegisterToDashboard := gCfg.SkipRegisterToDashboard
	if !skipRegisterToDashboard {
		do.wg.Run(func() {
			do.info.ServerInfoSyncer().TopologySyncLoop(do.exit)
		}, "topologySyncerKeeper")
	}
	pdCli := do.GetPDClient()
	if pdCli != nil {
		do.wg.Run(func() {
			do.closestReplicaReadCheckLoop(do.ctx, pdCli)
		}, "closestReplicaReadCheckLoop")
	}

	if startMode != ddl.BR {
		err = do.initLogBackup(do.ctx, pdCli)
		if err != nil {
			return err
		}
	}

	// right now we only allow access system keyspace info schema after fully bootstrap.
	if kv.IsUserKS(do.store) && startMode == ddl.Normal {
		if err = do.loadSysKSInfoSchema(); err != nil {
			return err
		}
	}

	return nil
}

// TODO: we should sync the system keyspace info schema, not just load it,
// currently, we assume there is no upgrade, so we only load the info schema of
// system keyspace once, and it will not change during the lifetime of the domain,
// it's not right, but it's enough to push subtasks which depends on it forward,
// we will fix it in the future.
func (do *Domain) loadSysKSInfoSchema() error {
	logutil.BgLogger().Info("loading system keyspace info schema")
	_, err := do.GetKSStore(keyspace.System)
	return err
}
