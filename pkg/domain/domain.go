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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	meter_config "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/crossks"
	"github.com/pingcap/tidb/pkg/domain/globalconfigsync"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	disthandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/infoschema/perfschema"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lcom "github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	metrics2 "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// NewInstancePlanCache creates a new instance level plan cache, this function is designed to avoid cycle-import.
	NewInstancePlanCache func(softMemLimit, hardMemLimit int64) sessionctx.InstancePlanCache
)

const (
	indexUsageGCDuration  = 30 * time.Minute
	systemSessionPoolSize = 200
	dxfSessionPoolSize    = 100
)

// NewMockDomain is only used for test
func NewMockDomain() *Domain {
	do := &Domain{}
	do.infoCache = infoschema.NewCache(nil, 1)
	do.infoCache.Insert(infoschema.MockInfoSchema(nil), 0)
	return do
}

// Domain manages life cycle of nearly all other components related to SQL execution
// of a TiDB instance, only one domain can exist at a time.
type Domain struct {
	store           kv.Storage
	infoCache       *infoschema.InfoCache
	privHandle      *privileges.Handle
	bindHandle      atomic.Value
	statsHandle     atomic.Pointer[handle.Handle]
	statsLease      time.Duration
	ddl             ddl.DDL
	ddlExecutor     ddl.Executor
	ddlNotifier     *notifier.DDLNotifier
	info            *infosync.InfoSyncer
	isSyncer        *issyncer.Syncer
	globalCfgSyncer *globalconfigsync.GlobalConfigSyncer
	schemaLease     time.Duration
	// advancedSysSessionPool is a more powerful session pool that returns a wrapped session which can detect
	// some misuse of the session to avoid potential bugs.
	// It is recommended to use this pool instead of `sysSessionPool`.
	advancedSysSessionPool *syssession.AdvancedSessionPool
	// Note: If you no longer need the session, you must call Destroy to release it.
	// Otherwise, the session will be leaked. Because there is a strong reference from the domain to the session.
	// Deprecated: Use `advancedSysSessionPool` instead.
	sysSessionPool util.DestroyableSessionPool
	dxfSessionPool util.DestroyableSessionPool
	exit           chan struct{}
	// `etcdClient` must be used when keyspace is not set, or when the logic to each etcd path needs to be separated by keyspace.
	etcdClient *clientv3.Client
	// autoidClient is used when there are tables with AUTO_ID_CACHE=1, it is the client to the autoid service.
	autoidClient *autoid.ClientDiscover
	// `unprefixedEtcdCli` will never set the etcd namespace prefix by keyspace.
	// It is only used in storeMinStartTS and RemoveMinStartTS now.
	// It must be used when the etcd path isn't needed to separate by keyspace.
	// See keyspace RFC: https://github.com/pingcap/tidb/pull/39685
	unprefixedEtcdCli       *clientv3.Client
	sysVarCache             sysVarCache // replaces GlobalVariableCache
	slowQuery               *topNSlowQueries
	expensiveQueryHandle    *expensivequery.Handle
	memoryUsageAlarmHandle  *memoryusagealarm.Handle
	serverMemoryLimitHandle *servermemorylimit.Handle
	// TODO: use Run for each process in future pr
	wg            *util.WaitGroupEnhancedWrapper
	statsUpdating atomicutil.Int32
	// this is the parent context of DDL, and also used by other loops such as closestReplicaReadCheckLoop.
	// there are other top level contexts in the domain, such as the ones used in
	// InitDistTaskLoop and loadStatsWorker, domain only stores the cancelFns of them.
	// TODO unify top level context.
	ctx       context.Context
	cancelFns struct {
		mu  sync.Mutex
		fns []context.CancelFunc
	}
	dumpFileGcChecker   *dumpFileGcChecker
	planReplayerHandle  *planReplayerHandle
	extractTaskHandle   *ExtractHandle
	expiredTimeStamp4PC struct {
		// let `expiredTimeStamp4PC` use its own lock to avoid any block across domain.Reload()
		// and compiler.Compile(), see issue https://github.com/pingcap/tidb/issues/45400
		sync.RWMutex
		expiredTimeStamp types.Time
	}

	brOwnerMgr             owner.Manager
	logBackupAdvancer      *daemon.OwnerDaemon
	historicalStatsWorker  *HistoricalStatsWorker
	ttlJobManager          atomic.Pointer[ttlworker.JobManager]
	mvDemoSchedulerStarted atomicutil.Bool
	runawayManager         *runaway.Manager
	// resourceGroupsController can be changed via `SetResourceGroupsController`
	// in unit test.
	resourceGroupsController atomic.Pointer[rmclient.ResourceGroupsController]

	serverID             uint64
	serverIDSession      *concurrency.Session
	isLostConnectionToPD atomicutil.Int32 // !0: true, 0: false.
	connIDAllocator      globalconn.Allocator

	onClose            func()
	sysExecutorFactory func(*Domain) (pools.Resource, error)

	sysProcesses SysProcesses

	stopAutoAnalyze   atomicutil.Bool
	minJobIDRefresher *systable.MinJobIDRefresher

	instancePlanCache sessionctx.InstancePlanCache // the instance level plan cache

	statsOwner owner.Manager

	// only used for nextgen
	crossKSSessMgr           *crossks.Manager
	crossKSSessFactoryGetter func(string, validatorapi.Validator) pools.Factory
}

var _ sqlsvrapi.Server = (*Domain)(nil)

// InfoCache export for test.
func (do *Domain) InfoCache() *infoschema.InfoCache {
	return do.infoCache
}

// UnprefixedEtcdCli export for test.
func (do *Domain) UnprefixedEtcdCli() *clientv3.Client {
	return do.unprefixedEtcdCli
}

// InfoSchema gets the latest information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoCache.GetLatest()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	// if the snapshotTS is new enough, we can get infoschema directly through snapshotTS.
	if is := do.infoCache.GetBySnapshotTS(snapshotTS); is != nil {
		return is, nil
	}
	is, _, _, _, err := do.isSyncer.LoadWithTS(snapshotTS, true)
	infoschema_metrics.LoadSchemaCounterSnapshot.Inc()
	return is, err
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) meta.Reader {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	return meta.NewReader(snapshot)
}

// ExpiredTimeStamp4PC gets expiredTimeStamp4PC from domain.
func (do *Domain) ExpiredTimeStamp4PC() types.Time {
	do.expiredTimeStamp4PC.RLock()
	defer do.expiredTimeStamp4PC.RUnlock()

	return do.expiredTimeStamp4PC.expiredTimeStamp
}

// SetExpiredTimeStamp4PC sets the expiredTimeStamp4PC from domain.
func (do *Domain) SetExpiredTimeStamp4PC(time types.Time) {
	do.expiredTimeStamp4PC.Lock()
	defer do.expiredTimeStamp4PC.Unlock()

	do.expiredTimeStamp4PC.expiredTimeStamp = time
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// DDLExecutor gets the ddl executor from domain.
func (do *Domain) DDLExecutor() ddl.Executor {
	return do.ddlExecutor
}

// GetDDLOwnerMgr implements the sqlsvrapi.Server interface.
func (do *Domain) GetDDLOwnerMgr() owner.Manager {
	return do.DDL().OwnerManager()
}

// SetDDL sets DDL to domain, it's only used in tests.
func (do *Domain) SetDDL(d ddl.DDL, executor ddl.Executor) {
	do.ddl = d
	do.ddlExecutor = executor
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *infosync.InfoSyncer {
	return do.info
}

// NotifyGlobalConfigChange notify global config syncer to store the global config into PD.
func (do *Domain) NotifyGlobalConfigChange(name, value string) {
	do.globalCfgSyncer.Notify(pd.GlobalConfigItem{Name: name, Value: value, EventType: pdpb.EventType_PUT})
}

// GetGlobalConfigSyncer exports for testing.
func (do *Domain) GetGlobalConfigSyncer() *globalconfigsync.GlobalConfigSyncer {
	return do.globalCfgSyncer
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// GetScope gets the status variables scope.
func (*Domain) GetScope(string) vardef.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	return do.isSyncer.Reload()
}

// GetSchemaValidator returns the schema validator from domain.
func (do *Domain) GetSchemaValidator() validatorapi.Validator {
	return do.isSyncer.GetSchemaValidator()
}

// LogSlowQuery keeps topN recent slow queries in domain.
func (do *Domain) LogSlowQuery(query *SlowQueryInfo) {
	do.slowQuery.mu.RLock()
	defer do.slowQuery.mu.RUnlock()
	if do.slowQuery.mu.closed {
		return
	}

	select {
	case do.slowQuery.ch <- query:
	default:
	}
}

// ShowSlowQuery returns the slow queries.
func (do *Domain) ShowSlowQuery(showSlow *ast.ShowSlow) []*SlowQueryInfo {
	msg := &showSlowMessage{
		request: showSlow,
	}
	msg.Add(1)
	do.slowQuery.msgCh <- msg
	msg.Wait()
	return msg.result
}

func (do *Domain) topNSlowQueryLoop() {
	defer util.Recover(metrics.LabelDomain, "topNSlowQueryLoop", nil, false)
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("topNSlowQueryLoop exited.")
	}()
	for {
		select {
		case now := <-ticker.C:
			do.slowQuery.RemoveExpired(now)
		case info, ok := <-do.slowQuery.ch:
			if !ok {
				return
			}
			do.slowQuery.Append(info)
		case msg := <-do.slowQuery.msgCh:
			req := msg.request
			switch req.Tp {
			case ast.ShowSlowTop:
				msg.result = do.slowQuery.QueryTop(int(req.Count), req.Kind)
			case ast.ShowSlowRecent:
				msg.result = do.slowQuery.QueryRecent(int(req.Count))
			default:
				msg.result = do.slowQuery.QueryAll()
			}
			msg.Done()
		}
	}
}

func (do *Domain) globalConfigSyncerKeeper() {
	defer func() {
		logutil.BgLogger().Info("globalConfigSyncerKeeper exited.")
	}()

	defer util.Recover(metrics.LabelDomain, "globalConfigSyncerKeeper", nil, false)

	for {
		select {
		case entry := <-do.globalCfgSyncer.NotifyCh:
			err := do.globalCfgSyncer.StoreGlobalConfig(context.Background(), entry)
			if err != nil {
				logutil.BgLogger().Error("global config syncer store failed", zap.Error(err))
			}
		// TODO(crazycs520): Add owner to maintain global config is consistency with global variable.
		case <-do.exit:
			return
		}
	}
}

// CheckAutoAnalyzeWindows checks the auto analyze windows and kill the auto analyze process if it is not in the window.
func (do *Domain) CheckAutoAnalyzeWindows() {
	se, err := do.sysSessionPool.Get()

	if err != nil {
		logutil.BgLogger().Warn("get system session failed", zap.Error(err))
		return
	}
	// Make sure the session is new.
	sctx := se.(sessionctx.Context)
	defer do.sysSessionPool.Put(se)
	start, end, ok := autoanalyze.CheckAutoAnalyzeWindow(sctx)
	if !ok {
		for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
			statslogutil.StatsLogger().Warn("Kill auto analyze process because it exceeded the window",
				zap.Uint64("processID", id),
				zap.Time("now", time.Now()),
				zap.String("start", start),
				zap.String("end", end),
			)
			do.SysProcTracker().KillSysProcess(id)
		}
	}
}

func (do *Domain) isClose() bool {
	select {
	case <-do.exit:
		logutil.BgLogger().Info("domain is closed")
		return true
	default:
	}
	return false
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	if do == nil {
		return
	}
	startTime := time.Now()
	if do.ddl != nil {
		terror.Log(do.ddl.Stop())
	}
	ttlJobManager := do.ttlJobManager.Load()
	if ttlJobManager != nil {
		logutil.BgLogger().Info("stopping ttlJobManager")
		ttlJobManager.Stop()
		err := ttlJobManager.WaitStopped(context.Background(), func() time.Duration {
			if intest.InTest {
				return 10 * time.Second
			}
			return 30 * time.Second
		}())
		if err != nil {
			logutil.BgLogger().Warn("fail to wait until the ttl job manager stop", zap.Error(err))
		} else {
			logutil.BgLogger().Info("ttlJobManager exited.")
		}
	}
	do.releaseServerID(context.Background())
	close(do.exit)
	if do.brOwnerMgr != nil {
		do.brOwnerMgr.Close()
	}

	do.runawayManager.Stop()

	do.slowQuery.Close()

	do.cancelFns.mu.Lock()
	for _, f := range do.cancelFns.fns {
		f()
	}
	do.cancelFns.mu.Unlock()

	// Clean etcd session and close the clients.
	// We should wait all the etcd keys keeper to exit
	// in case the keeper rewrite the key after the cleaning.
	do.wg.Wait()
	if do.info != nil {
		do.info.ServerInfoSyncer().RemoveServerInfo()
		do.info.RemoveMinStartTS()
		do.info.ServerInfoSyncer().RemoveTopologyInfo()
	}
	if do.unprefixedEtcdCli != nil {
		terror.Log(errors.Trace(do.unprefixedEtcdCli.Close()))
	}
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}

	do.sysSessionPool.Close()
	do.dxfSessionPool.Close()
	do.advancedSysSessionPool.Close()
	variable.UnregisterStatistics(do.BindingHandle())
	if do.onClose != nil {
		do.onClose()
	}
	gctuner.WaitMemoryLimitTunerExitInTest()

	// close MockGlobalServerInfoManagerEntry in order to refresh mock server info.
	if intest.InTest {
		infosync.MockGlobalServerInfoManagerEntry.Close()
	}
	if handle := do.statsHandle.Load(); handle != nil {
		handle.Close()
	}

	do.crossKSSessMgr.Close()

	logutil.BgLogger().Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

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

// GetKSStore returns the kv.Storage for the given keyspace.
func (do *Domain) GetKSStore(targetKS string) (store kv.Storage, err error) {
	mgr, err := do.crossKSSessMgr.GetOrCreate(targetKS, do.crossKSSessFactoryGetter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mgr.Store(), nil
}

// GetKSInfoCache returns the system keyspace info cache.
func (do *Domain) GetKSInfoCache(targetKS string) (*infoschema.InfoCache, error) {
	mgr, err := do.crossKSSessMgr.GetOrCreate(targetKS, do.crossKSSessFactoryGetter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mgr.InfoCache(), nil
}

// GetKSSessPool returns the session pool for the given keyspace.
func (do *Domain) GetKSSessPool(targetKS string) (util.DestroyableSessionPool, error) {
	mgr, err := do.crossKSSessMgr.GetOrCreate(targetKS, do.crossKSSessFactoryGetter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mgr.SessPool(), nil
}

// CloseKSSessMgr closes the session manager for the given keyspace.
// it's exported for test only.
func (do *Domain) CloseKSSessMgr(targetKS string) {
	do.crossKSSessMgr.CloseKS(targetKS)
}

// GetCrossKSMgr returns the cross keyspace session manager.
// it's exported for test only.
func (do *Domain) GetCrossKSMgr() *crossks.Manager {
	return do.crossKSSessMgr
}

// GetSchemaLease return the schema lease.
func (do *Domain) GetSchemaLease() time.Duration {
	return do.schemaLease
}

// IsLeaseExpired returns whether lease has expired
func (do *Domain) IsLeaseExpired() bool {
	return do.isSyncer.GetSchemaValidator().IsLeaseExpired()
}

// InitInfo4Test init infosync for distributed execution test.
func (do *Domain) InitInfo4Test() {
	infosync.MockGlobalServerInfoManagerEntry.Add(do.ddl.GetID(), do.ServerID)
}

// SetOnClose used to set do.onClose func.
func (do *Domain) SetOnClose(onClose func()) {
	do.onClose = onClose
}

func (do *Domain) initLogBackup(ctx context.Context, pdClient pd.Client) error {
	cfg := config.GetGlobalConfig()
	if pdClient == nil || do.etcdClient == nil {
		log.Warn("pd / etcd client not provided, won't begin Advancer.")
		return nil
	}
	tikvStore, ok := do.Store().(tikv.Storage)
	if !ok {
		log.Warn("non tikv store, stop begin Advancer.")
		return nil
	}
	env, err := streamhelper.TiDBEnv(tikvStore, pdClient, do.etcdClient, cfg)
	if err != nil {
		return err
	}
	adv := streamhelper.NewTiDBCheckpointAdvancer(env)
	do.brOwnerMgr = streamhelper.OwnerManagerForLogBackup(ctx, do.etcdClient)
	do.logBackupAdvancer = daemon.New(adv, do.brOwnerMgr, adv.Config().TickTimeout())
	loop, err := do.logBackupAdvancer.Begin(ctx)
	if err != nil {
		return err
	}
	do.wg.Run(loop, "logBackupAdvancer")
	return nil
}

// when tidb_replica_read = 'closest-adaptive', check tidb and tikv's zone label matches.
// if not match, disable replica_read to avoid uneven read traffic distribution.
func (do *Domain) closestReplicaReadCheckLoop(ctx context.Context, pdClient pd.Client) {
	defer util.Recover(metrics.LabelDomain, "closestReplicaReadCheckLoop", nil, false)

	// trigger check once instantly.
	if err := do.checkReplicaRead(ctx, pdClient); err != nil {
		logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
	}

	ticker := time.NewTicker(time.Minute)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("closestReplicaReadCheckLoop exited.")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := do.checkReplicaRead(ctx, pdClient); err != nil {
				logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
			}
		}
	}
}

// Periodically check and update the replica-read status when `tidb_replica_read` is set to "closest-adaptive"
// We disable "closest-adaptive" in following conditions to ensure the read traffic is evenly distributed across
// all AZs:
// - There are no TiKV servers in the AZ of this tidb instance
// - The AZ if this tidb contains more tidb than other AZ and this tidb's id is the bigger one.
func (do *Domain) checkReplicaRead(ctx context.Context, pdClient pd.Client) error {
	do.sysVarCache.RLock()
	replicaRead := do.sysVarCache.global[vardef.TiDBReplicaRead]
	do.sysVarCache.RUnlock()

	if !strings.EqualFold(replicaRead, "closest-adaptive") {
		logutil.BgLogger().Debug("closest replica read is not enabled, skip check!", zap.String("mode", replicaRead))
		return nil
	}

	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	zone := ""
	for k, v := range serverInfo.Labels {
		if k == placement.DCLabelKey && v != "" {
			zone = v
			break
		}
	}
	if zone == "" {
		logutil.BgLogger().Debug("server contains no 'zone' label, disable closest replica read", zap.Any("labels", serverInfo.Labels))
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	stores, err := pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return err
	}

	storeZones := make(map[string]int)
	for _, s := range stores {
		// skip tumbstone stores or tiflash
		if s.NodeState == metapb.NodeState_Removing || s.NodeState == metapb.NodeState_Removed || engine.IsTiFlash(s) {
			continue
		}
		for _, label := range s.Labels {
			if label.Key == placement.DCLabelKey && label.Value != "" {
				storeZones[label.Value] = 0
				break
			}
		}
	}

	// no stores in this AZ
	if _, ok := storeZones[zone]; !ok {
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	servers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return err
	}
	svrIDsInThisZone := make([]string, 0)
	for _, s := range servers {
		if v, ok := s.Labels[placement.DCLabelKey]; ok && v != "" {
			if _, ok := storeZones[v]; ok {
				storeZones[v]++
				if v == zone {
					svrIDsInThisZone = append(svrIDsInThisZone, s.ID)
				}
			}
		}
	}
	enabledCount := math.MaxInt
	for _, count := range storeZones {
		if count < enabledCount {
			enabledCount = count
		}
	}
	// sort tidb in the same AZ by ID and disable the tidb with bigger ID
	// because ID is unchangeable, so this is a simple and stable algorithm to select
	// some instances across all tidb servers.
	if enabledCount < len(svrIDsInThisZone) {
		sort.Slice(svrIDsInThisZone, func(i, j int) bool {
			return strings.Compare(svrIDsInThisZone[i], svrIDsInThisZone[j]) < 0
		})
	}
	enabled := true
	if slices.Contains(svrIDsInThisZone[enabledCount:], serverInfo.ID) {
		enabled = false
	}

	if variable.SetEnableAdaptiveReplicaRead(enabled) {
		logutil.BgLogger().Info("tidb server adaptive closest replica read is changed", zap.Bool("enable", enabled))
	}
	return nil
}

// InitDistTaskLoop initializes the distributed task framework.
func (do *Domain) InitDistTaskLoop() error {
	taskManager := storage.NewTaskManager(do.dxfSessionPool)
	storage.SetTaskManager(taskManager)
	failpoint.Inject("MockDisableDistTask", func() {
		failpoint.Return(nil)
	})

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	if kv.IsUserKS(do.store) {
		sp, err := do.GetKSSessPool(keyspace.System)
		if err != nil {
			return err
		}
		storage.SetDXFSvcTaskMgr(storage.NewTaskManager(sp))

		// in nextgen, DXF runs as a service on SYSTEM ks and are shared by all
		// user keyspace
		logutil.BgLogger().Info("skip running DXF in user keyspace")
		return nil
	}
	if kv.IsSystemKS(do.store) {
		tidbCfg := config.GetGlobalConfig()
		if tidbCfg.MeteringStorageURI == "" {
			logutil.BgLogger().Warn("metering storage uri is empty, metering will be disabled")
		} else {
			mCfg, err := meter_config.NewFromURI(tidbCfg.MeteringStorageURI)
			if err != nil {
				return errors.Wrap(err, "failed to parse metering storage uri")
			}
			m, err := metering.NewMeter(mCfg)
			if err != nil {
				return errors.Wrap(err, "failed to create metering")
			}
			metering.SetMetering(m)
			do.wg.Run(func() {
				defer func() {
					metering.SetMetering(nil)
				}()
				m.StartFlushLoop(do.ctx)
			}, "dxfMeteringFlushLoop")
		}
	}

	var serverID string
	if intest.InTest {
		do.InitInfo4Test()
		serverID = disttaskutil.GenerateSubtaskExecID4Test(do.ddl.GetID())
	} else {
		serverID = disttaskutil.GenerateSubtaskExecID(ctx, do.ddl.GetID())
	}

	if serverID == "" {
		errMsg := fmt.Sprintf("TiDB node ID( = %s ) not found in available TiDB nodes list", do.ddl.GetID())
		return errors.New(errMsg)
	}
	managerCtx, cancel := context.WithCancel(ctx)
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancel)
	do.cancelFns.mu.Unlock()
	nodeRes, err := calculateNodeResource()
	if err != nil {
		return err
	}
	disthandle.SetNodeResource(nodeRes)
	executorManager, err := taskexecutor.NewManager(managerCtx, do.store, serverID, taskManager, nodeRes)
	if err != nil {
		return err
	}

	if err = executorManager.InitMeta(); err != nil {
		// executor manager loop will try to recover meta repeatedly, so we can
		// just log the error here.
		logutil.BgLogger().Warn("init task executor manager meta failed", zap.Error(err))
	}
	do.wg.Run(func() {
		defer func() {
			storage.SetTaskManager(nil)
		}()
		do.distTaskFrameworkLoop(ctx, taskManager, executorManager, serverID, nodeRes)
	}, "distTaskFrameworkLoop")
	if err := kv.RunInNewTxn(ctx, do.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		logger := logutil.BgLogger()
		return local.InitializeRateLimiterParam(m, logger)
	}); err != nil {
		logutil.BgLogger().Error("initialize global max batch split ranges failed", zap.Error(err))
	}
	return nil
}

func calculateNodeResource() (*proto.NodeResource, error) {
	logger := logutil.ErrVerboseLogger()
	totalMem, err := memory.MemTotal()
	if err != nil {
		// should not happen normally, as in main function of tidb-server, we assert
		// that memory.MemTotal() will not fail.
		return nil, err
	}
	totalCPU := cpu.GetCPUCount()
	if totalCPU <= 0 || totalMem <= 0 {
		return nil, errors.Errorf("invalid cpu or memory, cpu: %d, memory: %d", totalCPU, totalMem)
	}
	var totalDisk uint64
	cfg := config.GetGlobalConfig()
	sz, err := lcom.GetStorageSize(cfg.TempDir)
	if err != nil {
		logger.Warn("get storage size failed, use tidb_ddl_disk_quota instead", zap.Error(err))
		totalDisk = vardef.DDLDiskQuota.Load()
	} else {
		totalDisk = sz.Capacity
	}
	logger.Info("initialize node resource",
		zap.Int("total-cpu", totalCPU),
		zap.String("total-mem", units.BytesSize(float64(totalMem))),
		zap.String("total-disk", units.BytesSize(float64(totalDisk))))
	return proto.NewNodeResource(totalCPU, int64(totalMem), totalDisk), nil
}

func (do *Domain) distTaskFrameworkLoop(ctx context.Context, taskManager *storage.TaskManager, executorManager *taskexecutor.Manager, serverID string, nodeRes *proto.NodeResource) {
	err := executorManager.Start()
	if err != nil {
		logutil.BgLogger().Error("dist task executor manager start failed", zap.Error(err))
		return
	}
	logutil.BgLogger().Info("dist task executor manager started")
	defer func() {
		logutil.BgLogger().Info("stopping dist task executor manager")
		executorManager.Stop()
		logutil.BgLogger().Info("dist task executor manager stopped")
	}()

	var schedulerManager *scheduler.Manager
	startSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			return
		}
		schedulerManager = scheduler.NewManager(ctx, do.store, taskManager, serverID, nodeRes)
		schedulerManager.Start()
	}
	stopSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			logutil.BgLogger().Info("stopping dist task scheduler manager because the current node is not DDL owner anymore", zap.String("id", do.ddl.GetID()))
			schedulerManager.Stop()
			logutil.BgLogger().Info("dist task scheduler manager stopped", zap.String("id", do.ddl.GetID()))
		}
	}

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-do.exit:
			stopSchedulerMgrIfNeeded()
			return
		case <-ticker.C:
			if do.ddl.OwnerManager().IsOwner() {
				startSchedulerMgrIfNeeded()
			} else {
				stopSchedulerMgrIfNeeded()
			}
		}
	}
}

// SysSessionPool returns the system session pool.
// Deprecated: Use AdvancedSysSessionPool instead.
func (do *Domain) SysSessionPool() util.DestroyableSessionPool {
	return do.sysSessionPool
}

// AdvancedSysSessionPool is a more powerful session pool that returns a wrapped session which can detect
// some misuse of the session to avoid potential bugs.
// It is recommended to use this pool instead of `sysSessionPool`.
func (do *Domain) AdvancedSysSessionPool() syssession.Pool {
	return do.advancedSysSessionPool
}

// SysProcTracker returns the system processes tracker.
func (do *Domain) SysProcTracker() sysproctrack.Tracker {
	return &do.sysProcesses
}

// DDLNotifier returns the DDL notifier.
func (do *Domain) DDLNotifier() *notifier.DDLNotifier {
	return do.ddlNotifier
}

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// AutoIDClient returns the autoid client.
func (do *Domain) AutoIDClient() *autoid.ClientDiscover {
	return do.autoidClient
}

// GetPDClient returns the PD client.
func (do *Domain) GetPDClient() pd.Client {
	if store, ok := do.store.(kv.StorageWithPD); ok {
		return store.GetPDClient()
	}
	return nil
}

// GetPDHTTPClient returns the PD HTTP client.
func (do *Domain) GetPDHTTPClient() pdhttp.Client {
	if store, ok := do.store.(kv.StorageWithPD); ok {
		return store.GetPDHTTPClient()
	}
	return nil
}

func (do *Domain) decodePrivilegeEvent(resp clientv3.WatchResponse) PrivilegeEvent {
	var msg PrivilegeEvent
	isNewVersionEvents := false
	for _, event := range resp.Events {
		if event.Kv != nil {
			val := event.Kv.Value
			if len(val) > 0 {
				var tmp PrivilegeEvent
				err := json.Unmarshal(val, &tmp)
				if err != nil {
					logutil.BgLogger().Warn("decodePrivilegeEvent unmarshal fail", zap.Error(err))
					break
				}
				isNewVersionEvents = true
				if do.ServerID() != 0 && tmp.ServerID == do.ServerID() {
					// Skip the events from this TiDB-Server
					continue
				}
				if tmp.All {
					msg.All = true
					break
				}
				// duplicated users in list is ok.
				msg.UserList = append(msg.UserList, tmp.UserList...)
			}
		}
	}

	// In case old version triggers the event, the event value is empty,
	// Then we fall back to the old way: reload all the users.
	if len(msg.UserList) == 0 && !isNewVersionEvents {
		msg.All = true
	}
	return msg
}

func (do *Domain) batchReadMoreData(ch clientv3.WatchChan, event PrivilegeEvent) PrivilegeEvent {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	const maxBatchSize = 128
	for range maxBatchSize {
		select {
		case resp, ok := <-ch:
			if !ok {
				return event
			}
			tmp := do.decodePrivilegeEvent(resp)
			if tmp.All {
				event.All = true
			} else {
				if !event.All {
					event.UserList = append(event.UserList, tmp.UserList...)
				}
			}
			succ := timer.Reset(5 * time.Millisecond)
			if !succ {
				return event
			}
		case <-timer.C:
			return event
		}
	}
	return event
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sctx.GetSessionVars().InRestrictedSQL = true
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "set @@autocommit = 1")
	if err != nil {
		return err
	}
	do.privHandle = privileges.NewHandle(do.SysSessionPool(), sctx.GetSessionVars().GlobalVarsAccessor)

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "loadPrivilegeInLoop", nil, false)

		var count int
		for {
			var event PrivilegeEvent
			select {
			case <-do.exit:
				return
			case resp, ok := <-watchCh:
				if ok {
					count = 0
					event = do.decodePrivilegeEvent(resp)
					event = do.batchReadMoreData(watchCh, event)
				} else {
					if do.ctx.Err() == nil {
						logutil.BgLogger().Warn("load privilege loop watch channel closed")
						watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
						count++
						if count > 10 {
							time.Sleep(time.Duration(count) * time.Second)
						}
						continue
					}
				}
			case <-time.After(duration):
				event.All = true
				event = do.batchReadMoreData(watchCh, event)
			}

			// All events are from this TiDB-Server, skip them
			if !event.All && len(event.UserList) == 0 {
				continue
			}

			err := privReloadEvent(do.privHandle, &event)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}, "loadPrivilegeInLoop")
	return nil
}

func privReloadEvent(h *privileges.Handle, event *PrivilegeEvent) (err error) {
	switch {
	case !vardef.AccelerateUserCreationUpdate.Load():
		err = h.UpdateAll()
	case event.All:
		err = h.UpdateAllActive()
	default:
		err = h.Update(event.UserList)
	}
	return
}

// LoadSysVarCacheLoop create a goroutine loads sysvar cache in a loop,
// it should be called only once in BootstrapSession.
func (do *Domain) LoadSysVarCacheLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := do.rebuildSysVarCache(ctx)
	if err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSysVarCacheLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSysVarCacheLoop", nil, false)

		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}

			failpoint.Inject("skipLoadSysVarCacheLoop", func(val failpoint.Value) {
				// In some pkg integration test, there are many testSuite, and each testSuite has separate storage and
				// `LoadSysVarCacheLoop` background goroutine. Then each testSuite `RebuildSysVarCache` from it's
				// own storage.
				// Each testSuit will also call `checkEnableServerGlobalVar` to update some local variables.
				// That's the problem, each testSuit use different storage to update some same local variables.
				// So just skip `RebuildSysVarCache` in some integration testing.
				if val.(bool) {
					failpoint.Continue()
				}
			})

			if !ok {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			logutil.BgLogger().Debug("Rebuilding sysvar cache from etcd watch event.")
			err := do.rebuildSysVarCache(ctx)
			metrics.LoadSysVarCacheCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Warn("LoadSysVarCacheLoop failed", zap.Error(err))
			}
		}
	}, "LoadSysVarCacheLoop")
	return nil
}

// WatchTiFlashComputeNodeChange create a routine to watch if the topology of tiflash_compute node is changed.
// TODO: tiflashComputeNodeKey is not put to etcd yet(finish this when AutoScaler is done)
//
//	store cache will only be invalidated every n seconds.
func (do *Domain) WatchTiFlashComputeNodeChange() error {
	var watchCh clientv3.WatchChan
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
	}
	duration := 10 * time.Second
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("WatchTiFlashComputeNodeChange exit")
		}()
		defer util.Recover(metrics.LabelDomain, "WatchTiFlashComputeNodeChange", nil, false)

		var count int
		var logCount int
		for {
			ok := true
			var watched bool
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
				watched = true
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("WatchTiFlashComputeNodeChange watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			switch s := do.store.(type) {
			case tikv.Storage:
				logCount++
				s.GetRegionCache().InvalidateTiFlashComputeStores()
				if logCount == 6 {
					// Print log every 6*duration seconds.
					logutil.BgLogger().Debug("tiflash_compute store cache invalied, will update next query", zap.Bool("watched", watched))
					logCount = 0
				}
			default:
				logutil.BgLogger().Debug("No need to watch tiflash_compute store cache for non-tikv store")
				return
			}
		}
	}, "WatchTiFlashComputeNodeChange")
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindingHandle returns domain's bindHandle.
func (do *Domain) BindingHandle() bindinfo.BindingHandle {
	v := do.bindHandle.Load()
	if v == nil {
		return nil
	}
	return v.(bindinfo.BindingHandle)
}

// InitBindingHandle create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) InitBindingHandle() error {
	do.bindHandle.Store(bindinfo.NewBindingHandle(do.sysSessionPool))
	err := do.BindingHandle().LoadFromStorageToCache(true, false)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	owner := do.NewOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
	err = owner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
		return err
	}
	do.globalBindHandleWorkerLoop(owner)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop(owner owner.Manager) {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)

		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		gcBindTicker := time.NewTicker(100 * bindinfo.Lease)
		writeBindingUsageTicker := time.NewTicker(100 * bindinfo.Lease)
		defer func() {
			bindWorkerTicker.Stop()
			gcBindTicker.Stop()
			writeBindingUsageTicker.Stop()
		}()
		for {
			select {
			case <-do.exit:
				do.BindingHandle().Close()
				owner.Close()
				return
			case <-bindWorkerTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.LoadFromStorageToCache(false, false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
			case <-gcBindTicker.C:
				if !owner.IsOwner() {
					continue
				}
				err := do.BindingHandle().GCBinding()
				if err != nil {
					logutil.BgLogger().Error("GC bind record failed", zap.Error(err))
				}
			case <-writeBindingUsageTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.UpdateBindingUsageInfoToStorage()
				if err != nil {
					logutil.BgLogger().Warn("BindingHandle.UpdateBindingUsageInfoToStorage", zap.Error(err))
				}
				// randomize the next write interval to avoid thundering herd problem
				// if there are many tidb servers. The next write interval is [3h, 6h].
				writeBindingUsageTicker.Reset(
					randomDuration(
						3*60*60, // 3h
						6*60*60, // 6h
					),
				)
			}
		}
	}, "globalBindHandleWorkerLoop")
}

func randomDuration(minSeconds, maxSeconds int) time.Duration {
	randomIntervalSeconds := rand.Intn(maxSeconds-minSeconds+1) + minSeconds
	newDuration := time.Duration(randomIntervalSeconds) * time.Second
	return newDuration
}

// TelemetryLoop create a goroutine that reports usage data in a loop, it should be called only once
// in BootstrapSession.
func (do *Domain) TelemetryLoop(ctx sessionctx.Context) {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := telemetry.InitialRun(ctx)
	if err != nil {
		logutil.BgLogger().Warn("Initial telemetry run failed", zap.Error(err))
	}

	reportTicker := time.NewTicker(telemetry.ReportInterval)
	subWindowTicker := time.NewTicker(telemetry.SubWindowSize)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("TelemetryReportLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "TelemetryReportLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-reportTicker.C:
				err := telemetry.ReportUsageData(ctx)
				if err != nil {
					logutil.BgLogger().Warn("TelemetryLoop retports usaged data failed", zap.Error(err))
				}
			case <-subWindowTicker.C:
				telemetry.RotateSubWindow()
			}
		}
	}, "TelemetryLoop")
}

// SetupPlanReplayerHandle setup plan replayer handle
func (do *Domain) SetupPlanReplayerHandle(collectorSctx sessionctx.Context, workersSctxs []sessionctx.Context) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	do.planReplayerHandle = &planReplayerHandle{}
	do.planReplayerHandle.planReplayerTaskCollectorHandle = &planReplayerTaskCollectorHandle{
		ctx:  ctx,
		sctx: collectorSctx,
	}
	taskCH := make(chan *PlanReplayerDumpTask, 16)
	taskStatus := &planReplayerDumpTaskStatus{}
	taskStatus.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
	taskStatus.runningTaskMu.runningTasks = map[replayer.PlanReplayerTaskKey]struct{}{}

	do.planReplayerHandle.planReplayerTaskDumpHandle = &planReplayerTaskDumpHandle{
		taskCH: taskCH,
		status: taskStatus,
	}
	do.planReplayerHandle.planReplayerTaskDumpHandle.workers = make([]*planReplayerTaskDumpWorker, 0)
	for i := range workersSctxs {
		worker := &planReplayerTaskDumpWorker{
			ctx:    ctx,
			sctx:   workersSctxs[i],
			taskCH: taskCH,
			status: taskStatus,
		}
		do.planReplayerHandle.planReplayerTaskDumpHandle.workers = append(do.planReplayerHandle.planReplayerTaskDumpHandle.workers, worker)
	}
}

// RunawayManager returns the runaway manager.
func (do *Domain) RunawayManager() *runaway.Manager {
	return do.runawayManager
}

// ResourceGroupsController returns the resource groups controller.
func (do *Domain) ResourceGroupsController() *rmclient.ResourceGroupsController {
	return do.resourceGroupsController.Load()
}

// SetResourceGroupsController is only used in test.
func (do *Domain) SetResourceGroupsController(controller *rmclient.ResourceGroupsController) {
	do.resourceGroupsController.Store(controller)
}

// SetupHistoricalStatsWorker setups worker
func (do *Domain) SetupHistoricalStatsWorker(ctx sessionctx.Context) {
	do.historicalStatsWorker = &HistoricalStatsWorker{
		tblCH: make(chan int64, 16),
		sctx:  ctx,
	}
}

// SetupDumpFileGCChecker setup sctx
func (do *Domain) SetupDumpFileGCChecker(ctx sessionctx.Context) {
	do.dumpFileGcChecker.setupSctx(ctx)
	do.dumpFileGcChecker.planReplayerTaskStatus = do.planReplayerHandle.status
}

// SetupExtractHandle setups extract handler
func (do *Domain) SetupExtractHandle(sctxs []sessionctx.Context) {
	do.extractTaskHandle = newExtractHandler(do.ctx, sctxs)
}

var planReplayerHandleLease atomic.Uint64

func init() {
	planReplayerHandleLease.Store(uint64(10 * time.Second))
	enableDumpHistoricalStats.Store(true)
}

// DisablePlanReplayerBackgroundJob4Test disable plan replayer handle for test
func DisablePlanReplayerBackgroundJob4Test() {
	planReplayerHandleLease.Store(0)
}

// DisableDumpHistoricalStats4Test disable historical dump worker for test
func DisableDumpHistoricalStats4Test() {
	enableDumpHistoricalStats.Store(false)
}

// StartPlanReplayerHandle start plan replayer handle job
func (do *Domain) StartPlanReplayerHandle() {
	lease := planReplayerHandleLease.Load()
	if lease < 1 {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskCollectHandle started")
		tikcer := time.NewTicker(time.Duration(lease))
		defer func() {
			tikcer.Stop()
			logutil.BgLogger().Info("PlanReplayerTaskCollectHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskCollectHandle", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-tikcer.C:
				err := do.planReplayerHandle.CollectPlanReplayerTask()
				if err != nil {
					logutil.BgLogger().Warn("plan replayer handle collect tasks failed", zap.Error(err))
				}
			}
		}
	}, "PlanReplayerTaskCollectHandle")

	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskDumpHandle started")
		defer func() {
			logutil.BgLogger().Info("PlanReplayerTaskDumpHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpHandle", nil, false)

		for _, worker := range do.planReplayerHandle.planReplayerTaskDumpHandle.workers {
			go worker.run()
		}
		<-do.exit
		do.planReplayerHandle.planReplayerTaskDumpHandle.Close()
	}, "PlanReplayerTaskDumpHandle")
}

// GetPlanReplayerHandle returns plan replayer handle
func (do *Domain) GetPlanReplayerHandle() *planReplayerHandle {
	return do.planReplayerHandle
}

// GetExtractHandle returns extract handle
func (do *Domain) GetExtractHandle() *ExtractHandle {
	return do.extractTaskHandle
}

// GetDumpFileGCChecker returns dump file GC checker for plan replayer and plan trace
func (do *Domain) GetDumpFileGCChecker() *dumpFileGcChecker {
	return do.dumpFileGcChecker
}

// DumpFileGcCheckerLoop creates a goroutine that handles `exit` and `gc`.
func (do *Domain) DumpFileGcCheckerLoop() {
	do.wg.Run(func() {
		logutil.BgLogger().Info("dumpFileGcChecker started")
		gcTicker := time.NewTicker(do.dumpFileGcChecker.gcLease)
		defer func() {
			gcTicker.Stop()
			logutil.BgLogger().Info("dumpFileGcChecker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "dumpFileGcCheckerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-gcTicker.C:
				do.dumpFileGcChecker.GCDumpFiles(time.Hour, time.Hour*24*7)
			}
		}
	}, "dumpFileGcChecker")
}

// GetHistoricalStatsWorker gets historical workers
func (do *Domain) GetHistoricalStatsWorker() *HistoricalStatsWorker {
	return do.historicalStatsWorker
}

// EnableDumpHistoricalStats used to control whether enable dump stats for unit test
var enableDumpHistoricalStats atomic.Bool

// StartHistoricalStatsWorker start historical workers running
func (do *Domain) StartHistoricalStatsWorker() {
	if !enableDumpHistoricalStats.Load() {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("HistoricalStatsWorker started")
		defer func() {
			logutil.BgLogger().Info("HistoricalStatsWorker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "HistoricalStatsWorkerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				close(do.historicalStatsWorker.tblCH)
				return
			case tblID, ok := <-do.historicalStatsWorker.tblCH:
				if !ok {
					return
				}
				err := do.historicalStatsWorker.DumpHistoricalStats(tblID, do.StatsHandle())
				if err != nil {
					logutil.BgLogger().Warn("dump historical stats failed", zap.Error(err), zap.Int64("tableID", tblID))
				}
			}
		}
	}, "HistoricalStatsWorker")
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	return do.statsHandle.Load()
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx context.Context) error {
	h, err := handle.NewHandle(
		ctx,
		do.statsLease,
		do.advancedSysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	h.StartWorker()
	do.statsHandle.Store(h)
	return nil
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	return do.statsUpdating.Load() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	if val {
		do.statsUpdating.Store(1)
	} else {
		do.statsUpdating.Store(0)
	}
}

// LoadAndUpdateStatsLoop loads and updates stats info.
func (do *Domain) LoadAndUpdateStatsLoop(concurrency int) error {
	if err := do.UpdateTableStatsLoop(); err != nil {
		return err
	}
	do.StartLoadStatsSubWorkers(concurrency)
	return nil
}

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop() error {
	statsHandle, err := handle.NewHandle(
		do.ctx,
		do.statsLease,
		do.advancedSysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	statsHandle.StartWorker()
	do.statsHandle.Store(statsHandle)
	do.ddl.RegisterStatsHandle(statsHandle)
	// Negative stats lease indicates that it is in test or in br binary mode, it does not need update.
	if do.statsLease >= 0 {
		do.wg.Run(do.loadStatsWorker, "loadStatsWorker")
	}
	variable.EnableStatsOwner = do.enableStatsOwner
	variable.DisableStatsOwner = do.disableStatsOwner
	do.statsOwner = do.NewOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	do.statsOwner.SetListener(owner.NewListenersWrapper(do.ddlNotifier))
	if config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load() {
		err := do.statsOwner.CampaignOwner()
		if err != nil {
			logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
			return err
		}
	}
	do.wg.Run(func() {
		do.indexUsageWorker()
	}, "indexUsageWorker")
	if do.statsLease <= 0 {
		// For statsLease > 0, `gcStatsWorker` handles the quit of stats owner.
		do.wg.Run(func() { quitStatsOwner(do, do.statsOwner) }, "quitStatsOwner")
		return nil
	}
	waitStartTask := func(do *Domain, fn func()) {
		select {
		case <-do.StatsHandle().InitStatsDone:
		case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
			return
		}
		fn()
	}
	do.SetStatsUpdating(true)
	// The asyncLoadHistogram/dumpColStatsUsageWorker/deltaUpdateTickerWorker doesn't require the stats initialization to be completed.
	// This is because thos workers' primary responsibilities are to update the change delta and handle DDL operations.
	// These tasks need to be in work mod as soon as possible to avoid the problem.
	do.wg.Run(do.asyncLoadHistogram, "asyncLoadHistogram")
	do.wg.Run(do.deltaUpdateTickerWorker, "deltaUpdateTickerWorker")
	do.wg.Run(do.dumpColStatsUsageWorker, "dumpColStatsUsageWorker")
	do.wg.Run(func() { waitStartTask(do, do.gcStatsWorker) }, "gcStatsWorker")

	// Wait for the stats worker to finish the initialization.
	// Otherwise, we may start the auto analyze worker before the stats cache is initialized.
	do.wg.Run(func() { waitStartTask(do, do.autoAnalyzeWorker) }, "autoAnalyzeWorker")
	do.wg.Run(func() { waitStartTask(do, do.analyzeJobsCleanupWorker) }, "analyzeJobsCleanupWorker")
	return nil
}

// enableStatsOwner enables this node to execute stats owner jobs.
// Since ownerManager.CampaignOwner will start a new goroutine to run ownerManager.campaignLoop,
// we should make sure that before invoking enableStatsOwner(), stats owner is DISABLE.
func (do *Domain) enableStatsOwner() error {
	if !do.statsOwner.IsOwner() {
		err := do.statsOwner.CampaignOwner()
		return errors.Trace(err)
	}
	return nil
}

// disableStatsOwner disable this node to execute stats owner.
// We should make sure that before invoking disableStatsOwner(), stats owner is ENABLE.
func (do *Domain) disableStatsOwner() error {
	// disable campaign by interrupting campaignLoop
	do.statsOwner.CampaignCancel()
	return nil
}

func quitStatsOwner(do *Domain, mgr owner.Manager) {
	<-do.exit
	mgr.Close()
}

// StartLoadStatsSubWorkers starts sub workers with new sessions to load stats concurrently.
func (do *Domain) StartLoadStatsSubWorkers(concurrency int) {
	statsHandle := do.StatsHandle()
	for range concurrency {
		do.wg.Add(1)
		go statsHandle.SubLoadWorker(do.exit, do.wg)
	}
	logutil.BgLogger().Info("start load stats sub workers", zap.Int("workerCount", concurrency))
}

// NewOwnerManager returns the owner manager for use outside of the domain.
func (do *Domain) NewOwnerManager(prompt, ownerKey string) owner.Manager {
	id := do.ddl.OwnerManager().ID()
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(do.ctx, id, do.store, ownerKey)
	} else {
		statsOwner = owner.NewOwnerManager(do.ctx, do.etcdClient, prompt, id, ownerKey)
	}
	return statsOwner
}

func (do *Domain) initStats(ctx context.Context) {
	statsHandle := do.StatsHandle()
	// If skip-init-stats is configured, skip the heavy initial stats loading as well.
	// Still close InitStatsDone to unblock waiters that may depend on it.
	if config.GetGlobalConfig().Performance.SkipInitStats {
		close(statsHandle.InitStatsDone)
		statslogutil.StatsLogger().Info("Skipping initial stats due to skip-grant-table being set")
		return
	}

	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("panic when initiating stats", zap.Any("r", r),
				zap.Stack("stack"))
		}
		close(statsHandle.InitStatsDone)
	}()
	t := time.Now()
	liteInitStats := config.GetGlobalConfig().Performance.LiteInitStats
	var err error
	if liteInitStats {
		err = statsHandle.InitStatsLite(ctx)
	} else {
		err = statsHandle.InitStats(ctx, do.InfoSchema())
	}
	if err != nil {
		statslogutil.StatsLogger().Error("Init stats failed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)), zap.Error(err))
	} else {
		statslogutil.StatsLogger().Info("Init stats succeed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)))
	}
}

func (do *Domain) loadStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "loadStatsWorker", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	loadTicker := time.NewTicker(lease)
	defer func() {
		loadTicker.Stop()
		logutil.BgLogger().Info("loadStatsWorker exited.")
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancelFunc)
	do.cancelFns.mu.Unlock()

	do.initStats(ctx)
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.Update(ctx, do.InfoSchema())
			if err != nil {
				logutil.BgLogger().Warn("update stats info failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) asyncLoadHistogram() {
	defer util.Recover(metrics.LabelDomain, "asyncLoadStats", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	cleanupTicker := time.NewTicker(lease)
	defer func() {
		cleanupTicker.Stop()
		logutil.BgLogger().Info("asyncLoadStats exited.")
	}()
	select {
	case <-do.StatsHandle().InitStatsDone:
	case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
		return
	}
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-cleanupTicker.C:
			err = statsHandle.LoadNeededHistograms(do.InfoSchema())
			if err != nil {
				statslogutil.StatsErrVerboseSampleLogger().Warn("Load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) indexUsageWorker() {
	defer util.Recover(metrics.LabelDomain, "indexUsageWorker", nil, false)
	gcStatsTicker := time.NewTicker(indexUsageGCDuration)
	handle := do.StatsHandle()
	defer func() {
		logutil.BgLogger().Info("indexUsageWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			return
		case <-gcStatsTicker.C:
			if err := handle.GCIndexUsage(); err != nil {
				statslogutil.StatsLogger().Error("gc index usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) gcStatsWorkerExitPreprocessing() {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("gcStatsWorker ready to release owner")
		do.statsOwner.Close()
		ch <- struct{}{}
	}()
	if intest.InTest {
		// We should wait for statistics owner to close on exit.
		// Otherwise, the goroutine leak detection may fail.
		<-ch
		logutil.BgLogger().Info("gcStatsWorker exit preprocessing finished")
		return
	}
	select {
	case <-ch:
		logutil.BgLogger().Info("gcStatsWorker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("gcStatsWorker exit preprocessing timeout, force exiting")
		return
	}
}

func (*Domain) deltaUpdateTickerWorkerExitPreprocessing(statsHandle *handle.Handle) {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("deltaUpdateTicker is going to exit, start to flush stats")
		statsHandle.FlushStats()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		logutil.BgLogger().Info("deltaUpdateTicker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("deltaUpdateTicker exit preprocessing timeout, force exiting")
		return
	}
}

func (do *Domain) gcStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)
	logutil.BgLogger().Info("gcStatsWorker started.")
	lease := do.statsLease
	gcStatsTicker := time.NewTicker(100 * lease)
	updateStatsHealthyTicker := time.NewTicker(20 * lease)
	readMemTicker := time.NewTicker(memory.ReadMemInterval)
	statsHandle := do.StatsHandle()
	defer func() {
		gcStatsTicker.Stop()
		readMemTicker.Stop()
		updateStatsHealthyTicker.Stop()
		do.SetStatsUpdating(false)
		logutil.BgLogger().Info("gcStatsWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)

	for {
		select {
		case <-do.exit:
			do.gcStatsWorkerExitPreprocessing()
			return
		case <-gcStatsTicker.C:
			if !do.statsOwner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.InfoSchema(), do.GetSchemaLease())
			if err != nil {
				logutil.BgLogger().Warn("GC stats failed", zap.Error(err))
			}
			do.CheckAutoAnalyzeWindows()
		case <-readMemTicker.C:
			memory.ForceReadMemStats()
			do.StatsHandle().StatsCache.TriggerEvict()
		case <-updateStatsHealthyTicker.C:
			statsHandle.UpdateStatsHealthyMetrics()
		}
	}
}

func (do *Domain) dumpColStatsUsageWorker() {
	logutil.BgLogger().Info("dumpColStatsUsageWorker started.")
	// We need to have different nodes trigger tasks at different times to avoid the herd effect.
	randDuration := time.Duration(rand.Int63n(int64(time.Minute)))
	dumpDuration := 100*do.statsLease + randDuration
	dumpColStatsUsageTicker := time.NewTicker(dumpDuration)
	statsHandle := do.StatsHandle()
	defer func() {
		dumpColStatsUsageTicker.Stop()
		logutil.BgLogger().Info("dumpColStatsUsageWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "dumpColStatsUsageWorker", nil, false)

	for {
		select {
		case <-do.exit:
			return
		case <-dumpColStatsUsageTicker.C:
			err := statsHandle.DumpColStatsUsageToKV()
			if err != nil {
				logutil.BgLogger().Warn("dump column stats usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) deltaUpdateTickerWorker() {
	defer util.Recover(metrics.LabelDomain, "deltaUpdateTickerWorker", nil, false)
	logutil.BgLogger().Info("deltaUpdateTickerWorker started.")
	lease := do.statsLease
	// We need to have different nodes trigger tasks at different times to avoid the herd effect.
	randDuration := time.Duration(rand.Int63n(int64(time.Minute)))
	updateDuration := 20*lease + randDuration
	failpoint.Inject("deltaUpdateDuration", func() {
		updateDuration = 20 * time.Second
	})

	deltaUpdateTicker := time.NewTicker(updateDuration)
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-do.exit:
			do.deltaUpdateTickerWorkerExitPreprocessing(statsHandle)
			return
		case <-deltaUpdateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV(false)
			if err != nil {
				logutil.BgLogger().Warn("dump stats delta failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker() {
	defer util.Recover(metrics.LabelDomain, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		statslogutil.StatsLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			// In order to prevent tidb from being blocked by the auto analyze task during shutdown,
			// a stopautoanalyze is added here for judgment.
			//
			// The reason for checking of stopAutoAnalyze is following:
			// According to the issue#41318, if we don't check stopAutoAnalyze here, the autoAnalyzeWorker will be tricker
			// again when domain.exit is true.
			// The "case <-analyzeTicker.C" condition and "case <-do.exit" condition are satisfied at the same time
			// when the system is already executing the shutdown task.
			// At this time, the Go language will randomly select a case that meets the conditions to execute,
			// and there is a probability that a new autoanalyze task will be started again
			// when the system has already executed the shutdown.
			// Because the time interval of statsLease is much smaller than the execution speed of auto analyze.
			// Therefore, when the current auto analyze is completed,
			// the probability of this happening is very high that the ticker condition and exist condition will be met
			// at the same time.
			// This causes the auto analyze task to be triggered all the time and block the shutdown of tidb.
			if vardef.RunAutoAnalyze.Load() && !do.stopAutoAnalyze.Load() && do.statsOwner.IsOwner() {
				statsHandle.HandleAutoAnalyze()
			} else if !vardef.RunAutoAnalyze.Load() || !do.statsOwner.IsOwner() {
				// Once the auto analyze is disabled or this instance is not the owner,
				// we close the priority queue to release resources.
				// This would guarantee that when auto analyze is re-enabled or this instance becomes the owner again,
				// the priority queue would be re-initialized.
				statsHandle.ClosePriorityQueue()
			}
		case <-do.exit:
			return
		}
	}
}

// analyzeJobsCleanupWorker is a background worker that periodically performs two main tasks:
//
//  1. Garbage Collection: It removes outdated analyze jobs from the statistics handle.
//     This operation is performed every hour and only if the current instance is the owner.
//     Analyze jobs older than 7 days are considered outdated and are removed.
//
//  2. Cleanup: It cleans up corrupted analyze jobs.
//     A corrupted analyze job is one that is in a 'pending' or 'running' state,
//     but is associated with a TiDB instance that is either not currently running or has been restarted.
//     Also, if the analyze job is killed by the user, it is considered corrupted.
//     This operation is performed every 100 stats leases.
//     It first retrieves the list of current analyze processes, then removes any analyze job
//     that is not associated with a current process. Additionally, if the current instance is the owner,
//     it also cleans up corrupted analyze jobs on dead instances.
func (do *Domain) analyzeJobsCleanupWorker() {
	defer util.Recover(metrics.LabelDomain, "analyzeJobsCleanupWorker", nil, false)
	// For GC.
	const gcInterval = time.Hour
	const daysToKeep = 7
	gcTicker := time.NewTicker(gcInterval)
	// For clean up.
	// Default stats lease is 3 * time.Second.
	// So cleanupInterval is 100 * 3 * time.Second = 5 * time.Minute.
	var cleanupInterval = do.statsLease * 100
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer func() {
		gcTicker.Stop()
		cleanupTicker.Stop()
		logutil.BgLogger().Info("analyzeJobsCleanupWorker exited.")
	}()
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-gcTicker.C:
			// Only the owner should perform this operation.
			if do.statsOwner.IsOwner() {
				updateTime := time.Now().AddDate(0, 0, -daysToKeep)
				err := statsHandle.DeleteAnalyzeJobs(updateTime)
				if err != nil {
					logutil.BgLogger().Warn("gc analyze history failed", zap.Error(err))
				}
			}
		case <-cleanupTicker.C:
			sm := do.InfoSyncer().GetSessionManager()
			if sm == nil {
				continue
			}
			analyzeProcessIDs := make(map[uint64]struct{}, 8)
			for _, process := range sm.ShowProcessList() {
				if isAnalyzeTableSQL(process.Info) {
					analyzeProcessIDs[process.ID] = struct{}{}
				}
			}

			err := statsHandle.CleanupCorruptedAnalyzeJobsOnCurrentInstance(analyzeProcessIDs)
			if err != nil {
				logutil.BgLogger().Warn("cleanup analyze jobs on current instance failed", zap.Error(err))
			}

			if do.statsOwner.IsOwner() {
				err = statsHandle.CleanupCorruptedAnalyzeJobsOnDeadInstances()
				if err != nil {
					logutil.BgLogger().Warn("cleanup analyze jobs on dead instances failed", zap.Error(err))
				}
			}
		case <-do.exit:
			return
		}
	}
}

func isAnalyzeTableSQL(sql string) bool {
	// Get rid of the comments.
	normalizedSQL := parser.Normalize(sql, "ON")
	return strings.HasPrefix(normalizedSQL, "analyze table")
}

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Domain) ExpensiveQueryHandle() *expensivequery.Handle {
	return do.expensiveQueryHandle
}

// MemoryUsageAlarmHandle returns the memory usage alarm handle.
func (do *Domain) MemoryUsageAlarmHandle() *memoryusagealarm.Handle {
	return do.memoryUsageAlarmHandle
}

// ServerMemoryLimitHandle returns the expensive query handle.
func (do *Domain) ServerMemoryLimitHandle() *servermemorylimit.Handle {
	return do.serverMemoryLimitHandle
}

const (
	privilegeKey          = "/tidb/privilege"
	sysVarCacheKey        = "/tidb/sysvars"
	tiflashComputeNodeKey = "/tiflash/new_tiflash_compute_nodes"
)

// PrivilegeEvent is the message definition for NotifyUpdatePrivilege(), encoded in json.
// TiDB old version do not use no such message.
type PrivilegeEvent struct {
	All      bool
	ServerID uint64
	UserList []string
}

// NotifyUpdateAllUsersPrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdateAllUsersPrivilege() error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{All: true})
}

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(userList []string) error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{UserList: userList})
}

func (do *Domain) notifyUpdatePrivilege(event PrivilegeEvent) error {
	// No matter skip-grant-table is configured or not, sending an etcd message is required.
	// Because we need to tell other TiDB instances to update privilege data, say, we're changing the
	// password using a special TiDB instance and want the new password to take effect.
	if do.etcdClient != nil {
		event.ServerID = do.serverID
		data, err := json.Marshal(event)
		if err != nil {
			return errors.Trace(err)
		}
		if uint64(len(data)) > size.MB {
			logutil.BgLogger().Warn("notify update privilege message too large", zap.ByteString("value", data))
		}
		err = ddlutil.PutKVToEtcd(do.ctx, do.etcdClient, etcd.KeyOpDefaultRetryCnt, privilegeKey, string(data))
		if err != nil {
			logutil.BgLogger().Warn("notify update privilege failed", zap.Error(err))
		}
	}

	// If skip-grant-table is configured, do not flush privileges.
	// Because LoadPrivilegeLoop does not run and the privilege Handle is nil,
	// the call to do.PrivilegeHandle().Update would panic.
	if config.GetGlobalConfig().Security.SkipGrantTable {
		return nil
	}

	return privReloadEvent(do.PrivilegeHandle(), &event)
}

// NotifyUpdateSysVarCache updates the sysvar cache key in etcd, which other TiDB
// clients are subscribed to for updates. For the caller, the cache is also built
// synchronously so that the effect is immediate.
func (do *Domain) NotifyUpdateSysVarCache(updateLocal bool) {
	if do.etcdClient != nil {
		err := ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcd.KeyOpDefaultRetryCnt, sysVarCacheKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update sysvar cache failed", zap.Error(err))
		}
	}
	// update locally
	if updateLocal {
		if err := do.rebuildSysVarCache(nil); err != nil {
			logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
		}
	}
}

// LoadSigningCertLoop loads the signing cert periodically to make sure it's fresh new.
func (do *Domain) LoadSigningCertLoop(signingCert, signingKey string) {
	sessionstates.SetCertPath(signingCert)
	sessionstates.SetKeyPath(signingKey)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Debug("loadSigningCertLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSigningCertLoop", nil, false)

		for {
			select {
			case <-time.After(sessionstates.LoadCertInterval):
				sessionstates.ReloadSigningCert()
			case <-do.exit:
				return
			}
		}
	}, "loadSigningCertLoop")
}

// ServerID gets serverID.
func (do *Domain) ServerID() uint64 {
	return atomic.LoadUint64(&do.serverID)
}

// IsLostConnectionToPD indicates lost connection to PD or not.
func (do *Domain) IsLostConnectionToPD() bool {
	return do.isLostConnectionToPD.Load() != 0
}

// NextConnID return next connection ID.
func (do *Domain) NextConnID() uint64 {
	return do.connIDAllocator.NextID()
}

// ReleaseConnID releases connection ID.
func (do *Domain) ReleaseConnID(connID uint64) {
	do.connIDAllocator.Release(connID)
}

const (
	serverIDEtcdPath               = "/tidb/server_id"
	refreshServerIDRetryCnt        = 3
	acquireServerIDRetryInterval   = 300 * time.Millisecond
	acquireServerIDTimeout         = 10 * time.Second
	retrieveServerIDSessionTimeout = 10 * time.Second

	acquire32BitsServerIDRetryCnt = 3
)

var (
	// serverIDTTL should be LONG ENOUGH to avoid barbarically killing an on-going long-run SQL.
	serverIDTTL = 12 * time.Hour
	// serverIDTimeToKeepAlive is the interval that we keep serverID TTL alive periodically.
	serverIDTimeToKeepAlive = 5 * time.Minute
	// serverIDTimeToCheckPDConnectionRestored is the interval that we check connection to PD restored (after broken) periodically.
	serverIDTimeToCheckPDConnectionRestored = 10 * time.Second
	// lostConnectionToPDTimeout is the duration that when TiDB cannot connect to PD excceeds this limit,
	//   we realize the connection to PD is lost utterly, and server ID acquired before should be released.
	//   Must be SHORTER than `serverIDTTL`.
	lostConnectionToPDTimeout = 6 * time.Hour
)

var (
	ldflagIsGlobalKillTest                        = "0"  // 1:Yes, otherwise:No.
	ldflagServerIDTTL                             = "10" // in seconds.
	ldflagServerIDTimeToKeepAlive                 = "1"  // in seconds.
	ldflagServerIDTimeToCheckPDConnectionRestored = "1"  // in seconds.
	ldflagLostConnectionToPDTimeout               = "5"  // in seconds.
)

func initByLDFlagsForGlobalKill() {
	if ldflagIsGlobalKillTest == "1" {
		var (
			i   int
			err error
		)

		if i, err = strconv.Atoi(ldflagServerIDTTL); err != nil {
			panic("invalid ldflagServerIDTTL")
		}
		serverIDTTL = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToKeepAlive); err != nil {
			panic("invalid ldflagServerIDTimeToKeepAlive")
		}
		serverIDTimeToKeepAlive = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToCheckPDConnectionRestored); err != nil {
			panic("invalid ldflagServerIDTimeToCheckPDConnectionRestored")
		}
		serverIDTimeToCheckPDConnectionRestored = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagLostConnectionToPDTimeout); err != nil {
			panic("invalid ldflagLostConnectionToPDTimeout")
		}
		lostConnectionToPDTimeout = time.Duration(i) * time.Second

		logutil.BgLogger().Info("global_kill_test is enabled", zap.Duration("serverIDTTL", serverIDTTL),
			zap.Duration("serverIDTimeToKeepAlive", serverIDTimeToKeepAlive),
			zap.Duration("serverIDTimeToCheckPDConnectionRestored", serverIDTimeToCheckPDConnectionRestored),
			zap.Duration("lostConnectionToPDTimeout", lostConnectionToPDTimeout))
	}
}

func (do *Domain) retrieveServerIDSession(ctx context.Context) (*concurrency.Session, error) {
	if do.serverIDSession != nil {
		return do.serverIDSession, nil
	}

	// `etcdClient.Grant` needs a shortterm timeout, to avoid blocking if connection to PD lost,
	//   while `etcdClient.KeepAlive` should be longterm.
	//   So we separately invoke `etcdClient.Grant` and `concurrency.NewSession` with leaseID.
	childCtx, cancel := context.WithTimeout(ctx, retrieveServerIDSessionTimeout)
	resp, err := do.etcdClient.Grant(childCtx, int64(serverIDTTL.Seconds()))
	cancel()
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.Grant fail", zap.Error(err))
		return nil, err
	}
	leaseID := resp.ID

	session, err := concurrency.NewSession(do.etcdClient,
		concurrency.WithLease(leaseID), concurrency.WithContext(context.Background()))
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.NewSession fail", zap.Error(err))
		return nil, err
	}
	do.serverIDSession = session
	return session, nil
}

func (do *Domain) acquireServerID(ctx context.Context) error {
	atomic.StoreUint64(&do.serverID, 0)

	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	conflictCnt := 0
	for {
		var proposeServerID uint64
		if config.GetGlobalConfig().Enable32BitsConnectionID {
			proposeServerID, err = do.proposeServerID(ctx, conflictCnt)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			// get a random serverID: [1, MaxServerID64]
			proposeServerID = uint64(rand.Int63n(int64(globalconn.MaxServerID64)) + 1) // #nosec G404
		}

		key := fmt.Sprintf("%s/%v", serverIDEtcdPath, proposeServerID)
		cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		value := "0"

		childCtx, cancel := context.WithTimeout(ctx, acquireServerIDTimeout)
		txn := do.etcdClient.Txn(childCtx)
		t := txn.If(cmp)
		resp, err := t.Then(clientv3.OpPut(key, value, clientv3.WithLease(session.Lease()))).Commit()
		cancel()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			logutil.BgLogger().Info("propose serverID exists, try again", zap.Uint64("proposeServerID", proposeServerID))
			time.Sleep(acquireServerIDRetryInterval)
			conflictCnt++
			continue
		}

		atomic.StoreUint64(&do.serverID, proposeServerID)
		logutil.BgLogger().Info("acquireServerID", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
		return nil
	}
}

func (do *Domain) releaseServerID(context.Context) {
	serverID := do.ServerID()
	if serverID == 0 {
		return
	}
	atomic.StoreUint64(&do.serverID, 0)

	if do.etcdClient == nil {
		return
	}

	// closing session releases attached server id and etcd lease.
	leaseID := int64(do.serverIDSession.Lease())
	if err := do.serverIDSession.Close(); err != nil {
		logutil.BgLogger().Error("releaseServerID fail",
			zap.Uint64("serverID", serverID),
			zap.Int64("leaseID", leaseID),
			zap.Error(err))
	} else {
		logutil.BgLogger().Info("releaseServerID succeed",
			zap.Uint64("serverID", serverID),
			zap.Int64("leaseID", leaseID),
		)
	}
}

// propose server ID by random.
func (*Domain) proposeServerID(ctx context.Context, conflictCnt int) (uint64, error) {
	// get a random server ID in range [min, max]
	randomServerID := func(minv uint64, maxv uint64) uint64 {
		return uint64(rand.Int63n(int64(maxv-minv+1)) + int64(minv)) // #nosec G404
	}

	if conflictCnt < acquire32BitsServerIDRetryCnt {
		// get existing server IDs.
		allServerInfo, err := infosync.GetAllServerInfo(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		// `allServerInfo` contains current TiDB.
		if float32(len(allServerInfo)) <= 0.9*float32(globalconn.MaxServerID32) {
			serverIDs := make(map[uint64]struct{}, len(allServerInfo))
			for _, info := range allServerInfo {
				serverID := info.ServerIDGetter()
				if serverID <= globalconn.MaxServerID32 {
					serverIDs[serverID] = struct{}{}
				}
			}

			for range 15 {
				randServerID := randomServerID(1, globalconn.MaxServerID32)
				if _, ok := serverIDs[randServerID]; !ok {
					return randServerID, nil
				}
			}
		}
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to used up", zap.Int("len(allServerInfo)", len(allServerInfo)))
	} else {
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to conflict", zap.Int("conflictCnt", conflictCnt))
	}

	// upgrade to 64 bits.
	return randomServerID(globalconn.MaxServerID32+1, globalconn.MaxServerID64), nil
}

func (do *Domain) refreshServerIDTTL(ctx context.Context) error {
	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%v", serverIDEtcdPath, do.ServerID())
	value := "0"
	err = ddlutil.PutKVToEtcd(ctx, do.etcdClient, refreshServerIDRetryCnt, key, value, clientv3.WithLease(session.Lease()))
	if err != nil {
		logutil.BgLogger().Error("refreshServerIDTTL fail", zap.Uint64("serverID", do.ServerID()), zap.Error(err))
	} else {
		logutil.BgLogger().Info("refreshServerIDTTL succeed", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
	}
	return err
}

func (do *Domain) serverIDKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("serverIDKeeper exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "serverIDKeeper", func() {
		logutil.BgLogger().Info("recover serverIDKeeper.")
		// should be called before `do.wg.Done()`, to ensure that Domain.Close() waits for the new `serverIDKeeper()` routine.
		do.wg.Add(1)
		go do.serverIDKeeper()
	}, false)

	tickerKeepAlive := time.NewTicker(serverIDTimeToKeepAlive)
	tickerCheckRestored := time.NewTicker(serverIDTimeToCheckPDConnectionRestored)
	defer func() {
		tickerKeepAlive.Stop()
		tickerCheckRestored.Stop()
	}()

	blocker := make(chan struct{}) // just used for blocking the sessionDone() when session is nil.
	sessionDone := func() <-chan struct{} {
		if do.serverIDSession == nil {
			return blocker
		}
		return do.serverIDSession.Done()
	}

	var lastSucceedTimestamp time.Time

	onConnectionToPDRestored := func() {
		logutil.BgLogger().Info("restored connection to PD")
		do.isLostConnectionToPD.Store(0)
		lastSucceedTimestamp = time.Now()

		if err := do.info.ServerInfoSyncer().StoreServerInfo(context.Background()); err != nil {
			logutil.BgLogger().Error("StoreServerInfo failed", zap.Error(err))
		}
	}

	onConnectionToPDLost := func() {
		logutil.BgLogger().Warn("lost connection to PD")
		do.isLostConnectionToPD.Store(1)

		// Kill all connections when lost connection to PD,
		//   to avoid the possibility that another TiDB instance acquires the same serverID and generates a same connection ID,
		//   which will lead to a wrong connection killed.
		do.InfoSyncer().GetSessionManager().KillAllConnections()
	}

	for {
		select {
		case <-tickerKeepAlive.C:
			if !do.IsLostConnectionToPD() {
				if err := do.refreshServerIDTTL(context.Background()); err == nil {
					lastSucceedTimestamp = time.Now()
				} else {
					if lostConnectionToPDTimeout > 0 && time.Since(lastSucceedTimestamp) > lostConnectionToPDTimeout {
						onConnectionToPDLost()
					}
				}
			}
		case <-tickerCheckRestored.C:
			if do.IsLostConnectionToPD() {
				if err := do.acquireServerID(context.Background()); err == nil {
					onConnectionToPDRestored()
				}
			}
		case <-sessionDone():
			// inform that TTL of `serverID` is expired. See https://godoc.org/github.com/coreos/etcd/clientv3/concurrency#Session.Done
			//   Should be in `IsLostConnectionToPD` state, as `lostConnectionToPDTimeout` is shorter than `serverIDTTL`.
			//   So just set `do.serverIDSession = nil` to restart `serverID` session in `retrieveServerIDSession()`.
			logutil.BgLogger().Info("serverIDSession need restart")
			do.serverIDSession = nil
		case <-do.exit:
			return
		}
	}
}

// StartTTLJobManager creates and starts the ttl job manager
func (do *Domain) StartTTLJobManager() {
	ttlJobManager := ttlworker.NewJobManager(do.ddl.GetID(), do.advancedSysSessionPool, do.store, do.etcdClient, do.ddl.OwnerManager().IsOwner)
	do.ttlJobManager.Store(ttlJobManager)
	ttlJobManager.Start()
}

// TTLJobManager returns the ttl job manager on this domain
func (do *Domain) TTLJobManager() *ttlworker.JobManager {
	return do.ttlJobManager.Load()
}

// StopAutoAnalyze stops (*Domain).autoAnalyzeWorker to launch new auto analyze jobs.
func (do *Domain) StopAutoAnalyze() {
	do.stopAutoAnalyze.Store(true)
}

// InitInstancePlanCache initializes the instance level plan cache for this Domain.
func (do *Domain) InitInstancePlanCache() {
	hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
	softLimit := float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load())
	do.instancePlanCache = NewInstancePlanCache(int64(softLimit), hardLimit)
	// use a separate goroutine to avoid the eviction blocking other operations.
	do.wg.Run(do.planCacheEvictTrigger, "planCacheEvictTrigger")
	do.wg.Run(do.planCacheMetricsAndVars, "planCacheMetricsAndVars")
}

// GetInstancePlanCache returns the instance level plan cache in this Domain.
func (do *Domain) GetInstancePlanCache() sessionctx.InstancePlanCache {
	return do.instancePlanCache
}

// planCacheMetricsAndVars updates metrics and variables for Instance Plan Cache periodically.
func (do *Domain) planCacheMetricsAndVars() {
	defer util.Recover(metrics.LabelDomain, "planCacheMetricsAndVars", nil, false)
	ticker := time.NewTicker(time.Second * 15) // 15s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheMetricsAndVars exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// update limits
			hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
			softLimit := int64(float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load()))
			curSoft, curHard := do.instancePlanCache.GetLimits()
			if curSoft != softLimit || curHard != hardLimit {
				do.instancePlanCache.SetLimits(softLimit, hardLimit)
			}

			// update the metrics
			size := do.instancePlanCache.Size()
			memUsage := do.instancePlanCache.MemUsage()
			metrics2.GetPlanCacheInstanceNumCounter(true).Set(float64(size))
			metrics2.GetPlanCacheInstanceMemoryUsage(true).Set(float64(memUsage))
		case <-do.exit:
			return
		}
	}
}

// planCacheEvictTrigger triggers the plan cache eviction periodically.
func (do *Domain) planCacheEvictTrigger() {
	defer util.Recover(metrics.LabelDomain, "planCacheEvictTrigger", nil, false)
	ticker := time.NewTicker(time.Second * 30) // 30s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheEvictTrigger exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// trigger the eviction
			begin := time.Now()
			enabled := vardef.EnableInstancePlanCache.Load()
			detailInfo, numEvicted := do.instancePlanCache.Evict(!enabled) // evict all if the plan cache is disabled
			metrics2.GetPlanCacheInstanceEvict().Set(float64(numEvicted))
			if numEvicted > 0 {
				logutil.BgLogger().Info("instance plan eviction",
					zap.String("detail", detailInfo),
					zap.Int64("num_evicted", int64(numEvicted)),
					zap.Duration("time_spent", time.Since(begin)))
			}
		case <-do.exit:
			return
		}
	}
}

// SetupWorkloadBasedLearningWorker sets up all of the workload based learning workers.
func (do *Domain) SetupWorkloadBasedLearningWorker() {
	wbLearningHandle := workloadlearning.NewWorkloadLearningHandle(do.sysSessionPool)
	wbCacheWorker := workloadlearning.NewWLCacheWorker(do.sysSessionPool)
	// Start the workload based learning worker to analyze the read workload by statement_summary.
	do.wg.Run(
		func() {
			do.readTableCostWorker(wbLearningHandle, wbCacheWorker)
		},
		"readTableCostWorker",
	)
	// TODO: Add more workers for other workload based learning tasks.
}

// readTableCostWorker is a background worker that periodically analyze the read path table cost by statement_summary.
func (do *Domain) readTableCostWorker(wbLearningHandle *workloadlearning.Handle, wbCacheWorker *workloadlearning.WLCacheWorker) {
	// Recover the panic and log the error when worker exit.
	defer util.Recover(metrics.LabelDomain, "readTableCostWorker", nil, false)
	readTableCostTicker := time.NewTicker(vardef.WorkloadBasedLearningInterval.Load())
	defer func() {
		readTableCostTicker.Stop()
		logutil.BgLogger().Info("readTableCostWorker exited.")
	}()
	for {
		select {
		case <-readTableCostTicker.C:
			if vardef.EnableWorkloadBasedLearning.Load() && do.statsOwner.IsOwner() {
				wbLearningHandle.HandleTableReadCost(do.InfoSchema())
				wbCacheWorker.UpdateTableReadCostCache()
			}
		case <-do.exit:
			return
		}
	}
}

func init() {
	initByLDFlagsForGlobalKill()
	telemetry.GetDomainInfoSchema = func(ctx sessionctx.Context) infoschema.InfoSchema {
		return GetDomain(ctx).InfoSchema()
	}
}

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = dbterror.ClassDomain.NewStd(errno.ErrInfoSchemaExpired)
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = dbterror.ClassDomain.NewStdErr(errno.ErrInfoSchemaChanged,
		mysql.Message(errno.MySQLErrName[errno.ErrInfoSchemaChanged].Raw+". "+kv.TxnRetryableMark, nil))
)

// SysProcesses holds the sys processes infos
type SysProcesses struct {
	mu      *sync.RWMutex
	procMap map[uint64]sysproctrack.TrackProc
}

// Track tracks the sys process into procMap
func (s *SysProcesses) Track(id uint64, proc sysproctrack.TrackProc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldProc, ok := s.procMap[id]; ok && oldProc != proc {
		return errors.Errorf("The ID is in use: %v", id)
	}
	s.procMap[id] = proc
	proc.GetSessionVars().ConnectionID = id
	proc.GetSessionVars().SQLKiller.Reset()
	return nil
}

// UnTrack removes the sys process from procMap
func (s *SysProcesses) UnTrack(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		delete(s.procMap, id)
		proc.GetSessionVars().ConnectionID = 0
		proc.GetSessionVars().SQLKiller.Reset()
	}
}

// GetSysProcessList gets list of system ProcessInfo
func (s *SysProcesses) GetSysProcessList() map[uint64]*sessmgr.ProcessInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs := make(map[uint64]*sessmgr.ProcessInfo)
	for connID, proc := range s.procMap {
		// if session is still tracked in this map, it's not returned to sysSessionPool yet
		if pi := proc.ShowProcess(); pi != nil && pi.ID == connID {
			rs[connID] = pi
		}
	}
	return rs
}

// KillSysProcess kills sys process with specified ID
func (s *SysProcesses) KillSysProcess(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		proc.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}
}
