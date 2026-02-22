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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/domain/crossks"
	"github.com/pingcap/tidb/pkg/domain/globalconfigsync"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
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

	brOwnerMgr            owner.Manager
	logBackupAdvancer     *daemon.OwnerDaemon
	historicalStatsWorker *HistoricalStatsWorker
	ttlJobManager         atomic.Pointer[ttlworker.JobManager]
	runawayManager        *runaway.Manager
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

	do.advancedSysSessionPool.Close()

	do.crossKSSessMgr.Close()

	logutil.BgLogger().Info("domain closed", zap.Duration("take time", time.Since(startTime)))
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

// TelemetryLoop create a goroutine that reports usage data in a loop, it should be called only once
// in BootstrapSession.
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
