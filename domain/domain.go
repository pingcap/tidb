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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/schematracker"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/globalconfigsync"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/sessionstates"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/ttl/ttlworker"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/engine"
	"github.com/pingcap/tidb/util/expensivequery"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/memoryusagealarm"
	"github.com/pingcap/tidb/util/servermemorylimit"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// NewMockDomain is only used for test
func NewMockDomain() *Domain {
	do := &Domain{
		infoCache: infoschema.NewCache(1),
	}
	do.infoCache.Insert(infoschema.MockInfoSchema(nil), 1)
	return do
}

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store                   kv.Storage
	infoCache               *infoschema.InfoCache
	privHandle              *privileges.Handle
	bindHandle              atomic.Pointer[bindinfo.BindHandle]
	statsHandle             unsafe.Pointer
	statsLease              time.Duration
	ddl                     ddl.DDL
	info                    *infosync.InfoSyncer
	globalCfgSyncer         *globalconfigsync.GlobalConfigSyncer
	m                       sync.Mutex
	SchemaValidator         SchemaValidator
	sysSessionPool          *sessionPool
	exit                    chan struct{}
	etcdClient              *clientv3.Client
	sysVarCache             sysVarCache // replaces GlobalVariableCache
	slowQuery               *topNSlowQueries
	expensiveQueryHandle    *expensivequery.Handle
	memoryUsageAlarmHandle  *memoryusagealarm.Handle
	serverMemoryLimitHandle *servermemorylimit.Handle
	wg                      util.WaitGroupWrapper
	statsUpdating           atomicutil.Int32
	cancel                  context.CancelFunc
	indexUsageSyncLease     time.Duration
	dumpFileGcChecker       *dumpFileGcChecker
	planReplayerHandle      *planReplayerHandle
	expiredTimeStamp4PC     types.Time
	logBackupAdvancer       *daemon.OwnerDaemon
	historicalStatsWorker   *HistoricalStatsWorker
	ttlJobManager           *ttlworker.JobManager

	serverID             uint64
	serverIDSession      *concurrency.Session
	isLostConnectionToPD atomicutil.Int32 // !0: true, 0: false.
	onClose              func()
	sysExecutorFactory   func(*Domain) (pools.Resource, error)

	sysProcesses SysProcesses

	mdlCheckTableInfo *mdlCheckTableInfo

	analyzeMu struct {
		sync.Mutex
		sctxs map[sessionctx.Context]bool
	}
}

type mdlCheckTableInfo struct {
	mu         sync.Mutex
	newestVer  int64
	jobsVerMap map[int64]int64
	jobsIdsMap map[int64]string
}

// InfoCache export for test.
func (do *Domain) InfoCache() *infoschema.InfoCache {
	return do.infoCache
}

// EtcdClient export for test.
func (do *Domain) EtcdClient() *clientv3.Client {
	return do.etcdClient
}

// loadInfoSchema loads infoschema at startTS.
// It returns:
// 1. the needed infoschema
// 2. cache hit indicator
// 3. currentSchemaVersion(before loading)
// 4. the changed table IDs if it is not full load
// 5. an error if any
func (do *Domain) loadInfoSchema(startTS uint64) (infoschema.InfoSchema, bool, int64, *transaction.RelatedSchemaChange, error) {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	m := meta.NewSnapshotMeta(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, false, 0, nil, err
	}

	if is := do.infoCache.GetByVersion(neededSchemaVersion); is != nil {
		return is, true, 0, nil, nil
	}

	currentSchemaVersion := int64(0)
	if oldInfoSchema := do.infoCache.GetLatest(); oldInfoSchema != nil {
		currentSchemaVersion = oldInfoSchema.SchemaMetaVersion()
	}

	// TODO: tryLoadSchemaDiffs has potential risks of failure. And it becomes worse in history reading cases.
	// It is only kept because there is no alternative diff/partial loading solution.
	// And it is only used to diff upgrading the current latest infoschema, if:
	// 1. Not first time bootstrap loading, which needs a full load.
	// 2. It is newer than the current one, so it will be "the current one" after this function call.
	// 3. There are less 100 diffs.
	startTime := time.Now()
	if currentSchemaVersion != 0 && neededSchemaVersion > currentSchemaVersion && neededSchemaVersion-currentSchemaVersion < 100 {
		is, relatedChanges, err := do.tryLoadSchemaDiffs(m, currentSchemaVersion, neededSchemaVersion)
		if err == nil {
			do.infoCache.Insert(is, startTS)
			logutil.BgLogger().Info("diff load InfoSchema success",
				zap.Int64("currentSchemaVersion", currentSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion),
				zap.Duration("start time", time.Since(startTime)),
				zap.Int64s("phyTblIDs", relatedChanges.PhyTblIDS),
				zap.Uint64s("actionTypes", relatedChanges.ActionTypes))
			return is, false, currentSchemaVersion, relatedChanges, nil
		}
		// We can fall back to full load, don't need to return the error.
		logutil.BgLogger().Error("failed to load schema diff", zap.Error(err))
	}

	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	policies, err := do.fetchPolicies(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	newISBuilder, err := infoschema.NewBuilder(do.Store(), do.sysFacHack).InitWithDBInfos(schemas, policies, neededSchemaVersion)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	logutil.BgLogger().Info("full load InfoSchema success",
		zap.Int64("currentSchemaVersion", currentSchemaVersion),
		zap.Int64("neededSchemaVersion", neededSchemaVersion),
		zap.Duration("start time", time.Since(startTime)))

	is := newISBuilder.Build()
	do.infoCache.Insert(is, startTS)
	return is, false, currentSchemaVersion, nil, nil
}

func (do *Domain) sysFacHack() (pools.Resource, error) {
	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	return do.sysExecutorFactory(do)
}

func (do *Domain) fetchPolicies(m *meta.Meta) ([]*model.PolicyInfo, error) {
	allPolicies, err := m.ListPolicies()
	if err != nil {
		return nil, err
	}
	return allPolicies, nil
}

func (do *Domain) fetchAllSchemasWithTables(m *meta.Meta) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	doneCh := make(chan error, len(splittedSchemas))
	for _, schemas := range splittedSchemas {
		go do.fetchSchemasWithTables(schemas, m, doneCh)
	}
	for range splittedSchemas {
		err = <-doneCh
		if err != nil {
			return nil, err
		}
	}
	return allSchemas, nil
}

// fetchSchemaConcurrency controls the goroutines to load schemas, but more goroutines
// increase the memory usage when calling json.Unmarshal(), which would cause OOM,
// so we decrease the concurrency.
const fetchSchemaConcurrency = 1

func (do *Domain) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
	groupSize := (len(schemas) + fetchSchemaConcurrency - 1) / fetchSchemaConcurrency
	splitted := make([][]*model.DBInfo, 0, fetchSchemaConcurrency)
	schemaCnt := len(schemas)
	for i := 0; i < schemaCnt; i += groupSize {
		end := i + groupSize
		if end > schemaCnt {
			end = schemaCnt
		}
		splitted = append(splitted, schemas[i:end])
	}
	return splitted
}

func (do *Domain) fetchSchemasWithTables(schemas []*model.DBInfo, m *meta.Meta, done chan error) {
	for _, di := range schemas {
		if di.State != model.StatePublic {
			// schema is not public, can't be used outside.
			continue
		}
		tables, err := m.ListTables(di.ID)
		if err != nil {
			done <- err
			return
		}
		// If TreatOldVersionUTF8AsUTF8MB4 was enable, need to convert the old version schema UTF8 charset to UTF8MB4.
		if config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
			for _, tbInfo := range tables {
				infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo)
			}
		}
		di.Tables = make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			if tbl.State != model.StatePublic {
				// schema is not public, can't be used outside.
				continue
			}
			infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
			// Check whether the table is in repair mode.
			if domainutil.RepairInfo.InRepairMode() && domainutil.RepairInfo.CheckAndFetchRepairedTable(di, tbl) {
				continue
			}
			di.Tables = append(di.Tables, tbl)
		}
	}
	done <- nil
}

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table and partition IDs.
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (infoschema.InfoSchema, *transaction.RelatedSchemaChange, error) {
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return nil, nil, err
		}
		if diff == nil {
			// Empty diff means the txn of generating schema version is committed, but the txn of `runDDLJob` is not or fail.
			// It is safe to skip the empty diff because the infoschema is new enough and consistent.
			continue
		}
		diffs = append(diffs, diff)
	}
	builder := infoschema.NewBuilder(do.Store(), do.sysFacHack).InitWithOldInfoSchema(do.infoCache.GetLatest())
	builder.SetDeltaUpdateBundles()
	phyTblIDs := make([]int64, 0, len(diffs))
	actions := make([]uint64, 0, len(diffs))
	for _, diff := range diffs {
		IDs, err := builder.ApplyDiff(m, diff)
		if err != nil {
			return nil, nil, err
		}
		if canSkipSchemaCheckerDDL(diff.Type) {
			continue
		}
		phyTblIDs = append(phyTblIDs, IDs...)
		for i := 0; i < len(IDs); i++ {
			actions = append(actions, uint64(diff.Type))
		}
	}

	is := builder.Build()
	relatedChange := transaction.RelatedSchemaChange{}
	relatedChange.PhyTblIDS = phyTblIDs
	relatedChange.ActionTypes = actions
	return is, &relatedChange, nil
}

func canSkipSchemaCheckerDDL(tp model.ActionType) bool {
	switch tp {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionSetTiFlashReplica:
		return true
	}
	return false
}

// InfoSchema gets the latest information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoCache.GetLatest()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	// if the snapshotTS is new enough, we can get infoschema directly through sanpshotTS.
	if is := do.infoCache.GetBySnapshotTS(snapshotTS); is != nil {
		return is, nil
	}
	is, _, _, _, err := do.loadInfoSchema(snapshotTS)
	return is, err
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) (*meta.Meta, error) {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	return meta.NewSnapshotMeta(snapshot), nil
}

// ExpiredTimeStamp4PC gets expiredTimeStamp4PC from domain.
func (do *Domain) ExpiredTimeStamp4PC() types.Time {
	do.m.Lock()
	defer do.m.Unlock()

	return do.expiredTimeStamp4PC
}

// SetExpiredTimeStamp4PC sets the expiredTimeStamp4PC from domain.
func (do *Domain) SetExpiredTimeStamp4PC(time types.Time) {
	do.m.Lock()
	defer do.m.Unlock()

	do.expiredTimeStamp4PC = time
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// SetDDL sets DDL to domain, it's only used in tests.
func (do *Domain) SetDDL(d ddl.DDL) {
	do.ddl = d
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *infosync.InfoSyncer {
	return do.info
}

// NotifyGlobalConfigChange notify global config syncer to store the global config into PD.
func (do *Domain) NotifyGlobalConfigChange(name, value string) {
	do.globalCfgSyncer.Notify(pd.GlobalConfigItem{Name: name, Value: value})
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
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	failpoint.Inject("ErrorMockReloadFailed", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock reload failed"))
		}
	})

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()
	ver, err := do.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}

	is, hitCache, oldSchemaVersion, changes, err := do.loadInfoSchema(ver.Ver)
	metrics.LoadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		metrics.LoadSchemaCounter.WithLabelValues("failed").Inc()
		return err
	}
	metrics.LoadSchemaCounter.WithLabelValues("succ").Inc()

	// only update if it is not from cache
	if !hitCache {
		// loaded newer schema
		if oldSchemaVersion < is.SchemaMetaVersion() {
			// Update self schema version to etcd.
			err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), 0, is.SchemaMetaVersion())
			if err != nil {
				logutil.BgLogger().Info("update self version failed",
					zap.Int64("oldSchemaVersion", oldSchemaVersion),
					zap.Int64("neededSchemaVersion", is.SchemaMetaVersion()), zap.Error(err))
			}
		}

		// it is full load
		if changes == nil {
			logutil.BgLogger().Info("full load and reset schema validator")
			do.SchemaValidator.Reset()
		}
	}

	// lease renew, so it must be executed despite it is cache or not
	do.SchemaValidator.Update(ver.Ver, oldSchemaVersion, is.SchemaMetaVersion(), changes)
	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schema time elapses more than this interval,
	// some query maybe responded by ErrInfoSchemaExpired error.
	if sub > (lease/2) && lease > 0 {
		logutil.BgLogger().Warn("loading schema takes a long time", zap.Duration("take time", sub))
	}

	return nil
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
		do.wg.Done()
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

func (do *Domain) infoSyncerKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("infoSyncerKeeper exited.")
		util.Recover(metrics.LabelDomain, "infoSyncerKeeper", nil, false)
	}()
	ticker := time.NewTicker(infosync.ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			do.info.ReportMinStartTS(do.Store())
		case <-do.info.Done():
			logutil.BgLogger().Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				logutil.BgLogger().Error("server info syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server info syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) globalConfigSyncerKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("globalConfigSyncerKeeper exited.")
		util.Recover(metrics.LabelDomain, "globalConfigSyncerKeeper", nil, false)
	}()
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

func (do *Domain) topologySyncerKeeper() {
	defer util.Recover(metrics.LabelDomain, "topologySyncerKeeper", nil, false)
	ticker := time.NewTicker(infosync.TopologyTimeToRefresh)
	defer func() {
		ticker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("topologySyncerKeeper exited.")
	}()

	for {
		select {
		case <-ticker.C:
			err := do.info.StoreTopologyInfo(context.Background())
			if err != nil {
				logutil.BgLogger().Error("refresh topology in loop failed", zap.Error(err))
			}
		case <-do.info.TopologyDone():
			logutil.BgLogger().Info("server topology syncer need to restart")
			if err := do.info.RestartTopology(context.Background()); err != nil {
				logutil.BgLogger().Error("server topology syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server topology syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) refreshMDLCheckTableInfo() {
	se, err := do.sysSessionPool.Get()

	if err != nil {
		logutil.BgLogger().Warn("get system session failed", zap.Error(err))
		return
	}
	defer do.sysSessionPool.Put(se)
	exec := se.(sqlexec.RestrictedSQLExecutor)
	domainSchemaVer := do.InfoSchema().SchemaMetaVersion()
	rows, _, err := exec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnTelemetry), nil, fmt.Sprintf("select job_id, version, table_ids from mysql.tidb_mdl_info where version <= %d", domainSchemaVer))
	if err != nil {
		logutil.BgLogger().Warn("get mdl info from tidb_mdl_info failed", zap.Error(err))
		return
	}
	do.mdlCheckTableInfo.mu.Lock()
	defer do.mdlCheckTableInfo.mu.Unlock()

	do.mdlCheckTableInfo.newestVer = domainSchemaVer
	do.mdlCheckTableInfo.jobsVerMap = make(map[int64]int64, len(rows))
	do.mdlCheckTableInfo.jobsIdsMap = make(map[int64]string, len(rows))
	for i := 0; i < len(rows); i++ {
		do.mdlCheckTableInfo.jobsVerMap[rows[i].GetInt64(0)] = rows[i].GetInt64(1)
		do.mdlCheckTableInfo.jobsIdsMap[rows[i].GetInt64(0)] = rows[i].GetString(2)
	}
}

func (do *Domain) mdlCheckLoop() {
	ticker := time.Tick(time.Millisecond * 50)
	var saveMaxSchemaVersion int64
	jobNeedToSync := false
	jobCache := make(map[int64]int64, 1000)

	for {
		select {
		case <-ticker:
			if !variable.EnableMDL.Load() {
				continue
			}

			do.mdlCheckTableInfo.mu.Lock()
			maxVer := do.mdlCheckTableInfo.newestVer
			if maxVer > saveMaxSchemaVersion {
				saveMaxSchemaVersion = maxVer
			} else if !jobNeedToSync {
				// Schema doesn't change, and no job to check in the last run.
				do.mdlCheckTableInfo.mu.Unlock()
				continue
			}

			jobNeedToCheckCnt := len(do.mdlCheckTableInfo.jobsVerMap)
			if jobNeedToCheckCnt == 0 {
				jobNeedToSync = false
				do.mdlCheckTableInfo.mu.Unlock()
				continue
			}

			jobsVerMap := make(map[int64]int64, len(do.mdlCheckTableInfo.jobsVerMap))
			jobsIdsMap := make(map[int64]string, len(do.mdlCheckTableInfo.jobsIdsMap))
			for k, v := range do.mdlCheckTableInfo.jobsVerMap {
				jobsVerMap[k] = v
			}
			for k, v := range do.mdlCheckTableInfo.jobsIdsMap {
				jobsIdsMap[k] = v
			}
			do.mdlCheckTableInfo.mu.Unlock()

			jobNeedToSync = true

			sm := do.InfoSyncer().GetSessionManager()
			if sm == nil {
				logutil.BgLogger().Info("session manager is nil")
			} else {
				sm.CheckOldRunningTxn(jobsVerMap, jobsIdsMap)
			}

			if len(jobsVerMap) == jobNeedToCheckCnt {
				jobNeedToSync = false
			}

			// Try to gc jobCache.
			if len(jobCache) > 1000 {
				jobCache = make(map[int64]int64, 1000)
			}

			for jobID, ver := range jobsVerMap {
				if cver, ok := jobCache[jobID]; ok && cver >= ver {
					// Already update, skip it.
					continue
				}
				logutil.BgLogger().Info("mdl gets lock, update to owner", zap.Int64("jobID", jobID), zap.Int64("version", ver))
				err := do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), jobID, ver)
				if err != nil {
					logutil.BgLogger().Warn("update self version failed", zap.Error(err))
				} else {
					jobCache[jobID] = ver
				}
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) loadSchemaInLoop(ctx context.Context, lease time.Duration) {
	defer util.Recover(metrics.LabelDomain, "loadSchemaInLoop", nil, true)
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer func() {
		ticker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("loadSchemaInLoop exited.")
	}()
	syncer := do.ddl.SchemaSyncer()

	for {
		select {
		case <-ticker.C:
			err := do.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
		case _, ok := <-syncer.GlobalVersionCh():
			err := do.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
			if !ok {
				logutil.BgLogger().Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			logutil.BgLogger().Info("reload schema in loop, schema syncer need restart")
			// The etcd is responsible for schema synchronization, we should ensure there is at most two different schema version
			// in the TiDB cluster, to make the data/schema be consistent. If we lost connection/session to etcd, the cluster
			// will treats this TiDB as a down instance, and etcd will remove the key of `/tidb/ddl/all_schema_versions/tidb-id`.
			// Say the schema version now is 1, the owner is changing the schema version to 2, it will not wait for this down TiDB syncing the schema,
			// then continue to change the TiDB schema to version 3. Unfortunately, this down TiDB schema version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schema validator to prohibit the DML executing.
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer(ctx)
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			exitLoop := do.mustReload()
			// domain is cosed.
			if exitLoop {
				logutil.BgLogger().Error("domain is closed, exit loadSchemaInLoop")
				return
			}
			do.SchemaValidator.Restart()
			logutil.BgLogger().Info("schema syncer restarted")
		case <-do.exit:
			return
		}
		do.refreshMDLCheckTableInfo()
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stoped.
func (do *Domain) mustRestartSyncer(ctx context.Context) error {
	syncer := do.ddl.SchemaSyncer()

	for {
		err := syncer.Restart(ctx)
		if err == nil {
			return nil
		}
		// If the domain has stopped, we return an error immediately.
		if do.isClose() {
			return err
		}
		logutil.BgLogger().Error("restart the schema syncer failed", zap.Error(err))
		time.Sleep(time.Second)
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (do *Domain) mustReload() (exitLoop bool) {
	for {
		err := do.Reload()
		if err == nil {
			logutil.BgLogger().Info("mustReload succeed")
			return false
		}

		// If the domain is closed, we returns immediately.
		logutil.BgLogger().Info("reload the schema failed", zap.Error(err))
		if do.isClose() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
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
	if do.info != nil {
		do.info.RemoveServerInfo()
		do.info.RemoveMinStartTS()
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}

	do.slowQuery.Close()
	if do.cancel != nil {
		do.cancel()
	}
	do.wg.Wait()
	do.sysSessionPool.Close()
	variable.UnregisterStatistics(do.bindHandle.Load())
	if do.onClose != nil {
		do.onClose()
	}
	logutil.BgLogger().Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, ddlLease time.Duration, statsLease time.Duration, idxUsageSyncLease time.Duration, dumpFileGcLease time.Duration, factory pools.Factory, onClose func()) *Domain {
	capacity := 200 // capacity of the sysSessionPool size
	do := &Domain{
		store:               store,
		exit:                make(chan struct{}),
		sysSessionPool:      newSessionPool(capacity, factory),
		statsLease:          statsLease,
		infoCache:           infoschema.NewCache(16),
		slowQuery:           newTopNSlowQueries(30, time.Hour*24*7, 500),
		indexUsageSyncLease: idxUsageSyncLease,
		dumpFileGcChecker:   &dumpFileGcChecker{gcLease: dumpFileGcLease, paths: []string{GetPlanReplayerDirName(), GetOptimizerTraceDirName()}},
		onClose:             onClose,
		expiredTimeStamp4PC: types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp),
		mdlCheckTableInfo: &mdlCheckTableInfo{
			mu:         sync.Mutex{},
			jobsVerMap: make(map[int64]int64),
			jobsIdsMap: make(map[int64]string),
		},
	}

	do.SchemaValidator = NewSchemaValidator(ddlLease, do)
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
	do.memoryUsageAlarmHandle = memoryusagealarm.NewMemoryUsageAlarmHandle(do.exit)
	do.serverMemoryLimitHandle = servermemorylimit.NewServerMemoryLimitHandle(do.exit)
	do.sysProcesses = SysProcesses{mu: &sync.RWMutex{}, procMap: make(map[uint64]sessionctx.Context)}
	do.initDomainSysVars()
	return do
}

const serverIDForStandalone = 1 // serverID for standalone deployment.

// Init initializes a domain.
func (do *Domain) Init(
	ddlLease time.Duration,
	sysExecutorFactory func(*Domain) (pools.Resource, error),
	ddlInjector func(ddl.DDL) *schematracker.Checker,
) error {
	do.sysExecutorFactory = sysExecutorFactory
	perfschema.Init()
	if ebd, ok := do.store.(kv.EtcdBackend); ok {
		var addrs []string
		var err error
		if addrs, err = ebd.EtcdAddrs(); err != nil {
			return err
		}
		if addrs != nil {
			cfg := config.GetGlobalConfig()
			// silence etcd warn log, when domain closed, it won't randomly print warn log
			// see details at the issue https://github.com/pingcap/tidb/issues/15479
			etcdLogCfg := zap.NewProductionConfig()
			etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
			cli, err := clientv3.New(clientv3.Config{
				LogConfig:        &etcdLogCfg,
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
						Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
					}),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				return errors.Trace(err)
			}
			do.etcdClient = cli
		}
	}

	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	sysFac := func() (pools.Resource, error) {
		return sysExecutorFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 128, 128, resourceIdleTimeout)
	ctx, cancelFunc := context.WithCancel(context.Background())
	do.cancel = cancelFunc
	var callback ddl.Callback
	newCallbackFunc, err := ddl.GetCustomizedHook("default_hook")
	if err != nil {
		return errors.Trace(err)
	}
	callback = newCallbackFunc(do)
	d := do.ddl
	do.ddl = ddl.NewDDL(
		ctx,
		ddl.WithEtcdClient(do.etcdClient),
		ddl.WithStore(do.store),
		ddl.WithInfoCache(do.infoCache),
		ddl.WithHook(callback),
		ddl.WithLease(ddlLease),
	)
	failpoint.Inject("MockReplaceDDL", func(val failpoint.Value) {
		if val.(bool) {
			do.ddl = d
		}
	})
	if ddlInjector != nil {
		checker := ddlInjector(do.ddl)
		checker.CreateTestDB()
		do.ddl = checker
	}

	if config.GetGlobalConfig().EnableGlobalKill {
		if do.etcdClient != nil {
			err := do.acquireServerID(ctx)
			if err != nil {
				logutil.BgLogger().Error("acquire serverID failed", zap.Error(err))
				do.isLostConnectionToPD.Store(1) // will retry in `do.serverIDKeeper`
			} else {
				do.isLostConnectionToPD.Store(0)
			}

			do.wg.Add(1)
			go do.serverIDKeeper()
		} else {
			// set serverID for standalone deployment to enable 'KILL'.
			atomic.StoreUint64(&do.serverID, serverIDForStandalone)
		}
	}

	// step 1: prepare the info/schema syncer which domain reload needed.
	skipRegisterToDashboard := config.GetGlobalConfig().SkipRegisterToDashboard
	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.ddl.GetID(), do.ServerID, do.etcdClient, skipRegisterToDashboard)
	if err != nil {
		return err
	}

	var pdClient pd.Client
	if store, ok := do.store.(kv.StorageWithPD); ok {
		pdClient = store.GetPDClient()
	}
	do.globalCfgSyncer = globalconfigsync.NewGlobalConfigSyncer(pdClient)

	err = do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return err
	}
	// step 2: domain reload the infoSchema.
	err = do.Reload()
	if err != nil {
		return err
	}
	// step 3: start the ddl after the domain reload, avoiding some internal sql running before infoSchema construction.
	err = do.ddl.Start(sysCtxPool)
	if err != nil {
		return err
	}

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop.
	if ddlLease > 0 {
		do.wg.Add(1)
		// Local store needs to get the change information for every DDL state in each session.
		go do.loadSchemaInLoop(ctx, ddlLease)
	}
	do.wg.Run(do.mdlCheckLoop)
	do.wg.Add(3)
	go do.topNSlowQueryLoop()
	go do.infoSyncerKeeper()
	go do.globalConfigSyncerKeeper()
	if !skipRegisterToDashboard {
		do.wg.Add(1)
		go do.topologySyncerKeeper()
	}
	if pdClient != nil {
		do.wg.Add(1)
		go do.closestReplicaReadCheckLoop(ctx, pdClient)
	}
	err = do.initLogBackup(ctx, pdClient)
	if err != nil {
		return err
	}

	do.wg.Run(func() {
		do.runTTLJobManager(ctx)
	})

	return nil
}

func (do *Domain) initLogBackup(ctx context.Context, pdClient pd.Client) error {
	cfg := config.GetGlobalConfig()
	if pdClient == nil || do.etcdClient == nil {
		log.Warn("pd / etcd client not provided, won't begin Advancer.")
		return nil
	}
	env, err := streamhelper.TiDBEnv(pdClient, do.etcdClient, cfg)
	if err != nil {
		return err
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	do.logBackupAdvancer = daemon.New(adv, streamhelper.OwnerManagerForLogBackup(ctx, do.etcdClient), adv.Config().TickDuration)
	loop, err := do.logBackupAdvancer.Begin(ctx)
	if err != nil {
		return err
	}
	do.wg.Run(loop)
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
		do.wg.Done()
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
	replicaRead := do.sysVarCache.global[variable.TiDBReplicaRead]
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

	stores, err := pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())
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
	svrIdsInThisZone := make([]string, 0)
	for _, s := range servers {
		if v, ok := s.Labels[placement.DCLabelKey]; ok && v != "" {
			if _, ok := storeZones[v]; ok {
				storeZones[v] += 1
				if v == zone {
					svrIdsInThisZone = append(svrIdsInThisZone, s.ID)
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
	if enabledCount < len(svrIdsInThisZone) {
		sort.Slice(svrIdsInThisZone, func(i, j int) bool {
			return strings.Compare(svrIdsInThisZone[i], svrIdsInThisZone[j]) < 0
		})
	}
	enabled := true
	for _, s := range svrIdsInThisZone[enabledCount:] {
		if s == serverInfo.ID {
			enabled = false
			break
		}
	}

	if variable.SetEnableAdaptiveReplicaRead(enabled) {
		logutil.BgLogger().Info("tidb server adaptive closest replica read is changed", zap.Bool("enable", enabled))
	}
	return nil
}

type sessionPool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

func newSessionPool(capacity int, factory pools.Factory) *sessionPool {
	return &sessionPool{
		resources: make(chan pools.Resource, capacity),
		factory:   factory,
	}
}

func (p *sessionPool) Get() (resource pools.Resource, err error) {
	var ok bool
	select {
	case resource, ok = <-p.resources:
		if !ok {
			err = errors.New("session pool closed")
		}
	default:
		resource, err = p.factory()
	}

	// Put the internal session to the map of SessionManager
	failpoint.Inject("mockSessionPoolReturnError", func() {
		err = errors.New("mockSessionPoolReturnError")
	})

	if nil == err {
		infosync.StoreInternalSession(resource)
	}

	return
}

func (p *sessionPool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Delete the internal session to the map of SessionManager
	infosync.DeleteInternalSession(resource)
	if p.mu.closed {
		resource.Close()
		return
	}

	select {
	case p.resources <- resource:
	default:
		resource.Close()
	}
}
func (p *sessionPool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Close()
	}
}

// SysSessionPool returns the system session pool.
func (do *Domain) SysSessionPool() *sessionPool {
	return do.sysSessionPool
}

// SysProcTracker returns the system processes tracker.
func (do *Domain) SysProcTracker() sessionctx.SysProcTracker {
	return &do.sysProcesses
}

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sctx.GetSessionVars().InRestrictedSQL = true
	_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "set @@autocommit = 1")
	if err != nil {
		return err
	}
	do.privHandle = privileges.NewHandle()
	err = do.privHandle.Update(sctx)
	if err != nil {
		return err
	}

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
			util.Recover(metrics.LabelDomain, "loadPrivilegeInLoop", nil, false)
		}()
		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("load privilege loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}

			count = 0
			err := do.privHandle.Update(sctx)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}()
	return nil
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
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("LoadSysVarCacheLoop exited.")
			util.Recover(metrics.LabelDomain, "LoadSysVarCacheLoop", nil, false)
		}()
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
				logutil.BgLogger().Error("LoadSysVarCacheLoop loop watch channel closed")
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
				logutil.BgLogger().Error("LoadSysVarCacheLoop failed", zap.Error(err))
			}
		}
	}()
	return nil
}

// WatchTiFlashComputeNodeChange create a routine to watch if the topology of tiflash_compute node is changed.
// TODO: tiflashComputeNodeKey is not put to etcd yet(finish this when AutoScaler is done)
//
//	store cache will only be invalidated every 30 seconds.
func (do *Domain) WatchTiFlashComputeNodeChange() error {
	var watchCh clientv3.WatchChan
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
	}
	do.wg.Add(1)
	duration := 10 * time.Second
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("WatchTiFlashComputeNodeChange exit")
			util.Recover(metrics.LabelDomain, "WatchTiFlashComputeNodeChange", nil, false)
		}()

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
				if logCount == 60 {
					// Print log every 60*duration seconds.
					logutil.BgLogger().Debug("tiflash_compute store cache invalied, will update next query", zap.Bool("watched", watched))
					logCount = 0
				}
			default:
				logutil.BgLogger().Debug("No need to watch tiflash_compute store cache for non-tikv store")
				return
			}
		}
	}()
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindHandle returns domain's bindHandle.
func (do *Domain) BindHandle() *bindinfo.BindHandle {
	return do.bindHandle.Load()
}

// LoadBindInfoLoop create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) LoadBindInfoLoop(ctxForHandle sessionctx.Context, ctxForEvolve sessionctx.Context) error {
	ctxForHandle.GetSessionVars().InRestrictedSQL = true
	ctxForEvolve.GetSessionVars().InRestrictedSQL = true
	if !do.bindHandle.CompareAndSwap(nil, bindinfo.NewBindHandle(ctxForHandle)) {
		do.bindHandle.Load().Reset(ctxForHandle)
	}

	err := do.bindHandle.Load().Update(true)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	owner := do.newOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
	do.globalBindHandleWorkerLoop(owner)
	do.handleEvolvePlanTasksLoop(ctxForEvolve, owner)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop(owner owner.Manager) {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
			util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)
		}()
		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		gcBindTicker := time.NewTicker(100 * bindinfo.Lease)
		defer func() {
			bindWorkerTicker.Stop()
			gcBindTicker.Stop()
		}()
		for {
			select {
			case <-do.exit:
				return
			case <-bindWorkerTicker.C:
				bindHandle := do.bindHandle.Load()
				err := bindHandle.Update(false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
				bindHandle.DropInvalidBindRecord()
				// Get Global
				optVal, err := do.GetGlobalVar(variable.TiDBCapturePlanBaseline)
				if err == nil && variable.TiDBOptOn(optVal) {
					bindHandle.CaptureBaselines()
				}
				bindHandle.SaveEvolveTasksToStore()
			case <-gcBindTicker.C:
				if !owner.IsOwner() {
					continue
				}
				err := do.bindHandle.Load().GCBindRecord()
				if err != nil {
					logutil.BgLogger().Error("GC bind record failed", zap.Error(err))
				}
			}
		}
	}()
}

func (do *Domain) handleEvolvePlanTasksLoop(ctx sessionctx.Context, owner owner.Manager) {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("handleEvolvePlanTasksLoop exited.")
			util.Recover(metrics.LabelDomain, "handleEvolvePlanTasksLoop", nil, false)
		}()
		for {
			select {
			case <-do.exit:
				owner.Cancel()
				return
			case <-time.After(bindinfo.Lease):
			}
			if owner.IsOwner() {
				err := do.bindHandle.Load().HandleEvolvePlanTask(ctx, false)
				if err != nil {
					logutil.BgLogger().Info("evolve plan failed", zap.Error(err))
				}
			}
		}
	}()
}

// TelemetryReportLoop create a goroutine that reports usage data in a loop, it should be called only once
// in BootstrapSession.
func (do *Domain) TelemetryReportLoop(ctx sessionctx.Context) {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := telemetry.InitialRun(ctx, do.GetEtcdClient())
	if err != nil {
		logutil.BgLogger().Warn("Initial telemetry run failed", zap.Error(err))
	}

	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("TelemetryReportLoop exited.")
			util.Recover(metrics.LabelDomain, "TelemetryReportLoop", nil, false)
		}()
		owner := do.newOwnerManager(telemetry.Prompt, telemetry.OwnerKey)
		for {
			select {
			case <-do.exit:
				owner.Cancel()
				return
			case <-time.After(telemetry.ReportInterval):
				if !owner.IsOwner() {
					continue
				}
				err := telemetry.ReportUsageData(ctx, do.GetEtcdClient())
				if err != nil {
					// Only status update errors will be printed out
					logutil.BgLogger().Warn("TelemetryReportLoop status update failed", zap.Error(err))
				}
			}
		}
	}()
}

// TelemetryRotateSubWindowLoop create a goroutine that rotates the telemetry window regularly.
func (do *Domain) TelemetryRotateSubWindowLoop(ctx sessionctx.Context) {
	ctx.GetSessionVars().InRestrictedSQL = true
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("TelemetryRotateSubWindowLoop exited.")
			util.Recover(metrics.LabelDomain, "TelemetryRotateSubWindowLoop", nil, false)
		}()
		for {
			select {
			case <-do.exit:
				return
			case <-time.After(telemetry.SubWindowSize):
				telemetry.RotateSubWindow()
			}
		}
	}()
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
	taskStatus.finishedTaskMu.finishedTask = map[PlanReplayerTaskKey]struct{}{}
	taskStatus.runningTaskMu.runningTasks = map[PlanReplayerTaskKey]struct{}{}

	do.planReplayerHandle.planReplayerTaskDumpHandle = &planReplayerTaskDumpHandle{
		taskCH: taskCH,
		status: taskStatus,
	}
	do.planReplayerHandle.planReplayerTaskDumpHandle.workers = make([]*planReplayerTaskDumpWorker, 0)
	for i := 0; i < len(workersSctxs); i++ {
		worker := &planReplayerTaskDumpWorker{
			ctx:    ctx,
			sctx:   workersSctxs[i],
			taskCH: taskCH,
			status: taskStatus,
		}
		do.planReplayerHandle.planReplayerTaskDumpHandle.workers = append(do.planReplayerHandle.planReplayerTaskDumpHandle.workers, worker)
	}
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
	do.wg.Add(2)
	go func() {
		tikcer := time.NewTicker(time.Duration(lease))
		defer func() {
			tikcer.Stop()
			do.wg.Done()
			logutil.BgLogger().Info("PlanReplayerTaskCollectHandle exited.")
			util.Recover(metrics.LabelDomain, "PlanReplayerTaskCollectHandle", nil, false)
		}()
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
	}()
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("PlanReplayerTaskDumpHandle exited.")
			util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpHandle", nil, false)
		}()
		for _, worker := range do.planReplayerHandle.planReplayerTaskDumpHandle.workers {
			go worker.run()
		}
		<-do.exit
		do.planReplayerHandle.planReplayerTaskDumpHandle.Close()
	}()
}

// GetPlanReplayerHandle returns plan replayer handle
func (do *Domain) GetPlanReplayerHandle() *planReplayerHandle {
	return do.planReplayerHandle
}

// DumpFileGcCheckerLoop creates a goroutine that handles `exit` and `gc`.
func (do *Domain) DumpFileGcCheckerLoop() {
	do.wg.Add(1)
	go func() {
		gcTicker := time.NewTicker(do.dumpFileGcChecker.gcLease)
		defer func() {
			gcTicker.Stop()
			do.wg.Done()
			logutil.BgLogger().Info("dumpFileGcChecker exited.")
			util.Recover(metrics.LabelDomain, "dumpFileGcCheckerLoop", nil, false)
		}()
		for {
			select {
			case <-do.exit:
				return
			case <-gcTicker.C:
				do.dumpFileGcChecker.gcDumpFiles(time.Hour)
			}
		}
	}()
}

// GetHistoricalStatsWorker gets historical workers
func (do *Domain) GetHistoricalStatsWorker() *HistoricalStatsWorker {
	return do.historicalStatsWorker
}

// EnableDumpHistoricalStats used to control whether enbale dump stats for unit test
var enableDumpHistoricalStats atomic.Bool

// StartHistoricalStatsWorker start historical workers running
func (do *Domain) StartHistoricalStatsWorker() {
	if !enableDumpHistoricalStats.Load() {
		return
	}
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("HistoricalStatsWorker exited.")
			util.Recover(metrics.LabelDomain, "HistoricalStatsWorkerLoop", nil, false)
		}()
		for {
			select {
			case <-do.exit:
				return
			case tblID := <-do.historicalStatsWorker.tblCH:
				err := do.historicalStatsWorker.DumpHistoricalStats(tblID, do.StatsHandle())
				if err != nil {
					logutil.BgLogger().Warn("dump historical stats failed", zap.Error(err), zap.Int64("tableID", tblID))
				}
			}
		}
	}()
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	return (*handle.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx sessionctx.Context) error {
	h, err := handle.NewHandle(ctx, do.statsLease, do.sysSessionPool, &do.sysProcesses, do.ServerID)
	if err != nil {
		return err
	}
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(h))
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

// ReleaseAnalyzeExec returned extra exec for Analyze
func (do *Domain) ReleaseAnalyzeExec(sctxs []sessionctx.Context) {
	do.analyzeMu.Lock()
	defer do.analyzeMu.Unlock()
	for _, ctx := range sctxs {
		do.analyzeMu.sctxs[ctx] = false
	}
}

// FetchAnalyzeExec get needed exec for analyze
func (do *Domain) FetchAnalyzeExec(need int) []sessionctx.Context {
	if need < 1 {
		return nil
	}
	count := 0
	r := make([]sessionctx.Context, 0)
	do.analyzeMu.Lock()
	defer do.analyzeMu.Unlock()
	for sctx, used := range do.analyzeMu.sctxs {
		if used {
			continue
		}
		r = append(r, sctx)
		do.analyzeMu.sctxs[sctx] = true
		count++
		if count >= need {
			break
		}
	}
	return r
}

// SetupAnalyzeExec setups exec for Analyze Executor
func (do *Domain) SetupAnalyzeExec(ctxs []sessionctx.Context) {
	do.analyzeMu.sctxs = make(map[sessionctx.Context]bool)
	for _, ctx := range ctxs {
		do.analyzeMu.sctxs[ctx] = false
	}
}

// LoadAndUpdateStatsLoop loads and updates stats info.
func (do *Domain) LoadAndUpdateStatsLoop(ctxs []sessionctx.Context) error {
	if err := do.UpdateTableStatsLoop(ctxs[0]); err != nil {
		return err
	}
	do.StartLoadStatsSubWorkers(ctxs[1:])
	return nil
}

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	statsHandle, err := handle.NewHandle(ctx, do.statsLease, do.sysSessionPool, &do.sysProcesses, do.ServerID)
	if err != nil {
		return err
	}
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.ddl.RegisterStatsHandle(statsHandle)
	// Negative stats lease indicates that it is in test, it does not need update.
	if do.statsLease >= 0 {
		do.wg.Run(do.loadStatsWorker)
	}
	owner := do.newOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	if do.indexUsageSyncLease > 0 {
		do.wg.Add(1)
		go do.syncIndexUsageWorker(owner)
	}
	if do.statsLease <= 0 {
		return nil
	}
	do.SetStatsUpdating(true)
	do.wg.Run(func() { do.updateStatsWorker(ctx, owner) })
	do.wg.Run(func() { do.autoAnalyzeWorker(owner) })
	do.wg.Run(func() { do.gcAnalyzeHistory(owner) })
	return nil
}

// StartLoadStatsSubWorkers starts sub workers with new sessions to load stats concurrently.
func (do *Domain) StartLoadStatsSubWorkers(ctxList []sessionctx.Context) {
	statsHandle := do.StatsHandle()
	for i, ctx := range ctxList {
		statsHandle.StatsLoad.SubCtxs[i] = ctx
		do.wg.Add(1)
		go statsHandle.SubLoadWorker(ctx, do.exit, &do.wg)
	}
}

func (do *Domain) newOwnerManager(prompt, ownerKey string) owner.Manager {
	id := do.ddl.OwnerManager().ID()
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(context.Background(), id)
	} else {
		statsOwner = owner.NewOwnerManager(context.Background(), do.etcdClient, prompt, id, ownerKey)
	}
	// TODO: Need to do something when err is not nil.
	err := statsOwner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
	}
	return statsOwner
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
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.InfoSchema())
	if err != nil {
		logutil.BgLogger().Error("init stats info failed", zap.Duration("take time", time.Since(t)), zap.Error(err))
	} else {
		logutil.BgLogger().Info("init stats info time", zap.Duration("take time", time.Since(t)))
	}
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.RefreshVars()
			if err != nil {
				logutil.BgLogger().Debug("refresh variables failed", zap.Error(err))
			}
			err = statsHandle.Update(do.InfoSchema())
			if err != nil {
				logutil.BgLogger().Debug("update stats info failed", zap.Error(err))
			}
			err = statsHandle.LoadNeededHistograms()
			if err != nil {
				logutil.BgLogger().Debug("load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) syncIndexUsageWorker(owner owner.Manager) {
	defer util.Recover(metrics.LabelDomain, "syncIndexUsageWorker", nil, false)
	idxUsageSyncTicker := time.NewTicker(do.indexUsageSyncLease)
	gcStatsTicker := time.NewTicker(100 * do.indexUsageSyncLease)
	handle := do.StatsHandle()
	defer func() {
		idxUsageSyncTicker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("syncIndexUsageWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			// TODO: need flush index usage
			return
		case <-idxUsageSyncTicker.C:
			if err := handle.DumpIndexUsageToKV(); err != nil {
				logutil.BgLogger().Debug("dump index usage failed", zap.Error(err))
			}
		case <-gcStatsTicker.C:
			if !owner.IsOwner() {
				continue
			}
			if err := handle.GCIndexUsage(); err != nil {
				logutil.BgLogger().Error("[stats] gc index usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) updateStatsWorker(ctx sessionctx.Context, owner owner.Manager) {
	defer util.Recover(metrics.LabelDomain, "updateStatsWorker", nil, false)
	lease := do.statsLease
	deltaUpdateTicker := time.NewTicker(20 * lease)
	gcStatsTicker := time.NewTicker(100 * lease)
	dumpFeedbackTicker := time.NewTicker(200 * lease)
	loadFeedbackTicker := time.NewTicker(5 * lease)
	loadLockedTablesTicker := time.NewTicker(5 * lease)
	dumpColStatsUsageTicker := time.NewTicker(100 * lease)
	readMemTricker := time.NewTicker(memory.ReadMemInterval)
	statsHandle := do.StatsHandle()
	defer func() {
		dumpColStatsUsageTicker.Stop()
		loadFeedbackTicker.Stop()
		dumpFeedbackTicker.Stop()
		gcStatsTicker.Stop()
		deltaUpdateTicker.Stop()
		readMemTricker.Stop()
		do.SetStatsUpdating(false)
		logutil.BgLogger().Info("updateStatsWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			statsHandle.FlushStats()
			owner.Cancel()
			return
			// This channel is sent only by ddl owner.
		case t := <-statsHandle.DDLEventCh():
			err := statsHandle.HandleDDLEvent(t)
			if err != nil {
				logutil.BgLogger().Error("handle ddl event failed", zap.String("event", t.String()), zap.Error(err))
			}
		case <-deltaUpdateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV(handle.DumpDelta)
			if err != nil {
				logutil.BgLogger().Debug("dump stats delta failed", zap.Error(err))
			}
			statsHandle.UpdateErrorRate(do.InfoSchema())
		case <-loadFeedbackTicker.C:
			statsHandle.UpdateStatsByLocalFeedback(do.InfoSchema())
			if !owner.IsOwner() {
				continue
			}
			err := statsHandle.HandleUpdateStats(do.InfoSchema())
			if err != nil {
				logutil.BgLogger().Debug("update stats using feedback failed", zap.Error(err))
			}
		case <-loadLockedTablesTicker.C:
			err := statsHandle.LoadLockedTables()
			if err != nil {
				logutil.BgLogger().Debug("load locked table failed", zap.Error(err))
			}
		case <-dumpFeedbackTicker.C:
			err := statsHandle.DumpStatsFeedbackToKV()
			if err != nil {
				logutil.BgLogger().Debug("dump stats feedback failed", zap.Error(err))
			}
		case <-gcStatsTicker.C:
			if !owner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.InfoSchema(), do.DDL().GetLease())
			if err != nil {
				logutil.BgLogger().Debug("GC stats failed", zap.Error(err))
			}
		case <-dumpColStatsUsageTicker.C:
			err := statsHandle.DumpColStatsUsageToKV()
			if err != nil {
				logutil.BgLogger().Debug("dump column stats usage failed", zap.Error(err))
			}

		case <-readMemTricker.C:
			memory.ForceReadMemStats()
		}
	}
}

func (do *Domain) autoAnalyzeWorker(owner owner.Manager) {
	defer util.Recover(metrics.LabelDomain, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		logutil.BgLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			if variable.RunAutoAnalyze.Load() && owner.IsOwner() {
				statsHandle.HandleAutoAnalyze(do.InfoSchema())
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) gcAnalyzeHistory(owner owner.Manager) {
	defer util.Recover(metrics.LabelDomain, "gcAnalyzeHistory", nil, false)
	const gcInterval = time.Hour
	statsHandle := do.StatsHandle()
	gcTicker := time.NewTicker(gcInterval)
	defer func() {
		gcTicker.Stop()
		logutil.BgLogger().Info("gcAnalyzeHistory exited.")
	}()
	for {
		select {
		case <-gcTicker.C:
			if owner.IsOwner() {
				const DaysToKeep = 7
				updateTime := time.Now().AddDate(0, 0, -DaysToKeep)
				err := statsHandle.DeleteAnalyzeJobs(updateTime)
				if err != nil {
					logutil.BgLogger().Warn("gc analyze history failed", zap.Error(err))
				}
			}
		case <-do.exit:
			return
		}
	}
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

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege() error {
	// No matter skip-grant-table is configured or not, sending an etcd message is required.
	// Because we need to tell other TiDB instances to update privilege data, say, we're changing the
	// password using a special TiDB instance and want the new password to take effect.
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), privilegeKey, "")
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

	// update locally
	sysSessionPool := do.SysSessionPool()
	ctx, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(ctx)
	return do.PrivilegeHandle().Update(ctx.(sessionctx.Context))
}

// NotifyUpdateSysVarCache updates the sysvar cache key in etcd, which other TiDB
// clients are subscribed to for updates. For the caller, the cache is also built
// synchronously so that the effect is immediate.
func (do *Domain) NotifyUpdateSysVarCache() {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), sysVarCacheKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update sysvar cache failed", zap.Error(err))
		}
	}
	// update locally
	if err := do.rebuildSysVarCache(nil); err != nil {
		logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
	}
}

// LoadSigningCertLoop loads the signing cert periodically to make sure it's fresh new.
func (do *Domain) LoadSigningCertLoop(signingCert, signingKey string) {
	sessionstates.SetCertPath(signingCert)
	sessionstates.SetKeyPath(signingKey)

	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Debug("loadSigningCertLoop exited.")
			util.Recover(metrics.LabelDomain, "LoadSigningCertLoop", nil, false)
		}()
		for {
			select {
			case <-time.After(sessionstates.LoadCertInterval):
				sessionstates.ReloadSigningCert()
			case <-do.exit:
				return
			}
		}
	}()
}

// ServerID gets serverID.
func (do *Domain) ServerID() uint64 {
	return atomic.LoadUint64(&do.serverID)
}

// IsLostConnectionToPD indicates lost connection to PD or not.
func (do *Domain) IsLostConnectionToPD() bool {
	return do.isLostConnectionToPD.Load() != 0
}

const (
	serverIDEtcdPath               = "/tidb/server_id"
	refreshServerIDRetryCnt        = 3
	acquireServerIDRetryInterval   = 300 * time.Millisecond
	acquireServerIDTimeout         = 10 * time.Second
	retrieveServerIDSessionTimeout = 10 * time.Second
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

	for {
		// get a random serverID: [1, MaxServerID]
		randServerID := rand.Int63n(int64(util.MaxServerID)) + 1 // #nosec G404
		key := fmt.Sprintf("%s/%v", serverIDEtcdPath, randServerID)
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
			logutil.BgLogger().Info("proposed random serverID exists, will randomize again", zap.Int64("randServerID", randServerID))
			time.Sleep(acquireServerIDRetryInterval)
			continue
		}

		atomic.StoreUint64(&do.serverID, uint64(randServerID))
		logutil.BgLogger().Info("acquireServerID", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
		return nil
	}
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

		if err := do.info.StoreServerInfo(context.Background()); err != nil {
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

func (do *Domain) runTTLJobManager(ctx context.Context) {
	ttlJobManager := ttlworker.NewJobManager(do.ddl.GetID(), do.sysSessionPool, do.store)
	ttlJobManager.Start()
	do.ttlJobManager = ttlJobManager

	// TODO: read the worker count from `do.sysVarCache` and resize the workers
	ttlworker.ScanWorkersCount.Store(4)
	ttlworker.DeleteWorkerCount.Store(4)

	<-do.exit

	ttlJobManager.Stop()
	err := ttlJobManager.WaitStopped(ctx, 30*time.Second)
	if err != nil {
		logutil.BgLogger().Warn("fail to wait until the ttl job manager stop", zap.Error(err))
	}
}

// TTLJobManager returns the ttl job manager on this domain
func (do *Domain) TTLJobManager() *ttlworker.JobManager {
	return do.ttlJobManager
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
	procMap map[uint64]sessionctx.Context
}

// Track tracks the sys process into procMap
func (s *SysProcesses) Track(id uint64, proc sessionctx.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldProc, ok := s.procMap[id]; ok && oldProc != proc {
		return errors.Errorf("The ID is in use: %v", id)
	}
	s.procMap[id] = proc
	proc.GetSessionVars().ConnectionID = id
	atomic.StoreUint32(&proc.GetSessionVars().Killed, 0)
	return nil
}

// UnTrack removes the sys process from procMap
func (s *SysProcesses) UnTrack(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		delete(s.procMap, id)
		proc.GetSessionVars().ConnectionID = 0
		atomic.StoreUint32(&proc.GetSessionVars().Killed, 0)
	}
}

// GetSysProcessList gets list of system ProcessInfo
func (s *SysProcesses) GetSysProcessList() map[uint64]*util.ProcessInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs := make(map[uint64]*util.ProcessInfo)
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
		atomic.StoreUint32(&proc.GetSessionVars().Killed, 1)
	}
}
