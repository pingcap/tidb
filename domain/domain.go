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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/pools"
	"github.com/ngaut/sync2"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/expensivequery"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store                kv.Storage
	infoCache            *infoschema.InfoCache
	privHandle           *privileges.Handle
	bindHandle           *bindinfo.BindHandle
	statsHandle          unsafe.Pointer
	statsLease           time.Duration
	ddl                  ddl.DDL
	info                 *infosync.InfoSyncer
	m                    sync.Mutex
	SchemaValidator      SchemaValidator
	sysSessionPool       *sessionPool
	exit                 chan struct{}
	etcdClient           *clientv3.Client
	sysVarCache          SysVarCache // replaces GlobalVariableCache
	slowQuery            *topNSlowQueries
	expensiveQueryHandle *expensivequery.Handle
	wg                   sync.WaitGroup
	statsUpdating        sync2.AtomicInt32
	cancel               context.CancelFunc
	indexUsageSyncLease  time.Duration

	serverID             uint64
	serverIDSession      *concurrency.Session
	isLostConnectionToPD sync2.AtomicInt32 // !0: true, 0: false.
}

// loadInfoSchema loads infoschema at startTS.
// It returns:
// 1. the needed infoschema
// 2. cache hit indicator
// 3. currentSchemaVersion(before loading)
// 4. the changed table IDs if it is not full load
// 5. an error if any
func (do *Domain) loadInfoSchema(startTS uint64) (infoschema.InfoSchema, bool, int64, *tikv.RelatedSchemaChange, error) {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	m := meta.NewSnapshotMeta(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersion()
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
			do.infoCache.Insert(is)
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

	bundles, err := infosync.GetAllRuleBundles(context.TODO())
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	newISBuilder, err := infoschema.NewBuilder(do.Store()).InitWithDBInfos(schemas, bundles, neededSchemaVersion)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	logutil.BgLogger().Info("full load InfoSchema success",
		zap.Int64("currentSchemaVersion", currentSchemaVersion),
		zap.Int64("neededSchemaVersion", neededSchemaVersion),
		zap.Duration("start time", time.Since(startTime)))

	is := newISBuilder.Build()
	do.infoCache.Insert(is)
	return is, false, currentSchemaVersion, nil, nil
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
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (infoschema.InfoSchema, *tikv.RelatedSchemaChange, error) {
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return nil, nil, err
		}
		if diff == nil {
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return nil, nil, fmt.Errorf("failed to get schemadiff")
		}
		diffs = append(diffs, diff)
	}
	builder := infoschema.NewBuilder(do.Store()).InitWithOldInfoSchema(do.infoCache.GetLatest())
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
			actions = append(actions, uint64(1<<diff.Type))
		}
	}
	is := builder.Build()
	relatedChange := tikv.RelatedSchemaChange{}
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
	is, _, _, _, err := do.loadInfoSchema(snapshotTS)
	return is, err
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) (*meta.Meta, error) {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	return meta.NewSnapshotMeta(snapshot), nil
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *infosync.InfoSyncer {
	return do.info
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
	ver, err := do.store.CurrentVersion(oracle.GlobalTxnScope)
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
			err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), is.SchemaMetaVersion())
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

	do.sysSessionPool.Close()
	do.slowQuery.Close()
	do.cancel()
	do.wg.Wait()
	logutil.BgLogger().Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, ddlLease time.Duration, statsLease time.Duration, idxUsageSyncLease time.Duration, factory pools.Factory) *Domain {
	capacity := 200 // capacity of the sysSessionPool size
	do := &Domain{
		store:               store,
		exit:                make(chan struct{}),
		sysSessionPool:      newSessionPool(capacity, factory),
		statsLease:          statsLease,
		infoCache:           infoschema.NewCache(16),
		slowQuery:           newTopNSlowQueries(30, time.Hour*24*7, 500),
		indexUsageSyncLease: idxUsageSyncLease,
	}

	do.SchemaValidator = NewSchemaValidator(ddlLease, do)
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
	return do
}

const serverIDForStandalone = 1 // serverID for standalone deployment.

// Init initializes a domain.
func (do *Domain) Init(ddlLease time.Duration, sysFactory func(*Domain) (pools.Resource, error)) error {
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
		return sysFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 2, 2, resourceIdleTimeout)
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
	err = do.ddl.Start(sysCtxPool)
	if err != nil {
		return err
	}
	failpoint.Inject("MockReplaceDDL", func(val failpoint.Value) {
		if val.(bool) {
			if err := do.ddl.Stop(); err != nil {
				logutil.BgLogger().Error("stop DDL failed", zap.Error(err))
			}
			do.ddl = d
		}
	})

	skipRegisterToDashboard := config.GetGlobalConfig().SkipRegisterToDashboard
	err = do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return err
	}

	if config.GetGlobalConfig().Experimental.EnableGlobalKill {
		if do.etcdClient != nil {
			err := do.acquireServerID(ctx)
			if err != nil {
				logutil.BgLogger().Error("acquire serverID failed", zap.Error(err))
				do.isLostConnectionToPD.Set(1) // will retry in `do.serverIDKeeper`
			} else {
				do.isLostConnectionToPD.Set(0)
			}

			do.wg.Add(1)
			go do.serverIDKeeper()
		} else {
			// set serverID for standalone deployment to enable 'KILL'.
			atomic.StoreUint64(&do.serverID, serverIDForStandalone)
		}
	}

	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.ddl.GetID(), do.ServerID, do.etcdClient, skipRegisterToDashboard)
	if err != nil {
		return err
	}
	err = do.Reload()
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
	do.wg.Add(1)
	go do.topNSlowQueryLoop()

	do.wg.Add(1)
	go do.infoSyncerKeeper()

	if !skipRegisterToDashboard {
		do.wg.Add(1)
		go do.topologySyncerKeeper()
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

func newSessionPool(cap int, factory pools.Factory) *sessionPool {
	return &sessionPool{
		resources: make(chan pools.Resource, cap),
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
	return
}

func (p *sessionPool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
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

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	do.privHandle = privileges.NewHandle()
	err := do.privHandle.Update(ctx)
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
			err := do.privHandle.Update(ctx)
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
	err := do.sysVarCache.RebuildSysVarCache(ctx)
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
			err := do.sysVarCache.RebuildSysVarCache(ctx)
			metrics.LoadSysVarCacheCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("LoadSysVarCacheLoop failed", zap.Error(err))
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
	return do.bindHandle
}

// LoadBindInfoLoop create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) LoadBindInfoLoop(ctxForHandle sessionctx.Context, ctxForEvolve sessionctx.Context) error {
	ctxForHandle.GetSessionVars().InRestrictedSQL = true
	ctxForEvolve.GetSessionVars().InRestrictedSQL = true
	do.bindHandle = bindinfo.NewBindHandle(ctxForHandle)
	err := do.bindHandle.Update(true)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	do.globalBindHandleWorkerLoop()
	do.handleEvolvePlanTasksLoop(ctxForEvolve)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop() {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
			util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)
		}()
		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		defer bindWorkerTicker.Stop()
		for {
			select {
			case <-do.exit:
				return
			case <-bindWorkerTicker.C:
				err := do.bindHandle.Update(false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
				do.bindHandle.DropInvalidBindRecord()
				if variable.TiDBOptOn(variable.CapturePlanBaseline.GetVal()) {
					do.bindHandle.CaptureBaselines()
				}
				do.bindHandle.SaveEvolveTasksToStore()
			}
		}
	}()
}

func (do *Domain) handleEvolvePlanTasksLoop(ctx sessionctx.Context) {
	do.wg.Add(1)
	go func() {
		defer func() {
			do.wg.Done()
			logutil.BgLogger().Info("handleEvolvePlanTasksLoop exited.")
			util.Recover(metrics.LabelDomain, "handleEvolvePlanTasksLoop", nil, false)
		}()
		owner := do.newOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
		for {
			select {
			case <-do.exit:
				owner.Cancel()
				return
			case <-time.After(bindinfo.Lease):
			}
			if owner.IsOwner() {
				err := do.bindHandle.HandleEvolvePlanTask(ctx, false)
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

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	return (*handle.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx sessionctx.Context) error {
	h, err := handle.NewHandle(ctx, do.statsLease, do.sysSessionPool)
	if err != nil {
		return err
	}
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(h))
	return nil
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	return do.statsUpdating.Get() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	if val {
		do.statsUpdating.Set(1)
	} else {
		do.statsUpdating.Set(0)
	}
}

// RunAutoAnalyze indicates if this TiDB server starts auto analyze worker and can run auto analyze job.
var RunAutoAnalyze = true

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	statsHandle, err := handle.NewHandle(ctx, do.statsLease, do.sysSessionPool)
	if err != nil {
		return err
	}
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.ddl.RegisterStatsHandle(statsHandle)
	// Negative stats lease indicates that it is in test, it does not need update.
	if do.statsLease >= 0 {
		do.wg.Add(1)
		go do.loadStatsWorker()
	}
	owner := do.newOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	if do.indexUsageSyncLease > 0 {
		do.wg.Add(1)
		go do.syncIndexUsageWorker(owner)
	}
	if do.statsLease <= 0 {
		return nil
	}
	do.wg.Add(1)
	do.SetStatsUpdating(true)
	go do.updateStatsWorker(ctx, owner)
	if RunAutoAnalyze {
		do.wg.Add(1)
		go do.autoAnalyzeWorker(owner)
	}
	return nil
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
		do.wg.Done()
		logutil.BgLogger().Info("loadStatsWorker exited.")
	}()
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.InfoSchema())
	if err != nil {
		logutil.BgLogger().Debug("init stats info failed", zap.Error(err))
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
	statsHandle := do.StatsHandle()
	defer func() {
		loadFeedbackTicker.Stop()
		dumpFeedbackTicker.Stop()
		gcStatsTicker.Stop()
		deltaUpdateTicker.Stop()
		do.SetStatsUpdating(false)
		do.wg.Done()
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
				logutil.BgLogger().Debug("handle ddl event failed", zap.Error(err))
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
		}
	}
}

func (do *Domain) autoAnalyzeWorker(owner owner.Manager) {
	defer util.Recover(metrics.LabelDomain, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		do.wg.Done()
		logutil.BgLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			if owner.IsOwner() {
				statsHandle.HandleAutoAnalyze(do.InfoSchema())
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

const (
	privilegeKey   = "/tidb/privilege"
	sysVarCacheKey = "/tidb/sysvars"
)

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(ctx sessionctx.Context) {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), privilegeKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update privilege failed", zap.Error(err))
		}
	}
	// update locally
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	if stmt, err := exec.ParseWithParams(context.Background(), `FLUSH PRIVILEGES`); err == nil {
		_, _, err := exec.ExecRestrictedStmt(context.Background(), stmt)
		if err != nil {
			logutil.BgLogger().Error("unable to update privileges", zap.Error(err))
		}
	}
}

// NotifyUpdateSysVarCache updates the sysvar cache key in etcd, which other TiDB
// clients are subscribed to for updates. For the caller, the cache is also built
// synchronously so that the effect is immediate.
func (do *Domain) NotifyUpdateSysVarCache(ctx sessionctx.Context) {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), sysVarCacheKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update sysvar cache failed", zap.Error(err))
		}
	}
	// update locally
	if err := do.sysVarCache.RebuildSysVarCache(ctx); err != nil {
		logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
	}
}

// ServerID gets serverID.
func (do *Domain) ServerID() uint64 {
	return atomic.LoadUint64(&do.serverID)
}

// IsLostConnectionToPD indicates lost connection to PD or not.
func (do *Domain) IsLostConnectionToPD() bool {
	return do.isLostConnectionToPD.Get() != 0
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
		randServerID := rand.Int63n(int64(util.MaxServerID)) + 1 // get a random serverID: [1, MaxServerID]
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
		do.isLostConnectionToPD.Set(0)
		lastSucceedTimestamp = time.Now()

		if err := do.info.StoreServerInfo(context.Background()); err != nil {
			logutil.BgLogger().Error("StoreServerInfo failed", zap.Error(err))
		}
	}

	onConnectionToPDLost := func() {
		logutil.BgLogger().Warn("lost connection to PD")
		do.isLostConnectionToPD.Set(1)

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

func init() {
	initByLDFlagsForGlobalKill()
}

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = dbterror.ClassDomain.NewStd(errno.ErrInfoSchemaExpired)
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = dbterror.ClassDomain.NewStdErr(errno.ErrInfoSchemaChanged,
		mysql.Message(errno.MySQLErrName[errno.ErrInfoSchemaChanged].Raw+". "+kv.TxnRetryableMark, nil))
)
