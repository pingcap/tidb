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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/extension/extensionimpl"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

func splitAndScatterTable(store kv.Storage, tableIDs []int64) {
	if s, ok := store.(kv.SplittableStore); ok && atomic.LoadUint32(&ddl.EnableSplitTableRegion) == 1 {
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), vardef.DefWaitSplitRegionTimeout*time.Second)
		defer cancel()
		keys := make([][]byte, 0, len(tableIDs))
		for _, id := range tableIDs {
			keys = append(keys, tablecodec.GenTablePrefix(id))
		}
		gid := ddl.GlobalScatterGroupID
		// tables created through DDL during bootstrap also don't scatter, we keep
		// the same behavior here.
		_, err := s.SplitRegions(ctxWithTimeout, keys, false, &gid)
		if err != nil {
			// It will be automatically split by TiKV later.
			logutil.BgLogger().Warn("split table region failed", zap.Error(err))
		}
	}
}

// InitDDLTables creates system tables that DDL uses. Because CREATE TABLE is
// also a DDL, we must directly modify KV data to create these tables.
func InitDDLTables(store kv.Storage) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	return kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		currVer, err := t.GetDDLTableVersion()
		if err != nil {
			return errors.Trace(err)
		}
		dbID, err := t.CreateMySQLDatabaseIfNotExists()
		if err != nil {
			return err
		}

		largestVer := currVer
		for _, vt := range ddlTableVersionTables {
			if currVer >= vt.ver {
				continue
			}
			logutil.BgLogger().Info("init DDL tables", zap.Int("currVer", int(currVer)),
				zap.Int("targetVer", int(vt.ver)))
			largestVer = max(largestVer, vt.ver)
			if err = createAndSplitTables(store, t, dbID, vt.tables); err != nil {
				return err
			}
		}
		if largestVer > currVer {
			return t.SetDDLTableVersion(largestVer)
		}
		return nil
	})
}

func createAndSplitTables(store kv.Storage, t *meta.Mutator, dbID int64, tables []TableBasicInfo) error {
	var (
		tableIDs = make([]int64, 0, len(tables))
		tblInfos = make([]*model.TableInfo, 0, len(tables))
	)
	p := parser.New()
	for _, tbl := range tables {
		failpoint.InjectCall("mockCreateSystemTableSQL", &tbl)
		tableIDs = append(tableIDs, tbl.ID)

		stmt, err := p.ParseOneStmt(tbl.SQL, "", "")
		if err != nil {
			return errors.Trace(err)
		}
		// bootstrap session set sessionctx.Initing = true, and uses None SQL mode,
		// we also use it here.
		evalCtx := exprstatic.NewEvalContext(exprstatic.WithSQLMode(mysql.ModeNone))
		exprCtx := exprstatic.NewExprContext(exprstatic.WithEvalCtx(evalCtx))
		mbCtx := metabuild.NewContext(metabuild.WithExprCtx(exprCtx))
		tblInfo, err := ddl.BuildTableInfoFromAST(mbCtx, stmt.(*ast.CreateTableStmt))
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.ID = tbl.ID
		tblInfo.UpdateTS = t.StartTS
		if err = checkSystemTableConstraint(tblInfo); err != nil {
			return errors.Trace(err)
		}

		tblInfos = append(tblInfos, tblInfo)
	}

	splitAndScatterTable(store, tableIDs)
	for _, tblInfo := range tblInfos {
		err := t.CreateTableOrView(dbID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// InitMDLVariableForBootstrap initializes the metadata lock variable.
func InitMDLVariableForBootstrap(store kv.Storage) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetMetadataLock(true)
	})
	if err != nil {
		return err
	}
	vardef.SetEnableMDL(true)
	return nil
}

// InitTiDBSchemaCacheSize initializes the tidb schema cache size.
func InitTiDBSchemaCacheSize(store kv.Storage) error {
	var (
		isNull bool
		size   uint64
		err    error
	)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		size, isNull, err = t.GetSchemaCacheSize()
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			size = vardef.DefTiDBSchemaCacheSize
			return t.SetSchemaCacheSize(size)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	vardef.SchemaCacheSize.Store(size)
	return nil
}

// InitMDLVariable initializes the metadata lock variable.
func InitMDLVariable(store kv.Storage) error {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		if isNull {
			// Workaround for version: nightly-2022-11-07 to nightly-2022-11-17.
			enable = true
			logutil.BgLogger().Warn("metadata lock is null")
			err = t.SetMetadataLock(true)
			if err != nil {
				return err
			}
		}
		return nil
	})
	vardef.SetEnableMDL(enable)
	return err
}

// BootstrapSession bootstrap session and domain.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(context.Background(), store, createSessions)
}

// BootstrapSession4DistExecution bootstrap session and dom for Distributed execution test, only for unit testing.
func BootstrapSession4DistExecution(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(context.Background(), store, createSessions4DistExecution)
}

// bootstrapSessionImpl bootstraps session and domain.
// the process works as follows:
// - if we haven't bootstrapped to the target version
//   - create/init/start domain
//   - bootstrap or upgrade, some variables will be initialized and stored to system
//     table in the process, such as system time-zone
//   - close domain
//
// - create/init another domain
// - initialization global variables from system table that's required to use sessionCtx,
// such as system time zone
// - start domain and other routines.
func bootstrapSessionImpl(ctx context.Context, store kv.Storage, createSessionsImpl func(store kv.Storage, cnt int) ([]*session, error)) (*domain.Domain, error) {
	ver := getStoreBootstrapVersionWithCache(store)
	if kv.IsUserKS(store) {
		systemKSVer := mustGetStoreBootstrapVersion(kvstore.GetSystemStorage())
		if systemKSVer == notBootstrapped {
			logutil.BgLogger().Fatal("SYSTEM keyspace is not bootstrapped")
		} else if ver > systemKSVer {
			logutil.BgLogger().Fatal("bootstrap version of user keyspace must be smaller or equal to that of SYSTEM keyspace",
				zap.Int64("user", ver), zap.Int64("system", systemKSVer))
		}
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	cfg := config.GetGlobalConfig()
	if len(cfg.Instance.PluginLoad) > 0 {
		err := plugin.Load(context.Background(), plugin.Config{
			Plugins:   strings.Split(cfg.Instance.PluginLoad, ","),
			PluginDir: cfg.Instance.PluginDir,
		})
		if err != nil {
			return nil, err
		}
	}
	if kerneltype.IsNextGen() {
		if err := bootstrapSchemas(store); err != nil {
			return nil, err
		}
	}
	err := InitDDLTables(store)
	if err != nil {
		return nil, err
	}
	err = InitTiDBSchemaCacheSize(store)
	if err != nil {
		return nil, err
	}
	if ver < currentBootstrapVersion {
		runInBootstrapSession(store, ver)
	} else {
		logutil.BgLogger().Info("cluster already bootstrapped", zap.Int64("version", ver))
		err = InitMDLVariable(store)
		if err != nil {
			return nil, err
		}
	}

	// initiate disttask framework components which need a store
	scheduler.RegisterSchedulerFactory(
		proto.ImportInto,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return importinto.NewImportScheduler(ctx, task, param)
		},
	)
	taskexecutor.RegisterTaskType(
		proto.ImportInto,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return importinto.NewImportExecutor(ctx, task, param)
		},
	)

	concurrency := config.GetGlobalConfig().Performance.StatsLoadConcurrency
	if concurrency == 0 {
		// if concurrency is 0, we will set the concurrency of sync load by CPU.
		concurrency = syncload.GetSyncLoadConcurrencyByCPU()
	}
	if concurrency < 0 { // it is only for test, in the production, negative value is illegal.
		concurrency = 0
	}

	ses, err := createSessionsImpl(store, 10)
	if err != nil {
		return nil, err
	}
	// Mark all bootstrap sessions as restricted since they are used for internal operations
	// ses[0]: main bootstrap session
	// ses[1-2]: reserved
	// ses[3]: privilege loading
	// ses[4]: sysvar cache
	// ses[5]: telemetry, expression pushdown
	// ses[6]: plan replayer collector
	// ses[7]: dump file GC
	// ses[8]: historical stats
	// ses[9]: bootstrap SQL file
	for i := range ses {
		ses[i].GetSessionVars().InRestrictedSQL = true
	}

	// get system tz from mysql.tidb
	tz, err := ses[0].getTableValue(ctx, mysql.TiDBTable, tidbSystemTZ)
	if err != nil {
		return nil, err
	}
	timeutil.SetSystemTZ(tz)

	// get the flag from `mysql`.`tidb` which indicating if new collations are enabled.
	newCollationEnabled, err := loadCollationParameter(ctx, ses[0])
	if err != nil {
		return nil, err
	}
	collate.SetNewCollationEnabledForTest(newCollationEnabled)

	// only start the domain after we have initialized some global variables.
	dom := domain.GetDomain(ses[0])
	err = dom.Start(ddl.Normal)
	if err != nil {
		return nil, err
	}

	// To deal with the location partition failure caused by inconsistent NewCollationEnabled values(see issue #32416).
	rebuildAllPartitionValueMapAndSorted(ctx, ses[0])

	if !config.GetGlobalConfig().Security.SkipGrantTable {
		err = dom.LoadPrivilegeLoop(ses[3])
		if err != nil {
			return nil, err
		}
	}

	// Rebuild sysvar cache in a loop
	err = dom.LoadSysVarCacheLoop(ses[4])
	if err != nil {
		return nil, err
	}

	// We should make the load bind-info loop before other loops which has internal SQL.
	// Binding Handle must be initialized after LoadSysVarCacheLoop since
	// it'll use `tidb_mem_quota_binding_cache` to set the cache size.
	err = dom.InitBindingHandle()
	if err != nil {
		return nil, err
	}

	if config.GetGlobalConfig().DisaggregatedTiFlash && !config.GetGlobalConfig().UseAutoScaler {
		// Invalid client-go tiflash_compute store cache if necessary.
		err = dom.WatchTiFlashComputeNodeChange()
		if err != nil {
			return nil, err
		}
	}

	if err = extensionimpl.Bootstrap(context.Background(), dom); err != nil {
		return nil, err
	}

	if len(cfg.Instance.PluginLoad) > 0 {
		err := plugin.Init(context.Background(), plugin.Config{EtcdClient: dom.GetEtcdClient()})
		if err != nil {
			return nil, err
		}
	}

	err = executor.LoadExprPushdownBlacklist(ses[5])
	if err != nil {
		return nil, err
	}
	err = executor.LoadOptRuleBlacklist(ctx, ses[5])
	if err != nil {
		return nil, err
	}

	if config.GetGlobalConfig().EnableTelemetry {
		// There is no way to turn telemetry on with global variable `tidb_enable_telemetry`
		// when it is disabled in config. See IsTelemetryEnabled function in telemetry/telemetry.go
		go func() {
			dom.TelemetryLoop(ses[5])
		}()
	}

	planReplayerWorkerCnt := config.GetGlobalConfig().Performance.PlanReplayerDumpWorkerConcurrency
	planReplayerWorkersSctx := make([]sessionctx.Context, planReplayerWorkerCnt)
	pworkerSes, err := createSessions(store, int(planReplayerWorkerCnt))
	if err != nil {
		return nil, err
	}
	for i := range int(planReplayerWorkerCnt) {
		planReplayerWorkersSctx[i] = pworkerSes[i]
	}
	// setup plan replayer handle
	dom.SetupPlanReplayerHandle(ses[6], planReplayerWorkersSctx)
	dom.StartPlanReplayerHandle()
	// setup dumpFileGcChecker
	dom.SetupDumpFileGCChecker(ses[7])
	dom.DumpFileGcCheckerLoop()
	// setup historical stats worker
	dom.SetupHistoricalStatsWorker(ses[8])
	dom.StartHistoricalStatsWorker()
	failToLoadOrParseSQLFile := false // only used for unit test
	if runBootstrapSQLFile {
		pm := &privileges.UserPrivileges{
			Handle: dom.PrivilegeHandle(),
		}
		privilege.BindPrivilegeManager(ses[9], pm)
		if err := doBootstrapSQLFile(ses[9]); err != nil && intest.EnableInternalCheck {
			failToLoadOrParseSQLFile = true
		}
	}

	// setup extract Handle
	extractWorkers := 1
	sctxs, err := createSessions(store, extractWorkers)
	if err != nil {
		return nil, err
	}
	extractWorkerSctxs := make([]sessionctx.Context, 0)
	for _, sctx := range sctxs {
		extractWorkerSctxs = append(extractWorkerSctxs, sctx)
	}
	dom.SetupExtractHandle(extractWorkerSctxs)

	// setup init stats loader
	if err = dom.LoadAndUpdateStatsLoop(concurrency); err != nil {
		return nil, err
	}

	// init the instance plan cache
	dom.InitInstancePlanCache()

	// setup workload-based learning worker
	dom.SetupWorkloadBasedLearningWorker()

	// start TTL job manager after setup stats collector
	// because TTL could modify a lot of columns, and need to trigger auto analyze
	statshandle.AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return attachStatsCollector(s, dom)
		}
		return s
	}
	statshandle.DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return detachStatsCollector(s)
		}
		return s
	}
	dom.StartTTLJobManager()

	dom.LoadSigningCertLoop(cfg.Security.SessionTokenSigningCert, cfg.Security.SessionTokenSigningKey)

	if raw, ok := store.(kv.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, err
		}
	}

	// This only happens in testing, since the failure of loading or parsing sql file
	// would panic the bootstrapping.
	if intest.EnableInternalCheck && failToLoadOrParseSQLFile {
		dom.Close()
		return nil, errors.New("Fail to load or parse sql file")
	}
	err = dom.InitDistTaskLoop()
	if err != nil {
		return nil, err
	}
	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// GetOrCreateDomainWithFilter gets the associated domain for store. If domain not created, create a new one with the given schema filter.
func GetOrCreateDomainWithFilter(store kv.Storage, filter issyncer.Filter) (*domain.Domain, error) {
	return domap.GetOrCreateWithFilter(store, filter)
}

// getStartMode gets the start mode according to the bootstrap version.
func getStartMode(ver int64) ddl.StartMode {
	if ver == notBootstrapped {
		return ddl.Bootstrap
	} else if ver < currentBootstrapVersion {
		return ddl.Upgrade
	}
	return ddl.Normal
}

// runInBootstrapSession create a special session for bootstrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, ver int64) {
	startMode := getStartMode(ver)
	startTime := time.Now()
	defer func() {
		logutil.BgLogger().Info("bootstrap cluster finished",
			zap.String("bootMode", string(startMode)),
			zap.Duration("cost", time.Since(startTime)))
	}()
	if startMode == ddl.Upgrade {
		// TODO at this time domain must not be created, else it will register server
		// info, and cause deadlock, we need to make sure this in a clear way
		logutil.BgLogger().Info("[upgrade] get owner lock to upgrade")
		releaseFn, err := acquireLock(store)
		if err != nil {
			logutil.BgLogger().Fatal("[upgrade] get owner lock failed", zap.Error(err))
		}
		defer releaseFn()
		currVer := mustGetStoreBootstrapVersion(store)
		if currVer >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by another TiDB instance, but
			// we still need to go through the following domain Start/Close code
			// right now as we have already initialized it when creating the session,
			// so we switch to normal mode.
			// TODO remove this after we can refactor below code out in this case.
			logutil.BgLogger().Info("[upgrade] already upgraded by other nodes, switch to normal mode")
			startMode = ddl.Normal
		}
	}
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	dom := domain.GetDomain(s)
	err = dom.Start(startMode)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("start domain error", zap.Error(err))
	}

	// For the bootstrap SQLs, the following variables should be compatible with old TiDB versions.
	// TODO we should have a createBootstrapSession to init those special variables.
	s.sessionVars.EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly

	s.SetValue(sessionctx.Initing, true)
	switch startMode {
	case ddl.Bootstrap:
		bootstrap(s)
	case ddl.Upgrade:
		// below sleep is used to mitigate https://github.com/pingcap/tidb/issues/57003,
		// to let the older owner have time to notice that it's already retired.
		time.Sleep(owner.WaitTimeOnForceOwner)
		upgrade(s)
	case ddl.Normal:
		// We need to init MDL variable before start the domain to prevent potential stuck issue
		// when upgrade is skipped. See https://github.com/pingcap/tidb/issues/64539.
		if err := InitMDLVariable(store); err != nil {
			logutil.BgLogger().Fatal("init metadata lock failed during normal startup", zap.Error(err))
		}
	}
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom.Close()
	if intest.InTest {
		infosync.MockGlobalServerInfoManagerEntry.Close()
	}
	domap.Delete(store)
}

