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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/extension/extensionimpl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session/internal"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/tikv/client-go/v2/oracle"
	tikvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func init() {
	executor.CreateSession = func(ctx sessionctx.Context) (sessionctx.Context, error) {
		return CreateSession(ctx.GetStore())
	}
	executor.CloseSession = func(ctx sessionctx.Context) {
		if se, ok := ctx.(types.Session); ok {
			se.Close()
		}
	}
}

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type ExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (ExecStmtVarKeyType) String() string {
	return "exec_stmt_var_key"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const ExecStmtVarKey ExecStmtVarKeyType = 0

// execStmtResult is the return value of ExecuteStmt and it implements the sqlexec.RecordSet interface.
// Why we need a struct to wrap a RecordSet and provide another RecordSet?
// This is because there are so many session state related things that definitely not belongs to the original
// RecordSet, so this struct exists and RecordSet.Close() is overrided handle that.
type execStmtResult struct {
	sqlexec.RecordSet
	se     *session
	sql    sqlexec.Statement
	closed bool
}

func (rs *execStmtResult) Close() error {
	if rs.closed {
		return nil
	}
	se := rs.se
	err := rs.RecordSet.Close()
	err = finishStmt(context.Background(), se, err, rs.sql)
	rs.closed = true
	return err
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (types.Session, error) {
	se, err := CreateSession4TestWithOpt(store, nil)
	if err == nil {
		// Cover both chunk rpc encoding and default encoding.
		// nolint:gosec
		if rand.Intn(2) == 0 {
			se.GetSessionVars().EnableChunkRPC = false
		} else {
			se.GetSessionVars().EnableChunkRPC = true
		}
	}
	return se, err
}

// Opt describes the option for creating session
type Opt struct {
	PreparedPlanCache sessionctx.PlanCache
}

// CreateSession4TestWithOpt creates a new session environment for test.
func CreateSession4TestWithOpt(store kv.Storage, opt *Opt) (types.Session, error) {
	s, err := CreateSessionWithOpt(store, opt)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
		s.GetSessionVars().MinPagingSize = variable.DefMinPagingSize
		s.GetSessionVars().EnablePaging = variable.DefTiDBEnablePaging
		err = s.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, "utf8mb4")
	}
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (types.Session, error) {
	return CreateSessionWithOpt(store, nil)
}

// CreateSessionWithOpt creates a new session environment with option.
// Use default option if opt is nil.
func CreateSessionWithOpt(store kv.Storage, opt *Opt) (types.Session, error) {
	s, err := createSessionWithOpt(store, opt)
	if err != nil {
		return nil, err
	}

	// Add auth here.
	do, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	extensions, err := extension.GetExtensions()
	if err != nil {
		return nil, err
	}
	pm := privileges.NewUserPrivileges(do.PrivilegeHandle(), extensions)
	privilege.BindPrivilegeManager(s, pm)

	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	if do.StatsHandle() != nil && do.StatsUpdating() {
		s.statsCollector = do.StatsHandle().NewSessionStatsItem().(*usage.SessionStatsItem)
		if GetIndexUsageSyncLease() > 0 {
			s.idxUsageCollector = do.StatsHandle().NewSessionIndexUsageCollector().(*usage.SessionIndexUsageCollector)
		}
	}

	return s, nil
}

// loadCollationParameter loads collation parameter from mysql.tidb
func loadCollationParameter(ctx context.Context, se *session) (bool, error) {
	para, err := se.getTableValue(ctx, mysql.TiDBTable, tidbNewCollationEnabled)
	if err != nil {
		return false, err
	}
	if para == varTrue {
		return true, nil
	} else if para == varFalse {
		return false, nil
	}
	logutil.BgLogger().Warn(
		"Unexpected value of 'new_collation_enabled' in 'mysql.tidb', use 'False' instead",
		zap.String("value", para))
	return false, nil
}

type tableBasicInfo struct {
	SQL string
	id  int64
}

var (
	errResultIsEmpty = dbterror.ClassExecutor.NewStd(errno.ErrResultIsEmpty)
	// DDLJobTables is a list of tables definitions used in concurrent DDL.
	DDLJobTables = []tableBasicInfo{
		{ddl.JobTableSQL, ddl.JobTableID},
		{ddl.ReorgTableSQL, ddl.ReorgTableID},
		{ddl.HistoryTableSQL, ddl.HistoryTableID},
	}
	// BackfillTables is a list of tables definitions used in dist reorg DDL.
	BackfillTables = []tableBasicInfo{
		{ddl.BackgroundSubtaskTableSQL, ddl.BackgroundSubtaskTableID},
		{ddl.BackgroundSubtaskHistoryTableSQL, ddl.BackgroundSubtaskHistoryTableID},
	}
	mdlTable = "create table mysql.tidb_mdl_info(job_id BIGINT NOT NULL PRIMARY KEY, version BIGINT NOT NULL, table_ids text(65535));"
)

func splitAndScatterTable(store kv.Storage, tableIDs []int64) {
	if s, ok := store.(kv.SplittableStore); ok && atomic.LoadUint32(&ddl.EnableSplitTableRegion) == 1 {
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), variable.DefWaitSplitRegionTimeout*time.Second)
		var regionIDs []uint64
		for _, id := range tableIDs {
			regionIDs = append(regionIDs, ddl.SplitRecordRegion(ctxWithTimeout, s, id, id, variable.DefTiDBScatterRegion))
		}
		if variable.DefTiDBScatterRegion {
			ddl.WaitScatterRegionFinish(ctxWithTimeout, s, regionIDs...)
		}
		cancel()
	}
}

// InitDDLJobTables is to create tidb_ddl_job, tidb_ddl_reorg and tidb_ddl_history, or tidb_background_subtask and tidb_background_subtask_history.
func InitDDLJobTables(store kv.Storage, targetVer meta.DDLTableVersion) error {
	targetTables := DDLJobTables
	if targetVer == meta.BackfillTableVersion {
		targetTables = BackfillTables
	}
	return kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		tableVer, err := t.CheckDDLTableVersion()
		if err != nil || tableVer >= targetVer {
			return errors.Trace(err)
		}
		dbID, err := t.CreateMySQLDatabaseIfNotExists()
		if err != nil {
			return err
		}
		if err = createAndSplitTables(store, t, dbID, targetTables); err != nil {
			return err
		}
		return t.SetDDLTables(targetVer)
	})
}

func createAndSplitTables(store kv.Storage, t *meta.Meta, dbID int64, tables []tableBasicInfo) error {
	tableIDs := make([]int64, 0, len(tables))
	for _, tbl := range tables {
		tableIDs = append(tableIDs, tbl.id)
	}
	splitAndScatterTable(store, tableIDs)
	p := parser.New()
	for _, tbl := range tables {
		stmt, err := p.ParseOneStmt(tbl.SQL, "", "")
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.ID = tbl.id
		tblInfo.UpdateTS = t.StartTS
		err = t.CreateTableOrView(dbID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// InitMDLTable is to create tidb_mdl_info, which is used for metadata lock.
func InitMDLTable(store kv.Storage) error {
	return kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		ver, err := t.CheckDDLTableVersion()
		if err != nil || ver >= meta.MDLTableVersion {
			return errors.Trace(err)
		}
		dbID, err := t.CreateMySQLDatabaseIfNotExists()
		if err != nil {
			return err
		}
		splitAndScatterTable(store, []int64{ddl.MDLTableID})
		p := parser.New()
		stmt, err := p.ParseOneStmt(mdlTable, "", "")
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.ID = ddl.MDLTableID
		tblInfo.UpdateTS = t.StartTS
		err = t.CreateTableOrView(dbID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}

		return t.SetDDLTables(meta.MDLTableVersion)
	})
}

// InitMDLVariableForBootstrap initializes the metadata lock variable.
func InitMDLVariableForBootstrap(store kv.Storage) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		return t.SetMetadataLock(true)
	})
	if err != nil {
		return err
	}
	variable.EnableMDL.Store(true)
	return nil
}

// InitMDLVariableForUpgrade initializes the metadata lock variable.
func InitMDLVariableForUpgrade(store kv.Storage) (bool, error) {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		return nil
	})
	if isNull || !enable {
		variable.EnableMDL.Store(false)
	} else {
		variable.EnableMDL.Store(true)
	}
	return isNull, err
}

// InitMDLVariable initializes the metadata lock variable.
func InitMDLVariable(store kv.Storage) error {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
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
	variable.EnableMDL.Store(enable)
	return err
}

// BootstrapSession bootstrap session and domain.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(store, createSessions)
}

// BootstrapSession4DistExecution bootstrap session and dom for Distributed execution test, only for unit testing.
func BootstrapSession4DistExecution(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(store, createSessions4DistExecution)
}

func bootstrapSessionImpl(store kv.Storage, createSessionsImpl func(store kv.Storage, cnt int) ([]*session, error)) (*domain.Domain, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
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
	err := InitDDLJobTables(store, meta.BaseDDLTableVersion)
	if err != nil {
		return nil, err
	}
	err = InitMDLTable(store)
	if err != nil {
		return nil, err
	}
	err = InitDDLJobTables(store, meta.BackfillTableVersion)
	if err != nil {
		return nil, err
	}
	ver := getStoreBootstrapVersion(store)
	if ver == notBootstrapped {
		runInBootstrapSession(store, bootstrap)
	} else if ver < currentBootstrapVersion {
		runInBootstrapSession(store, upgrade)
	} else {
		err = InitMDLVariable(store)
		if err != nil {
			return nil, err
		}
	}

	analyzeConcurrencyQuota := int(config.GetGlobalConfig().Performance.AnalyzePartitionConcurrencyQuota)
	concurrency := int(config.GetGlobalConfig().Performance.StatsLoadConcurrency)
	ses, err := createSessionsImpl(store, 10)
	if err != nil {
		return nil, err
	}
	ses[0].GetSessionVars().InRestrictedSQL = true

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
	// To deal with the location partition failure caused by inconsistent NewCollationEnabled values(see issue #32416).
	rebuildAllPartitionValueMapAndSorted(ses[0])

	dom := domain.GetDomain(ses[0])

	// We should make the load bind-info loop before other loops which has internal SQL.
	// Because the internal SQL may access the global bind-info handler. As the result, the data race occurs here as the
	// LoadBindInfoLoop inits global bind-info handler.
	err = dom.LoadBindInfoLoop(ses[1], ses[2])
	if err != nil {
		return nil, err
	}

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

	if dom.GetEtcdClient() != nil {
		// We only want telemetry data in production-like clusters. When TiDB is deployed over other engines,
		// for example, unistore engine (used for local tests), we just skip it. Its etcd client is nil.
		if config.GetGlobalConfig().EnableTelemetry {
			// There is no way to turn telemetry on with global variable `tidb_enable_telemetry`
			// when it is disabled in config. See IsTelemetryEnabled function in telemetry/telemetry.go
			go func() {
				dom.TelemetryReportLoop(ses[5])
				dom.TelemetryRotateSubWindowLoop(ses[5])
			}()
		}
	}

	planReplayerWorkerCnt := config.GetGlobalConfig().Performance.PlanReplayerDumpWorkerConcurrency
	planReplayerWorkersSctx := make([]sessionctx.Context, planReplayerWorkerCnt)
	pworkerSes, err := createSessions(store, int(planReplayerWorkerCnt))
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(planReplayerWorkerCnt); i++ {
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
		if err := doBootstrapSQLFile(ses[9]); err != nil && intest.InTest {
			failToLoadOrParseSQLFile = true
		}
	}
	// A sub context for update table stats, and other contexts for concurrent stats loading.
	cnt := 1 + concurrency
	syncStatsCtxs, err := createSessions(store, cnt)
	if err != nil {
		return nil, err
	}
	subCtxs := make([]sessionctx.Context, cnt)
	for i := 0; i < cnt; i++ {
		subCtxs[i] = sessionctx.Context(syncStatsCtxs[i])
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
	initStatsCtx, err := createSession(store)
	if err != nil {
		return nil, err
	}
	if err = dom.LoadAndUpdateStatsLoop(subCtxs, initStatsCtx); err != nil {
		return nil, err
	}

	// start TTL job manager after setup stats collector
	// because TTL could modify a lot of columns, and need to trigger auto analyze
	ttlworker.AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return attachStatsCollector(s, dom)
		}
		return s
	}
	ttlworker.DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return detachStatsCollector(s)
		}
		return s
	}
	dom.StartTTLJobManager()

	analyzeCtxs, err := createSessions(store, analyzeConcurrencyQuota)
	if err != nil {
		return nil, err
	}
	subCtxs2 := make([]sessionctx.Context, analyzeConcurrencyQuota)
	for i := 0; i < analyzeConcurrencyQuota; i++ {
		subCtxs2[i] = analyzeCtxs[i]
	}
	dom.SetupAnalyzeExec(subCtxs2)
	dom.LoadSigningCertLoop(cfg.Security.SessionTokenSigningCert, cfg.Security.SessionTokenSigningKey)

	if raw, ok := store.(kv.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, err
		}
	}

	// This only happens in testing, since the failure of loading or parsing sql file
	// would panic the bootstrapping.
	if intest.InTest && failToLoadOrParseSQLFile {
		dom.Close()
		return nil, errors.New("Fail to load or parse sql file")
	}
	err = dom.InitDistTaskLoop(ctx)
	if err != nil {
		return nil, err
	}
	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// runInBootstrapSession create a special session for bootstrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(types.Session)) {
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	// For the bootstrap SQLs, the following variables should be compatible with old TiDB versions.
	s.sessionVars.EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

	s.SetValue(sessionctx.Initing, true)
	bootstrap(s)
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom := domain.GetDomain(s)
	dom.Close()
	if intest.InTest {
		infosync.MockGlobalServerInfoManagerEntry.Close()
	}
	domap.Delete(store)
}

func createSessions(store kv.Storage, cnt int) ([]*session, error) {
	return createSessionsImpl(store, cnt, createSession)
}

func createSessions4DistExecution(store kv.Storage, cnt int) ([]*session, error) {
	domap.Delete(store)

	return createSessionsImpl(store, cnt, createSession4DistExecution)
}

func createSessionsImpl(store kv.Storage, cnt int, createSessionImpl func(kv.Storage) (*session, error)) ([]*session, error) {
	// Then we can create new dom
	ses := make([]*session, cnt)
	for i := 0; i < cnt; i++ {
		se, err := createSessionImpl(store)
		if err != nil {
			return nil, err
		}
		ses[i] = se
	}

	return ses, nil
}

// createSession creates a new session.
// Please note that such a session is not tracked by the internal session list.
// This means the min ts reporter is not aware of it and may report a wrong min start ts.
// In most cases you should use a session pool in domain instead.
func createSession(store kv.Storage) (*session, error) {
	return createSessionWithOpt(store, nil)
}

func createSession4DistExecution(store kv.Storage) (*session, error) {
	return createSessionWithOpt(store, nil)
}

func createSessionWithOpt(store kv.Storage, opt *Opt) (*session, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	s := &session{
		store:                 store,
		ddlOwnerManager:       dom.DDL().OwnerManager(),
		client:                store.GetClient(),
		mppClient:             store.GetMPPClient(),
		stmtStats:             stmtstats.CreateStatementStats(),
		sessionStatesHandlers: make(map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler),
	}
	s.sessionVars = variable.NewSessionVars(s)

	s.functionUsageMu.builtinFunctionUsage = make(telemetry.BuiltinFunctionsUsage)
	if opt != nil && opt.PreparedPlanCache != nil {
		s.sessionPlanCache = opt.PreparedPlanCache
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	s.advisoryLocks = make(map[string]*internal.advisoryLock)

	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpsClient()
	s.txn.init()

	sessionBindHandle := bindinfo.NewSessionBindHandle()
	s.SetValue(bindinfo.SessionBindInfoKeyType, sessionBindHandle)
	s.SetSessionStatesHandler(sessionstates.StateBinding, sessionBindHandle)
	return s, nil
}

// attachStatsCollector attaches the stats collector in the dom for the session
func attachStatsCollector(s *session, dom *domain.Domain) *session {
	if dom.StatsHandle() != nil && dom.StatsUpdating() {
		if s.statsCollector == nil {
			s.statsCollector = dom.StatsHandle().NewSessionStatsItem().(*usage.SessionStatsItem)
		}
		if s.idxUsageCollector == nil && GetIndexUsageSyncLease() > 0 {
			s.idxUsageCollector = dom.StatsHandle().NewSessionIndexUsageCollector().(*usage.SessionIndexUsageCollector)
		}
	}

	return s
}

// detachStatsCollector removes the stats collector in the session
func detachStatsCollector(s *session) *session {
	if s.statsCollector != nil {
		s.statsCollector.Delete()
		s.statsCollector = nil
	}
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Delete()
		s.idxUsageCollector = nil
	}
	return s
}

// CreateSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSession directly.
func CreateSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	s := &session{
		store:                 store,
		sessionVars:           variable.NewSessionVars(nil),
		client:                store.GetClient(),
		mppClient:             store.GetMPPClient(),
		stmtStats:             stmtstats.CreateStatementStats(),
		sessionStatesHandlers: make(map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler),
	}
	s.functionUsageMu.builtinFunctionUsage = make(telemetry.BuiltinFunctionsUsage)
	s.mu.values = make(map[fmt.Stringer]interface{})
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()
	return s, nil
}

const (
	notBootstrapped = 0
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return currentBootstrapVersion
	}

	var ver int64
	// check in kv store
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	modifyBootstrapVersionForTest(ver)
	return ver
}

func finishBootstrap(store kv.Storage) {
	setStoreBootstrapped(store.UUID())

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}

	vars.CommonGlobalLoaded = true

	// Deep copy sessionvar cache
	sessionCache, err := domain.GetDomain(s).GetSessionCache()
	if err != nil {
		return err
	}
	for varName, varVal := range sessionCache {
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = vars.SetSystemVarWithRelaxedValidation(varName, varVal)
			if err != nil {
				if variable.ErrUnknownSystemVar.Equal(err) {
					continue // sessionCache is stale; sysvar has likely been unregistered
				}
				return err
			}
		}
	}
	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(variable.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(variable.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareTxnCtx begins a transaction, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) error {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return nil
	}

	txnMode := ast.Optimistic
	if !s.sessionVars.IsAutocommit() || config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		if s.sessionVars.TxnMode == ast.Pessimistic {
			txnMode = ast.Pessimistic
		}
	}

	if s.sessionVars.RetryInfo.Retrying {
		txnMode = ast.Pessimistic
	}

	return sessiontxn.GetTxnManager(s).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: txnMode,
	})
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error {
	if s.txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(sessiontxn.ConstantFuture); !ok && !s.isInternal() {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = internal.txnFailFuture{}
	})

	s.txn.changeToPending(&internal.txnFuture{
		future:   future,
		store:    s.store,
		txnScope: scope,
	})
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
// It returns nil otherwise.
func (s *session) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !s.txn.validOrPending() {
		return nil
	}
	return &s.txn
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(nil, commitDetail)
	}
	if err != nil {
		return err
	}

	s.updateStatsDeltaToCollector()

	return sessiontxn.NewTxn(ctx, s)
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) ShowProcess() *util.ProcessInfo {
	var pi *util.ProcessInfo
	tmp := s.processInfo.Load()
	if tmp != nil {
		pi = tmp.(*util.ProcessInfo)
	}
	return pi
}

// GetStartTSFromSession returns the startTS in the session `se`
func GetStartTSFromSession(se interface{}) (startTS, processInfoID uint64) {
	tmp, ok := se.(*session)
	if !ok {
		logutil.BgLogger().Error("GetStartTSFromSession failed, can't transform to session struct")
		return 0, 0
	}
	txnInfo := tmp.TxnInfo()
	if txnInfo != nil {
		startTS = txnInfo.StartTS
		processInfoID = txnInfo.ConnectionID
	}

	logutil.BgLogger().Debug(
		"GetStartTSFromSession getting startTS of internal session",
		zap.Uint64("startTS", startTS), zap.Time("start time", oracle.GetTimeFromTS(startTS)))

	return startTS, processInfoID
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(execStmt *executor.ExecStmt, s *session) {
	vars := s.GetSessionVars()
	isCrucial := false
	switch stmt := execStmt.StmtNode.(type) {
	case *ast.DropIndexStmt:
		isCrucial = true
		if stmt.IsHypo {
			isCrucial = false
		}
	case *ast.CreateIndexStmt:
		isCrucial = true
		if stmt.IndexOption != nil && stmt.IndexOption.Tp == model.IndexTypeHypo {
			isCrucial = false
		}
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt,
		*ast.RenameUserStmt:
		isCrucial = true
	}

	if isCrucial {
		user := vars.User
		schemaVersion := s.GetInfoSchema().SchemaMetaVersion()
		if ss, ok := execStmt.StmtNode.(ast.SensitiveStmtNode); ok {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", execStmt.StmtNode.Text()),
				zap.Stringer("user", user))
		}
	} else {
		logGeneralQuery(execStmt, s, false)
	}
}

func logGeneralQuery(execStmt *executor.ExecStmt, s *session, isPrepared bool) {
	vars := s.GetSessionVars()
	if variable.ProcessGeneralLog.Load() && !vars.InRestrictedSQL {
		var query string
		if isPrepared {
			query = execStmt.OriginText()
		} else {
			query = execStmt.GetTextToLog(false)
		}

		query = executor.QueryReplacer.Replace(query)
		if !vars.EnableRedactLog {
			query += vars.PlanCacheParams.String()
		}
		logutil.BgLogger().Info("GENERAL_LOG",
			zap.Uint64("conn", vars.ConnectionID),
			zap.String("session_alias", vars.SessionAlias),
			zap.String("user", vars.User.LoginString()),
			zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.Uint64("forUpdateTS", vars.TxnCtx.GetForUpdateTS()),
			zap.Bool("isReadConsistency", vars.IsIsolation(ast.ReadCommitted)),
			zap.String("currentDB", vars.CurrentDB),
			zap.Bool("isPessimistic", vars.TxnCtx.IsPessimistic),
			zap.String("sessionTxnMode", vars.GetReadableTxnMode()),
			zap.String("sql", query))
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64, isInternal bool) {
	if s.sessionVars.TxnCtx.IsPessimistic {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationPessimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationPessimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKGeneral.Observe(float64(counter))
			}
		}
	} else {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationOptimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationOptimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKGeneral.Observe(float64(counter))
			}
		}
	}
}

func (s *session) checkPlacementPolicyBeforeCommit() error {
	var err error
	// Get the txnScope of the transaction we're going to commit.
	txnScope := s.GetSessionVars().TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = kv.GlobalTxnScope
	}
	if txnScope != kv.GlobalTxnScope {
		is := s.GetInfoSchema().(infoschema.InfoSchema)
		deltaMap := s.GetSessionVars().TxnCtx.TableDeltaMap
		for physicalTableID := range deltaMap {
			var tableName string
			var partitionName string
			tblInfo, _, partInfo := is.FindTableByPartitionID(physicalTableID)
			if tblInfo != nil && partInfo != nil {
				tableName = tblInfo.Meta().Name.String()
				partitionName = partInfo.Name.String()
			} else {
				tblInfo, _ := is.TableByID(physicalTableID)
				tableName = tblInfo.Meta().Name.String()
			}
			bundle, ok := is.PlacementBundleByPhysicalTableID(physicalTableID)
			if !ok {
				errMsg := fmt.Sprintf("table %v doesn't have placement policies with txn_scope %v",
					tableName, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v doesn't have placement policies with txn_scope %v",
						tableName, partitionName, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			dcLocation, ok := bundle.GetLeaderDC(placement.DCLabelKey)
			if !ok {
				errMsg := fmt.Sprintf("table %v's leader placement policy is not defined", tableName)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader placement policy is not defined", tableName, partitionName)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			if dcLocation != txnScope {
				errMsg := fmt.Sprintf("table %v's leader location %v is out of txn_scope %v", tableName, dcLocation, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader location %v is out of txn_scope %v",
						tableName, partitionName, dcLocation, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			// FIXME: currently we assume the physicalTableID is the partition ID. In future, we should consider the situation
			// if the physicalTableID belongs to a Table.
			partitionID := physicalTableID
			tbl, _, partitionDefInfo := is.FindTableByPartitionID(partitionID)
			if tbl != nil {
				tblInfo := tbl.Meta()
				state := tblInfo.Partition.GetStateByID(partitionID)
				if state == model.StateGlobalTxnOnly {
					err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
						fmt.Sprintf("partition %s of table %s can not be written by local transactions when its placement policy is being altered",
							tblInfo.Name, partitionDefInfo.Name))
					break
				}
			}
		}
	}
	return err
}

func (s *session) SetPort(port string) {
	s.sessionVars.Port = port
}

// GetTxnWriteThroughputSLI implements the Context interface.
func (s *session) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &s.txn.writeSLI
}

// GetInfoSchema returns snapshotInfoSchema if snapshot schema is set.
// Transaction infoschema is returned if inside an explicit txn.
// Otherwise the latest infoschema is returned.
func (s *session) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	vars := s.GetSessionVars()
	var is infoschema.InfoSchema
	if snap, ok := vars.SnapshotInfoschema.(infoschema.InfoSchema); ok {
		logutil.BgLogger().Info("use snapshot schema", zap.Uint64("conn", vars.ConnectionID), zap.Int64("schemaVersion", snap.SchemaMetaVersion()))
		is = snap
	} else {
		vars.TxnCtxMu.Lock()
		if vars.TxnCtx != nil {
			if tmp, ok := vars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok {
				is = tmp
			}
		}
		vars.TxnCtxMu.Unlock()
	}

	if is == nil {
		is = domain.GetDomain(s).InfoSchema()
	}

	// Override the infoschema if the session has temporary table.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is)
}

func (s *session) GetDomainInfoSchema() sessionctx.InfoschemaMetaVersion {
	is := domain.GetDomain(s).InfoSchema()
	extIs := &infoschema.SessionExtendedInfoSchema{InfoSchema: is}
	return temptable.AttachLocalTemporaryTableInfoSchema(s, extIs)
}

func getSnapshotInfoSchema(s sessionctx.Context, snapshotTS uint64) (infoschema.InfoSchema, error) {
	is, err := domain.GetDomain(s).GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	// Set snapshot does not affect the witness of the local temporary table.
	// The session always see the latest temporary tables.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is), nil
}

func (s *session) updateTelemetryMetric(es *executor.ExecStmt) {
	if es.Ti == nil {
		return
	}
	if s.isInternal() {
		return
	}

	ti := es.Ti
	if ti.UseRecursive {
		session_metrics.TelemetryCTEUsageRecurCTE.Inc()
	} else if ti.UseNonRecursive {
		session_metrics.TelemetryCTEUsageNonRecurCTE.Inc()
	} else {
		session_metrics.TelemetryCTEUsageNotCTE.Inc()
	}

	if ti.UseIndexMerge {
		session_metrics.TelemetryIndexMerge.Inc()
	}

	if ti.UseMultiSchemaChange {
		session_metrics.TelemetryMultiSchemaChangeUsage.Inc()
	}

	if ti.UseFlashbackToCluster {
		session_metrics.TelemetryFlashbackClusterUsage.Inc()
	}

	if ti.UseExchangePartition {
		session_metrics.TelemetryExchangePartitionUsage.Inc()
	}

	if ti.PartitionTelemetry != nil {
		if ti.PartitionTelemetry.UseTablePartition {
			session_metrics.TelemetryTablePartitionUsage.Inc()
			session_metrics.TelemetryTablePartitionMaxPartitionsUsage.Add(float64(ti.PartitionTelemetry.TablePartitionMaxPartitionsNum))
		}
		if ti.PartitionTelemetry.UseTablePartitionList {
			session_metrics.TelemetryTablePartitionListUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRange {
			session_metrics.TelemetryTablePartitionRangeUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionHash {
			session_metrics.TelemetryTablePartitionHashUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumns {
			session_metrics.TelemetryTablePartitionRangeColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt1 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt1Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt2 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt2Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt3 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt3Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionListColumns {
			session_metrics.TelemetryTablePartitionListColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCreateIntervalPartition {
			session_metrics.TelemetryTablePartitionCreateIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseAddIntervalPartition {
			session_metrics.TelemetryTablePartitionAddIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseDropIntervalPartition {
			session_metrics.TelemetryTablePartitionDropIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCompactTablePartition {
			session_metrics.TelemetryTableCompactPartitionUsage.Inc()
		}
		if ti.PartitionTelemetry.UseReorganizePartition {
			session_metrics.TelemetryReorganizePartitionUsage.Inc()
		}
	}

	if ti.AccountLockTelemetry != nil {
		session_metrics.TelemetryLockUserUsage.Add(float64(ti.AccountLockTelemetry.LockUser))
		session_metrics.TelemetryUnlockUserUsage.Add(float64(ti.AccountLockTelemetry.UnlockUser))
		session_metrics.TelemetryCreateOrAlterUserUsage.Add(float64(ti.AccountLockTelemetry.CreateOrAlterUser))
	}

	if ti.UseTableLookUp.Load() && s.sessionVars.StoreBatchSize > 0 {
		session_metrics.TelemetryStoreBatchedUsage.Inc()
	}
}

// GetBuiltinFunctionUsage returns the replica of counting of builtin function usage
func (s *session) GetBuiltinFunctionUsage() map[string]uint32 {
	replica := make(map[string]uint32)
	s.functionUsageMu.RLock()
	defer s.functionUsageMu.RUnlock()
	for key, value := range s.functionUsageMu.builtinFunctionUsage {
		replica[key] = value
	}
	return replica
}

// BuiltinFunctionUsageInc increase the counting of the builtin function usage
func (s *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {
	s.functionUsageMu.Lock()
	defer s.functionUsageMu.Unlock()
	s.functionUsageMu.builtinFunctionUsage.Inc(scalarFuncSigName)
}

func (s *session) GetStmtStats() *stmtstats.StatementStats {
	return s.stmtStats
}

// SetMemoryFootprintChangeHook sets the hook that is called when the memdb changes its size.
// Call this after s.txn becomes valid, since TxnInfo is initialized when the txn becomes valid.
func (s *session) SetMemoryFootprintChangeHook() {
	if config.GetGlobalConfig().Performance.TxnTotalSizeLimit != config.DefTxnTotalSizeLimit {
		// if the user manually specifies the config, don't involve the new memory tracker mechanism, let the old config
		// work as before.
		return
	}
	hook := func(mem uint64) {
		if s.sessionVars.MemDBFootprint == nil {
			tracker := memory.NewTracker(memory.LabelForMemDB, -1)
			tracker.AttachTo(s.sessionVars.MemTracker)
			s.sessionVars.MemDBFootprint = tracker
		}
		s.sessionVars.MemDBFootprint.ReplaceBytesUsed(int64(mem))
	}
	s.txn.SetMemoryFootprintChangeHook(hook)
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (s *session) EncodeSessionStates(ctx context.Context,
	_ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	// Transaction status is hard to encode, so we do not support it.
	s.txn.mu.Lock()
	valid := s.txn.Valid()
	s.txn.mu.Unlock()
	if valid {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has an active transaction")
	}
	// Data in local temporary tables is hard to encode, so we do not support it.
	// Check temporary tables here to avoid circle dependency.
	if s.sessionVars.LocalTemporaryTables != nil {
		localTempTables := s.sessionVars.LocalTemporaryTables.(*infoschema.SessionTables)
		if localTempTables.Count() > 0 {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has local temporary tables")
		}
	}
	// The advisory locks will be released when the session is closed.
	if len(s.advisoryLocks) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has advisory locks")
	}
	// The TableInfo stores session ID and server ID, so the session cannot be migrated.
	if len(s.lockedTables) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has locked tables")
	}
	// It's insecure to migrate sandBoxMode because users can fake it.
	if s.InSandBoxMode() {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session is in sandbox mode")
	}

	if err := s.sessionVars.EncodeSessionStates(ctx, sessionStates); err != nil {
		return err
	}

	hasRestrictVarPriv := false
	checker := privilege.GetPrivilegeManager(s)
	if checker == nil || checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		hasRestrictVarPriv = true
	}
	// Encode session variables. We put it here instead of SessionVars to avoid cycle import.
	sessionStates.SystemVars = make(map[string]string)
	for _, sv := range variable.GetSysVars() {
		switch {
		case sv.HasNoneScope(), !sv.HasSessionScope():
			// Hidden attribute is deprecated.
			// None-scoped variables cannot be modified.
			// Noop variables should also be migrated even if they are noop.
			continue
		case sv.ReadOnly:
			// Skip read-only variables here. We encode them into SessionStates manually.
			continue
		}
		// Get all session variables because the default values may change between versions.
		val, keep, err := s.sessionVars.GetSessionStatesSystemVar(sv.Name)
		switch {
		case err != nil:
			return err
		case !keep:
			continue
		case !hasRestrictVarPriv && sem.IsEnabled() && sem.IsInvisibleSysVar(sv.Name):
			// If the variable has a global scope, it should be the same with the global one.
			// Otherwise, it should be the same with the default value.
			defaultVal := sv.Value
			if sv.HasGlobalScope() {
				// If the session value is the same with the global one, skip it.
				if defaultVal, err = sv.GetGlobalFromHook(ctx, s.sessionVars); err != nil {
					return err
				}
			}
			if val != defaultVal {
				// Case 1: the RESTRICTED_VARIABLES_ADMIN is revoked after setting the session variable.
				// Case 2: the global variable is updated after the session is created.
				// In any case, the variable can't be set in the new session, so give up.
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs(fmt.Sprintf("session has set invisible variable '%s'", sv.Name))
			}
		default:
			sessionStates.SystemVars[sv.Name] = val
		}
	}

	// Encode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.EncodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}
	return nil
}

// DecodeSessionStates implements SessionStatesHandler.DecodeSessionStates interface.
func (s *session) DecodeSessionStates(ctx context.Context,
	_ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	// Decode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.DecodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}

	// Decode session variables.
	names := variable.OrderByDependency(sessionStates.SystemVars)
	// Some variables must be set before others, e.g. tidb_enable_noop_functions should be before noop variables.
	for _, name := range names {
		val := sessionStates.SystemVars[name]
		// Experimental system variables may change scope, data types, or even be removed.
		// We just ignore the errors and continue.
		if err := s.sessionVars.SetSystemVar(name, val); err != nil {
			logutil.Logger(ctx).Warn("set session variable during decoding session states error",
				zap.String("name", name), zap.String("value", val), zap.Error(err))
		}
	}

	// Decoding session vars / prepared statements may override stmt ctx, such as warnings,
	// so we decode stmt ctx at last.
	return s.sessionVars.DecodeSessionStates(ctx, sessionStates)
}

func (s *session) setRequestSource(ctx context.Context, stmtLabel string, stmtNode ast.StmtNode) {
	if !s.isInternal() {
		if txn, _ := s.Txn(false); txn != nil && txn.Valid() {
			txn.SetOption(kv.RequestSourceType, stmtLabel)
		}
		s.sessionVars.RequestSourceType = stmtLabel
		return
	}
	if source := ctx.Value(kv.RequestSourceKey); source != nil {
		requestSource := source.(kv.RequestSource)
		if requestSource.RequestSourceType != "" {
			s.sessionVars.RequestSourceType = requestSource.RequestSourceType
			return
		}
	}
	// panic in test mode in case there are requests without source in the future.
	// log warnings in production mode.
	if intest.InTest {
		panic("unexpected no source type context, if you see this error, " +
			"the `RequestSourceTypeKey` is missing in your context")
	}
	logutil.Logger(ctx).Warn("unexpected no source type context, if you see this warning, "+
		"the `RequestSourceTypeKey` is missing in the context",
		zap.Bool("internal", s.isInternal()),
		zap.String("sql", stmtNode.Text()))
}

// RemoveLockDDLJobs removes the DDL jobs which doesn't get the metadata lock from job2ver.
func RemoveLockDDLJobs(s types.Session, job2ver map[int64]int64, job2ids map[int64]string, printLog bool) {
	sv := s.GetSessionVars()
	if sv.InRestrictedSQL {
		return
	}
	sv.TxnCtxMu.Lock()
	defer sv.TxnCtxMu.Unlock()
	if sv.TxnCtx == nil {
		return
	}
	sv.GetRelatedTableForMDL().Range(func(tblID, value any) bool {
		for jobID, ver := range job2ver {
			ids := util.Str2Int64Map(job2ids[jobID])
			if _, ok := ids[tblID.(int64)]; ok && value.(int64) < ver {
				delete(job2ver, jobID)
				elapsedTime := time.Since(oracle.GetTimeFromTS(sv.TxnCtx.StartTS))
				if elapsedTime > time.Minute && printLog {
					logutil.BgLogger().Info("old running transaction block DDL", zap.Int64("table ID", tblID.(int64)), zap.Int64("jobID", jobID), zap.Uint64("connection ID", sv.ConnectionID), zap.Duration("elapsed time", elapsedTime))
				} else {
					logutil.BgLogger().Debug("old running transaction block DDL", zap.Int64("table ID", tblID.(int64)), zap.Int64("jobID", jobID), zap.Uint64("connection ID", sv.ConnectionID), zap.Duration("elapsed time", elapsedTime))
				}
			}
		}
		return true
	})
}

// GetDBNames gets the sql layer database names from the session.
func GetDBNames(seVar *variable.SessionVars) []string {
	dbNames := make(map[string]struct{})
	if seVar == nil || !config.GetGlobalConfig().Status.RecordDBLabel {
		return []string{""}
	}
	if seVar.StmtCtx != nil {
		for _, t := range seVar.StmtCtx.Tables {
			dbNames[t.DB] = struct{}{}
		}
	}
	if len(dbNames) == 0 {
		dbNames[seVar.CurrentDB] = struct{}{}
	}
	ns := make([]string, 0, len(dbNames))
	for n := range dbNames {
		ns = append(ns, n)
	}
	return ns
}
