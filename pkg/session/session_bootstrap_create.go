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

package session

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"go.uber.org/zap"
)

func createSessions(store kv.Storage, cnt int) ([]*session, error) {
	return createSessionsImpl(store, cnt)
}

func createSessions4DistExecution(store kv.Storage, cnt int) ([]*session, error) {
	domap.Delete(store)

	return createSessionsImpl(store, cnt)
}

func createSessionsImpl(store kv.Storage, cnt int) ([]*session, error) {
	// Then we can create new dom
	ses := make([]*session, cnt)
	for i := range cnt {
		se, err := createSession(store)
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
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	return createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
}

func createCrossKSSession(currKSStore kv.Storage, targetKS string, validator validatorapi.Validator) (*session, error) {
	if currKSStore.GetKeyspace() == targetKS {
		return nil, errors.New("cannot create session for the same keyspace")
	}
	dom, err := domap.Get(currKSStore)
	if err != nil {
		return nil, err
	}

	store, err := dom.GetKSStore(targetKS)
	if err != nil {
		return nil, err
	}
	infoCache, err := dom.GetKSInfoCache(targetKS)
	if err != nil {
		return nil, err
	}
	// TODO: use the schema validator of the target keyspace when we implement
	// the info schema syncer for cross keyspace access.
	return createSessionWithOpt(store, nil, validator, infoCache, nil)
}

func createSessionWithOpt(
	store kv.Storage,
	dom *domain.Domain,
	schemaValidator validatorapi.Validator,
	infoCache *infoschema.InfoCache,
	opt *Opt,
) (*session, error) {
	var ddlOwnerMgr owner.Manager
	if dom != nil {
		// we don't set dom for cross keyspace access.
		ddlOwnerMgr = dom.DDL().OwnerManager()
	}
	crossKS := dom == nil
	s := &session{
		dom:                   dom,
		crossKS:               crossKS,
		schemaValidator:       schemaValidator,
		infoCache:             infoCache,
		store:                 store,
		ddlOwnerManager:       ddlOwnerMgr,
		client:                store.GetClient(),
		mppClient:             store.GetMPPClient(),
		stmtStats:             stmtstats.CreateStatementStats(),
		sessionStatesHandlers: make(map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler),
	}
	s.sessionVars = variable.NewSessionVars(s)
	s.exprctx = sessionexpr.NewExprContext(s)
	s.pctx = newPlanContextImpl(s)
	s.tblctx = tblsession.NewMutateContext(s)

	s.functionUsageMu.builtinFunctionUsage = make(telemetry.BuiltinFunctionsUsage)
	if opt != nil && opt.PreparedPlanCache != nil {
		s.sessionPlanCache = opt.PreparedPlanCache
	}
	s.mu.values = make(map[fmt.Stringer]any)
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	s.advisoryLocks = make(map[string]*advisoryLock)

	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()

	sessionBindHandle := bindinfo.NewSessionBindingHandle()
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
		if s.idxUsageCollector == nil && config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load() {
			s.idxUsageCollector = dom.StatsHandle().NewSessionIndexUsageCollector()
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
		s.idxUsageCollector.Flush()
		s.idxUsageCollector = nil
	}
	return s
}

// CreateSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSession directly.
func CreateSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	return createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
}
// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (sessionapi.Session, error) {
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
	PreparedPlanCache sessionctx.SessionPlanCache
}

// CreateSession4TestWithOpt creates a new session environment for test.
func CreateSession4TestWithOpt(store kv.Storage, opt *Opt) (sessionapi.Session, error) {
	s, err := CreateSessionWithOpt(store, opt)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
		s.GetSessionVars().MinPagingSize = vardef.DefMinPagingSize
		s.GetSessionVars().EnablePaging = vardef.DefTiDBEnablePaging
		s.GetSessionVars().StmtCtx.SetTimeZone(s.GetSessionVars().Location())
		err = s.GetSessionVars().SetSystemVarWithoutValidation(vardef.CharacterSetConnection, "utf8mb4")
	}
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (sessionapi.Session, error) {
	return CreateSessionWithOpt(store, nil)
}

// CreateSessionWithOpt creates a new session environment with option.
// Use default option if opt is nil.
func CreateSessionWithOpt(store kv.Storage, opt *Opt) (sessionapi.Session, error) {
	do, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	s, err := createSessionWithOpt(store, do, do.GetSchemaValidator(), do.InfoCache(), opt)
	if err != nil {
		return nil, err
	}

	// Add auth here.
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
		if config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load() {
			s.idxUsageCollector = do.StatsHandle().NewSessionIndexUsageCollector()
		}
	}

	s.cursorTracker = cursor.NewTracker()

	return s, nil
}

// loadCollationParameter loads collation parameter from mysql.tidb
func loadCollationParameter(ctx context.Context, se *session) (bool, error) {
	para, err := se.getTableValue(ctx, mysql.TiDBTable, TidbNewCollationEnabled)
	if err != nil {
		return false, err
	}
	switch para {
	case varTrue:
		return true, nil
	case varFalse:
		return false, nil
	}
	logutil.BgLogger().Warn(
		"Unexpected value of 'new_collation_enabled' in 'mysql.tidb', use 'False' instead",
		zap.String("value", para))
	return false, nil
}

// DatabaseBasicInfo contains the basic information of a database.
type DatabaseBasicInfo struct {
	ID     int64
	Name   string
	Tables []TableBasicInfo
}

// TableBasicInfo contains the basic information of a table used in DDL.
type TableBasicInfo struct {
	ID   int64
	Name string
	SQL  string
}

type versionedDDLTables struct {
	ver    meta.DDLTableVersion
	tables []TableBasicInfo
}

var (
	errResultIsEmpty = dbterror.ClassExecutor.NewStd(errno.ErrResultIsEmpty)
	// DDLJobTables is a list of tables definitions used in concurrent DDL.
	DDLJobTables = []TableBasicInfo{
		{ID: metadef.TiDBDDLJobTableID, Name: "tidb_ddl_job", SQL: metadef.CreateTiDBDDLJobTable},
		{ID: metadef.TiDBDDLReorgTableID, Name: "tidb_ddl_reorg", SQL: metadef.CreateTiDBReorgTable},
		{ID: metadef.TiDBDDLHistoryTableID, Name: "tidb_ddl_history", SQL: metadef.CreateTiDBDDLHistoryTable},
	}
	// MDLTables is a list of tables definitions used for metadata lock.
	MDLTables = []TableBasicInfo{
		{ID: metadef.TiDBMDLInfoTableID, Name: "tidb_mdl_info", SQL: metadef.CreateTiDBMDLTable},
	}
	// BackfillTables is a list of tables definitions used in dist reorg DDL.
	BackfillTables = []TableBasicInfo{
		{ID: metadef.TiDBBackgroundSubtaskTableID, Name: "tidb_background_subtask", SQL: metadef.CreateTiDBBackgroundSubtaskTable},
		{ID: metadef.TiDBBackgroundSubtaskHistoryTableID, Name: "tidb_background_subtask_history", SQL: metadef.CreateTiDBBackgroundSubtaskHistoryTable},
	}
	// DDLNotifierTables contains the table definitions used in DDL notifier.
	// It only contains the notifier table.
	// Put it here to reuse a unified initialization function and make it easier to find.
	DDLNotifierTables = []TableBasicInfo{
		{ID: metadef.TiDBDDLNotifierTableID, Name: "tidb_ddl_notifier", SQL: metadef.CreateTiDBDDLNotifierTable},
	}

	ddlTableVersionTables = []versionedDDLTables{
		{ver: meta.BaseDDLTableVersion, tables: DDLJobTables},
		{ver: meta.MDLTableVersion, tables: MDLTables},
		{ver: meta.BackfillTableVersion, tables: BackfillTables},
		{ver: meta.DDLNotifierTableVersion, tables: DDLNotifierTables},
	}
)


const (
	notBootstrapped = 0
)

func mustGetStoreBootstrapVersion(store kv.Storage) int64 {
	var ver int64
	// check in kv store
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		var err error
		t := meta.NewReader(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("get store bootstrap version failed", zap.Error(err))
	}
	return ver
}

func getStoreBootstrapVersionWithCache(store kv.Storage) int64 {
	// check in memory
	_, ok := store.GetOption(StoreBootstrappedKey)
	if ok {
		return currentBootstrapVersion
	}

	ver := mustGetStoreBootstrapVersion(store)

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		store.SetOption(StoreBootstrappedKey, true)
	}

	modifyBootstrapVersionForTest(ver)
	return ver
}

func finishBootstrap(store kv.Storage) {
	store.SetOption(StoreBootstrappedKey, true)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}
