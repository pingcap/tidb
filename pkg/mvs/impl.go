// Copyright 2026 PingCAP, Inc.
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

package mvs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
)

type serverHelper struct {
	durationObserverCache durationObserverCache
	runEventCounterCache  runEventCounterCache

	reportCache struct {
		submittedCount int64
		completedCount int64
		failedCount    int64
		timeoutCount   int64
		rejectedCount  int64
	}
}

type mvMetricTypeResultKey struct {
	typ    string
	result string
}

type durationObserverCache struct {
	mu   sync.RWMutex
	data map[mvMetricTypeResultKey]prometheus.Observer
}

func newDurationObserverCache(capacity int) durationObserverCache {
	if capacity < 0 {
		capacity = 0
	}
	return durationObserverCache{
		data: make(map[mvMetricTypeResultKey]prometheus.Observer, capacity),
	}
}

type runEventCounterCache struct {
	mu   sync.RWMutex
	data map[string]prometheus.Counter
}

func newRunEventCounterCache(capacity int) runEventCounterCache {
	if capacity < 0 {
		capacity = 0
	}
	return runEventCounterCache{
		data: make(map[string]prometheus.Counter, capacity),
	}
}

// newServerHelper builds a default helper used by MVService.
func newServerHelper() *serverHelper {
	return &serverHelper{
		durationObserverCache: newDurationObserverCache(8),
		runEventCounterCache:  newRunEventCounterCache(16),
	}
}

func (_ *serverHelper) serverFilter(s serverInfo) bool {
	return true
}

func (_ *serverHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID: localSrv.ID,
	}, nil
}

func (_ *serverHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
	servers := make(map[string]serverInfo)
	allServers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	for _, srv := range allServers {
		servers[srv.ID] = serverInfo{
			ID: srv.ID,
		}
	}
	return servers, nil
}

// RefreshMV executes one incremental refresh round for a materialized view.
//
// It:
// 1. Gets a system session from the pool and temporarily clears SQL mode.
// 2. Resolves schema/table names from MVIEW_ID.
// 3. Executes `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE FAST`.
// 4. Reads NEXT_TIME from mysql.tidb_mview_refresh_info.
//
// The returned error only represents execution failures. A zero nextRefresh means
// no further scheduling is needed (for example, the MV metadata was removed).
func (*serverHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findNextTimeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	)

	se, err := sysSessionPool.Get()
	if err != nil {
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	sctx := se.(sessionctx.Context)
	originalSQLMode := sctx.GetSessionVars().SQLMode
	sctx.GetSessionVars().SQLMode = 0
	defer func() {
		sctx.GetSessionVars().SQLMode = originalSQLMode
	}()
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvTable, ok := infoSchema.TableByID(ctx, mvID)
	if !ok {
		return time.Time{}, nil
	}
	mvMeta := mvTable.Meta()
	if mvMeta == nil {
		return time.Time{}, errors.New("mview metadata is invalid")
	}
	mviewName := mvMeta.Name.L
	dbInfo, ok := infoSchema.SchemaByID(mvMeta.DBID)
	if !ok || dbInfo == nil {
		return time.Time{}, errors.New("mview metadata is invalid")
	}
	schemaName := dbInfo.Name.L
	if schemaName == "" || mviewName == "" {
		return time.Time{}, errors.New("mview metadata is invalid")
	}

	if _, err = execRCRestrictedSQL(ctx, sctx, refreshMVSQL, []any{schemaName, mviewName}); err != nil {
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQL(ctx, sctx, findNextTimeSQL, []any{mvID})
	if err != nil {
		return time.Time{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, nil
	}
	nextRefresh = mvsUnix(rows[0].GetInt64(0), 0)
	return nextRefresh, nil
}

// PurgeMVLog executes one purge round for an MV log.
//
// Behavior overview:
// 1. Resolve MV log and base table metadata from mvLogID, then parse scheduling fields.
// 2. Query MIN(LAST_SUCCESS_READ_TSO) across dependent MVs as purge upper bound.
// 3. In one transaction, lock purge state row and decide whether this round should run.
// 4. Delete eligible rows, record history, update purge state, and calculate NEXT_TIME.
//
// When autoPurge is true, missing/NULL NEXT_TIME is treated as "no future auto purge".
// A zero nextPurge means no further scheduling is needed.
func PurgeMVLog(ctx context.Context, sctx sessionctx.Context, mvLogID int64, autoPurge bool) (nextPurge time.Time, err error) {
	const (
		purgeMethodManually      = "MANUALLY"
		purgeMethodAutomatically = "AUTOMATICALLY"
		lockPurgeSQL             = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
		deleteMLogSQL            = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		updatePurgeSQL           = `UPDATE mysql.tidb_mlog_purge_info SET PURGE_TIME = %?, PURGE_ENDTIME = %?, PURGE_ROWS = %?, PURGE_STATUS = 'SUCCESS', PURGE_JOB_ID = '', NEXT_TIME = %? WHERE MLOG_ID = %?`
	)
	originalSQLMode := sctx.GetSessionVars().SQLMode
	sctx.GetSessionVars().SQLMode = 0
	defer func() {
		sctx.GetSessionVars().SQLMode = originalSQLMode
	}()
	if mvLogID <= 0 {
		return time.Time{}, errors.New("materialized view log id is invalid")
	}
	purgeMethod := purgeMethodManually
	if autoPurge {
		purgeMethod = purgeMethodAutomatically
	}
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvLogTable, ok := infoSchema.TableByID(ctx, mvLogID)
	if !ok {
		return time.Time{}, nil
	}
	mvLogMeta := mvLogTable.Meta()
	if mvLogMeta == nil {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}
	mvLogName := mvLogMeta.Name.L
	if mvLogName == "" {
		return time.Time{}, errors.New("materialized view log table name is empty")
	}
	mvLogSchemaName := ""
	if dbInfo, ok := infoSchema.SchemaByID(mvLogMeta.DBID); ok && dbInfo != nil && dbInfo.Name.L != "" {
		mvLogSchemaName = dbInfo.Name.L
	}
	if mvLogSchemaName == "" {
		return time.Time{}, errors.New("materialized view log schema name is empty")
	}
	mvLogInfo := mvLogMeta.MaterializedViewLog
	if mvLogInfo == nil {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}
	if mvLogInfo.BaseTableID <= 0 {
		return time.Time{}, errors.New("materialized view base table id is invalid")
	}
	baseTable, ok := infoSchema.TableByID(ctx, mvLogInfo.BaseTableID)
	if !ok {
		return time.Time{}, errors.New("materialized view base table not found")
	}
	baseTableMeta := baseTable.Meta()
	if baseTableMeta == nil {
		return time.Time{}, errors.New("materialized view base table metadata is invalid")
	}
	mvBaseInfo := baseTableMeta.MaterializedViewBase
	if mvBaseInfo == nil {
		return time.Time{}, errors.New("materialized view base table metadata is invalid")
	}
	if mvBaseInfo.MLogID != mvLogID {
		return time.Time{}, errors.New("materialized view log metadata is inconsistent")
	}
	loc := time.Local
	if vars := sctx.GetSessionVars(); vars != nil && vars.Location() != nil {
		loc = vars.Location()
	}
	purgeStartWithText := strings.TrimSpace(mvLogInfo.PurgeStartWith)
	if purgeStartWithText == "" {
		return time.Time{}, errors.New("materialized view log purge start with is empty")
	}
	purgeStartWithText = strings.Trim(purgeStartWithText, "'")
	purgeStartWith, err := time.ParseInLocation("2006-01-02 15:04:05", purgeStartWithText, loc)
	if err != nil {
		return time.Time{}, err
	}
	purgeNextText := strings.TrimSpace(mvLogInfo.PurgeNext)
	if purgeNextText == "" {
		return time.Time{}, errors.New("materialized view log purge next is empty")
	}
	purgeNextSec, err := strconv.ParseInt(purgeNextText, 10, 64)
	if err != nil || purgeNextSec <= 0 {
		return time.Time{}, errors.New("materialized view log purge next is invalid")
	}
	minRefreshReadTSO, err := getMinRefreshReadTSOFromInfo(ctx, sctx, mvBaseInfo.MViewIDs)
	if err != nil {
		return time.Time{}, err
	}

	err = withRCRestrictedTxn(ctx, sctx, func(txnCtx context.Context, sctx sessionctx.Context) error {
		rows, err := execRCRestrictedSQLWithSession(txnCtx, sctx, lockPurgeSQL, []any{mvLogID})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			if autoPurge {
				nextPurge = time.Time{}
				return nil
			}
			return errors.New("mvlog purge state not found")
		}
		if rows[0].IsNull(0) {
			if autoPurge {
				nextPurge = time.Time{}
				return nil
			}
		} else {
			nextPurge = mvsUnix(rows[0].GetInt64(0), 0)
		}
		if autoPurge {
			if nextPurge.After(mvsNow()) {
				return nil
			}
		}

		purgeTime := mvsNow()
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{mvLogSchemaName, mvLogName, minRefreshReadTSO}); err != nil {
			return err
		}
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		purgeEndTime := mvsNow()
		nextPurge = calcNextExecTime(purgeStartWith, purgeNextSec, purgeEndTime)

		if err = recordMLogPurgeHist(txnCtx, sctx, mvLogID, mvLogName, purgeMethod, purgeTime, purgeEndTime, affectedRows, "SUCCESS"); err != nil {
			return err
		}
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeTime, purgeEndTime, affectedRows, nextPurge, mvLogID}); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return nextPurge, nil
}

// PurgeMVLog runs one auto-purge round for the specified mvLogID.
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return PurgeMVLog(ctx, sctx, mvLogID, true)
}

// calcNextExecTime returns the first schedule time after last based on start+N*interval.
func calcNextExecTime(start time.Time, intervalSec int64, last time.Time) time.Time {
	if intervalSec <= 0 {
		return last
	}
	startSec := start.Unix()
	lastSec := last.Unix()
	if lastSec < startSec {
		return mvsUnix(startSec, 0)
	}
	elapsed := lastSec - startSec
	intervals := elapsed / intervalSec
	nextSec := startSec + (intervals+1)*intervalSec
	return mvsUnix(nextSec, 0)
}

// fetchAllTiDBMLogPurge loads all scheduled MV log purge tasks from metadata.
func (*serverHelper) fetchAllTiDBMLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error) {
	const sql = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[int64]*mvLog, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mvLogID := row.GetInt64(1)
		if mvLogID <= 0 {
			continue
		}
		nextPurge := mvsUnix(row.GetInt64(0), 0)
		l := &mvLog{
			ID:        mvLogID,
			nextPurge: nextPurge,
		}
		l.orderTs = l.nextPurge.UnixMilli()
		newPending[mvLogID] = l
	}
	return newPending, nil
}

// fetchAllTiDBMViews loads all scheduled MV refresh tasks from metadata.
func (*serverHelper) fetchAllTiDBMViews(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error) {
	const sql = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[int64]*mv, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mvID := row.GetInt64(1)
		if mvID <= 0 {
			continue
		}
		nextRefresh := mvsUnix(row.GetInt64(0), 0)
		m := &mv{
			ID:          mvID,
			nextRefresh: nextRefresh,
		}
		m.orderTs = m.nextRefresh.UnixMilli()
		newPending[mvID] = m
	}
	return newPending, nil
}

// execRCRestrictedSQLWithSessionPool executes restricted SQL with a borrowed session.
func execRCRestrictedSQLWithSessionPool(ctx context.Context, sysSessionPool basic.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return execRCRestrictedSQL(ctx, se.(sessionctx.Context), sql, params)
}

// execRCRestrictedSQL is a small wrapper over execRCRestrictedSQLWithSession.
func execRCRestrictedSQL(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	return execRCRestrictedSQLWithSession(ctx, sctx, sql, params)
}

// execRCRestrictedSQLWithSession executes SQL through the restricted SQL executor.
func execRCRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

// withRCRestrictedTxn runs fn in a pessimistic internal transaction.
// It commits on success and rolls back on failure or panic.
func withRCRestrictedTxn(ctx context.Context, sctx sessionctx.Context, fn func(txnCtx context.Context, sctx sessionctx.Context) error) (err error) {
	sqlExec := sctx.GetSQLExecutor()

	if _, err = sqlExec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_, _ = sqlExec.ExecuteInternal(ctx, "ROLLBACK")
			panic(r)
		}
		if err == nil {
			_, err = sqlExec.ExecuteInternal(ctx, "COMMIT")
			return
		}
		_, _ = sqlExec.ExecuteInternal(ctx, "ROLLBACK")
	}()

	err = fn(ctx, sctx)
	return err
}

// getMinRefreshReadTSOFromInfo returns MIN(LAST_SUCCESS_READ_TSO) for given MV IDs.
func getMinRefreshReadTSOFromInfo(ctx context.Context, sctx sessionctx.Context, mvIDs []int64) (int64, error) {
	if len(mvIDs) == 0 {
		return 0, nil
	}
	var b strings.Builder
	params := make([]any, 0, len(mvIDs))
	b.WriteString(`SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (`)
	for i, id := range mvIDs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("%?")
		params = append(params, id)
	}
	b.WriteString(")")
	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, b.String(), params)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetInt64(0), nil
}

// recordMLogPurgeHist appends one purge history row and marks it as newest.
func recordMLogPurgeHist(
	ctx context.Context,
	sctx sessionctx.Context,
	mvLogID int64,
	mlogName string,
	purgeMethod string,
	purgeTime time.Time,
	purgeEndTime time.Time,
	purgeRows uint64,
	purgeStatus string,
) error {
	mvLogIDText := strconv.FormatInt(mvLogID, 10)
	if mlogName == "" {
		mlogName = mvLogIDText
	}
	if purgeMethod == "" {
		purgeMethod = "UNKNOWN"
	}
	const clearNewestSQL = `UPDATE mysql.tidb_mlog_purge_hist SET IS_NEWEST_PURGE = 'NO' WHERE MLOG_ID = %? AND IS_NEWEST_PURGE = 'YES'`
	if _, err := execRCRestrictedSQLWithSession(ctx, sctx, clearNewestSQL, []any{mvLogID}); err != nil {
		return err
	}
	const insertHistSQL = `INSERT INTO mysql.tidb_mlog_purge_hist (MLOG_ID, MLOG_NAME, IS_NEWEST_PURGE, PURGE_METHOD, PURGE_TIME, PURGE_ENDTIME, PURGE_ROWS, PURGE_STATUS) VALUES (%?, %?, 'YES', %?, %?, %?, %?, %?)`
	_, err := execRCRestrictedSQLWithSession(ctx, sctx, insertHistSQL, []any{mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, purgeRows, purgeStatus})
	return err
}

// RegisterMVS registers a DDL event handler for MV-related events.
// onDDLHandled is invoked after the local MV service is notified, and can be
// used by callers to fan out this event to other nodes.
func RegisterMVS(
	ctx context.Context,
	registerHandler func(notifier.HandlerID, notifier.SchemaChangeHandler),
	se basic.SessionPool,
	onDDLHandled func(),
) *MVService {
	if registerHandler == nil || se == nil || onDDLHandled == nil {
		return nil
	}

	cfg := DefaultMVServiceConfig()
	cfg.TaskBackpressure = TaskBackpressureConfig{
		CPUThreshold: defaultMVTaskBackpressureCPUThreshold,
		MemThreshold: defaultMVTaskBackpressureMemThreshold,
		Delay:        defaultTaskBackpressureDelay,
	}
	mvs := NewMVService(ctx, se, newServerHelper(), cfg)
	mvs.NotifyDDLChange() // always trigger a refresh after startup to make sure the in-memory state is up-to-date

	// callback for DDL events only will be triggered on the DDL owner
	// other nodes will get notified through the NotifyDDLChange method from the domain service registry
	registerHandler(notifier.MVServiceHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		// TODO: Events related to materialized view metadata changes should also trigger refresh.
		case meta.ActionCreateMaterializedViewLog:
			onDDLHandled()
		}
		return nil
	})

	return mvs
}
