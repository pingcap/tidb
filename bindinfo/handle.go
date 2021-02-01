// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// BindHandle is used to handle all global sql bind operations.
type BindHandle struct {
	sctx struct {
		sync.Mutex
		sessionctx.Context
	}

	// bindInfo caches the sql bind info from storage.
	//
	// The Mutex protects that there is only one goroutine changes the content
	// of atmoic.Value.
	//
	// NOTE: Concurrent Value Write:
	//
	//    bindInfo.Lock()
	//    newCache := bindInfo.Value.Load()
	//    do the write operation on the newCache
	//    bindInfo.Value.Store(newCache)
	//
	// NOTE: Concurrent Value Read:
	//
	//    cache := bindInfo.Load().
	//    read the content
	//
	bindInfo struct {
		sync.Mutex
		atomic.Value
		parser *parser.Parser
	}

	// invalidBindRecordMap indicates the invalid bind records found during querying.
	// A record will be deleted from this map, after 2 bind-lease, after it is dropped from the kv.
	invalidBindRecordMap struct {
		sync.Mutex
		atomic.Value
	}

	lastUpdateTime types.Time
}

// Lease influences the duration of loading bind info and handling invalid bind.
var Lease = 3 * time.Second

type invalidBindRecordMap struct {
	bindRecord  *BindRecord
	droppedTime time.Time
}

// NewBindHandle creates a new BindHandle.
func NewBindHandle(ctx sessionctx.Context) *BindHandle {
	handle := &BindHandle{}
	handle.sctx.Context = ctx
	handle.bindInfo.Value.Store(make(cache, 32))
	handle.bindInfo.parser = parser.New()
	handle.invalidBindRecordMap.Value.Store(make(map[string]*invalidBindRecordMap))
	return handle
}

// Update updates the global sql bind cache.
func (h *BindHandle) Update(fullLoad bool) (err error) {
	sql := "select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info"
	if !fullLoad {
		sql += " where update_time > \"" + h.lastUpdateTime.String() + "\""
	}

	// No need to acquire the session context lock for ExecRestrictedSQL, it
	// uses another background session.
	rows, _, err := h.sctx.Context.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(nil, sql)
	if err != nil {
		return err
	}

	// Make sure there is only one goroutine writes the cache.
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	for _, row := range rows {
		hash, meta, err := h.newBindMeta(newBindRecord(row))
		// Update lastUpdateTime to the newest one.
		if meta.UpdateTime.Compare(h.lastUpdateTime) > 0 {
			h.lastUpdateTime = meta.UpdateTime
		}
		if err != nil {
			logutil.Logger(context.Background()).Error("update bindinfo failed", zap.Error(err))
			continue
		}

		newCache.removeStaleBindMetas(hash, meta, metrics.ScopeGlobal)
		if meta.Status == Using {
			newCache[hash] = append(newCache[hash], meta)
			metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, meta.Status).Add(meta.size())
		}
	}
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindMeta to the cache.
func (h *BindHandle) AddBindRecord(record *BindRecord) (err error) {
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	h.sctx.Lock()
	_, err = exec.Execute(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.Execute(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.Execute(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		// Make sure there is only one goroutine writes the cache and use parser.
		h.bindInfo.Lock()
		// update the BindMeta to the cache.
		hash, meta, err1 := h.newBindMeta(record)
		if err1 != nil {
			err = err1
			h.bindInfo.Unlock()
			return
		}

		h.appendBindMeta(hash, meta)
		h.bindInfo.Unlock()
	}()

	// remove all the unused sql binds.
	_, err = exec.Execute(context.TODO(), h.deleteBindInfoSQL(record.OriginalSQL, record.Db))
	if err != nil {
		return err
	}

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}
	record.CreateTime = types.Time{
		Time: types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.UpdateTime = record.CreateTime
	record.Status = Using

	// insert the BindRecord to the storage.
	_, err = exec.Execute(context.TODO(), h.insertBindInfoSQL(record))
	return err
}

// DropBindRecord drops a BindRecord to the storage and BindMeta int the cache.
func (h *BindHandle) DropBindRecord(record *BindRecord) (err error) {
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	h.sctx.Lock()

	_, err = exec.Execute(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.Execute(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.Execute(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		hash, meta := newBindMetaWithoutAst(record)
		h.removeBindMeta(hash, meta)
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}

	updateTs := types.Time{
		Time: types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())),
		Type: mysql.TypeDatetime,
		Fsp:  3,
	}
	record.Status = deleted
	record.UpdateTime = updateTs

	_, err = exec.Execute(context.TODO(), h.logicalDeleteBindInfoSQL(record.OriginalSQL, record.Db, updateTs))
	return err
}

// DropInvalidBindRecord execute the drop bindRecord task.
func (h *BindHandle) DropInvalidBindRecord() {
	invalidBindRecordMap := copyInvalidBindRecordMap(h.invalidBindRecordMap.Load().(map[string]*invalidBindRecordMap))
	for key, invalidBindRecord := range invalidBindRecordMap {
		if invalidBindRecord.droppedTime.IsZero() {
			err := h.DropBindRecord(invalidBindRecord.bindRecord)
			if err != nil {
				logutil.Logger(context.Background()).Error("DropInvalidBindRecord failed", zap.Error(err))
			}
			invalidBindRecord.droppedTime = time.Now()
			continue
		}

		if time.Since(invalidBindRecord.droppedTime) > 6*time.Second {
			delete(invalidBindRecordMap, key)
			invalidBindRecord.bindRecord.updateMetrics(metrics.ScopeGlobal, false)
		}
	}
	h.invalidBindRecordMap.Store(invalidBindRecordMap)
}

// AddDropInvalidBindTask add bindRecord to invalidBindRecordMap when the bindRecord need to be deleted.
func (h *BindHandle) AddDropInvalidBindTask(invalidBindRecord *BindRecord) {
	key := invalidBindRecord.OriginalSQL + ":" + invalidBindRecord.Db
	if _, ok := h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap)[key]; ok {
		return
	}
	h.invalidBindRecordMap.Lock()
	defer h.invalidBindRecordMap.Unlock()
	if _, ok := h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap)[key]; ok {
		return
	}
	newMap := copyInvalidBindRecordMap(h.invalidBindRecordMap.Value.Load().(map[string]*invalidBindRecordMap))
	newMap[key] = &invalidBindRecordMap{
		bindRecord: invalidBindRecord,
	}
	h.invalidBindRecordMap.Store(newMap)
	invalidBindRecord.updateMetrics(metrics.ScopeGlobal, true)
}

// Size return the size of bind info cache.
func (h *BindHandle) Size() int {
	size := 0
	for _, bindRecords := range h.bindInfo.Load().(cache) {
		size += len(bindRecords)
	}
	return size
}

// GetBindRecord return the bindMeta of the (normdOrigSQL,db) if bindMeta exist.
func (h *BindHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindMeta {
	return h.bindInfo.Load().(cache).getBindRecord(hash, normdOrigSQL, db)
}

// GetAllBindRecord return all bind record in cache.
func (h *BindHandle) GetAllBindRecord() (bindRecords []*BindMeta) {
	bindRecordMap := h.bindInfo.Load().(cache)
	for _, bindRecord := range bindRecordMap {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

func (h *BindHandle) newBindMeta(record *BindRecord) (hash string, meta *BindMeta, err error) {
	hash = parser.DigestNormalized(record.OriginalSQL)
	meta = &BindMeta{BindRecord: record}
	stmtNodes, _, err := h.bindInfo.parser.Parse(record.BindSQL, record.Charset, record.Collation)
	if err != nil {
		return hash, meta, err
	}
	meta.Hint = CollectHint(stmtNodes[0])
	return hash, meta, nil
}

func newBindMetaWithoutAst(record *BindRecord) (hash string, meta *BindMeta) {
	hash = parser.DigestNormalized(record.OriginalSQL)
	meta = &BindMeta{BindRecord: record}
	return hash, meta
}

// appendBindMeta addes the BindMeta to the cache, all the stale bindMetas are
// removed from the cache after this operation.
func (h *BindHandle) appendBindMeta(hash string, meta *BindMeta) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	newCache.removeStaleBindMetas(hash, meta, metrics.ScopeGlobal)
	newCache[hash] = append(newCache[hash], meta)
	meta.updateMetrics(metrics.ScopeGlobal, true)
	h.bindInfo.Value.Store(newCache)
}

// removeBindMeta removes the BindMeta from the cache.
func (h *BindHandle) removeBindMeta(hash string, meta *BindMeta) {
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	newCache.removeDeletedBindMeta(hash, meta, metrics.ScopeGlobal)
}

// removeDeletedBindMeta removes all the BindMeta which originSQL and db are the same with the parameter's meta.
func (c cache) removeDeletedBindMeta(hash string, meta *BindMeta, scope string) {
	metas, ok := c[hash]
	if !ok {
		return
	}

	for i := len(metas) - 1; i >= 0; i-- {
		if metas[i].isSame(meta) {
			metas[i].updateMetrics(scope, false)
			metas = append(metas[:i], metas[i+1:]...)
			if len(metas) == 0 {
				delete(c, hash)
				return
			}
		}
	}
	c[hash] = metas
}

// removeStaleBindMetas removes all the stale BindMeta in the cache.
func (c cache) removeStaleBindMetas(hash string, meta *BindMeta, scope string) {
	metas, ok := c[hash]
	if !ok {
		return
	}

	for i := len(metas) - 1; i >= 0; i-- {
		if metas[i].isStale(meta) {
			metas[i].updateMetrics(scope, false)
			metas = append(metas[:i], metas[i+1:]...)
			if len(metas) == 0 {
				delete(c, hash)
				return
			}
		}
	}
	c[hash] = metas
}

func (c cache) copy() cache {
	newCache := make(cache, len(c))
	for k, v := range c {
		bindMetas := make([]*BindMeta, len(v))
		copy(bindMetas, v)
		newCache[k] = bindMetas
	}
	return newCache
}

func copyInvalidBindRecordMap(oldMap map[string]*invalidBindRecordMap) map[string]*invalidBindRecordMap {
	newMap := make(map[string]*invalidBindRecordMap, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = v
	}
	return newMap
}

func (c cache) getBindRecord(hash, normdOrigSQL, db string) *BindMeta {
	bindRecords := c[hash]
	if bindRecords != nil {
		for _, bindRecord := range bindRecords {
			if bindRecord.OriginalSQL == normdOrigSQL && bindRecord.Db == db {
				return bindRecord
			}
		}
	}
	return nil
}

// isStale checks whether this BindMeta is stale compared with the other BindMeta.
func (m *BindMeta) isStale(other *BindMeta) bool {
	return m.OriginalSQL == other.OriginalSQL && m.Db == other.Db &&
		m.UpdateTime.Compare(other.UpdateTime) <= 0
}

func (m *BindMeta) isSame(other *BindMeta) bool {
	return m.OriginalSQL == other.OriginalSQL && m.Db == other.Db
}

func (h *BindHandle) deleteBindInfoSQL(normdOrigSQL, db string) string {
	return fmt.Sprintf(
		`DELETE FROM mysql.bind_info WHERE original_sql=%s AND default_db=%s`,
		expression.Quote(normdOrigSQL),
		expression.Quote(db),
	)
}

func (h *BindHandle) insertBindInfoSQL(record *BindRecord) string {
	return fmt.Sprintf(`INSERT INTO mysql.bind_info VALUES (%s, %s, %s, %s, %s, %s,%s, %s)`,
		expression.Quote(record.OriginalSQL),
		expression.Quote(record.BindSQL),
		expression.Quote(record.Db),
		expression.Quote(record.Status),
		expression.Quote(record.CreateTime.String()),
		expression.Quote(record.UpdateTime.String()),
		expression.Quote(record.Charset),
		expression.Quote(record.Collation),
	)
}

func (h *BindHandle) logicalDeleteBindInfoSQL(normdOrigSQL, db string, updateTs types.Time) string {
	return fmt.Sprintf(`UPDATE mysql.bind_info SET status=%s,update_time=%s WHERE original_sql=%s and default_db=%s`,
		expression.Quote(deleted),
<<<<<<< HEAD
		expression.Quote(updateTs.String()),
		expression.Quote(normdOrigSQL),
		expression.Quote(db))
=======
		expression.Quote(updateTsStr),
		expression.Quote(originalSQL),
		expression.Quote(updateTsStr))
	if bindingSQL == "" {
		return sql
	}
	return sql + fmt.Sprintf(` and bind_sql = %s`, expression.Quote(bindingSQL))
}

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines() {
	parser4Capture := parser.New()
	bindableStmts := stmtsummary.StmtSummaryByDigestMap.GetMoreThanOnceBindableStmt()
	for _, bindableStmt := range bindableStmts {
		stmt, err := parser4Capture.ParseOneStmt(bindableStmt.Query, bindableStmt.Charset, bindableStmt.Collation)
		if err != nil {
			logutil.BgLogger().Debug("[sql-bind] parse SQL failed in baseline capture", zap.String("SQL", bindableStmt.Query), zap.Error(err))
			continue
		}
		if insertStmt, ok := stmt.(*ast.InsertStmt); ok && insertStmt.Select == nil {
			continue
		}
		dbName := utilparser.GetDefaultDB(stmt, bindableStmt.Schema)
		normalizedSQL, digest := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(stmt, dbName))
		if r := h.GetBindRecord(digest, normalizedSQL, dbName); r != nil && r.HasUsingBinding() {
			continue
		}
		bindSQL := GenerateBindSQL(context.TODO(), stmt, bindableStmt.PlanHint, true, dbName)
		if bindSQL == "" {
			continue
		}
		charset, collation := h.sctx.GetSessionVars().GetCharsetInfo()
		binding := Binding{
			BindSQL:   bindSQL,
			Status:    Using,
			Charset:   charset,
			Collation: collation,
			Source:    Capture,
		}
		// We don't need to pass the `sctx` because the BindSQL has been validated already.
		err = h.CreateBindRecord(nil, &BindRecord{OriginalSQL: normalizedSQL, Db: dbName, Bindings: []Binding{binding}})
		if err != nil {
			logutil.BgLogger().Debug("[sql-bind] create bind record failed in baseline capture", zap.String("SQL", bindableStmt.Query), zap.Error(err))
		}
	}
}

func getHintsForSQL(sctx sessionctx.Context, sql string) (string, error) {
	origVals := sctx.GetSessionVars().UsePlanBaselines
	sctx.GetSessionVars().UsePlanBaselines = false
	rs, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), fmt.Sprintf("explain format='hint' %s", sql))
	sctx.GetSessionVars().UsePlanBaselines = origVals
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return "", err
	}
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	if err != nil {
		return "", err
	}
	return chk.GetRow(0).GetString(0), nil
}

// GenerateBindSQL generates binding sqls from stmt node and plan hints.
func GenerateBindSQL(ctx context.Context, stmtNode ast.StmtNode, planHint string, captured bool, defaultDB string) string {
	// If would be nil for very simple cases such as point get, we do not need to evolve for them.
	if planHint == "" {
		return ""
	}
	if !captured {
		paramChecker := &paramMarkerChecker{}
		stmtNode.Accept(paramChecker)
		// We need to evolve on current sql, but we cannot restore values for paramMarkers yet,
		// so just ignore them now.
		if paramChecker.hasParamMarker {
			return ""
		}
	}
	// We need to evolve plan based on the current sql, not the original sql which may have different parameters.
	// So here we would remove the hint and inject the current best plan hint.
	hint.BindHint(stmtNode, &hint.HintsSet{})
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	restoreCtx.DefaultDB = defaultDB
	err := stmtNode.Restore(restoreCtx)
	if err != nil {
		logutil.Logger(ctx).Debug("[sql-bind] restore SQL failed when generating bind SQL", zap.Error(err))
	}
	bindSQL := sb.String()
	switch n := stmtNode.(type) {
	case *ast.DeleteStmt:
		deleteIdx := strings.Index(bindSQL, "DELETE")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[deleteIdx:]
		return strings.Replace(bindSQL, "DELETE", fmt.Sprintf("DELETE /*+ %s*/", planHint), 1)
	case *ast.UpdateStmt:
		updateIdx := strings.Index(bindSQL, "UPDATE")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[updateIdx:]
		return strings.Replace(bindSQL, "UPDATE", fmt.Sprintf("UPDATE /*+ %s*/", planHint), 1)
	case *ast.SelectStmt:
		selectIdx := strings.Index(bindSQL, "SELECT")
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[selectIdx:]
		return strings.Replace(bindSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
	case *ast.InsertStmt:
		insertIdx := int(0)
		if n.IsReplace {
			insertIdx = strings.Index(bindSQL, "REPLACE")
		} else {
			insertIdx = strings.Index(bindSQL, "INSERT")
		}
		// Remove possible `explain` prefix.
		bindSQL = bindSQL[insertIdx:]
		return strings.Replace(bindSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
	}
	logutil.Logger(ctx).Debug("[sql-bind] unexpected statement type when generating bind SQL", zap.Any("statement", stmtNode))
	return ""
}

type paramMarkerChecker struct {
	hasParamMarker bool
}

func (e *paramMarkerChecker) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*driver.ParamMarkerExpr); ok {
		e.hasParamMarker = true
		return in, true
	}
	return in, false
}

func (e *paramMarkerChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// AddEvolvePlanTask adds the evolve plan task into memory cache. It would be flushed to store periodically.
func (h *BindHandle) AddEvolvePlanTask(originalSQL, DB string, binding Binding) {
	br := &BindRecord{
		OriginalSQL: originalSQL,
		Db:          DB,
		Bindings:    []Binding{binding},
	}
	h.pendingVerifyBindRecordMap.Add(br)
}

// SaveEvolveTasksToStore saves the evolve task into store.
func (h *BindHandle) SaveEvolveTasksToStore() {
	h.pendingVerifyBindRecordMap.flushToStore()
}

func getEvolveParameters(ctx sessionctx.Context) (time.Duration, time.Time, time.Time, error) {
	sql := fmt.Sprintf("select variable_name, variable_value from mysql.global_variables where variable_name in ('%s', '%s', '%s')",
		variable.TiDBEvolvePlanTaskMaxTime, variable.TiDBEvolvePlanTaskStartTime, variable.TiDBEvolvePlanTaskEndTime)
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	maxTime, startTimeStr, endTimeStr := int64(variable.DefTiDBEvolvePlanTaskMaxTime), variable.DefTiDBEvolvePlanTaskStartTime, variable.DefAutoAnalyzeEndTime
	for _, row := range rows {
		switch row.GetString(0) {
		case variable.TiDBEvolvePlanTaskMaxTime:
			maxTime, err = strconv.ParseInt(row.GetString(1), 10, 64)
			if err != nil {
				return 0, time.Time{}, time.Time{}, err
			}
		case variable.TiDBEvolvePlanTaskStartTime:
			startTimeStr = row.GetString(1)
		case variable.TiDBEvolvePlanTaskEndTime:
			endTimeStr = row.GetString(1)
		}
	}
	startTime, err := time.ParseInLocation(variable.FullDayTimeFormat, startTimeStr, time.UTC)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err

	}
	endTime, err := time.ParseInLocation(variable.FullDayTimeFormat, endTimeStr, time.UTC)
	if err != nil {
		return 0, time.Time{}, time.Time{}, err
	}
	return time.Duration(maxTime) * time.Second, startTime, endTime, nil
}

const (
	// acceptFactor is the factor to decide should we accept the pending verified plan.
	// A pending verified plan will be accepted if it performs at least `acceptFactor` times better than the accepted plans.
	acceptFactor = 1.5
	// verifyTimeoutFactor is how long to wait to verify the pending plan.
	// For debugging purposes it is useful to wait a few times longer than the current execution time so that
	// an informative error can be written to the log.
	verifyTimeoutFactor = 2.0
	// nextVerifyDuration is the duration that we will retry the rejected plans.
	nextVerifyDuration = 7 * 24 * time.Hour
)

func (h *BindHandle) getOnePendingVerifyJob() (string, string, Binding) {
	cache := h.bindInfo.Value.Load().(cache)
	for _, bindRecords := range cache {
		for _, bindRecord := range bindRecords {
			for _, bind := range bindRecord.Bindings {
				if bind.Status == PendingVerify {
					return bindRecord.OriginalSQL, bindRecord.Db, bind
				}
				if bind.Status != Rejected {
					continue
				}
				dur, err := bind.SinceUpdateTime()
				// Should not happen.
				if err != nil {
					continue
				}
				// Rejected and retry it now.
				if dur > nextVerifyDuration {
					return bindRecord.OriginalSQL, bindRecord.Db, bind
				}
			}
		}
	}
	return "", "", Binding{}
}

func (h *BindHandle) getRunningDuration(sctx sessionctx.Context, db, sql string, maxTime time.Duration) (time.Duration, error) {
	ctx := context.TODO()
	if db != "" {
		_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, fmt.Sprintf("use `%s`", db))
		if err != nil {
			return 0, err
		}
	}
	ctx, cancelFunc := context.WithCancel(ctx)
	timer := time.NewTimer(maxTime)
	resultChan := make(chan error)
	startTime := time.Now()
	go runSQL(ctx, sctx, sql, resultChan)
	select {
	case err := <-resultChan:
		cancelFunc()
		if err != nil {
			return 0, err
		}
		return time.Since(startTime), nil
	case <-timer.C:
		cancelFunc()
		logutil.BgLogger().Debug("[sql-bind] plan verification timed out", zap.Duration("timeElapsed", time.Since(startTime)), zap.String("query", sql))
	}
	<-resultChan
	return -1, nil
}

func runSQL(ctx context.Context, sctx sessionctx.Context, sql string, resultChan chan<- error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			resultChan <- fmt.Errorf("run sql panicked: %v", string(buf))
		}
	}()
	rs, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql)
	if err != nil {
		if rs != nil {
			terror.Call(rs.Close)
		}
		resultChan <- err
		return
	}
	chk := rs.NewChunk()
	for {
		err = rs.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			break
		}
	}
	terror.Call(rs.Close)
	resultChan <- err
}

// HandleEvolvePlanTask tries to evolve one plan task.
// It only handle one tasks once because we want each task could use the latest parameters.
func (h *BindHandle) HandleEvolvePlanTask(sctx sessionctx.Context, adminEvolve bool) error {
	originalSQL, db, binding := h.getOnePendingVerifyJob()
	if originalSQL == "" {
		return nil
	}
	maxTime, startTime, endTime, err := getEvolveParameters(sctx)
	if err != nil {
		return err
	}
	if maxTime == 0 || (!timeutil.WithinDayTimePeriod(startTime, endTime, time.Now()) && !adminEvolve) {
		return nil
	}
	sctx.GetSessionVars().UsePlanBaselines = true
	currentPlanTime, err := h.getRunningDuration(sctx, db, binding.BindSQL, maxTime)
	// If we just return the error to the caller, this job will be retried again and again and cause endless logs,
	// since it is still in the bind record. Now we just drop it and if it is actually retryable,
	// we will hope for that we can capture this evolve task again.
	if err != nil {
		return h.DropBindRecord(originalSQL, db, &binding)
	}
	// If the accepted plan timeouts, it is hard to decide the timeout for verify plan.
	// Currently we simply mark the verify plan as `using` if it could run successfully within maxTime.
	if currentPlanTime > 0 {
		maxTime = time.Duration(float64(currentPlanTime) * verifyTimeoutFactor)
	}
	sctx.GetSessionVars().UsePlanBaselines = false
	verifyPlanTime, err := h.getRunningDuration(sctx, db, binding.BindSQL, maxTime)
	if err != nil {
		return h.DropBindRecord(originalSQL, db, &binding)
	}
	if verifyPlanTime == -1 || (float64(verifyPlanTime)*acceptFactor > float64(currentPlanTime)) {
		binding.Status = Rejected
		digestText, _ := parser.NormalizeDigest(binding.BindSQL) // for log desensitization
		logutil.BgLogger().Debug("[sql-bind] new plan rejected",
			zap.Duration("currentPlanTime", currentPlanTime),
			zap.Duration("verifyPlanTime", verifyPlanTime),
			zap.String("digestText", digestText),
		)
	} else {
		binding.Status = Using
	}
	// We don't need to pass the `sctx` because the BindSQL has been validated already.
	return h.AddBindRecord(nil, &BindRecord{OriginalSQL: originalSQL, Db: db, Bindings: []Binding{binding}})
>>>>>>> 7ca1629d1... *: refactor ExecuteInternal to return single resultset (#22546)
}

// Clear resets the bind handle. It is used for test.
func (h *BindHandle) Clear() {
	h.bindInfo.Store(make(cache))
	h.invalidBindRecordMap.Store(make(map[string]*invalidBindRecordMap))
	h.lastUpdateTime = types.ZeroTimestamp
}
