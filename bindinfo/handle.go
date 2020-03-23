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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/timeutil"
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
		parser         *parser.Parser
		lastUpdateTime types.Time
	}

	// invalidBindRecordMap indicates the invalid bind records found during querying.
	// A record will be deleted from this map, after 2 bind-lease, after it is dropped from the kv.
	invalidBindRecordMap tmpBindRecordMap

	// pendingVerifyBindRecordMap indicates the pending verify bind records that found during query.
	pendingVerifyBindRecordMap tmpBindRecordMap
}

// Lease influences the duration of loading bind info and handling invalid bind.
var Lease = 3 * time.Second

const (
	// OwnerKey is the bindinfo owner path that is saved to etcd.
	OwnerKey = "/tidb/bindinfo/owner"
	// Prompt is the prompt for bindinfo owner manager.
	Prompt = "bindinfo"
)

type bindRecordUpdate struct {
	bindRecord *BindRecord
	updateTime time.Time
}

// NewBindHandle creates a new BindHandle.
func NewBindHandle(ctx sessionctx.Context) *BindHandle {
	handle := &BindHandle{}
	handle.sctx.Context = ctx
	handle.bindInfo.Value.Store(make(cache, 32))
	handle.bindInfo.parser = parser.New()
	handle.invalidBindRecordMap.Value.Store(make(map[string]*bindRecordUpdate))
	handle.invalidBindRecordMap.flushFunc = func(record *BindRecord) error {
		return handle.DropBindRecord(record.OriginalSQL, record.Db, &record.Bindings[0])
	}
	handle.pendingVerifyBindRecordMap.Value.Store(make(map[string]*bindRecordUpdate))
	handle.pendingVerifyBindRecordMap.flushFunc = func(record *BindRecord) error {
		// We do not need the first parameter because it is only use to generate hint,
		// and we already have the hint.
		return handle.AddBindRecord(nil, record)
	}
	return handle
}

// Update updates the global sql bind cache.
func (h *BindHandle) Update(fullLoad bool) (err error) {
	h.bindInfo.Lock()
	lastUpdateTime := h.bindInfo.lastUpdateTime
	h.bindInfo.Unlock()

	sql := "select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info"
	if !fullLoad {
		sql += " where update_time > \"" + lastUpdateTime.String() + "\""
	}
	// We need to apply the updates by order, wrong apply order of same original sql may cause inconsistent state.
	sql += " order by update_time"

	// No need to acquire the session context lock for ExecRestrictedSQL, it
	// uses another background session.
	rows, _, err := h.sctx.Context.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(sql)
	if err != nil {
		return err
	}

	// Make sure there is only one goroutine writes the cache.
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		h.bindInfo.lastUpdateTime = lastUpdateTime
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	for _, row := range rows {
		hash, meta, err := h.newBindRecord(row)
		// Update lastUpdateTime to the newest one.
		if meta.Bindings[0].UpdateTime.Compare(lastUpdateTime) > 0 {
			lastUpdateTime = meta.Bindings[0].UpdateTime
		}
		if err != nil {
			logutil.BgLogger().Info("update bindinfo failed", zap.Error(err))
			continue
		}

		oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
		newRecord := merge(oldRecord, meta).removeDeletedBindings()
		if len(newRecord.Bindings) > 0 {
			newCache.setBindRecord(hash, newRecord)
		} else {
			newCache.removeDeletedBindRecord(hash, newRecord)
		}
		updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db), true)
	}
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindRecord to the cache.
func (h *BindHandle) AddBindRecord(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	br := h.GetBindRecord(parser.DigestNormalized(record.OriginalSQL), record.OriginalSQL, record.Db)
	var duplicateBinding *Binding
	if br != nil {
		binding := br.FindBinding(record.Bindings[0].ID)
		if binding != nil {
			// There is already a binding with status `Using`, `PendingVerify` or `Rejected`, we could directly cancel the job.
			if record.Bindings[0].Status == PendingVerify {
				return nil
			}
			// Otherwise, we need to remove it before insert.
			duplicateBinding = binding
		}
	}

	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	h.sctx.Lock()
	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		// Make sure there is only one goroutine writes the cache and use parser.
		h.bindInfo.Lock()
		// update the BindRecord to the cache.
		h.appendBindRecord(parser.DigestNormalized(record.OriginalSQL), record)
		h.bindInfo.Unlock()
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}

	if duplicateBinding != nil {
		_, err = exec.ExecuteInternal(context.TODO(), h.deleteBindInfoSQL(record.OriginalSQL, record.Db, duplicateBinding.BindSQL))
		if err != nil {
			return err
		}
	}

	now := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())), mysql.TypeTimestamp, 3)
	for i := range record.Bindings {
		if duplicateBinding != nil {
			record.Bindings[i].CreateTime = duplicateBinding.CreateTime
		} else {
			record.Bindings[i].CreateTime = now
		}
		record.Bindings[i].UpdateTime = now

		// insert the BindRecord to the storage.
		_, err = exec.ExecuteInternal(context.TODO(), h.insertBindInfoSQL(record.OriginalSQL, record.Db, record.Bindings[i]))
		if err != nil {
			return err
		}
	}
	return nil
}

// DropBindRecord drops a BindRecord to the storage and BindRecord int the cache.
func (h *BindHandle) DropBindRecord(originalSQL, db string, binding *Binding) (err error) {
	h.sctx.Lock()
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)

	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		h.sctx.Unlock()
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			h.sctx.Unlock()
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		h.sctx.Unlock()
		if err != nil {
			return
		}

		record := &BindRecord{OriginalSQL: originalSQL, Db: db}
		if binding != nil {
			record.Bindings = append(record.Bindings, *binding)
		}
		err = h.removeBindRecord(parser.DigestNormalized(originalSQL), record)
	}()

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}

	updateTs := types.NewTime(types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())), mysql.TypeTimestamp, 3)

	bindSQL := ""
	if binding != nil {
		bindSQL = binding.BindSQL
	}

	_, err = exec.ExecuteInternal(context.TODO(), h.logicalDeleteBindInfoSQL(originalSQL, db, updateTs, bindSQL))
	return err
}

// tmpBindRecordMap is used to temporarily save bind record changes.
// Those changes will be flushed into store periodically.
type tmpBindRecordMap struct {
	sync.Mutex
	atomic.Value
	flushFunc func(record *BindRecord) error
}

func (tmpMap *tmpBindRecordMap) flushToStore() {
	newMap := copyBindRecordUpdateMap(tmpMap.Load().(map[string]*bindRecordUpdate))
	for key, bindRecord := range newMap {
		if bindRecord.updateTime.IsZero() {
			err := tmpMap.flushFunc(bindRecord.bindRecord)
			if err != nil {
				logutil.BgLogger().Error("flush bind record failed", zap.Error(err))
			}
			bindRecord.updateTime = time.Now()
			continue
		}

		if time.Since(bindRecord.updateTime) > 6*time.Second {
			delete(newMap, key)
			updateMetrics(metrics.ScopeGlobal, bindRecord.bindRecord, nil, false)
		}
	}
	tmpMap.Store(newMap)
}

func (tmpMap *tmpBindRecordMap) saveToCache(bindRecord *BindRecord) {
	key := bindRecord.OriginalSQL + ":" + bindRecord.Db + ":" + bindRecord.Bindings[0].ID
	if _, ok := tmpMap.Load().(map[string]*bindRecordUpdate)[key]; ok {
		return
	}
	tmpMap.Lock()
	defer tmpMap.Unlock()
	if _, ok := tmpMap.Load().(map[string]*bindRecordUpdate)[key]; ok {
		return
	}
	newMap := copyBindRecordUpdateMap(tmpMap.Load().(map[string]*bindRecordUpdate))
	newMap[key] = &bindRecordUpdate{
		bindRecord: bindRecord,
	}
	tmpMap.Store(newMap)
	updateMetrics(metrics.ScopeGlobal, nil, bindRecord, false)
}

// DropInvalidBindRecord execute the drop bindRecord task.
func (h *BindHandle) DropInvalidBindRecord() {
	h.invalidBindRecordMap.flushToStore()
}

// AddDropInvalidBindTask add bindRecord to invalidBindRecordMap when the bindRecord need to be deleted.
func (h *BindHandle) AddDropInvalidBindTask(invalidBindRecord *BindRecord) {
	h.invalidBindRecordMap.saveToCache(invalidBindRecord)
}

// Size return the size of bind info cache.
func (h *BindHandle) Size() int {
	size := 0
	for _, bindRecords := range h.bindInfo.Load().(cache) {
		size += len(bindRecords)
	}
	return size
}

// GetBindRecord return the BindRecord of the (normdOrigSQL,db) if BindRecord exist.
func (h *BindHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	return h.bindInfo.Load().(cache).getBindRecord(hash, normdOrigSQL, db)
}

// GetAllBindRecord return all bind record in cache.
func (h *BindHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	bindRecordMap := h.bindInfo.Load().(cache)
	for _, bindRecord := range bindRecordMap {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

func (h *BindHandle) newBindRecord(row chunk.Row) (string, *BindRecord, error) {
	hint := Binding{
		BindSQL:    row.GetString(1),
		Status:     row.GetString(3),
		CreateTime: row.GetTime(4),
		UpdateTime: row.GetTime(5),
		Charset:    row.GetString(6),
		Collation:  row.GetString(7),
	}
	bindRecord := &BindRecord{
		OriginalSQL: row.GetString(0),
		Db:          row.GetString(2),
		Bindings:    []Binding{hint},
	}
	hash := parser.DigestNormalized(bindRecord.OriginalSQL)
	h.sctx.Lock()
	defer h.sctx.Unlock()
	h.sctx.GetSessionVars().CurrentDB = bindRecord.Db
	err := bindRecord.prepareHints(h.sctx.Context)
	return hash, bindRecord, err
}

// appendBindRecord addes the BindRecord to the cache, all the stale BindRecords are
// removed from the cache after this operation.
func (h *BindHandle) appendBindRecord(hash string, meta *BindRecord) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
	newRecord := merge(oldRecord, meta)
	newCache.setBindRecord(hash, newRecord)
	h.bindInfo.Value.Store(newCache)
	updateMetrics(metrics.ScopeGlobal, oldRecord, newRecord, false)
}

// removeBindRecord removes the BindRecord from the cache.
func (h *BindHandle) removeBindRecord(hash string, meta *BindRecord) error {
	h.bindInfo.Lock()
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
	defer func() {
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
		updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db), false)
	}()

	newCache.removeDeletedBindRecord(hash, meta)
	return nil
}

// removeDeletedBindRecord removes all the BindRecord which originSQL and db are the same with the parameter's meta.
func (c cache) removeDeletedBindRecord(hash string, meta *BindRecord) {
	metas, ok := c[hash]
	if !ok {
		return
	}

	for i := len(metas) - 1; i >= 0; i-- {
		if metas[i].isSame(meta) {
			metas[i] = metas[i].remove(meta)
			if len(metas[i].Bindings) == 0 {
				metas = append(metas[:i], metas[i+1:]...)
			}
			if len(metas) == 0 {
				delete(c, hash)
				return
			}
		}
	}
	c[hash] = metas
}

func (c cache) setBindRecord(hash string, meta *BindRecord) {
	metas := c[hash]
	for i := range metas {
		if metas[i].Db == meta.Db && metas[i].OriginalSQL == meta.OriginalSQL {
			metas[i] = meta
			return
		}
	}
	c[hash] = append(c[hash], meta)
}

func (c cache) copy() cache {
	newCache := make(cache, len(c))
	for k, v := range c {
		bindRecords := make([]*BindRecord, len(v))
		copy(bindRecords, v)
		newCache[k] = bindRecords
	}
	return newCache
}

func copyBindRecordUpdateMap(oldMap map[string]*bindRecordUpdate) map[string]*bindRecordUpdate {
	newMap := make(map[string]*bindRecordUpdate, len(oldMap))
	for k, v := range oldMap {
		newMap[k] = v
	}
	return newMap
}

func (c cache) getBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	bindRecords := c[hash]
	for _, bindRecord := range bindRecords {
		if bindRecord.OriginalSQL == normdOrigSQL && bindRecord.Db == db {
			return bindRecord
		}
	}
	return nil
}

func (h *BindHandle) deleteBindInfoSQL(normdOrigSQL, db, bindSQL string) string {
	return fmt.Sprintf(
		`DELETE FROM mysql.bind_info WHERE original_sql=%s AND default_db=%s AND bind_sql=%s`,
		expression.Quote(normdOrigSQL),
		expression.Quote(db),
		expression.Quote(bindSQL),
	)
}

func (h *BindHandle) insertBindInfoSQL(orignalSQL string, db string, info Binding) string {
	return fmt.Sprintf(`INSERT INTO mysql.bind_info VALUES (%s, %s, %s, %s, %s, %s, %s, %s)`,
		expression.Quote(orignalSQL),
		expression.Quote(info.BindSQL),
		expression.Quote(db),
		expression.Quote(info.Status),
		expression.Quote(info.CreateTime.String()),
		expression.Quote(info.UpdateTime.String()),
		expression.Quote(info.Charset),
		expression.Quote(info.Collation),
	)
}

func (h *BindHandle) logicalDeleteBindInfoSQL(originalSQL, db string, updateTs types.Time, bindingSQL string) string {
	sql := fmt.Sprintf(`UPDATE mysql.bind_info SET status=%s,update_time=%s WHERE original_sql=%s and default_db=%s`,
		expression.Quote(deleted),
		expression.Quote(updateTs.String()),
		expression.Quote(originalSQL),
		expression.Quote(db))
	if bindingSQL == "" {
		return sql
	}
	return sql + fmt.Sprintf(` and bind_sql = %s`, expression.Quote(bindingSQL))
}

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines() {
	parser4Capture := parser.New()
	schemas, sqls := stmtsummary.StmtSummaryByDigestMap.GetMoreThanOnceSelect()
	for i := range sqls {
		stmt, err := parser4Capture.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			logutil.BgLogger().Debug("parse SQL failed", zap.String("SQL", sqls[i]), zap.Error(err))
			continue
		}
		normalizedSQL, digiest := parser.NormalizeDigest(sqls[i])
		if r := h.GetBindRecord(digiest, normalizedSQL, schemas[i]); r != nil && r.HasUsingBinding() {
			continue
		}
		h.sctx.Lock()
		h.sctx.GetSessionVars().CurrentDB = schemas[i]
		hints, err := getHintsForSQL(h.sctx.Context, sqls[i])
		h.sctx.Unlock()
		if err != nil {
			logutil.BgLogger().Info("generate hints failed", zap.String("SQL", sqls[i]), zap.Error(err))
			continue
		}
		bindSQL := GenerateBindSQL(context.TODO(), stmt, hints)
		if bindSQL == "" {
			continue
		}
		charset, collation := h.sctx.GetSessionVars().GetCharsetInfo()
		hintsSet, err := ParseHintsSet(parser4Capture, bindSQL, charset, collation)
		if err != nil {
			logutil.BgLogger().Debug("parse BindSQL failed", zap.String("SQL", bindSQL), zap.Error(err))
			continue
		}
		binding := Binding{
			BindSQL:   bindSQL,
			Status:    Using,
			Hint:      hintsSet,
			ID:        hints,
			Charset:   charset,
			Collation: collation,
		}
		// We don't need to pass the `sctx` because they are used to generate hints and we already filled hints in.
		err = h.AddBindRecord(nil, &BindRecord{OriginalSQL: normalizedSQL, Db: schemas[i], Bindings: []Binding{binding}})
		if err != nil {
			logutil.BgLogger().Info("capture baseline failed", zap.String("SQL", sqls[i]), zap.Error(err))
		}
	}
}

func getHintsForSQL(sctx sessionctx.Context, sql string) (string, error) {
	oriVals := sctx.GetSessionVars().UsePlanBaselines
	sctx.GetSessionVars().UsePlanBaselines = false
	recordSets, err := sctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("explain format='hint' %s", sql))
	sctx.GetSessionVars().UsePlanBaselines = oriVals
	if len(recordSets) > 0 {
		defer terror.Log(recordSets[0].Close())
	}
	if err != nil {
		return "", err
	}
	chk := recordSets[0].NewChunk()
	err = recordSets[0].Next(context.TODO(), chk)
	if err != nil {
		return "", err
	}
	return chk.GetRow(0).GetString(0), nil
}

// GenerateBindSQL generates binding sqls from stmt node and plan hints.
func GenerateBindSQL(ctx context.Context, stmtNode ast.StmtNode, planHint string) string {
	// If would be nil for very simple cases such as point get, we do not need to evolve for them.
	if planHint == "" {
		return ""
	}
	paramChecker := &paramMarkerChecker{}
	stmtNode.Accept(paramChecker)
	// We need to evolve on current sql, but we cannot restore values for paramMarkers yet,
	// so just ignore them now.
	if paramChecker.hasParamMarker {
		return ""
	}
	// We need to evolve plan based on the current sql, not the original sql which may have different parameters.
	// So here we would remove the hint and inject the current best plan hint.
	BindHint(stmtNode, &HintsSet{})
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := stmtNode.Restore(restoreCtx)
	if err != nil {
		logutil.Logger(ctx).Warn("Restore SQL failed", zap.Error(err))
	}
	bindSQL := sb.String()
	selectIdx := strings.Index(bindSQL, "SELECT")
	// Remove possible `explain` prefix.
	bindSQL = bindSQL[selectIdx:]
	return strings.Replace(bindSQL, "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
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
	h.pendingVerifyBindRecordMap.saveToCache(br)
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
	recordSets, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql)
	if err != nil {
		if len(recordSets) > 0 {
			terror.Call(recordSets[0].Close)
		}
		resultChan <- err
		return
	}
	recordSet := recordSets[0]
	chk := recordSets[0].NewChunk()
	for {
		err = recordSet.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			break
		}
	}
	terror.Call(recordSets[0].Close)
	resultChan <- err
}

// HandleEvolvePlanTask tries to evolve one plan task.
// It only handle one tasks once because we want each task could use the latest parameters.
func (h *BindHandle) HandleEvolvePlanTask(sctx sessionctx.Context) error {
	originalSQL, db, binding := h.getOnePendingVerifyJob()
	if originalSQL == "" {
		return nil
	}
	maxTime, startTime, endTime, err := getEvolveParameters(sctx)
	if err != nil {
		return err
	}
	if maxTime == 0 || !timeutil.WithinDayTimePeriod(startTime, endTime, time.Now()) {
		return nil
	}
	sctx.GetSessionVars().UsePlanBaselines = true
	acceptedPlanTime, err := h.getRunningDuration(sctx, db, binding.BindSQL, maxTime)
	// If we just return the error to the caller, this job will be retried again and again and cause endless logs,
	// since it is still in the bind record. Now we just drop it and if it is actually retryable,
	// we will hope for that we can capture this evolve task again.
	if err != nil {
		return h.DropBindRecord(originalSQL, db, &binding)
	}
	// If the accepted plan timeouts, it is hard to decide the timeout for verify plan.
	// Currently we simply mark the verify plan as `using` if it could run successfully within maxTime.
	if acceptedPlanTime > 0 {
		maxTime = time.Duration(float64(acceptedPlanTime) / acceptFactor)
	}
	sctx.GetSessionVars().UsePlanBaselines = false
	verifyPlanTime, err := h.getRunningDuration(sctx, db, binding.BindSQL, maxTime)
	if err != nil {
		return h.DropBindRecord(originalSQL, db, &binding)
	}
	if verifyPlanTime < 0 {
		binding.Status = Rejected
	} else {
		binding.Status = Using
	}
	return h.AddBindRecord(sctx, &BindRecord{OriginalSQL: originalSQL, Db: db, Bindings: []Binding{binding}})
}

// Clear resets the bind handle. It is used for test.
func (h *BindHandle) Clear() {
	h.bindInfo.Store(make(cache))
	h.invalidBindRecordMap.Store(make(map[string]*bindRecordUpdate))
	h.bindInfo.lastUpdateTime = types.ZeroTimestamp
}
