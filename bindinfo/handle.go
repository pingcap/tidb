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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
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

	parser4Baseline *parser.Parser
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
	handle.parser4Baseline = parser.New()
	handle.invalidBindRecordMap.Value.Store(make(map[string]*invalidBindRecordMap))
	return handle
}

// Update updates the global sql bind cache.
func (h *BindHandle) Update(fullLoad bool) (err error) {
	sql := "select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info"
	if !fullLoad {
		sql += " where update_time >= \"" + h.lastUpdateTime.String() + "\""
	}

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
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	for _, row := range rows {
		hash, meta, err := h.newBindRecord(row)
		// Update lastUpdateTime to the newest one.
		if meta.Bindings[0].UpdateTime.Compare(h.lastUpdateTime) > 0 {
			h.lastUpdateTime = meta.Bindings[0].UpdateTime
		}
		if err != nil {
			logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
			continue
		}

		oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
		newRecord := merge(oldRecord, meta)
		if meta.HasUsingBinding() {
			newCache.setBindRecord(hash, newRecord)
		} else {
			newCache.removeDeletedBindRecord(hash, oldRecord)
		}
		updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db), true)
	}
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindRecord to the cache.
func (h *BindHandle) AddBindRecord(sctx sessionctx.Context, is infoschema.InfoSchema, record *BindRecord) (err error) {
	err = record.prepareHints(sctx, is)
	if err != nil {
		return err
	}
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
		// update the BindRecord to the cache.
		h.appendBindRecord(parser.DigestHash(record.OriginalSQL), record)
		h.bindInfo.Unlock()
	}()

	oldBindRecord := h.GetBindRecord(parser.DigestHash(record.OriginalSQL), record.OriginalSQL, record.Db)
	if oldBindRecord != nil {
		for _, newBinding := range record.Bindings {
			binding := oldBindRecord.FindUsingBinding(newBinding.id)
			if binding == nil {
				continue
			}
			// Remove duplicates before insert.
			_, err = exec.Execute(context.TODO(), h.deleteBindInfoSQL(record.OriginalSQL, record.Db, binding.BindSQL))
			if err != nil {
				return err
			}
		}
	}

	txn, err1 := h.sctx.Context.Txn(true)
	if err1 != nil {
		return err1
	}
	for i := range record.Bindings {
		record.Bindings[i].CreateTime = types.Time{
			Time: types.FromGoTime(oracle.GetTimeFromTS(txn.StartTS())),
			Type: mysql.TypeDatetime,
			Fsp:  3,
		}
		record.Bindings[i].UpdateTime = record.Bindings[0].CreateTime
		record.Bindings[i].Status = Using

		// insert the BindRecord to the storage.
		_, err = exec.Execute(context.TODO(), h.insertBindInfoSQL(record.OriginalSQL, record.Db, record.Bindings[i]))
		if err != nil {
			return err
		}
	}
	return nil
}

// DropBindRecord drops a BindRecord to the storage and BindRecord int the cache.
func (h *BindHandle) DropBindRecord(sctx sessionctx.Context, is infoschema.InfoSchema, record *BindRecord) (err error) {
	err = record.prepareHints(sctx, is)
	if err != nil {
		return err
	}
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

		err = h.removeBindRecord(parser.DigestHash(record.OriginalSQL), record)
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
	oldBindRecord := h.GetBindRecord(parser.DigestHash(record.OriginalSQL), record.OriginalSQL, record.Db)
	bindingSQLs := make([]string, 0, len(record.Bindings))
	for i := range record.Bindings {
		record.Bindings[i].Status = deleted
		record.Bindings[i].UpdateTime = updateTs
		if oldBindRecord == nil {
			continue
		}
		binding := oldBindRecord.FindUsingBinding(record.Bindings[i].id)
		if binding != nil {
			bindingSQLs = append(bindingSQLs, binding.BindSQL)
		}
	}

	_, err = exec.Execute(context.TODO(), h.logicalDeleteBindInfoSQL(record.OriginalSQL, record.Db, updateTs, bindingSQLs))
	return err
}

// DropInvalidBindRecord execute the drop bindRecord task.
func (h *BindHandle) DropInvalidBindRecord() {
	invalidBindRecordMap := copyInvalidBindRecordMap(h.invalidBindRecordMap.Load().(map[string]*invalidBindRecordMap))
	for key, invalidBindRecord := range invalidBindRecordMap {
		if invalidBindRecord.droppedTime.IsZero() {
			err := h.DropBindRecord(nil, nil, invalidBindRecord.bindRecord)
			if err != nil {
				logutil.BgLogger().Error("DropInvalidBindRecord failed", zap.Error(err))
			}
			invalidBindRecord.droppedTime = time.Now()
			continue
		}

		if time.Since(invalidBindRecord.droppedTime) > 6*time.Second {
			delete(invalidBindRecordMap, key)
			updateMetrics(metrics.ScopeGlobal, invalidBindRecord.bindRecord, nil, false)
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
	updateMetrics(metrics.ScopeGlobal, nil, invalidBindRecord, false)
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
	hash := parser.DigestHash(bindRecord.OriginalSQL)
	h.sctx.Lock()
	defer h.sctx.Unlock()
	err := h.sctx.RefreshTxnCtx(context.TODO())
	if err != nil {
		return "", nil, err
	}
	h.sctx.GetSessionVars().CurrentDB = bindRecord.Db
	err = bindRecord.prepareHints(h.sctx.Context, h.sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema))
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
		newCache[k] = v
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

func (c cache) getBindRecord(hash, normdOrigSQL, db string) *BindRecord {
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

func (h *BindHandle) deleteBindInfoSQL(normdOrigSQL, db, bindSQL string) string {
	return fmt.Sprintf(
		`DELETE FROM mysql.bind_info WHERE original_sql=%s AND default_db=%s AND bind_sql = %s`,
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

func (h *BindHandle) logicalDeleteBindInfoSQL(originalSQL, db string, updateTs types.Time, bindingSQLs []string) string {
	sql := fmt.Sprintf(`UPDATE mysql.bind_info SET status=%s,update_time=%s WHERE original_sql=%s and default_db=%s`,
		expression.Quote(deleted),
		expression.Quote(updateTs.String()),
		expression.Quote(originalSQL),
		expression.Quote(db))
	if len(bindingSQLs) == 0 {
		return sql
	}
	for i, sql := range bindingSQLs {
		bindingSQLs[i] = fmt.Sprintf(`%s`, expression.Quote(sql))
	}
	return sql + fmt.Sprintf(` and bind_sql in (%s)`, strings.Join(bindingSQLs, ","))
}

// GenHintsFromSQL is used to generate hints from SQL.
// It is used to avoid the circle dependence with planner package.
var GenHintsFromSQL func(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (string, error)

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines() {
	schemas, sqls := stmtsummary.StmtSummaryByDigestMap.GetMoreThanOnceSelect()
	for i := range sqls {
		stmt, err := h.parser4Baseline.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			logutil.BgLogger().Debug("parse SQL failed", zap.String("SQL", sqls[i]), zap.Error(err))
			continue
		}
		normalizedSQL, digiest := parser.NormalizeDigest(sqls[i])
		if r := h.GetBindRecord(digiest, normalizedSQL, schemas[i]); r != nil && r.HasUsingBinding() {
			continue
		}
		h.sctx.Lock()
		err = h.sctx.RefreshTxnCtx(context.TODO())
		var hints string
		if err == nil {
			h.sctx.GetSessionVars().CurrentDB = schemas[i]
			hints, err = GenHintsFromSQL(context.TODO(), h.sctx.Context, stmt, h.sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema))
		}
		h.sctx.Unlock()
		if err != nil {
			logutil.BgLogger().Info("generate hints failed", zap.String("SQL", sqls[i]), zap.Error(err))
			continue
		}
		// We can skip simple query like point get.
		if hints == "" {
			continue
		}
		bindsql := strings.Replace(normalizedSQL, "select", fmt.Sprintf("select /*+ %s*/", hints), 1)
		binding := Binding{
			BindSQL: bindsql,
			Status:  Using,
			Hint:    CollectHint(stmt),
			id:      hints,
		}
		// We don't need to pass the `sctx` and `is` because they are used to generate hints and we already filled hints in.
		err = h.AddBindRecord(nil, nil, &BindRecord{OriginalSQL: sqls[i], Db: schemas[i], Bindings: []Binding{binding}})
		if err != nil {
			logutil.BgLogger().Info("capture baseline failed", zap.String("SQL", sqls[i]), zap.Error(err))
		}
	}
}

// Clear resets the bind handle. It is used for test.
func (h *BindHandle) Clear() {
	h.bindInfo.Store(make(cache))
	h.invalidBindRecordMap.Store(make(map[string]*invalidBindRecordMap))
	h.lastUpdateTime = types.ZeroTimestamp
}
