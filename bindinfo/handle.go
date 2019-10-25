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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
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
		hash, meta, err := h.newBindMeta(newBindRecord(row))
		// Update lastUpdateTime to the newest one.
		if meta.UpdateTime.Compare(h.lastUpdateTime) > 0 {
			h.lastUpdateTime = meta.UpdateTime
		}
		if err != nil {
			logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
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

		hash, meta := newBindMetaWithoutHints(record)
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
				logutil.BgLogger().Error("DropInvalidBindRecord failed", zap.Error(err))
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
	hash = parser.DigestHash(record.OriginalSQL)
	stmtNodes, _, err := h.bindInfo.parser.Parse(record.BindSQL, record.Charset, record.Collation)
	if err != nil {
		return "", nil, err
	}
	meta = &BindMeta{BindRecord: record, HintsSet: CollectHint(stmtNodes[0])}
	return hash, meta, nil
}

func newBindMetaWithoutHints(record *BindRecord) (hash string, meta *BindMeta) {
	hash = parser.DigestHash(record.OriginalSQL)
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
		"DELETE FROM mysql.bind_info WHERE original_sql='%s' AND default_db='%s'",
		normdOrigSQL,
		db,
	)
}

func (h *BindHandle) insertBindInfoSQL(record *BindRecord) string {
	return fmt.Sprintf(`INSERT INTO mysql.bind_info VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s')`,
		record.OriginalSQL,
		record.BindSQL,
		record.Db,
		record.Status,
		record.CreateTime,
		record.UpdateTime,
		record.Charset,
		record.Collation,
	)
}

func (h *BindHandle) logicalDeleteBindInfoSQL(normdOrigSQL, db string, updateTs types.Time) string {
	return fmt.Sprintf(`UPDATE mysql.bind_info SET status='%s',update_time='%s' WHERE original_sql='%s' and default_db='%s'`,
		deleted,
		updateTs,
		normdOrigSQL,
		db)
}

// GenHintsFromSQL is used to generate hints from SQL.
// It is used to avoid the circle dependence with planner package.
var GenHintsFromSQL func(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (string, error)

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines(is infoschema.InfoSchema) {
	schemas, sqls := stmtsummary.StmtSummaryByDigestMap.GetMoreThanOnceSelect()
	for i := range sqls {
		stmt, err := h.parser4Baseline.ParseOneStmt(sqls[i], "", "")
		if err != nil {
			logutil.BgLogger().Debug("parse SQL failed", zap.String("SQL", sqls[i]), zap.Error(err))
			continue
		}
		normalizedSQL, digiest := parser.NormalizeDigest(sqls[i])
		if r := h.GetBindRecord(digiest, normalizedSQL, schemas[i]); r != nil && r.Status == Using {
			continue
		}
		h.sctx.Lock()
		err = h.sctx.RefreshTxnCtx(context.TODO())
		var hints string
		if err == nil {
			h.sctx.GetSessionVars().CurrentDB = schemas[i]
			hints, err = GenHintsFromSQL(context.TODO(), h.sctx.Context, stmt, is)
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
		err = h.AddBindRecord(&BindRecord{OriginalSQL: sqls[i], BindSQL: bindsql, Db: schemas[i], Status: Using})
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
