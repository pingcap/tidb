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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/logutil"
	utilparser "github.com/pingcap/tidb/util/parser"
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
	// of atomic.Value.
	//
	// NOTE: Concurrent Value Write:
	//
	//    bindInfo.Lock()
	//    newCache := bindInfo.Value.Load()
	//    do the write operation on the newCache
	//    bindInfo.Value.Store(newCache)
	//    bindInfo.Unlock()
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
	// BuiltinPseudoSQL4BindLock is used to simulate LOCK TABLE for mysql.bind_info.
	BuiltinPseudoSQL4BindLock = "builtin_pseudo_sql_for_bind_lock"
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
		// BindSQL has already been validated when coming here, so we use nil sctx parameter.
		return handle.AddBindRecord(nil, record)
	}
	variable.RegisterStatistics(handle)
	return handle
}

// Update updates the global sql bind cache.
func (h *BindHandle) Update(fullLoad bool) (err error) {
	h.bindInfo.Lock()
	lastUpdateTime := h.bindInfo.lastUpdateTime
	updateTime := lastUpdateTime.String()
	if fullLoad {
		updateTime = "0000-00-00 00:00:00"
	}

	exec := h.sctx.Context.(sqlexec.RestrictedSQLExecutor)

	// No need to acquire the session context lock for ExecRestrictedSQL, it
	// uses another background session.
	rows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, `SELECT original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source
	FROM mysql.bind_info WHERE update_time > %? ORDER BY update_time, create_time`, updateTime)

	if err != nil {
		h.bindInfo.Unlock()
		return err
	}

	newCache := h.bindInfo.Value.Load().(cache).copy()
	defer func() {
		h.bindInfo.lastUpdateTime = lastUpdateTime
		h.bindInfo.Value.Store(newCache)
		h.bindInfo.Unlock()
	}()

	for _, row := range rows {
		// Skip the builtin record which is designed for binding synchronization.
		if row.GetString(0) == BuiltinPseudoSQL4BindLock {
			continue
		}
		hash, meta, err := h.newBindRecord(row)

		// Update lastUpdateTime to the newest one.
		// Even if this one is an invalid bind.
		if meta.Bindings[0].UpdateTime.Compare(lastUpdateTime) > 0 {
			lastUpdateTime = meta.Bindings[0].UpdateTime
		}

		if err != nil {
			logutil.BgLogger().Debug("[sql-bind] failed to generate bind record from data row", zap.Error(err))
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

// CreateBindRecord creates a BindRecord to the storage and the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *BindHandle) CreateBindRecord(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	record.Db = strings.ToLower(record.Db)
	h.bindInfo.Lock()
	h.sctx.Lock()
	defer func() {
		h.sctx.Unlock()
		h.bindInfo.Unlock()
	}()
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN PESSIMISTIC")
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		if err != nil {
			return
		}

		sqlDigest := parser.DigestNormalized(record.OriginalSQL)
		h.setBindRecord(sqlDigest.String(), record)
	}()

	// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
	if err = h.lockBindInfoTable(); err != nil {
		return err
	}

	now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)

	updateTs := now.String()
	_, err = exec.ExecuteInternal(context.TODO(), `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?`,
		deleted, updateTs, record.OriginalSQL, updateTs)
	if err != nil {
		return err
	}

	for i := range record.Bindings {
		record.Bindings[i].CreateTime = now
		record.Bindings[i].UpdateTime = now

		// Insert the BindRecord to the storage.
		_, err = exec.ExecuteInternal(context.TODO(), `INSERT INTO mysql.bind_info VALUES (%?,%?, %?, %?, %?, %?, %?, %?, %?)`,
			record.OriginalSQL,
			record.Bindings[i].BindSQL,
			record.Db,
			record.Bindings[i].Status,
			record.Bindings[i].CreateTime.String(),
			record.Bindings[i].UpdateTime.String(),
			record.Bindings[i].Charset,
			record.Bindings[i].Collation,
			record.Bindings[i].Source,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddBindRecord adds a BindRecord to the storage and BindRecord to the cache.
func (h *BindHandle) AddBindRecord(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	record.Db = strings.ToLower(record.Db)
	oldRecord := h.GetBindRecord(parser.DigestNormalized(record.OriginalSQL).String(), record.OriginalSQL, record.Db)
	var duplicateBinding *Binding
	if oldRecord != nil {
		binding := oldRecord.FindBinding(record.Bindings[0].ID)
		if binding != nil {
			// There is already a binding with status `Using`, `PendingVerify` or `Rejected`, we could directly cancel the job.
			if record.Bindings[0].Status == PendingVerify {
				return nil
			}
			// Otherwise, we need to remove it before insert.
			duplicateBinding = binding
		}
	}

	h.bindInfo.Lock()
	h.sctx.Lock()
	defer func() {
		h.sctx.Unlock()
		h.bindInfo.Unlock()
	}()
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN PESSIMISTIC")
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		if err != nil {
			return
		}

		h.appendBindRecord(parser.DigestNormalized(record.OriginalSQL).String(), record)
	}()

	// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
	if err = h.lockBindInfoTable(); err != nil {
		return err
	}
	if duplicateBinding != nil {
		_, err = exec.ExecuteInternal(context.TODO(), `DELETE FROM mysql.bind_info WHERE original_sql = %? AND bind_sql = %?`, record.OriginalSQL, duplicateBinding.BindSQL)
		if err != nil {
			return err
		}
	}

	now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)
	for i := range record.Bindings {
		if duplicateBinding != nil {
			record.Bindings[i].CreateTime = duplicateBinding.CreateTime
		} else {
			record.Bindings[i].CreateTime = now
		}
		record.Bindings[i].UpdateTime = now

		// Insert the BindRecord to the storage.
		_, err = exec.ExecuteInternal(context.TODO(), `INSERT INTO mysql.bind_info VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?)`,
			record.OriginalSQL,
			record.Bindings[i].BindSQL,
			record.Db,
			record.Bindings[i].Status,
			record.Bindings[i].CreateTime.String(),
			record.Bindings[i].UpdateTime.String(),
			record.Bindings[i].Charset,
			record.Bindings[i].Collation,
			record.Bindings[i].Source,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// DropBindRecord drops a BindRecord to the storage and BindRecord int the cache.
func (h *BindHandle) DropBindRecord(originalSQL, db string, binding *Binding) (err error) {
	db = strings.ToLower(db)
	h.bindInfo.Lock()
	h.sctx.Lock()
	defer func() {
		h.sctx.Unlock()
		h.bindInfo.Unlock()
	}()
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN PESSIMISTIC")
	if err != nil {
		return err
	}
	var deleteRows int
	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		if err != nil || deleteRows == 0 {
			return
		}

		record := &BindRecord{OriginalSQL: originalSQL, Db: db}
		if binding != nil {
			record.Bindings = append(record.Bindings, *binding)
		}
		h.removeBindRecord(parser.DigestNormalized(originalSQL).String(), record)
	}()

	// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
	if err = h.lockBindInfoTable(); err != nil {
		return err
	}

	updateTs := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3).String()

	if binding == nil {
		_, err = exec.ExecuteInternal(context.TODO(), `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND status != %?`,
			deleted, updateTs, originalSQL, updateTs, deleted)
	} else {
		_, err = exec.ExecuteInternal(context.TODO(), `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND bind_sql = %? and status != %?`,
			deleted, updateTs, originalSQL, updateTs, binding.BindSQL, deleted)
	}

	deleteRows = int(h.sctx.Context.GetSessionVars().StmtCtx.AffectedRows())
	return err
}

// GCBindRecord physically removes the deleted bind records in mysql.bind_info.
func (h *BindHandle) GCBindRecord() (err error) {
	h.bindInfo.Lock()
	h.sctx.Lock()
	defer func() {
		h.sctx.Unlock()
		h.bindInfo.Unlock()
	}()
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(context.TODO(), "BEGIN PESSIMISTIC")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(context.TODO(), "ROLLBACK")
			terror.Log(err1)
			return
		}

		_, err = exec.ExecuteInternal(context.TODO(), "COMMIT")
		if err != nil {
			return
		}
	}()

	// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
	if err = h.lockBindInfoTable(); err != nil {
		return err
	}

	// To make sure that all the deleted bind records have been acknowledged to all tidb,
	// we only garbage collect those records with update_time before 10 leases.
	updateTime := time.Now().Add(-(10 * Lease))
	updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 3).String()
	_, err = exec.ExecuteInternal(context.TODO(), `DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < %?`, updateTimeStr)
	return err
}

// lockBindInfoTable simulates `LOCK TABLE mysql.bind_info WRITE` by acquiring a pessimistic lock on a
// special builtin row of mysql.bind_info. Note that this function must be called with h.sctx.Lock() held.
// We can replace this implementation to normal `LOCK TABLE mysql.bind_info WRITE` if that feature is
// generally available later.
// This lock would enforce the CREATE / DROP GLOBAL BINDING statements to be executed sequentially,
// even if they come from different tidb instances.
func (h *BindHandle) lockBindInfoTable() error {
	// h.sctx already locked.
	exec, _ := h.sctx.Context.(sqlexec.SQLExecutor)
	_, err := exec.ExecuteInternal(context.TODO(), h.LockBindInfoSQL())
	return err
}

// LockBindInfoSQL simulates LOCK TABLE by updating a same row in each pessimistic transaction.
func (h *BindHandle) LockBindInfoSQL() string {
	sql, err := sqlexec.EscapeSQL("UPDATE mysql.bind_info SET source= %? WHERE original_sql= %?", Builtin, BuiltinPseudoSQL4BindLock)
	if err != nil {
		return ""
	}
	return sql
}

// tmpBindRecordMap is used to temporarily save bind record changes.
// Those changes will be flushed into store periodically.
type tmpBindRecordMap struct {
	sync.Mutex
	atomic.Value
	flushFunc func(record *BindRecord) error
}

// flushToStore calls flushFunc for items in tmpBindRecordMap and removes them with a delay.
func (tmpMap *tmpBindRecordMap) flushToStore() {
	tmpMap.Lock()
	defer tmpMap.Unlock()
	newMap := copyBindRecordUpdateMap(tmpMap.Load().(map[string]*bindRecordUpdate))
	for key, bindRecord := range newMap {
		if bindRecord.updateTime.IsZero() {
			err := tmpMap.flushFunc(bindRecord.bindRecord)
			if err != nil {
				logutil.BgLogger().Debug("[sql-bind] flush bind record failed", zap.Error(err))
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

// Add puts a BindRecord into tmpBindRecordMap.
func (tmpMap *tmpBindRecordMap) Add(bindRecord *BindRecord) {
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

// DropInvalidBindRecord executes the drop BindRecord tasks.
func (h *BindHandle) DropInvalidBindRecord() {
	h.invalidBindRecordMap.flushToStore()
}

// AddDropInvalidBindTask adds BindRecord which needs to be deleted into invalidBindRecordMap.
func (h *BindHandle) AddDropInvalidBindTask(invalidBindRecord *BindRecord) {
	h.invalidBindRecordMap.Add(invalidBindRecord)
}

// Size returns the size of bind info cache.
func (h *BindHandle) Size() int {
	size := 0
	for _, bindRecords := range h.bindInfo.Load().(cache) {
		size += len(bindRecords)
	}
	return size
}

// GetBindRecord returns the BindRecord of the (normdOrigSQL,db) if BindRecord exist.
func (h *BindHandle) GetBindRecord(hash, normdOrigSQL, db string) *BindRecord {
	return h.bindInfo.Load().(cache).getBindRecord(hash, normdOrigSQL, db)
}

// GetAllBindRecord returns all bind records in cache.
func (h *BindHandle) GetAllBindRecord() (bindRecords []*BindRecord) {
	bindRecordMap := h.bindInfo.Load().(cache)
	for _, bindRecord := range bindRecordMap {
		bindRecords = append(bindRecords, bindRecord...)
	}
	return bindRecords
}

// newBindRecord builds BindRecord from a tuple in storage.
func (h *BindHandle) newBindRecord(row chunk.Row) (string, *BindRecord, error) {
	hint := Binding{
		BindSQL:    row.GetString(1),
		Status:     row.GetString(3),
		CreateTime: row.GetTime(4),
		UpdateTime: row.GetTime(5),
		Charset:    row.GetString(6),
		Collation:  row.GetString(7),
		Source:     row.GetString(8),
	}
	bindRecord := &BindRecord{
		OriginalSQL: row.GetString(0),
		Db:          strings.ToLower(row.GetString(2)),
		Bindings:    []Binding{hint},
	}
	hash := parser.DigestNormalized(bindRecord.OriginalSQL)
	h.sctx.Lock()
	defer h.sctx.Unlock()
	h.sctx.GetSessionVars().CurrentDB = bindRecord.Db
	err := bindRecord.prepareHints(h.sctx.Context)
	return hash.String(), bindRecord, err
}

// setBindRecord sets the BindRecord to the cache, if there already exists a BindRecord,
// it will be overridden.
func (h *BindHandle) setBindRecord(hash string, meta *BindRecord) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
	newCache.setBindRecord(hash, meta)
	h.bindInfo.Value.Store(newCache)
	updateMetrics(metrics.ScopeGlobal, oldRecord, meta, false)
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
func (h *BindHandle) removeBindRecord(hash string, meta *BindRecord) {
	newCache := h.bindInfo.Value.Load().(cache).copy()
	oldRecord := newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db)
	newCache.removeDeletedBindRecord(hash, meta)
	h.bindInfo.Value.Store(newCache)
	updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.getBindRecord(hash, meta.OriginalSQL, meta.Db), false)
}

// removeDeletedBindRecord removes the BindRecord which has same originSQL and db with specified BindRecord.
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
		if metas[i].OriginalSQL == meta.OriginalSQL {
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
		if bindRecord.OriginalSQL == normdOrigSQL {
			return bindRecord
		}
	}
	return nil
}

type captureFilter struct {
	dbs       map[string]struct{}
	frequency int64
	tables    map[stmtctx.TableEntry]struct{}

	fail      bool
	currentDB string
}

func (cf *captureFilter) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch x := in.(type) {
	case *ast.TableName:
		tblEntry := stmtctx.TableEntry{
			DB:    x.Schema.L,
			Table: x.Name.L,
		}
		if x.Schema.L == "" {
			tblEntry.DB = cf.currentDB
		}
		if _, ok := cf.dbs[tblEntry.DB]; ok {
			cf.fail = true
		} else if _, ok := cf.tables[tblEntry]; ok {
			cf.fail = true
		}
	}
	return in, cf.fail
}

func (cf *captureFilter) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (cf *captureFilter) isEmpty() bool {
	return len(cf.dbs) == 0 && len(cf.tables) == 0
}

func (h *BindHandle) extractCaptureFilterFromStorage() (filter *captureFilter) {
	filter = &captureFilter{
		dbs:       make(map[string]struct{}),
		frequency: 1,
		tables:    make(map[stmtctx.TableEntry]struct{}),
	}
	exec := h.sctx.Context.(sqlexec.RestrictedSQLExecutor)
	// No need to acquire the session context lock for ExecRestrictedSQL, it
	// uses another background session.
	rows, _, err := exec.ExecRestrictedSQL(context.TODO(), nil, `SELECT filter_type, filter_value FROM mysql.capture_plan_baselines_blacklist order by filter_type`)
	if err != nil {
		logutil.BgLogger().Warn("[sql-bind] failed to load mysql.capture_plan_baselines_blacklist", zap.Error(err))
		return
	}
	for _, row := range rows {
		filterTp := strings.ToLower(row.GetString(0))
		valStr := strings.ToLower(row.GetString(1))
		switch filterTp {
		case "db":
			filter.dbs[valStr] = struct{}{}
		case "table":
			strs := strings.Split(valStr, ".")
			if len(strs) != 2 {
				logutil.BgLogger().Warn("[sql-bind] failed to parse table name, ignore it", zap.String("filter_value", valStr))
				continue
			}
			tblEntry := stmtctx.TableEntry{
				DB:    strs[0],
				Table: strs[1],
			}
			filter.tables[tblEntry] = struct{}{}
		case "frequency":
			f, err := strconv.ParseInt(valStr, 10, 64)
			if err != nil {
				logutil.BgLogger().Warn("[sql-bind] failed to parse frequency type value, ignore it", zap.String("filter_value", valStr), zap.Error(err))
				continue
			}
			if f < 1 {
				logutil.BgLogger().Warn("[sql-bind] frequency threshold is less than 1, ignore it", zap.Int64("frequency", f))
				continue
			}
			if f > filter.frequency {
				filter.frequency = f
			}
		default:
			logutil.BgLogger().Warn("[sql-bind] unknown capture filter type, ignore it", zap.String("filter_type", filterTp))
		}
	}
	return
}

// CaptureBaselines is used to automatically capture plan baselines.
func (h *BindHandle) CaptureBaselines() {
	parser4Capture := parser.New()
	captureFilter := h.extractCaptureFilterFromStorage()
	emptyCaptureFilter := captureFilter.isEmpty()
	bindableStmts := stmtsummary.StmtSummaryByDigestMap.GetMoreThanCntBindableStmt(captureFilter.frequency)
	for _, bindableStmt := range bindableStmts {
		stmt, err := parser4Capture.ParseOneStmt(bindableStmt.Query, bindableStmt.Charset, bindableStmt.Collation)
		if err != nil {
			logutil.BgLogger().Debug("[sql-bind] parse SQL failed in baseline capture", zap.String("SQL", bindableStmt.Query), zap.Error(err))
			continue
		}
		if insertStmt, ok := stmt.(*ast.InsertStmt); ok && insertStmt.Select == nil {
			continue
		}
		if !emptyCaptureFilter {
			captureFilter.fail = false
			captureFilter.currentDB = bindableStmt.Schema
			stmt.Accept(captureFilter)
			if captureFilter.fail {
				continue
			}
		}
		dbName := utilparser.GetDefaultDB(stmt, bindableStmt.Schema)
		normalizedSQL, digest := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(stmt, dbName, bindableStmt.Query))
		if r := h.GetBindRecord(digest.String(), normalizedSQL, dbName); r != nil && r.HasUsingBinding() {
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

	// Usually passing a sprintf to ExecuteInternal is not recommended, but in this case
	// it is safe because ExecuteInternal does not permit MultiStatement execution. Thus,
	// the statement won't be able to "break out" from EXPLAIN.
	rs, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), fmt.Sprintf("EXPLAIN FORMAT='hint' %s", sql))
	sctx.GetSessionVars().UsePlanBaselines = origVals
	if rs != nil {
		defer func() {
			// Audit log is collected in Close(), set InRestrictedSQL to avoid 'create sql binding' been recorded as 'explain'.
			origin := sctx.GetSessionVars().InRestrictedSQL
			sctx.GetSessionVars().InRestrictedSQL = true
			terror.Call(rs.Close)
			sctx.GetSessionVars().InRestrictedSQL = origin
		}()
	}
	if err != nil {
		return "", err
	}
	chk := rs.NewChunk(nil)
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
	bindSQL := utilparser.RestoreWithDefaultDB(stmtNode, defaultDB, "")
	if bindSQL == "" {
		return ""
	}
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
		var selectIdx int
		if n.With != nil {
			var withSb strings.Builder
			withIdx := strings.Index(bindSQL, "WITH")
			restoreCtx := format.NewRestoreCtx(format.RestoreStringSingleQuotes|format.RestoreSpacesAroundBinaryOperation|format.RestoreStringWithoutCharset|format.RestoreNameBackQuotes, &withSb)
			restoreCtx.DefaultDB = defaultDB
			err := n.With.Restore(restoreCtx)
			if err != nil {
				logutil.BgLogger().Debug("[sql-bind] restore SQL failed", zap.Error(err))
				return ""
			}
			withEnd := withIdx + len(withSb.String())
			tmp := strings.Replace(bindSQL[withEnd:], "SELECT", fmt.Sprintf("SELECT /*+ %s*/", planHint), 1)
			return strings.Join([]string{bindSQL[withIdx:withEnd], tmp}, "")
		}
		selectIdx = strings.Index(bindSQL, "SELECT")
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
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(
		context.TODO(),
		nil,
		"SELECT variable_name, variable_value FROM mysql.global_variables WHERE variable_name IN (%?, %?, %?)",
		variable.TiDBEvolvePlanTaskMaxTime,
		variable.TiDBEvolvePlanTaskStartTime,
		variable.TiDBEvolvePlanTaskEndTime,
	)
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
		_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "use %n", db)
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
	chk := rs.NewChunk(nil)
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
// It only processes one task at a time because we want each task to use the latest parameters.
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
}

// Clear resets the bind handle. It is only used for test.
func (h *BindHandle) Clear() {
	h.bindInfo.Lock()
	h.bindInfo.Store(make(cache))
	h.bindInfo.lastUpdateTime = types.ZeroTimestamp
	h.bindInfo.Unlock()
	h.invalidBindRecordMap.Store(make(map[string]*bindRecordUpdate))
	h.pendingVerifyBindRecordMap.Store(make(map[string]*bindRecordUpdate))
}

// FlushBindings flushes the BindRecord in temp maps to storage and loads them into cache.
func (h *BindHandle) FlushBindings() error {
	h.DropInvalidBindRecord()
	h.SaveEvolveTasksToStore()
	return h.Update(false)
}

// ReloadBindings clears existing binding cache and do a full load from mysql.bind_info.
// It is used to maintain consistency between cache and mysql.bind_info if the table is deleted or truncated.
func (h *BindHandle) ReloadBindings() error {
	h.bindInfo.Lock()
	h.bindInfo.Store(make(cache))
	h.bindInfo.lastUpdateTime = types.ZeroTimestamp
	h.bindInfo.Unlock()
	return h.Update(true)
}
