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
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

// GlobalBindingHandle is used to handle all global sql bind operations.
type GlobalBindingHandle interface {
	// Methods for create, get, drop global sql bindings.

	// GetGlobalBinding returns the BindRecord of the (normalizedSQL,db) if BindRecord exist.
	GetGlobalBinding(sqlDigest, normalizedSQL, db string) *BindRecord

	// GetGlobalBindingBySQLDigest returns the BindRecord of the sql digest.
	GetGlobalBindingBySQLDigest(sqlDigest string) (*BindRecord, error)

	// GetAllGlobalBindings returns all bind records in cache.
	GetAllGlobalBindings() (bindRecords []*BindRecord)

	// CreateGlobalBinding creates a BindRecord to the storage and the cache.
	// It replaces all the exists bindings for the same normalized SQL.
	CreateGlobalBinding(sctx sessionctx.Context, record *BindRecord) (err error)

	// DropGlobalBinding drops a BindRecord to the storage and BindRecord int the cache.
	DropGlobalBinding(originalSQL, db string, binding *Binding) (deletedRows uint64, err error)

	// DropGlobalBindingByDigest drop BindRecord to the storage and BindRecord int the cache.
	DropGlobalBindingByDigest(sqlDigest string) (deletedRows uint64, err error)

	// SetGlobalBindingStatus set a BindRecord's status to the storage and bind cache.
	SetGlobalBindingStatus(originalSQL string, binding *Binding, newStatus string) (ok bool, err error)

	// SetGlobalBindingStatusByDigest set a BindRecord's status to the storage and bind cache.
	SetGlobalBindingStatusByDigest(newStatus, sqlDigest string) (ok bool, err error)

	// AddInvalidGlobalBinding adds BindRecord which needs to be deleted into invalidBindRecordMap.
	AddInvalidGlobalBinding(invalidBindRecord *BindRecord)

	// DropInvalidGlobalBinding executes the drop BindRecord tasks.
	DropInvalidGlobalBinding()

	// Methods for load and clear global sql bindings.

	// Reset is to reset the BindHandle and clean old info.
	Reset()

	// Update updates the global sql bind cache.
	Update(fullLoad bool) (err error)

	// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
	GCGlobalBinding() (err error)

	// LockBindInfoSQL simulates LOCK TABLE by updating a same row in each pessimistic transaction.
	LockBindInfoSQL() string

	// Methods for memory control.

	// Size returns the size of bind info cache.
	Size() int

	// SetBindCacheCapacity reset the capacity for the bindCache.
	SetBindCacheCapacity(capacity int64)

	// GetMemUsage returns the memory usage for the bind cache.
	GetMemUsage() (memUsage int64)

	// GetMemCapacity returns the memory capacity for the bind cache.
	GetMemCapacity() (memCapacity int64)

	// Clear resets the bind handle. It is only used for test.
	Clear()

	// FlushGlobalBindings flushes the BindRecord in temp maps to storage and loads them into cache.
	FlushGlobalBindings() error

	// ReloadGlobalBindings clears existing binding cache and do a full load from mysql.bind_info.
	ReloadGlobalBindings() error

	// Methods for Auto Capture.

	// CaptureBaselines is used to automatically capture plan baselines.
	CaptureBaselines()

	variable.Statistics
}

// globalBindingHandle is used to handle all global sql bind operations.
type globalBindingHandle struct {
	sPool SessionPool

	bindingCache atomic.Pointer[bindCache]

	// lastTaskTime records the last update time for the global sql bind cache.
	// This value is used to avoid reload duplicated bindings from storage.
	lastUpdateTime atomic.Value

	// invalidBindRecordMap indicates the invalid bind records found during querying.
	// A record will be deleted from this map, after 2 bind-lease, after it is dropped from the kv.
	invalidBindRecordMap tmpBindRecordMap
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

	// StmtRemoveDuplicatedPseudoBinding is used to remove duplicated pseudo binding.
	// After using BR to sync bind_info between two clusters, the pseudo binding may be duplicated, and
	// BR use this statement to remove duplicated rows, and this SQL should only be executed by BR.
	StmtRemoveDuplicatedPseudoBinding = `DELETE FROM mysql.bind_info
       WHERE original_sql='builtin_pseudo_sql_for_bind_lock' AND
       _tidb_rowid NOT IN ( -- keep one arbitrary pseudo binding
         SELECT _tidb_rowid FROM mysql.bind_info WHERE original_sql='builtin_pseudo_sql_for_bind_lock' limit 1)`
)

type bindRecordUpdate struct {
	bindRecord *BindRecord
	updateTime time.Time
}

// NewGlobalBindingHandle creates a new GlobalBindingHandle.
func NewGlobalBindingHandle(sPool SessionPool) GlobalBindingHandle {
	handle := &globalBindingHandle{sPool: sPool}
	handle.Reset()
	return handle
}

func (h *globalBindingHandle) getCache() *bindCache {
	return h.bindingCache.Load()
}

func (h *globalBindingHandle) setCache(c *bindCache) {
	// TODO: update the global cache in-place instead of replacing it and remove this function.
	h.bindingCache.Store(c)
}

// Reset is to reset the BindHandle and clean old info.
func (h *globalBindingHandle) Reset() {
	h.lastUpdateTime.Store(types.ZeroTimestamp)
	h.invalidBindRecordMap.Value.Store(make(map[string]*bindRecordUpdate))
	h.invalidBindRecordMap.flushFunc = func(record *BindRecord) error {
		_, err := h.DropGlobalBinding(record.OriginalSQL, record.Db, &record.Bindings[0])
		return err
	}
	h.setCache(newBindCache())
	variable.RegisterStatistics(h)
}

func (h *globalBindingHandle) getLastUpdateTime() types.Time {
	return h.lastUpdateTime.Load().(types.Time)
}

func (h *globalBindingHandle) setLastUpdateTime(t types.Time) {
	h.lastUpdateTime.Store(t)
}

// Update updates the global sql bind cache.
func (h *globalBindingHandle) Update(fullLoad bool) (err error) {
	lastUpdateTime := h.getLastUpdateTime()
	var timeCondition string
	if !fullLoad {
		timeCondition = fmt.Sprintf("WHERE update_time>'%s'", lastUpdateTime.String())
	}
	selectStmt := fmt.Sprintf(`SELECT original_sql, bind_sql, default_db, status, create_time,
       update_time, charset, collation, source, sql_digest, plan_digest FROM mysql.bind_info
       %s ORDER BY update_time, create_time`, timeCondition)

	return h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, selectStmt)
		if err != nil {
			return err
		}

		newCache, memExceededErr := h.getCache().Copy()
		defer func() {
			h.setLastUpdateTime(lastUpdateTime)
			h.setCache(newCache) // TODO: update it in place
		}()

		for _, row := range rows {
			// If the memory usage of the binding_cache exceeds its capacity, we will break and do not handle.
			if memExceededErr != nil {
				break
			}
			// Skip the builtin record which is designed for binding synchronization.
			if row.GetString(0) == BuiltinPseudoSQL4BindLock {
				continue
			}
			sqlDigest, meta, err := newBindRecord(sctx, row)

			// Update lastUpdateTime to the newest one.
			// Even if this one is an invalid bind.
			if meta.Bindings[0].UpdateTime.Compare(lastUpdateTime) > 0 {
				lastUpdateTime = meta.Bindings[0].UpdateTime
			}

			if err != nil {
				logutil.BgLogger().Debug("failed to generate bind record from data row", zap.String("category", "sql-bind"), zap.Error(err))
				continue
			}

			oldRecord := newCache.GetBinding(sqlDigest, meta.OriginalSQL, meta.Db)
			newRecord := merge(oldRecord, meta).removeDeletedBindings()
			if len(newRecord.Bindings) > 0 {
				err = newCache.SetBinding(sqlDigest, newRecord)
				if err != nil {
					memExceededErr = err
				}
			} else {
				newCache.RemoveBinding(sqlDigest, newRecord)
			}
			updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.GetBinding(sqlDigest, meta.OriginalSQL, meta.Db), true)
		}
		if memExceededErr != nil {
			// When the memory capacity of bing_cache is not enough,
			// there will be some memory-related errors in multiple places.
			// Only needs to be handled once.
			logutil.BgLogger().Warn("BindHandle.Update", zap.String("category", "sql-bind"), zap.Error(memExceededErr))
		}
		return nil
	})
}

// CreateGlobalBinding creates a BindRecord to the storage and the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *globalBindingHandle) CreateGlobalBinding(sctx sessionctx.Context, record *BindRecord) (err error) {
	err = record.prepareHints(sctx)
	if err != nil {
		return err
	}

	record.Db = strings.ToLower(record.Db)
	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		defer func() {
			if err != nil {
				return
			}
			sqlDigest := parser.DigestNormalized(record.OriginalSQL)
			h.setGlobalCacheBinding(sqlDigest.String(), record)
		}()

		// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
		if err = h.lockBindInfoTable(sctx); err != nil {
			return err
		}

		now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)

		updateTs := now.String()
		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?`,
			deleted, updateTs, record.OriginalSQL, updateTs)
		if err != nil {
			return err
		}

		for i := range record.Bindings {
			record.Bindings[i].CreateTime = now
			record.Bindings[i].UpdateTime = now

			// Insert the BindRecord to the storage.
			_, err = exec(sctx, `INSERT INTO mysql.bind_info VALUES (%?,%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
				record.OriginalSQL,
				record.Bindings[i].BindSQL,
				record.Db,
				record.Bindings[i].Status,
				record.Bindings[i].CreateTime.String(),
				record.Bindings[i].UpdateTime.String(),
				record.Bindings[i].Charset,
				record.Bindings[i].Collation,
				record.Bindings[i].Source,
				record.Bindings[i].SQLDigest,
				record.Bindings[i].PlanDigest,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// DropGlobalBinding drops a BindRecord to the storage and BindRecord int the cache.
func (h *globalBindingHandle) DropGlobalBinding(originalSQL, db string, binding *Binding) (deletedRows uint64, err error) {
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		db = strings.ToLower(db)
		defer func() {
			if err != nil || deletedRows == 0 {
				return
			}

			record := &BindRecord{OriginalSQL: originalSQL, Db: db}
			if binding != nil {
				record.Bindings = append(record.Bindings, *binding)
			}
			h.removeGlobalCacheBinding(parser.DigestNormalized(originalSQL).String(), record)
		}()

		// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
		if err = h.lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3).String()

		if binding == nil {
			_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND status != %?`,
				deleted, updateTs, originalSQL, updateTs, deleted)
		} else {
			_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND bind_sql = %? and status != %?`,
				deleted, updateTs, originalSQL, updateTs, binding.BindSQL, deleted)
		}
		if err != nil {
			return err
		}
		deletedRows = sctx.GetSessionVars().StmtCtx.AffectedRows()
		return nil
	})
	return
}

// DropGlobalBindingByDigest drop BindRecord to the storage and BindRecord int the cache.
func (h *globalBindingHandle) DropGlobalBindingByDigest(sqlDigest string) (deletedRows uint64, err error) {
	oldRecord, err := h.GetGlobalBindingBySQLDigest(sqlDigest)
	if err != nil {
		return 0, err
	}
	return h.DropGlobalBinding(oldRecord.OriginalSQL, strings.ToLower(oldRecord.Db), nil)
}

// SetGlobalBindingStatus set a BindRecord's status to the storage and bind cache.
func (h *globalBindingHandle) SetGlobalBindingStatus(originalSQL string, binding *Binding, newStatus string) (ok bool, err error) {
	var (
		updateTs               types.Time
		oldStatus0, oldStatus1 string
		affectRows             int
	)
	if newStatus == Disabled {
		// For compatibility reasons, when we need to 'set binding disabled for <stmt>',
		// we need to consider both the 'enabled' and 'using' status.
		oldStatus0 = Using
		oldStatus1 = Enabled
	} else if newStatus == Enabled {
		// In order to unify the code, two identical old statuses are set.
		oldStatus0 = Disabled
		oldStatus1 = Disabled
	}

	err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		defer func() {
			if err != nil || affectRows == 0 {
				return
			}

			// The set binding status operation is success.
			ok = true
			record := &BindRecord{OriginalSQL: originalSQL}
			sqlDigest := parser.DigestNormalized(record.OriginalSQL)
			oldRecord := h.GetGlobalBinding(sqlDigest.String(), originalSQL, "")
			setBindingStatusInCacheSucc := false
			if oldRecord != nil && len(oldRecord.Bindings) > 0 {
				record.Bindings = make([]Binding, len(oldRecord.Bindings))
				copy(record.Bindings, oldRecord.Bindings)
				for ind, oldBinding := range record.Bindings {
					if oldBinding.Status == oldStatus0 || oldBinding.Status == oldStatus1 {
						if binding == nil || (binding != nil && oldBinding.isSame(binding)) {
							setBindingStatusInCacheSucc = true
							record.Bindings[ind].Status = newStatus
							record.Bindings[ind].UpdateTime = updateTs
						}
					}
				}
			}
			if setBindingStatusInCacheSucc {
				h.setGlobalCacheBinding(sqlDigest.String(), record)
			}
		}()

		// Lock mysql.bind_info to synchronize with SetBindingStatus on other tidb instances.
		if err = h.lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)
		updateTsStr := updateTs.String()

		if binding == nil {
			_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND status IN (%?, %?)`,
				newStatus, updateTsStr, originalSQL, updateTsStr, oldStatus0, oldStatus1)
		} else {
			_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %? AND bind_sql = %? AND status IN (%?, %?)`,
				newStatus, updateTsStr, originalSQL, updateTsStr, binding.BindSQL, oldStatus0, oldStatus1)
		}
		affectRows = int(sctx.GetSessionVars().StmtCtx.AffectedRows())
		return nil
	})
	return
}

// SetGlobalBindingStatusByDigest set a BindRecord's status to the storage and bind cache.
func (h *globalBindingHandle) SetGlobalBindingStatusByDigest(newStatus, sqlDigest string) (ok bool, err error) {
	oldRecord, err := h.GetGlobalBindingBySQLDigest(sqlDigest)
	if err != nil {
		return false, err
	}
	return h.SetGlobalBindingStatus(oldRecord.OriginalSQL, nil, newStatus)
}

// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
func (h *globalBindingHandle) GCGlobalBinding() (err error) {
	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBindRecord / AddBindRecord / DropBindRecord on other tidb instances.
		if err = h.lockBindInfoTable(sctx); err != nil {
			return err
		}

		// To make sure that all the deleted bind records have been acknowledged to all tidb,
		// we only garbage collect those records with update_time before 10 leases.
		updateTime := time.Now().Add(-(10 * Lease))
		updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 3).String()
		_, err = exec(sctx, `DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < %?`, updateTimeStr)
		return err
	})
}

// lockBindInfoTable simulates `LOCK TABLE mysql.bind_info WRITE` by acquiring a pessimistic lock on a
// special builtin row of mysql.bind_info. Note that this function must be called with h.sctx.Lock() held.
// We can replace this implementation to normal `LOCK TABLE mysql.bind_info WRITE` if that feature is
// generally available later.
// This lock would enforce the CREATE / DROP GLOBAL BINDING statements to be executed sequentially,
// even if they come from different tidb instances.
func (h *globalBindingHandle) lockBindInfoTable(sctx sessionctx.Context) error {
	// h.sctx already locked.
	_, err := exec(sctx, h.LockBindInfoSQL())
	return err
}

// LockBindInfoSQL simulates LOCK TABLE by updating a same row in each pessimistic transaction.
func (*globalBindingHandle) LockBindInfoSQL() string {
	sql, err := sqlescape.EscapeSQL("UPDATE mysql.bind_info SET source= %? WHERE original_sql= %?", Builtin, BuiltinPseudoSQL4BindLock)
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
				logutil.BgLogger().Debug("flush bind record failed", zap.String("category", "sql-bind"), zap.Error(err))
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

// DropInvalidGlobalBinding executes the drop BindRecord tasks.
func (h *globalBindingHandle) DropInvalidGlobalBinding() {
	h.invalidBindRecordMap.flushToStore()
}

// AddInvalidGlobalBinding adds BindRecord which needs to be deleted into invalidBindRecordMap.
func (h *globalBindingHandle) AddInvalidGlobalBinding(invalidBindRecord *BindRecord) {
	h.invalidBindRecordMap.Add(invalidBindRecord)
}

// Size returns the size of bind info cache.
func (h *globalBindingHandle) Size() int {
	size := len(h.getCache().GetAllBindings())
	return size
}

// GetGlobalBinding returns the BindRecord of the (normalizedSQL,db) if BindRecord exist.
func (h *globalBindingHandle) GetGlobalBinding(sqlDigest, normalizedSQL, db string) *BindRecord {
	return h.getCache().GetBinding(sqlDigest, normalizedSQL, db)
}

// GetGlobalBindingBySQLDigest returns the BindRecord of the sql digest.
func (h *globalBindingHandle) GetGlobalBindingBySQLDigest(sqlDigest string) (*BindRecord, error) {
	return h.getCache().GetBindingBySQLDigest(sqlDigest)
}

// GetAllGlobalBindings returns all bind records in cache.
func (h *globalBindingHandle) GetAllGlobalBindings() (bindRecords []*BindRecord) {
	return h.getCache().GetAllBindings()
}

// SetBindCacheCapacity reset the capacity for the bindCache.
// It will not affect already cached BindRecords.
func (h *globalBindingHandle) SetBindCacheCapacity(capacity int64) {
	h.getCache().SetMemCapacity(capacity)
}

// GetMemUsage returns the memory usage for the bind cache.
func (h *globalBindingHandle) GetMemUsage() (memUsage int64) {
	return h.getCache().GetMemUsage()
}

// GetMemCapacity returns the memory capacity for the bind cache.
func (h *globalBindingHandle) GetMemCapacity() (memCapacity int64) {
	return h.getCache().GetMemCapacity()
}

// newBindRecord builds BindRecord from a tuple in storage.
func newBindRecord(sctx sessionctx.Context, row chunk.Row) (string, *BindRecord, error) {
	status := row.GetString(3)
	// For compatibility, the 'Using' status binding will be converted to the 'Enabled' status binding.
	if status == Using {
		status = Enabled
	}
	binding := Binding{
		BindSQL:    row.GetString(1),
		Status:     status,
		CreateTime: row.GetTime(4),
		UpdateTime: row.GetTime(5),
		Charset:    row.GetString(6),
		Collation:  row.GetString(7),
		Source:     row.GetString(8),
		SQLDigest:  row.GetString(9),
		PlanDigest: row.GetString(10),
	}
	bindRecord := &BindRecord{
		OriginalSQL: row.GetString(0),
		Db:          strings.ToLower(row.GetString(2)),
		Bindings:    []Binding{binding},
	}
	sqlDigest := parser.DigestNormalized(bindRecord.OriginalSQL)
	sctx.GetSessionVars().CurrentDB = bindRecord.Db
	err := bindRecord.prepareHints(sctx)
	return sqlDigest.String(), bindRecord, err
}

// setGlobalCacheBinding sets the BindRecord to the cache, if there already exists a BindRecord,
// it will be overridden.
func (h *globalBindingHandle) setGlobalCacheBinding(sqlDigest string, meta *BindRecord) {
	newCache, err0 := h.getCache().Copy()
	if err0 != nil {
		logutil.BgLogger().Warn("BindHandle.setGlobalCacheBindRecord", zap.String("category", "sql-bind"), zap.Error(err0))
	}
	oldRecord := newCache.GetBinding(sqlDigest, meta.OriginalSQL, meta.Db)
	err1 := newCache.SetBinding(sqlDigest, meta)
	if err1 != nil && err0 == nil {
		logutil.BgLogger().Warn("BindHandle.setGlobalCacheBindRecord", zap.String("category", "sql-bind"), zap.Error(err1))
	}
	h.setCache(newCache) // TODO: update it in place
	updateMetrics(metrics.ScopeGlobal, oldRecord, meta, false)
}

// removeGlobalCacheBinding removes the BindRecord from the cache.
func (h *globalBindingHandle) removeGlobalCacheBinding(sqlDigest string, meta *BindRecord) {
	newCache, err := h.getCache().Copy()
	if err != nil {
		logutil.BgLogger().Warn("", zap.String("category", "sql-bind"), zap.Error(err))
	}
	oldRecord := newCache.GetBinding(sqlDigest, meta.OriginalSQL, meta.Db)
	newCache.RemoveBinding(sqlDigest, meta)
	h.setCache(newCache) // TODO: update it in place
	updateMetrics(metrics.ScopeGlobal, oldRecord, newCache.GetBinding(sqlDigest, meta.OriginalSQL, meta.Db), false)
}

func copyBindRecordUpdateMap(oldMap map[string]*bindRecordUpdate) map[string]*bindRecordUpdate {
	newMap := make(map[string]*bindRecordUpdate, len(oldMap))
	maps.Copy(newMap, oldMap)
	return newMap
}

func getHintsForSQL(sctx sessionctx.Context, sql string) (string, error) {
	origVals := sctx.GetSessionVars().UsePlanBaselines
	sctx.GetSessionVars().UsePlanBaselines = false

	// Usually passing a sprintf to ExecuteInternal is not recommended, but in this case
	// it is safe because ExecuteInternal does not permit MultiStatement execution. Thus,
	// the statement won't be able to "break out" from EXPLAIN.
	rs, err := exec(sctx, fmt.Sprintf("EXPLAIN FORMAT='hint' %s", sql))
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
func GenerateBindSQL(ctx context.Context, stmtNode ast.StmtNode, planHint string, skipCheckIfHasParam bool, defaultDB string) string {
	// If would be nil for very simple cases such as point get, we do not need to evolve for them.
	if planHint == "" {
		return ""
	}
	if !skipCheckIfHasParam {
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
			if err := n.With.Restore(restoreCtx); err != nil {
				logutil.BgLogger().Debug("restore SQL failed", zap.String("category", "sql-bind"), zap.Error(err))
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
	logutil.Logger(ctx).Debug("unexpected statement type when generating bind SQL", zap.String("category", "sql-bind"), zap.Any("statement", stmtNode))
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

func (*paramMarkerChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// Clear resets the bind handle. It is only used for test.
func (h *globalBindingHandle) Clear() {
	h.setCache(newBindCache())
	h.setLastUpdateTime(types.ZeroTimestamp)
	h.invalidBindRecordMap.Store(make(map[string]*bindRecordUpdate))
}

// FlushGlobalBindings flushes the BindRecord in temp maps to storage and loads them into cache.
func (h *globalBindingHandle) FlushGlobalBindings() error {
	h.DropInvalidGlobalBinding()
	return h.Update(false)
}

// ReloadGlobalBindings clears existing binding cache and do a full load from mysql.bind_info.
// It is used to maintain consistency between cache and mysql.bind_info if the table is deleted or truncated.
func (h *globalBindingHandle) ReloadGlobalBindings() error {
	h.setCache(newBindCache())
	h.setLastUpdateTime(types.ZeroTimestamp)
	return h.Update(true)
}

func (h *globalBindingHandle) callWithSCtx(wrapTxn bool, f func(sctx sessionctx.Context) error) (err error) {
	resource, err := h.sPool.Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			h.sPool.Put(resource)
		}
	}()

	sctx := resource.(sessionctx.Context)
	if wrapTxn {
		if _, err = exec(sctx, "BEGIN PESSIMISTIC"); err != nil {
			return
		}
		defer func() {
			if err == nil {
				_, err = exec(sctx, "COMMIT")
			} else {
				_, err1 := exec(sctx, "ROLLBACK")
				terror.Log(errors.Trace(err1))
			}
		}()
	}

	err = f(sctx)
	return
}

var (
	lastPlanBindingUpdateTime = "last_plan_binding_update_time"
)

// GetScope gets the status variables scope.
func (*globalBindingHandle) GetScope(_ string) variable.ScopeFlag {
	return variable.ScopeSession
}

// Stats returns the server statistics.
func (h *globalBindingHandle) Stats(_ *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[lastPlanBindingUpdateTime] = h.getLastUpdateTime().String()
	return m, nil
}
