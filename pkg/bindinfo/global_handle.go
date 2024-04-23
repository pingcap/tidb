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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/bindinfo/internal/logutil"
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
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// GlobalBindingHandle is used to handle all global sql bind operations.
type GlobalBindingHandle interface {
	// Methods for create, get, drop global sql bindings.

	// MatchGlobalBinding returns the matched binding for this statement.
	MatchGlobalBinding(sctx sessionctx.Context, fuzzyDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool)

	// GetAllGlobalBindings returns all bind records in cache.
	GetAllGlobalBindings() (bindings Bindings)

	// CreateGlobalBinding creates a Bindings to the storage and the cache.
	// It replaces all the exists bindings for the same normalized SQL.
	CreateGlobalBinding(sctx sessionctx.Context, binding Binding) (err error)

	// DropGlobalBinding drop Bindings to the storage and Bindings int the cache.
	DropGlobalBinding(sqlDigest string) (deletedRows uint64, err error)

	// SetGlobalBindingStatus set a Bindings's status to the storage and bind cache.
	SetGlobalBindingStatus(newStatus, sqlDigest string) (ok bool, err error)

	// AddInvalidGlobalBinding adds Bindings which needs to be deleted into invalidBindingCache.
	AddInvalidGlobalBinding(invalidBinding Binding)

	// DropInvalidGlobalBinding executes the drop Bindings tasks.
	DropInvalidGlobalBinding()

	// Methods for load and clear global sql bindings.

	// Reset is to reset the BindHandle and clean old info.
	Reset()

	// LoadFromStorageToCache loads global bindings from storage to the memory cache.
	LoadFromStorageToCache(fullLoad bool) (err error)

	// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
	GCGlobalBinding() (err error)

	// Methods for memory control.

	// Size returns the size of bind info cache.
	Size() int

	// SetBindingCacheCapacity reset the capacity for the bindingCache.
	SetBindingCacheCapacity(capacity int64)

	// GetMemUsage returns the memory usage for the bind cache.
	GetMemUsage() (memUsage int64)

	// GetMemCapacity returns the memory capacity for the bind cache.
	GetMemCapacity() (memCapacity int64)

	// Clear resets the bind handle. It is only used for test.
	Clear()

	// FlushGlobalBindings flushes the Bindings in temp maps to storage and loads them into cache.
	FlushGlobalBindings() error

	// Methods for Auto Capture.

	// CaptureBaselines is used to automatically capture plan baselines.
	CaptureBaselines()

	variable.Statistics
}

// globalBindingHandle is used to handle all global sql bind operations.
type globalBindingHandle struct {
	sPool SessionPool

	fuzzyBindingCache atomic.Value

	// lastTaskTime records the last update time for the global sql bind cache.
	// This value is used to avoid reload duplicated bindings from storage.
	lastUpdateTime atomic.Value

	// invalidBindings indicates the invalid bindings found during querying.
	// A binding will be deleted from this map, after 2 bind-lease, after it is dropped from the kv.
	invalidBindings *invalidBindingCache

	// syncBindingSingleflight is used to synchronize the execution of `LoadFromStorageToCache` method.
	syncBindingSingleflight singleflight.Group
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

	// LockBindInfoSQL simulates LOCK TABLE by updating a same row in each pessimistic transaction.
	LockBindInfoSQL = `UPDATE mysql.bind_info SET source= 'builtin' WHERE original_sql= 'builtin_pseudo_sql_for_bind_lock'`

	// StmtRemoveDuplicatedPseudoBinding is used to remove duplicated pseudo binding.
	// After using BR to sync bind_info between two clusters, the pseudo binding may be duplicated, and
	// BR use this statement to remove duplicated rows, and this SQL should only be executed by BR.
	StmtRemoveDuplicatedPseudoBinding = `DELETE FROM mysql.bind_info
       WHERE original_sql='builtin_pseudo_sql_for_bind_lock' AND
       _tidb_rowid NOT IN ( -- keep one arbitrary pseudo binding
         SELECT _tidb_rowid FROM mysql.bind_info WHERE original_sql='builtin_pseudo_sql_for_bind_lock' limit 1)`
)

// NewGlobalBindingHandle creates a new GlobalBindingHandle.
func NewGlobalBindingHandle(sPool SessionPool) GlobalBindingHandle {
	handle := &globalBindingHandle{sPool: sPool}
	handle.Reset()
	return handle
}

func (h *globalBindingHandle) getCache() FuzzyBindingCache {
	return h.fuzzyBindingCache.Load().(FuzzyBindingCache)
}

func (h *globalBindingHandle) setCache(c FuzzyBindingCache) {
	// TODO: update the global cache in-place instead of replacing it and remove this function.
	h.fuzzyBindingCache.Store(c)
}

// Reset is to reset the BindHandle and clean old info.
func (h *globalBindingHandle) Reset() {
	h.lastUpdateTime.Store(types.ZeroTimestamp)
	h.invalidBindings = newInvalidBindingCache()
	h.setCache(newFuzzyBindingCache(h.LoadBindingsFromStorage))
	variable.RegisterStatistics(h)
}

func (h *globalBindingHandle) getLastUpdateTime() types.Time {
	return h.lastUpdateTime.Load().(types.Time)
}

func (h *globalBindingHandle) setLastUpdateTime(t types.Time) {
	h.lastUpdateTime.Store(t)
}

// LoadFromStorageToCache loads bindings from the storage into the cache.
func (h *globalBindingHandle) LoadFromStorageToCache(fullLoad bool) (err error) {
	var lastUpdateTime types.Time
	var timeCondition string
	var newCache FuzzyBindingCache
	if fullLoad {
		lastUpdateTime = types.ZeroTimestamp
		timeCondition = ""
		newCache = newFuzzyBindingCache(h.LoadBindingsFromStorage)
	} else {
		lastUpdateTime = h.getLastUpdateTime()
		timeCondition = fmt.Sprintf("WHERE update_time>'%s'", lastUpdateTime.String())
		newCache, err = h.getCache().Copy()
		if err != nil {
			return err
		}
	}

	selectStmt := fmt.Sprintf(`SELECT original_sql, bind_sql, default_db, status, create_time,
       update_time, charset, collation, source, sql_digest, plan_digest FROM mysql.bind_info
       %s ORDER BY update_time, create_time`, timeCondition)

	return h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, selectStmt)
		if err != nil {
			return err
		}

		defer func() {
			h.setLastUpdateTime(lastUpdateTime)
			h.setCache(newCache)

			metrics.BindingCacheMemUsage.Set(float64(h.GetMemUsage()))
			metrics.BindingCacheMemLimit.Set(float64(h.GetMemCapacity()))
			metrics.BindingCacheNumBindings.Set(float64(h.Size()))
		}()

		for _, row := range rows {
			// Skip the builtin record which is designed for binding synchronization.
			if row.GetString(0) == BuiltinPseudoSQL4BindLock {
				continue
			}
			sqlDigest, binding, err := newBinding(sctx, row)

			// Update lastUpdateTime to the newest one.
			// Even if this one is an invalid bind.
			if binding.UpdateTime.Compare(lastUpdateTime) > 0 {
				lastUpdateTime = binding.UpdateTime
			}

			if err != nil {
				logutil.BindLogger().Warn("failed to generate bind record from data row", zap.Error(err))
				continue
			}

			oldBinding := newCache.GetBinding(sqlDigest)
			newBinding := removeDeletedBindings(merge(oldBinding, []Binding{binding}))
			if len(newBinding) > 0 {
				err = newCache.SetBinding(sqlDigest, newBinding)
				if err != nil {
					// When the memory capacity of bing_cache is not enough,
					// there will be some memory-related errors in multiple places.
					// Only needs to be handled once.
					logutil.BindLogger().Warn("BindHandle.Update", zap.Error(err))
				}
			} else {
				newCache.RemoveBinding(sqlDigest)
			}
		}
		return nil
	})
}

// CreateGlobalBinding creates a Bindings to the storage and the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (h *globalBindingHandle) CreateGlobalBinding(sctx sessionctx.Context, binding Binding) (err error) {
	if err := prepareHints(sctx, &binding); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()

	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)

		updateTs := now.String()
		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?`,
			deleted, updateTs, binding.OriginalSQL, updateTs)
		if err != nil {
			return err
		}

		binding.CreateTime = now
		binding.UpdateTime = now

		// Insert the Bindings to the storage.
		_, err = exec(sctx, `INSERT INTO mysql.bind_info VALUES (%?,%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
			binding.OriginalSQL,
			binding.BindSQL,
			strings.ToLower(binding.Db),
			binding.Status,
			binding.CreateTime.String(),
			binding.UpdateTime.String(),
			binding.Charset,
			binding.Collation,
			binding.Source,
			binding.SQLDigest,
			binding.PlanDigest,
		)
		if err != nil {
			return err
		}
		return nil
	})
}

// dropGlobalBinding drops a Bindings to the storage and Bindings int the cache.
func (h *globalBindingHandle) dropGlobalBinding(sqlDigest string) (deletedRows uint64, err error) {
	err = h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3).String()

		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status != %?`,
			deleted, updateTs, sqlDigest, updateTs, deleted)
		if err != nil {
			return err
		}
		deletedRows = sctx.GetSessionVars().StmtCtx.AffectedRows()
		return nil
	})
	return
}

// DropGlobalBinding drop Bindings to the storage and Bindings int the cache.
func (h *globalBindingHandle) DropGlobalBinding(sqlDigest string) (deletedRows uint64, err error) {
	if sqlDigest == "" {
		return 0, errors.New("sql digest is empty")
	}
	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()
	return h.dropGlobalBinding(sqlDigest)
}

// SetGlobalBindingStatus set a Bindings's status to the storage and bind cache.
func (h *globalBindingHandle) SetGlobalBindingStatus(newStatus, sqlDigest string) (ok bool, err error) {
	var (
		updateTs               types.Time
		oldStatus0, oldStatus1 string
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

	defer func() {
		if err == nil {
			err = h.LoadFromStorageToCache(false)
		}
	}()

	err = h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with SetBindingStatus on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 3)
		updateTsStr := updateTs.String()

		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status IN (%?, %?)`,
			newStatus, updateTsStr, sqlDigest, updateTsStr, oldStatus0, oldStatus1)
		return err
	})
	return
}

// GCGlobalBinding physically removes the deleted bind records in mysql.bind_info.
func (h *globalBindingHandle) GCGlobalBinding() (err error) {
	return h.callWithSCtx(true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
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
func lockBindInfoTable(sctx sessionctx.Context) error {
	// h.sctx already locked.
	_, err := exec(sctx, LockBindInfoSQL)
	return err
}

// invalidBindingCache is used to store invalid bindings temporarily.
type invalidBindingCache struct {
	mu sync.RWMutex
	m  map[string]Binding // key: sqlDigest
}

func newInvalidBindingCache() *invalidBindingCache {
	return &invalidBindingCache{
		m: make(map[string]Binding),
	}
}

func (c *invalidBindingCache) add(binding Binding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[binding.SQLDigest] = binding
}

func (c *invalidBindingCache) getAll() Bindings {
	c.mu.Lock()
	defer c.mu.Unlock()
	bindings := make(Bindings, 0, len(c.m))
	for _, binding := range c.m {
		bindings = append(bindings, binding)
	}
	return bindings
}

func (c *invalidBindingCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m = make(map[string]Binding)
}

// DropInvalidGlobalBinding executes the drop Bindings tasks.
func (h *globalBindingHandle) DropInvalidGlobalBinding() {
	defer func() {
		if err := h.LoadFromStorageToCache(false); err != nil {
			logutil.BindLogger().Warn("drop invalid global binding error", zap.Error(err))
		}
	}()

	invalidBindings := h.invalidBindings.getAll()
	h.invalidBindings.reset()
	for _, invalidBinding := range invalidBindings {
		if _, err := h.dropGlobalBinding(invalidBinding.SQLDigest); err != nil {
			logutil.BindLogger().Debug("flush bind record failed", zap.Error(err))
		}
	}
}

// AddInvalidGlobalBinding adds Bindings which needs to be deleted into invalidBindings.
func (h *globalBindingHandle) AddInvalidGlobalBinding(invalidBinding Binding) {
	h.invalidBindings.add(invalidBinding)
}

// Size returns the size of bind info cache.
func (h *globalBindingHandle) Size() int {
	size := len(h.getCache().GetAllBindings())
	return size
}

// MatchGlobalBinding returns the matched binding for this statement.
func (h *globalBindingHandle) MatchGlobalBinding(sctx sessionctx.Context, fuzzyDigest string, tableNames []*ast.TableName) (matchedBinding Binding, isMatched bool) {
	return h.getCache().FuzzyMatchingBinding(sctx, fuzzyDigest, tableNames)
}

// GetAllGlobalBindings returns all bind records in cache.
func (h *globalBindingHandle) GetAllGlobalBindings() (bindings Bindings) {
	return h.getCache().GetAllBindings()
}

// SetBindingCacheCapacity reset the capacity for the bindingCache.
// It will not affect already cached Bindings.
func (h *globalBindingHandle) SetBindingCacheCapacity(capacity int64) {
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

// newBinding builds Bindings from a tuple in storage.
func newBinding(sctx sessionctx.Context, row chunk.Row) (string, Binding, error) {
	status := row.GetString(3)
	// For compatibility, the 'Using' status binding will be converted to the 'Enabled' status binding.
	if status == Using {
		status = Enabled
	}
	binding := Binding{
		OriginalSQL: row.GetString(0),
		Db:          strings.ToLower(row.GetString(2)),
		BindSQL:     row.GetString(1),
		Status:      status,
		CreateTime:  row.GetTime(4),
		UpdateTime:  row.GetTime(5),
		Charset:     row.GetString(6),
		Collation:   row.GetString(7),
		Source:      row.GetString(8),
		SQLDigest:   row.GetString(9),
		PlanDigest:  row.GetString(10),
	}
	sqlDigest := parser.DigestNormalized(binding.OriginalSQL)
	err := prepareHints(sctx, &binding)
	sctx.GetSessionVars().CurrentDB = binding.Db
	return sqlDigest.String(), binding, err
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

// GenerateBindingSQL generates binding sqls from stmt node and plan hints.
func GenerateBindingSQL(stmtNode ast.StmtNode, planHint string, skipCheckIfHasParam bool, defaultDB string) string {
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
				logutil.BindLogger().Debug("restore SQL failed", zap.Error(err))
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
	logutil.BindLogger().Debug("unexpected statement type when generating bind SQL", zap.Any("statement", stmtNode))
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
	h.setCache(newFuzzyBindingCache(h.LoadBindingsFromStorage))
	h.setLastUpdateTime(types.ZeroTimestamp)
	h.invalidBindings.reset()
}

// FlushGlobalBindings flushes the Bindings in temp maps to storage and loads them into cache.
func (h *globalBindingHandle) FlushGlobalBindings() error {
	h.DropInvalidGlobalBinding()
	return h.LoadFromStorageToCache(false)
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
func (h *globalBindingHandle) Stats(_ *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any)
	m[lastPlanBindingUpdateTime] = h.getLastUpdateTime().String()
	return m, nil
}

// LoadBindingsFromStorageToCache loads global bindings from storage to the memory cache.
func (h *globalBindingHandle) LoadBindingsFromStorage(sctx sessionctx.Context, sqlDigest string) (Bindings, error) {
	if sqlDigest == "" {
		return nil, nil
	}
	timeout := time.Duration(sctx.GetSessionVars().LoadBindingTimeout) * time.Millisecond
	resultChan := h.syncBindingSingleflight.DoChan(sqlDigest, func() (any, error) {
		return h.loadBindingsFromStorageInternal(sqlDigest)
	})
	select {
	case result := <-resultChan:
		if result.Err != nil {
			return nil, result.Err
		}
		bindings := result.Val
		if bindings == nil {
			return nil, nil
		}
		return bindings.(Bindings), nil
	case <-time.After(timeout):
		return nil, errors.New("load bindings from storage timeout")
	}
}

func (h *globalBindingHandle) loadBindingsFromStorageInternal(sqlDigest string) (any, error) {
	failpoint.Inject("load_bindings_from_storage_internal_timeout", func() {
		time.Sleep(time.Second)
	})
	var bindings Bindings
	selectStmt := fmt.Sprintf("SELECT original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest FROM mysql.bind_info where sql_digest = '%s'", sqlDigest)
	err := h.callWithSCtx(false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, selectStmt)
		if err != nil {
			return err
		}
		bindings = make([]Binding, 0, len(rows))
		for _, row := range rows {
			// Skip the builtin record which is designed for binding synchronization.
			if row.GetString(0) == BuiltinPseudoSQL4BindLock {
				continue
			}
			_, binding, err := newBinding(sctx, row)
			if err != nil {
				logutil.BindLogger().Warn("failed to generate bind record from data row", zap.Error(err))
				continue
			}
			bindings = append(bindings, binding)
		}
		return nil
	})
	return bindings, err
}
