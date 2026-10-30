// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crossks

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// globalVarCacheRefreshInterval is the same as the sysvar cache of a Domain.
const globalVarCacheRefreshInterval = 30 * time.Second

// globalVarCache is the global variable accessor of the sessions of a runtime,
// which have no Domain to read global variables, and only read them. Like the
// sysvar cache of a Domain, it loads the variables through a session of the
// pool, but it never applies them to this instance. It loads them on the first
// read, and from then on reloads them in the background periodically and when
// a TiDB of the keyspace notifies a change, keeping the old values if a reload
// fails. A runtime that never reads them never queries or watches etcd.
//
// It only covers the reads through the GlobalVarsAccessor. A variable with a
// GetGlobal hook, and the code that reads the instance level value in vardef
// directly, still see the value of this instance, since they assume that an
// instance serves only one keyspace.
type globalVarCache struct {
	ctx     context.Context
	ks      string
	pool    util.SessionPool
	etcdCli *clientv3.Client

	// firstLoaded is closed when the cache is first loaded, which starts the refresh.
	firstLoaded chan struct{}
	loadedOnce  sync.Once

	// loadMu serializes loads, so that an earlier load cannot overwrite a later one.
	loadMu sync.Mutex
	mu     sync.RWMutex
	global map[string]string
}

var _ variable.GlobalVarAccessor = (*globalVarCache)(nil)

func newGlobalVarCache(ctx context.Context, ks string, pool util.SessionPool, etcdCli *clientv3.Client) *globalVarCache {
	return &globalVarCache{ctx: ctx, ks: ks, pool: pool, etcdCli: etcdCli, firstLoaded: make(chan struct{})}
}

func (c *globalVarCache) get(name string) (value string, found bool, err error) {
	global := c.cached()
	if global == nil {
		if global, err = c.loadIfEmpty(); err != nil {
			return "", false, err
		}
		c.loadedOnce.Do(func() { close(c.firstLoaded) })
	}
	value, found = global[name]
	return value, found, nil
}

func (c *globalVarCache) cached() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.global
}

// loadIfEmpty loads the cache unless another reader loaded it while this one
// waited for loadMu, so that readers of a cold cache load it only once.
func (c *globalVarCache) loadIfEmpty() (map[string]string, error) {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	if global := c.cached(); global != nil {
		return global, nil
	}
	return c.loadLocked()
}

// load reloads the cache. A failed load keeps the cache.
//
// The session that loads the cache reads global variables from the cache too,
// so the query must not read any global variable, or it waits for loadMu that
// it holds. The sysvar cache of a Domain relies on the same.
func (c *globalVarCache) load() (map[string]string, error) {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	return c.loadLocked()
}

func (c *globalVarCache) loadLocked() (map[string]string, error) {
	rows, err := c.query("SELECT variable_name, variable_value FROM %n.%n", mysql.SystemDB, mysql.GlobalVariablesTable)
	if err != nil {
		return nil, err
	}
	tableContents := make(map[string]string, len(rows))
	for _, row := range rows {
		tableContents[row.GetString(0)] = row.GetString(1)
	}
	_, global := variable.SysVarCacheValues(tableContents)
	vars := variable.NewSessionVars(nil)
	for name, value := range global {
		sv := variable.GetSysVar(name)
		if sv == nil {
			continue
		}
		// A value written by an earlier version may not fit the variable.
		if global[name], err = sv.ValidateFromType(vars, value, vardef.ScopeGlobal); err != nil {
			global[name] = sv.Value
		}
	}
	c.mu.Lock()
	c.global = global
	c.mu.Unlock()
	return global, nil
}

func (c *globalVarCache) query(sql string, args ...any) ([]chunk.Row, error) {
	resource, err := c.pool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer c.pool.Put(resource)
	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		return nil, errors.Errorf("unexpected session type %T", resource)
	}
	ctx := kv.WithInternalSourceType(c.ctx, kv.InternalTxnSysVar)
	rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
	return rows, err
}

// refreshLoop reloads the cache until ctx is done, starting when the cache is
// first loaded. It follows Domain.LoadSysVarCacheLoop.
func (c *globalVarCache) refreshLoop() {
	select {
	case <-c.ctx.Done():
		return
	case <-c.firstLoaded:
	}
	etcdCli := c.etcdCli
	logger := logutil.BgLogger().With(zap.String("targetKS", c.ks))
	var watchCh clientv3.WatchChan
	if etcdCli != nil {
		watchCh = etcdCli.Watch(c.ctx, ddlutil.SysVarCacheKey)
	}
	var count int
	for {
		ok := true
		select {
		case <-c.ctx.Done():
			return
		case _, ok = <-watchCh:
		case <-time.After(globalVarCacheRefreshInterval):
		}
		if !ok {
			logger.Warn("global variable cache watch channel closed")
			watchCh = etcdCli.Watch(c.ctx, ddlutil.SysVarCacheKey)
			count++
			if count > 10 {
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(time.Duration(count) * time.Second):
				}
			}
			continue
		}
		count = 0
		if _, err := c.load(); err != nil {
			logger.Warn("reload global variable cache failed", zap.Error(err))
		}
	}
}

// GetGlobalSysVar implements variable.GlobalVarAccessor. It returns an error
// when the cache fails to load.
func (c *globalVarCache) GetGlobalSysVar(name string) (string, error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	value, found, err := c.get(name)
	if err != nil {
		return "", err
	}
	// A variable registered after the last load is not cached yet.
	if !found {
		return sv.Value, nil
	}
	return value, nil
}

// GetTiDBTableValue implements variable.GlobalVarAccessor.
func (c *globalVarCache) GetTiDBTableValue(name string) (string, error) {
	rows, err := c.query("SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, mysql.TiDBTable, name)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.Errorf("%s not found in %s.%s", name, mysql.SystemDB, mysql.TiDBTable)
	}
	return rows[0].GetString(0), nil
}

// SetGlobalSysVar implements variable.GlobalVarAccessor.
func (c *globalVarCache) SetGlobalSysVar(_ context.Context, name string, _ string) error {
	return c.errReadOnly(name)
}

// SetInstanceSysVar implements variable.GlobalVarAccessor.
func (c *globalVarCache) SetInstanceSysVar(_ context.Context, name string, _ string) error {
	return c.errReadOnly(name)
}

// SetGlobalSysVarOnly implements variable.GlobalVarAccessor.
func (c *globalVarCache) SetGlobalSysVarOnly(_ context.Context, name string, _ string, _ bool) error {
	return c.errReadOnly(name)
}

// SetTiDBTableValue implements variable.GlobalVarAccessor.
func (c *globalVarCache) SetTiDBTableValue(name, _, _ string) error {
	return c.errReadOnly(name)
}

func (c *globalVarCache) errReadOnly(name string) error {
	return errors.Errorf("cannot set %s from a session of the runtime of keyspace %s", name, c.ks)
}
