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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// globalVarCacheRefreshInterval is the same as the sysvar cache of a Domain.
const globalVarCacheRefreshInterval = 30 * time.Second

// globalVarCache caches the global variables of a keyspace for the sessions of
// its runtime, which have no Domain to cache them. Like the sysvar cache of a
// Domain, it reloads them periodically and when a TiDB of the keyspace notifies
// a change, but it never applies them to this instance.
type globalVarCache struct {
	ks      string
	factory pools.Factory
	ctx     context.Context
	cancel  context.CancelFunc

	// sessMu serializes the use of se, which reads the tables of the keyspace.
	// It is created on first use, as creating a session needs the runtime to be
	// registered. Its own lookups take the defaults, so a load never loads again.
	sessMu sync.Mutex
	se     sessionctx.Context
	closed bool

	mu     sync.RWMutex
	global map[string]string
}

func newGlobalVarCache(ks string, factory pools.Factory) *globalVarCache {
	ctx, cancel := context.WithCancel(context.Background())
	return &globalVarCache{ks: ks, factory: factory, ctx: ctx, cancel: cancel}
}

// wrap makes the sessions that factory creates read global variables from c.
func (c *globalVarCache) wrap(factory pools.Factory) pools.Factory {
	return withGlobalVars(factory, func(vars *variable.SessionVars) variable.GlobalVarAccessor {
		return &keyspaceGlobalVars{GlobalVarAccessor: vars.GlobalVarsAccessor, vars: vars, cache: c}
	})
}

func (c *globalVarCache) get(name string) (value string, found bool, err error) {
	c.mu.RLock()
	loaded := c.global != nil
	value, found = c.global[name]
	c.mu.RUnlock()
	if loaded {
		return value, found, nil
	}
	if err = c.load(); err != nil {
		return "", false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, found = c.global[name]
	return value, found, nil
}

// load reloads the cache. It updates the cache while holding the session, so
// that an earlier load cannot overwrite a later one.
func (c *globalVarCache) load() error {
	return c.withSession(func(se sessionctx.Context) error {
		ctx := kv.WithInternalSourceType(c.ctx, kv.InternalTxnSysVar)
		rows, _, err := se.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
			"SELECT variable_name, variable_value FROM %n.%n", mysql.SystemDB, mysql.GlobalVariablesTable)
		if err != nil {
			return err
		}
		tableContents := make(map[string]string, len(rows))
		for _, row := range rows {
			tableContents[row.GetString(0)] = row.GetString(1)
		}
		_, global := variable.SysVarCacheValues(tableContents)
		c.mu.Lock()
		c.global = global
		c.mu.Unlock()
		return nil
	})
}

func (c *globalVarCache) readTiDBTable(name string) (value string, found bool, err error) {
	err = c.withSession(func(se sessionctx.Context) error {
		ctx := kv.WithInternalSourceType(c.ctx, kv.InternalTxnSysVar)
		rows, _, err := se.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
			"SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, mysql.TiDBTable, name)
		if err != nil || len(rows) == 0 {
			return err
		}
		value, found = rows[0].GetString(0), true
		return nil
	})
	return value, found, err
}

func (c *globalVarCache) withSession(fn func(sessionctx.Context) error) error {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	if c.closed {
		return errors.Errorf("global variables of keyspace %s are closed", c.ks)
	}
	if c.se == nil {
		resource, err := withGlobalVars(c.factory, func(vars *variable.SessionVars) variable.GlobalVarAccessor {
			return defaultGlobalVars{vars.GlobalVarsAccessor}
		})()
		if err != nil {
			return errors.Trace(err)
		}
		se, ok := resource.(sessionctx.Context)
		if !ok {
			resource.Close()
			return errors.Errorf("unexpected session type %T", resource)
		}
		c.se = se
	}
	return fn(c.se)
}

// refreshLoop reloads the cache until ctx is done, and closes the cache then.
func (c *globalVarCache) refreshLoop(ctx context.Context, etcdCli *clientv3.Client) {
	defer c.close()
	logger := logutil.BgLogger().With(zap.String("targetKS", c.ks))
	var watchCh clientv3.WatchChan
	if etcdCli != nil {
		watchCh = etcdCli.Watch(ctx, ddlutil.SysVarCacheKey)
	}
	var count int
	for {
		ok := true
		select {
		case <-ctx.Done():
			return
		case _, ok = <-watchCh:
		case <-time.After(globalVarCacheRefreshInterval):
		}
		if !ok {
			logger.Warn("global variable cache watch channel closed")
			watchCh = etcdCli.Watch(ctx, ddlutil.SysVarCacheKey)
			count++
			if count > 10 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(count) * time.Second):
				}
			}
			continue
		}
		count = 0
		if err := c.load(); err != nil {
			logger.Warn("reload global variable cache failed", zap.Error(err))
		}
	}
}

func (c *globalVarCache) close() {
	c.cancel()
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.closed = true
	if r, ok := c.se.(pools.Resource); ok {
		r.Close()
	}
	c.se = nil
}

func withGlobalVars(factory pools.Factory, accessor func(*variable.SessionVars) variable.GlobalVarAccessor) pools.Factory {
	return func() (pools.Resource, error) {
		r, err := factory()
		if err != nil {
			return nil, err
		}
		if sctx, ok := r.(sessionctx.Context); ok {
			vars := sctx.GetSessionVars() //nolint:forbidigo
			vars.GlobalVarsAccessor = accessor(vars)
		}
		return r, nil
	}
}

// keyspaceGlobalVars reads global variables of the keyspace from the cache and
// mysql.tidb, and leaves the other methods to the session.
type keyspaceGlobalVars struct {
	variable.GlobalVarAccessor
	vars  *variable.SessionVars
	cache *globalVarCache
}

// GetGlobalSysVar implements variable.GlobalVarAccessor. Like
// session.GetGlobalSysVar, it returns the default when the value does not fit
// the variable, but returns an error when the cache fails to load.
func (g *keyspaceGlobalVars) GetGlobalSysVar(name string) (string, error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	value, found, err := g.cache.get(name)
	if err != nil {
		return "", err
	}
	// A variable registered after the last load is not cached yet.
	if !found {
		return sv.Value, nil
	}
	// A value written by an earlier version may not fit the variable.
	if value, err = sv.ValidateFromType(g.vars, value, vardef.ScopeGlobal); err != nil {
		return sv.Value, nil
	}
	return value, nil
}

// GetTiDBTableValue implements variable.GlobalVarAccessor.
func (g *keyspaceGlobalVars) GetTiDBTableValue(name string) (string, error) {
	value, found, err := g.cache.readTiDBTable(name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.Errorf("%s not found in %s.%s", name, mysql.SystemDB, mysql.TiDBTable)
	}
	return value, nil
}

// defaultGlobalVars is the accessor of the session that loads the cache.
type defaultGlobalVars struct {
	variable.GlobalVarAccessor
}

// GetGlobalSysVar implements variable.GlobalVarAccessor.
func (defaultGlobalVars) GetGlobalSysVar(name string) (string, error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return sv.Value, nil
}

// GetTiDBTableValue implements variable.GlobalVarAccessor.
func (defaultGlobalVars) GetTiDBTableValue(name string) (string, error) {
	return "", errors.Errorf("%s is not read by the session that loads global variables", name)
}
