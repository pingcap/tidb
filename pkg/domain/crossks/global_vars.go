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
// read after it is invalidated periodically or by a change notified by a TiDB
// of the keyspace, so a runtime that never reads them never queries.
type globalVarCache struct {
	ctx  context.Context
	ks   string
	pool util.SessionPool

	// loadMu serializes loads.
	loadMu sync.Mutex
	mu     sync.RWMutex
	global map[string]string
	// version increases on each invalidation, so that a load started before an
	// invalidation does not fill the cache.
	version uint64
}

var _ variable.GlobalVarAccessor = (*globalVarCache)(nil)

func newGlobalVarCache(ctx context.Context, ks string, pool util.SessionPool) *globalVarCache {
	return &globalVarCache{ctx: ctx, ks: ks, pool: pool}
}

func (c *globalVarCache) get(name string) (value string, found bool, err error) {
	c.mu.RLock()
	global := c.global
	c.mu.RUnlock()
	if global == nil {
		if global, err = c.load(); err != nil {
			return "", false, err
		}
	}
	value, found = global[name]
	return value, found, nil
}

func (c *globalVarCache) load() (map[string]string, error) {
	c.loadMu.Lock()
	defer c.loadMu.Unlock()
	c.mu.RLock()
	global, version := c.global, c.version
	c.mu.RUnlock()
	if global != nil {
		return global, nil
	}
	rows, err := c.query("SELECT variable_name, variable_value FROM %n.%n", mysql.SystemDB, mysql.GlobalVariablesTable)
	if err != nil {
		return nil, err
	}
	tableContents := make(map[string]string, len(rows))
	for _, row := range rows {
		tableContents[row.GetString(0)] = row.GetString(1)
	}
	_, global = variable.SysVarCacheValues(tableContents)
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
	if c.version == version {
		c.global = global
	}
	c.mu.Unlock()
	return global, nil
}

func (c *globalVarCache) invalidate() {
	c.mu.Lock()
	c.global = nil
	c.version++
	c.mu.Unlock()
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

// refreshLoop invalidates the cache until ctx is done.
func (c *globalVarCache) refreshLoop(etcdCli *clientv3.Client) {
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
		c.invalidate()
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
