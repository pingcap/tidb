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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// globalVarReader reads the global variables of a keyspace for the sessions of
// its runtime, which have no Domain to cache them. It reads through a session
// of its own, which takes the defaults for its own lookups, so a read never
// reads again.
type globalVarReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	// pool holds the reading session.
	pool util.DestroyableSessionPool
}

func newGlobalVarReader(factory pools.Factory, getCallback, putCallback, destroyCallback func(pools.Resource)) *globalVarReader {
	ctx, cancel := context.WithCancel(context.Background())
	factory = withGlobalVars(factory, func(vars *variable.SessionVars) variable.GlobalVarAccessor {
		return defaultGlobalVars{vars.GlobalVarsAccessor}
	})
	return &globalVarReader{
		ctx:    ctx,
		cancel: cancel,
		pool:   util.NewSessionPool(1, factory, getCallback, putCallback, destroyCallback),
	}
}

// wrap makes the sessions that factory creates read global variables through r.
func (r *globalVarReader) wrap(factory pools.Factory) pools.Factory {
	return withGlobalVars(factory, func(vars *variable.SessionVars) variable.GlobalVarAccessor {
		return &keyspaceGlobalVars{GlobalVarAccessor: vars.GlobalVarsAccessor, vars: vars, reader: r}
	})
}

func (r *globalVarReader) read(table, name string) (value string, found bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	resource, err := r.pool.Get()
	if err != nil {
		return "", false, errors.Trace(err)
	}
	defer r.pool.Put(resource)
	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		return "", false, errors.Errorf("unexpected session type %T", resource)
	}
	ctx := kv.WithInternalSourceType(r.ctx, kv.InternalTxnSysVar)
	rows, fields, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, table, name)
	if err != nil {
		return "", false, err
	}
	if len(rows) == 0 {
		return "", false, nil
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err = d.ToString()
	return value, true, err
}

func (r *globalVarReader) close() {
	r.cancel()
	r.pool.Close()
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

// keyspaceGlobalVars reads global variables from the tables of the keyspace,
// and leaves the other methods to the session.
type keyspaceGlobalVars struct {
	variable.GlobalVarAccessor
	vars   *variable.SessionVars
	reader *globalVarReader
}

// GetGlobalSysVar implements variable.GlobalVarAccessor. Like
// session.GetGlobalSysVar, it returns the default when the keyspace has no
// value or the value does not fit the variable, but returns an error when the
// read fails.
func (g *keyspaceGlobalVars) GetGlobalSysVar(name string) (string, error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	value, found, err := g.reader.read(mysql.GlobalVariablesTable, name)
	if err != nil {
		return "", err
	}
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
	value, found, err := g.reader.read(mysql.TiDBTable, name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errors.Errorf("%s not found in %s.%s", name, mysql.SystemDB, mysql.TiDBTable)
	}
	return value, nil
}

// defaultGlobalVars is the accessor of the reading session.
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
	return "", errors.Errorf("%s is not read by the global variable reader", name)
}
