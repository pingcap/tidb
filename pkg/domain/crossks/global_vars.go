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

// keyspaceGlobalVars reads the global variables of the keyspace for a session
// of the runtime, which has no Domain to cache them. It reads them from the
// table of the keyspace through another session of the pool, and leaves the
// other methods to the session.
type keyspaceGlobalVars struct {
	variable.GlobalVarAccessor
	vars *variable.SessionVars
	pool util.SessionPool
	// reading is set while the session reads for another one, so that its own
	// lookups take the defaults instead of reading again.
	reading bool
}

// withKeyspaceGlobalVars makes the sessions that factory creates read the
// global variables of the keyspace through pool.
func withKeyspaceGlobalVars(factory pools.Factory, pool func() util.SessionPool) pools.Factory {
	return func() (pools.Resource, error) {
		r, err := factory()
		if err != nil {
			return nil, err
		}
		if sctx, ok := r.(sessionctx.Context); ok {
			vars := sctx.GetSessionVars() //nolint:forbidigo
			vars.GlobalVarsAccessor = &keyspaceGlobalVars{
				GlobalVarAccessor: vars.GlobalVarsAccessor,
				vars:              vars,
				pool:              pool(),
			}
		}
		return r, nil
	}
}

// GetGlobalSysVar implements variable.GlobalVarAccessor. A failed read returns
// an error instead of the default value.
func (g *keyspaceGlobalVars) GetGlobalSysVar(name string) (string, error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if g.reading {
		return sv.Value, nil
	}
	value, found, err := g.read(name)
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

func (g *keyspaceGlobalVars) read(name string) (value string, found bool, err error) {
	resource, err := g.pool.Get()
	if err != nil {
		return "", false, errors.Trace(err)
	}
	defer g.pool.Put(resource)
	sctx, ok := resource.(sessionctx.Context)
	if !ok {
		return "", false, errors.Errorf("unexpected session type %T", resource)
	}
	reader, ok := sctx.GetSessionVars().GlobalVarsAccessor.(*keyspaceGlobalVars) //nolint:forbidigo
	if !ok {
		return "", false, errors.Errorf("unexpected global variable accessor %T", sctx.GetSessionVars().GlobalVarsAccessor) //nolint:forbidigo
	}
	reader.reading = true
	defer func() { reader.reading = false }()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnSysVar)
	rows, fields, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		"SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, mysql.GlobalVariablesTable, name)
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
