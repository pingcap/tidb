// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessionctx/varsutil"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next() (Row, error) {
	if e.done {
		return nil, nil
	}
	err := e.executeSet()
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.done = true
	return nil, nil
}

func (e *SetExecutor) executeSet() error {
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames {
			// This is set charset stmt.
			cs := v.Expr.(*expression.Constant).Value.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err := e.setCharset(cs, co)
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(nil)
			if err != nil {
				return errors.Trace(err)
			}

			if value.IsNull() {
				delete(sessionVars.Users, name)
			} else {
				svalue, err1 := value.ToString()
				if err1 != nil {
					return errors.Trace(err1)
				}
				sessionVars.Users[name] = fmt.Sprintf("%v", svalue)
			}
			continue
		}

		// Set system variable
		sysVar := variable.GetSysVar(name)
		if sysVar == nil {
			return variable.UnknownSystemVar.GenByArgs(name)
		}
		if sysVar.Scope == variable.ScopeNone {
			return errors.Errorf("Variable '%s' is a read only variable", name)
		}
		if v.IsGlobal {
			// Set global scope system variable.
			if sysVar.Scope&variable.ScopeGlobal == 0 {
				return errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
			}
			value, err := e.getVarValue(v, sysVar)
			if err != nil {
				return errors.Trace(err)
			}
			if value.IsNull() {
				value.SetString("")
			}
			svalue, err := value.ToString()
			if err != nil {
				return errors.Trace(err)
			}
			err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, svalue)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			// Set session scope system variable.
			if sysVar.Scope&variable.ScopeSession == 0 {
				return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
			}
			value, err := e.getVarValue(v, nil)
			if err != nil {
				return errors.Trace(err)
			}
			oldSnapshotTS := sessionVars.SnapshotTS
			err = varsutil.SetSessionSystemVar(sessionVars, name, value)
			if err != nil {
				return errors.Trace(err)
			}
			newSnapshotIsSet := sessionVars.SnapshotTS > 0 && sessionVars.SnapshotTS != oldSnapshotTS
			if newSnapshotIsSet {
				err = validateSnapshot(e.ctx, sessionVars.SnapshotTS)
				if err != nil {
					sessionVars.SnapshotTS = oldSnapshotTS
					return errors.Trace(err)
				}
			}
			err = e.loadSnapshotInfoSchemaIfNeeded(name)
			if err != nil {
				sessionVars.SnapshotTS = oldSnapshotTS
				return errors.Trace(err)
			}
			var valStr string
			if value.IsNull() {
				valStr = "NULL"
			} else {
				var err error
				valStr, err = value.ToString()
				terror.Log(errors.Trace(err))
			}
			log.Infof("[%d] set system variable %s = %s", sessionVars.ConnectionID, name, valStr)
		}

		if name == variable.TxnIsolation {
			if sessionVars.Systems[variable.TxnIsolation] == ast.ReadCommitted {
				e.ctx.Txn().SetOption(kv.IsolationLevel, kv.RC)
			}
		}
	}
	return nil
}

// validateSnapshot checks that the newly set snapshot time is after GC safe point time.
func validateSnapshot(ctx context.Context, snapshotTS uint64) error {
	sql := "SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_safe_point'"
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) != 1 {
		return errors.New("can not get 'tikv_gc_safe_point'")
	}
	safePointString := rows[0].Data[0].GetString()
	const gcTimeFormat = "20060102-15:04:05 -0700 MST"
	safePointTime, err := time.Parse(gcTimeFormat, safePointString)
	if err != nil {
		return errors.Trace(err)
	}
	safePointTS := varsutil.GoTimeToTS(safePointTime)
	if safePointTS > snapshotTS {
		return variable.ErrSnapshotTooOld.GenByArgs(safePointString)
	}
	return nil
}

func (e *SetExecutor) setCharset(cs, co string) error {
	var err error
	if len(co) == 0 {
		co, err = charset.GetDefaultCollation(cs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range variable.SetNamesVariables {
		sessionVars.Systems[v] = cs
	}
	sessionVars.Systems[variable.CollationConnection] = co
	return nil
}

func (e *SetExecutor) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value types.Datum, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			value = types.NewStringDatum(sysVar.Value)
		} else {
			s, err1 := varsutil.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
			if err1 != nil {
				return value, errors.Trace(err1)
			}
			value = types.NewStringDatum(s)
		}
		return
	}
	value, err = v.Expr.Eval(nil)
	return value, errors.Trace(err)
}

func (e *SetExecutor) loadSnapshotInfoSchemaIfNeeded(name string) error {
	if name != variable.TiDBSnapshot {
		return nil
	}
	vars := e.ctx.GetSessionVars()
	if vars.SnapshotTS == 0 {
		vars.SnapshotInfoschema = nil
		return nil
	}
	log.Infof("[%d] loadSnapshotInfoSchema, SnapshotTS:%d", vars.ConnectionID, vars.SnapshotTS)
	dom := sessionctx.GetDomain(e.ctx)
	snapInfo, err := dom.GetSnapshotInfoSchema(vars.SnapshotTS)
	if err != nil {
		return errors.Trace(err)
	}
	vars.SnapshotInfoschema = snapInfo
	return nil
}
