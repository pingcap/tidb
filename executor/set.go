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
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/gcutil"
	log "github.com/sirupsen/logrus"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames {
			// This is set charset stmt.
			dt, err := v.Expr.(*expression.Constant).Eval(chunk.Row{})
			if err != nil {
				return errors.Trace(err)
			}
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err = e.setCharset(cs, co)
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Row{})
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

		syns := e.getSynonyms(name)
		// Set system variable
		for _, n := range syns {
			err := e.setSysVariable(n, v)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (e *SetExecutor) getSynonyms(varName string) []string {
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		return synonyms
	}

	synonyms = []string{varName}
	return synonyms
}

func (e *SetExecutor) setSysVariable(name string, v *expression.VarAssignment) error {
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.UnknownSystemVar.GenWithStackByArgs(name)
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
		if name == variable.TxnIsolationOneShot && sessionVars.InTxn() {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		err = variable.SetSessionSystemVar(sessionVars, name, value)
		if err != nil {
			return errors.Trace(err)
		}
		newSnapshotIsSet := sessionVars.SnapshotTS > 0 && sessionVars.SnapshotTS != oldSnapshotTS
		if newSnapshotIsSet {
			err = gcutil.ValidateSnapshot(e.ctx, sessionVars.SnapshotTS)
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
		log.Infof("con:%d %s=%s", sessionVars.ConnectionID, name, valStr)
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
		terror.Log(errors.Trace(sessionVars.SetSystemVar(v, cs)))
	}
	terror.Log(errors.Trace(sessionVars.SetSystemVar(variable.CollationConnection, co)))
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
			s, err1 := variable.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
			if err1 != nil {
				return value, errors.Trace(err1)
			}
			value = types.NewStringDatum(s)
		}
		return
	}
	value, err = v.Expr.Eval(chunk.Row{})
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
	log.Infof("con:%d loadSnapshotInfoSchema, SnapshotTS:%d", vars.ConnectionID, vars.SnapshotTS)
	dom := domain.GetDomain(e.ctx)
	snapInfo, err := dom.GetSnapshotInfoSchema(vars.SnapshotTS)
	if err != nil {
		return errors.Trace(err)
	}
	vars.SnapshotInfoschema = snapInfo
	return nil
}
