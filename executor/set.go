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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stmtsummary"
	"go.uber.org/zap"
)

// SetExecutor executes set statement.
type SetExecutor struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

// Next implements the Executor Next interface.
func (e *SetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		// Variable is case insensitive, we use lower case.
		if v.Name == ast.SetNames || v.Name == ast.SetCharset {
			// This is set charset stmt.
			if v.IsDefault {
				err := e.setCharset(mysql.DefaultCharset, "", v.Name == ast.SetNames)
				if err != nil {
					return err
				}
				continue
			}
			dt, err := v.Expr.(*expression.Constant).Eval(chunk.Row{})
			if err != nil {
				return err
			}
			cs := dt.GetString()
			var co string
			if v.ExtendValue != nil {
				co = v.ExtendValue.Value.GetString()
			}
			err = e.setCharset(cs, co, v.Name == ast.SetNames)
			if err != nil {
				return err
			}
			continue
		}
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Row{})
			if err != nil {
				return err
			}
			sessionVars.UsersLock.Lock()
			if value.IsNull() {
				delete(sessionVars.Users, name)
				delete(sessionVars.UserVarTypes, name)
			} else {
				sessionVars.Users[name] = value
				sessionVars.UserVarTypes[name] = v.Expr.GetType()
			}
			sessionVars.UsersLock.Unlock()
			continue
		}

		if err := e.setSysVariable(name, v); err != nil {
			return err
		}
	}
	return nil
}

func (e *SetExecutor) setSysVariable(name string, v *expression.VarAssignment) error {
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	var valStr string
	var err error
	if v.IsGlobal {
		valStr, err = e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
		err = plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
			auditPlugin := plugin.DeclareAuditManifest(p.Manifest)
			if auditPlugin.OnGlobalVariableEvent != nil {
				auditPlugin.OnGlobalVariableEvent(context.Background(), e.ctx.GetSessionVars(), name, valStr)
			}
			return nil
		})
		if err != nil {
			return err
		}
		logutil.BgLogger().Info("set global var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	} else {
		valStr, err = e.getVarValue(v, nil)
		if err != nil {
			return err
		}
		getSnapshotTSByName := func() uint64 {
			if name == variable.TiDBSnapshot {
				return sessionVars.SnapshotTS
			} else if name == variable.TiDBTxnReadTS {
				return sessionVars.TxnReadTS.PeakTxnReadTS()
			}
			return 0
		}
		oldSnapshotTS := getSnapshotTSByName()
		fallbackOldSnapshotTS := func() {
			if name == variable.TiDBSnapshot {
				sessionVars.SnapshotTS = oldSnapshotTS
			} else if name == variable.TiDBTxnReadTS {
				sessionVars.TxnReadTS.SetTxnReadTS(oldSnapshotTS)
			}
		}
		if name == variable.TxnIsolationOneShot && sessionVars.InTxn() {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		err = variable.SetSessionSystemVar(sessionVars, name, valStr)
		if err != nil {
			return err
		}
		newSnapshotTS := getSnapshotTSByName()
		newSnapshotIsSet := newSnapshotTS > 0 && newSnapshotTS != oldSnapshotTS
		if newSnapshotIsSet {
			err = gcutil.ValidateSnapshot(e.ctx, newSnapshotTS)
			if err != nil {
				fallbackOldSnapshotTS()
				return err
			}
		}
		err = e.loadSnapshotInfoSchemaIfNeeded(newSnapshotTS)
		if err != nil {
			fallbackOldSnapshotTS()
			return err
		}
		// Clients are often noisy in setting session variables such as
		// autocommit, timezone, query cache
		logutil.BgLogger().Debug("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	}

	// These are server instance scoped variables, and have special semantics.
	// i.e. after SET SESSION, other users sessions will reflect the new value.
	// TODO: in future these could be better managed as a post-set hook.

	valStrToBoolStr := variable.BoolToOnOff(variable.TiDBOptOn(valStr))

	switch name {
	case variable.TiDBEnableStmtSummary:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabled(valStr, !v.IsGlobal)
	case variable.TiDBStmtSummaryInternalQuery:
		return stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(valStr, !v.IsGlobal)
	case variable.TiDBStmtSummaryRefreshInterval:
		return stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(valStr, !v.IsGlobal)
	case variable.TiDBStmtSummaryHistorySize:
		return stmtsummary.StmtSummaryByDigestMap.SetHistorySize(valStr, !v.IsGlobal)
	case variable.TiDBStmtSummaryMaxStmtCount:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(valStr, !v.IsGlobal)
	case variable.TiDBStmtSummaryMaxSQLLength:
		return stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(valStr, !v.IsGlobal)
	case variable.TiDBCapturePlanBaseline:
		variable.CapturePlanBaseline.Set(valStrToBoolStr, !v.IsGlobal)
	}

	return nil
}

func (e *SetExecutor) setCharset(cs, co string, isSetName bool) error {
	var err error
	sessionVars := e.ctx.GetSessionVars()
	if co == "" {
		if co, err = charset.GetDefaultCollation(cs); err != nil {
			return err
		}
	} else {
		var coll *charset.Collation
		if coll, err = collate.GetCollationByName(co); err != nil {
			return err
		}
		if coll.CharsetName != cs {
			return charset.ErrCollationCharsetMismatch.GenWithStackByArgs(coll.Name, cs)
		}
	}
	if isSetName {
		for _, v := range variable.SetNamesVariables {
			if err = variable.SetSessionSystemVar(sessionVars, v, cs); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(variable.SetSessionSystemVar(sessionVars, variable.CollationConnection, co))
	}
	// Set charset statement, see also https://dev.mysql.com/doc/refman/8.0/en/set-character-set.html.
	for _, v := range variable.SetCharsetVariables {
		if err = variable.SetSessionSystemVar(sessionVars, v, cs); err != nil {
			return errors.Trace(err)
		}
	}
	csDb, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(variable.CharsetDatabase)
	if err != nil {
		return err
	}
	coDb, err := sessionVars.GlobalVarsAccessor.GetGlobalSysVar(variable.CollationDatabase)
	if err != nil {
		return err
	}
	err = variable.SetSessionSystemVar(sessionVars, variable.CharacterSetConnection, csDb)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(variable.SetSessionSystemVar(sessionVars, variable.CollationConnection, coDb))
}

func (e *SetExecutor) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value string, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			return sysVar.Value, nil
		}
		return variable.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
	}
	nativeVal, err := v.Expr.Eval(chunk.Row{})
	if err != nil || nativeVal.IsNull() {
		return "", err
	}
	return nativeVal.ToString()
}

func (e *SetExecutor) loadSnapshotInfoSchemaIfNeeded(snapshotTS uint64) error {
	vars := e.ctx.GetSessionVars()
	if snapshotTS == 0 {
		vars.SnapshotInfoschema = nil
		return nil
	}
	logutil.BgLogger().Info("load snapshot info schema",
		zap.Uint64("conn", vars.ConnectionID),
		zap.Uint64("SnapshotTS", snapshotTS))
	dom := domain.GetDomain(e.ctx)
	snapInfo, err := dom.GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return err
	}
	vars.SnapshotInfoschema = snapInfo
	return nil
}
