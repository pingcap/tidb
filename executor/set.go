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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	pd "github.com/tikv/pd/client"
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

		if err := e.setSysVariable(ctx, name, v); err != nil {
			return err
		}
	}
	return nil
}

func (e *SetExecutor) setSysVariable(ctx context.Context, name string, v *expression.VarAssignment) error {
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		if variable.IsRemovedSysVar(name) {
			return nil // removed vars permit parse-but-ignore
		}
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}

	if sysVar.HasInstanceScope() && !v.IsGlobal && sessionVars.EnableLegacyInstanceScope {
		// For backward compatibility we will change the v.IsGlobal to true,
		// and append a warning saying this will not be supported in future.
		v.IsGlobal = true
		sessionVars.StmtCtx.AppendWarning(ErrInstanceScope.GenWithStackByArgs(sysVar.Name))
	}

	if v.IsGlobal {
		valStr, err := e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
		// Some PD client dynamic options need to be checked first and set here.
		err = e.checkPDClientDynamicOption(name, sessionVars)
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
		logutil.BgLogger().Info("set global var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		return err
	}
	// Set session variable
	valStr, err := e.getVarValue(v, nil)
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
	if sessionVars.InTxn() {
		if name == variable.TxnIsolationOneShot ||
			name == variable.TiDBTxnReadTS {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
		if name == variable.TiDBSnapshot && sessionVars.TxnCtx.IsStaleness {
			return errors.Trace(ErrCantChangeTxCharacteristics)
		}
	}
	err = variable.SetSessionSystemVar(sessionVars, name, valStr)
	if err != nil {
		return err
	}
	newSnapshotTS := getSnapshotTSByName()
	newSnapshotIsSet := newSnapshotTS > 0 && newSnapshotTS != oldSnapshotTS
	if newSnapshotIsSet {
		if name == variable.TiDBTxnReadTS {
			err = sessionctx.ValidateStaleReadTS(ctx, e.ctx, newSnapshotTS)
		} else {
			err = sessionctx.ValidateSnapshotReadTS(ctx, e.ctx, newSnapshotTS)
			// Also check gc safe point for snapshot read.
			// We don't check snapshot with gc safe point for read_ts
			// Client-go will automatically check the snapshotTS with gc safe point. It's unnecessary to check gc safe point during set executor.
			if err == nil {
				err = gcutil.ValidateSnapshot(e.ctx, newSnapshotTS)
			}
		}
		if err != nil {
			fallbackOldSnapshotTS()
			return err
		}
	}

	err = e.loadSnapshotInfoSchemaIfNeeded(name, newSnapshotTS)
	if err != nil {
		fallbackOldSnapshotTS()
		return err
	}
	// Clients are often noisy in setting session variables such as
	// autocommit, timezone, query cache
	logutil.BgLogger().Debug("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	return nil
}

func (e *SetExecutor) checkPDClientDynamicOption(name string, sessionVars *variable.SessionVars) error {
	if name != variable.TiDBTSOClientBatchMaxWaitTime &&
		name != variable.TiDBEnableTSOFollowerProxy {
		return nil
	}
	var (
		err    error
		valStr string
	)
	valStr, err = sessionVars.GlobalVarsAccessor.GetGlobalSysVar(name)
	if err != nil {
		return err
	}
	switch name {
	case variable.TiDBTSOClientBatchMaxWaitTime:
		var val float64
		val, err = strconv.ParseFloat(valStr, 64)
		if err != nil {
			return err
		}
		err = domain.GetDomain(e.ctx).SetPDClientDynamicOption(
			pd.MaxTSOBatchWaitInterval,
			time.Duration(float64(time.Millisecond)*val),
		)
		if err != nil {
			return err
		}
		logutil.BgLogger().Info("set pd client dynamic option", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
	case variable.TiDBEnableTSOFollowerProxy:
		val := variable.TiDBOptOn(valStr)
		err = domain.GetDomain(e.ctx).SetPDClientDynamicOption(pd.EnableTSOFollowerProxy, val)
		if err != nil {
			return err
		}
		logutil.BgLogger().Info("set pd client dynamic option", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
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

func (e *SetExecutor) loadSnapshotInfoSchemaIfNeeded(name string, snapshotTS uint64) error {
	if name != variable.TiDBSnapshot && name != variable.TiDBTxnReadTS {
		return nil
	}
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

	vars.SnapshotInfoschema = temptable.AttachLocalTemporaryTableInfoSchema(e.ctx, snapInfo)
	return nil
}
