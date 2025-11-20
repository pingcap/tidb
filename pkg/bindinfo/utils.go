// Copyright 2025 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

func callWithSCtx(sPool util.DestroyableSessionPool, wrapTxn bool, f func(sctx sessionctx.Context) error) (err error) {
	resource, err := sPool.Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			sPool.Put(resource)
		} else {
			// Note: Otherwise, the session will be leaked.
			sPool.Destroy(resource)
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

// exec is a helper function to execute sql and return RecordSet.
func exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	return sqlExec.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), sql, args...)
}

// execRows is a helper function to execute sql and return rows and fields.
func execRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

// bindingLogger with category "sql-bind" is used to log statistic related messages.
func bindingLogger() *zap.Logger {
	return logutil.BgLogger().With(zap.String("category", "sql-bind"))
}

// GenerateBindingSQL generates binding sqls from stmt node and plan hints.
func GenerateBindingSQL(stmtNode ast.StmtNode, planHint string, defaultDB string) string {
	// We need to evolve plan based on the current sql, not the original sql which may have different parameters.
	// So here we would remove the hint and inject the current best plan hint.
	hint.BindHint(stmtNode, &hint.HintsSet{})
	bindSQL := RestoreDBForBinding(stmtNode, defaultDB)
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
				bindingLogger().Debug("restore SQL failed", zap.Error(err))
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
	bindingLogger().Debug("unexpected statement type when generating bind SQL", zap.Any("statement", stmtNode))
	return ""
}

func readBindingsFromStorage(sPool util.DestroyableSessionPool, condition string, args ...any) (bindings []*Binding, err error) {
	selectStmt := fmt.Sprintf(`SELECT original_sql, bind_sql, default_db, status, create_time,
       update_time, charset, collation, source, sql_digest, plan_digest FROM mysql.bind_info
       %s`, condition)

	err = callWithSCtx(sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err := execRows(sctx, selectStmt, args...)
		if err != nil {
			return err
		}
		bindings = make([]*Binding, 0, len(rows))
		for _, row := range rows {
			// Skip the builtin record which is designed for binding synchronization.
			if row.GetString(0) == BuiltinPseudoSQL4BindLock {
				continue
			}
			binding := newBindingFromStorage(row)
			if hErr := prepareHints(sctx, binding); hErr != nil {
				bindingLogger().Warn("failed to generate bind record from data row", zap.Error(hErr))
				continue
			}
			bindings = append(bindings, binding)
		}
		return nil
	})
	return
}

var (
	// UpdateBindingUsageInfoBatchSize indicates the batch size when updating binding usage info to storage.
	UpdateBindingUsageInfoBatchSize = 100
	// MaxWriteInterval indicates the interval at which a write operation needs to be performed after a binding has not been read.
	MaxWriteInterval = 6 * time.Hour
)

func updateBindingUsageInfoToStorage(sPool util.DestroyableSessionPool, bindings []*Binding) error {
	toWrite := make([]*Binding, 0, UpdateBindingUsageInfoBatchSize)
	now := time.Now()
	cnt := 0
	defer func() {
		if cnt > 0 {
			bindingLogger().Info("update binding usage info to storage", zap.Int("count", cnt), zap.Duration("duration", time.Since(now)))
		}
	}()
	for _, binding := range bindings {
		lastUsed := binding.UsageInfo.LastUsedAt.Load()
		if lastUsed == nil {
			continue
		}
		lastSaved := binding.UsageInfo.LastSavedAt.Load()
		if shouldUpdateBinding(lastSaved, lastUsed) {
			toWrite = append(toWrite, binding)
			cnt++
		}
		if len(toWrite) == UpdateBindingUsageInfoBatchSize {
			err := updateBindingUsageInfoToStorageInternal(sPool, toWrite)
			if err != nil {
				return err
			}
			toWrite = toWrite[:0]
		}
	}
	if len(toWrite) > 0 {
		err := updateBindingUsageInfoToStorageInternal(sPool, toWrite)
		if err != nil {
			return err
		}
	}
	return nil
}

func shouldUpdateBinding(lastSaved, lastUsed *time.Time) bool {
	if lastSaved == nil {
		// If it has never been written before, it will be written.
		return true
	}
	// If a certain amount of time specified by MaxWriteInterval has passed since the last record was written,
	// and it has been used in between, it will be written.
	return time.Since(*lastSaved) >= MaxWriteInterval && lastUsed.After(*lastSaved)
}

func updateBindingUsageInfoToStorageInternal(sPool util.DestroyableSessionPool, bindings []*Binding) error {
	err := callWithSCtx(sPool, true, func(sctx sessionctx.Context) (err error) {
		if err = lockBindInfoTable(sctx); err != nil {
			return errors.Trace(err)
		}
		// lockBindInfoTable is to prefetch the rows and lock them, it is good for performance when
		// there are many bindings to update with multi tidb nodes.
		// in the performance test, it takes 26.24s to update 44679 bindings.
		if err = addLockForBinds(sctx, bindings); err != nil {
			return errors.Trace(err)
		}
		for _, binding := range bindings {
			lastUsed := binding.UsageInfo.LastUsedAt.Load()
			intest.Assert(lastUsed != nil)
			err = saveBindingUsage(sctx, binding.SQLDigest, binding.PlanDigest, *lastUsed)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err == nil {
		ts := time.Now()
		for _, binding := range bindings {
			binding.UpdateLastSavedAt(&ts)
		}
	}
	return err
}

func addLockForBinds(sctx sessionctx.Context, bindings []*Binding) error {
	condition := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		sqlDigest := binding.SQLDigest
		planDigest := binding.PlanDigest
		sql := fmt.Sprintf("('%s'", sqlDigest)
		if planDigest == "" {
			sql += ",NULL)"
		} else {
			sql += fmt.Sprintf(",'%s')", planDigest)
		}
		condition = append(condition, sql)
	}
	locksql := "select 1 from mysql.bind_info use index(digest_index) where (plan_digest, sql_digest) in (" +
		strings.Join(condition, " , ") + ") for update"
	_, err := exec(sctx, locksql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func saveBindingUsage(sctx sessionctx.Context, sqldigest, planDigest string, ts time.Time) error {
	lastUsedTime := ts.UTC().Format(types.TimeFormat)
	var sql = "UPDATE mysql.bind_info USE INDEX(digest_index) SET last_used_date = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE) WHERE sql_digest = %?"
	if planDigest == "" {
		sql += " AND plan_digest IS NULL"
	} else {
		sql += fmt.Sprintf(" AND plan_digest = '%s'", planDigest)
	}
	_, err := exec(
		sctx,
		sql,
		lastUsedTime, sqldigest,
	)
	return err
}

// newBindingFromStorage builds Bindings from a tuple in storage.
func newBindingFromStorage(row chunk.Row) *Binding {
	status := row.GetString(3)
	// For compatibility, the 'Using' status binding will be converted to the 'Enabled' status binding.
	if status == StatusUsing {
		status = StatusEnabled
	}
	return &Binding{
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
}

// getBindingPlanDigest does the best efforts to fill binding's plan_digest.
func getBindingPlanDigest(sctx sessionctx.Context, schema, bindingSQL string) (planDigest string) {
	defer func() {
		if r := recover(); r != nil {
			bindingLogger().Error("panic when filling plan digest for binding",
				zap.String("binding_sql", bindingSQL), zap.Reflect("panic", r))
		}
	}()

	vars := sctx.GetSessionVars()
	defer func(originalBaseline bool, originalDB string) {
		vars.UsePlanBaselines = originalBaseline
		vars.CurrentDB = originalDB
	}(vars.UsePlanBaselines, vars.CurrentDB)
	vars.UsePlanBaselines = false
	vars.CurrentDB = schema

	p := utilparser.GetParser()
	defer utilparser.DestroyParser(p)
	p.SetSQLMode(vars.SQLMode)
	p.SetParserConfig(vars.BuildParserConfig())

	charset, collation := vars.GetCharsetInfo()
	if stmt, err := p.ParseOneStmt(bindingSQL, charset, collation); err == nil {
		if !hasParam(stmt) {
			// if there is '?' from `create binding using select a from t where a=?`,
			// the final plan digest might be incorrect.
			planDigest, _ = CalculatePlanDigest(sctx, stmt)
		}
	}
	return
}
