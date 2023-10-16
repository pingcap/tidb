// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"context"
	"strconv"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/tikv/client-go/v2/oracle"
)

var (
	// UseCurrentSessionOpt to make sure the sql is executed in current session.
	UseCurrentSessionOpt = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}

	// StatsCtx is used to mark the request is from stats module.
	StatsCtx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
)

// SessionPool is used to recycle sessionctx.
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// FinishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func FinishTransaction(sctx sessionctx.Context, err error) error {
	if err == nil {
		_, _, err = ExecRows(sctx, "commit")
	} else {
		_, _, err1 := ExecRows(sctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}

var (
	// FlagWrapTxn indicates whether to wrap a transaction.
	FlagWrapTxn = 0
)

// CallWithSCtx allocates a sctx from the pool and call the f().
func CallWithSCtx(pool SessionPool, f func(sctx sessionctx.Context) error, flags ...int) (err error) {
	se, err := pool.Get()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil { // only recycle when no error
			pool.Put(se)
		}
	}()
	sctx := se.(sessionctx.Context)
	if err := UpdateSCtxVarsForStats(sctx); err != nil { // update stats variables automatically
		return err
	}

	wrapTxn := false
	for _, flag := range flags {
		if flag == FlagWrapTxn {
			wrapTxn = true
		}
	}
	if wrapTxn {
		err = WrapTxn(sctx, f)
	} else {
		err = f(sctx)
	}
	return err
}

// UpdateSCtxVarsForStats updates all necessary variables that may affect the behavior of statistics.
func UpdateSCtxVarsForStats(sctx sessionctx.Context) error {
	// analyzer version
	verInString, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeVersion)
	if err != nil {
		return err
	}
	ver, err := strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().AnalyzeVersion = int(ver)

	// enable historical stats
	val, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableHistoricalStats)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().EnableHistoricalStats = variable.TiDBOptOn(val)

	// partition mode
	pruneMode, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBPartitionPruneMode)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().PartitionPruneMode.Store(pruneMode)

	// enable analyze snapshot
	analyzeSnapshot, err := sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableAnalyzeSnapshot)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().EnableAnalyzeSnapshot = variable.TiDBOptOn(analyzeSnapshot)

	// enable skip column types
	val, err = sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBAnalyzeSkipColumnTypes)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().AnalyzeSkipColumnTypes = variable.ParseAnalyzeSkipColumnTypes(val)

	// skip missing partition stats
	val, err = sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBSkipMissingPartitionStats)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().SkipMissingPartitionStats = variable.TiDBOptOn(val)
	verInString, err = sctx.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBMergePartitionStatsConcurrency)
	if err != nil {
		return err
	}
	ver, err = strconv.ParseInt(verInString, 10, 64)
	if err != nil {
		return err
	}
	sctx.GetSessionVars().AnalyzePartitionMergeConcurrency = int(ver)
	return nil
}

// WrapTxn uses a transaction here can let different SQLs in this operation have the same data visibility.
func WrapTxn(sctx sessionctx.Context, f func(sctx sessionctx.Context) error) (err error) {
	// TODO: check whether this sctx is already in a txn
	if _, _, err := ExecRows(sctx, "begin"); err != nil {
		return err
	}
	defer func() {
		err = FinishTransaction(sctx, err)
	}()
	err = f(sctx)
	return
}

// GetStartTS gets the start ts from current transaction.
func GetStartTS(sctx sessionctx.Context) (uint64, error) {
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// Exec is a helper function to execute sql and return RecordSet.
func Exec(sctx sessionctx.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	sqlExec, ok := sctx.(sqlexec.SQLExecutor)
	if !ok {
		return nil, errors.Errorf("invalid sql executor")
	}
	// TODO: use RestrictedSQLExecutor + ExecOptionUseCurSession instead of SQLExecutor
	return sqlExec.ExecuteInternal(StatsCtx, sql, args...)
}

// ExecRows is a helper function to execute sql and return rows and fields.
func ExecRows(sctx sessionctx.Context, sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	if intest.InTest {
		if v := sctx.Value(mock.MockRestrictedSQLExecutorKey{}); v != nil {
			return v.(*mock.MockRestrictedSQLExecutor).ExecRestrictedSQL(StatsCtx,
				UseCurrentSessionOpt, sql, args...)
		}
	}

	sqlExec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, nil, errors.Errorf("invalid sql executor")
	}
	return sqlExec.ExecRestrictedSQL(StatsCtx, UseCurrentSessionOpt, sql, args...)
}

// ExecWithOpts is a helper function to execute sql and return rows and fields.
func ExecWithOpts(sctx sessionctx.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	sqlExec, ok := sctx.(sqlexec.RestrictedSQLExecutor)
	if !ok {
		return nil, nil, errors.Errorf("invalid sql executor")
	}
	return sqlExec.ExecRestrictedSQL(StatsCtx, opts, sql, args...)
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// GetFullTableName returns the full table name.
func GetFullTableName(is infoschema.InfoSchema, tblInfo *model.TableInfo) string {
	for _, schema := range is.AllSchemas() {
		if t, err := is.TableByName(schema.Name, tblInfo.Name); err == nil {
			if t.Meta().ID == tblInfo.ID {
				return schema.Name.O + "." + tblInfo.Name.O
			}
		}
	}
	return strconv.FormatInt(tblInfo.ID, 10)
}
