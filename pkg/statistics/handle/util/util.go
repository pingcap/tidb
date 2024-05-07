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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/sqlexec/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// StatsMetaHistorySourceAnalyze indicates stats history meta source from analyze
	StatsMetaHistorySourceAnalyze = "analyze"
	// StatsMetaHistorySourceLoadStats indicates stats history meta source from load stats
	StatsMetaHistorySourceLoadStats = "load stats"
	// StatsMetaHistorySourceFlushStats indicates stats history meta source from flush stats
	StatsMetaHistorySourceFlushStats = "flush stats"
	// StatsMetaHistorySourceSchemaChange indicates stats history meta source from schema change
	StatsMetaHistorySourceSchemaChange = "schema change"
	// StatsMetaHistorySourceExtendedStats indicates stats history meta source from extended stats
	StatsMetaHistorySourceExtendedStats = "extended stats"

	// TiDBGlobalStats represents the global-stats for a partitioned table.
	TiDBGlobalStats = "global"
)

var (
	// UseCurrentSessionOpt to make sure the sql is executed in current session.
	UseCurrentSessionOpt = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}

	// StatsCtx is used to mark the request is from stats module.
	StatsCtx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
)

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(sctx sessionctx.Context, err error) error {
	if err == nil {
		_, _, err = ExecRows(sctx, "COMMIT")
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

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func GetCurrentPruneMode(pool SessionPool) (mode string, err error) {
	err = CallWithSCtx(pool, func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}

// WrapTxn uses a transaction here can let different SQLs in this operation have the same data visibility.
func WrapTxn(sctx sessionctx.Context, f func(sctx sessionctx.Context) error) (err error) {
	// TODO: check whether this sctx is already in a txn
	if _, _, err := ExecRows(sctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	defer func() {
		err = finishTransaction(sctx, err)
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
func Exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	// TODO: use RestrictedSQLExecutor + ExecOptionUseCurSession instead of SQLExecutor
	return sqlExec.ExecuteInternal(StatsCtx, sql, args...)
}

// ExecRows is a helper function to execute sql and return rows and fields.
func ExecRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	if intest.InTest {
		if v := sctx.Value(mock.RestrictedSQLExecutorKey{}); v != nil {
			return v.(*mock.MockRestrictedSQLExecutor).ExecRestrictedSQL(StatsCtx,
				UseCurrentSessionOpt, sql, args...)
		}
	}

	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(StatsCtx, UseCurrentSessionOpt, sql, args...)
}

// ExecWithOpts is a helper function to execute sql and return rows and fields.
func ExecWithOpts(sctx sessionctx.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(StatsCtx, opts, sql, args...)
}

// DurationToTS converts duration to timestamp.
func DurationToTS(d time.Duration) uint64 {
	return oracle.ComposeTS(d.Nanoseconds()/int64(time.Millisecond), 0)
}

// JSONTable is used for dumping statistics.
type JSONTable struct {
	Columns           map[string]*JSONColumn `json:"columns"`
	Indices           map[string]*JSONColumn `json:"indices"`
	Partitions        map[string]*JSONTable  `json:"partitions"`
	DatabaseName      string                 `json:"database_name"`
	TableName         string                 `json:"table_name"`
	ExtStats          []*JSONExtendedStats   `json:"ext_stats"`
	Count             int64                  `json:"count"`
	ModifyCount       int64                  `json:"modify_count"`
	Version           uint64                 `json:"version"`
	IsHistoricalStats bool                   `json:"is_historical_stats"`
}

// JSONExtendedStats is used for dumping extended statistics.
type JSONExtendedStats struct {
	StatsName  string  `json:"stats_name"`
	StringVals string  `json:"string_vals"`
	ColIDs     []int64 `json:"cols"`
	ScalarVals float64 `json:"scalar_vals"`
	Tp         uint8   `json:"type"`
}

// JSONColumn is used for dumping statistics.
type JSONColumn struct {
	Histogram *tipb.Histogram `json:"histogram"`
	CMSketch  *tipb.CMSketch  `json:"cm_sketch"`
	FMSketch  *tipb.FMSketch  `json:"fm_sketch"`
	// StatsVer is a pointer here since the old version json file would not contain version information.
	StatsVer          *int64  `json:"stats_ver"`
	NullCount         int64   `json:"null_count"`
	TotColSize        int64   `json:"tot_col_size"`
	LastUpdateVersion uint64  `json:"last_update_version"`
	Correlation       float64 `json:"correlation"`
}

// TotalMemoryUsage returns the total memory usage of this column.
func (col *JSONColumn) TotalMemoryUsage() (size int64) {
	if col.Histogram != nil {
		size += int64(col.Histogram.Size())
	}
	if col.CMSketch != nil {
		size += int64(col.CMSketch.Size())
	}
	if col.FMSketch != nil {
		size += int64(col.FMSketch.Size())
	}
	return size
}
