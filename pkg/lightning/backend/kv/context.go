// Copyright 2024 PingCAP, Inc.
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

package kv

import (
	"maps"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

var _ exprctx.ExprContext = &litExprContext{}

// litExprContext implements the `exprctx.ExprContext` interface for lightning import.
// It provides the context to build and evaluate expressions, furthermore, it allows to set user variables
// for `IMPORT INTO ...` statements.
type litExprContext struct {
	*contextstatic.StaticExprContext
	userVars *variable.UserVars
}

// NewExpressionContext creates a new `*StaticExprContext` for lightning import.
func newLitExprContext(sqlMode mysql.SQLMode, sysVars map[string]string, timestamp int64) (*litExprContext, error) {
	flags := types.DefaultStmtFlags.
		WithTruncateAsWarning(!sqlMode.HasStrictMode()).
		WithIgnoreInvalidDateErr(sqlMode.HasAllowInvalidDatesMode()).
		WithIgnoreZeroInDate(!sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode() ||
			!sqlMode.HasNoZeroInDateMode() || !sqlMode.HasNoZeroDateMode())

	errLevels := stmtctx.DefaultStmtErrLevels
	errLevels[errctx.ErrGroupTruncate] = errctx.ResolveErrLevel(flags.IgnoreTruncateErr(), flags.TruncateAsWarning())
	errLevels[errctx.ErrGroupBadNull] = errctx.ResolveErrLevel(false, !sqlMode.HasStrictMode())
	errLevels[errctx.ErrGroupDividedByZero] =
		errctx.ResolveErrLevel(!sqlMode.HasErrorForDivisionByZeroMode(), !sqlMode.HasStrictMode())

	userVars := variable.NewUserVars()
	evalCtx := contextstatic.NewStaticEvalContext(
		contextstatic.WithSQLMode(sqlMode),
		contextstatic.WithTypeFlags(flags),
		contextstatic.WithLocation(timeutil.SystemLocation()),
		contextstatic.WithErrLevelMap(errLevels),
		contextstatic.WithUserVarsReader(userVars),
	)

	// no need to build as plan cache.
	planCacheTracker := contextutil.NewPlanCacheTracker(contextutil.IgnoreWarn)
	intest.Assert(!planCacheTracker.UseCache())
	ctx := contextstatic.NewStaticExprContext(
		contextstatic.WithEvalCtx(evalCtx),
		contextstatic.WithPlanCacheTracker(&planCacheTracker),
	)

	if len(sysVars) > 0 {
		var err error
		ctx, err = ctx.LoadSystemVars(sysVars)
		if err != nil {
			return nil, err
		}
		evalCtx = ctx.GetStaticEvalCtx()
	}

	currentTime := func() (time.Time, error) { return time.Now(), nil }
	if timestamp > 0 {
		currentTime = func() (time.Time, error) { return time.Unix(timestamp, 0), nil }
	}

	evalCtx = evalCtx.Apply(contextstatic.WithCurrentTime(currentTime))
	ctx = ctx.Apply(contextstatic.WithEvalCtx(evalCtx))

	return &litExprContext{
		StaticExprContext: ctx,
		userVars:          userVars,
	}, nil
}

// setUserVarVal sets the value of a user variable.
func (ctx *litExprContext) setUserVarVal(name string, dt types.Datum) {
	ctx.userVars.SetUserVarVal(name, dt)
}

// UnsetUserVar unsets a user variable.
func (ctx *litExprContext) unsetUserVar(varName string) {
	ctx.userVars.UnsetUserVar(varName)
}

var _ table.MutateContext = &litTableMutateContext{}

// litTableMutateContext implements the `table.MutateContext` interface for lightning import.
type litTableMutateContext struct {
	exprCtx               *litExprContext
	encodingConfig        tbctx.RowEncodingConfig
	mutateBuffers         *tbctx.MutateBuffers
	shardID               *variable.RowIDShardGenerator
	reservedRowIDAlloc    stmtctx.ReservedRowIDAlloc
	enableMutationChecker bool
	assertionLevel        variable.AssertionLevel
	tableDelta            struct {
		sync.Mutex
		// tblID -> (colID -> deltaSize)
		m map[int64]map[int64]int64
	}
}

// AlternativeAllocators implements the `table.MutateContext` interface.
func (*litTableMutateContext) AlternativeAllocators(*model.TableInfo) (autoid.Allocators, bool) {
	// lightning does not support temporary tables, so we don't need to provide alternative allocators.
	return autoid.Allocators{}, false
}

// GetExprCtx implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
}

// ConnectionID implements the `table.MutateContext` interface.
func (*litTableMutateContext) ConnectionID() uint64 {
	// Just return 0 because lightning import does not in any connection.
	return 0
}

// InRestrictedSQL implements the `table.MutateContext` interface.
func (*litTableMutateContext) InRestrictedSQL() bool {
	// Just return false because lightning import does not in any SQL.
	return false
}

// TxnAssertionLevel implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) TxnAssertionLevel() variable.AssertionLevel {
	return ctx.assertionLevel
}

// EnableMutationChecker implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) EnableMutationChecker() bool {
	return ctx.enableMutationChecker
}

// GetRowEncodingConfig implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetRowEncodingConfig() tbctx.RowEncodingConfig {
	return ctx.encodingConfig
}

// GetMutateBuffers implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetMutateBuffers() *tbctx.MutateBuffers {
	return ctx.mutateBuffers
}

// GetRowIDShardGenerator implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetRowIDShardGenerator() *variable.RowIDShardGenerator {
	return ctx.shardID
}

// GetReservedRowIDAlloc implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetReservedRowIDAlloc() (*stmtctx.ReservedRowIDAlloc, bool) {
	return &ctx.reservedRowIDAlloc, true
}

// GetBinlogSupport implements the `table.MutateContext` interface.
func (*litTableMutateContext) GetBinlogSupport() (tbctx.BinlogSupport, bool) {
	// lightning import does not support binlog.
	return nil, false
}

// GetStatisticsSupport implements the `table.MutateContext` interface.
func (ctx *litTableMutateContext) GetStatisticsSupport() (tbctx.StatisticsSupport, bool) {
	return ctx, true
}

// UpdatePhysicalTableDelta implements the `table.StatisticsSupport` interface.
func (ctx *litTableMutateContext) UpdatePhysicalTableDelta(
	physicalTableID int64, _ int64,
	_ int64, cols variable.DeltaCols,
) {
	ctx.tableDelta.Lock()
	defer ctx.tableDelta.Unlock()
	if ctx.tableDelta.m == nil {
		ctx.tableDelta.m = make(map[int64]map[int64]int64)
	}
	tableMap := ctx.tableDelta.m
	colSize := tableMap[physicalTableID]
	tableMap[physicalTableID] = cols.UpdateColSizeMap(colSize)
}

// GetColumnSize returns the colum size map (colID -> deltaSize) for the given table ID.
func (ctx *litTableMutateContext) GetColumnSize(tblID int64) (ret map[int64]int64) {
	ctx.tableDelta.Lock()
	defer ctx.tableDelta.Unlock()
	return maps.Clone(ctx.tableDelta.m[tblID])
}

// GetCachedTableSupport implements the `table.MutateContext` interface.
func (*litTableMutateContext) GetCachedTableSupport() (tbctx.CachedTableSupport, bool) {
	// lightning import does not support cached table.
	return nil, false
}

func (*litTableMutateContext) GetTemporaryTableSupport() (tbctx.TemporaryTableSupport, bool) {
	// lightning import does not support temporary table.
	return nil, false
}

func (*litTableMutateContext) GetExchangePartitionDMLSupport() (tbctx.ExchangePartitionDMLSupport, bool) {
	// lightning import is not in a DML query, we do not need to support it.
	return nil, false
}

// newLitTableMutateContext creates a new `*litTableMutateContext` for lightning import.
func newLitTableMutateContext(exprCtx *litExprContext, sysVars map[string]string) (*litTableMutateContext, error) {
	intest.AssertNotNil(exprCtx)
	sessVars := variable.NewSessionVars(nil)
	for k, v := range sysVars {
		if err := sessVars.SetSystemVar(k, v); err != nil {
			return nil, err
		}
	}

	return &litTableMutateContext{
		exprCtx: exprCtx,
		encodingConfig: tbctx.RowEncodingConfig{
			IsRowLevelChecksumEnabled: sessVars.IsRowLevelChecksumEnabled(),
			RowEncoder:                &sessVars.RowEncoder,
		},
		mutateBuffers: tbctx.NewMutateBuffers(sessVars.GetWriteStmtBufs()),
		// Though the row ID is generated by lightning itself, and `GetRowIDShardGenerator` is useless,
		// still return a valid object to make the context complete and avoid some potential panic
		// if there are some changes in the future.
		shardID: variable.NewRowIDShardGenerator(
			rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404
			int(sessVars.ShardAllocateStep),
		),
		enableMutationChecker: sessVars.EnableMutationChecker,
		assertionLevel:        sessVars.AssertionLevel,
	}, nil
}
