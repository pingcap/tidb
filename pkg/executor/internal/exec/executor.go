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

package exec

import (
	"context"
	"fmt"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/atomic"
)

// Executor is the physical implementation of an algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	NewChunk() *chunk.Chunk
	NewChunkWithCapacity(fields []*types.FieldType, capacity int, maxCachesize int) *chunk.Chunk

	RuntimeStats() *execdetails.BasicRuntimeStats

	HandleSQLKillerSignal() error
	RegisterSQLAndPlanInExecForTopSQL()

	AllChildren() []Executor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error

	// `Close()` may be called at any time after `Open()` and it may be called with `Next()` at the same time
	Close() error
	Schema() *expression.Schema
	RetFieldTypes() []*types.FieldType
	InitCap() int
	MaxChunkSize() int
}

var _ Executor = &BaseExecutor{}

// executorChunkAllocator is a helper to implement `Chunk` related methods in `Executor` interface
type executorChunkAllocator struct {
	AllocPool     chunk.Allocator
	retFieldTypes []*types.FieldType
	initCap       int
	maxChunkSize  int
}

// newExecutorChunkAllocator creates a new `executorChunkAllocator`
func newExecutorChunkAllocator(vars *variable.SessionVars, retFieldTypes []*types.FieldType) executorChunkAllocator {
	return executorChunkAllocator{
		AllocPool:     vars.GetChunkAllocator(),
		initCap:       vars.InitChunkSize,
		maxChunkSize:  vars.MaxChunkSize,
		retFieldTypes: retFieldTypes,
	}
}

// InitCap returns the initial capacity for chunk
func (e *executorChunkAllocator) InitCap() int {
	failpoint.Inject("initCap", func(val failpoint.Value) {
		failpoint.Return(val.(int))
	})
	return e.initCap
}

// SetInitCap sets the initial capacity for chunk
func (e *executorChunkAllocator) SetInitCap(c int) {
	e.initCap = c
}

// MaxChunkSize returns the max chunk size.
func (e *executorChunkAllocator) MaxChunkSize() int {
	failpoint.Inject("maxChunkSize", func(val failpoint.Value) {
		failpoint.Return(val.(int))
	})
	return e.maxChunkSize
}

// SetMaxChunkSize sets the max chunk size.
func (e *executorChunkAllocator) SetMaxChunkSize(size int) {
	e.maxChunkSize = size
}

// NewChunk creates a new chunk according to the executor configuration
func (e *executorChunkAllocator) NewChunk() *chunk.Chunk {
	return e.NewChunkWithCapacity(e.retFieldTypes, e.InitCap(), e.MaxChunkSize())
}

// NewChunkWithCapacity allows the caller to allocate the chunk with any types, capacity and max size in the pool
func (e *executorChunkAllocator) NewChunkWithCapacity(fields []*types.FieldType, capacity int, maxCachesize int) *chunk.Chunk {
	return e.AllocPool.Alloc(fields, capacity, maxCachesize)
}

// executorMeta is a helper to store metadata for an execturo and implement the getter
type executorMeta struct {
	schema        *expression.Schema
	children      []Executor
	retFieldTypes []*types.FieldType
	id            int
}

// newExecutorMeta creates a new `executorMeta`
func newExecutorMeta(schema *expression.Schema, id int, children ...Executor) executorMeta {
	e := executorMeta{
		id:       id,
		schema:   schema,
		children: children,
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// NewChunkWithCapacity allows the caller to allocate the chunk with any types, capacity and max size in the pool
func (e *executorMeta) RetFieldTypes() []*types.FieldType {
	return e.retFieldTypes
}

// ID returns the id of an executor.
func (e *executorMeta) ID() int {
	return e.id
}

// AllChildren returns all children.
func (e *executorMeta) AllChildren() []Executor {
	return e.children
}

// ChildrenLen returns the length of children.
func (e *executorMeta) ChildrenLen() int {
	return len(e.children)
}

// EmptyChildren judges whether the children is empty.
func (e *executorMeta) EmptyChildren() bool {
	return len(e.children) == 0
}

// SetChildren sets the children for an executor.
func (e *executorMeta) SetChildren(idx int, ex Executor) {
	e.children[idx] = ex
}

// Children returns the children for an executor.
func (e *executorMeta) Children(idx int) Executor {
	return e.children[idx]
}

// Schema returns the current BaseExecutor's schema. If it is nil, then create and return a new one.
func (e *executorMeta) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// GetSchema gets the schema.
func (e *executorMeta) GetSchema() *expression.Schema {
	return e.schema
}

// executorStats is a helper to implement the stats related methods for `Executor`
type executorStats struct {
	runtimeStats           *execdetails.BasicRuntimeStats
	isSQLAndPlanRegistered *atomic.Bool
	sqlDigest              *parser.Digest
	planDigest             *parser.Digest
	normalizedSQL          string
	normalizedPlan         string
	inRestrictedSQL        bool
}

// newExecutorStats creates a new `executorStats`
func newExecutorStats(stmtCtx *stmtctx.StatementContext, id int) executorStats {
	normalizedSQL, sqlDigest := stmtCtx.SQLDigest()
	normalizedPlan, planDigest := stmtCtx.GetPlanDigest()
	e := executorStats{
		isSQLAndPlanRegistered: &stmtCtx.IsSQLAndPlanRegistered,
		normalizedSQL:          normalizedSQL,
		sqlDigest:              sqlDigest,
		normalizedPlan:         normalizedPlan,
		planDigest:             planDigest,
		inRestrictedSQL:        stmtCtx.InRestrictedSQL,
	}

	if stmtCtx.RuntimeStatsColl != nil {
		if id > 0 {
			e.runtimeStats = stmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(id)
		}
	}

	return e
}

// RuntimeStats returns the runtime stats of an executor.
func (e *executorStats) RuntimeStats() *execdetails.BasicRuntimeStats {
	return e.runtimeStats
}

// RegisterSQLAndPlanInExecForTopSQL registers the current SQL and Plan on top sql
func (e *executorStats) RegisterSQLAndPlanInExecForTopSQL() {
	if topsqlstate.TopSQLEnabled() && e.isSQLAndPlanRegistered.CompareAndSwap(false, true) {
		topsql.RegisterSQL(e.normalizedSQL, e.sqlDigest, e.inRestrictedSQL)
		if len(e.normalizedPlan) > 0 {
			topsql.RegisterPlan(e.normalizedPlan, e.planDigest)
		}
	}
}

type signalHandler interface {
	HandleSignal() error
}

// executorKillerHandler is a helper to implement the killer related methods for `Executor`.
type executorKillerHandler struct {
	handler signalHandler
}

func (e *executorKillerHandler) HandleSQLKillerSignal() error {
	return e.handler.HandleSignal()
}

func newExecutorKillerHandler(handler signalHandler) executorKillerHandler {
	return executorKillerHandler{handler}
}

// BaseExecutorV2 is a simplified version of `BaseExecutor`, which doesn't contain a full session context
type BaseExecutorV2 struct {
	executorMeta
	executorKillerHandler
	executorStats
	executorChunkAllocator
}

// NewBaseExecutorV2 creates a new BaseExecutorV2 instance.
func NewBaseExecutorV2(vars *variable.SessionVars, schema *expression.Schema, id int, children ...Executor) BaseExecutorV2 {
	executorMeta := newExecutorMeta(schema, id, children...)
	e := BaseExecutorV2{
		executorMeta:           executorMeta,
		executorStats:          newExecutorStats(vars.StmtCtx, id),
		executorChunkAllocator: newExecutorChunkAllocator(vars, executorMeta.RetFieldTypes()),
		executorKillerHandler:  newExecutorKillerHandler(&vars.SQLKiller),
	}
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *BaseExecutorV2) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := Open(ctx, child)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *BaseExecutorV2) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := Close(src); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Next fills multiple rows into a chunk.
func (*BaseExecutorV2) Next(_ context.Context, _ *chunk.Chunk) error {
	return nil
}

// BaseExecutor holds common information for executors.
type BaseExecutor struct {
	ctx sessionctx.Context

	BaseExecutorV2
}

// NewBaseExecutor creates a new BaseExecutor instance.
func NewBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id int, children ...Executor) BaseExecutor {
	return BaseExecutor{
		ctx:            ctx,
		BaseExecutorV2: NewBaseExecutorV2(ctx.GetSessionVars(), schema, id, children...),
	}
}

// Ctx return ```sessionctx.Context``` of Executor
func (e *BaseExecutor) Ctx() sessionctx.Context {
	return e.ctx
}

// UpdateDeltaForTableID updates the delta info for the table with tableID.
func (e *BaseExecutor) UpdateDeltaForTableID(id int64) {
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
}

// GetSysSession gets a system session context from executor.
func (e *BaseExecutor) GetSysSession() (sessionctx.Context, error) {
	dom := domain.GetDomain(e.Ctx())
	sysSessionPool := dom.SysSessionPool()
	ctx, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	restrictedCtx := ctx.(sessionctx.Context)
	restrictedCtx.GetSessionVars().InRestrictedSQL = true
	return restrictedCtx, nil
}

// ReleaseSysSession releases a system session context to executor.
func (e *BaseExecutor) ReleaseSysSession(ctx context.Context, sctx sessionctx.Context) {
	if sctx == nil {
		return
	}
	dom := domain.GetDomain(e.Ctx())
	sysSessionPool := dom.SysSessionPool()
	if _, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "rollback"); err != nil {
		sctx.(pools.Resource).Close()
		return
	}
	sysSessionPool.Put(sctx.(pools.Resource))
}

// TryNewCacheChunk tries to get a cached chunk
func TryNewCacheChunk(e Executor) *chunk.Chunk {
	return e.NewChunk()
}

// RetTypes returns all output column types.
func RetTypes(e Executor) []*types.FieldType {
	return e.RetFieldTypes()
}

// NewFirstChunk creates a new chunk to buffer current executor's result.
func NewFirstChunk(e Executor) *chunk.Chunk {
	return chunk.New(e.RetFieldTypes(), e.InitCap(), e.MaxChunkSize())
}

// Open is a wrapper function on e.Open(), it handles some common codes.
func Open(ctx context.Context, e Executor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
	}()
	return e.Open(ctx)
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, req *chunk.Chunk) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
	}()
	if e.RuntimeStats() != nil {
		start := time.Now()
		defer func() { e.RuntimeStats().Record(time.Since(start), req.NumRows()) }()
	}

	if err := e.HandleSQLKillerSignal(); err != nil {
		return err
	}

	r, ctx := tracing.StartRegionEx(ctx, fmt.Sprintf("%T.Next", e))
	defer r.End()

	e.RegisterSQLAndPlanInExecForTopSQL()
	err = e.Next(ctx, req)

	if err != nil {
		return err
	}
	// recheck whether the session/query is killed during the Next()
	return e.HandleSQLKillerSignal()
}

// Close is a wrapper function on e.Close(), it handles some common codes.
func Close(e Executor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util.GetRecoverError(r)
		}
	}()
	return e.Close()
}
