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
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/topsql"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tidb/util/tracing"
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
	Base() *BaseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
	RetFieldTypes() []*types.FieldType
	InitCap() int
	MaxChunkSize() int
}

var _ Executor = &BaseExecutor{}

// BaseExecutor holds common information for executors.
type BaseExecutor struct {
	ctx           sessionctx.Context
	AllocPool     chunk.Allocator
	schema        *expression.Schema // output schema
	runtimeStats  *execdetails.BasicRuntimeStats
	children      []Executor
	retFieldTypes []*types.FieldType
	id            int
	initCap       int
	maxChunkSize  int
}

// NewBaseExecutor creates a new BaseExecutor instance.
func NewBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id int, children ...Executor) BaseExecutor {
	e := BaseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().InitChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
		AllocPool:    ctx.GetSessionVars().ChunkPool.Alloc,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if e.id > 0 {
			e.runtimeStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(id)
		}
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

// RuntimeStats returns the runtime stats of an executor.
func (e *BaseExecutor) RuntimeStats() *execdetails.BasicRuntimeStats {
	return e.runtimeStats
}

// ID returns the id of an executor.
func (e *BaseExecutor) ID() int {
	return e.id
}

// AllChildren returns all children.
func (e *BaseExecutor) AllChildren() []Executor {
	return e.children
}

// ChildrenLen returns the length of children.
func (e *BaseExecutor) ChildrenLen() int {
	return len(e.children)
}

// EmptyChildren judges whether the children is empty.
func (e *BaseExecutor) EmptyChildren() bool {
	return len(e.children) == 0
}

// SetChildren sets the children for an executor.
func (e *BaseExecutor) SetChildren(idx int, ex Executor) {
	e.children[idx] = ex
}

// Children returns the children for an executor.
func (e *BaseExecutor) Children(idx int) Executor {
	return e.children[idx]
}

// RetFieldTypes returns the return field types of an executor.
func (e *BaseExecutor) RetFieldTypes() []*types.FieldType {
	return e.retFieldTypes
}

// InitCap returns the initial capacity for chunk
func (e *BaseExecutor) InitCap() int {
	return e.initCap
}

// SetInitCap sets the initial capacity for chunk
func (e *BaseExecutor) SetInitCap(c int) {
	e.initCap = c
}

// MaxChunkSize returns the max chunk size.
func (e *BaseExecutor) MaxChunkSize() int {
	return e.maxChunkSize
}

// SetMaxChunkSize sets the max chunk size.
func (e *BaseExecutor) SetMaxChunkSize(size int) {
	e.maxChunkSize = size
}

// Base returns the BaseExecutor of an executor, don't override this method!
func (e *BaseExecutor) Base() *BaseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *BaseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *BaseExecutor) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current BaseExecutor's schema. If it is nil, then create and return a new one.
func (e *BaseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// Next fills multiple rows into a chunk.
func (*BaseExecutor) Next(_ context.Context, _ *chunk.Chunk) error {
	return nil
}

// Ctx return ```sessionctx.Context``` of Executor
func (e *BaseExecutor) Ctx() sessionctx.Context {
	return e.ctx
}

// GetSchema gets the schema.
func (e *BaseExecutor) GetSchema() *expression.Schema {
	return e.schema
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
	if _, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "rollback"); err != nil {
		sctx.(pools.Resource).Close()
		return
	}
	sysSessionPool.Put(sctx.(pools.Resource))
}

// TryNewCacheChunk tries to get a cached chunk
func TryNewCacheChunk(e Executor) *chunk.Chunk {
	base := e.Base()
	s := base.Ctx().GetSessionVars()
	return s.GetNewChunkWithCapacity(base.RetFieldTypes(), base.InitCap(), base.MaxChunkSize(), base.AllocPool)
}

// RetTypes returns all output column types.
func RetTypes(e Executor) []*types.FieldType {
	base := e.Base()
	return base.RetFieldTypes()
}

// NewFirstChunk creates a new chunk to buffer current executor's result.
func NewFirstChunk(e Executor) *chunk.Chunk {
	base := e.Base()
	return chunk.New(base.RetFieldTypes(), base.InitCap(), base.MaxChunkSize())
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, req *chunk.Chunk) error {
	base := e.Base()
	if base.RuntimeStats() != nil {
		start := time.Now()
		defer func() { base.RuntimeStats().Record(time.Since(start), req.NumRows()) }()
	}
	sessVars := base.Ctx().GetSessionVars()
	if atomic.LoadUint32(&sessVars.Killed) == 2 {
		return exeerrors.ErrMaxExecTimeExceeded
	}
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		return exeerrors.ErrQueryInterrupted
	}

	r, ctx := tracing.StartRegionEx(ctx, fmt.Sprintf("%T.Next", e))
	defer r.End()

	if topsqlstate.TopSQLEnabled() && sessVars.StmtCtx.IsSQLAndPlanRegistered.CompareAndSwap(false, true) {
		RegisterSQLAndPlanInExecForTopSQL(sessVars)
	}
	err := e.Next(ctx, req)

	if err != nil {
		return err
	}
	// recheck whether the session/query is killed during the Next()
	if atomic.LoadUint32(&sessVars.Killed) == 2 {
		err = exeerrors.ErrMaxExecTimeExceeded
	}
	if atomic.LoadUint32(&sessVars.Killed) == 1 {
		err = exeerrors.ErrQueryInterrupted
	}
	return err
}

// RegisterSQLAndPlanInExecForTopSQL register the sql and plan information if it doesn't register before execution.
// This uses to catch the running SQL when Top SQL is enabled in execution.
func RegisterSQLAndPlanInExecForTopSQL(sessVars *variable.SessionVars) {
	stmtCtx := sessVars.StmtCtx
	normalizedSQL, sqlDigest := stmtCtx.SQLDigest()
	topsql.RegisterSQL(normalizedSQL, sqlDigest, sessVars.InRestrictedSQL)
	normalizedPlan, planDigest := stmtCtx.GetPlanDigest()
	if len(normalizedPlan) > 0 {
		topsql.RegisterPlan(normalizedPlan, planDigest)
	}
}
