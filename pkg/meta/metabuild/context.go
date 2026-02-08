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

package metabuild

import (
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Option is used to set context options.
type Option interface {
	applyCtx(*Context)
}

// funcCtxOption implements the Option interface.
type funcCtxOption struct {
	f func(*Context)
}

func (o funcCtxOption) applyCtx(ctx *Context) {
	o.f(ctx)
}

func funcOpt(f func(ctx *Context)) Option {
	return funcCtxOption{f: f}
}

// WithExprCtx sets the expression context.
func WithExprCtx(exprCtx exprctx.ExprContext) Option {
	intest.AssertNotNil(exprCtx)
	return funcOpt(func(ctx *Context) {
		ctx.exprCtx = exprCtx
	})
}

// WithEnableAutoIncrementInGenerated sets whether enable auto increment in generated column.
func WithEnableAutoIncrementInGenerated(enable bool) Option {
	return funcOpt(func(ctx *Context) {
		ctx.enableAutoIncrementInGenerated = enable
	})
}

// WithPrimaryKeyRequired sets whether primary key is required.
func WithPrimaryKeyRequired(required bool) Option {
	return funcOpt(func(ctx *Context) {
		ctx.primaryKeyRequired = required
	})
}

// WithClusteredIndexDefMode sets the clustered index mode.
func WithClusteredIndexDefMode(mode vardef.ClusteredIndexDefMode) Option {
	return funcOpt(func(ctx *Context) {
		ctx.clusteredIndexDefMode = mode
	})
}

// WithShardRowIDBits sets the shard row id bits.
func WithShardRowIDBits(bits uint64) Option {
	return funcOpt(func(ctx *Context) {
		ctx.shardRowIDBits = bits
	})
}

// WithPreSplitRegions sets the pre-split regions.
func WithPreSplitRegions(regions uint64) Option {
	return funcOpt(func(ctx *Context) {
		ctx.preSplitRegions = regions
	})
}

// WithSuppressTooLongIndexErr sets whether to suppress too long index error.
func WithSuppressTooLongIndexErr(suppress bool) Option {
	return funcOpt(func(ctx *Context) {
		ctx.suppressTooLongIndexErr = suppress
	})
}

// WithInfoSchema sets the info schema.
func WithInfoSchema(schema infoschemactx.MetaOnlyInfoSchema) Option {
	return funcOpt(func(ctx *Context) {
		ctx.is = schema
	})
}

// Context is used to build meta like `TableInfo`, `IndexInfo`, etc...
type Context struct {
	exprCtx                        exprctx.ExprContext
	enableAutoIncrementInGenerated bool
	primaryKeyRequired             bool
	clusteredIndexDefMode          vardef.ClusteredIndexDefMode
	shardRowIDBits                 uint64
	preSplitRegions                uint64
	suppressTooLongIndexErr        bool
	is                             infoschemactx.MetaOnlyInfoSchema
}

// NewContext creates a new context for meta-building.
func NewContext(opts ...Option) *Context {
	ctx := &Context{
		enableAutoIncrementInGenerated: vardef.DefTiDBEnableAutoIncrementInGenerated,
		primaryKeyRequired:             false,
		clusteredIndexDefMode:          vardef.DefTiDBEnableClusteredIndex,
		shardRowIDBits:                 vardef.DefShardRowIDBits,
		preSplitRegions:                vardef.DefPreSplitRegions,
		suppressTooLongIndexErr:        false,
	}

	for _, opt := range opts {
		opt.applyCtx(ctx)
	}

	if ctx.exprCtx == nil {
		ctx.exprCtx = exprstatic.NewExprContext()
	}

	return ctx
}

// NewNonStrictContext creates a new context for meta-building with non-strict mode.
func NewNonStrictContext() *Context {
	evalCtx := exprstatic.NewEvalContext(
		// use mysql.ModeNone to avoid some special values like datetime `0000-00-00 00:00:00`
		exprstatic.WithSQLMode(mysql.ModeNone),
	)
	return NewContext(WithExprCtx(exprstatic.NewExprContext(
		exprstatic.WithEvalCtx(evalCtx),
	)))
}

// GetExprCtx returns the expression context of the session.
func (ctx *Context) GetExprCtx() exprctx.ExprContext {
	return ctx.exprCtx
}

// GetDefaultCollationForUTF8MB4 returns the default collation for utf8mb4.
func (ctx *Context) GetDefaultCollationForUTF8MB4() string {
	return ctx.exprCtx.GetDefaultCollationForUTF8MB4()
}

// GetSQLMode returns the SQL mode.
func (ctx *Context) GetSQLMode() mysql.SQLMode {
	return ctx.exprCtx.GetEvalCtx().SQLMode()
}

// AppendWarning appends a warning.
func (ctx *Context) AppendWarning(err error) {
	ctx.GetExprCtx().GetEvalCtx().AppendWarning(err)
}

// AppendNote appends a note.
func (ctx *Context) AppendNote(note error) {
	ctx.GetExprCtx().GetEvalCtx().AppendNote(note)
}

// EnableAutoIncrementInGenerated returns whether enable auto increment in generated column.
func (ctx *Context) EnableAutoIncrementInGenerated() bool {
	return ctx.enableAutoIncrementInGenerated
}

// PrimaryKeyRequired returns whether primary key is required.
func (ctx *Context) PrimaryKeyRequired() bool {
	return ctx.primaryKeyRequired
}

// GetClusteredIndexDefMode returns the clustered index mode.
func (ctx *Context) GetClusteredIndexDefMode() vardef.ClusteredIndexDefMode {
	return ctx.clusteredIndexDefMode
}

// GetShardRowIDBits returns the shard row id bits.
func (ctx *Context) GetShardRowIDBits() uint64 {
	return ctx.shardRowIDBits
}

// GetPreSplitRegions returns the pre-split regions.
func (ctx *Context) GetPreSplitRegions() uint64 {
	return ctx.preSplitRegions
}

// SuppressTooLongIndexErr returns whether suppress too long index error.
func (ctx *Context) SuppressTooLongIndexErr() bool {
	return ctx.suppressTooLongIndexErr
}

// GetInfoSchema returns the info schema for check some constraints between tables.
// If the second return value is false, it means that we do not need to check the constraints referred to other tables.
func (ctx *Context) GetInfoSchema() (infoschemactx.MetaOnlyInfoSchema, bool) {
	return ctx.is, ctx.is != nil
}
