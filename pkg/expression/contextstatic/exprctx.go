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

package contextstatic

import (
	"sync/atomic"

	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

// StaticExprContext implements the `exprctx.ExprContext` interface.
var _ exprctx.ExprContext = &StaticExprContext{}

// staticExprCtxState is the internal state for `StaticExprContext`.
// We make it as a standalone private struct here to make sure `StaticExprCtxOption` can only be called in constructor.
type staticExprCtxState struct {
	evalCtx                    *StaticEvalContext
	charset                    string
	collation                  string
	defaultCollationForUTF8MB4 string
	blockEncryptionMode        string
	sysDateIsNow               bool
	noopFuncsMode              int
	rng                        *mathutil.MysqlRng
	canUseCache                *atomic.Bool
	skipCacheHandleFunc        func(useCache *atomic.Bool, skipReason string)
	columnIDAllocator          exprctx.PlanColumnIDAllocator
	connectionID               uint64
	windowingUseHighPrecision  bool
	groupConcatMaxLen          uint64
}

// StaticExprCtxOption is the option to create or update the `StaticExprContext`
type StaticExprCtxOption func(*staticExprCtxState)

// WithEvalCtx sets the `StaticEvalContext` for `StaticExprContext`.
func WithEvalCtx(ctx *StaticEvalContext) StaticExprCtxOption {
	intest.AssertNotNil(ctx)
	return func(s *staticExprCtxState) {
		s.evalCtx = ctx
	}
}

// WithCharset sets the charset and collation for `StaticExprContext`.
func WithCharset(charset, collation string) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.charset = charset
		s.collation = collation
	}
}

// WithDefaultCollationForUTF8MB4 sets the default collation for utf8mb4 for `StaticExprContext`.
func WithDefaultCollationForUTF8MB4(collation string) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.defaultCollationForUTF8MB4 = collation
	}
}

// WithBlockEncryptionMode sets the block encryption mode for `StaticExprContext`.
func WithBlockEncryptionMode(mode string) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.blockEncryptionMode = mode
	}
}

// WithSysDateIsNow sets the sysdate is now for `StaticExprContext`.
func WithSysDateIsNow(now bool) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.sysDateIsNow = now
	}
}

// WithNoopFuncsMode sets the noop funcs mode for `StaticExprContext`.
func WithNoopFuncsMode(mode int) StaticExprCtxOption {
	intest.Assert(mode == variable.OnInt || mode == variable.OffInt || mode == variable.WarnInt)
	return func(s *staticExprCtxState) {
		s.noopFuncsMode = mode
	}
}

// WithRng sets the rng for `StaticExprContext`.
func WithRng(rng *mathutil.MysqlRng) StaticExprCtxOption {
	intest.AssertNotNil(rng)
	return func(s *staticExprCtxState) {
		s.rng = rng
	}
}

// WithUseCache sets the return value of `IsUseCache` for `StaticExprContext`.
func WithUseCache(useCache bool) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.canUseCache.Store(useCache)
	}
}

// WithSkipCacheHandleFunc sets inner skip plan cache function for StaticExprContext
func WithSkipCacheHandleFunc(fn func(useCache *atomic.Bool, skipReason string)) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.skipCacheHandleFunc = fn
	}
}

// WithColumnIDAllocator sets the column id allocator for `StaticExprContext`.
func WithColumnIDAllocator(allocator exprctx.PlanColumnIDAllocator) StaticExprCtxOption {
	intest.AssertNotNil(allocator)
	return func(s *staticExprCtxState) {
		s.columnIDAllocator = allocator
	}
}

// WithConnectionID sets the connection id for `StaticExprContext`.
func WithConnectionID(id uint64) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.connectionID = id
	}
}

// WithWindowingUseHighPrecision sets the windowing use high precision for `StaticExprContext`.
func WithWindowingUseHighPrecision(useHighPrecision bool) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.windowingUseHighPrecision = useHighPrecision
	}
}

// WithGroupConcatMaxLen sets the group concat max len for `StaticExprContext`.
func WithGroupConcatMaxLen(maxLen uint64) StaticExprCtxOption {
	return func(s *staticExprCtxState) {
		s.groupConcatMaxLen = maxLen
	}
}

// StaticExprContext implements the `exprctx.ExprContext` interface.
// The "static" means comparing with `SessionExprContext`, its internal state does not relay on the session or other
// complex contexts that keeps immutable for most fields.
type StaticExprContext struct {
	staticExprCtxState
}

// NewStaticExprContext creates a new StaticExprContext
func NewStaticExprContext(opts ...StaticExprCtxOption) *StaticExprContext {
	cs, err := charset.GetCharsetInfo(mysql.DefaultCharset)
	intest.AssertNoError(err)

	ctx := &StaticExprContext{
		staticExprCtxState: staticExprCtxState{
			charset:                    cs.Name,
			collation:                  cs.DefaultCollation,
			defaultCollationForUTF8MB4: mysql.DefaultCollationName,
			blockEncryptionMode:        variable.DefBlockEncryptionMode,
			sysDateIsNow:               variable.DefSysdateIsNow,
			noopFuncsMode:              variable.TiDBOptOnOffWarn(variable.DefTiDBEnableNoopFuncs),
			windowingUseHighPrecision:  true,
			groupConcatMaxLen:          variable.DefGroupConcatMaxLen,
		},
	}

	ctx.canUseCache = &atomic.Bool{}
	ctx.canUseCache.Store(true)

	for _, opt := range opts {
		opt(&ctx.staticExprCtxState)
	}

	if ctx.evalCtx == nil {
		ctx.evalCtx = NewStaticEvalContext()
	}

	if ctx.rng == nil {
		ctx.rng = mathutil.NewWithTime()
	}

	if ctx.columnIDAllocator == nil {
		ctx.columnIDAllocator = exprctx.NewSimplePlanColumnIDAllocator(0)
	}

	return ctx
}

// Apply returns a new `StaticExprContext` with the fields updated according to the given options.
func (ctx *StaticExprContext) Apply(opts ...StaticExprCtxOption) *StaticExprContext {
	newCtx := &StaticExprContext{
		staticExprCtxState: ctx.staticExprCtxState,
	}

	newCtx.canUseCache = &atomic.Bool{}
	newCtx.canUseCache.Store(ctx.canUseCache.Load())

	for _, opt := range opts {
		opt(&newCtx.staticExprCtxState)
	}

	return newCtx
}

// GetEvalCtx implements the `ExprContext.GetEvalCtx`.
func (ctx *StaticExprContext) GetEvalCtx() exprctx.EvalContext {
	return ctx.evalCtx
}

// GetCharsetInfo implements the `ExprContext.GetCharsetInfo`.
func (ctx *StaticExprContext) GetCharsetInfo() (string, string) {
	return ctx.charset, ctx.collation
}

// GetDefaultCollationForUTF8MB4 implements the `ExprContext.GetDefaultCollationForUTF8MB4`.
func (ctx *StaticExprContext) GetDefaultCollationForUTF8MB4() string {
	return ctx.defaultCollationForUTF8MB4
}

// GetBlockEncryptionMode implements the `ExprContext.GetBlockEncryptionMode`.
func (ctx *StaticExprContext) GetBlockEncryptionMode() string {
	return ctx.blockEncryptionMode
}

// GetSysdateIsNow implements the `ExprContext.GetSysdateIsNow`.
func (ctx *StaticExprContext) GetSysdateIsNow() bool {
	return ctx.sysDateIsNow
}

// GetNoopFuncsMode implements the `ExprContext.GetNoopFuncsMode`.
func (ctx *StaticExprContext) GetNoopFuncsMode() int {
	return ctx.noopFuncsMode
}

// Rng implements the `ExprContext.Rng`.
func (ctx *StaticExprContext) Rng() *mathutil.MysqlRng {
	return ctx.rng
}

// IsUseCache implements the `ExprContext.IsUseCache`.
func (ctx *StaticExprContext) IsUseCache() bool {
	return ctx.canUseCache.Load()
}

// SetSkipPlanCache implements the `ExprContext.SetSkipPlanCache`.
func (ctx *StaticExprContext) SetSkipPlanCache(reason string) {
	if fn := ctx.skipCacheHandleFunc; fn != nil {
		fn(ctx.canUseCache, reason)
		return
	}
	ctx.canUseCache.Store(false)
}

// AllocPlanColumnID implements the `ExprContext.AllocPlanColumnID`.
func (ctx *StaticExprContext) AllocPlanColumnID() int64 {
	return ctx.columnIDAllocator.AllocPlanColumnID()
}

// IsInNullRejectCheck implements the `ExprContext.IsInNullRejectCheck` and should always return false.
func (ctx *StaticExprContext) IsInNullRejectCheck() bool {
	return false
}

// ConnectionID implements the `ExprContext.ConnectionID`.
func (ctx *StaticExprContext) ConnectionID() uint64 {
	return ctx.connectionID
}

// GetWindowingUseHighPrecision implements the `ExprContext.GetWindowingUseHighPrecision`.
func (ctx *StaticExprContext) GetWindowingUseHighPrecision() bool {
	return ctx.windowingUseHighPrecision
}

// GetGroupConcatMaxLen implements the `ExprContext.GetGroupConcatMaxLen`.
func (ctx *StaticExprContext) GetGroupConcatMaxLen() uint64 {
	return ctx.groupConcatMaxLen
}
