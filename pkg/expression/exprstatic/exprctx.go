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

package exprstatic

import (
	"strings"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

// ExprContext implements the `exprctx.ExprContext` interface.
var _ exprctx.ExprContext = &ExprContext{}

// exprCtxState is the internal state for `ExprContext`.
// We make it as a standalone private struct here to make sure `ExprCtxOption` can only be called in constructor.
type exprCtxState struct {
	evalCtx                    *EvalContext
	charset                    string
	collation                  string
	defaultCollationForUTF8MB4 string
	blockEncryptionMode        string
	sysDateIsNow               bool
	noopFuncsMode              int
	rng                        *mathutil.MysqlRng
	planCacheTracker           *contextutil.PlanCacheTracker
	columnIDAllocator          exprctx.PlanColumnIDAllocator
	connectionID               uint64
	windowingUseHighPrecision  bool
	groupConcatMaxLen          uint64
}

// ExprCtxOption is the option to create or update the `ExprContext`
type ExprCtxOption func(*exprCtxState)

// WithEvalCtx sets the `EvalContext` for `ExprContext`.
func WithEvalCtx(ctx *EvalContext) ExprCtxOption {
	intest.AssertNotNil(ctx)
	return func(s *exprCtxState) {
		s.evalCtx = ctx
	}
}

// WithCharset sets the charset and collation for `ExprContext`.
func WithCharset(charset, collation string) ExprCtxOption {
	return func(s *exprCtxState) {
		s.charset = charset
		s.collation = collation
	}
}

// WithDefaultCollationForUTF8MB4 sets the default collation for utf8mb4 for `ExprContext`.
func WithDefaultCollationForUTF8MB4(collation string) ExprCtxOption {
	return func(s *exprCtxState) {
		s.defaultCollationForUTF8MB4 = collation
	}
}

// WithBlockEncryptionMode sets the block encryption mode for `ExprContext`.
func WithBlockEncryptionMode(mode string) ExprCtxOption {
	return func(s *exprCtxState) {
		s.blockEncryptionMode = mode
	}
}

// WithSysDateIsNow sets the sysdate is now for `ExprContext`.
func WithSysDateIsNow(now bool) ExprCtxOption {
	return func(s *exprCtxState) {
		s.sysDateIsNow = now
	}
}

// WithNoopFuncsMode sets the noop funcs mode for `ExprContext`.
func WithNoopFuncsMode(mode int) ExprCtxOption {
	intest.Assert(mode == variable.OnInt || mode == variable.OffInt || mode == variable.WarnInt)
	return func(s *exprCtxState) {
		s.noopFuncsMode = mode
	}
}

// WithRng sets the rng for `ExprContext`.
func WithRng(rng *mathutil.MysqlRng) ExprCtxOption {
	intest.AssertNotNil(rng)
	return func(s *exprCtxState) {
		s.rng = rng
	}
}

// WithPlanCacheTracker sets the plan cache tracker for `ExprContext`.
func WithPlanCacheTracker(tracker *contextutil.PlanCacheTracker) ExprCtxOption {
	intest.AssertNotNil(tracker)
	return func(s *exprCtxState) {
		s.planCacheTracker = tracker
	}
}

// WithColumnIDAllocator sets the column id allocator for `ExprContext`.
func WithColumnIDAllocator(allocator exprctx.PlanColumnIDAllocator) ExprCtxOption {
	intest.AssertNotNil(allocator)
	return func(s *exprCtxState) {
		s.columnIDAllocator = allocator
	}
}

// WithConnectionID sets the connection id for `ExprContext`.
func WithConnectionID(id uint64) ExprCtxOption {
	return func(s *exprCtxState) {
		s.connectionID = id
	}
}

// WithWindowingUseHighPrecision sets the windowing use high precision for `ExprContext`.
func WithWindowingUseHighPrecision(useHighPrecision bool) ExprCtxOption {
	return func(s *exprCtxState) {
		s.windowingUseHighPrecision = useHighPrecision
	}
}

// WithGroupConcatMaxLen sets the group concat max len for `ExprContext`.
func WithGroupConcatMaxLen(maxLen uint64) ExprCtxOption {
	return func(s *exprCtxState) {
		s.groupConcatMaxLen = maxLen
	}
}

// ExprContext implements the `exprctx.ExprContext` interface.
// The "static" means comparing with `ExprContext`, its internal state does not relay on the session or other
// complex contexts that keeps immutable for most fields.
type ExprContext struct {
	exprCtxState
}

// NewExprContext creates a new ExprContext
func NewExprContext(opts ...ExprCtxOption) *ExprContext {
	cs, err := charset.GetCharsetInfo(mysql.DefaultCharset)
	intest.AssertNoError(err)

	ctx := &ExprContext{
		exprCtxState: exprCtxState{
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
	for _, opt := range opts {
		opt(&ctx.exprCtxState)
	}

	if ctx.evalCtx == nil {
		ctx.evalCtx = NewEvalContext()
	}

	if ctx.rng == nil {
		ctx.rng = mathutil.NewWithTime()
	}

	if ctx.columnIDAllocator == nil {
		ctx.columnIDAllocator = exprctx.NewSimplePlanColumnIDAllocator(0)
	}

	if ctx.planCacheTracker == nil {
		cacheTracker := contextutil.NewPlanCacheTracker(ctx.evalCtx)
		ctx.planCacheTracker = &cacheTracker
		ctx.planCacheTracker.EnablePlanCache()
	}

	return ctx
}

// Apply returns a new `ExprContext` with the fields updated according to the given options.
func (ctx *ExprContext) Apply(opts ...ExprCtxOption) *ExprContext {
	newCtx := &ExprContext{
		exprCtxState: ctx.exprCtxState,
	}

	for _, opt := range opts {
		opt(&newCtx.exprCtxState)
	}

	return newCtx
}

// GetEvalCtx implements the `ExprContext.GetEvalCtx`.
func (ctx *ExprContext) GetEvalCtx() exprctx.EvalContext {
	return ctx.evalCtx
}

// GetStaticEvalCtx returns the inner `EvalContext`.
func (ctx *ExprContext) GetStaticEvalCtx() *EvalContext {
	return ctx.evalCtx
}

// GetCharsetInfo implements the `ExprContext.GetCharsetInfo`.
func (ctx *ExprContext) GetCharsetInfo() (string, string) {
	return ctx.charset, ctx.collation
}

// GetDefaultCollationForUTF8MB4 implements the `ExprContext.GetDefaultCollationForUTF8MB4`.
func (ctx *ExprContext) GetDefaultCollationForUTF8MB4() string {
	return ctx.defaultCollationForUTF8MB4
}

// GetBlockEncryptionMode implements the `ExprContext.GetBlockEncryptionMode`.
func (ctx *ExprContext) GetBlockEncryptionMode() string {
	return ctx.blockEncryptionMode
}

// GetSysdateIsNow implements the `ExprContext.GetSysdateIsNow`.
func (ctx *ExprContext) GetSysdateIsNow() bool {
	return ctx.sysDateIsNow
}

// GetNoopFuncsMode implements the `ExprContext.GetNoopFuncsMode`.
func (ctx *ExprContext) GetNoopFuncsMode() int {
	return ctx.noopFuncsMode
}

// Rng implements the `ExprContext.Rng`.
func (ctx *ExprContext) Rng() *mathutil.MysqlRng {
	return ctx.rng
}

// IsUseCache implements the `ExprContext.IsUseCache`.
func (ctx *ExprContext) IsUseCache() bool {
	return ctx.planCacheTracker.UseCache()
}

// SetSkipPlanCache implements the `ExprContext.SetSkipPlanCache`.
func (ctx *ExprContext) SetSkipPlanCache(reason string) {
	ctx.planCacheTracker.SetSkipPlanCache(reason)
}

// AllocPlanColumnID implements the `ExprContext.AllocPlanColumnID`.
func (ctx *ExprContext) AllocPlanColumnID() int64 {
	return ctx.columnIDAllocator.AllocPlanColumnID()
}

// IsInNullRejectCheck implements the `ExprContext.IsInNullRejectCheck` and should always return false.
func (ctx *ExprContext) IsInNullRejectCheck() bool {
	return false
}

// IsConstantPropagateCheck implements the `ExprContext.IsConstantPropagateCheck` and should always return false.
func (ctx *ExprContext) IsConstantPropagateCheck() bool {
	return false
}

// ConnectionID implements the `ExprContext.ConnectionID`.
func (ctx *ExprContext) ConnectionID() uint64 {
	return ctx.connectionID
}

// GetWindowingUseHighPrecision implements the `ExprContext.GetWindowingUseHighPrecision`.
func (ctx *ExprContext) GetWindowingUseHighPrecision() bool {
	return ctx.windowingUseHighPrecision
}

// GetGroupConcatMaxLen implements the `ExprContext.GetGroupConcatMaxLen`.
func (ctx *ExprContext) GetGroupConcatMaxLen() uint64 {
	return ctx.groupConcatMaxLen
}

var _ exprctx.StaticConvertibleExprContext = &ExprContext{}

// GetLastPlanColumnID implements context.StaticConvertibleExprContext.
func (ctx *ExprContext) GetLastPlanColumnID() int64 {
	return ctx.columnIDAllocator.GetLastPlanColumnID()
}

// GetPlanCacheTracker implements context.StaticConvertibleExprContext.
func (ctx *ExprContext) GetPlanCacheTracker() *contextutil.PlanCacheTracker {
	return ctx.planCacheTracker
}

// GetStaticConvertibleEvalContext implements context.StaticConvertibleExprContext.
func (ctx *ExprContext) GetStaticConvertibleEvalContext() exprctx.StaticConvertibleEvalContext {
	return ctx.evalCtx
}

// MakeExprContextStatic converts the `exprctx.StaticConvertibleExprContext` to `ExprContext`.
func MakeExprContextStatic(ctx exprctx.StaticConvertibleExprContext) *ExprContext {
	staticEvalContext := MakeEvalContextStatic(ctx.GetStaticConvertibleEvalContext())

	return NewExprContext(
		WithEvalCtx(staticEvalContext),
		WithCharset(ctx.GetCharsetInfo()),
		WithDefaultCollationForUTF8MB4(ctx.GetDefaultCollationForUTF8MB4()),
		WithBlockEncryptionMode(ctx.GetBlockEncryptionMode()),
		WithSysDateIsNow(ctx.GetSysdateIsNow()),
		WithNoopFuncsMode(ctx.GetNoopFuncsMode()),
		WithRng(ctx.Rng()),
		WithPlanCacheTracker(ctx.GetPlanCacheTracker()),
		WithColumnIDAllocator(
			exprctx.NewSimplePlanColumnIDAllocator(ctx.GetLastPlanColumnID())),
		WithConnectionID(ctx.ConnectionID()),
		WithWindowingUseHighPrecision(ctx.GetWindowingUseHighPrecision()),
		WithGroupConcatMaxLen(ctx.GetGroupConcatMaxLen()),
	)
}

// LoadSystemVars loads system variables and returns a new `EvalContext` with system variables loaded.
func (ctx *ExprContext) LoadSystemVars(sysVars map[string]string) (*ExprContext, error) {
	sessionVars, err := newSessionVarsWithSystemVariables(sysVars)
	if err != nil {
		return nil, err
	}
	return ctx.loadSessionVarsInternal(sessionVars, sysVars), nil
}

func (ctx *ExprContext) loadSessionVarsInternal(
	sessionVars *variable.SessionVars, sysVars map[string]string,
) *ExprContext {
	opts := make([]ExprCtxOption, 0, 8)
	opts = append(opts, WithEvalCtx(ctx.evalCtx.loadSessionVarsInternal(sessionVars, sysVars)))
	for name := range sysVars {
		name = strings.ToLower(name)
		switch name {
		case variable.CharacterSetConnection, variable.CollationConnection:
			opts = append(opts, WithCharset(sessionVars.GetCharsetInfo()))
		case variable.DefaultCollationForUTF8MB4:
			opts = append(opts, WithDefaultCollationForUTF8MB4(sessionVars.DefaultCollationForUTF8MB4))
		case variable.BlockEncryptionMode:
			blockMode, ok := sessionVars.GetSystemVar(variable.BlockEncryptionMode)
			intest.Assert(ok)
			if ok {
				opts = append(opts, WithBlockEncryptionMode(blockMode))
			}
		case variable.TiDBSysdateIsNow:
			opts = append(opts, WithSysDateIsNow(sessionVars.SysdateIsNow))
		case variable.TiDBEnableNoopFuncs:
			opts = append(opts, WithNoopFuncsMode(sessionVars.NoopFuncsMode))
		case variable.WindowingUseHighPrecision:
			opts = append(opts, WithWindowingUseHighPrecision(sessionVars.WindowingUseHighPrecision))
		case variable.GroupConcatMaxLen:
			opts = append(opts, WithGroupConcatMaxLen(sessionVars.GroupConcatMaxLen))
		}
	}
	return ctx.Apply(opts...)
}
