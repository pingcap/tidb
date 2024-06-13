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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// StaticEvalContext should implement `EvalContext`
var _ exprctx.EvalContext = &StaticEvalContext{}

type timeOnce struct {
	sync.Mutex
	time   atomic.Pointer[time.Time]
	timeFn func() (time.Time, error)
}

func (t *timeOnce) getTime(loc *time.Location) (tm time.Time, err error) {
	if p := t.time.Load(); p != nil {
		return *p, nil
	}

	t.Lock()
	defer t.Unlock()

	if p := t.time.Load(); p != nil {
		return *p, nil
	}

	if fn := t.timeFn; fn != nil {
		tm, err = fn()
	} else {
		tm, err = time.Now(), nil
	}

	if err != nil {
		return
	}

	tm = tm.In(loc)
	t.time.Store(&tm)
	return
}

// staticEvalCtxState is the internal state for `StaticEvalContext`.
// We make it as a standalone private struct here to make sure `StaticEvalCtxOption` can only be called in constructor.
type staticEvalCtxState struct {
	warnHandler                  contextutil.WarnHandler
	sqlMode                      mysql.SQLMode
	typeCtx                      types.Context
	errCtx                       errctx.Context
	currentDB                    string
	currentTime                  *timeOnce
	maxAllowedPacket             uint64
	defaultWeekFormatMode        string
	divPrecisionIncrement        int
	requestVerificationFn        func(db, table, column string, priv mysql.PrivilegeType) bool
	requestDynamicVerificationFn func(privName string, grantable bool) bool
	paramList                    []types.Datum
	props                        contextopt.OptionalEvalPropProviders
}

// StaticEvalCtxOption is the option to set `StaticEvalContext`.
type StaticEvalCtxOption func(*staticEvalCtxState)

// WithWarnHandler sets the warn handler for the `StaticEvalContext`.
func WithWarnHandler(h contextutil.WarnHandler) StaticEvalCtxOption {
	intest.AssertNotNil(h)
	if h == nil {
		// this should not happen, just to keep code safe
		h = contextutil.IgnoreWarn
	}

	return func(s *staticEvalCtxState) {
		s.warnHandler = h
	}
}

// WithSQLMode sets the sql mode for the `StaticEvalContext`.
func WithSQLMode(sqlMode mysql.SQLMode) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.sqlMode = sqlMode
	}
}

// WithTypeFlags sets the type flags for the `StaticEvalContext`.
func WithTypeFlags(flags types.Flags) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.typeCtx = s.typeCtx.WithFlags(flags)
	}
}

// WithLocation sets the timezone info for the `StaticEvalContext`.
func WithLocation(loc *time.Location) StaticEvalCtxOption {
	intest.AssertNotNil(loc)
	if loc == nil {
		// this should not happen, just to keep code safe
		loc = time.UTC
	}
	return func(s *staticEvalCtxState) {
		s.typeCtx = s.typeCtx.WithLocation(loc)
	}
}

// WithErrLevelMap sets the error level map for the `StaticEvalContext`.
func WithErrLevelMap(level errctx.LevelMap) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.errCtx = s.errCtx.WithErrGroupLevels(level)
	}
}

// WithCurrentDB sets the current database name for the `StaticEvalContext`.
func WithCurrentDB(db string) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.currentDB = db
	}
}

// WithCurrentTime sets the current time for the `StaticEvalContext`.
func WithCurrentTime(fn func() (time.Time, error)) StaticEvalCtxOption {
	intest.AssertNotNil(fn)
	return func(s *staticEvalCtxState) {
		s.currentTime = &timeOnce{timeFn: fn}
	}
}

// WithMaxAllowedPacket sets the value of the 'max_allowed_packet' system variable.
func WithMaxAllowedPacket(size uint64) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.maxAllowedPacket = size
	}
}

// WithDefaultWeekFormatMode sets the value of the 'default_week_format' system variable.
func WithDefaultWeekFormatMode(mode string) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.defaultWeekFormatMode = mode
	}
}

// WithDivPrecisionIncrement sets the value of the 'div_precision_increment' system variable.
func WithDivPrecisionIncrement(inc int) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.divPrecisionIncrement = inc
	}
}

// WithPrivCheck sets the requestVerificationFn
func WithPrivCheck(fn func(db, table, column string, priv mysql.PrivilegeType) bool) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.requestVerificationFn = fn
	}
}

// WithDynamicPrivCheck sets the requestDynamicVerificationFn
func WithDynamicPrivCheck(fn func(privName string, grantable bool) bool) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.requestDynamicVerificationFn = fn
	}
}

// WithOptionalProperty sets the optional property providers
func WithOptionalProperty(providers ...exprctx.OptionalEvalPropProvider) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.props = contextopt.OptionalEvalPropProviders{}
		for _, p := range providers {
			s.props.Add(p)
		}
	}
}

// WithParamList sets the param list for the `StaticEvalContext`.
func WithParamList(params *variable.PlanCacheParamList) StaticEvalCtxOption {
	return func(s *staticEvalCtxState) {
		s.paramList = make([]types.Datum, len(params.AllParamValues()))
		for i, v := range params.AllParamValues() {
			s.paramList[i] = v
		}
	}
}

var defaultSQLMode = func() mysql.SQLMode {
	mode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	if err != nil {
		panic(err)
	}
	return mode
}()

// StaticEvalContext implements `EvalContext` to provide a static context for expression evaluation.
// The "static" means comparing with `SessionEvalContext`, its internal state does not relay on the session or other
// complex contexts that keeps immutable for most fields.
type StaticEvalContext struct {
	id uint64
	staticEvalCtxState
}

// NewStaticEvalContext creates a new `StaticEvalContext` with the given options.
func NewStaticEvalContext(opt ...StaticEvalCtxOption) *StaticEvalContext {
	ctx := &StaticEvalContext{
		id: contextutil.GenContextID(),
		staticEvalCtxState: staticEvalCtxState{
			currentTime:           &timeOnce{},
			sqlMode:               defaultSQLMode,
			maxAllowedPacket:      variable.DefMaxAllowedPacket,
			defaultWeekFormatMode: variable.DefDefaultWeekFormat,
			divPrecisionIncrement: variable.DefDivPrecisionIncrement,
		},
	}

	ctx.typeCtx = types.NewContext(types.StrictFlags, time.UTC, ctx)
	ctx.errCtx = errctx.NewContext(ctx)

	for _, o := range opt {
		o(&ctx.staticEvalCtxState)
	}

	if ctx.warnHandler == nil {
		ctx.warnHandler = contextutil.NewStaticWarnHandler(0)
	}

	return ctx
}

// CtxID returns the context ID.
func (ctx *StaticEvalContext) CtxID() uint64 {
	return ctx.id
}

// SQLMode returns the sql mode
func (ctx *StaticEvalContext) SQLMode() mysql.SQLMode {
	return ctx.sqlMode
}

// TypeCtx returns the types.Context
func (ctx *StaticEvalContext) TypeCtx() types.Context {
	return ctx.typeCtx
}

// ErrCtx returns the errctx.Context
func (ctx *StaticEvalContext) ErrCtx() errctx.Context {
	return ctx.errCtx
}

// Location returns the timezone info
func (ctx *StaticEvalContext) Location() *time.Location {
	return ctx.typeCtx.Location()
}

// AppendWarning append warnings to the context.
func (ctx *StaticEvalContext) AppendWarning(err error) {
	if h := ctx.warnHandler; h != nil {
		h.AppendWarning(err)
	}
}

// WarningCount gets warning count.
func (ctx *StaticEvalContext) WarningCount() int {
	if h := ctx.warnHandler; h != nil {
		return h.WarningCount()
	}
	return 0
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (ctx *StaticEvalContext) TruncateWarnings(start int) []contextutil.SQLWarn {
	if h := ctx.warnHandler; h != nil {
		return h.TruncateWarnings(start)
	}
	return nil
}

// CopyWarnings copies warnings to dst slice.
func (ctx *StaticEvalContext) CopyWarnings(dst []contextutil.SQLWarn) []contextutil.SQLWarn {
	if h := ctx.warnHandler; h != nil {
		return h.CopyWarnings(dst)
	}
	return nil
}

// CurrentDB return the current database name
func (ctx *StaticEvalContext) CurrentDB() string {
	return ctx.currentDB
}

// CurrentTime return the current time
func (ctx *StaticEvalContext) CurrentTime() (tm time.Time, err error) {
	return ctx.currentTime.getTime(ctx.Location())
}

// GetMaxAllowedPacket returns the value of the 'max_allowed_packet' system variable.
func (ctx *StaticEvalContext) GetMaxAllowedPacket() uint64 {
	return ctx.maxAllowedPacket
}

// GetDefaultWeekFormatMode returns the value of the 'default_week_format' system variable.
func (ctx *StaticEvalContext) GetDefaultWeekFormatMode() string {
	return ctx.defaultWeekFormatMode
}

// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
func (ctx *StaticEvalContext) GetDivPrecisionIncrement() int {
	return ctx.divPrecisionIncrement
}

// RequestVerification verifies user privilege
func (ctx *StaticEvalContext) RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool {
	if fn := ctx.requestVerificationFn; fn != nil {
		return fn(db, table, column, priv)
	}
	return true
}

// RequestDynamicVerification verifies user privilege for a DYNAMIC privilege.
func (ctx *StaticEvalContext) RequestDynamicVerification(privName string, grantable bool) bool {
	if fn := ctx.requestDynamicVerificationFn; fn != nil {
		return fn(privName, grantable)
	}
	return true
}

// GetOptionalPropSet gets the optional property set from context
func (ctx *StaticEvalContext) GetOptionalPropSet() exprctx.OptionalEvalPropKeySet {
	return ctx.props.PropKeySet()
}

// GetOptionalPropProvider gets the optional property provider by key
func (ctx *StaticEvalContext) GetOptionalPropProvider(key exprctx.OptionalEvalPropKey) (exprctx.OptionalEvalPropProvider, bool) {
	return ctx.props.Get(key)
}

// Apply returns a new `StaticEvalContext` with the fields updated according to the given options.
func (ctx *StaticEvalContext) Apply(opt ...StaticEvalCtxOption) *StaticEvalContext {
	newCtx := &StaticEvalContext{
		id:                 contextutil.GenContextID(),
		staticEvalCtxState: ctx.staticEvalCtxState,
	}

	// current time should use the previous one by default
	newCtx.currentTime = &timeOnce{timeFn: ctx.CurrentTime}

	// typeCtx and errCtx should be reset because warn handler changed
	newCtx.typeCtx = types.NewContext(ctx.typeCtx.Flags(), ctx.typeCtx.Location(), newCtx)
	newCtx.errCtx = errctx.NewContextWithLevels(ctx.errCtx.LevelMap(), newCtx)

	// Apply options
	for _, o := range opt {
		o(&newCtx.staticEvalCtxState)
	}

	return newCtx
}

// GetParamValue returns the value of the parameter by index.
func (ctx *StaticEvalContext) GetParamValue(idx int) types.Datum {
	if idx < 0 || idx >= len(ctx.paramList) {
		return types.Datum{}
	}
	return ctx.paramList[idx]
}
