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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// EvalContext should implement `EvalContext`
var _ exprctx.EvalContext = &EvalContext{}

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

// evalCtxState is the internal state for `EvalContext`.
// We make it as a standalone private struct here to make sure `EvalCtxOption` can only be called in constructor.
type evalCtxState struct {
	warnHandler                  contextutil.WarnHandler
	sqlMode                      mysql.SQLMode
	typeCtx                      types.Context
	errCtx                       errctx.Context
	currentDB                    string
	currentTime                  *timeOnce
	maxAllowedPacket             uint64
	enableRedactLog              string
	defaultWeekFormatMode        string
	divPrecisionIncrement        int
	requestVerificationFn        func(db, table, column string, priv mysql.PrivilegeType) bool
	requestDynamicVerificationFn func(privName string, grantable bool) bool
	paramList                    []types.Datum
	userVars                     variable.UserVarsReader
	props                        expropt.OptionalEvalPropProviders
}

// EvalCtxOption is the option to set `EvalContext`.
type EvalCtxOption func(*evalCtxState)

// WithWarnHandler sets the warn handler for the `EvalContext`.
func WithWarnHandler(h contextutil.WarnHandler) EvalCtxOption {
	intest.AssertNotNil(h)
	if h == nil {
		// this should not happen, just to keep code safe
		h = contextutil.IgnoreWarn
	}

	return func(s *evalCtxState) {
		s.warnHandler = h
	}
}

// WithSQLMode sets the sql mode for the `EvalContext`.
func WithSQLMode(sqlMode mysql.SQLMode) EvalCtxOption {
	return func(s *evalCtxState) {
		s.sqlMode = sqlMode
	}
}

// WithTypeFlags sets the type flags for the `EvalContext`.
func WithTypeFlags(flags types.Flags) EvalCtxOption {
	return func(s *evalCtxState) {
		s.typeCtx = s.typeCtx.WithFlags(flags)
	}
}

// WithLocation sets the timezone info for the `EvalContext`.
func WithLocation(loc *time.Location) EvalCtxOption {
	intest.AssertNotNil(loc)
	if loc == nil {
		// this should not happen, just to keep code safe
		loc = time.UTC
	}
	return func(s *evalCtxState) {
		s.typeCtx = s.typeCtx.WithLocation(loc)
	}
}

// WithErrLevelMap sets the error level map for the `EvalContext`.
func WithErrLevelMap(level errctx.LevelMap) EvalCtxOption {
	return func(s *evalCtxState) {
		s.errCtx = s.errCtx.WithErrGroupLevels(level)
	}
}

// WithCurrentDB sets the current database name for the `EvalContext`.
func WithCurrentDB(db string) EvalCtxOption {
	return func(s *evalCtxState) {
		s.currentDB = db
	}
}

// WithCurrentTime sets the current time for the `EvalContext`.
func WithCurrentTime(fn func() (time.Time, error)) EvalCtxOption {
	intest.AssertNotNil(fn)
	return func(s *evalCtxState) {
		s.currentTime = &timeOnce{timeFn: fn}
	}
}

// WithMaxAllowedPacket sets the value of the 'max_allowed_packet' system variable.
func WithMaxAllowedPacket(size uint64) EvalCtxOption {
	return func(s *evalCtxState) {
		s.maxAllowedPacket = size
	}
}

// WithDefaultWeekFormatMode sets the value of the 'default_week_format' system variable.
func WithDefaultWeekFormatMode(mode string) EvalCtxOption {
	return func(s *evalCtxState) {
		s.defaultWeekFormatMode = mode
	}
}

// WithDivPrecisionIncrement sets the value of the 'div_precision_increment' system variable.
func WithDivPrecisionIncrement(inc int) EvalCtxOption {
	return func(s *evalCtxState) {
		s.divPrecisionIncrement = inc
	}
}

// WithPrivCheck sets the requestVerificationFn
func WithPrivCheck(fn func(db, table, column string, priv mysql.PrivilegeType) bool) EvalCtxOption {
	return func(s *evalCtxState) {
		s.requestVerificationFn = fn
	}
}

// WithDynamicPrivCheck sets the requestDynamicVerificationFn
func WithDynamicPrivCheck(fn func(privName string, grantable bool) bool) EvalCtxOption {
	return func(s *evalCtxState) {
		s.requestDynamicVerificationFn = fn
	}
}

// WithOptionalProperty sets the optional property providers
func WithOptionalProperty(providers ...exprctx.OptionalEvalPropProvider) EvalCtxOption {
	return func(s *evalCtxState) {
		s.props = expropt.OptionalEvalPropProviders{}
		for _, p := range providers {
			s.props.Add(p)
		}
	}
}

// WithParamList sets the param list for the `EvalContext`.
func WithParamList(params *variable.PlanCacheParamList) EvalCtxOption {
	return func(s *evalCtxState) {
		s.paramList = make([]types.Datum, len(params.AllParamValues()))
		for i, v := range params.AllParamValues() {
			s.paramList[i] = v
		}
	}
}

// WithEnableRedactLog sets the value of the 'tidb_redact_log' system variable.
func WithEnableRedactLog(enableRedactLog string) EvalCtxOption {
	return func(s *evalCtxState) {
		s.enableRedactLog = enableRedactLog
	}
}

// WithUserVarsReader set the user variables reader for the `EvalContext`.
func WithUserVarsReader(vars variable.UserVarsReader) EvalCtxOption {
	return func(s *evalCtxState) {
		s.userVars = vars
	}
}

var defaultSQLMode = func() mysql.SQLMode {
	mode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	if err != nil {
		panic(err)
	}
	return mode
}()

// EvalContext implements `EvalContext` to provide a static context for expression evaluation.
// The "static" means comparing with `EvalContext`, its internal state does not relay on the session or other
// complex contexts that keeps immutable for most fields.
type EvalContext struct {
	id uint64
	evalCtxState
}

// NewEvalContext creates a new `EvalContext` with the given options.
func NewEvalContext(opt ...EvalCtxOption) *EvalContext {
	ctx := &EvalContext{
		id: contextutil.GenContextID(),
		evalCtxState: evalCtxState{
			currentTime:           &timeOnce{},
			sqlMode:               defaultSQLMode,
			maxAllowedPacket:      variable.DefMaxAllowedPacket,
			enableRedactLog:       variable.DefTiDBRedactLog,
			defaultWeekFormatMode: variable.DefDefaultWeekFormat,
			divPrecisionIncrement: variable.DefDivPrecisionIncrement,
		},
	}

	ctx.typeCtx = types.NewContext(types.StrictFlags, time.UTC, ctx)
	ctx.errCtx = errctx.NewContext(ctx)

	for _, o := range opt {
		o(&ctx.evalCtxState)
	}

	if ctx.warnHandler == nil {
		ctx.warnHandler = contextutil.NewStaticWarnHandler(0)
	}

	if ctx.userVars == nil {
		ctx.userVars = variable.NewUserVars()
	}

	return ctx
}

// CtxID returns the context ID.
func (ctx *EvalContext) CtxID() uint64 {
	return ctx.id
}

// SQLMode returns the sql mode
func (ctx *EvalContext) SQLMode() mysql.SQLMode {
	return ctx.sqlMode
}

// TypeCtx returns the types.Context
func (ctx *EvalContext) TypeCtx() types.Context {
	return ctx.typeCtx
}

// ErrCtx returns the errctx.Context
func (ctx *EvalContext) ErrCtx() errctx.Context {
	return ctx.errCtx
}

// Location returns the timezone info
func (ctx *EvalContext) Location() *time.Location {
	return ctx.typeCtx.Location()
}

// AppendWarning append warnings to the context.
func (ctx *EvalContext) AppendWarning(err error) {
	if h := ctx.warnHandler; h != nil {
		h.AppendWarning(err)
	}
}

// AppendNote appends notes to the context.
func (ctx *EvalContext) AppendNote(err error) {
	if h := ctx.warnHandler; h != nil {
		h.AppendNote(err)
	}
}

// WarningCount gets warning count.
func (ctx *EvalContext) WarningCount() int {
	if h := ctx.warnHandler; h != nil {
		return h.WarningCount()
	}
	return 0
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (ctx *EvalContext) TruncateWarnings(start int) []contextutil.SQLWarn {
	if h := ctx.warnHandler; h != nil {
		return h.TruncateWarnings(start)
	}
	return nil
}

// CopyWarnings copies warnings to dst slice.
func (ctx *EvalContext) CopyWarnings(dst []contextutil.SQLWarn) []contextutil.SQLWarn {
	if h := ctx.warnHandler; h != nil {
		return h.CopyWarnings(dst)
	}
	return nil
}

// CurrentDB return the current database name
func (ctx *EvalContext) CurrentDB() string {
	return ctx.currentDB
}

// CurrentTime return the current time
func (ctx *EvalContext) CurrentTime() (tm time.Time, err error) {
	return ctx.currentTime.getTime(ctx.Location())
}

// GetMaxAllowedPacket returns the value of the 'max_allowed_packet' system variable.
func (ctx *EvalContext) GetMaxAllowedPacket() uint64 {
	return ctx.maxAllowedPacket
}

// GetTiDBRedactLog returns the value of the 'tidb_redact_log' system variable.
func (ctx *EvalContext) GetTiDBRedactLog() string {
	return ctx.enableRedactLog
}

// GetDefaultWeekFormatMode returns the value of the 'default_week_format' system variable.
func (ctx *EvalContext) GetDefaultWeekFormatMode() string {
	return ctx.defaultWeekFormatMode
}

// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
func (ctx *EvalContext) GetDivPrecisionIncrement() int {
	return ctx.divPrecisionIncrement
}

// GetUserVarsReader returns the user variables.
func (ctx *EvalContext) GetUserVarsReader() variable.UserVarsReader {
	return ctx.userVars
}

// RequestVerification verifies user privilege
func (ctx *EvalContext) RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool {
	if fn := ctx.requestVerificationFn; fn != nil {
		return fn(db, table, column, priv)
	}
	return true
}

// RequestDynamicVerification verifies user privilege for a DYNAMIC privilege.
func (ctx *EvalContext) RequestDynamicVerification(privName string, grantable bool) bool {
	if fn := ctx.requestDynamicVerificationFn; fn != nil {
		return fn(privName, grantable)
	}
	return true
}

// GetOptionalPropSet gets the optional property set from context
func (ctx *EvalContext) GetOptionalPropSet() exprctx.OptionalEvalPropKeySet {
	return ctx.props.PropKeySet()
}

// GetOptionalPropProvider gets the optional property provider by key
func (ctx *EvalContext) GetOptionalPropProvider(key exprctx.OptionalEvalPropKey) (exprctx.OptionalEvalPropProvider, bool) {
	return ctx.props.Get(key)
}

// Apply returns a new `EvalContext` with the fields updated according to the given options.
func (ctx *EvalContext) Apply(opt ...EvalCtxOption) *EvalContext {
	newCtx := &EvalContext{
		id:           contextutil.GenContextID(),
		evalCtxState: ctx.evalCtxState,
	}

	// current time should use the previous one by default
	newCtx.currentTime = &timeOnce{timeFn: ctx.CurrentTime}

	// typeCtx and errCtx should be reset because warn handler changed
	newCtx.typeCtx = types.NewContext(ctx.typeCtx.Flags(), ctx.typeCtx.Location(), newCtx)
	newCtx.errCtx = errctx.NewContextWithLevels(ctx.errCtx.LevelMap(), newCtx)

	// Apply options
	for _, o := range opt {
		o(&newCtx.evalCtxState)
	}

	return newCtx
}

// GetParamValue returns the value of the parameter by index.
func (ctx *EvalContext) GetParamValue(idx int) (types.Datum, error) {
	if idx < 0 || idx >= len(ctx.paramList) {
		return types.Datum{}, exprctx.ErrParamIndexExceedParamCounts
	}
	return ctx.paramList[idx], nil
}

var _ exprctx.StaticConvertibleEvalContext = &EvalContext{}

// AllParamValues implements context.StaticConvertibleEvalContext.
func (ctx *EvalContext) AllParamValues() []types.Datum {
	return ctx.paramList
}

// GetDynamicPrivCheckFn implements context.StaticConvertibleEvalContext.
func (ctx *EvalContext) GetDynamicPrivCheckFn() func(privName string, grantable bool) bool {
	return ctx.requestDynamicVerificationFn
}

// GetRequestVerificationFn implements context.StaticConvertibleEvalContext.
func (ctx *EvalContext) GetRequestVerificationFn() func(db string, table string, column string, priv mysql.PrivilegeType) bool {
	return ctx.requestVerificationFn
}

// GetWarnHandler implements context.StaticConvertibleEvalContext.
func (ctx *EvalContext) GetWarnHandler() contextutil.WarnHandler {
	return ctx.warnHandler
}

// LoadSystemVars loads system variables and returns a new `EvalContext` with system variables loaded.
func (ctx *EvalContext) LoadSystemVars(sysVars map[string]string) (*EvalContext, error) {
	sessionVars, err := newSessionVarsWithSystemVariables(sysVars)
	if err != nil {
		return nil, err
	}
	return ctx.loadSessionVarsInternal(sessionVars, sysVars), nil
}

func (ctx *EvalContext) loadSessionVarsInternal(
	sessionVars *variable.SessionVars, sysVars map[string]string,
) *EvalContext {
	opts := make([]EvalCtxOption, 0, 8)
	for name, val := range sysVars {
		name = strings.ToLower(name)
		switch name {
		case variable.TimeZone:
			opts = append(opts, WithLocation(sessionVars.Location()))
		case variable.SQLModeVar:
			opts = append(opts, WithSQLMode(sessionVars.SQLMode))
		case variable.Timestamp:
			opts = append(opts, WithCurrentTime(ctx.currentTimeFuncFromStringVal(val)))
		case variable.MaxAllowedPacket:
			opts = append(opts, WithMaxAllowedPacket(sessionVars.MaxAllowedPacket))
		case variable.TiDBRedactLog:
			opts = append(opts, WithEnableRedactLog(sessionVars.EnableRedactLog))
		case variable.DefaultWeekFormat:
			opts = append(opts, WithDefaultWeekFormatMode(val))
		case variable.DivPrecisionIncrement:
			opts = append(opts, WithDivPrecisionIncrement(sessionVars.DivPrecisionIncrement))
		}
	}
	return ctx.Apply(opts...)
}

func (ctx *EvalContext) currentTimeFuncFromStringVal(val string) func() (time.Time, error) {
	return func() (time.Time, error) {
		if val == variable.DefTimestamp {
			return time.Now(), nil
		}

		ts, err := types.StrToFloat(types.StrictContext, val, false)
		if err != nil {
			return time.Time{}, err
		}
		seconds, fractionalSeconds := math.Modf(ts)
		return time.Unix(int64(seconds), int64(fractionalSeconds*float64(time.Second))), nil
	}
}

func newSessionVarsWithSystemVariables(vars map[string]string) (*variable.SessionVars, error) {
	sessionVars := variable.NewSessionVars(nil)
	var cs, col []string
	for name, val := range vars {
		switch strings.ToLower(name) {
		// `charset_connection` and `collation_connection` will overwrite each other.
		// To make the result more determinate, just set them at last step in order:
		// `charset_connection` first, then `collation_connection`.
		case variable.CharacterSetConnection:
			cs = []string{name, val}
		case variable.CollationConnection:
			col = []string{name, val}
		default:
			if err := sessionVars.SetSystemVar(name, val); err != nil {
				return nil, err
			}
		}
	}

	if cs != nil {
		if err := sessionVars.SetSystemVar(cs[0], cs[1]); err != nil {
			return nil, err
		}
	}

	if col != nil {
		if err := sessionVars.SetSystemVar(col[0], col[1]); err != nil {
			return nil, err
		}
	}

	return sessionVars, nil
}

// MakeEvalContextStatic converts the `exprctx.StaticConvertibleEvalContext` to `EvalContext`.
func MakeEvalContextStatic(ctx exprctx.StaticConvertibleEvalContext) *EvalContext {
	typeCtx := ctx.TypeCtx()
	errCtx := ctx.ErrCtx()

	// TODO: at least provide some optional eval prop provider which is suitable to be used in the static context.
	props := make([]exprctx.OptionalEvalPropProvider, 0, exprctx.OptPropsCnt)

	params := variable.NewPlanCacheParamList()
	for _, param := range ctx.AllParamValues() {
		params.Append(param)
	}

	// TODO: use a more structural way to replace the closure.
	// These closure makes sure the fields which may be changed in the execution of the next statement will not be embedded into them, to make
	// sure it's safe to call them after the session continues to execute other statements.
	staticCtx := NewEvalContext(
		WithWarnHandler(ctx.GetWarnHandler()),
		WithSQLMode(ctx.SQLMode()),
		WithTypeFlags(typeCtx.Flags()),
		WithLocation(typeCtx.Location()),
		WithErrLevelMap(errCtx.LevelMap()),
		WithCurrentDB(ctx.CurrentDB()),
		WithCurrentTime(func() func() (time.Time, error) {
			currentTime, currentTimeErr := ctx.CurrentTime()

			return func() (time.Time, error) {
				return currentTime, currentTimeErr
			}
		}()),
		WithMaxAllowedPacket(ctx.GetMaxAllowedPacket()),
		WithDefaultWeekFormatMode(ctx.GetDefaultWeekFormatMode()),
		WithDivPrecisionIncrement(ctx.GetDivPrecisionIncrement()),
		WithPrivCheck(ctx.GetRequestVerificationFn()),
		WithDynamicPrivCheck(ctx.GetDynamicPrivCheckFn()),
		WithParamList(params),
		WithUserVarsReader(ctx.GetUserVarsReader().Clone()),
		WithOptionalProperty(props...),
		WithEnableRedactLog(ctx.GetTiDBRedactLog()),
	)

	return staticCtx
}
