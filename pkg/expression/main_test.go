// Copyright 2021 PingCAP, Inc.
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

package expression

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	testmain.ShortCircuitForBench(m)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()

	// Some test depends on the values of timeutil.SystemLocation()
	// If we don't SetSystemTZ() here, the value would change unpredictable.
	// Affected by the order whether a testsuite runs before or after integration test.
	// Note, SetSystemTZ() is a sync.Once operation.
	timeutil.SetSystemTZ("system")

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func mockEvalCtx(opts ...any) *contextstatic.StaticEvalContext {
	evalOpts := make([]contextstatic.StaticEvalCtxOption, 0, len(opts))
	optionalProps := make([]OptionalEvalPropProvider, 0, len(opts))
	var sessionVars *variable.SessionVars
	for _, opt := range opts {
		if o, ok := opt.(contextstatic.StaticEvalCtxOption); ok {
			evalOpts = append(evalOpts, o)
			continue
		}

		if vars, ok := opt.(*variable.SessionVars); ok {
			sessionVars = vars
			optionalProps = append(optionalProps,
				contextopt.NewSessionVarsProvider(contextopt.SessionVarsAsProvider(vars)),
				contextopt.CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) {
					return vars.User, vars.ActiveRoles
				}),
			)
			continue
		}

		if p, ok := opt.(OptionalEvalPropProvider); ok {
			optionalProps = append(optionalProps, p)
			continue
		}

		panic(fmt.Sprintf("unexpected option type: %T", opt))
	}

	if len(optionalProps) > 0 {
		evalOpts = append(evalOpts, contextstatic.WithOptionalProperty(optionalProps...))
	}

	ctx := contextstatic.NewStaticEvalContext(
		// sets default time zone to UTC+11 value to make it different with most CI and development environments and forbid
		// some tests are success in some environments but failed in some others.
		contextstatic.WithLocation(time.FixedZone("UTC+11", 11*3600)),
	)
	if len(opts) > 0 {
		ctx = ctx.Apply(evalOpts...)
	}

	if sessionVars != nil {
		// keep timezone all the same to pass some `intest.Assert` for location assertion
		sessionVars.TimeZone = ctx.Location()
		sessionVars.StmtCtx.SetTimeZone(ctx.Location())
	}

	return ctx
}

func mockStmtTruncateAsWarningExprCtx(opts ...any) *contextstatic.StaticExprContext {
	flags := types.DefaultStmtFlags.WithTruncateAsWarning(true)
	levelMap := stmtctx.DefaultStmtErrLevels
	levelMap[errctx.ErrGroupTruncate] = errctx.LevelWarn
	opts = append([]any{
		contextstatic.WithTypeFlags(flags),
		contextstatic.WithErrLevelMap(levelMap),
	}, opts...)

	return mockExprCtx(opts...)
}

func mockStmtIgnoreTruncateExprCtx(opts ...any) *contextstatic.StaticExprContext {
	flags := types.DefaultStmtFlags.WithIgnoreTruncateErr(true)
	levelMap := stmtctx.DefaultStmtErrLevels
	levelMap[errctx.ErrGroupTruncate] = errctx.LevelIgnore
	opts = append([]any{
		contextstatic.WithTypeFlags(flags),
		contextstatic.WithErrLevelMap(levelMap),
	}, opts...)
	return mockExprCtx(opts...)
}

func mockStmtExprCtx(opts ...any) *contextstatic.StaticExprContext {
	opts = append([]any{
		contextstatic.WithTypeFlags(types.DefaultStmtFlags),
		contextstatic.WithErrLevelMap(stmtctx.DefaultStmtErrLevels),
	}, opts...)
	return mockExprCtx(opts...)
}

func mockExprCtx(opts ...any) *contextstatic.StaticExprContext {
	exprOptions := make([]contextstatic.StaticExprCtxOption, 0, len(opts))
	otherOpts := make([]any, 0, len(opts))
	for _, opt := range opts {
		if o, ok := opt.(contextstatic.StaticExprCtxOption); ok {
			exprOptions = append(exprOptions, o)
			continue
		}
		otherOpts = append(otherOpts, opt)
	}

	ctx := contextstatic.NewStaticExprContext(contextstatic.WithEvalCtx(mockEvalCtx(otherOpts...)))
	if len(exprOptions) > 0 {
		ctx = ctx.Apply(exprOptions...)
	}
	return ctx
}

func applyExprCtx(ctx *contextstatic.StaticExprContext, opts ...any) *contextstatic.StaticExprContext {
	evalOptions := make([]contextstatic.StaticEvalCtxOption, 0, len(opts))
	exprOptions := make([]contextstatic.StaticExprCtxOption, 0, len(opts))
	for _, opt := range opts {
		if o, ok := opt.(contextstatic.StaticEvalCtxOption); ok {
			evalOptions = append(evalOptions, o)
			continue
		}

		if o, ok := opt.(contextstatic.StaticExprCtxOption); ok {
			exprOptions = append(exprOptions, o)
			continue
		}

		panic(fmt.Sprintf("unexpected option type: %T", opt))
	}

	if len(evalOptions) > 0 {
		exprOptions = append(
			exprOptions,
			contextstatic.WithEvalCtx(
				ctx.GetEvalCtx().(*contextstatic.StaticEvalContext).Apply(evalOptions...),
			),
		)
	}

	return ctx.Apply(exprOptions...)
}
