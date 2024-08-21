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

package errctx_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
)

func TestContext(t *testing.T) {
	var warn error
	ctx := errctx.NewContext(contextutil.NewFuncWarnAppenderForTest(func(err error) {
		warn = err
	}))

	testInternalErr := types.ErrOverflow
	testErr := errors.New("error")
	testWarn := errors.New("warn")
	// by default, all errors will be returned directly
	require.Equal(t, ctx.HandleErrorWithAlias(testInternalErr, testErr, testWarn), testErr)

	// set level to "warn"
	newCtx := ctx.WithErrGroupLevel(errctx.ErrGroupTruncate, errctx.LevelWarn)
	// ctx is not affected
	require.Equal(t, ctx.HandleErrorWithAlias(testInternalErr, testErr, testWarn), testErr)
	// newCtx will handle the error as a warn
	require.NoError(t, newCtx.HandleErrorWithAlias(testInternalErr, testErr, testWarn))
	require.Equal(t, warn, testWarn)
	levels := newCtx.LevelMap()
	for i := errctx.ErrGroup(0); i <= errctx.ErrGroup(len(levels)-1); i++ {
		if i == errctx.ErrGroupTruncate {
			require.Equal(t, errctx.LevelWarn, levels[i])
		} else {
			require.Equal(t, errctx.LevelError, levels[i])
			require.Equal(t, levels[i], newCtx.LevelForGroup(i))
		}
	}

	warn = nil
	newCtx2 := newCtx.WithStrictErrGroupLevel()
	// newCtx is not affected
	require.NoError(t, newCtx.HandleErrorWithAlias(testInternalErr, testErr, testWarn))
	require.Equal(t, warn, testWarn)
	// newCtx2 will return all errors
	require.Equal(t, newCtx2.HandleErrorWithAlias(testInternalErr, testErr, testWarn), testErr)
	require.Equal(t, errctx.LevelMap{}, newCtx2.LevelMap())

	// test `multierr`
	testErrs := multierr.Append(testInternalErr, testErr)
	require.Equal(t, ctx.HandleError(testErrs), testInternalErr)
	require.Equal(t, newCtx.HandleError(testErrs), testErr)
	require.Equal(t, warn, testInternalErr)

	// test nil
	require.Nil(t, ctx.HandleError(nil))

	// test with a level map
	levels = errctx.LevelMap{}
	levels[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelWarn
	ctx = errctx.NewContextWithLevels(levels, contextutil.NewFuncWarnAppenderForTest(func(err error) {
		warn = err
	}))
	require.Equal(t, levels, ctx.LevelMap())
	levels2 := errctx.LevelMap{}
	levels2[errctx.ErrGroupAutoIncReadFailed] = errctx.LevelIgnore
	ctx = ctx.WithErrGroupLevels(levels2)
	require.Equal(t, levels2, ctx.LevelMap())

	// original levels should not change
	ctx = ctx.WithErrGroupLevels(errctx.LevelMap{})
	require.Equal(t, errctx.LevelMap{}, ctx.LevelMap())
	require.Equal(t, errctx.LevelWarn, levels[errctx.ErrGroupAutoIncReadFailed])
	require.Equal(t, errctx.LevelIgnore, levels2[errctx.ErrGroupAutoIncReadFailed])
}
