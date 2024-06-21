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

package context_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextstatic"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCtxWithHandleTruncateErrLevel(t *testing.T) {
	for _, level := range []errctx.Level{errctx.LevelWarn, errctx.LevelIgnore, errctx.LevelError} {
		originalLevelMap := errctx.LevelMap{errctx.ErrGroupDividedByZero: errctx.LevelError}
		expectedLevelMap := originalLevelMap
		expectedLevelMap[errctx.ErrGroupTruncate] = level

		originalFlags := types.DefaultStmtFlags
		originalLoc := time.FixedZone("tz1", 3600*2)
		var expectedFlags types.Flags
		switch level {
		case errctx.LevelError:
			originalFlags = originalFlags.WithTruncateAsWarning(true)
			originalLevelMap[errctx.ErrGroupTruncate] = errctx.LevelWarn
			expectedFlags = originalFlags.WithTruncateAsWarning(false)
		case errctx.LevelWarn:
			expectedFlags = originalFlags.WithTruncateAsWarning(true)
		case errctx.LevelIgnore:
			expectedFlags = originalFlags.WithIgnoreTruncateErr(true)
		default:
			require.FailNow(t, "unexpected level")
		}

		evalCtx := contextstatic.NewStaticEvalContext(
			contextstatic.WithTypeFlags(originalFlags),
			contextstatic.WithLocation(originalLoc),
			contextstatic.WithErrLevelMap(errctx.LevelMap{errctx.ErrGroupTruncate: level}),
		)

		tc, ec := evalCtx.TypeCtx(), evalCtx.ErrCtx()
		ctx := contextstatic.NewStaticExprContext(
			contextstatic.WithEvalCtx(evalCtx),
			contextstatic.WithConnectionID(1234),
		)

		// override should take effect
		newCtx := context.CtxWithHandleTruncateErrLevel(ctx, level)
		newEvalCtx := newCtx.GetEvalCtx()
		newTypeCtx, newErrCtx := newEvalCtx.TypeCtx(), newEvalCtx.ErrCtx()
		require.Equal(t, expectedFlags, newTypeCtx.Flags())
		require.Equal(t, expectedLevelMap, newErrCtx.LevelMap())

		// other fields should not change
		require.Equal(t, originalLoc, newTypeCtx.Location())
		require.Equal(t, originalLoc, newEvalCtx.Location())
		require.Equal(t, uint64(1234), newCtx.ConnectionID())

		// old ctx should not change
		require.Same(t, evalCtx, ctx.GetEvalCtx())
		require.Equal(t, tc, evalCtx.TypeCtx())
		require.Equal(t, ec, evalCtx.ErrCtx())
		require.Same(t, originalLoc, evalCtx.Location())
		require.Equal(t, uint64(1234), ctx.ConnectionID())

		// not create new ctx case
		newCtx2 := context.CtxWithHandleTruncateErrLevel(newCtx, level)
		require.Same(t, newCtx, newCtx2)
	}
}
