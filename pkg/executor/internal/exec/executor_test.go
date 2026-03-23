// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockNextIOAccExecutor struct {
	BaseExecutorV2
}

func newMockNextIOAccExecutor(children ...Executor) *mockNextIOAccExecutor {
	ctx := mock.NewContext()
	return &mockNextIOAccExecutor{
		BaseExecutorV2: NewBaseExecutorV2(ctx.GetSessionVars(), nil, 0, children...),
	}
}

func TestNextIOAccAddInputCountsRowsWithZeroCols(t *testing.T) {
	t.Run("add input counts rows with zero cols", func(t *testing.T) {
		acc := &nextIOAcc{}

		acc.addInput(3, 0)

		require.Equal(t, int64(3), acc.inRows)
		require.Equal(t, int64(0), acc.inCells)
	})

	t.Run("base executor reuses local accumulator state", func(t *testing.T) {
		exec := newMockNextIOAccExecutor()

		first := getReusableNextIOAcc(exec)
		first.addInput(4, 2)

		second := getReusableNextIOAcc(exec)
		require.Same(t, first, second)
		require.Equal(t, int64(0), second.inRows)
		require.Equal(t, int64(0), second.inCells)

		allocs := testing.AllocsPerRun(1000, func() {
			acc := getReusableNextIOAcc(exec)
			acc.addInput(1, 1)
		})
		require.Less(t, allocs, 1.)
	})

	t.Run("only executors with children need local accumulator", func(t *testing.T) {
		parentAcc := &nextIOAcc{}

		require.False(t, needNextIOAcc(true, nil, 0))
		require.True(t, needNextIOAcc(true, nil, 1))
		require.False(t, needNextIOAcc(false, parentAcc, 0))
		require.True(t, needNextIOAcc(false, parentAcc, 1))
	})
}
