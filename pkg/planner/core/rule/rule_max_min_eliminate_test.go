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

package rule

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/stretchr/testify/require"
)

func TestMaxMinEliminateSkipsEmptyScalarAgg(t *testing.T) {
	sctx := coretestsdk.MockContext()
	agg := logicalop.LogicalAggregation{}.Init(sctx, 0)
	opt := &MaxMinEliminator{}

	require.NotPanics(t, func() {
		p, changed, err := opt.Optimize(context.Background(), agg)
		require.NoError(t, err)
		require.False(t, changed)
		require.Same(t, agg, p)
	})
}
