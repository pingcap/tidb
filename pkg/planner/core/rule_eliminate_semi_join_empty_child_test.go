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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestEliminateSemiJoinEmptyChildRequiresFromSetOperator constructs a
// SemiJoin plan tree directly (bypassing SQL-level rewrites such as IN
// decorrelating to InnerJoin, or OR-combined EXISTS becoming
// LeftOuterSemiJoin, which make it hard to reach this rule with an ordinary,
// non-set-operator SemiJoin over an empty child through SQL alone) to prove
// EliminateSemiJoinEmptyChild only rewrites joins built for INTERSECT/EXCEPT.
func TestEliminateSemiJoinEmptyChildRequiresFromSetOperator(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()

	newDual := func() *logicalop.LogicalTableDual {
		dual := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
		dual.SetSchema(expression.NewSchema(&expression.Column{UniqueID: 1}))
		return dual
	}
	newNonEmptyLeft := func() base.LogicalPlan {
		dual := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
		dual.SetSchema(expression.NewSchema(&expression.Column{UniqueID: 2}))
		return dual
	}
	newJoin := func(fromSetOperator bool, left, right base.LogicalPlan) *logicalop.LogicalJoin {
		join := logicalop.LogicalJoin{JoinType: base.SemiJoin, FromSetOperator: fromSetOperator}.Init(ctx, 0)
		join.SetChildren(left, right)
		join.SetSchema(left.Schema())
		join.SetOutputNames(make(types.NameSlice, left.Schema().Len()))
		return join
	}

	t.Run("ordinary semi-join with empty child is left untouched", func(t *testing.T) {
		join := newJoin(false, newNonEmptyLeft(), newDual())
		got, changed := eliminateSemiJoinEmptyChild(join)
		require.False(t, changed)
		require.Same(t, join, got)
	})

	t.Run("set-operator semi-join with empty child folds to dual", func(t *testing.T) {
		join := newJoin(true, newNonEmptyLeft(), newDual())
		got, changed := eliminateSemiJoinEmptyChild(join)
		require.True(t, changed)
		dual, ok := got.(*logicalop.LogicalTableDual)
		require.True(t, ok)
		require.Equal(t, 0, dual.RowCount)
	})

	t.Run("set-operator semi-join with no empty child is left untouched", func(t *testing.T) {
		join := newJoin(true, newNonEmptyLeft(), newNonEmptyLeft())
		got, changed := eliminateSemiJoinEmptyChild(join)
		require.False(t, changed)
		require.Same(t, join, got)
	})

	t.Run("ordinary semi-join nested under a set-operator join is unaffected by the outer flag", func(t *testing.T) {
		inner := newJoin(false, newNonEmptyLeft(), newDual())
		outer := newJoin(true, inner, newNonEmptyLeft())
		got, changed := eliminateSemiJoinEmptyChild(outer)
		require.False(t, changed)
		gotOuter, ok := got.(*logicalop.LogicalJoin)
		require.True(t, ok)
		require.Same(t, inner, gotOuter.Children()[0])
	})
}
