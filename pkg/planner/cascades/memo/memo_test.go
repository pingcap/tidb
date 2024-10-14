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

package memo

import (
	"testing"

	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/stretchr/testify/require"
)

func TestMemo(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	memo := NewMemo(ctx)
	memo.Init(join)
	require.Equal(t, 3, memo.groups.Len())
	require.Equal(t, 3, len(memo.groupID2Group))

	// iter memo.groups to assert group ids.
	cnt := 1
	for e := memo.groups.Front(); e != nil; e = e.Next() {
		group := e.Value.(*Group)
		require.NotNil(t, group)
		require.Equal(t, GroupID(cnt), group.groupID)
		cnt++
	}
}

func TestInsertGE(t *testing.T) {
	ctx := plannercore.MockContext()
	t1 := logicalop.DataSource{}.Init(ctx, 0)
	t2 := logicalop.DataSource{}.Init(ctx, 0)
	join := logicalop.LogicalJoin{}.Init(ctx, 0)
	join.SetChildren(t1, t2)

	memo := NewMemo(ctx)
	memo.Init(join)
	require.Equal(t, 3, memo.groups.Len())
	require.Equal(t, 3, len(memo.groupID2Group))

	// prepare a new group expression with join's group as its children.
	limit := logicalop.LogicalLimit{}.Init(ctx, 0)
	hasher := memo.GetHasher()
	groupExpr := NewGroupExpression(limit, []*Group{memo.rootGroup})
	groupExpr.Init(hasher)

	// Insert a new group with a new expression.
	memo.insertGroupExpression(groupExpr, nil)
	require.Equal(t, 4, memo.groups.Len())
	require.Equal(t, 4, len(memo.groupID2Group))

	// iter memo.groups to assert group ids.
	cnt := 1
	for e := memo.groups.Front(); e != nil; e = e.Next() {
		group := e.Value.(*Group)
		require.NotNil(t, group)
		require.Equal(t, GroupID(cnt), group.groupID)
		cnt++
	}
	require.Equal(t, memo.groups.Back().Value.(*Group).groupID, GroupID(cnt-1))
}
