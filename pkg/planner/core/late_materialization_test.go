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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/stretchr/testify/require"
)

func TestLateMaterializationRegionReplayValidation(t *testing.T) {
	ctx := coretestsdk.MockContext()
	makeRegion := func(leftID, rightID int64, joinType base.JoinType) *lateMaterializeRegion {
		left := logicalop.DataSource{TableInfo: &model.TableInfo{ID: leftID}}.Init(ctx, 0)
		right := logicalop.DataSource{TableInfo: &model.TableInfo{ID: rightID}}.Init(ctx, 0)
		join := logicalop.LogicalJoin{JoinType: joinType}.Init(ctx, 0)
		join.SetChildren(left, right)
		return &lateMaterializeRegion{root: join, targets: []*lateMaterializeTarget{{ds: left}, {ds: right}}}
	}
	original := makeRegion(101, 102, base.InnerJoin)
	proposal := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
		region: 0, targets: []int{0}, shape: lateMaterializationRegionShape(original.root), targetTables: []int64{101},
	}}}
	// Reallocated logical node IDs do not affect the table/join check.
	rebuilt := makeRegion(101, 102, base.InnerJoin)
	require.NotEqual(t, original.root.ID(), rebuilt.root.ID())
	require.NoError(t, proposal.validate([]*lateMaterializeRegion{rebuilt}))

	t.Run("same count swapped regions", func(t *testing.T) {
		other := makeRegion(201, 202, base.InnerJoin)
		require.NoError(t, proposal.validate([]*lateMaterializeRegion{rebuilt, other}))
		require.EqualError(t, proposal.validate([]*lateMaterializeRegion{other, rebuilt}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("swapped table positions", func(t *testing.T) {
		require.EqualError(t, proposal.validate([]*lateMaterializeRegion{makeRegion(102, 101, base.InnerJoin)}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("changed join type", func(t *testing.T) {
		require.EqualError(t, proposal.validate([]*lateMaterializeRegion{makeRegion(101, 102, base.LeftOuterJoin)}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("changed join nesting", func(t *testing.T) {
		leftDeep := makeRegion(101, 102, base.InnerJoin)
		root := makeRegion(104, 103, base.InnerJoin)
		root.root.SetChild(0, leftDeep.root)
		rightDeep := makeRegion(102, 103, base.InnerJoin)
		otherRoot := makeRegion(101, 104, base.InnerJoin)
		otherRoot.root.SetChild(1, rightDeep.root)
		// Both trees have two inner joins and three tables in the same leaf order.
		nestedProposal := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
			region: 0, shape: lateMaterializationRegionShape(root.root),
		}}}
		require.EqualError(t, nestedProposal.validate([]*lateMaterializeRegion{otherRoot}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("shifted eligible targets", func(t *testing.T) {
		region := makeRegion(101, 102, base.InnerJoin)
		region.targets = region.targets[1:]
		require.EqualError(t, proposal.validate([]*lateMaterializeRegion{region}),
			"late materialization target table changed during replay")
	})
	t.Run("missing target", func(t *testing.T) {
		region := makeRegion(101, 102, base.InnerJoin)
		region.targets = nil
		require.EqualError(t, proposal.validate([]*lateMaterializeRegion{region}),
			"late materialization target missing from rebuilt plan")
	})
	require.EqualError(t, proposal.validate(nil), "late materialization region missing from rebuilt plan")
}
