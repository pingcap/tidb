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
	"github.com/pingcap/tidb/pkg/parser/ast"
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
		region: 0, targets: []lateMaterializationTargetID{original.targets[0].identity()}, shape: lateMaterializationRegionShape(original.root),
	}}}
	validate := func(regions []*lateMaterializeRegion) error { _, err := proposal.validate(regions); return err }
	// Reallocated logical node IDs do not affect the table/join check.
	rebuilt := makeRegion(101, 102, base.InnerJoin)
	require.NotEqual(t, original.root.ID(), rebuilt.root.ID())
	require.NoError(t, validate([]*lateMaterializeRegion{rebuilt}))

	t.Run("same count swapped regions", func(t *testing.T) {
		other := makeRegion(201, 202, base.InnerJoin)
		require.NoError(t, validate([]*lateMaterializeRegion{rebuilt, other}))
		require.EqualError(t, validate([]*lateMaterializeRegion{other, rebuilt}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("swapped table positions", func(t *testing.T) {
		require.EqualError(t, validate([]*lateMaterializeRegion{makeRegion(102, 101, base.InnerJoin)}),
			"late materialization region tables or join types changed during replay")
	})
	t.Run("changed join type", func(t *testing.T) {
		require.EqualError(t, validate([]*lateMaterializeRegion{makeRegion(101, 102, base.LeftOuterJoin)}),
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
		_, err := nestedProposal.validate([]*lateMaterializeRegion{otherRoot})
		require.EqualError(t, err,
			"late materialization region tables or join types changed during replay")
	})
	t.Run("shifted eligible targets", func(t *testing.T) {
		region := makeRegion(101, 102, base.InnerJoin)
		region.targets = region.targets[1:]
		require.EqualError(t, validate([]*lateMaterializeRegion{region}),
			"late materialization target missing from rebuilt plan")
	})

	t.Run("self join cannot substitute another alias", func(t *testing.T) {
		region := makeRegion(101, 101, base.InnerJoin)
		leftName, rightName := ast.NewCIStr("x"), ast.NewCIStr("y")
		region.targets[0].ds.TableAsName = &leftName
		region.targets[1].ds.TableAsName = &rightName
		selected := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
			region: 0, targets: []lateMaterializationTargetID{region.targets[0].identity()}, shape: lateMaterializationRegionShape(region.root),
		}}}
		// Both aliases share a table ID. Eligibility pruning removes x, so index 0
		// now refers to y; replay must not substitute it for x.
		region.targets = region.targets[1:]
		_, err := selected.validate([]*lateMaterializeRegion{region})
		require.EqualError(t, err, "late materialization target missing from rebuilt plan")
	})

	t.Run("self join target remapped after eligibility changes", func(t *testing.T) {
		region := makeRegion(101, 101, base.InnerJoin)
		x, y := ast.NewCIStr("x"), ast.NewCIStr("y")
		region.targets[0].ds.TableAsName, region.targets[1].ds.TableAsName = &x, &y
		selected := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
			region: 0, targets: []lateMaterializationTargetID{region.targets[1].identity()}, shape: lateMaterializationRegionShape(region.root),
		}}}
		// Baseline can have only y eligible, whereas replay sees both x and y.
		// It must resolve y despite its eligible-list position changing from 0 to 1.
		resolved, err := selected.validate([]*lateMaterializeRegion{region})
		require.NoError(t, err)
		require.Same(t, region.targets[1], resolved[0][0])
		region.targets[0], region.targets[1] = region.targets[1], region.targets[0]
		resolved, err = selected.validate([]*lateMaterializeRegion{region})
		require.NoError(t, err)
		require.Same(t, region.targets[0], resolved[0][0])
	})
	t.Run("repeated alias in different query blocks", func(t *testing.T) {
		alias := ast.NewCIStr("x")
		left := logicalop.DataSource{TableInfo: &model.TableInfo{ID: 101}, TableAsName: &alias}.Init(ctx, 1)
		right := logicalop.DataSource{TableInfo: &model.TableInfo{ID: 101}, TableAsName: &alias}.Init(ctx, 2)
		join := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, 0)
		join.SetChildren(left, right)
		region := &lateMaterializeRegion{root: join, targets: []*lateMaterializeTarget{{ds: left}, {ds: right}}}
		selected := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
			region: 0, targets: []lateMaterializationTargetID{region.targets[1].identity()}, shape: lateMaterializationRegionShape(region.root),
		}}}
		resolved, err := selected.validate([]*lateMaterializeRegion{region})
		require.NoError(t, err)
		require.Same(t, region.targets[1], resolved[0][0])
		region.targets = region.targets[:1]
		_, err = selected.validate([]*lateMaterializeRegion{region})
		require.EqualError(t, err, "late materialization target missing from rebuilt plan")
	})
	t.Run("ambiguous occurrence is rejected", func(t *testing.T) {
		region := makeRegion(101, 101, base.InnerJoin)
		selected := &LateMaterializationAlternative{tasks: []lateMaterializationTask{{
			region: 0, targets: []lateMaterializationTargetID{region.targets[0].identity()}, shape: lateMaterializationRegionShape(region.root),
		}}}
		_, err := selected.validate([]*lateMaterializeRegion{region})
		require.EqualError(t, err, "late materialization target occurrence is ambiguous in rebuilt plan")
	})

	t.Run("missing target", func(t *testing.T) {
		region := makeRegion(101, 102, base.InnerJoin)
		region.targets = nil
		require.EqualError(t, validate([]*lateMaterializeRegion{region}),
			"late materialization target missing from rebuilt plan")
	})
	require.EqualError(t, validate(nil), "late materialization region missing from rebuilt plan")
}
