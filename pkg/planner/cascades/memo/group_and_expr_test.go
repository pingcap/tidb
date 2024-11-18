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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/stretchr/testify/require"
)

func TestGroupHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	a := Group{groupID: 1}
	b := Group{groupID: 1}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the id.
	b.groupID = 2
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}

func TestGroupExpressionHashEquals(t *testing.T) {
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	child1 := &Group{groupID: 1}
	child2 := &Group{groupID: 2}
	a := GroupExpression{
		group:       &Group{groupID: 3},
		Inputs:      []*Group{child1, child2},
		logicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	b := GroupExpression{
		// root group should change the hash.
		group:       &Group{groupID: 4},
		Inputs:      []*Group{child1, child2},
		logicalPlan: &logicalop.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}},
	}
	a.Hash64(hasher1)
	b.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, a.Equals(b))
	require.True(t, a.Equals(&b))

	// change the children order, like join commutative.
	b.Inputs = []*Group{child2, child1}
	hasher2.Reset()
	b.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, a.Equals(b))
	require.False(t, a.Equals(&b))
}
