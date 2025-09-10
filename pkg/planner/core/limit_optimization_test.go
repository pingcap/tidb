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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCanRemoveLimit1(t *testing.T) {
	// Create a mock session context
	sctx := sessionctx.NewContext(nil)

	// Test case 1: LIMIT 1 should be removable for PointGetPlan
	pointGet := &physicalop.PointGetPlan{}
	limit := &physicalop.PhysicalLimit{
		Count:  1,
		Offset: 0,
	}

	result := canRemoveLimit1(limit, pointGet)
	require.True(t, result, "LIMIT 1 should be removable for PointGetPlan")

	// Test case 2: LIMIT 2 should not be removable
	limit2 := &physicalop.PhysicalLimit{
		Count:  2,
		Offset: 0,
	}

	result = canRemoveLimit1(limit2, pointGet)
	require.False(t, result, "LIMIT 2 should not be removable")

	// Test case 3: LIMIT 1 with offset should not be removable
	limitWithOffset := &physicalop.PhysicalLimit{
		Count:  1,
		Offset: 1,
	}

	result = canRemoveLimit1(limitWithOffset, pointGet)
	require.False(t, result, "LIMIT 1 with offset should not be removable")
}

func TestIsGuaranteedSingleRow(t *testing.T) {
	// Test case 1: PointGetPlan should guarantee single row
	pointGet := &physicalop.PointGetPlan{}
	result := isGuaranteedSingleRow(pointGet)
	require.True(t, result, "PointGetPlan should guarantee single row")

	// Test case 2: BatchPointGetPlan with single handle should guarantee single row
	batchPointGet := &physicalop.BatchPointGetPlan{
		Handles: []types.Handle{types.NewIntHandle(1)},
	}
	result = isGuaranteedSingleRow(batchPointGet)
	require.True(t, result, "BatchPointGetPlan with single handle should guarantee single row")

	// Test case 3: BatchPointGetPlan with multiple handles should not guarantee single row
	batchPointGetMultiple := &physicalop.BatchPointGetPlan{
		Handles: []types.Handle{types.NewIntHandle(1), types.NewIntHandle(2)},
	}
	result = isGuaranteedSingleRow(batchPointGetMultiple)
	require.False(t, result, "BatchPointGetPlan with multiple handles should not guarantee single row")
}
