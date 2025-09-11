// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/stretchr/testify/require"
)

func TestLimitOptimization(t *testing.T) {
	// Test the optimization rule
	rule := &LimitOptimization{}

	// Test basic functionality
	require.Equal(t, "limit_optimization", rule.Name())

	// Test canRemoveLimit1 function
	limit := &logicalop.LogicalLimit{
		Count:  1,
		Offset: 0,
	}

	// This should return true for LIMIT 1 with offset 0
	require.True(t, canRemoveLimit1(limit))

	// Test with different count
	limit.Count = 2
	require.False(t, canRemoveLimit1(limit))

	// Test with offset
	limit.Count = 1
	limit.Offset = 1
	require.False(t, canRemoveLimit1(limit))
}

func TestHasMaxOneRowGuarantee(t *testing.T) {
	// Test the hasMaxOneRowGuarantee function
	// This is a basic test to ensure the function doesn't panic
	// More comprehensive tests would require setting up actual logical plans

	// Test with nil plan (should not panic)
	require.False(t, hasMaxOneRowGuarantee(nil))
}

func TestCheckJoinColumnsFormUniqueKey(t *testing.T) {
	// Test the checkJoinColumnsFormUniqueKey function
	// This is a basic test to ensure the function doesn't panic
	// More comprehensive tests would require setting up actual schemas

	// Test with nil schema (should not panic)
	joinCols := make(map[int64]bool)
	require.False(t, checkJoinColumnsFormUniqueKey(joinCols, nil))
}

func TestIsSubsetOfJoinColumns(t *testing.T) {
	// Test the isSubsetOfJoinColumns function
	joinCols := map[int64]bool{
		1: true,
		2: true,
		3: true,
	}

	// Test with empty key
	key := expression.KeyInfo{}
	require.True(t, isSubsetOfJoinColumns(key, joinCols))

	// Test with matching columns
	key = expression.KeyInfo{
		&expression.Column{UniqueID: 1},
		&expression.Column{UniqueID: 2},
	}
	require.True(t, isSubsetOfJoinColumns(key, joinCols))

	// Test with non-matching column
	key = expression.KeyInfo{
		&expression.Column{UniqueID: 4},
	}
	require.False(t, isSubsetOfJoinColumns(key, joinCols))
}
