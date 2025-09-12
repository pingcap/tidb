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

	// This should return false for LIMIT 1 without children (can't determine uniqueness)
	require.False(t, canRemoveLimit1(limit))

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

func TestIsJoinKeysContainUniqueKey(t *testing.T) {
	// Test the isJoinKeysContainUniqueKey function
	// This is a basic test to ensure the function doesn't panic
	// More comprehensive tests would require setting up actual schemas and plans

	// Test with nil joinKeys (should not panic)
	result, err := isJoinKeysContainUniqueKey(nil, nil)
	require.NoError(t, err)
	require.False(t, result)

	// Test with empty joinKeys (should not panic)
	emptySchema := expression.NewSchema()
	result, err = isJoinKeysContainUniqueKey(nil, emptySchema)
	require.NoError(t, err)
	require.False(t, result)
}
