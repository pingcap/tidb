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

package utilfuncp

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCloneConstantsForPlanCacheWithNilEntry(t *testing.T) {
	// A Column with VirtualExpr set returns SafeToShareAcrossSession() == false.
	// Using it as DeferredExpr on a Constant makes that Constant unsafe,
	// which forces the cloning path (allSafe == false).
	unsafeDeferredExpr := &expression.Column{
		RetType:     types.NewFieldType(mysql.TypeLonglong),
		VirtualExpr: &expression.Constant{Value: types.NewIntDatum(0)},
	}
	unsafeConst := &expression.Constant{
		Value:        types.NewIntDatum(1),
		RetType:      types.NewFieldType(mysql.TypeLonglong),
		DeferredExpr: unsafeDeferredExpr,
	}

	// constants slice contains a nil entry — this is the scenario that
	// caused a panic before the fix (issue #66265).
	constants := []*expression.Constant{unsafeConst, nil, unsafeConst}

	// Should not panic.
	cloned := CloneConstantsForPlanCache(constants, nil)

	require.Len(t, cloned, 3)
	// The nil entry must be preserved as nil in the cloned slice.
	require.NotNil(t, cloned[0])
	require.Nil(t, cloned[1])
	require.NotNil(t, cloned[2])
}
