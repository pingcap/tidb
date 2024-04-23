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

package kv

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/stretchr/testify/require"
)

func TestAllocator(t *testing.T) {
	alloc := NewPanickingAllocators(true, 0)
	require.NoError(t, alloc.Get(autoid.RowIDAllocType).Rebase(nil, 123, false))
	// cannot revert back
	require.NoError(t, alloc.Get(autoid.RowIDAllocType).Rebase(nil, 100, false))
	require.NoError(t, alloc.Get(autoid.AutoIncrementType).Rebase(nil, 456, false))
	require.NoError(t, alloc.Get(autoid.AutoRandomType).Rebase(nil, 789, false))

	require.EqualValues(t, 123, alloc.Get(autoid.RowIDAllocType).Base())
	require.EqualValues(t, 456, alloc.Get(autoid.AutoIncrementType).Base())
	require.EqualValues(t, 789, alloc.Get(autoid.AutoRandomType).Base())
}
