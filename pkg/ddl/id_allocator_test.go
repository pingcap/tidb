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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIDAllocator(t *testing.T) {
	store := testkit.CreateMockStore(t)
	ia := ddl.NewAllocator(store)

	ids, err := ia.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{112}, ids)

	// cache
	ids, err = ia.AllocIDs(2)
	require.NoError(t, err)
	require.Equal(t, []int64{113, 114}, ids)

	// cache + new alloc
	ids, err = ia.AllocIDs(8)
	require.NoError(t, err)
	require.Equal(t, []int64{115, 116, 117, 118, 119, 120, 121, 122}, ids)

	// new alloc
	ids, err = ia.AllocIDs(11)
	require.NoError(t, err)
	require.Equal(t, []int64{123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133}, ids)

	// cache + new alloc
	ids, err = ia.AllocIDs(19)
	require.NoError(t, err)
	require.Equal(t, []int64{134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152}, ids)

	// multiple allocators
	ia2 := ddl.NewAllocator(store)
	ids, err = ia2.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{153}, ids)
	ids, err = ia.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{163}, ids)

	// cache + new alloc
	ids, err = ia2.AllocIDs(10)
	require.NoError(t, err)
	require.Equal(t, []int64{154, 155, 156, 157, 158, 159, 160, 161, 162, 173}, ids)
}
