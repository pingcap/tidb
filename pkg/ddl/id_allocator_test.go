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
	ddl.IDAllocatorStep = 10
	store := testkit.CreateMockStore(t)
	ia := ddl.NewAllocator(store)

	ids, err := ia.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{104}, ids)

	ids, err = ia.AllocIDs(2)
	require.NoError(t, err)
	require.Equal(t, []int64{105, 106}, ids)

	ids, err = ia.AllocIDs(8)
	require.NoError(t, err)
	require.Equal(t, []int64{107, 108, 109, 110, 111, 112, 113, 114}, ids)

	ids, err = ia.AllocIDs(11)
	require.NoError(t, err)
	require.Equal(t, []int64{115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125}, ids)

	ids, err = ia.AllocIDs(19)
	require.NoError(t, err)
	require.Equal(t, []int64{126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144}, ids)

	ia2 := ddl.NewAllocator(store)
	ids, err = ia2.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{145}, ids)
	ids, err = ia.AllocIDs(1)
	require.NoError(t, err)
	require.Equal(t, []int64{155}, ids)

	ids, err = ia2.AllocIDs(10)
	require.NoError(t, err)
	require.Equal(t, []int64{146, 147, 148, 149, 150, 151, 152, 153, 154, 165}, ids)
}
