// Copyright 2023 PingCAP, Inc.
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

package local

import (
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/mock/mocklocal"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCheckDiskQuota(t *testing.T) {
	controller := gomock.NewController(t)
	mockDiskUsage := mocklocal.NewMockDiskUsage(controller)

	uuid1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid3 := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	uuid5 := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	uuid7 := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	uuid9 := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	fileSizes := []backend.EngineFileSize{
		{
			UUID:        uuid1,
			DiskSize:    1000,
			MemSize:     0,
			IsImporting: false,
		},
		{
			UUID:        uuid3,
			DiskSize:    2000,
			MemSize:     1000,
			IsImporting: true,
		},
		{
			UUID:        uuid5,
			DiskSize:    1500,
			MemSize:     3500,
			IsImporting: false,
		},
		{
			UUID:        uuid7,
			DiskSize:    0,
			MemSize:     7000,
			IsImporting: true,
		},
		{
			UUID:        uuid9,
			DiskSize:    4500,
			MemSize:     4500,
			IsImporting: false,
		},
	}

	mockDiskUsage.EXPECT().EngineFileSizes().Return(fileSizes).Times(4)

	// No quota exceeded
	le, iple, ds, ms := CheckDiskQuota(mockDiskUsage, 30000)
	require.Len(t, le, 0)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the largest one is out
	le, iple, ds, ms = CheckDiskQuota(mockDiskUsage, 20000)
	require.Equal(t, []uuid.UUID{uuid9}, le)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the importing one should be ranked least priority
	le, iple, ds, ms = CheckDiskQuota(mockDiskUsage, 12000)
	require.Equal(t, []uuid.UUID{uuid5, uuid9}, le)
	require.Equal(t, 0, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)

	// Quota exceeded, the importing ones should not be visible
	le, iple, ds, ms = CheckDiskQuota(mockDiskUsage, 5000)
	require.Equal(t, []uuid.UUID{uuid1, uuid5, uuid9}, le)
	require.Equal(t, 1, iple)
	require.Equal(t, int64(9000), ds)
	require.Equal(t, int64(16000), ms)
}
