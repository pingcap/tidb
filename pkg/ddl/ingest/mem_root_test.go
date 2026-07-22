// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestMemoryRoot(t *testing.T) {
	memRoot := MemRoot(NewMemRootImpl(1024))
	require.Equal(t, int64(1024), memRoot.MaxMemoryQuota())
	require.Equal(t, int64(0), memRoot.CurrentUsage())

	require.True(t, memRoot.CheckConsume(1023))
	require.True(t, memRoot.CheckConsume(1024))
	require.False(t, memRoot.CheckConsume(1025))

	memRoot.Consume(512)
	require.Equal(t, int64(512), memRoot.CurrentUsage())
	require.True(t, memRoot.CheckConsume(512))
	require.False(t, memRoot.CheckConsume(513))
	require.Equal(t, int64(1024), memRoot.MaxMemoryQuota())

	memRoot.Release(10)
	require.Equal(t, int64(502), memRoot.CurrentUsage())
	require.Equal(t, int64(1024), memRoot.MaxMemoryQuota())
	memRoot.SetMaxMemoryQuota(512)
	require.False(t, memRoot.CheckConsume(20)) // 502+20 > 512
	memRoot.Release(502)

	require.Equal(t, int64(0), memRoot.CurrentUsage())
	memRoot.SetMaxMemoryQuota(1024)
	memRoot.ConsumeWithTag("a", 512)
	memRoot.ConsumeWithTag("b", 512)
	require.Equal(t, int64(1024), memRoot.CurrentUsage())
	require.False(t, memRoot.CheckConsume(1))
	memRoot.ReleaseWithTag("a")
	require.Equal(t, int64(512), memRoot.CurrentUsage())

	memRoot.ReleaseWithTag("a") // Double release.
	require.Equal(t, int64(512), memRoot.CurrentUsage())
	require.True(t, memRoot.CheckConsume(10))
	memRoot.Consume(10) // Mix usage of tag and non-tag.
	require.Equal(t, int64(522), memRoot.CurrentUsage())
}

func TestRiskOfDiskFull(t *testing.T) {
	require.False(t, RiskOfDiskFull(11, 100))
	require.False(t, RiskOfDiskFull(10, 100))
	require.True(t, RiskOfDiskFull(9, 100))

	t.Run("check local sort free disk", func(t *testing.T) {
		err := checkLocalSortFreeDisk("/tmp/local-sort", 20*size.GB, 20*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 4, UsedBytes: size.GB},
		}, 2)
		require.NoError(t, err)

		err = checkLocalSortFreeDisk("/tmp/local-sort", 12*size.GB, 20*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 3, UsedBytes: 0},
		}, 2)
		require.Error(t, err)

		err = checkLocalSortFreeDisk("/tmp/local-sort", 9*size.GB, 10*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 2, UsedBytes: 0},
			{RequiredSlots: 2, UsedBytes: 2 * size.GB},
		}, 1)
		require.Error(t, err)

		// 60 slots reserve 120 GB, capped at the 100 GB quota. After subtracting
		// 40 GB usage, the required space is 60 GB plus 20 GB capacity headroom.
		err = checkLocalSortFreeDisk("/tmp/local-sort", 81*size.GB, 200*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 40, UsedBytes: 40 * size.GB},
		}, 20)
		require.NoError(t, err)

		err = checkLocalSortFreeDisk("/tmp/local-sort", 80*size.GB, 200*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 40, UsedBytes: 40 * size.GB},
		}, 20)
		require.Error(t, err)

		err = checkLocalSortFreeDisk("/tmp/local-sort", 21*size.GB, 200*size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 40, UsedBytes: 101 * size.GB},
		}, 20)
		require.NoError(t, err)

		err = checkLocalSortFreeDisk("/tmp/local-sort", size.GB, size.GB, []LocalSortJobDiskRequirement{
			{RequiredSlots: 1, UsedBytes: 0},
		}, 1)
		require.Error(t, err)
		require.ErrorContains(t, err, "local sort requires at least")
		require.ErrorContains(t, err, "running local-sort job count 1")
		require.ErrorContains(t, err, "new job required slots 1")
		require.ErrorContains(t, err, "bytes per slot 2147483648")
		require.EqualValues(t, 2048*size.MB, LocalSortBytesPerSlot)
	})
}
