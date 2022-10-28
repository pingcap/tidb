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

package ingest_test

import (
	"testing"

	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/stretchr/testify/require"
)

func TestMemoryRoot(t *testing.T) {
	memRoot := ingest.MemRoot(ingest.NewMemRootImpl(1024, nil))
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
