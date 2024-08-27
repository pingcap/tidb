// Copyright 2021 PingCAP, Inc.
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

package cachetest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/stretchr/testify/require"
)

func TestNewCache(t *testing.T) {
	ic := infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)
}

func TestInsert(t *testing.T) {
	ic := infoschema.NewCache(nil, 3)
	require.NotNil(t, ic)

	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2, 2)
	require.Equal(t, is2, ic.GetByVersion(2))
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
	require.Equal(t, is2, ic.GetBySnapshotTS(10))
	require.Nil(t, ic.GetBySnapshotTS(0))

	// newer
	is5 := infoschema.MockInfoSchemaWithSchemaVer(nil, 5)
	ic.Insert(is5, 5)
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is2, ic.GetByVersion(2))
	// there is a gap in schema cache, so don't use this version
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is5, ic.GetBySnapshotTS(10))

	// older
	is0 := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	ic.Insert(is0, 0)
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is2, ic.GetByVersion(2))
	require.Equal(t, is0, ic.GetByVersion(0))

	// replace 5, drop 0
	is6 := infoschema.MockInfoSchemaWithSchemaVer(nil, 6)
	ic.Insert(is6, 6)
	require.Equal(t, is6, ic.GetByVersion(6))
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is2, ic.GetByVersion(2))
	require.Nil(t, ic.GetByVersion(0))
	// there is a gap in schema cache, so don't use this version
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is5, ic.GetBySnapshotTS(5))
	require.Equal(t, is6, ic.GetBySnapshotTS(10))

	// replace 2, drop 2
	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	ic.Insert(is3, 3)
	require.Equal(t, is6, ic.GetByVersion(6))
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is3, ic.GetByVersion(3))
	require.Nil(t, ic.GetByVersion(2))
	require.Nil(t, ic.GetByVersion(0))
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is6, ic.GetBySnapshotTS(10))

	// insert 2, but failed silently
	ic.Insert(is2, 2)
	require.Equal(t, is6, ic.GetByVersion(6))
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is3, ic.GetByVersion(3))
	require.Nil(t, ic.GetByVersion(2))
	require.Nil(t, ic.GetByVersion(0))
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is6, ic.GetBySnapshotTS(10))

	// insert 5, but it is already in
	ic.Insert(is5, 5)
	require.Equal(t, is6, ic.GetByVersion(6))
	require.Equal(t, is5, ic.GetByVersion(5))
	require.Equal(t, is3, ic.GetByVersion(3))
	require.Nil(t, ic.GetByVersion(2))
	require.Nil(t, ic.GetByVersion(0))
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is5, ic.GetBySnapshotTS(5))
	require.Equal(t, is6, ic.GetBySnapshotTS(10))
}

func TestGetByVersion(t *testing.T) {
	ic := infoschema.NewCache(nil, 2)
	require.NotNil(t, ic)
	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1, 1)
	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	ic.Insert(is3, 3)

	require.Equal(t, is1, ic.GetByVersion(1))
	require.Equal(t, is3, ic.GetByVersion(3))
	require.Nilf(t, ic.GetByVersion(0), "index == 0, but not found")
	require.Equal(t, int64(1), ic.GetByVersion(2).SchemaMetaVersion())
	require.Nilf(t, ic.GetByVersion(4), "index == length, but not found")
}

func TestGetLatest(t *testing.T) {
	ic := infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)
	require.Nil(t, ic.GetLatest())

	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1, 1)
	require.Equal(t, is1, ic.GetLatest())

	// newer change the newest
	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2, 2)
	require.Equal(t, is2, ic.GetLatest())

	// older schema doesn't change the newest
	is0 := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	ic.Insert(is0, 0)
	require.Equal(t, is2, ic.GetLatest())
}

func TestGetByTimestamp(t *testing.T) {
	ic := infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)
	require.Nil(t, ic.GetLatest())
	require.Equal(t, 0, ic.Len())

	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1, 1)
	require.Nil(t, ic.GetBySnapshotTS(0))
	require.Equal(t, is1, ic.GetBySnapshotTS(1))
	require.Equal(t, is1, ic.GetBySnapshotTS(2))
	require.Equal(t, 1, ic.Len())

	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	ic.Insert(is3, 3)
	require.Equal(t, is3, ic.GetLatest())
	require.Nil(t, ic.GetBySnapshotTS(0))
	// there is a gap, no schema returned for ts 2
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is3, ic.GetBySnapshotTS(3))
	require.Equal(t, is3, ic.GetBySnapshotTS(4))
	require.Equal(t, 2, ic.Len())

	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	// schema version 2 doesn't have timestamp set
	// thus all schema before ver 2 cannot be searched by timestamp anymore
	// because the ts of ver 2 is not accurate
	ic.Insert(is2, 0)
	require.Equal(t, is3, ic.GetLatest())
	require.Nil(t, ic.GetBySnapshotTS(0))
	require.Nil(t, ic.GetBySnapshotTS(1))
	require.Nil(t, ic.GetBySnapshotTS(2))
	require.Equal(t, is3, ic.GetBySnapshotTS(3))
	require.Equal(t, is3, ic.GetBySnapshotTS(4))
	require.Equal(t, 3, ic.Len())

	// insert is2 again with correct timestamp, to correct previous wrong timestamp
	ic.Insert(is2, 2)
	require.Equal(t, is3, ic.GetLatest())
	require.Equal(t, is1, ic.GetBySnapshotTS(1))
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
	require.Equal(t, is3, ic.GetBySnapshotTS(3))
	require.Equal(t, 3, ic.Len())
}

func TestReSize(t *testing.T) {
	ic := infoschema.NewCache(nil, 2)
	require.NotNil(t, ic)
	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1, 1)
	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2, 2)

	ic.ReSize(3)
	require.Equal(t, 2, ic.Size())
	require.Equal(t, is1, ic.GetByVersion(1))
	require.Equal(t, is2, ic.GetByVersion(2))
	is3 := infoschema.MockInfoSchemaWithSchemaVer(nil, 3)
	require.True(t, ic.Insert(is3, 3))
	require.Equal(t, is1, ic.GetByVersion(1))
	require.Equal(t, is2, ic.GetByVersion(2))
	require.Equal(t, is3, ic.GetByVersion(3))

	ic.ReSize(1)
	require.Equal(t, 1, ic.Size())
	require.Nil(t, ic.GetByVersion(1))
	require.Nil(t, ic.GetByVersion(2))
	require.Equal(t, is3, ic.GetByVersion(3))
	require.False(t, ic.Insert(is2, 2))
	require.Equal(t, 1, ic.Size())
	is4 := infoschema.MockInfoSchemaWithSchemaVer(nil, 4)
	require.True(t, ic.Insert(is4, 4))
	require.Equal(t, 1, ic.Size())
	require.Nil(t, ic.GetByVersion(1))
	require.Nil(t, ic.GetByVersion(2))
	require.Nil(t, ic.GetByVersion(3))
	require.Equal(t, is4, ic.GetByVersion(4))
}

func TestCacheWithSchemaTsZero(t *testing.T) {
	ic := infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)

	for i := 1; i <= 8; i++ {
		ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, int64(i)), uint64(i))
	}

	checkFn := func(start, end int64, exist bool) {
		require.True(t, start <= end)
		latestSchemaVersion := ic.GetLatest().SchemaMetaVersion()
		for ts := start; ts <= end; ts++ {
			is := ic.GetBySnapshotTS(uint64(ts))
			if exist {
				require.NotNil(t, is, fmt.Sprintf("ts %d", ts))
				if ts > latestSchemaVersion {
					require.Equal(t, latestSchemaVersion, is.SchemaMetaVersion(), fmt.Sprintf("ts %d", ts))
				} else {
					require.Equal(t, ts, is.SchemaMetaVersion(), fmt.Sprintf("ts %d", ts))
				}
			} else {
				require.Nil(t, is, fmt.Sprintf("ts %d", ts))
			}
		}
	}
	checkFn(1, 8, true)
	checkFn(8, 10, true)

	// mock for meet error There is no Write MVCC info for the schema version
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 9), 0)
	checkFn(1, 7, true)
	checkFn(8, 9, false)
	checkFn(9, 10, false)

	for i := 10; i <= 16; i++ {
		ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, int64(i)), uint64(i))
		checkFn(1, 7, true)
		checkFn(8, 9, false)
		checkFn(10, 16, true)
	}
	require.Equal(t, 16, ic.Size())

	// refill the cache
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 9), 9)
	checkFn(1, 16, true)
	require.Equal(t, 16, ic.Size())

	// Test more than capacity
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 17), 17)
	checkFn(1, 1, false)
	checkFn(2, 17, true)
	checkFn(2, 20, true)
	require.Equal(t, 16, ic.Size())

	// Test for there is a hole in the middle.
	ic = infoschema.NewCache(nil, 16)

	// mock for restart with full load the latest version schema.
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 100), 100)
	checkFn(1, 99, false)
	checkFn(100, 100, true)

	for i := 1; i <= 16; i++ {
		ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, int64(i)), uint64(i))
	}
	checkFn(1, 1, false)
	checkFn(2, 15, true)
	checkFn(16, 16, false)
	checkFn(100, 100, true)
	require.Equal(t, 16, ic.Size())

	for i := 85; i < 100; i++ {
		ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, int64(i)), uint64(i))
	}
	checkFn(1, 84, false)
	checkFn(85, 100, true)
	require.Equal(t, 16, ic.Size())

	// Test cache with schema version hole, which is cause by schema version doesn't has related schema-diff.
	ic = infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)
	for i := 1; i <= 8; i++ {
		ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, int64(i)), uint64(i))
	}
	checkFn(1, 10, true)
	// mock for schema version hole, schema-version 9 is missing.
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 10), 10)
	checkFn(1, 7, true)
	// without empty schema version map, get snapshot by ts 8, 9 will both failed.
	checkFn(8, 9, false)
	checkFn(10, 10, true)
	// add empty schema version 9.
	ic.InsertEmptySchemaVersion(9)
	// after set empty schema version, get snapshot by ts 8, 9 will both success.
	checkFn(1, 8, true)
	checkFn(10, 10, true)
	is := ic.GetBySnapshotTS(uint64(9))
	require.NotNil(t, is)
	// since schema version 9 is empty, so get by ts 9 will get schema which version is 8.
	require.Equal(t, int64(8), is.SchemaMetaVersion())
}

func TestCacheEmptySchemaVersion(t *testing.T) {
	ic := infoschema.NewCache(nil, 16)
	require.NotNil(t, ic)
	require.Equal(t, 0, len(ic.GetEmptySchemaVersions()))
	for i := 0; i < 16; i++ {
		ic.InsertEmptySchemaVersion(int64(i))
	}
	emptyVersions := ic.GetEmptySchemaVersions()
	require.Equal(t, 16, len(emptyVersions))
	for i := 0; i < 16; i++ {
		_, ok := emptyVersions[int64(i)]
		require.True(t, ok)
	}
	for i := 16; i < 20; i++ {
		ic.InsertEmptySchemaVersion(int64(i))
	}
	emptyVersions = ic.GetEmptySchemaVersions()
	require.Equal(t, 16, len(emptyVersions))
	for i := 4; i < 20; i++ {
		_, ok := emptyVersions[int64(i)]
		require.True(t, ok)
	}
}
