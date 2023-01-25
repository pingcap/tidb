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

package infoschema_test

import (
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/stretchr/testify/require"
)

func TestNewCache(t *testing.T) {
	ic := infoschema.NewCache(16)
	require.NotNil(t, ic)
}

func TestInsert(t *testing.T) {
	ic := infoschema.NewCache(3)
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
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
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
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
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
	ic := infoschema.NewCache(2)
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
	ic := infoschema.NewCache(16)
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
	ic := infoschema.NewCache(16)
	require.NotNil(t, ic)
	require.Nil(t, ic.GetLatest())

	is1 := infoschema.MockInfoSchemaWithSchemaVer(nil, 1)
	ic.Insert(is1, 1)
	require.Equal(t, is1, ic.GetLatest())
	_, err := ic.GetVersionByTimestamp(0)
	require.NotNil(t, err)
	ver, err := ic.GetVersionByTimestamp(1)
	require.Nil(t, err)
	require.Equal(t, int64(1), ver)
	require.Equal(t, is1, ic.GetBySnapshotTS(1))
	ver, err = ic.GetVersionByTimestamp(2)
	require.Nil(t, err)
	require.Equal(t, int64(1), ver)
	require.Equal(t, is1, ic.GetBySnapshotTS(2))

	is2 := infoschema.MockInfoSchemaWithSchemaVer(nil, 2)
	ic.Insert(is2, 2)
	require.Equal(t, is2, ic.GetLatest())
	_, err = ic.GetVersionByTimestamp(0)
	require.NotNil(t, err)
	ver, err = ic.GetVersionByTimestamp(1)
	require.Nil(t, err)
	require.Equal(t, int64(1), ver)
	require.Equal(t, is1, ic.GetBySnapshotTS(1))
	ver, err = ic.GetVersionByTimestamp(2)
	require.Nil(t, err)
	require.Equal(t, int64(2), ver)
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
	ver, err = ic.GetVersionByTimestamp(3)
	require.Nil(t, err)
	require.Equal(t, int64(2), ver)
	require.Equal(t, is2, ic.GetBySnapshotTS(3))

	is0 := infoschema.MockInfoSchemaWithSchemaVer(nil, 0)
	ic.Insert(is0, 0)
	require.Equal(t, is2, ic.GetLatest())
	ver, err = ic.GetVersionByTimestamp(0)
	require.Nil(t, err)
	require.Equal(t, int64(0), ver)
	require.Equal(t, is0, ic.GetBySnapshotTS(0))
	ver, err = ic.GetVersionByTimestamp(1)
	require.Nil(t, err)
	require.Equal(t, int64(1), ver)
	require.Equal(t, is1, ic.GetBySnapshotTS(1))
	ver, err = ic.GetVersionByTimestamp(2)
	require.Nil(t, err)
	require.Equal(t, int64(2), ver)
	require.Equal(t, is2, ic.GetBySnapshotTS(2))
	ver, err = ic.GetVersionByTimestamp(3)
	require.Nil(t, err)
	require.Equal(t, int64(2), ver)
	require.Equal(t, is2, ic.GetBySnapshotTS(3))
}
