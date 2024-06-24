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

package local_test

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/stretchr/testify/require"
)

func TestCheckRequirementsTiFlash(t *testing.T) {
	db, mock, err := sqlmock.New()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, err)
	ctx := context.Background()

	dbMetas := []*mydump.MDDatabaseMeta{
		{
			Name: "test",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test",
					Name:      "t1",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
		{
			Name: "test1",
			Tables: []*mydump.MDTableMeta{
				{
					DB:        "test1",
					Name:      "t",
					DataFiles: []mydump.FileInfo{{}},
				},
				{
					DB:        "test1",
					Name:      "tbl",
					DataFiles: []mydump.FileInfo{{}},
				},
			},
		},
	}
	checkCtx := &backend.CheckCtx{DBMetas: dbMetas}

	mock.ExpectQuery(local.TiFlashReplicaQueryForTest).WillReturnRows(sqlmock.NewRows([]string{"db", "tbl"}).
		AddRow("db", "tbl").
		AddRow("test", "t1").
		AddRow("test1", "tbl"))
	mock.ExpectClose()

	err = local.CheckTiFlashVersionForTest(ctx, db, checkCtx, *semver.New("4.0.2"))
	require.Regexp(t, "^lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: \\[`test`.`t1`, `test1`.`tbl`\\]", err.Error())
}

func TestGetRegionSplitSizeKeys(t *testing.T) {
	allStores := []*metapb.Store{
		{
			Address:       "172.16.102.1:20160",
			StatusAddress: "0.0.0.0:20180",
		},
		{
			Address:       "172.16.102.2:20160",
			StatusAddress: "0.0.0.0:20180",
		},
		{
			Address:       "172.16.102.3:20160",
			StatusAddress: "0.0.0.0:20180",
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := utiltest.NewFakePDClient(allStores, false, nil)
	defer func() {
		local.SetGetSplitConfFromStoreFunc(local.GetSplitConfFromStore)
	}()
	local.SetGetSplitConfFromStoreFunc(func(ctx context.Context, host string, tls *common.TLS) (int64, int64, error) {
		if strings.Contains(host, "172.16.102.3:20180") {
			return int64(1), int64(2), nil
		}
		return 0, 0, errors.New("invalid connection")
	})
	splitSize, splitKeys, err := local.GetRegionSplitSizeKeys(ctx, cli, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), splitSize)
	require.Equal(t, int64(2), splitKeys)
}
