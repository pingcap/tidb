// Copyright 2026 PingCAP, Inc.
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

package operator

import (
	"context"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type checksumPDClient struct {
	pd.Client
}

func (checksumPDClient) GetClusterID(context.Context) uint64 { return 123 }

func TestChecksumPiTRStorageIDMap(t *testing.T) {
	t.Run("checksum uses a table route's effective database", func(t *testing.T) {
		c := &checksumTableCtx{}
		table := &model.TableInfo{
			ID: 100, Name: ast.NewCIStr("CoPy"),
			Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{{ID: 101, Name: ast.NewCIStr("p0")}}},
		}
		maps := []*backuppb.PitrDBMap{{
			Name: "SoUrCe", IdMap: &backuppb.IDMap{UpstreamId: 1, DownstreamId: 200},
			Tables: []*backuppb.PitrTableMap{{
				Name: "CoPy", DownstreamDbName: "TaRgEt", DownstreamDbId: 300,
				IdMap:      &backuppb.IDMap{UpstreamId: 10, DownstreamId: 100},
				Partitions: []*backuppb.IDMap{{UpstreamId: 11, DownstreamId: 101}},
			}},
		}}
		requests, err := c.genRequestsWithIDMap([]tableInDB{{info: table, dbName: "target"}}, maps, 50)
		require.NoError(t, err)
		require.Len(t, requests, 1)
		require.Equal(t, "target", requests[0].dbName)
		require.Equal(t, "copy", requests[0].tableName)
		// An identity mapping uses the same case-insensitive schema lookup.
		maps[0].Tables[0].DownstreamDbName = ""
		_, err = c.genRequestsWithIDMap([]tableInDB{{info: table, dbName: "source"}}, maps, 50)
		require.NoError(t, err)
	})
	ctx := context.Background()
	for _, testCase := range []struct {
		name     string
		owners   []uint64
		legacy   bool
		wantName string
		wantErr  string
	}{
		{name: "v2 map", owners: []uint64{101}, wantName: "current"},
		{name: "v2 takes precedence over stale legacy", owners: []uint64{101}, legacy: true, wantName: "current"},
		{name: "ambiguous owners", owners: []uint64{101, 102}, legacy: true, wantErr: "multiple PiTR ID maps"},
		{name: "legacy map", legacy: true, wantName: "legacy"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			storage, err := objstore.NewLocalStorage(dir)
			require.NoError(t, err)
			write := func(path, name string) {
				data, err := (&backuppb.BackupMeta{DbMaps: []*backuppb.PitrDBMap{{Name: name}}, BackupSchemaVersion: backuppb.BackupSchemaVersion}).Marshal()
				require.NoError(t, err)
				require.NoError(t, storage.WriteFile(ctx, path, data))
			}
			for _, owner := range testCase.owners {
				write(logclient.PitrIDMapsFilenameForTask(123, 3, 50, owner), "current")
			}
			// Files belonging to another upstream or downstream are not candidates.
			write(logclient.PitrIDMapsFilenameForTask(123, 4, 50, 101), "other-upstream")
			write(logclient.PitrIDMapsFilenameForTask(124, 3, 50, 101), "other-downstream")
			if testCase.legacy {
				write(logclient.PitrIDMapsFilename(123, 50), "legacy")
			}
			c := &checksumTableCtx{
				cfg: ChecksumWithRewriteRulesConfig{Config: task.Config{Storage: "local://" + filepath.ToSlash(dir)}},
				mgr: &conn.Mgr{PdController: pdutil.NewPdControllerWithPDClient(checksumPDClient{}, nil, nil)},
			}
			maps, err := c.loadPitrIdMap(ctx, nil, 50, 3)
			if testCase.wantErr != "" {
				require.ErrorContains(t, err, testCase.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, maps, 1)
			require.Equal(t, testCase.wantName, maps[0].Name)
		})
	}
}

func TestEnsureUniquePitrIDMapOwner(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		owners  []uint64
		wantErr string
	}{
		{name: "no owner is decided by the caller"},
		{name: "single owner", owners: []uint64{101}},
		{name: "repeated owner", owners: []uint64{101, 101}},
		{name: "ambiguous owners", owners: []uint64{101, 102}, wantErr: "multiple PiTR ID maps"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := ensureUniquePitrIDMapOwner(testCase.owners, 50, 3)
			if testCase.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, testCase.wantErr)
			require.ErrorContains(t, err, "restored TS 50")
			require.ErrorContains(t, err, "upstream cluster 3")
		})
	}
}
