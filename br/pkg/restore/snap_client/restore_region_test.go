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

package snapclient

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/opt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type restoreRegionTestPD struct {
	split.SplitClient
	regions []*split.RegionInfo
}

func (pd *restoreRegionTestPD) ScanRegions(_ context.Context, start, end []byte, _ int, _ ...opt.GetRegionOption) ([]*split.RegionInfo, error) {
	var result []*split.RegionInfo
	for _, r := range pd.regions {
		if (len(r.Region.EndKey) == 0 || bytes.Compare(r.Region.EndKey, start) > 0) &&
			(len(end) == 0 || bytes.Compare(r.Region.StartKey, end) < 0) {
			result = append(result, r)
		}
	}
	return result, nil
}

type restoreRegionTestClient struct {
	importclient.ImporterClient // Calling any old RPC is a test failure (nil interface).
	send                        func(context.Context, uint64, *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error)
}

func (c *restoreRegionTestClient) RestoreRegion(ctx context.Context, store uint64, req *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error) {
	return c.send(ctx, store, req)
}

func restoreRegionFixture(t *testing.T) (*SnapFileImporter, *restoreRegionTestClient, restore.BackupFileSet, []*split.RegionInfo) {
	oldPrefix, newPrefix := tablecodec.GenTableRecordPrefix(10), tablecodec.GenTableRecordPrefix(20)
	file := func(cf string) *backuppb.File {
		return &backuppb.File{Name: "backup_" + cf + ".sst", Cf: cf, Size_: 123,
			StartKey: append(bytes.Clone(oldPrefix), 'a'), EndKey: append(bytes.Clone(oldPrefix), 'z')}
	}
	set := restore.BackupFileSet{TableID: 20, SSTFiles: []*backuppb.File{file("write"), file("default")},
		RewriteRules: &restoreutils.RewriteRules{Data: []*import_sstpb.RewriteRule{{OldKeyPrefix: oldPrefix, NewKeyPrefix: newPrefix}}}}
	bounds := [][]byte{codec.EncodeBytes(nil, append(bytes.Clone(newPrefix), 'a')),
		codec.EncodeBytes(nil, append(bytes.Clone(newPrefix), 'm')),
		codec.EncodeBytes(nil, append(bytes.Clone(newPrefix), 'z'))}
	regions := make([]*split.RegionInfo, 2)
	for i := range regions {
		regions[i] = &split.RegionInfo{Region: &metapb.Region{Id: uint64(i + 1), StartKey: bounds[i], EndKey: bounds[i+1],
			RegionEpoch: &metapb.RegionEpoch{Version: 5, ConfVer: 2}}, Leader: &metapb.Peer{Id: uint64(i + 10), StoreId: 1}}
	}
	client := &restoreRegionTestClient{send: func(context.Context, uint64, *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error) {
		return &import_sstpb.IngestResponse{}, nil
	}}
	options := NewSnapFileImporterOptions(nil, &restoreRegionTestPD{regions: regions}, client,
		&backuppb.StorageBackend{Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "source"}}},
		RewriteModeKeyspace, []*metapb.Store{{Id: 1}}, 10, 1, false, nil, nil)
	options.restoreRegion = true
	importer, err := NewSnapFileImporter(context.Background(), kvrpcpb.APIVersion_V2, TiDBFull, options)
	require.NoError(t, err)
	return importer, client, set, regions
}

func TestRestoreRegionRequest(t *testing.T) {
	importer, _, set, regions := restoreRegionFixture(t)
	// Real plaintext backups can still contain an IV in their file metadata.
	set.SSTFiles[0].CipherIv = bytes.Repeat([]byte{2}, 16)
	req, err := importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.NoError(t, err)
	require.Len(t, req.Sources, 2)
	require.Equal(t, kvrpcpb.APIVersion_V2, req.Context.ApiVersion)
	require.Equal(t, regions[0].Region.RegionEpoch, req.Context.RegionEpoch)
	require.Equal(t, regions[0].Leader, req.Context.Peer)
	require.NotEmpty(t, req.Context.RequestSource)
	require.Equal(t, importer.backend, req.StorageBackend)
	require.Nil(t, req.CipherInfo)
	for i, source := range req.Sources {
		require.Equal(t, set.SSTFiles[i].Name, source.Name)
		require.Equal(t, uint64(123), source.Length)
		require.Equal(t, set.SSTFiles[i].Cf, source.Cf)
		require.Equal(t, set.RewriteRules.Data[0], source.RewriteRule)
		require.Empty(t, source.CipherIv)
	}
	importer.cipher = &backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	req, err = importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.NoError(t, err)
	require.Nil(t, req.CipherInfo)
	require.Empty(t, req.Sources[0].CipherIv)
	importer.cipher.CipherType = encryptionpb.EncryptionMethod_AES256_CTR
	importer.cipher.CipherKey = bytes.Repeat([]byte{1}, 32)
	set.SSTFiles[0].CipherIv = bytes.Repeat([]byte{2}, 16)
	req, err = importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.NoError(t, err)
	require.Equal(t, importer.cipher, req.CipherInfo)
	require.Equal(t, set.SSTFiles[0].CipherIv, req.Sources[0].CipherIv)
	// A source starting at the exclusive Region end does not belong to it.
	for _, file := range set.SSTFiles {
		file.StartKey[len(file.StartKey)-1] = 'm'
	}
	req, err = importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.NoError(t, err)
	require.Empty(t, req.Sources)
	regions[0].Leader = nil
	_, err = importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.Error(t, err)
}

func TestRestoreRegionCompletionAndNoRetry(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *import_sstpb.IngestResponse
		err      error
	}{
		{"success", &import_sstpb.IngestResponse{}, nil},
		{"busy", &import_sstpb.IngestResponse{Error: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}}, nil},
		{"not-leader", &import_sstpb.IngestResponse{Error: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}}, nil},
		{"epoch", &import_sstpb.IngestResponse{Error: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}}, nil},
		{"unavailable", nil, status.Error(codes.Unavailable, "response lost")},
		{"nil-response", nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			importer, client, set, _ := restoreRegionFixture(t)
			var calls []uint64
			client.send = func(_ context.Context, store uint64, req *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error) {
				require.Equal(t, uint64(1), store)
				require.Len(t, req.Sources, 2)
				calls = append(calls, req.Context.RegionId)
				if req.Context.RegionId == 1 {
					return &import_sstpb.IngestResponse{}, nil
				}
				return test.response, test.err
			}
			completed := false
			importer.beforeIngestCallbacks = append(importer.beforeIngestCallbacks,
				func(context.Context, restore.BatchBackupFileSet) (func() error, error) {
					return func() error { completed = true; return nil }, nil
				})
			err := importer.Import(context.Background(), set)
			if test.name == "success" {
				require.NoError(t, err)
				require.True(t, completed)
			} else {
				require.Error(t, err)
				require.False(t, completed)
			}
			require.Equal(t, []uint64{1, 2}, calls) // The first Region must never be replayed.
			require.False(t, importer.ShouldBlock())
		})
	}
}

func TestRestoreRegionAdmission(t *testing.T) {
	importer, client, set, regions := restoreRegionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := importer.buildRestoreRegionRequest(regions[0], []restore.BackupFileSet{set})
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	client.send = func(context.Context, uint64, *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("worker failed")
	}
	done := make(chan error, 1)
	go func() { done <- importer.restoreOneRegion(ctx, req) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.True(t, importer.ShouldBlock())
	canceled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, importer.restoreOneRegion(canceled, req), context.Canceled)
	close(release)
	require.ErrorContains(t, <-done, "worker failed")
	require.False(t, importer.ShouldBlock())
	client.send = func(context.Context, uint64, *import_sstpb.RestoreRegionRequest) (*import_sstpb.IngestResponse, error) {
		return &import_sstpb.IngestResponse{}, nil
	}
	require.NoError(t, importer.restoreOneRegion(ctx, req))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); importer.PauseForBackpressure() }()
	wg.Wait()
}
