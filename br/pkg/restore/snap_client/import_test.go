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

package snapclient_test

import (
	"context"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestGetKeyRangeByMode(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
		TableMetas: []*backuppb.TableMeta{
			{
				PhysicalId: 3,
			},
			{
				PhysicalId: 4,
			},
		},
	}
	endFile := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte(""),
	}
	rule := map[int64]*restoreutils.RewriteRules{
		4: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: []byte("t1"),
					NewKeyPrefix: []byte("t4"),
				},
			},
		},
		3: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: []byte("t1"),
					NewKeyPrefix: []byte("t3"),
				},
			},
		},
		2: {
			Data: []*import_sstpb.RewriteRule{
				{
					OldKeyPrefix: []byte("t1"),
					NewKeyPrefix: []byte("t2"),
				},
			},
		},
	}
	// raw kv
	testRawFn := snapclient.GetKeyRangeByMode(snapclient.Raw)
	start, end, err := testRawFn(file, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte("t1ccc"), end)

	start, end, err = testRawFn(endFile, 0, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte(""), end)

	// txn kv: the keys must be encoded.
	testTxnFn := snapclient.GetKeyRangeByMode(snapclient.Txn)
	start, end, err = testTxnFn(file, 0, nil)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1ccc")), end)

	start, end, err = testTxnFn(endFile, 0, nil)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, []byte(""), end)

	// normal kv: the keys must be encoded.
	testFn := snapclient.GetKeyRangeByMode(snapclient.TiDBFull)
	start, end, err = testFn(file, 2, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t3a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t4ccc")), end)

	// TODO maybe fix later
	// current restore does not support rewrite empty endkey.
	// because backup guarantees that the end key is not empty.
	// start, end, err = testFn(endFile, rule)
	// require.NoError(t, err)
	// require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	// require.Equal(t, []byte(""), end)
}

func TestGetSSTMetaFromFile(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta, err := snapclient.GetSSTMetaFromFile(file, region, rule.NewKeyPrefix, rule.NewKeyPrefix, snapclient.RewriteModeLegacy)
	require.Nil(t, err)
	require.Equal(t, "t2abc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}

func TestGetSSTMetaFromFile2(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t2ccc"),
	}
	startRule := &import_sstpb.RewriteRule{
		NewKeyPrefix: []byte("tA"),
	}
	endRule := &import_sstpb.RewriteRule{
		NewKeyPrefix: []byte("tB"),
	}
	region := &metapb.Region{
		StartKey: []byte("tAabc"),
		EndKey:   []byte("tC"),
	}
	sstMeta, err := snapclient.GetSSTMetaFromFile(file, region, startRule.NewKeyPrefix, endRule.NewKeyPrefix, snapclient.RewriteModeLegacy)
	require.Nil(t, err)
	require.Equal(t, "tAabc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "tB\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}

type fakeImporterClient struct {
	importclient.ImporterClient

	speedLimit map[uint64]uint64
}

func newFakeImporterClient() *fakeImporterClient {
	return &fakeImporterClient{
		speedLimit: make(map[uint64]uint64),
	}
}

func (client *fakeImporterClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	client.speedLimit[storeID] = req.SpeedLimit
	return &import_sstpb.SetDownloadSpeedLimitResponse{}, nil
}

func (client *fakeImporterClient) CheckMultiIngestSupport(ctx context.Context, stores []uint64) error {
	return nil
}

func (client *fakeImporterClient) CloseGrpcClient() error {
	return nil
}

func (client *fakeImporterClient) DownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	return &import_sstpb.DownloadResponse{Range: *req.Sst.Range}, nil
}

func (client *fakeImporterClient) MultiIngest(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.MultiIngestRequest,
) (*import_sstpb.IngestResponse, error) {
	return &import_sstpb.IngestResponse{}, nil
}

func TestUnproperConfigSnapImporter(t *testing.T) {
	ctx := context.Background()
	opt := snapclient.NewSnapFileImporterOptionsForTest(nil, nil, nil, snapclient.RewriteModeKeyspace, 0)
	_, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.TiDBFull, opt)
	require.Error(t, err)
}

func TestSnapImporter(t *testing.T) {
	ctx := context.Background()
	splitClient := split.NewFakeSplitClient()
	for _, region := range generateRegions() {
		splitClient.AppendPdRegion(region)
	}
	importClient := newFakeImporterClient()
	opt := snapclient.NewSnapFileImporterOptionsForTest(splitClient, importClient, generateStores(), snapclient.RewriteModeKeyspace, 10)
	importer, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.TiDBFull, opt)
	require.NoError(t, err)
	err = importer.SetDownloadSpeedLimit(ctx, 1, 5)
	require.NoError(t, err)
	require.Equal(t, uint64(5), importClient.speedLimit[1])
	err = importer.SetRawRange(nil, nil)
	require.Error(t, err)
	files, id, rules := generateFiles()
	rulesMap := map[int64]*restoreutils.RewriteRules{
		id: rules,
	}
	for _, file := range files {
		importer.PauseForBackpressure()
		err = importer.Import(ctx, restore.BackupFileSet{SSTFiles: []*backuppb.File{file}, RewriteRules: rulesMap})
		require.NoError(t, err)
	}
	err = importer.Close()
	require.NoError(t, err)
}

func TestSnapImporterRaw(t *testing.T) {
	ctx := context.Background()
	splitClient := split.NewFakeSplitClient()
	for _, region := range generateRegions() {
		splitClient.AppendPdRegion(region)
	}
	importClient := newFakeImporterClient()
	opt := snapclient.NewSnapFileImporterOptionsForTest(splitClient, importClient, generateStores(), snapclient.RewriteModeKeyspace, 10)
	importer, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.Raw, opt)
	require.NoError(t, err)
	err = importer.SetRawRange([]byte(""), []byte(""))
	require.NoError(t, err)
	files, _, rules := generateFiles()
	rulesMap := map[int64]*restoreutils.RewriteRules{
		0: rules,
	}
	for _, file := range files {
		importer.PauseForBackpressure()
		err = importer.Import(ctx, restore.BackupFileSet{SSTFiles: []*backuppb.File{file}, RewriteRules: rulesMap})
		require.NoError(t, err)
	}
	err = importer.Close()
	require.NoError(t, err)
}
