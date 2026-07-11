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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
)

func TestGetKeyRangeByMode(t *testing.T) {
	file := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	endFile := &backuppb.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte(""),
	}
	rule := &restoreutils.RewriteRules{
		Data: []*import_sstpb.RewriteRule{
			{
				OldKeyPrefix: []byte("t1"),
				NewKeyPrefix: []byte("t2"),
			},
		},
	}
	// raw kv
	testRawFn := snapclient.GetKeyRangeByMode(snapclient.Raw)
	start, end, err := testRawFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte("t1ccc"), end)

	start, end, err = testRawFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, []byte("t1a"), start)
	require.Equal(t, []byte(""), end)

	// txn kv: the keys must be encoded.
	testTxnFn := snapclient.GetKeyRangeByMode(snapclient.Txn)
	start, end, err = testTxnFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1ccc")), end)

	start, end, err = testTxnFn(endFile, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t1a")), start)
	require.Equal(t, []byte(""), end)

	// normal kv: the keys must be encoded.
	testFn := snapclient.GetKeyRangeByMode(snapclient.TiDBFull)
	start, end, err = testFn(file, rule)
	require.NoError(t, err)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2a")), start)
	require.Equal(t, codec.EncodeBytes(nil, []byte("t2ccc")), end)

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
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta, err := snapclient.GetSSTMetaFromFile(file, region, rule, snapclient.RewriteModeLegacy)
	require.Nil(t, err)
	require.Equal(t, "t2abc", string(sstMeta.GetRange().GetStart()))
	require.Equal(t, "t2\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff", string(sstMeta.GetRange().GetEnd()))
}

type fakeImporterClient struct {
	importclient.ImporterClient

	speedLimitReq map[uint64]*import_sstpb.SetDownloadSpeedLimitRequest
}

func newFakeImporterClient() *fakeImporterClient {
	return &fakeImporterClient{
		speedLimitReq: make(map[uint64]*import_sstpb.SetDownloadSpeedLimitRequest),
	}
}

func (client *fakeImporterClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	client.speedLimitReq[storeID] = &import_sstpb.SetDownloadSpeedLimitRequest{
		TaskId:     req.GetTaskId(),
		SpeedLimit: req.GetSpeedLimit(),
		TtlSeconds: req.GetTtlSeconds(),
	}
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
	setReq := importClient.speedLimitReq[1]
	require.NotNil(t, setReq)
	require.Equal(t, uint64(5), setReq.GetSpeedLimit())
	require.NotEmpty(t, setReq.GetTaskId())
	require.Equal(t, uint64(snapclient.DownloadRateLimitTTLSeconds), setReq.GetTtlSeconds())

	err = importer.SetDownloadSpeedLimit(ctx, 1, 0)
	require.NoError(t, err)
	resetReq := importClient.speedLimitReq[1]
	require.NotNil(t, resetReq)
	require.Equal(t, uint64(0), resetReq.GetSpeedLimit())
	require.Equal(t, setReq.GetTaskId(), resetReq.GetTaskId())
	require.Equal(t, uint64(snapclient.DownloadRateLimitTTLSeconds), resetReq.GetTtlSeconds())

	err = importer.SetRawRange(nil, nil)
	require.Error(t, err)
	files, rules := generateFiles()
	for _, file := range files {
		importer.PauseForBackpressure()
		err = importer.Import(ctx, restore.BackupFileSet{SSTFiles: []*backuppb.File{file}, RewriteRules: rules})
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
	files, rules := generateFiles()
	for _, file := range files {
		importer.PauseForBackpressure()
		err = importer.Import(ctx, restore.BackupFileSet{SSTFiles: []*backuppb.File{file}, RewriteRules: rules})
		require.NoError(t, err)
	}
	err = importer.Close()
	require.NoError(t, err)
}

type flowControlSplitClient struct {
	split.SplitClient

	t         *testing.T
	inFlight  atomic.Int32
	maxFlight int32
}

func (c *flowControlSplitClient) ScanRegions(
	ctx context.Context,
	key, endKey []byte,
	limit int,
	opts ...opt.GetRegionOption,
) ([]*split.RegionInfo, error) {
	cur := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	require.LessOrEqual(c.t, cur, c.maxFlight)

	return []*split.RegionInfo{
		{
			Region: &metapb.Region{
				StartKey: key,
				EndKey:   endKey,
			},
			Leader: &metapb.Peer{
				StoreId: 1,
			},
		},
	}, nil
}

func (*flowControlSplitClient) GetCodecPDClient() *tikvclient.CodecPDClient {
	return nil
}

func TestSnapImporterPDScanRequestFlowControl(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	maxFlight := 1
	splitClient := &flowControlSplitClient{
		t:         t,
		maxFlight: int32(maxFlight),
	}
	importClient := newFakeImporterClient()
	opt := snapclient.NewSnapFileImporterOptionsForTest(
		splitClient, importClient, generateStores(), snapclient.RewriteModeKeyspace, 10,
	)
	opt.SetRegionScanConcurrency(uint(maxFlight))
	importer, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.TiDBFull, opt)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			_, err := importer.PaginateScanRegionForTest(ctx, []byte{}, []byte{})
			require.NoError(t, err)
		})
	}
	wg.Wait()
}

type blockingBatchDownloadImporterClient struct {
	fakeImporterClient

	active    atomic.Int32
	maxActive atomic.Int32
	startedCh chan struct{}
	unblock   chan struct{}
}

func newBlockingBatchDownloadImporterClient() *blockingBatchDownloadImporterClient {
	return &blockingBatchDownloadImporterClient{
		fakeImporterClient: *newFakeImporterClient(),
		startedCh:          make(chan struct{}, 16),
		unblock:            make(chan struct{}),
	}
}

func (client *blockingBatchDownloadImporterClient) waitUntilUnblocked(ctx context.Context) error {
	active := client.active.Add(1)
	defer client.active.Add(-1)
	for {
		maxActive := client.maxActive.Load()
		if active <= maxActive || client.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	client.startedCh <- struct{}{}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-client.unblock:
	}
	return nil
}

func (client *blockingBatchDownloadImporterClient) BatchDownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	if err := client.waitUntilUnblocked(ctx); err != nil {
		return nil, err
	}
	return &import_sstpb.DownloadResponse{Range: *req.Sst.GetRange()}, nil
}

func (client *blockingBatchDownloadImporterClient) BatchDownloadLatestMVCC(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	if err := client.waitUntilUnblocked(ctx); err != nil {
		return nil, err
	}

	sst := req.Sst
	return &import_sstpb.DownloadResponse{
		Range: *req.Sst.GetRange(),
		Ssts:  []*import_sstpb.SSTMeta{&sst},
	}, nil
}

func (client *blockingBatchDownloadImporterClient) CheckBatchDownloadSupport(ctx context.Context, stores []uint64) (bool, error) {
	return true, nil
}

func makeCompactedFileSets(fileGroupCount, filesPerGroup int) []restore.BackupFileSet {
	fileSets := make([]restore.BackupFileSet, 0, fileGroupCount)
	for i := 0; i < fileGroupCount; i++ {
		files := make([]*backuppb.File, 0, filesPerGroup)
		for j := 0; j < filesPerGroup; j++ {
			files = append(files, &backuppb.File{
				Name:     fmt.Sprintf("file-%d-%d_write.sst", i, j),
				Cf:       restoreutils.WriteCFName,
				StartKey: tablecodec.EncodeTablePrefix(100),
				EndKey:   append(tablecodec.EncodeTablePrefix(100), 'z'),
			})
		}
		fileSets = append(fileSets, restore.BackupFileSet{
			SSTFiles: files,
			RewriteRules: &restoreutils.RewriteRules{
				Data: []*import_sstpb.RewriteRule{{
					OldKeyPrefix: tablecodec.EncodeTablePrefix(100),
					NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
				}},
			},
		})
	}
	return fileSets
}

func waitForConcurrentDownloads(t *testing.T, importClient *blockingBatchDownloadImporterClient, errCh <-chan error) {
	t.Helper()
	for i := 0; i < 2; i++ {
		select {
		case <-importClient.startedCh:
		case err := <-errCh:
			require.NoError(t, err)
			require.Fail(t, "import finished before multiple download requests started")
		case <-time.After(time.Second):
			require.Fail(t, "expected multiple batch download requests to run concurrently")
		}
	}
	close(importClient.unblock)
	require.GreaterOrEqual(t, importClient.maxActive.Load(), int32(2))

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "import did not finish")
	}
}

func TestBatchDownloadLatestMVCCParallelizesFileGroupsPerPeer(t *testing.T) {
	ctx := context.Background()
	splitClient := split.NewFakeSplitClient()
	splitClient.AppendPdRegion(&router.Region{
		Meta: &metapb.Region{
			Id:       1,
			StartKey: codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(1)),
			EndKey:   codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(2)),
			Peers:    []*metapb.Peer{{StoreId: 1}},
		},
		Leader: &metapb.Peer{StoreId: 1},
	})
	importClient := newBlockingBatchDownloadImporterClient()
	opt := snapclient.NewSnapFileImporterOptions(
		nil,
		splitClient,
		importClient,
		nil,
		snapclient.RewriteModeKeyspace,
		[]*metapb.Store{{Id: 1, State: metapb.StoreState_Up}},
		2,
		0,
		true,
		nil,
		nil,
	)
	importer, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.TiDBCompacted, opt)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, importer.Close())
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- importer.Import(ctx, makeCompactedFileSets(3, 1)...)
	}()
	unblocked := false
	defer func() {
		if !unblocked {
			close(importClient.unblock)
		}
	}()
	waitForConcurrentDownloads(t, importClient, errCh)
	unblocked = true
}

func TestBatchDownloadSSTParallelizesFileGroupsPerPeer(t *testing.T) {
	ctx := context.Background()
	splitClient := split.NewFakeSplitClient()
	splitClient.AppendPdRegion(&router.Region{
		Meta: &metapb.Region{
			Id:       1,
			StartKey: codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(1)),
			EndKey:   codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(2)),
			Peers:    []*metapb.Peer{{StoreId: 1}},
		},
		Leader: &metapb.Peer{StoreId: 1},
	})
	stores := []*metapb.Store{{Id: 1, State: metapb.StoreState_Up}}
	importClient := newBlockingBatchDownloadImporterClient()
	opt := snapclient.NewSnapFileImporterOptions(
		nil,
		splitClient,
		importClient,
		nil,
		snapclient.RewriteModeKeyspace,
		stores,
		2,
		0,
		false,
		nil,
		nil,
	)
	importer, err := snapclient.NewSnapFileImporter(ctx, kvrpcpb.APIVersion_V1, snapclient.TiDBCompacted, opt)
	require.NoError(t, err)
	require.NoError(t, importer.CheckBatchDownloadSupport(ctx, stores))
	defer func() {
		require.NoError(t, importer.Close())
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- importer.Import(ctx, makeCompactedFileSets(3, 2)...)
	}()
	unblocked := false
	defer func() {
		if !unblocked {
			close(importClient.unblock)
		}
	}()
	waitForConcurrentDownloads(t, importClient, errCh)
	unblocked = true
}
