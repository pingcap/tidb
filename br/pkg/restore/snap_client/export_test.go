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

package snapclient

import (
	"cmp"
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

var (
	RestoreLabelKey   = restoreLabelKey
	RestoreLabelValue = restoreLabelValue

	GetSSTMetaFromFile             = getSSTMetaFromFile
	GetKeyRangeByMode              = getKeyRangeByMode
	MapTableToFiles                = mapTableToFiles
	GetFileRangeKey                = getFileRangeKey
	GetSortedPhysicalTables        = getSortedPhysicalTables
	BuildSystemTableReplaceColumns = buildSystemTableReplaceColumns
)

func TestMVSystemTablesAreUnrecoverable(t *testing.T) {
	for _, tableName := range []string{
		"tidb_mview_refresh_info",
		"tidb_mlog_purge_info",
		"tidb_mview_refresh_hist",
		"tidb_mview_refresh_alert",
		"tidb_mlog_purge_hist",
	} {
		require.True(t, isUnrecoverableTable(mysql.SystemDB, tableName), tableName)
	}
}

// MockClient create a fake Client used to test.
func MockClient(dbs map[string]*metautil.Database) *SnapClient {
	return &SnapClient{databases: dbs}
}

func (rc *SnapClient) SetDatabases(dbs map[string]*metautil.Database) {
	rc.databases = dbs
}

// Mock the call of setSpeedLimit function
func MockCallSetSpeedLimit(ctx context.Context, stores []*metapb.Store, fakeImportClient importclient.ImporterClient, rc *SnapClient, concurrency uint) (err error) {
	rc.SetRateLimit(42)
	rc.workerPool = tidbutil.NewWorkerPool(128, "set-speed-limit")
	setFn := SetSpeedLimitFn(ctx, stores, rc.workerPool)
	var createCallBacks []func(*SnapFileImporter) error
	var closeCallBacks []func(*SnapFileImporter) error

	createCallBacks = append(createCallBacks, func(importer *SnapFileImporter) error {
		return setFn(importer, rc.rateLimit)
	})
	closeCallBacks = append(createCallBacks, func(importer *SnapFileImporter) error {
		return setFn(importer, 0)
	})
	opt := NewSnapFileImporterOptions(nil, nil, fakeImportClient, nil, rc.rewriteMode, nil, 128, 128, false, createCallBacks, closeCallBacks)
	fileImporter, err := NewSnapFileImporter(ctx, kvrpcpb.APIVersion(0), TiDBFull, opt)
	rc.restorer = restore.NewSimpleSstRestorer(ctx, fileImporter, rc.workerPool, nil)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// CreateTables creates multiple tables, and returns their rewrite rules.
func (rc *SnapClient) CreateTablesTest(
	dom *domain.Domain,
	tables []*metautil.Table,
	newTS uint64,
) (*restoreutils.RewriteRules, []*model.TableInfo, error) {
	rc.dom = dom
	rewriteRules := &restoreutils.RewriteRules{
		Data: make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	tbMapping := map[string]int{}
	for i, t := range tables {
		tbMapping[t.Info.Name.String()] = i
	}
	createdTables, err := rc.CreateTables(context.TODO(), tables, newTS)
	if err != nil {
		return nil, nil, err
	}
	for _, table := range createdTables {
		rules := table.RewriteRule
		rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
		newTables = append(newTables, table.Table)
	}
	// Let's ensure that it won't break the original order.
	slices.SortFunc(newTables, func(i, j *model.TableInfo) int {
		return cmp.Compare(tbMapping[i.Name.String()], tbMapping[j.Name.String()])
	})
	return rewriteRules, newTables, nil
}

func (options *SnapFileImporterOptions) SetRegionScanConcurrency(concurrency uint) {
	options.scanConcurrency = concurrency
}

func NewSnapFileImporterOptionsForTest(
	splitClient split.SplitClient,
	importClient importclient.ImporterClient,
	tikvStores []*metapb.Store,
	rewriteMode RewriteMode,
	concurrencyPerStore uint,
) *SnapFileImporterOptions {
	return &SnapFileImporterOptions{
		metaClient:          splitClient,
		importClient:        importClient,
		tikvStores:          tikvStores,
		rewriteMode:         rewriteMode,
		concurrencyPerStore: concurrencyPerStore,
	}
}

func (importer *SnapFileImporter) PaginateScanRegionForTest(ctx context.Context, startKey, endKey []byte) ([]*split.RegionInfo, error) {
	return importer.paginateScanRegion(ctx, startKey, endKey)
}

// snapshotPeerDownloadRetryProbe models capability support independently for each store.
type snapshotPeerDownloadRetryProbe struct {
	importclient.ImporterClient
	supported map[uint64]bool
	probeErr  error
	probed    []uint64
}

func (p *snapshotPeerDownloadRetryProbe) IsBatchDownloadLatestMVCCSupported(_ context.Context, stores []uint64) (bool, error) {
	p.probed = append([]uint64(nil), stores...)
	if p.probeErr != nil {
		return false, p.probeErr
	}
	for _, id := range stores {
		if !p.supported[id] {
			return false, nil
		}
	}
	return true, nil
}

func TestSnapshotPeerDownloadRetrySupport(t *testing.T) {
	for _, tt := range []struct {
		name             string
		hasTiFlash       bool
		tiKVSupported    bool
		tiFlashSupported bool
		tiFlashState     metapb.StoreState
		pitr             bool
		probeErr         error
		wantRetry        bool
		wantStores       []uint64
	}{
		{name: "TiKV only", tiKVSupported: true, wantRetry: true, wantStores: []uint64{1}},
		{name: "unsupported TiKV", wantStores: []uint64{1}},
		{name: "unsupported TiFlash", hasTiFlash: true, tiKVSupported: true, wantStores: []uint64{1, 2}},
		{name: "supported TiFlash", hasTiFlash: true, tiKVSupported: true, tiFlashSupported: true, wantRetry: true, wantStores: []uint64{1, 2}},
		{name: "offline TiFlash", hasTiFlash: true, tiKVSupported: true, tiFlashState: metapb.StoreState_Offline, wantRetry: true, wantStores: []uint64{1}},
		{name: "probe error", hasTiFlash: true, tiKVSupported: true, probeErr: errors.New("probe unavailable"), wantStores: []uint64{1, 2}},
		{name: "PiTR ignores unsupported TiFlash", hasTiFlash: true, tiKVSupported: true, pitr: true, wantRetry: true, wantStores: []uint64{1}},
		{name: "PiTR unsupported TiKV", hasTiFlash: true, tiFlashSupported: true, pitr: true, wantStores: []uint64{1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tiKVStores := []*metapb.Store{{Id: 1, State: metapb.StoreState_Up}}
			allStores := append([]*metapb.Store(nil), tiKVStores...)
			if tt.hasTiFlash {
				allStores = append(allStores, &metapb.Store{
					Id: 2, State: tt.tiFlashState,
					Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}},
				})
			}
			probe := &snapshotPeerDownloadRetryProbe{
				supported: map[uint64]bool{1: tt.tiKVSupported, 2: tt.tiFlashSupported},
				probeErr:  tt.probeErr,
			}
			importer := &SnapFileImporter{importClient: probe, peerDownloadRetry: true}
			ctx := context.Background()
			rc := &SnapClient{pdClient: split.NewFakePDClient(allStores, false, nil)}
			rc.SetIsPiTR(tt.pitr)
			err := rc.checkPeerDownloadRetrySupport(ctx, importer, tiKVStores)
			require.NoError(t, err)
			require.Equal(t, tt.wantRetry, importer.peerDownloadRetry)
			require.Equal(t, tt.wantStores, probe.probed)
			require.Len(t, tiKVStores, 1)
			require.Equal(t, uint64(1), tiKVStores[0].Id)
		})
	}
}
