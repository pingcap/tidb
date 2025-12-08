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

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"golang.org/x/exp/slices"
)

var (
	RestoreLabelKey   = restoreLabelKey
	RestoreLabelValue = restoreLabelValue

	GetSSTMetaFromFile      = getSSTMetaFromFile
	GetKeyRangeByMode       = getKeyRangeByMode
	MapTableToFiles         = mapTableToFiles
	GetFileRangeKey         = getFileRangeKey
	GetSortedPhysicalTables = getSortedPhysicalTables
)

// MockClient create a fake Client used to test.
func MockClient(dbs map[string]*metautil.Database) *SnapClient {
	return &SnapClient{databases: dbs}
}

// Mock the call of setSpeedLimit function
func MockCallSetSpeedLimit(ctx context.Context, fakeImportClient importclient.ImporterClient, rc *SnapClient, concurrency uint) (err error) {
	rc.SetRateLimit(42)
	rc.workerPool = tidbutil.NewWorkerPool(128, "set-speed-limit")
	rc.hasSpeedLimited = false
	rc.fileImporter, err = NewSnapFileImporter(ctx, nil, fakeImportClient, nil, false, false, nil, rc.rewriteMode, 128)
	if err != nil {
		return errors.Trace(err)
	}
	return rc.setSpeedLimit(ctx, rc.rateLimit)
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
<<<<<<< HEAD
=======

func (rc *SnapClient) RegisterUpdateMetaAndLoadStats(
	builder *PipelineConcurrentBuilder,
	s storage.ExternalStorage,
	updateCh glue.Progress,
	statsConcurrency uint,
) {
	rc.registerUpdateMetaAndLoadStats(builder, s, updateCh, statsConcurrency)
}

func (rc *SnapClient) ReplaceTables(
	ctx context.Context,
	createdTables []*restoreutils.CreatedTable,
	schemaVersionPair SchemaVersionPairT,
	restoreTS uint64,
	loadStatsPhysical, loadSysTablePhysical bool,
	kvClient kv.Client,
	checksum bool,
	checksumConcurrency uint,
) (int, error) {
	return rc.replaceTables(
		ctx,
		createdTables,
		schemaVersionPair,
		restoreTS,
		loadStatsPhysical,
		loadSysTablePhysical,
		kvClient,
		checksum,
		checksumConcurrency,
	)
}

func NewTemporaryTableChecker(loadStatsPhysical, loadSysTablePhysical bool) *TemporaryTableChecker {
	return &TemporaryTableChecker{loadStatsPhysical: loadStatsPhysical, loadSysTablePhysical: loadSysTablePhysical}
}

func (rc *SnapClient) CheckPrivilegeTableRowsCollateCompatibility(
	ctx context.Context,
	dbNameL, tableNameL string,
	upstreamTable, downstreamTable *model.TableInfo,
) error {
	return rc.checkPrivilegeTableRowsCollateCompatibility(ctx, dbNameL, tableNameL, upstreamTable, downstreamTable)
}
>>>>>>> 0cb39391dce (br: support restore privileges tables from v6.5 to v7.5 (#64668))
