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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/restore"
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
	GetFileRangeKey         = getFileRangeKey
	GetSortedPhysicalTables = getSortedPhysicalTables
)

// MockClient create a fake Client used to test.
func MockClient(dbs map[string]*metautil.Database) *SnapClient {
	return &SnapClient{databases: dbs}
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
	opt := NewSnapFileImporterOptions(nil, nil, fakeImportClient, nil, rc.rewriteMode, nil, 128, createCallBacks, closeCallBacks)
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
	rc.AllocTableIDs(context.TODO(), tables)
	rewriteRules := &restoreutils.RewriteRules{
		Data: make([]*import_sstpb.RewriteRule, 0),
	}
	newTables := make([]*model.TableInfo, 0, len(tables))
	tbMapping := map[string]int{}
	for i, t := range tables {
		tbMapping[t.Info.Name.String()] = i
	}
	rc.AllocTableIDs(context.Background(), tables)
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
